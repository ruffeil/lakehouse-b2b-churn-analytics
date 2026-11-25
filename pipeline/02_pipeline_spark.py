"""
02_pipeline_spark.py
Lakehouse em camadas (Medallion) com Apache Spark:
  Bronze  -> dado bruto + metadados de ingestão (batch do ERP e streaming de eventos do CRM)
  Silver  -> tipagem, regras de Data Quality, deduplicação e quarentena
  Gold    -> tabela de features por cliente (sem vazamento: só dados até a data de corte) + margem por canal
"""
import json, os, shutil
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType

BASE = "data"
CORTE = "2025-09-30"          # data de corte: features só usam dados até aqui
JANELA_FIM = "2025-12-31"     # rótulo de churn: sem pedidos entre o corte e esta data

spark = (SparkSession.builder.appName("lakehouse-b2b").master("local[*]")
         .config("spark.sql.ansi.enabled", "false")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.showConsoleProgress", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
for d in ["bronze", "silver", "gold", "_chk"]:
    shutil.rmtree(f"{BASE}/{d}", ignore_errors=True)

# =============== BRONZE ===============
# 1) Batch: ERP (CSV) — tudo como string, sem interpretar nada
ped_raw = (spark.read.option("header", True).csv(f"{BASE}/raw/erp_pedidos.csv")
           .withColumn("_ingest_ts", F.current_timestamp()).withColumn("_fonte", F.lit("erp_pedidos")))
cli_raw = (spark.read.option("header", True).csv(f"{BASE}/raw/erp_clientes.csv")
           .withColumn("_ingest_ts", F.current_timestamp()).withColumn("_fonte", F.lit("erp_clientes")))
ped_raw.write.mode("overwrite").parquet(f"{BASE}/bronze/erp_pedidos")
cli_raw.write.mode("overwrite").parquet(f"{BASE}/bronze/erp_clientes")

# 2) Streaming: eventos do CRM. Em produção: .format("kafka").option("subscribe", "crm.eventos")
#    Aqui os micro-lotes JSON simulam as mensagens do tópico; mesmo código de Structured Streaming.
schema_ev = StructType([StructField(c, StringType()) for c in ["cliente_id", "tipo", "data_evento"]])
q = (spark.readStream.schema(schema_ev).option("maxFilesPerTrigger", 3).json(f"{BASE}/raw/crm_events")
     .withColumn("_ingest_ts", F.current_timestamp()).withColumn("_fonte", F.lit("crm_eventos"))
     .writeStream.format("parquet").option("path", f"{BASE}/bronze/crm_eventos")
     .option("checkpointLocation", f"{BASE}/_chk/crm").trigger(availableNow=True).start())
q.awaitTermination()

# =============== SILVER + DATA QUALITY ===============
bp = spark.read.parquet(f"{BASE}/bronze/erp_pedidos")
tp = (bp.withColumn("data_pedido_dt", F.to_date("data_pedido", "yyyy-MM-dd"))
        .withColumn("valor_bruto", F.col("valor_bruto").cast("double"))
        .withColumn("desconto_pct", F.col("desconto_pct").cast("double"))
        .withColumn("margem_rs", F.col("margem_rs").cast("double")))

regras = {                                   # nome da regra -> condição de violação
    "chave_cliente_nula":   F.col("cliente_id").isNull(),
    "data_invalida":        F.col("data_pedido_dt").isNull(),
    "valor_nao_positivo":   F.col("valor_bruto") <= 0,
    "desconto_fora_faixa":  (F.col("desconto_pct") < 0) | (F.col("desconto_pct") > 0.6),
}
motivo = F.concat_ws(",", *[F.when(c, F.lit(n)) for n, c in regras.items()])
tp = tp.withColumn("_motivo_rejeicao", motivo)

# deduplicação por chave de negócio (mantém o registro mais recente ingerido)
w = Window.partitionBy("pedido_id").orderBy(F.col("_ingest_ts").desc())
tp = tp.withColumn("_rn", F.row_number().over(w))
duplicados = tp.filter(F.col("_rn") > 1).count()
tp = tp.filter(F.col("_rn") == 1).drop("_rn")

rejeitados = tp.filter(F.col("_motivo_rejeicao") != "")
validos = tp.filter(F.col("_motivo_rejeicao") == "").drop("_motivo_rejeicao")
rejeitados.write.mode("overwrite").parquet(f"{BASE}/silver/quarentena_pedidos")
validos.write.mode("overwrite").parquet(f"{BASE}/silver/pedidos")

total_bronze = bp.count()
rel = {"linhas_bronze": total_bronze, "duplicados_removidos": duplicados,
       "rejeitados_quarentena": rejeitados.count(), "linhas_silver": validos.count()}
rel["violacoes_por_regra"] = {n: tp.filter(c).count() for n, c in regras.items()}
rel["taxa_aprovacao_pct"] = round(100 * rel["linhas_silver"] / total_bronze, 2)
json.dump(rel, open(f"{BASE}/dq_relatorio.json", "w"), indent=2, ensure_ascii=False)

ev = (spark.read.parquet(f"{BASE}/bronze/crm_eventos")
      .withColumn("data_evento", F.to_date("data_evento", "yyyy-MM-dd"))
      .filter(F.col("cliente_id").isNotNull() & F.col("data_evento").isNotNull()).dropDuplicates())
ev.write.mode("overwrite").parquet(f"{BASE}/silver/crm_eventos")
(spark.read.parquet(f"{BASE}/bronze/erp_clientes").drop("_ingest_ts", "_fonte")
      .withColumn("ticket_base", F.col("ticket_base").cast("double")).dropDuplicates(["cliente_id"])
      .write.mode("overwrite").parquet(f"{BASE}/silver/clientes"))

# =============== GOLD ===============
ped = spark.read.parquet(f"{BASE}/silver/pedidos").withColumnRenamed("data_pedido_dt", "dt")
ev = spark.read.parquet(f"{BASE}/silver/crm_eventos")
cli = spark.read.parquet(f"{BASE}/silver/clientes").select("cliente_id", "canal", "porte", "uf")
corte = F.lit(CORTE).cast("date")

hist = ped.filter(F.col("dt") <= corte)                    # SOMENTE passado em relação ao corte
feat = (hist.groupBy("cliente_id").agg(
    F.datediff(corte, F.max("dt")).alias("recencia_dias"),
    F.count("*").alias("pedidos_total"),
    F.sum(F.when(F.col("dt") > F.add_months(corte, -3), 1).otherwise(0)).alias("pedidos_3m"),
    F.sum(F.when((F.col("dt") <= F.add_months(corte, -3)) & (F.col("dt") > F.add_months(corte, -6)), 1).otherwise(0)).alias("pedidos_3m_ant"),
    F.sum(F.when(F.col("dt") > F.add_months(corte, -12), F.col("valor_bruto") * (1 - F.col("desconto_pct"))).otherwise(0)).alias("receita_12m"),
    F.sum(F.when(F.col("dt") > F.add_months(corte, -12), F.col("margem_rs")).otherwise(0)).alias("margem_12m"),
    F.avg("desconto_pct").alias("desconto_medio"),
    F.avg(F.when(F.col("dt") > F.add_months(corte, -3), F.col("desconto_pct"))).alias("desconto_3m"),
    F.datediff(corte, F.min("dt")).alias("tempo_casa_dias")))
feat = feat.withColumn("tendencia_pedidos", F.col("pedidos_3m") - F.col("pedidos_3m_ant"))

evf = (ev.filter(F.col("data_evento") <= corte).groupBy("cliente_id").agg(
    F.sum(F.when((F.col("tipo") == "chamado_suporte") & (F.col("data_evento") > F.add_months(corte, -3)), 1).otherwise(0)).alias("chamados_3m"),
    F.sum(F.when((F.col("tipo") == "reclamacao") & (F.col("data_evento") > F.add_months(corte, -3)), 1).otherwise(0)).alias("reclamacoes_3m"),
    F.sum(F.when((F.col("tipo") == "cotacao") & (F.col("data_evento") > F.add_months(corte, -3)), 1).otherwise(0)).alias("cotacoes_3m")))

# rótulo: cliente ativo nos 6 meses até o corte que NÃO compra no trimestre seguinte
futuro = (ped.filter((F.col("dt") > corte) & (F.col("dt") <= F.lit(JANELA_FIM).cast("date")))
          .groupBy("cliente_id").agg(F.count("*").alias("pedidos_futuros")))
gold = (feat.filter(F.col("recencia_dias") <= 183)
        .join(evf, "cliente_id", "left").join(cli, "cliente_id", "left").join(futuro, "cliente_id", "left")
        .fillna(0, subset=["chamados_3m", "reclamacoes_3m", "cotacoes_3m", "pedidos_futuros"])
        .fillna(0, subset=["desconto_3m"])
        .withColumn("churn", (F.col("pedidos_futuros") == 0).cast("int")).drop("pedidos_futuros"))
gold.write.mode("overwrite").parquet(f"{BASE}/gold/features_cliente")

margem_canal = (ped.withColumn("mes", F.date_format("dt", "yyyy-MM")).groupBy("mes", "canal").agg(
    F.sum(F.col("valor_bruto") * (1 - F.col("desconto_pct"))).alias("receita_liquida"),
    F.sum("margem_rs").alias("margem_rs"), F.avg("desconto_pct").alias("desconto_medio"),
    F.countDistinct("cliente_id").alias("clientes_ativos")).orderBy("mes", "canal"))
margem_canal.write.mode("overwrite").parquet(f"{BASE}/gold/margem_canal_mes")

print(json.dumps(rel, indent=2, ensure_ascii=False))
print("gold clientes:", gold.count(), "| taxa de churn no rótulo:", round(gold.agg(F.avg("churn")).first()[0], 3))
spark.stop()
