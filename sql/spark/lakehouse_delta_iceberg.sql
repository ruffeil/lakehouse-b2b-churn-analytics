-- =====================================================================
-- lakehouse_delta_iceberg.sql — Formatos de tabela transacionais (ACID) no data lake
-- ATENÇÃO: este arquivo NÃO foi executado neste repositório. Exige Spark com o pacote
-- Delta Lake (io.delta:delta-spark) ou Apache Iceberg configurado no catálogo, o que não
-- estava disponível no ambiente de desenvolvimento. É o desenho-alvo de produção para
-- substituir o Parquet "cru" da Silver/Gold por tabelas com transação, histórico e evolução.
-- =====================================================================

-- ======================= DELTA LAKE =======================
-- spark-submit --packages io.delta:delta-spark_2.13:<versão> \
--   --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

CREATE TABLE IF NOT EXISTS lakehouse.silver_pedido (
  pedido_id    STRING NOT NULL,
  cliente_id   STRING NOT NULL,
  canal        STRING,
  data_pedido  DATE   NOT NULL,
  valor_bruto  DOUBLE,
  desconto_pct DOUBLE,
  margem_rs    DOUBLE,
  ano_mes      STRING GENERATED ALWAYS AS (date_format(data_pedido, 'yyyy-MM'))
) USING DELTA
PARTITIONED BY (ano_mes)
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

-- Constraints aplicadas NA ESCRITA (o lake passa a recusar dado inválido, como o banco)
ALTER TABLE lakehouse.silver_pedido ADD CONSTRAINT valor_positivo  CHECK (valor_bruto > 0);
ALTER TABLE lakehouse.silver_pedido ADD CONSTRAINT desconto_faixa  CHECK (desconto_pct BETWEEN 0 AND 0.6);

-- Upsert idempotente (reprocessar o mesmo lote não duplica nem corrompe)
MERGE INTO lakehouse.silver_pedido AS d
USING (SELECT * FROM lakehouse.bronze_pedido_validado) AS s
ON d.pedido_id = s.pedido_id AND d.ano_mes = s.ano_mes
WHEN MATCHED AND d.margem_rs <> s.margem_rs THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- SCD Tipo 2 do cliente com MERGE (fecha a versão antiga e abre a nova em uma só transação)
MERGE INTO lakehouse.dim_cliente AS d
USING (
  SELECT s.cliente_id AS chave_merge, s.* FROM lakehouse.stg_cliente s
  UNION ALL
  SELECT NULL AS chave_merge, s.* FROM lakehouse.stg_cliente s
  JOIN lakehouse.dim_cliente c ON c.cliente_id = s.cliente_id AND c.is_current AND c.hash_atributos <> s.hash_atributos
) AS s
ON d.cliente_id = s.chave_merge AND d.is_current
WHEN MATCHED AND d.hash_atributos <> s.hash_atributos THEN UPDATE SET is_current = false, vigencia_fim = current_date()
WHEN NOT MATCHED THEN INSERT (cliente_id, canal, porte, uf, hash_atributos, vigencia_ini, vigencia_fim, is_current)
  VALUES (s.cliente_id, s.canal, s.porte, s.uf, s.hash_atributos, current_date(), DATE '9999-12-31', true);

-- Manutenção: compactação de arquivos pequenos + co-localização por colunas de filtro frequente
OPTIMIZE lakehouse.silver_pedido WHERE ano_mes >= '2025-01' ZORDER BY (cliente_id);
VACUUM lakehouse.silver_pedido RETAIN 168 HOURS;      -- retenção de 7 dias para time travel

-- Viagem no tempo: auditoria e reprodutibilidade do treino do modelo
SELECT count(*) FROM lakehouse.silver_pedido VERSION AS OF 12;
SELECT count(*) FROM lakehouse.silver_pedido TIMESTAMP AS OF '2025-10-01 00:00:00';
RESTORE TABLE lakehouse.silver_pedido TO VERSION AS OF 12;      -- rollback de carga ruim
DESCRIBE HISTORY lakehouse.silver_pedido;

-- Change Data Feed: só o que mudou (alimenta o PostgreSQL e o mart de forma incremental)
SELECT * FROM table_changes('lakehouse.silver_pedido', 20, 25) WHERE _change_type IN ('insert', 'update_postimage');

-- ======================= APACHE ICEBERG (alternativa) =======================
-- Particionamento "oculto" por transformação: consultas filtram por data sem conhecer a partição.
CREATE TABLE IF NOT EXISTS lake.silver.pedido (
  pedido_id STRING, cliente_id STRING, canal STRING, data_pedido DATE,
  valor_bruto DOUBLE, desconto_pct DOUBLE, margem_rs DOUBLE
) USING iceberg
PARTITIONED BY (months(data_pedido), bucket(16, cliente_id));

ALTER TABLE lake.silver.pedido ADD COLUMN origem STRING;                 -- evolução de esquema sem reescrever dados
ALTER TABLE lake.silver.pedido WRITE ORDERED BY cliente_id, data_pedido;

SELECT snapshot_id, committed_at, operation FROM lake.silver.pedido.snapshots;
CALL lake.system.rollback_to_snapshot('silver.pedido', 1234567890123456789);
CALL lake.system.rewrite_data_files(table => 'silver.pedido', strategy => 'sort');
CALL lake.system.expire_snapshots(table => 'silver.pedido', older_than => TIMESTAMP '2025-10-01 00:00:00');
