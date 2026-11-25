"""
05_spark_sql.py — Executa sql/spark/lakehouse_spark.sql com Spark SQL sobre o data lake (data/).
Imprime o resultado de cada consulta e falha se alguma instrução der erro.
"""
import os, re, sys, pathlib
from pyspark.sql import SparkSession

RAIZ = pathlib.Path(__file__).resolve().parent
DATA = str(RAIZ / "data")
SQL = (RAIZ.parent / "sql" / "spark" / "lakehouse_spark.sql").read_text(encoding="utf-8").replace("{{DATA}}", DATA)

spark = (SparkSession.builder.appName("lakehouse-sql").master("local[*]")
         .config("spark.sql.warehouse.dir", f"{DATA}/warehouse")
         .config("spark.sql.shuffle.partitions", "8").config("spark.ui.showConsoleProgress", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# remove comentários de linha e separa por ';' no fim de instrução
limpo = "\n".join(l for l in SQL.splitlines() if not l.strip().startswith("--"))
for i, stmt in enumerate([s.strip() for s in re.split(r";\s*(?:\n|$)", limpo) if s.strip()], 1):
    primeira = " ".join(stmt.split())[:90]
    print(f"\n[{i}] {primeira}")
    df = spark.sql(stmt)
    if stmt.upper().startswith(("SELECT", "WITH", "SHOW")):
        df.show(12, truncate=60)
spark.stop()
