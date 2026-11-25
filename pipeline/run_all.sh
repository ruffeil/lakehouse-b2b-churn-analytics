#!/usr/bin/env bash
# Executa o fluxo completo: dados sintéticos -> Spark (Bronze/Silver/Gold + DQ) -> modelo -> painéis
set -euo pipefail
cd "$(dirname "$0")"
python3 01_gerar_dados.py
python3 02_pipeline_spark.py
python3 03_modelo_churn.py
python3 04_paineis.py
python3 05_spark_sql.py
echo "Pronto. Resultados em data/gold/resultados.json e painel_*.png"
