#!/usr/bin/env bash
# Provisiona e carrega o DW relacional. Requer PostgreSQL 15+ e os dados gerados por pipeline/01_gerar_dados.py.
#   export PGHOST=... PGPORT=... PGUSER=postgres ; ETL_PWD='<senha>' ./run_pg.sh [diretorio_data_raw]
set -euo pipefail
AQUI="$(cd "$(dirname "$0")" && pwd)"
RAW="${1:-$AQUI/../../pipeline/data/raw}"
python3 -I - "$RAW" <<'PY'                      # eventos de CRM: JSON (micro-lotes) -> CSV para o \copy
import sys, glob, pandas as pd
raw = sys.argv[1]
pd.concat([pd.read_json(f, lines=True) for f in sorted(glob.glob(f"{raw}/crm_events/*.json"))]).to_csv(f"{raw}/crm_eventos.csv", index=False)
PY
psql -v ON_ERROR_STOP=1 -v etl_pwd="${ETL_PWD:?defina ETL_PWD}" -d postgres -f "$AQUI/00_admin_roles.sql"
psql -v ON_ERROR_STOP=1 -q -d dw_b2b -f "$AQUI/01_schema_dw.sql"
(cd "$RAW" && psql -v ON_ERROR_STOP=1 -q -d dw_b2b -f "$AQUI/02_carga_etl.sql")
psql -v ON_ERROR_STOP=1 -q -d dw_b2b -f "$AQUI/03_analytics.sql"
psql -v ON_ERROR_STOP=1 -d dw_b2b -f "$AQUI/04_operacao_dba.sql" > /dev/null
echo "DW pronto. Exemplo: psql -d dw_b2b -c 'SELECT * FROM mart.vw_cascata_preco'"
