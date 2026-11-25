-- =====================================================================
-- 02_carga_etl.sql — Carga ELT: staging -> core, com Data Quality e SCD2
-- Uso:  (cd /caminho/data/raw && psql -d dw_b2b -f /caminho/02_carga_etl.sql)
--       \copy lê os CSVs do diretório corrente do psql (erp_pedidos.csv, erp_clientes.csv, crm_eventos.csv).
-- Premissas: 01_schema_dw.sql aplicado. Reexecutável (idempotente por chave de negócio).
-- =====================================================================
\set ON_ERROR_STOP on
-- vigencia: início de vigência da 1ª versão de cada cliente (use -v vigencia=AAAA-MM-DD)
\if :{?vigencia}
\else
  \set vigencia 2024-01-01
\endif

-- ---------- 1) Extract/Load: arquivos -> staging (texto bruto) ----------
TRUNCATE stg.pedido_raw, stg.cliente_raw, stg.crm_evento_raw;
\copy stg.pedido_raw (pedido_id,cliente_id,canal,data_pedido,valor_bruto,desconto_pct,margem_rs) FROM 'erp_pedidos.csv' WITH (FORMAT csv, HEADER true)
\copy stg.cliente_raw FROM 'erp_clientes.csv' WITH (FORMAT csv, HEADER true)
\copy stg.crm_evento_raw FROM 'crm_eventos.csv' WITH (FORMAT csv, HEADER true)

BEGIN;

INSERT INTO audit.carga_log (fonte, linhas_lidas)
VALUES ('erp_pedidos', (SELECT count(*) FROM stg.pedido_raw))
RETURNING carga_id \gset

-- ---------- 2) Dimensão canal (upsert) ----------
INSERT INTO core.dim_canal (nome, margem_alvo)
SELECT DISTINCT canal,
       CASE canal WHEN 'Representante' THEN 0.30 WHEN 'Distribuidor' THEN 0.26 ELSE 0.34 END
FROM stg.cliente_raw
ON CONFLICT (nome) DO NOTHING;

-- ---------- 3) Dimensão cliente — SCD Tipo 2 ----------
-- (a) fecha a versão vigente quando algum atributo mudou; (b) insere a nova versão.
-- Um único comando com CTE: o INSERT enxerga o snapshot anterior ao UPDATE, então
-- "mudou" = existe vigente com hash diferente; "novo" = não existe vigente.
WITH novo AS (
  SELECT s.cliente_id, c.canal_sk, s.porte, s.uf,
         md5(concat_ws('|', c.canal_sk, s.porte, s.uf)) AS h
  FROM stg.cliente_raw s JOIN core.dim_canal c ON c.nome = s.canal
),
fecha AS (
  UPDATE core.dim_cliente d SET vigencia_fim = current_date
  FROM novo n
  WHERE d.cliente_id = n.cliente_id AND d.is_current AND d.hash_atributos <> n.h
  RETURNING d.cliente_id
)
INSERT INTO core.dim_cliente (cliente_id, canal_sk, porte, uf, vigencia_ini, hash_atributos)
SELECT n.cliente_id, n.canal_sk, n.porte, n.uf,
       CASE WHEN EXISTS (SELECT 1 FROM core.dim_cliente x WHERE x.cliente_id = n.cliente_id)
            THEN current_date ELSE :'vigencia'::date END,
       n.h
FROM novo n
WHERE NOT EXISTS (SELECT 1 FROM core.dim_cliente d
                  WHERE d.cliente_id = n.cliente_id AND d.is_current AND d.hash_atributos = n.h);

-- ---------- 4) Pedidos: dedupe + Data Quality + quarentena ----------
CREATE TEMP TABLE t_ped ON COMMIT DROP AS
WITH unico AS (            -- mantém a ingestão mais recente de cada pedido_id
  SELECT DISTINCT ON (pedido_id) *
  FROM stg.pedido_raw
  ORDER BY pedido_id, ingest_id DESC
),
tipado AS (
  SELECT u.ingest_id, u.pedido_id, u.cliente_id,
         core.try_date(u.data_pedido)        AS dt,
         core.try_numeric(u.valor_bruto)     AS valor_bruto,
         core.try_numeric(u.desconto_pct)    AS desconto_pct,
         core.try_numeric(u.margem_rs)       AS margem_rs,
         u.canal, to_jsonb(u) - 'ingest_ts'  AS registro
  FROM unico u
)
SELECT t.*, cl.cliente_sk, cl.canal_sk,
       array_remove(ARRAY[
         CASE WHEN t.cliente_id IS NULL                                   THEN 'chave_cliente_nula' END,
         CASE WHEN t.dt IS NULL                                           THEN 'data_invalida' END,
         CASE WHEN t.valor_bruto IS NULL OR t.valor_bruto <= 0            THEN 'valor_nao_positivo' END,
         CASE WHEN t.desconto_pct IS NULL OR t.desconto_pct NOT BETWEEN 0 AND 0.6 THEN 'desconto_fora_faixa' END,
         CASE WHEN t.cliente_id IS NOT NULL AND t.dt IS NOT NULL AND cl.cliente_sk IS NULL
                                                                          THEN 'cliente_inexistente_na_data' END
       ], NULL) AS motivos
FROM tipado t
LEFT JOIN core.dim_cliente cl                       -- junção temporal com a versão vigente na data do pedido
       ON cl.cliente_id = t.cliente_id AND t.dt >= cl.vigencia_ini AND t.dt < cl.vigencia_fim;

INSERT INTO core.pedido_quarentena (ingest_id, carga_id, motivos, registro)
SELECT ingest_id, :carga_id, motivos, registro FROM t_ped WHERE cardinality(motivos) > 0
ON CONFLICT (ingest_id) DO NOTHING;

INSERT INTO core.fato_pedido (pedido_id, data_pedido, cliente_sk, canal_sk, valor_bruto, desconto_pct, margem_rs, carga_id)
SELECT pedido_id, dt, cliente_sk, canal_sk, valor_bruto, desconto_pct, margem_rs, :carga_id
FROM t_ped WHERE cardinality(motivos) = 0
ON CONFLICT (pedido_id, data_pedido) DO NOTHING;

-- violações por regra (um registro pode violar mais de uma)
INSERT INTO audit.dq_resultado (carga_id, regra, violacoes)
SELECT :carga_id, m, count(*) FROM t_ped, unnest(motivos) AS m GROUP BY m;

-- ---------- 5) Eventos de CRM ----------
INSERT INTO core.fato_evento_crm (cliente_id, tipo, data_evento)
SELECT DISTINCT cliente_id, tipo, core.try_date(data_evento)
FROM stg.crm_evento_raw
WHERE cliente_id IS NOT NULL AND core.try_date(data_evento) IS NOT NULL
  AND tipo IN ('chamado_suporte','reclamacao','cotacao','visita');

-- ---------- 6) Fechamento da carga ----------
UPDATE audit.carga_log l SET fim = now(),
  linhas_validas    = (SELECT count(*) FROM t_ped WHERE cardinality(motivos) = 0),
  linhas_quarentena = (SELECT count(*) FROM t_ped WHERE cardinality(motivos) > 0),
  duplicados        = l.linhas_lidas - (SELECT count(*) FROM t_ped)
WHERE carga_id = :carga_id;
COMMIT;

ANALYZE core.fato_pedido; ANALYZE core.fato_evento_crm; ANALYZE core.dim_cliente;

-- Relatório de DQ da carga (equivalente ao dq_relatorio.json do pipeline Spark)
SELECT carga_id, linhas_lidas, duplicados, linhas_quarentena, linhas_validas,
       round(100.0 * linhas_validas / linhas_lidas, 2) AS taxa_aprovacao_pct
FROM audit.carga_log WHERE carga_id = :carga_id;
SELECT regra, violacoes FROM audit.dq_resultado WHERE carga_id = :carga_id ORDER BY violacoes DESC;
