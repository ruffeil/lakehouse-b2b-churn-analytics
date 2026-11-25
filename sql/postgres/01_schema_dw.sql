-- =====================================================================
-- 01_schema_dw.sql — Modelo dimensional (star schema) + governança
-- Banco: dw_b2b | Executar como dono do DW. Idempotente (IF NOT EXISTS).
--
-- Camadas:
--   stg   : staging (texto bruto, espelha a Bronze do lakehouse)
--   core  : dimensões e fatos tipados, com constraints (espelha a Silver)
--   mart  : visões/materializações de negócio (espelha a Gold)
--   audit : trilha de carga, resultados de Data Quality e log de alterações
-- =====================================================================
CREATE EXTENSION IF NOT EXISTS btree_gist;   -- exclusão de sobreposição em SCD2

CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS audit;

-- ---------- Domínios: regra de negócio declarada uma única vez ----------
DO $$ BEGIN
  CREATE DOMAIN core.dom_pct AS numeric(5,4) CHECK (VALUE BETWEEN 0 AND 0.6);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ---------- Funções de conversão segura (DQ sem derrubar a carga) ----------
CREATE OR REPLACE FUNCTION core.try_date(t text) RETURNS date
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
  IF t !~ '^\d{4}-\d{2}-\d{2}$' THEN RETURN NULL; END IF;
  RETURN t::date;
EXCEPTION WHEN others THEN RETURN NULL;       -- ex.: 2025-02-31
END $$;

CREATE OR REPLACE FUNCTION core.try_numeric(t text) RETURNS numeric
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE WHEN t ~ '^-?\d+(\.\d+)?$' THEN t::numeric END
$$;

-- =====================================================================
-- STAGING — tudo texto; nada é rejeitado na entrada (política de Bronze)
-- =====================================================================
CREATE TABLE IF NOT EXISTS stg.pedido_raw (
  ingest_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  pedido_id    text, cliente_id text, canal text, data_pedido text,
  valor_bruto  text, desconto_pct text, margem_rs text,
  ingest_ts    timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS stg.cliente_raw (
  cliente_id text, canal text, porte text, uf text,
  ticket_base text, freq_base text, desc_base text
);
CREATE TABLE IF NOT EXISTS stg.crm_evento_raw (
  cliente_id text, tipo text, data_evento text
);

-- =====================================================================
-- AUDITORIA E DATA QUALITY
-- =====================================================================
CREATE TABLE IF NOT EXISTS audit.carga_log (
  carga_id    int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  fonte       text NOT NULL,
  inicio      timestamptz NOT NULL DEFAULT now(),
  fim         timestamptz,
  linhas_lidas bigint, linhas_validas bigint, linhas_quarentena bigint, duplicados bigint
);
CREATE TABLE IF NOT EXISTS audit.dq_resultado (
  carga_id  int REFERENCES audit.carga_log,
  regra     text NOT NULL,
  violacoes bigint NOT NULL,
  PRIMARY KEY (carga_id, regra)
);
CREATE TABLE IF NOT EXISTS audit.log_alteracao (
  id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tabela   text NOT NULL, operacao text NOT NULL,
  usuario  text NOT NULL DEFAULT current_user, ts timestamptz NOT NULL DEFAULT now(),
  antes    jsonb, depois jsonb
);
CREATE OR REPLACE FUNCTION audit.fn_log() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO audit.log_alteracao(tabela, operacao, antes, depois)
  VALUES (TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, TG_OP,
          CASE WHEN TG_OP IN ('UPDATE','DELETE') THEN to_jsonb(OLD) END,
          CASE WHEN TG_OP = 'UPDATE' THEN to_jsonb(NEW) END);
  RETURN NULL;
END $$;

-- Quarentena: o registro rejeitado é preservado com os motivos (reprocessável)
CREATE TABLE IF NOT EXISTS core.pedido_quarentena (
  ingest_id bigint PRIMARY KEY, carga_id int REFERENCES audit.carga_log,
  motivos   text[] NOT NULL, registro jsonb NOT NULL, ts timestamptz NOT NULL DEFAULT now()
);

-- =====================================================================
-- DIMENSÕES
-- =====================================================================
CREATE TABLE IF NOT EXISTS core.dim_canal (
  canal_sk     smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nome         text NOT NULL UNIQUE,
  margem_alvo  numeric(5,4) CHECK (margem_alvo BETWEEN 0 AND 1)
);

-- SCD Tipo 2: histórico de atributos do cliente. A chave de negócio é cliente_id;
-- a chave substituta (cliente_sk) identifica a VERSÃO. O EXCLUDE impede, no banco,
-- duas versões vigentes ao mesmo tempo para o mesmo cliente.
CREATE TABLE IF NOT EXISTS core.dim_cliente (
  cliente_sk     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  cliente_id     text NOT NULL,
  canal_sk       smallint NOT NULL REFERENCES core.dim_canal,
  porte          text NOT NULL CHECK (porte IN ('Médio','Grande')),
  uf             char(2) NOT NULL,
  vigencia_ini   date NOT NULL,
  vigencia_fim   date NOT NULL DEFAULT DATE '9999-12-31',
  hash_atributos text NOT NULL,
  is_current     boolean GENERATED ALWAYS AS (vigencia_fim = DATE '9999-12-31') STORED,
  CHECK (vigencia_fim >= vigencia_ini),
  EXCLUDE USING gist (cliente_id WITH =, daterange(vigencia_ini, vigencia_fim) WITH &&)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_cliente_atual ON core.dim_cliente (cliente_id) WHERE is_current;
DROP TRIGGER IF EXISTS trg_audit_dim_cliente ON core.dim_cliente;
CREATE TRIGGER trg_audit_dim_cliente AFTER UPDATE OR DELETE ON core.dim_cliente
  FOR EACH ROW EXECUTE FUNCTION audit.fn_log();

-- Dimensão tempo gerada por série (sem dependência de ETL)
CREATE TABLE IF NOT EXISTS core.dim_tempo (
  data date PRIMARY KEY, ano_mes text NOT NULL, mes_inicio date NOT NULL,
  trimestre int NOT NULL, dia_semana int NOT NULL, fim_de_semana boolean NOT NULL
);
INSERT INTO core.dim_tempo
SELECT d::date, to_char(d, 'YYYY-MM'), date_trunc('month', d)::date,
       extract(quarter FROM d)::int, extract(isodow FROM d)::int, (extract(isodow FROM d) IN (6,7))
FROM generate_series(DATE '2024-01-01', DATE '2026-12-31', interval '1 day') d
ON CONFLICT (data) DO NOTHING;

-- =====================================================================
-- FATOS — particionados por mês (poda de partições + manutenção barata)
-- =====================================================================
CREATE TABLE IF NOT EXISTS core.fato_pedido (
  pedido_id      text NOT NULL,
  data_pedido    date NOT NULL,
  cliente_sk     bigint NOT NULL REFERENCES core.dim_cliente,
  canal_sk       smallint NOT NULL REFERENCES core.dim_canal,
  valor_bruto    numeric(14,2) NOT NULL CHECK (valor_bruto > 0),
  desconto_pct   core.dom_pct NOT NULL,
  valor_liquido  numeric(14,2) GENERATED ALWAYS AS (round(valor_bruto * (1 - desconto_pct), 2)) STORED,
  margem_rs      numeric(14,2) NOT NULL,
  carga_id       int NOT NULL REFERENCES audit.carga_log,
  PRIMARY KEY (pedido_id, data_pedido)          -- a chave de partição precisa estar na PK
) PARTITION BY RANGE (data_pedido);

CREATE TABLE IF NOT EXISTS core.fato_evento_crm (
  evento_id    bigint GENERATED ALWAYS AS IDENTITY,
  cliente_id   text NOT NULL,
  tipo         text NOT NULL CHECK (tipo IN ('chamado_suporte','reclamacao','cotacao','visita')),
  data_evento  date NOT NULL,
  PRIMARY KEY (evento_id, data_evento)
) PARTITION BY RANGE (data_evento);

-- Índices na tabela-mãe se propagam para as partições
CREATE INDEX IF NOT EXISTS ix_fp_cliente_data ON core.fato_pedido (cliente_sk, data_pedido) INCLUDE (valor_liquido, margem_rs);
CREATE INDEX IF NOT EXISTS ix_fp_data_brin    ON core.fato_pedido USING brin (data_pedido) WITH (pages_per_range = 32);
CREATE INDEX IF NOT EXISTS ix_fp_canal_data   ON core.fato_pedido (canal_sk, data_pedido);
CREATE INDEX IF NOT EXISTS ix_fe_cliente_data ON core.fato_evento_crm (cliente_id, data_evento);

-- Criação (e manutenção futura) de partições mensais
CREATE OR REPLACE PROCEDURE core.criar_particoes(p_tabela regclass, p_ini date, p_fim date)
LANGUAGE plpgsql AS $$
DECLARE d date := date_trunc('month', p_ini)::date; base text;
BEGIN
  SELECT relname INTO base FROM pg_class WHERE oid = p_tabela;
  WHILE d < p_fim LOOP
    EXECUTE format('CREATE TABLE IF NOT EXISTS core.%I PARTITION OF %s FOR VALUES FROM (%L) TO (%L)',
                   base || '_' || to_char(d, 'YYYYMM'), p_tabela, d, (d + interval '1 month')::date);
    d := (d + interval '1 month')::date;
  END LOOP;
END $$;

CALL core.criar_particoes('core.fato_pedido',     DATE '2024-01-01', DATE '2026-01-01');
CALL core.criar_particoes('core.fato_evento_crm', DATE '2024-01-01', DATE '2026-01-01');
CREATE TABLE IF NOT EXISTS core.fato_pedido_default     PARTITION OF core.fato_pedido     DEFAULT;
CREATE TABLE IF NOT EXISTS core.fato_evento_crm_default PARTITION OF core.fato_evento_crm DEFAULT;

-- Fato de pedido: autovacuum mais agressivo (tabela de alta escrita)
ALTER TABLE core.fato_pedido_default SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.02);

-- =====================================================================
-- SEGURANÇA — grants por papel + Row-Level Security por canal
-- =====================================================================
GRANT USAGE ON SCHEMA stg, core, mart, audit TO role_etl;
GRANT USAGE ON SCHEMA core, mart TO role_analista, role_gestor_canal;
GRANT USAGE ON SCHEMA audit TO role_dba;
GRANT ALL ON ALL TABLES IN SCHEMA stg TO role_etl;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA core TO role_etl;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA audit TO role_etl;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO role_analista, role_gestor_canal;
GRANT SELECT ON ALL TABLES IN SCHEMA audit TO role_dba;
ALTER DEFAULT PRIVILEGES IN SCHEMA mart GRANT SELECT ON TABLES TO role_analista, role_gestor_canal;

ALTER TABLE core.fato_pedido ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS pol_total ON core.fato_pedido;
DROP POLICY IF EXISTS pol_gestor_canal ON core.fato_pedido;
CREATE POLICY pol_total ON core.fato_pedido FOR ALL TO role_etl, role_analista, role_dba USING (true) WITH CHECK (true);
-- gestor de canal só enxerga o próprio canal: SET app.canal_sk = '1' na sessão (ou via função de login)
CREATE POLICY pol_gestor_canal ON core.fato_pedido FOR SELECT TO role_gestor_canal
  USING (canal_sk = NULLIF(current_setting('app.canal_sk', true), '')::smallint);

COMMENT ON TABLE core.dim_cliente IS 'SCD2 do cliente; is_current marca a versão vigente; EXCLUDE impede sobreposição.';
COMMENT ON TABLE core.fato_pedido IS 'Fato de pedidos, particionada por mês; valor_liquido é coluna gerada.';
COMMENT ON TABLE core.pedido_quarentena IS 'Pedidos rejeitados pelo Data Quality, com motivos e registro original em JSON.';
