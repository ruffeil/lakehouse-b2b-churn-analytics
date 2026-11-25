-- =====================================================================
-- 03_analytics.sql — Camada de negócio (mart): SQL analítico avançado
-- Técnicas: CTEs, funções de janela, FILTER, NTILE, LAG, percentile_cont,
-- GROUPING SETS/ROLLUP, LATERAL, gaps-and-islands, CTE recursiva,
-- função tabular parametrizada e materialized view com refresh concorrente.
-- =====================================================================

-- ---------------------------------------------------------------------
-- A) Feature store em SQL: mesma lógica da Gold do Spark, parametrizada
--    por data de corte (sem vazamento: só dados <= corte; rótulo no futuro)
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mart.fn_features_churn(p_corte date, p_fim_rotulo date)
RETURNS TABLE (
  cliente_id text, canal text, recencia_dias int, pedidos_total int, pedidos_3m int, pedidos_3m_ant int,
  tendencia_pedidos int, receita_12m numeric, margem_12m numeric, desconto_medio numeric, desconto_3m numeric,
  chamados_3m int, reclamacoes_3m int, cotacoes_3m int, churn int)
LANGUAGE sql STABLE AS $$
WITH hist AS (
  SELECT cl.cliente_id, ca.nome AS canal, p.data_pedido, p.valor_liquido, p.margem_rs, p.desconto_pct
  FROM core.fato_pedido p
  JOIN core.dim_cliente cl USING (cliente_sk)
  JOIN core.dim_canal  ca ON ca.canal_sk = p.canal_sk
  WHERE p.data_pedido <= p_corte
),
agg AS (
  SELECT cliente_id, min(canal) AS canal,
         (p_corte - max(data_pedido))                                                        AS recencia_dias,
         count(*)                                                                            AS pedidos_total,
         count(*) FILTER (WHERE data_pedido >  p_corte - interval '3 months')                AS pedidos_3m,
         count(*) FILTER (WHERE data_pedido >  p_corte - interval '6 months'
                            AND data_pedido <= p_corte - interval '3 months')                AS pedidos_3m_ant,
         coalesce(sum(valor_liquido) FILTER (WHERE data_pedido > p_corte - interval '12 months'), 0) AS receita_12m,
         coalesce(sum(margem_rs)     FILTER (WHERE data_pedido > p_corte - interval '12 months'), 0) AS margem_12m,
         avg(desconto_pct)                                                                   AS desconto_medio,
         coalesce(avg(desconto_pct) FILTER (WHERE data_pedido > p_corte - interval '3 months'), 0) AS desconto_3m
  FROM hist GROUP BY cliente_id
),
crm AS (
  SELECT cliente_id,
         count(*) FILTER (WHERE tipo = 'chamado_suporte') AS chamados_3m,
         count(*) FILTER (WHERE tipo = 'reclamacao')      AS reclamacoes_3m,
         count(*) FILTER (WHERE tipo = 'cotacao')         AS cotacoes_3m
  FROM core.fato_evento_crm
  WHERE data_evento <= p_corte AND data_evento > p_corte - interval '3 months'
  GROUP BY cliente_id
),
futuro AS (
  SELECT cl.cliente_id, count(*) AS n
  FROM core.fato_pedido p JOIN core.dim_cliente cl USING (cliente_sk)
  WHERE p.data_pedido > p_corte AND p.data_pedido <= p_fim_rotulo
  GROUP BY cl.cliente_id
)
SELECT a.cliente_id, a.canal, a.recencia_dias::int, a.pedidos_total::int, a.pedidos_3m::int, a.pedidos_3m_ant::int,
       (a.pedidos_3m - a.pedidos_3m_ant)::int, a.receita_12m, a.margem_12m, round(a.desconto_medio, 4), round(a.desconto_3m, 4),
       coalesce(c.chamados_3m, 0)::int, coalesce(c.reclamacoes_3m, 0)::int, coalesce(c.cotacoes_3m, 0)::int,
       (f.n IS NULL)::int AS churn
FROM agg a
LEFT JOIN crm c USING (cliente_id)
LEFT JOIN futuro f USING (cliente_id)
WHERE a.recencia_dias <= 183;      -- ativo nos últimos 6 meses até o corte
$$;

-- ---------------------------------------------------------------------
-- B) RFM com quintis (NTILE) e segmentação de ação comercial
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.vw_rfm AS
WITH base AS (
  SELECT cliente_id, recencia_dias, pedidos_total AS frequencia, receita_12m AS monetario
  FROM mart.fn_features_churn(DATE '2025-09-30', DATE '2025-12-31')
),
score AS (
  SELECT *,
         NTILE(5) OVER (ORDER BY recencia_dias DESC) AS r,     -- menor recência = nota maior
         NTILE(5) OVER (ORDER BY frequencia)         AS f,
         NTILE(5) OVER (ORDER BY monetario)          AS m
  FROM base
)
SELECT *, (r * 100 + f * 10 + m) AS rfm,
       CASE WHEN r >= 4 AND f >= 4 AND m >= 4 THEN 'Campeões'
            WHEN r <= 2 AND m >= 4             THEN 'Alto valor adormecido'
            WHEN r <= 2                        THEN 'Em risco'
            WHEN f >= 4                        THEN 'Fiéis'
            ELSE 'Regulares' END AS segmento_rfm
FROM score;

-- ---------------------------------------------------------------------
-- C) Coorte de retenção: % de clientes da coorte que compram no mês N
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.vw_coorte_retencao AS
WITH primeira AS (
  SELECT cl.cliente_id, date_trunc('month', min(p.data_pedido))::date AS coorte
  FROM core.fato_pedido p
  JOIN core.dim_cliente cl USING (cliente_sk)
  GROUP BY cl.cliente_id
),
atividade AS (
  SELECT DISTINCT cl.cliente_id, date_trunc('month', p.data_pedido)::date AS mes
  FROM core.fato_pedido p JOIN core.dim_cliente cl USING (cliente_sk)
),
matriz AS (
  SELECT pr.coorte,
         ((extract(year FROM a.mes) - extract(year FROM pr.coorte)) * 12
          + extract(month FROM a.mes) - extract(month FROM pr.coorte))::int AS mes_n,
         count(DISTINCT a.cliente_id) AS ativos
  FROM primeira pr JOIN atividade a USING (cliente_id)
  GROUP BY 1, 2
)
SELECT coorte, mes_n, ativos,
       first_value(ativos) OVER (PARTITION BY coorte ORDER BY mes_n) AS tamanho_coorte,
       round(100.0 * ativos / first_value(ativos) OVER (PARTITION BY coorte ORDER BY mes_n), 1) AS retencao_pct
FROM matriz;

-- ---------------------------------------------------------------------
-- D) Curva ABC (Pareto) de margem: acumulado com janela
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.vw_pareto_abc AS
WITH m AS (
  SELECT cl.cliente_id, sum(p.margem_rs) AS margem
  FROM core.fato_pedido p JOIN core.dim_cliente cl USING (cliente_sk)
  WHERE p.data_pedido > DATE '2024-12-31' AND p.data_pedido <= DATE '2025-12-31'
  GROUP BY cl.cliente_id
),
acum AS (
  SELECT cliente_id, margem,
         sum(margem) OVER (ORDER BY margem DESC, cliente_id ROWS UNBOUNDED PRECEDING) / sum(margem) OVER () AS pct_acum
  FROM m
)
SELECT *, CASE WHEN pct_acum <= 0.80 THEN 'A' WHEN pct_acum <= 0.95 THEN 'B' ELSE 'C' END AS classe
FROM acum;

-- ---------------------------------------------------------------------
-- E) Cascata de preço (bruto -> desconto -> líquido -> margem) com ROLLUP
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.vw_cascata_preco AS
SELECT coalesce(to_char(p.data_pedido, 'YYYY'), 'TOTAL') AS ano,
       coalesce(ca.nome, 'Todos os canais')              AS canal,
       sum(p.valor_bruto)                                AS receita_bruta,
       sum(p.valor_bruto - p.valor_liquido)              AS desconto_concedido,
       sum(p.valor_liquido)                              AS receita_liquida,
       sum(p.margem_rs)                                  AS margem,
       round(100 * sum(p.valor_bruto - p.valor_liquido) / sum(p.valor_bruto), 2) AS desconto_pct,
       round(100 * sum(p.margem_rs) / sum(p.valor_liquido), 2)                  AS margem_pct,
       GROUPING(to_char(p.data_pedido, 'YYYY'), ca.nome) AS nivel_agregacao
FROM core.fato_pedido p JOIN core.dim_canal ca USING (canal_sk)
GROUP BY ROLLUP (to_char(p.data_pedido, 'YYYY'), ca.nome);

-- ---------------------------------------------------------------------
-- F) Detector de escalada de desconto (sinal precoce de churn):
--    z-score do desconto dos últimos 3 meses contra o histórico do próprio cliente
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.vw_alerta_desconto AS
WITH ped AS (
  SELECT cl.cliente_id, p.data_pedido, p.desconto_pct
  FROM core.fato_pedido p JOIN core.dim_cliente cl USING (cliente_sk)
  WHERE p.data_pedido <= DATE '2025-09-30'
),
hist AS (
  SELECT cliente_id,
         avg(desconto_pct)  FILTER (WHERE data_pedido <= DATE '2025-06-30') AS media_hist,
         stddev_samp(desconto_pct) FILTER (WHERE data_pedido <= DATE '2025-06-30') AS dp_hist,
         count(*) FILTER (WHERE data_pedido <= DATE '2025-06-30') AS n_hist
  FROM ped GROUP BY cliente_id
),
recente AS (
  SELECT cliente_id, avg(desconto_pct) AS media_3m, count(*) AS n_3m
  FROM ped WHERE data_pedido > DATE '2025-06-30' GROUP BY cliente_id
)
SELECT h.cliente_id, round(h.media_hist, 4) AS media_hist, round(r.media_3m, 4) AS media_3m,
       round(((r.media_3m - h.media_hist) / NULLIF(h.dp_hist, 0))::numeric, 2) AS z_score
FROM hist h JOIN recente r USING (cliente_id)
WHERE h.n_hist >= 5 AND r.n_3m >= 2
  AND (r.media_3m - h.media_hist) / NULLIF(h.dp_hist, 0) > 2;

-- ---------------------------------------------------------------------
-- G) Gaps-and-islands: sequências de meses SEM compra por cliente
--    (CTE recursiva gera o calendário; diferença de row_number forma as ilhas)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.vw_ilhas_inatividade AS
WITH RECURSIVE cal(mes) AS (
  SELECT DATE '2024-01-01'
  UNION ALL
  SELECT (mes + interval '1 month')::date FROM cal WHERE mes < DATE '2025-12-01'
),
cli AS (
  SELECT cl.cliente_id, min(date_trunc('month', p.data_pedido))::date AS primeiro, max(date_trunc('month', p.data_pedido))::date AS ultimo
  FROM core.fato_pedido p JOIN core.dim_cliente cl USING (cliente_sk) GROUP BY cl.cliente_id
),
ativo AS (
  SELECT DISTINCT cl.cliente_id, date_trunc('month', p.data_pedido)::date AS mes
  FROM core.fato_pedido p JOIN core.dim_cliente cl USING (cliente_sk)
),
grade AS (               -- todos os meses entre a 1ª e a última compra, marcando se houve pedido
  SELECT c.cliente_id, k.mes, (a.mes IS NOT NULL) AS comprou
  FROM cli c
  JOIN cal k ON k.mes BETWEEN c.primeiro AND c.ultimo
  LEFT JOIN ativo a ON a.cliente_id = c.cliente_id AND a.mes = k.mes
),
numerada AS (
  SELECT *, row_number() OVER (PARTITION BY cliente_id ORDER BY mes)
          - row_number() OVER (PARTITION BY cliente_id, comprou ORDER BY mes) AS grp
  FROM grade
)
SELECT cliente_id, min(mes) AS inicio_gap, max(mes) AS fim_gap, count(*) AS meses_sem_compra
FROM numerada WHERE NOT comprou
GROUP BY cliente_id, grp
HAVING count(*) >= 2;

-- ---------------------------------------------------------------------
-- H) Top-N por grupo com LATERAL: 3 maiores contas de cada canal por margem em risco
--    (consome a função de features; sem modelo, usa heurística de recência/tendência)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.vw_top_contas_em_risco AS
SELECT ca.nome AS canal, t.*
FROM core.dim_canal ca
CROSS JOIN LATERAL (
  SELECT f.cliente_id, f.margem_12m, f.recencia_dias, f.tendencia_pedidos,
         round(f.margem_12m * LEAST(1, f.recencia_dias / 90.0), 2) AS margem_exposta
  FROM mart.fn_features_churn(DATE '2025-09-30', DATE '2025-12-31') f
  WHERE f.canal = ca.nome AND f.tendencia_pedidos < 0
  ORDER BY margem_exposta DESC
  LIMIT 3
) t;

-- ---------------------------------------------------------------------
-- I) Materialized view (KPI executivo) + índice único => REFRESH CONCURRENTLY
-- ---------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mart.mv_margem_canal_mes;
CREATE MATERIALIZED VIEW mart.mv_margem_canal_mes AS
WITH mensal AS (
  SELECT date_trunc('month', p.data_pedido)::date AS mes, ca.nome AS canal,
         sum(p.valor_liquido) AS receita_liquida, sum(p.margem_rs) AS margem_rs,
         avg(p.desconto_pct)  AS desconto_medio, count(DISTINCT p.cliente_sk) AS compras_distintas
  FROM core.fato_pedido p JOIN core.dim_canal ca USING (canal_sk)
  GROUP BY 1, 2
)
SELECT mes, canal, receita_liquida, margem_rs, desconto_medio, compras_distintas,
       margem_rs - lag(margem_rs) OVER w                                   AS delta_margem_mom,
       avg(margem_rs) OVER (w ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)    AS margem_media_3m
FROM mensal
WINDOW w AS (PARTITION BY canal ORDER BY mes)
WITH DATA;
CREATE UNIQUE INDEX ux_mv_margem_canal_mes ON mart.mv_margem_canal_mes (mes, canal);
GRANT SELECT ON mart.mv_margem_canal_mes TO role_analista, role_gestor_canal;
-- Atualização sem bloquear leitores (agendar após cada carga):
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mv_margem_canal_mes;

-- ---------------------------------------------------------------------
-- J) Distribuição do ticket por canal (percentis) — insumo para política de desconto
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.vw_ticket_percentis AS
SELECT ca.nome AS canal,
       percentile_cont(ARRAY[0.25, 0.5, 0.75, 0.95]) WITHIN GROUP (ORDER BY p.valor_liquido) AS p25_p50_p75_p95,
       round(avg(p.valor_liquido), 2) AS media, count(*) AS pedidos
FROM core.fato_pedido p JOIN core.dim_canal ca USING (canal_sk)
GROUP BY ca.nome;
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO role_analista, role_gestor_canal;
