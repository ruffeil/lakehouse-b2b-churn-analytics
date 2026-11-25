-- =====================================================================
-- lakehouse_spark.sql — Spark SQL sobre o data lake (Parquet em camadas)
-- Executado por pipeline/05_spark_sql.py, que substitui {{DATA}} pelo diretório data/.
-- Técnicas: tabelas externas, CTAS particionado, estatísticas, PIVOT, funções de
-- ordem superior (transform/aggregate), LATERAL VIEW explode, sequence, janelas,
-- percentis aproximados e reconciliação com o banco relacional.
-- =====================================================================

CREATE DATABASE IF NOT EXISTS lakehouse;
USE lakehouse;

-- ---------- 1) Catálogo: tabelas EXTERNAS sobre os arquivos Parquet (dado fica no lake) ----------
DROP TABLE IF EXISTS silver_pedidos;
CREATE TABLE silver_pedidos USING PARQUET LOCATION '{{DATA}}/silver/pedidos';
DROP TABLE IF EXISTS silver_eventos_crm;
CREATE TABLE silver_eventos_crm USING PARQUET LOCATION '{{DATA}}/silver/crm_eventos';
DROP TABLE IF EXISTS gold_features_cliente;
CREATE TABLE gold_features_cliente USING PARQUET LOCATION '{{DATA}}/gold/features_cliente';

-- ---------- 2) Layout físico: CTAS particionado por mês (poda de arquivos na leitura) ----------
DROP TABLE IF EXISTS pedidos_mes;
CREATE TABLE pedidos_mes USING PARQUET PARTITIONED BY (ano_mes) AS
SELECT pedido_id, cliente_id, canal, data_pedido_dt AS data_pedido, valor_bruto, desconto_pct, margem_rs,
       date_format(data_pedido_dt, 'yyyy-MM') AS ano_mes
FROM silver_pedidos;

ANALYZE TABLE pedidos_mes COMPUTE STATISTICS FOR COLUMNS canal, valor_bruto, desconto_pct, margem_rs;
SHOW PARTITIONS pedidos_mes;

-- ---------- 3) PIVOT: margem mensal por canal em colunas (formato de dashboard) ----------
SELECT * FROM (
  SELECT ano_mes, canal, margem_rs FROM pedidos_mes WHERE ano_mes >= '2025-01'
) PIVOT (round(sum(margem_rs)) FOR canal IN ('Representante', 'Distribuidor', 'E-commerce B2B'))
ORDER BY ano_mes;

-- ---------- 4) Funções de ordem superior: tendência (inclinação) da margem por cliente ----------
-- Monta o array ordenado de margem mensal e calcula a inclinação por mínimos quadrados com aggregate().
WITH mensal AS (
  SELECT cliente_id, ano_mes, sum(margem_rs) AS margem
  FROM pedidos_mes WHERE data_pedido <= DATE '2025-09-30' AND ano_mes >= '2025-01'
  GROUP BY cliente_id, ano_mes
),
serie AS (
  SELECT cliente_id, array_sort(collect_list(struct(ano_mes, margem))) AS pontos
  FROM mensal GROUP BY cliente_id HAVING count(*) >= 5
),
xy AS (
  SELECT cliente_id,
         transform(pontos, (p, i) -> struct(cast(i AS double) AS x, p.margem AS y)) AS pares
  FROM serie
)
SELECT cliente_id,
       round((size(pares) * aggregate(pares, 0D, (a, p) -> a + p.x * p.y)
              - aggregate(pares, 0D, (a, p) -> a + p.x) * aggregate(pares, 0D, (a, p) -> a + p.y))
           / (size(pares) * aggregate(pares, 0D, (a, p) -> a + p.x * p.x)
              - pow(aggregate(pares, 0D, (a, p) -> a + p.x), 2)), 2) AS inclinacao_margem_mensal
FROM xy
ORDER BY inclinacao_margem_mensal ASC
LIMIT 10;

-- ---------- 5) Calendário com sequence() + LATERAL VIEW explode: meses sem compra por cliente ----------
WITH faixa AS (
  SELECT cliente_id, trunc(min(data_pedido), 'MM') AS ini, trunc(max(data_pedido), 'MM') AS fim
  FROM pedidos_mes GROUP BY cliente_id
),
grade AS (
  SELECT f.cliente_id, m.mes
  FROM faixa f LATERAL VIEW explode(sequence(f.ini, f.fim, interval 1 month)) m AS mes
),
compras AS (SELECT DISTINCT cliente_id, trunc(data_pedido, 'MM') AS mes FROM pedidos_mes)
SELECT count(*) AS meses_sem_compra,
       count(DISTINCT g.cliente_id) AS clientes_afetados
FROM grade g LEFT ANTI JOIN compras c ON c.cliente_id = g.cliente_id AND c.mes = g.mes;

-- ---------- 6) Janelas + percentis aproximados (escala): ticket por canal e ranking de desconto ----------
SELECT canal,
       percentile_approx(valor_bruto * (1 - desconto_pct), array(0.5, 0.95), 1000) AS p50_p95_liquido,
       approx_count_distinct(cliente_id) AS clientes_aprox,
       round(avg(desconto_pct), 4) AS desconto_medio
FROM pedidos_mes GROUP BY canal ORDER BY canal;

SELECT canal, ano_mes, round(sum(margem_rs)) AS margem,
       round(sum(margem_rs) - lag(sum(margem_rs)) OVER (PARTITION BY canal ORDER BY ano_mes)) AS delta_mom,
       round(avg(sum(margem_rs)) OVER (PARTITION BY canal ORDER BY ano_mes ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)) AS media_3m
FROM pedidos_mes WHERE ano_mes >= '2025-07'
GROUP BY canal, ano_mes ORDER BY canal, ano_mes;

-- ---------- 7) Reconciliação lake x relacional (comparar com audit.carga_log no PostgreSQL) ----------
SELECT 'silver_pedidos' AS camada, count(*) AS linhas, count(DISTINCT pedido_id) AS pedidos_unicos FROM silver_pedidos
UNION ALL
SELECT 'gold_features_cliente', count(*), count(DISTINCT cliente_id) FROM gold_features_cliente;

-- Taxa de churn por canal na Gold (mesmo rótulo do mart.fn_features_churn do PostgreSQL)
SELECT canal, count(*) AS clientes, round(avg(churn), 3) AS taxa_churn
FROM gold_features_cliente GROUP BY canal ORDER BY canal;
