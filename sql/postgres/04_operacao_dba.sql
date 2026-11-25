-- =====================================================================
-- 04_operacao_dba.sql — Administração e operação do banco (runbook em SQL)
-- Consultas de saúde, capacidade, manutenção e ciclo de vida das partições.
-- Executar como role_dba / superusuário.
-- =====================================================================

-- 1) Capacidade: tamanho por partição (dados + índices) e linhas estimadas
SELECT c.relname AS particao,
       pg_size_pretty(pg_total_relation_size(c.oid))  AS tamanho_total,
       pg_size_pretty(pg_relation_size(c.oid))        AS so_dados,
       c.reltuples::bigint                            AS linhas_estimadas
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_class p ON p.oid = i.inhparent
WHERE p.relname = 'fato_pedido'
ORDER BY pg_total_relation_size(c.oid) DESC
LIMIT 10;

-- 2) Eficiência de cache (alvo > 99% em carga analítica estável)
SELECT datname,
       round(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 2) AS cache_hit_pct,
       xact_commit, xact_rollback, temp_files, pg_size_pretty(temp_bytes) AS temp_usado
FROM pg_stat_database WHERE datname = current_database();

-- 3) Índices: uso real e candidatos a remoção (custo de escrita sem benefício de leitura)
SELECT s.schemaname, s.relname AS tabela, s.indexrelname AS indice,
       s.idx_scan AS usos, pg_size_pretty(pg_relation_size(s.indexrelid)) AS tamanho
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE s.schemaname = 'core' AND NOT i.indisunique AND s.idx_scan = 0
ORDER BY pg_relation_size(s.indexrelid) DESC
LIMIT 10;

-- 4) Bloat/manutenção: tuplas mortas e última vacuum/analyze
SELECT relname, n_live_tup, n_dead_tup,
       round(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS pct_morto,
       last_autovacuum, last_autoanalyze
FROM pg_stat_user_tables
WHERE schemaname IN ('core', 'stg') AND n_dead_tup > 0
ORDER BY n_dead_tup DESC
LIMIT 10;

-- 5) Sessões longas e bloqueios (diagnóstico de contenção)
SELECT pid, usename, state, now() - query_start AS duracao,
       wait_event_type, wait_event, left(query, 80) AS consulta
FROM pg_stat_activity
WHERE datname = current_database() AND state <> 'idle' AND pid <> pg_backend_pid()
ORDER BY query_start
LIMIT 10;

SELECT blocked.pid AS bloqueado, blocker.pid AS bloqueador, left(blocked.query, 60) AS consulta_bloqueada
FROM pg_stat_activity blocked
JOIN LATERAL unnest(pg_blocking_pids(blocked.pid)) AS b(pid) ON true
JOIN pg_stat_activity blocker ON blocker.pid = b.pid;

-- 6) Qualidade operacional: rastreio das cargas e auditoria de mudanças
SELECT carga_id, fonte, fim - inicio AS duracao, linhas_lidas, linhas_validas, linhas_quarentena, duplicados
FROM audit.carga_log ORDER BY carga_id DESC LIMIT 5;

SELECT motivos, count(*) AS registros
FROM core.pedido_quarentena GROUP BY motivos ORDER BY registros DESC LIMIT 10;

-- 7) Poda de partições: o plano deve tocar só 1 partição (confirmar com EXPLAIN)
EXPLAIN (COSTS OFF)
SELECT sum(valor_liquido) FROM core.fato_pedido
WHERE data_pedido >= DATE '2025-03-01' AND data_pedido < DATE '2025-04-01';

-- =====================================================================
-- CICLO DE VIDA (executar sob janela de manutenção — exemplos, não rodam sozinhos)
-- =====================================================================
-- Nova partição futura (agendar mensalmente):
--   CALL core.criar_particoes('core.fato_pedido', DATE '2026-01-01', DATE '2026-04-01');
--
-- Retenção: desanexar sem bloquear leituras longas e arquivar no data lake antes de descartar:
--   ALTER TABLE core.fato_pedido DETACH PARTITION core.fato_pedido_202401 CONCURRENTLY;
--   \copy core.fato_pedido_202401 TO 'fato_pedido_202401.csv' CSV HEADER   -- ou COPY ... TO PROGRAM (parquet via DuckDB/Spark)
--   DROP TABLE core.fato_pedido_202401;
--
-- Manutenção de índices e estatísticas:
--   REINDEX TABLE CONCURRENTLY core.fato_pedido_202501;
--   VACUUM (ANALYZE, VERBOSE) core.fato_pedido_202501;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mv_margem_canal_mes;
--
-- Backup e recuperação (fora do SQL, via shell):
--   Lógico (por schema):  pg_dump -Fc -n core -n mart -n audit dw_b2b > dw_b2b.dump
--   Físico + PITR:        pg_basebackup -D /backup/base -Fp -Xs -P  (com archive_mode=on e archive_command)
--   Restore testado:      pg_restore -d dw_restore --jobs 4 dw_b2b.dump   (validar contagens contra audit.carga_log)
--
-- Parâmetros de partida para carga analítica (ajustar ao hardware):
--   shared_buffers = 25% da RAM | effective_cache_size = 50-75% | work_mem = 64MB (por operação!)
--   max_parallel_workers_per_gather = 4 | maintenance_work_mem = 1GB | random_page_cost = 1.1 (SSD)
