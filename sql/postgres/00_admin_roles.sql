-- =====================================================================
-- 00_admin_roles.sql — Provisionamento: banco, extensões e papéis (RBAC)
-- Executar como superusuário, conectado ao banco "postgres":
--   psql -v etl_pwd='<senha>' -f 00_admin_roles.sql
-- Princípio: privilégio mínimo. Ninguém consome o DW com o usuário dono.
-- =====================================================================

-- Banco do DW (CREATE DATABASE não roda em bloco transacional: \gexec)
SELECT 'CREATE DATABASE dw_b2b ENCODING ''UTF8'' TEMPLATE template0'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dw_b2b') \gexec

-- Papéis de grupo (sem login) — permissões vivem nos grupos, não nas pessoas
DO $$
DECLARE r text;
BEGIN
  FOREACH r IN ARRAY ARRAY['role_dba', 'role_etl', 'role_analista', 'role_gestor_canal'] LOOP
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = r) THEN
      EXECUTE format('CREATE ROLE %I NOLOGIN', r);
    END IF;
  END LOOP;
END $$;

-- Conta de serviço do ETL (login) — senha vem de variável, nunca versionada
SELECT format('CREATE ROLE svc_etl LOGIN PASSWORD %L IN ROLE role_etl CONNECTION LIMIT 5', :'etl_pwd')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'svc_etl') \gexec

-- Endurecimento: ninguém herda acesso por PUBLIC
REVOKE ALL ON DATABASE dw_b2b FROM PUBLIC;
GRANT CONNECT ON DATABASE dw_b2b TO role_dba, role_etl, role_analista, role_gestor_canal;

-- Guarda-chuva de segurança para consultas descontroladas
ALTER ROLE svc_etl SET statement_timeout = '30min';
-- ATENÇÃO: ALTER ROLE ... SET vale para o papel que faz login e NÃO é herdado de papéis de grupo.
-- Para cada login de analista criado: ALTER ROLE <login> SET statement_timeout = '2min';
