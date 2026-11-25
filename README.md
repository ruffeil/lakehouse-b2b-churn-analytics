# Lakehouse e Inteligência Preditiva para Margem e Churn em Vendas B2B Multicanal

Projeto de conclusão do módulo 3 do MBA em Big Data e Inteligência Competitiva (Faculdade Descomplica).
Autor: Fernando Ruffeil.

Protótipo executável de ponta a ponta: ingestão batch e streaming, Lakehouse em camadas (Bronze, Silver, Gold),
Data Quality, banco relacional dimensional, SQL analítico avançado, modelo de churn e dashboard.
**Os dados são sintéticos**: o objetivo é demonstrar arquitetura e método, não medir uma empresa real.

## Estrutura
```
pipeline/            Python + Spark (dados sintéticos, Medallion, modelo, painéis, Spark SQL)
sql/postgres/        Data warehouse relacional: papéis, modelo dimensional, carga ELT, analytics, runbook de DBA
sql/spark/           Spark SQL sobre o data lake (+ desenho Delta Lake / Iceberg, não executado)
docs/ARQUITETURA.md  Diagramas, decisões de projeto e reconciliação lake x banco
```

## O que cada parte demonstra
| Área | Onde | Técnicas |
|---|---|---|
| Projeto de banco | `sql/postgres/01_schema_dw.sql` | Star schema, SCD2 com `EXCLUDE USING gist`, fato particionada por mês, colunas geradas, domínios, BRIN e B-tree com `INCLUDE` |
| Administração | `00_admin_roles.sql`, `04_operacao_dba.sql` | RBAC por papéis, privilégio mínimo, Row-Level Security por canal, trilha de auditoria por trigger, monitoramento, ciclo de vida de partições, backup/PITR |
| Integração e qualidade | `02_carga_etl.sql`, `pipeline/02_pipeline_spark.py` | ELT com quarentena e motivos, dedupe, junção temporal com SCD2, idempotência, relatório de DQ |
| SQL analítico | `03_analytics.sql` | Função tabular de features sem vazamento, RFM (NTILE), coorte de retenção, curva ABC, ROLLUP, z-score com LATERAL, gaps-and-islands com CTE recursiva, materialized view com refresh concorrente |
| Big Data / Data Lake | `sql/spark/lakehouse_spark.sql`, `pipeline/` | Parquet particionado, tabelas externas, PIVOT, funções de ordem superior, `sequence` + `explode`, percentis aproximados, Structured Streaming |
| Analytics preditivo | `pipeline/03_modelo_churn.py` | Classificação de churn, LTV e margem em risco, segmentação, simulação de retenção |

## Como rodar
Requisitos: Python 3.11+, Java 17+ (PySpark), PostgreSQL 15+.
```bash
pip install -r pipeline/requirements.txt
cd pipeline && ./run_all.sh                 # dados -> Spark -> modelo -> painéis -> Spark SQL

export PGHOST=localhost PGUSER=postgres ETL_PWD='troque-esta-senha'
sql/postgres/run_pg.sh                      # DW relacional completo (00 a 04)
```

## Resultados da execução de referência (dados sintéticos)
- Data Quality: 51.366 registros brutos, 609 duplicados, 1.154 em quarentena (Spark), 96,6% aprovados.
- Modelo de churn: AUC 0,77 e lift de 2,9× no decil de maior risco. Uma regressão logística de base fica praticamente empatada, então o modelo simples é defensável.
- Simulação de retenção: ROI de ~10× sobre o custo da ação, **com premissas explícitas** (abordar 10% da carteira, 25% de salvamento, R$ 400 por conta). O ROI real depende de um piloto.
- Reconciliação lake x banco: ver `docs/ARQUITETURA.md`.

## Limites assumidos
- O gerador injeta os padrões de churn (queda de frequência, desconto maior, mais chamados). O AUC mostra que o pipeline captura o sinal, não que o modelo funciona em outra empresa.
- Não há Kafka rodando: o streaming usa o mesmo código de Structured Streaming sobre micro-lotes JSON.
- `sql/spark/lakehouse_delta_iceberg.sql` é o desenho-alvo de produção e não foi executado aqui (exige Delta Lake ou Iceberg).
