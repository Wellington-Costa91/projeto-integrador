# Relatório de Infraestrutura — Pipeline de Ingestão NYC Taxi Data

**Projeto:** Análise de Dados de Transporte Urbano de Nova York
**Data:** 18/03/2026
**Conta AWS:** <AWS_ACCOUNT_ID> | **Região:** us-east-1

---

## Resumo

Foi provisionada via Terraform (IaC modularizada) a infraestrutura de ingestão de dados da NYC Taxi and Limousine Commission (TLC) na AWS, contemplando armazenamento, processamento e orquestração.

---

## Recursos Criados

### S3 — 5 Buckets (Arquitetura Medalhão)
| Bucket | Finalidade |
|--------|-----------|
| `nyc-taxi-pipeline-bronze-*` | Dados brutos (Parquet) ingeridos da fonte |
| `nyc-taxi-pipeline-silver-*` | Dados limpos e harmonizados (uso futuro) |
| `nyc-taxi-pipeline-gold-*` | Dados analíticos agregados (uso futuro) |
| `nyc-taxi-pipeline-scripts-*` | Scripts ETL do Glue |
| `nyc-taxi-pipeline-athena-results-*` | Resultados de queries Athena (uso futuro) |

Todos com versionamento, criptografia SSE-S3 e bloqueio de acesso público.

### AWS Glue — 4 Jobs de Ingestão
Cada job baixa arquivos Parquet do endpoint público da TLC e armazena no bucket Bronze particionado por `year=` e `month=`.

| Job | Dataset | Período | Fonte |
|-----|---------|---------|-------|
| `ingest-yellow` | Yellow Taxi | 2016–2025 | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_*.parquet` |
| `ingest-green` | Green Taxi | 2016–2025 | `https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_*.parquet` |
| `ingest-fhvhv` | HVFHV (Uber/Lyft) | 2019–2025 | `https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_*.parquet` |
| `ingest-fhv` | FHV (rádio-táxi) | 2016–2025 | `https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_*.parquet` |

- **Configuração:** Glue 4.0, 2 workers G.1X, timeout 8h, sem retry automático
- **Resiliência:** Exponential backoff (5 tentativas) e validação de tamanho mínimo (1KB) em cada download
- **Destino:** `s3://nyc-taxi-pipeline-bronze-*/[dataset]/year=YYYY/month=MM/`

### AWS Glue Catalog
- Database `nyc_taxi_db` com 1 crawler (`bronze-crawler`) para catalogar os dados ingeridos.

### AWS Step Functions — Orquestração
- State machine `nyc-taxi-pipeline-ingest-orchestrator` executa os **4 jobs em paralelo** usando um estado `Parallel`, finalizando quando todos completam.

---

## Estrutura do S3 Bronze (após ingestão)

```
s3://nyc-taxi-pipeline-bronze-*/
├── yellow/year=2016/month=01/yellow_tripdata_2016-01.parquet
├── green/year=2016/month=01/green_tripdata_2016-01.parquet
├── fhvhv/year=2019/month=02/fhvhv_tripdata_2019-02.parquet
└── fhv/year=2016/month=01/fhv_tripdata_2016-01.parquet
```

---

## Estrutura Terraform

```
IAC/
├── provider.tf          # AWS provider us-east-1
├── variables.tf         # project_name, account_id, region
├── main.tf              # Chamada dos módulos S3, Glue, Step Functions
├── outputs.tf           # Outputs consolidados
├── scripts/etl/
│   └── ingest_to_bronze.py   # Script de ingestão (compartilhado pelos 4 jobs)
└── modules/
    ├── s3/              # 5 buckets com segurança
    ├── glue/            # IAM, catalog, crawler, 4 jobs
    └── step_functions/  # IAM, state machine paralela
```
