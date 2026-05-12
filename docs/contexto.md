# Análise Completa do Projeto — NYC Taxi Pipeline

Este é um **Projeto Integrador de Pós-Graduação** que constrói um pipeline de dados completo na AWS para analisar dados de transporte urbano de Nova York (NYC TLC). A infraestrutura é 100% provisionada via **Terraform** e segue a **arquitetura Medallion** (Bronze → Silver → Gold).

## Visão Geral das Fases

O pipeline tem **5 fases principais**, orquestradas por **2 Step Functions**:

---

### Fase 1 — Ingestão (Bronze)

**Scripts:** `ingest_to_bronze.py`, `ingest_zones.py`

- Baixa arquivos Parquet diretamente do endpoint público da NYC TLC (`d37ci6vzurychx.cloudfront.net`)
- **4 datasets** ingeridos em paralelo via Step Functions:
  - **Yellow Taxi** (2016–2025)
  - **Green Taxi** (2016–2025)
  - **HVFHV / Uber/Lyft** (2019–2025)
  - **FHV / rádio-táxi** (2016–2025)
- Também ingere o arquivo de referência `taxi_zone_lookup.csv` (mapeamento de zonas)
- Dados gravados no S3 Bronze particionados por `year=YYYY/month=MM/`
- Resiliência: exponential backoff (5 tentativas), validação de tamanho mínimo (1KB)
- Falhas registradas no **DynamoDB** para retry posterior

### Fase 2 — Retry de Falhas

**Script:** `retry_failed.py`

- Lê a tabela DynamoDB com downloads que falharam na Fase 1
- Tenta novamente com exponential backoff
- Se bem-sucedido, remove o registro do DynamoDB
- Executado automaticamente após a ingestão paralela

### Fase 3 — Catalogação (Crawler Bronze)

- O **Glue Crawler** (`bronze-crawler`) varre o bucket Bronze
- Registra schemas e partições no **Glue Catalog** (database `nyc_taxi_bronze`)
- A Step Function aguarda o crawler terminar (polling com wait de 30s) antes de prosseguir

### Fase 4 — Transformação Bronze → Silver

**Script:** `bronze_to_silver.py`

- Lê dados Parquet do Bronze e aplica:
  1. **Padronização de nomes** — cada dataset tem nomes de colunas diferentes (ex: `tpep_pickup_datetime` no Yellow vs `lpep_pickup_datetime` no Green vs `pickup_datetime` no HVFHV), todos mapeados para nomes comuns
  2. **Tipagem correta** — cast explícito para Timestamp, Integer, Double, String, Long
  3. **Texto em maiúsculo** — campos String normalizados com `UPPER(TRIM())`
  4. **Remoção de duplicatas** — `dropDuplicates()`
  5. **Filtro de nulos** — remove registros sem `pickup_datetime` ou `dropoff_datetime`
- Grava como **tabelas Iceberg** no Glue Catalog (database `nyc_taxi_silver`), particionadas por `year/month`
- Gera relatório de contagem: Bronze → Após Dedup → Após Filtro Nulos → Silver
- Atualmente processando apenas **2025, meses 1–6** (configurável via `YEAR_RANGE` e `MONTH_RANGE`)

### Fase 4.5 — Data Quality (Silver)

- Após a Silver, a Step Function executa **4 rulesets de qualidade** em paralelo (um por dataset)
- Regras DQDL validam:
  - `RowCount > 0`
  - Completeness de campos-chave (pickup/dropoff ≥ 99%, locations ≥ 90-95%)
  - Ranges de valores (fare ≥ -50, distance ≥ 0, tip ≥ 0, total entre -100 e 50000)
  - Campos específicos por dataset (passenger_count, trip_time, etc.)
- Resultado é registrado mas **não bloqueia** o pipeline (SUCCEEDED, FAILED e ERROR todos avançam)

### Fase 5 — Transformação Silver → Gold (Modelo Dimensional)

**Script:** `silver_to_gold.py`

- Lê as 4 tabelas Iceberg da Silver e cria um **modelo estrela** com:

**7 Dimensões:**

| Dimensão | Descrição |
|---|---|
| `dim_tipo_transporte` | Yellow(1), Green(2), Uber(3), Lyft(4), Via(5), Outro HVFHV(6), FHV(7) |
| `dim_empresa` | Empresas normalizadas (CMT, VERIFONE, HV0003, bases FHV, etc.) |
| `dim_local` | Zonas de NYC do `taxi_zone_lookup.csv` (distrito, zona, zona_servico) |
| `dim_distancia` | 6 faixas: 0-1mi, 1-3mi, 3-5mi, 5-10mi, 10-20mi, 20+mi |
| `dim_tempo_horario` | 7 faixas: Madrugada, Pico Manhã, Manhã, Almoço, Tarde, Pico Tarde, Noite |
| `dim_status_gorjeta` | SIM / NÃO |
| `dim_status_congestionamento` | SIM / NÃO (taxa de congestionamento) |

**1 Tabela Fato:**

- `fato_corrida` — contém todas as métricas: valor justo, gorjeta, pedágio, taxas (aeroporto, congestionamento, CBD, BFC, MTA, etc.), distância, duração, particionada por `year/month`

Tudo gravado como **Iceberg** no Glue Catalog (database `nyc_taxi_gold`).

---

## Infraestrutura (Terraform)

| Componente | Recursos |
|---|---|
| **S3** | 5 buckets (bronze, silver, gold, scripts, athena-results) com versionamento, SSE-S3, bloqueio público |
| **Glue** | 6 jobs (4 ingestão + retry + zones + silver + gold), 1 crawler, 3 databases, 4 DQ rulesets |
| **DynamoDB** | 1 tabela (controle de falhas, PAY_PER_REQUEST) |
| **Step Functions** | 2 state machines (ingest-orchestrator → transform-orchestrator) |
| **IAM** | 2 roles separadas (Glue e Step Functions) |

## Orquestração (Fluxo Completo)

```
Step Function 1 (Ingest)
  ├── [Paralelo] Ingest Yellow + Green + HVFHV + FHV + Zones
  ├── Retry Failed (DynamoDB)
  ├── Crawler Bronze (polling)
  └── Chama Step Function 2 (Transform)

Step Function 2 (Transform)
  ├── Bronze → Silver (Iceberg)
  ├── [Paralelo] Data Quality (4 rulesets)
  └── Silver → Gold (Modelo Dimensional Iceberg)
```

## Mapeamento de Campos (Bronze → Silver)

| Campo Gold | Yellow Taxi (original) | Green Taxi (original) | HVFHS (original) |
|---|---|---|---|
| `pickup_datetime` | `tpep_pickup_datetime` | `lpep_pickup_datetime` | `pickup_datetime` |
| `dropoff_datetime` | `tpep_dropoff_datetime` | `lpep_dropoff_datetime` | `dropoff_datetime` |
| `trip_time` | — | — | `trip_time` |
| `Trip_distance` | `trip_distance` | `trip_distance` | `trip_miles` |
| `PULocationID` | `pulocationid` | `pulocationid` | `pulocationid` |
| `DOLocationID` | `dolocationid` | `dolocationid` | `dolocationid` |
| `Company` | `vendorid` | `vendorid` | `hvfhs_license_num` |
| `Fare_amount` | `fare_amount` | `fare_amount` | `base_passenger_fare` |
| `Tip_amount` | `tip_amount` | `tip_amount` | `tips` |
| `Tolls_amount` | `tolls_amount` | `tolls_amount` | `tolls` |
| `Improvement_surcharge` | `improvement_surcharge` | `improvement_surcharge` | — |
| `Airport_fee` | `airport_fee` | — | `airport_fee` |
| `Congestion_Surcharge` | `congestion_surcharge` | `congestion_surcharge` | `congestion_surcharge` |
| `cbd_congestion_fee` | — | — | `cbd_congestion_fee` |
| `bfc_amount` | — | — | `bcf` |
| `Extra` | `extra` | `extra` | — |
| `MTA_tax` | `mta_tax` | `mta_tax` | — |
| `sales_tax` | — | — | `sales_tax` |
| `Total_amount` | `total_amount` | `total_amount` | — |

## Questões de Pesquisa (objetivo final)

Os dados Gold alimentarão análises para 4 questões:

1. **Q1 — Dinâmica competitiva Yellow Taxi vs HVFHV** — Como a participação de mercado evoluiu por zona, horário e tipo de corrida entre 2019 e 2025?
2. **Q2 — Fatores que influenciam a gorjeta** — Quais fatores influenciam a probabilidade e o valor da gorjeta? O comportamento difere entre táxis e apps?
3. **Q3 — Impacto da pandemia de COVID-19** — Como a pandemia impactou volume, perfil das corridas e gorjeta, e qual foi a trajetória de recuperação 2020–2025?
4. **Q4 — Taxa de congestionamento do CBD** — Como a taxa de $9 (jan/2025) impactou volume, distribuição e valor das corridas em Manhattan?

## Estrutura do Projeto

```
IAC/
├── provider.tf                    # AWS provider us-east-1
├── variables.tf                   # project_name, account_id, region
├── main.tf                        # Chamada dos módulos S3, Glue, DynamoDB, Step Functions
├── outputs.tf                     # Outputs consolidados
├── RELATORIO.md                   # Relatório de infraestrutura
├── DSR_SEGURANCA.md               # Documento de segurança e revisão
├── Atividade 3.md                 # Planejamento do projeto
├── diagrama_pipeline.drawio       # Diagrama visual do pipeline
├── taxi_zone_lookup.csv           # Arquivo de referência de zonas
├── tabela-gold.jpeg               # Imagem do modelo Gold
├── scripts/etl/
│   ├── ingest_to_bronze.py        # Ingestão dos 4 datasets → Bronze
│   ├── ingest_zones.py            # Ingestão taxi_zone_lookup.csv → Bronze
│   ├── retry_failed.py            # Retry de downloads falhos (DynamoDB)
│   ├── bronze_to_silver.py        # Transformação Bronze → Silver (Iceberg)
│   └── silver_to_gold.py          # Transformação Silver → Gold (Modelo Dimensional)
└── modules/
    ├── s3/                        # 5 buckets com segurança
    ├── glue/                      # IAM, catalog, crawler, jobs, DQ rulesets
    ├── dynamodb/                  # Tabela de controle de falhas
    └── step_functions/            # 2 state machines (ingestão + transformação)
```

## Documentação Existente

- **RELATORIO.md** — Relatório de infraestrutura com recursos criados e estrutura do S3
- **DSR_SEGURANCA.md** — Análise de segurança com controles existentes, lacunas, recomendações e conformidade Well-Architected
- **Atividade 3.md** — Planejamento do projeto com introdução, objetivos, ferramentas e mapeamento de campos
  
  ┌─────────┬──────────────────────────┐
  │ Dataset │ Range                    │
  ├─────────┼──────────────────────────┤
  │ yellow  │ 2016–2025 (120 meses)    │
  ├─────────┼──────────────────────────┤
  │ green   │ 2016–2025 (120 meses)    │
  ├─────────┼──────────────────────────┤
  │ fhvhv   │ 2019/fev–2025 (83 meses) │
  ├─────────┼──────────────────────────┤
  │ fhv     │ 2016–2025 (120 meses)    │
  └─────────┴──────────────────────────┘