# NYC Taxi Pipeline

> Pipeline de dados completo na AWS para análise de transporte urbano de Nova York (NYC TLC)

**Projeto Integrador — Pós-Graduação em Engenharia de Dados — USP**

---

## Integrantes

| Nome | Papel |
|------|-------|
| Wellington Antonio de Oliveira Costa | Engenharia de Dados |
| Leonardo Oliveira Dorta | Engenharia de Dados |
| Vinicius Castillo de Lima | Engenharia de Dados |
| Klaus Hilario Lambert Rezende | Engenharia de Dados |

---

## Descrição do Projeto

Pipeline de dados end-to-end na AWS que ingere, transforma e modela dados de corridas de táxi e aplicativos de transporte de Nova York (2016–2025). Utiliza arquitetura **Medallion** (Bronze → Silver → Gold) com formato **Apache Iceberg**, orquestração via **AWS Step Functions** e visualização em **Amazon QuickSight**.

---

## Problema Resolvido

A NYC TLC disponibiliza dados de mais de 313 milhões de corridas anuais em centenas de arquivos Parquet com schemas heterogêneos entre 4 datasets. Este projeto unifica, limpa, modela e disponibiliza esses dados para análise dimensional.

---

## Objetivos (Questões de Pesquisa)

| # | Questão |
|---|---------|
| Q1 | Dinâmica competitiva Yellow Taxi vs HVFHV — participação de mercado por zona/horário (2019–2025) |
| Q2 | Fatores que influenciam probabilidade e valor da gorjeta |
| Q3 | Impacto da pandemia COVID-19 no volume e perfil das corridas |
| Q4 | Efeito da taxa de congestionamento CBD ($9, jan/2025) em Manhattan |

---

## Arquitetura da Solução

```
┌─────────────────────────────────────────────────────────┐
│           STEP FUNCTION 1: INGEST ORCHESTRATOR          │
│  [Paralelo] Yellow + Green + FHVHV + FHV + Zones       │
│  → Retry Failed (DynamoDB) → Chama SF2                  │
└─────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────┐
│         STEP FUNCTION 2: TRANSFORM ORCHESTRATOR         │
│  Bronze→Silver (Iceberg) → [Paralelo] DQ (4 rulesets)  │
│  → Silver→Gold (Modelo Estrela) → SUCCESS               │
└─────────────────────────────────────────────────────────┘
```

| Camada | Formato | Descrição |
|--------|---------|-----------|
| Bronze | Parquet | Dados brutos particionados year/month |
| Silver | Iceberg | Dados limpos, deduplicados, normalizados |
| Gold | Iceberg | Modelo estrela: 7 dimensões + 1 fato |

---

## Tecnologias Utilizadas

| Tecnologia | Uso |
|------------|-----|
| Terraform >= 1.5 | Infraestrutura como código |
| AWS Glue 4.0 | Processamento Spark serverless |
| Apache Iceberg | Formato de tabela (ACID, schema evolution) |
| AWS Step Functions | Orquestração serverless |
| Amazon Athena | Queries SQL sobre Iceberg |
| Amazon QuickSight | Dashboards analíticos |
| Amazon DynamoDB | Dead-letter de downloads falhos |
| Python 3.x / PySpark | Scripts ETL |

---

## Estrutura do Repositório

```
IAC/
├── provider.tf              # AWS provider us-east-1
├── variables.tf             # Variáveis do projeto
├── main.tf                  # Chamada dos 4 módulos
├── outputs.tf               # Outputs consolidados
├── modules/
│   ├── s3/                  # 5 buckets com segurança
│   ├── glue/                # IAM, catalog, 8 jobs, 4 DQ rulesets
│   ├── dynamodb/            # Tabela de controle de falhas
│   └── step_functions/      # 2 state machines
├── scripts/etl/
│   ├── ingest_to_bronze.py  # Ingestão paralela (10 threads)
│   ├── ingest_zones.py      # Ingestão taxi_zone_lookup.csv
│   ├── retry_failed.py      # Retry de downloads falhos
│   ├── bronze_to_silver.py  # Transformação Iceberg
│   └── silver_to_gold.py    # Modelo dimensional
├── data/
│   └── taxi_zone_lookup.csv # 265 zonas de NYC
├── dashboard/
│   ├── quicksight-dashboard.yaml
│   ├── quicksight_dashboard_queries.sql
│   └── dashboard.png
└── docs/
    ├── contexto.md
    ├── RELATORIO.md
    ├── DSR_SEGURANCA.md
    ├── Atividade 3.md
    └── diagrama_pipeline.drawio
```

---

## Pré-requisitos

- Terraform >= 1.5
- AWS CLI v2 configurado
- Conta AWS com permissões para criar: S3, Glue, DynamoDB, Step Functions, IAM
- Acesso à região `us-east-1`
- Variável `account_id` configurada no `variables.tf`

---

## Instalação e Configuração

```bash
# Clonar repositório
git clone https://github.com/Wellington-Costa91/projeto-integrador
cd projeto-integrador

# Configurar AWS CLI
aws configure  # Região: us-east-1

# Inicializar e aplicar Terraform
terraform init
terraform plan
terraform apply
```

---

## Como Executar

### Pipeline completo (ingestão + transformação)
```bash
aws stepfunctions start-execution \
  --state-machine-arn $(terraform output -raw step_function_ingest_arn) \
  --region us-east-1
```

### Apenas transformação (reprocessamento)
```bash
aws stepfunctions start-execution \
  --state-machine-arn $(terraform output -raw step_function_transform_arn) \
  --region us-east-1
```

---

## Dados Utilizados

| Dataset | Período | Fonte |
|---------|---------|-------|
| Yellow Taxi | 2016–2025 | https://d37ci6vzurychx.cloudfront.net/trip-data/ |
| Green Taxi | 2016–2025 | https://d37ci6vzurychx.cloudfront.net/trip-data/ |
| FHVHV (Uber/Lyft) | 2019–2025 | https://d37ci6vzurychx.cloudfront.net/trip-data/ |
| FHV (rádio-táxi) | 2016–2025 | https://d37ci6vzurychx.cloudfront.net/trip-data/ |
| Zones Lookup | Estático | https://d37ci6vzurychx.cloudfront.net/misc/ |

---

## Apresentação Final

A apresentação executiva do projeto (slides de defesa) está disponível em:

**[docs/Apresentação_Final.pdf](docs/Apresentação_Final.pdf)**

Contém: contexto e questões de pesquisa, arquitetura serverless 100% IaC na AWS, volume processado (≈3 bilhões de corridas / 189 GB), modelo dimensional e principais insights (dinâmica táxi vs aplicativos, gorjetas, impacto COVID-19 e taxa CBD).

---

## Evidências

- `dashboard/dashboard.png` — Screenshot do dashboard QuickSight
- `docs/tabela-gold.jpeg` — Modelo dimensional Gold
- `docs/diagrama_pipeline.drawio` — Diagrama do pipeline
- `docs/Apresentação_Final.pdf` — Slides da apresentação final

---

## Melhorias Futuras

- Backend remoto para Terraform (S3 + DynamoDB lock)
- Alertas SNS para falhas no pipeline
- Lifecycle policies no S3
- Modelos de ML (previsão de demanda)
- Streaming para ingestão near-real-time

---

## 📧 Contato da Equipe

Projeto acadêmico — Pós-Graduação em Engenharia de Dados, USP, 2026.
