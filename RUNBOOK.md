# 🚕 NYC Taxi Pipeline — RUNBOOK Operacional

> Guia prático de operação, execução e manutenção do pipeline.

---

## Objetivo Operacional

Manter o pipeline NYC Taxi Pipeline operacional, garantindo ingestão, transformação e disponibilização dos dados para análise via QuickSight.

---

## Visão Geral da Operação

| Componente | Serviço | Estado Esperado |
|------------|---------|-----------------|
| Infraestrutura | Terraform | Provisionada (5 buckets, 8 jobs, 2 SFN, 1 DynamoDB) |
| Ingestão | Step Function 1 | Execução sob demanda |
| Transformação | Step Function 2 | Chamada automaticamente pela SF1 |
| Dashboard | QuickSight | Dataset atualizado após pipeline |

---

## Pré-requisitos

- [ ] Terraform >= 1.5 instalado
- [ ] AWS CLI v2 configurado (`aws configure` → região us-east-1)
- [ ] Credenciais com permissões: S3, Glue, DynamoDB, Step Functions, IAM
- [ ] Conta AWS configurada no `variables.tf`
- [ ] Região: `us-east-1`

---

## Dependências

| Dependência | Tipo | Observação |
|-------------|------|------------|
| Endpoint NYC TLC | Externa | https://d37ci6vzurychx.cloudfront.net/trip-data/ |
| AWS Glue 4.0 | Serviço | Spark serverless com suporte Iceberg |
| Apache Iceberg | Formato | Integrado nativamente ao Glue 4.0 |
| Glue Catalog | Serviço | 3 databases (bronze, silver, gold) |

---

## Configuração do Ambiente

```bash
# 1. Clonar repositório
git clone https://github.com/Wellington-Costa91/projeto-integrador
cd projeto-integrador/

# 2. Configurar AWS CLI
aws configure
# AWS Access Key ID: <sua-key>
# AWS Secret Access Key: <sua-secret>
# Default region: us-east-1
# Default output: json

# 3. Verificar acesso
aws sts get-caller-identity
```

---

## Provisionamento / Recriação do Ambiente

```bash
# Inicializar Terraform (baixa providers)
terraform init

# Verificar o que será criado/alterado
terraform plan

# Aplicar infraestrutura
terraform apply

# Verificar outputs
terraform output
```

### Outputs Importantes

| Output | Uso |
|--------|-----|
| `step_function_ingest_arn` | ARN para executar pipeline completo |
| `step_function_transform_arn` | ARN para reprocessar apenas transformação |
| `s3_buckets` | Nomes dos 5 buckets |
| `glue_jobs` | Nomes dos jobs de ingestão |

---

## Variáveis e Configurações

| Variável | Arquivo | Valor | Descrição |
|----------|---------|-------|-----------|
| `project_name` | variables.tf | nyc-taxi-pipeline | Prefixo de todos os recursos |
| `aws_region` | variables.tf | us-east-1 | Região AWS |
| `account_id` | variables.tf | <SEU_ACCOUNT_ID> | ID da conta |
| `FULL_LOAD` | bronze_to_silver.py | false | true=reprocessa tudo; false=incremental |
| `MAX_WORKERS` | ingest_to_bronze.py | 10 | Threads paralelas de download |
| `MAX_RETRIES` | ingest_to_bronze.py | 5 | Tentativas com backoff |

---

## Execução do Pipeline

### Pipeline Completo (Ingestão + Transformação)

```bash
aws stepfunctions start-execution \
  --state-machine-arn $(terraform output -raw step_function_ingest_arn) \
  --region us-east-1
```

**Tempo estimado:** 2–6 horas (depende do volume e throttling da fonte)

### Apenas Transformação (Reprocessamento Silver + Gold)

```bash
aws stepfunctions start-execution \
  --state-machine-arn $(terraform output -raw step_function_transform_arn) \
  --region us-east-1
```

**Tempo estimado:** 30–90 minutos

---

## Execução Manual de Jobs Individuais

```bash
# Ingestão de um dataset específico
aws glue start-job-run --job-name nyc-taxi-pipeline-ingest-yellow

# Retry de downloads falhos
aws glue start-job-run --job-name nyc-taxi-pipeline-retry-failed

# Transformação Bronze → Silver
aws glue start-job-run --job-name nyc-taxi-pipeline-bronze-to-silver

# Transformação Silver → Gold
aws glue start-job-run --job-name nyc-taxi-pipeline-silver-to-gold
```

---

## Reprocessamentos

### Reprocessar Silver (full load)

Alterar o argumento `--FULL_LOAD` para `true`:

```bash
aws glue start-job-run \
  --job-name nyc-taxi-pipeline-bronze-to-silver \
  --arguments '{"--FULL_LOAD":"true"}'
```

### Reprocessar Gold

Executar apenas a Step Function 2 (transform) — o job Gold sempre recria todas as tabelas.

### Reprocessar Ingestão de um Período Específico

Editar `YEAR_RANGE` no script `ingest_to_bronze.py`, fazer `terraform apply` (upload automático via etag), e executar o job correspondente.

---

## Rotinas Operacionais

| Rotina | Frequência | Procedimento |
|--------|-----------|--------------|
| Ingestão de novos meses | Mensal | Executar SF1 (ingest orchestrator) |
| Verificar falhas pendentes | Após ingestão | Scan DynamoDB (ver abaixo) |
| Atualizar dashboard | Após pipeline | Refresh dataset no QuickSight |
| Backup do tfstate | Antes de alterações | Copiar terraform.tfstate para local seguro |

---

## Monitoramento

### Step Functions
- Console AWS → Step Functions → Executions
- Status esperado: `SUCCEEDED`
- Duração típica SF1: 2–6h | SF2: 30–90min

### Glue Jobs
- Console AWS → Glue → Jobs → Run history
- Logs: CloudWatch → `/aws-glue/jobs/output`

### DynamoDB (falhas pendentes)
```bash
aws dynamodb scan \
  --table-name nyc-taxi-pipeline-failed-downloads \
  --select COUNT
```
**Esperado:** Count = 0 (sem pendências)

---

## Logs e Diagnóstico

```bash
# Logs do Glue Job (últimos erros)
aws logs filter-log-events \
  --log-group-name /aws-glue/jobs/output \
  --filter-pattern "ERROR" \
  --limit 20

# Execuções falhas do Step Functions
aws stepfunctions list-executions \
  --state-machine-arn <ARN> \
  --status-filter FAILED

# Detalhes de uma execução específica
aws stepfunctions describe-execution \
  --execution-arn <EXECUTION_ARN>

# Verificar partições no S3
aws s3 ls s3://nyc-taxi-pipeline-gold-<ACCOUNT_ID>/fato_corrida/ --recursive | tail -10
```

---

## Testes Básicos de Validação

```sql
-- Executar no Athena (database: nyc_taxi_gold)

-- 1. Verificar contagem da fato
SELECT COUNT(*) FROM nyc_taxi_gold.fato_corrida;

-- 2. Verificar dimensões populadas
SELECT COUNT(*) FROM nyc_taxi_gold.dim_local;          -- Esperado: 265
SELECT COUNT(*) FROM nyc_taxi_gold.dim_tipo_transporte; -- Esperado: 7
SELECT COUNT(*) FROM nyc_taxi_gold.dim_distancia;       -- Esperado: 6
SELECT COUNT(*) FROM nyc_taxi_gold.dim_tempo_horario;   -- Esperado: 7

-- 3. Verificar dados recentes
SELECT ano_mes, COUNT(*) FROM nyc_taxi_gold.fato_corrida
WHERE year = 2025 GROUP BY ano_mes ORDER BY ano_mes;

-- 4. Verificar integridade referencial
SELECT COUNT(*) FROM nyc_taxi_gold.fato_corrida f
LEFT JOIN nyc_taxi_gold.dim_local l ON f.id_local_embarque = l.id_local
WHERE l.id_local IS NULL;  -- Esperado: 0 ou próximo de 0
```

---

## Troubleshooting

| Sintoma | Causa | Solução |
|---------|-------|---------|
| Download timeout/503 | Throttling NYC TLC | Reduzir MAX_WORKERS para 5; executar retry-failed |
| "Table not found" no Silver | Placeholder não removido | Deletar tabela no Glue Catalog; re-executar job |
| AccessDenied no StartTransform | Policy da role SFN | `terraform apply` para corrigir |
| OutOfMemory no Gold | Workers insuficientes | Aumentar para 10 workers G.2X ou G.4X |
| Dashboard desatualizado | Cache SPICE | Refresh manual do dataset no QuickSight |
| Terraform state corrompido | Execução simultânea | Restaurar backup do .tfstate |

---

## Problemas Conhecidos

1. **Terraform state local** — risco de perda. Fazer backup antes de qualquer alteração.
2. **Data Quality não bloqueia** — dados com problemas podem chegar ao Gold. Verificar resultados manualmente.
3. **IDs não determinísticos** — `monotonically_increasing_id()` gera IDs diferentes a cada execução.
4. **Políticas IAM com wildcard** — Step Functions role usa `*` para Glue actions.

---

## Contingências

### Pipeline falha no meio da execução
1. Verificar qual estado falhou no console Step Functions
2. Corrigir a causa (ver Troubleshooting)
3. Re-executar a Step Function — o pipeline é idempotente

### Perda do terraform.tfstate
1. Importar recursos existentes: `terraform import module.s3.aws_s3_bucket.this["bronze"] nyc-taxi-pipeline-bronze-<ACCOUNT_ID>`
2. Repetir para cada recurso
3. **Prevenção:** migrar para backend remoto S3

### Endpoint NYC TLC indisponível
1. Aguardar e re-executar (dados históricos não mudam)
2. Verificar DynamoDB para itens pendentes
3. Executar job retry-failed quando endpoint voltar

---

## Procedimentos de Recuperação

### Reconstruir ambiente do zero
```bash
terraform destroy  # Remove tudo (CUIDADO: dados serão perdidos)
terraform apply    # Recria infraestrutura
# Executar SF1 para reingerir dados
```

### Reconstruir apenas Gold
```bash
aws stepfunctions start-execution \
  --state-machine-arn $(terraform output -raw step_function_transform_arn) \
  --region us-east-1
```

---

## Checklist Operacional

- [ ] AWS CLI configurado e testado (`aws sts get-caller-identity`)
- [ ] Terraform inicializado (`terraform init`)
- [ ] Infraestrutura provisionada (`terraform apply`)
- [ ] Pipeline executado com sucesso (SF1 → SUCCEEDED)
- [ ] Tabelas Gold populadas (Athena → COUNT > 0)
- [ ] Dashboard QuickSight atualizado (Refresh dataset)
- [ ] DynamoDB sem pendências (scan → Count = 0)
- [ ] Backup do terraform.tfstate realizado

---

## Boas Práticas Operacionais

1. **Sempre fazer backup do .tfstate** antes de `terraform apply`
2. **Executar `terraform plan`** antes de apply para revisar mudanças
3. **Testar com subset** (1-2 meses) antes de full load
4. **Verificar DynamoDB** após ingestão para garantir completude
5. **Refresh QuickSight** após cada execução do pipeline
6. **Monitorar custos** — Glue cobra por DPU-hora; Step Functions por transição

---

## Melhorias Futuras

- [ ] Migrar Terraform state para S3 + DynamoDB lock
- [ ] Adicionar alertas SNS para falhas
- [ ] Implementar lifecycle policies no S3
- [ ] Configurar refresh agendado no QuickSight
- [ ] Adicionar testes automatizados (pytest)
- [ ] Implementar CI/CD para deploy dos scripts
