# Documento de Segurança e Revisão (DSR)

# Pipeline de Dados NYC Taxi — Plano de Segurança

---

Projeto: nyc-taxi-pipeline
Data de elaboração: 25 de março de 2026
Responsável técnico: Leonardo Dorta
Ambiente: AWS — Conta <AWS_ACCOUNT_ID> — Região us-east-1
Versão do documento: 1.0

---

## 1. Introdução

Este documento apresenta a análise de segurança realizada sobre a infraestrutura do projeto NYC Taxi Pipeline, que consiste em uma pipeline de dados construída na AWS seguindo a arquitetura Medallion (Bronze, Silver e Gold). Toda a infraestrutura é provisionada via Terraform (versão 1.5 ou superior) utilizando o provider AWS na versão 5.x.

O objetivo desta revisão é identificar os controles de segurança já existentes, apontar lacunas e propor melhorias alinhadas às boas práticas da AWS e ao framework Well-Architected.

---

## 2. Escopo da Análise

A análise contemplou os seguintes serviços e componentes:

- Amazon S3 — armazenamento das camadas Bronze, Silver, Gold, scripts ETL e resultados do Athena (5 buckets)
- AWS Glue — 6 jobs de ETL, 2 crawlers e 1 database no catálogo
- Amazon DynamoDB — tabela de controle de falhas de download
- AWS Step Functions — 2 state machines de orquestração (ingestão e transformação)
- IAM — roles e políticas de acesso dos serviços
- Terraform — configuração de infraestrutura como código

---

## 3. Controles de Segurança Existentes

Durante a análise, foram identificados os seguintes controles já implementados no projeto:

No que diz respeito ao Amazon S3, todos os cinco buckets possuem bloqueio completo de acesso público, com as quatro flags de proteção ativas (block_public_acls, block_public_policy, ignore_public_acls e restrict_public_buckets). O versionamento está habilitado em todos os buckets, o que permite a recuperação de objetos em caso de exclusão ou sobrescrita acidental. A criptografia em repouso também está configurada, utilizando o algoritmo AES-256 via SSE-S3.

Para o AWS Glue, foi criada uma IAM Role dedicada com políticas de acesso restritas aos ARNs dos buckets S3 do projeto e ao ARN da tabela DynamoDB. Todos os jobs possuem logging contínuo no CloudWatch e métricas habilitadas, o que permite o acompanhamento da execução e a identificação de falhas.

O AWS Step Functions conta com uma IAM Role separada da role do Glue, respeitando o princípio de separação de responsabilidades. A permissão de invocação entre state machines está corretamente restrita ao ARN da state machine de transformação.

No Terraform, tanto a versão do provider quanto a versão mínima do Terraform estão fixadas, evitando incompatibilidades em atualizações futuras.

A arquitetura do projeto segue o padrão Medallion com separação clara entre as camadas de dados, e a orquestração foi dividida em duas state machines independentes, permitindo a execução isolada da transformação quando necessário.

---

## 4. Lacunas Identificadas

### 4.1. Gerenciamento do Estado do Terraform

O arquivo de estado do Terraform (terraform.tfstate) está armazenado localmente na máquina do desenvolvedor. Este arquivo contém informações sobre todos os recursos provisionados, incluindo identificadores de conta e ARNs. O armazenamento local apresenta dois riscos principais: a perda do arquivo em caso de falha no disco ou formatação da máquina, e a ausência de mecanismo de lock, que pode causar corrupção do estado em cenários de execução simultânea.

Além disso, o identificador da conta AWS está definido como valor padrão em uma variável do Terraform, o que não representa uma boa prática para ambientes compartilhados.

### 4.2. Políticas IAM com Escopo Amplo

As políticas da IAM Role do Step Functions utilizam o recurso wildcard ("*") para as ações de Glue (StartJobRun, GetJobRun, StartCrawler, GetCrawler) e para as ações do EventBridge. Embora funcional, essa configuração concede permissões além do necessário, contrariando o princípio do menor privilégio. Em um cenário onde a role fosse comprometida, seria possível interagir com qualquer job ou crawler da conta, não apenas os pertencentes a este projeto.

A policy gerenciada AWSGlueServiceRole, anexada à role do Glue, também inclui permissões que vão além do escopo necessário para este projeto específico.

### 4.3. Ausência de Criptografia em Trânsito

Não existe uma bucket policy que force o uso de conexões HTTPS (TLS) para acesso aos buckets S3. Sem essa restrição, requisições via HTTP não criptografado são aceitas, expondo os dados em trânsito a interceptação.

### 4.4. Ausência de Logging e Rastreamento no Step Functions

As state machines não possuem configuração de logging no CloudWatch nem rastreamento via X-Ray. Em caso de falha na orquestração, a investigação fica limitada aos logs dos jobs individuais do Glue, sem visibilidade sobre o fluxo de execução como um todo.

### 4.5. Ausência de Backup Contínuo no DynamoDB

A tabela de controle de falhas não possui Point-in-Time Recovery (PITR) habilitado, nem proteção contra exclusão acidental (deletion protection). Embora os dados desta tabela sejam transitórios, a perda da tabela durante uma execução pode comprometer o processo de retry.

### 4.6. Ausência de Políticas de Ciclo de Vida no S3

Não existem regras de lifecycle nos buckets, o que significa que todos os objetos permanecem na classe de armazenamento Standard indefinidamente. Para dados históricos na camada Bronze, que são acessados com menor frequência após o processamento, isso representa um custo desnecessário.

### 4.7. Ausência de Logging de Acesso no S3

Nenhum dos buckets possui S3 Access Logging habilitado. Sem esses logs, não é possível auditar quem acessou quais objetos, quando e a partir de qual endereço IP.

### 4.8. Ausência de Security Configuration no Glue

Os jobs do Glue não possuem uma Security Configuration associada, o que significa que os logs do CloudWatch e os dados temporários gravados no S3 durante a execução dos jobs não possuem criptografia adicional além da padrão.

---

## 5. Recomendações

### 5.1. Prioridade Alta

**Migração do Terraform State para backend remoto.** Recomenda-se a criação de um bucket S3 dedicado com criptografia habilitada para armazenar o arquivo de estado, acompanhado de uma tabela DynamoDB para controle de lock. O identificador da conta deve ser obtido dinamicamente via data source do Terraform (aws_caller_identity) em vez de ser definido como variável.

**Restrição das políticas IAM do Step Functions.** As permissões de Glue devem ser restritas utilizando o padrão de ARN do projeto (arn:aws:glue:us-east-1:<AWS_ACCOUNT_ID>:job/nyc-taxi-pipeline-*) em vez do wildcard. O mesmo se aplica aos crawlers e aos recursos do EventBridge.

**Habilitação de logging no Step Functions.** Ambas as state machines devem ter logging configurado no CloudWatch com nível ERROR no mínimo, incluindo os dados de execução para facilitar a depuração.

**Habilitação de PITR e proteção contra exclusão no DynamoDB.** O Point-in-Time Recovery deve ser ativado na tabela de falhas, junto com a flag de deletion protection.

### 5.2. Prioridade Média

**Criação de Security Configuration no Glue.** Deve-se configurar a criptografia dos logs do CloudWatch via SSE-KMS e dos dados temporários no S3 via SSE-S3, associando essa configuração a todos os jobs.

**Implementação de lifecycle policies no S3.** Para o bucket Bronze, recomenda-se a transição para a classe Standard-IA após 90 dias e para Glacier após 365 dias. Para o bucket de scripts, uma política de expiração de versões antigas após 30 dias é suficiente.

**Habilitação de S3 Access Logging.** Os logs de acesso de todos os buckets devem ser direcionados para um prefixo dedicado dentro do bucket de scripts ou para um bucket específico de logs.

**Implementação de bucket policy para forçar TLS.** Todos os buckets devem ter uma policy que negue qualquer requisição onde a condição aws:SecureTransport seja false, garantindo que apenas conexões HTTPS sejam aceitas.

### 5.3. Prioridade Baixa

**Habilitação de X-Ray Tracing no Step Functions.** O rastreamento distribuído permite visualizar o tempo de execução de cada etapa e identificar gargalos na pipeline.

**Padronização de tags em todos os recursos.** Recomenda-se a adoção de um conjunto mínimo de tags (Project, Environment, ManagedBy) aplicado de forma consistente a todos os recursos via bloco locals no Terraform.

**Avaliação de execução dos Glue Jobs em VPC.** Para ambientes produtivos, a execução dentro de uma VPC com endpoints privados para S3 e DynamoDB elimina o tráfego pela internet pública. No contexto atual, onde os jobs fazem download de dados de uma fonte externa (NYC TLC), essa recomendação se aplica especificamente aos jobs de transformação.

**Migração da criptografia S3 para SSE-KMS.** A substituição do AES-256 (SSE-S3) por SSE-KMS com chave gerenciada pelo cliente oferece maior controle sobre o acesso às chaves e permite auditoria via CloudTrail.

---

## 6. Análise de Riscos

A seguir, apresenta-se a classificação dos principais riscos identificados, considerando a probabilidade de ocorrência e o impacto potencial sobre o projeto.

A perda do arquivo de estado do Terraform representa o risco de maior criticidade. Como o arquivo está armazenado localmente, qualquer falha no equipamento do desenvolvedor resultaria na impossibilidade de gerenciar a infraestrutura existente via Terraform, exigindo a importação manual de cada recurso. A probabilidade é considerada alta em horizontes de tempo longos.

A utilização de wildcards nas políticas IAM do Step Functions apresenta risco médio de escalação de privilégio. Embora a exploração dependa do comprometimento prévio da role, o impacto seria alto, pois permitiria a manipulação de qualquer job ou crawler na conta.

A ausência de backup contínuo no DynamoDB e de lifecycle policies no S3 representam riscos de impacto alto e médio respectivamente, com probabilidade média de materialização.

A falta de enforcement de TLS nos buckets S3 representa um risco de baixa probabilidade, considerando que os acessos são realizados por serviços AWS que utilizam HTTPS por padrão, mas o impacto seria alto caso uma integração futura utilizasse HTTP.

---

## 7. Conformidade com o AWS Well-Architected Framework

No pilar de Segurança, o projeto atende parcialmente aos requisitos. A criptografia em repouso está implementada em todos os serviços, porém sem o uso de chaves gerenciadas pelo cliente. A criptografia em trânsito não possui enforcement explícito. O princípio do menor privilégio é atendido parcialmente, com escopo correto nas políticas do Glue mas amplo nas políticas do Step Functions.

No pilar de Confiabilidade, a principal lacuna é a ausência de backups automatizados (PITR) no DynamoDB. O versionamento do S3 atende parcialmente a esse requisito para os dados armazenados nos buckets.

No pilar de Excelência Operacional, o monitoramento está implementado nos jobs do Glue via CloudWatch Logs e métricas, mas ausente nas state machines do Step Functions. A adoção de Terraform como ferramenta de IaC é um ponto positivo, embora o gerenciamento do estado precise de melhorias.

No pilar de Otimização de Custos, a ausência de lifecycle policies no S3 representa a principal oportunidade de melhoria, especialmente para dados históricos na camada Bronze que poderiam ser migrados para classes de armazenamento mais econômicas.

---

## 8. Considerações Finais

O projeto NYC Taxi Pipeline apresenta uma base sólida de segurança, com controles fundamentais já implementados como criptografia em repouso, bloqueio de acesso público nos buckets, separação de roles IAM e monitoramento dos jobs de ETL. A arquitetura Medallion e a separação da orquestração em state machines independentes demonstram maturidade no desenho da solução.

As recomendações apresentadas neste documento visam elevar o nível de segurança do projeto, com foco principal na proteção do estado da infraestrutura, no refinamento das permissões IAM e na ampliação da observabilidade da pipeline. A implementação das melhorias de prioridade alta é fortemente recomendada antes da entrada em produção.

---

Documento elaborado em 25 de março de 2026.
