# 

# conta isengard <AWS_ACCOUNT_ID>

# 

# 

# 

# Planejamento do Projeto

## Análise de Dados de Transporte Urbano de Nova York – Pipeline de Dados na AWS

**Integrantes do Grupo:**

* Wellington Antonio de Oliveira Costa  
* Leonardo Oliveira Dorta  
* Vinicius Castillo de Lima  
* Klaus Hilario Lambert Rezende

2026

## 1\. Introdução

O transporte urbano por veículos de aluguel é o principal sistema de mobilidade sob demanda da cidade de Nova York. A NYC Taxi and Limousine Commission (TLC) regulamenta e registra todas as corridas realizadas por quatro categorias de veículos:

* **Yellow Taxis:** táxis tradicionais que operam em toda a cidade.  
* **Green Taxis:** táxis tradicionais que operam fora de Manhattan.  
* **HVFHV – High Volume For-Hire Vehicles:** Uber, Lyft, Via.  
* **FHV – For-Hire Vehicles:** rádio-táxis e serviços equivalentes.

Juntas, essas quatro modalidades somam mais de **313 milhões de corridas apenas em 2025**. Os dados incluem informações sobre horários, localização, distâncias, valores, gorjetas, taxas governamentais e, no caso do HVFHV, tempo de espera e corridas compartilhadas.

Este projeto utiliza esses dados para construir um pipeline de dados completo na AWS, desde a ingestão até a modelagem preditiva, respondendo a 4 questões de pesquisa — uma por cada integrante do grupo.

## 2\. Objetivos (Questões de Pesquisa)

### 2.1. Q1 – Dinâmica Competitiva Yellow Taxi vs HVFHV

**Pergunta:** Qual a dinâmica competitiva entre Yellow Taxis e HVFHV (Uber/Lyft), e como a participação de mercado evoluiu por zona, horário e tipo de corrida entre 2019 e 2025?  
O Yellow Taxi historicamente dominava NYC, mas o HVFHV cresceu drasticamente.

### 2.2. Q2 – Fatores que Influenciam a Gorjeta

**Pergunta:** Quais fatores influenciam a probabilidade e o valor da gorjeta, e o comportamento de gorjeta difere entre táxis tradicionais e apps (Uber/Lyft)?  
A gorjeta é uma variável presente em 3 dos 4 datasets: tip\_amount (Yellow e Green) e tips (HVFHV). O profiling inicial revela diferenças significativas: 57,7% das corridas Yellow têm gorjeta vs apenas 18,9% no HVFHV, porém quando há gorjeta no HVFHV o valor médio é maior ($6,55 vs $4,77). 

2.3. Q3 – Impacto da Pandemia de COVID-19  
**Pergunta:** Como a pandemia de COVID-19 impactou o volume, o perfil das corridas e o comportamento de gorjeta nos táxis de NYC, e qual foi a trajetória de recuperação de 2020 a 2025?  
O dataset Yellow Taxi cobre 2019 a 2025, permitindo uma análise do impacto da pandemia. 

2.4. Q4 – Taxa de Congestionamento do CBD  
**Pergunta:** Como a taxa de congestionamento do CBD (janeiro/2025) impactou o volume, distribuição e valor das corridas em Manhattan, e esse impacto foi diferente entre táxis e apps (Uber/Lyft)?  
Em janeiro de 2025, NYC implementou uma taxa de $9 para veículos entrando abaixo da 60th Street. O HVFHV registra a cbd*congestion*fee por corrida, e o Yellow/Green também. 

## 3\. Recursos (Máquinas e Ambientes)

O projeto será desenvolvido integralmente em ambiente Cloud (AWS), priorizando serviços serverless para minimizar custos operacionais e simplificar a gestão de infraestrutura.

* **Armazenamento:** Amazon S3 com organização em camadas Bronze / Silver / Gold (arquitetura medalhão).  
* **Processamento:** AWS Glue (Spark serverless) como engine principal de ETL.  
* **Consulta e Análise:** Amazon Athena para consultas SQL ad-hoc diretamente sobre S3.  
* **Machine Learning:** Amazon SageMaker Notebooks para treinamento e avaliação de modelos (XGBoost, séries temporais).  
* **Orquestração:** AWS Step Functions para coordenar o pipeline end-to-end.  
* **Desenvolvimento:** AWS Cloud9 (IDE web) para desenvolvimento Python.  
* **Controle e Metadados:** Amazon DynamoDB para metadados de controle de ingestão; AWS Glue Catalog para esquemas e partições.


## 4\. Escolha de Ferramentas

| Serviço AWS | Papel no Projeto | Etapa |
| :---- | :---- | :---- |
| **S3** | Armazenamento dos 4 datasets em Parquet (Bronze/Silver/Gold) | Armazenamento |
| **Glue Catalog** | Catálogo de metadados (schemas, partições, tabelas) | Catalogação |
| **Glue ETL Jobs** | Processamento Spark: harmonização, limpeza, features | Processamento |
| **Athena** | Consultas SQL direto sobre S3 (análise ad-hoc) | Análise |
| **SageMaker** | Notebooks para treinar e avaliar modelos de ML | Machine Learning |
| **Step Functions** | Orquestração do pipeline completo (state machine) | Orquestração |
| **DynamoDB** | Metadados de controle (status de ingestão, configs) | Controle |

poc-projeto-integrado - quicksight


Limpar erros do yellow 

dim_empresa tá estranho


bronze_to_silver.py` — Campos padronizados na Silver agora seguem a tabela Gold:                                                                
                                                                                                                                                   
  ┌─────────────────────────┬─────────────────────────┬─────────────────────────┬────────────────────────┐                                         
  │ Campo Gold (imagem)     │ Yellow Taxi (original)  │ Green Taxi (original)   │ HVFHS (original)       │                                         
  ├─────────────────────────┼─────────────────────────┼─────────────────────────┼────────────────────────┤                                         
  │ `pickup_datetime`       │ `tpep_pickup_datetime`  │ `lpep_pickup_datetime`  │ `pickup_datetime`      │                                         
  │ `dropoff_datetime`      │ `tpep_dropoff_datetime` │ `lpep_dropoff_datetime` │ `dropoff_datetime`     │                                         
  │ `trip_time`             │ —                       │ —                       │ `trip_time`            │                                         
  │ `Trip_distance`         │ `trip_distance`         │ `trip_distance`         │ `trip_miles`           │                                         
  │ `PULocationID`          │ `pulocationid`          │ `pulocationid`          │ `pulocationid`         │                                         
  │ `DOLocationID`          │ `dolocationid`          │ `dolocationid`          │ `dolocationid`         │                                         
  │ `Company`               │ `vendorid`              │ `vendorid`              │ `hvfhs_license_num`    │                                         
  │ `Fare_amount`           │ `fare_amount`           │ `fare_amount`           │ `base_passenger_fare`  │                                         
  │ `Tip_amount`            │ `tip_amount`            │ `tip_amount`            │ `tips`                 │                                         
  │ `Tolls_amount`          │ `tolls_amount`          │ `tolls_amount`          │ `tolls`                │                                         
  │ `Improvement_surcharge` │ `improvement_surcharge` │ `improvement_surcharge` │ —                      │                                         
  │ `Airport_fee`           │ `airport_fee`           │ —                       │ `airport_fee`          │                                         
  │ `Congestion_Surcharge`  │ `congestion_surcharge`  │ `congestion_surcharge`  │ `congestion_surcharge` │                                         
  │ `cbd_congestion_fee`    │ —                       │ —                       │ `cbd_congestion_fee`   │                                         
  │ `bfc_amount`            │ —                       │ —                       │ `bcf`                  │                                         
  │ `Extra`                 │ `extra`                 │ `extra`                 │ —                      │                                         
  │ `MTA_tax`               │ `mta_tax`               │ `mta_tax`               │ —                      │                                         
  │ `sales_tax`             │ —                       │ —                       │ `sales_tax`            │                                         
  │ `Total_amount`          │ `total_amount`          │ `total_amount`          │ —                      │                                         
  └─────────────────────────┴─────────────────────────┴─────────────────────────┴────────────────────────┘   


  verificar a tabela fato para agrupar os registros