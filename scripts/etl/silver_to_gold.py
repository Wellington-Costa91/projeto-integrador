"""
Glue Job: Silver -> Gold (Modelo Dimensional)
- Le tabelas Iceberg da Silver (yellow, green, fhvhv, fhv)
- Cria 7 dimensoes + 1 tabela fato
- Grava como Iceberg no Glue Catalog (database gold)
"""
import sys
import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SILVER_BUCKET", "GOLD_BUCKET", "BRONZE_BUCKET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{args['GOLD_BUCKET']}/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")

silver_bucket = args["SILVER_BUCKET"]
gold_bucket = args["GOLD_BUCKET"]
SILVER_DB = "nyc_taxi_silver"
GOLD_DB = "nyc_taxi_gold"

# ============================================================
# 1. Leitura das tabelas Silver
# ============================================================
datasets = {}
for name in ["yellow", "green", "fhvhv", "fhv"]:
    try:
        df = spark.read.format("iceberg").load(f"glue_catalog.{SILVER_DB}.{name}")
        datasets[name] = df
        print(f"OK leitura {name}: {df.count()} registros")
    except Exception as e:
        print(f"SKIP {name}: {e}")

if not datasets:
    raise Exception("Nenhuma tabela silver encontrada")

# ============================================================
# 2. Uniao de todos os datasets com tipo de transporte
# ============================================================
union_df = None
for name, df in datasets.items():
    tagged = df.withColumn("dataset", F.lit(name))

    # Padronizar colunas comuns
    for col_name in ["Fare_amount", "Tip_amount", "Tolls_amount", "Congestion_Surcharge",
                     "Airport_fee", "Improvement_surcharge", "Trip_distance",
                     "PULocationID", "DOLocationID", "pickup_datetime", "dropoff_datetime"]:
        if col_name not in tagged.columns:
            tagged = tagged.withColumn(col_name, F.lit(None).cast("double") if col_name != "pickup_datetime" and col_name != "dropoff_datetime" else F.lit(None).cast("timestamp"))

    # Campos especificos por dataset
    if name == "fhvhv":
        tagged = tagged.withColumn("empresa", F.col("Company"))
        for c in ["bfc_amount", "sales_tax", "Fare_amount", "driver_pay"]:
            if c not in tagged.columns:
                tagged = tagged.withColumn(c, F.lit(0.0))
    else:
        tagged = tagged.withColumn("empresa", F.when(F.col("Company") == 1, "CMT")
                                               .when(F.col("Company") == 2, "VERIFONE")
                                               .otherwise("OUTRO")) if "Company" in tagged.columns else tagged.withColumn("empresa", F.lit("DESCONHECIDO"))
        for c in ["bfc_amount", "sales_tax", "Fare_amount", "driver_pay"]:
            tagged = tagged.withColumn(c, F.lit(0.0))

    if "dispatching_base_num" in tagged.columns and name == "fhv":
        tagged = tagged.withColumn("empresa", F.col("dispatching_base_num"))

    # Garantir cbd_congestion_fee existe
    if "cbd_congestion_fee" not in tagged.columns:
        tagged = tagged.withColumn("cbd_congestion_fee", F.lit(0.0))

    union_df = tagged if union_df is None else union_df.unionByName(tagged, allowMissingColumns=True)

# Preencher nulos numericos com 0
numeric_cols = ["Fare_amount", "Tip_amount", "Tolls_amount", "Congestion_Surcharge",
                "Airport_fee", "Improvement_surcharge", "bfc_amount", "sales_tax",
                "driver_pay", "Trip_distance", "Extra", "MTA_tax", "cbd_congestion_fee"]
for c in numeric_cols:
    if c in union_df.columns:
        union_df = union_df.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)))

# Calcular duracao em segundos
union_df = union_df.withColumn(
    "duracao_segundos",
    F.coalesce(
        F.col("trip_time") if "trip_time" in union_df.columns else F.lit(None),
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime"))
    ).cast("int")
)

# ============================================================
# 3. DIM_TIPO_TRANSPORTE
# ============================================================
tipo_transporte_data = [
    (1, "TAXI"),
    (2, "TAXI"),
    (3, "APLICATIVO DE TRANSPORTE"),
    (4, "APLICATIVO DE TRANSPORTE"),
    (5, "APLICATIVO DE TRANSPORTE"),
    (6, "APLICATIVO DE TRANSPORTE"),
    (7, "APLICATIVO DE TRANSPORTE"),
]
dim_tipo_transporte = spark.createDataFrame(tipo_transporte_data, ["id_tipo_transporte", "grupo_tipo_transporte"])

def map_tipo_transporte(dataset, license_num):
    return (
        F.when(dataset == "yellow", F.lit(1))
         .when(dataset == "green", F.lit(2))
         .when((dataset == "fhvhv") & (license_num == "HV0003"), F.lit(3))  # Uber
         .when((dataset == "fhvhv") & (license_num == "HV0005"), F.lit(4))  # Lyft
         .when((dataset == "fhvhv") & (license_num == "HV0004"), F.lit(5))  # Via
         .when(dataset == "fhvhv", F.lit(6))
         .when(dataset == "fhv", F.lit(7))
         .otherwise(F.lit(6))
    )

license_col = F.col("Company") if "Company" in union_df.columns else F.lit(None)
union_df = union_df.withColumn("id_tipo_transporte", map_tipo_transporte(F.col("dataset"), license_col))

# ============================================================
# 4. DIM_EMPRESA
# ============================================================
empresas_df = union_df.select(F.upper(F.trim(F.coalesce(F.col("empresa"), F.lit("DESCONHECIDO")))).alias("empresa")).distinct()
w = Window.orderBy("empresa")
dim_empresa = empresas_df.withColumn("id_empresa", F.row_number().over(w))

union_df = union_df.withColumn("empresa_norm", F.upper(F.trim(F.coalesce(F.col("empresa"), F.lit("DESCONHECIDO")))))
union_df = union_df.join(dim_empresa, union_df["empresa_norm"] == dim_empresa["empresa"], "left").drop("empresa_norm")

# ============================================================
# 5. DIM_LOCAL (taxi_zone_lookup.csv do Bronze)
# ============================================================
zones_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{args['BRONZE_BUCKET']}/zones/taxi_zone_lookup.csv")

dim_local = zones_df.select(
    F.col("LocationID").cast("int").alias("id_local"),
    F.upper(F.trim(F.col("Borough"))).alias("distrito"),
    F.upper(F.trim(F.col("Zone"))).alias("zona"),
    F.upper(F.trim(F.col("service_zone"))).alias("zona_servico"),
)

# Classificacao de regiao agrupada
airport_ids = [1, 132, 138]
cbd_ids = [4, 12, 13, 45, 48, 50, 68, 79, 87, 88, 90, 100, 107, 113, 114, 125, 137,
            144, 148, 158, 161, 162, 163, 164, 170, 186, 209, 211, 224, 230, 231,
            232, 233, 234, 246, 249]

dim_local = dim_local.withColumn("regiao_agrupada",
    F.when(F.col("id_local").isin(airport_ids), "AEROPORTO")
     .when(F.col("id_local").isin(cbd_ids), "MANHATTAN CBD")
     .when(F.col("distrito") == "MANHATTAN", "MANHATTAN SEM CBD")
     .when(F.col("id_local").isin(264, 265), "DESCONHECIDOS")
     .otherwise("OUTROS BAIRROS")
)

# ============================================================
# 6. DIM_DISTANCIA
# ============================================================
distancia_data = [
    (1, "0-1 MILHAS", "ATE 1 MILHA"),
    (2, "1-3 MILHAS", "CURTA DISTANCIA"),
    (3, "3-5 MILHAS", "MEDIA DISTANCIA"),
    (4, "5-10 MILHAS", "MEDIA-LONGA DISTANCIA"),
    (5, "10-20 MILHAS", "LONGA DISTANCIA"),
    (6, "20+ MILHAS", "MUITO LONGA DISTANCIA"),
]
dim_distancia = spark.createDataFrame(distancia_data, ["id_distancia", "faixa_distancia", "descricao_distancia"])

union_df = union_df.withColumn("id_faixa_distancia",
    F.when(F.col("Trip_distance") < 1, 1)
     .when(F.col("Trip_distance") < 3, 2)
     .when(F.col("Trip_distance") < 5, 3)
     .when(F.col("Trip_distance") < 10, 4)
     .when(F.col("Trip_distance") < 20, 5)
     .otherwise(6)
)

# ============================================================
# 7. DIM_TEMPO_HORARIO
# ============================================================
faixa_horaria_data = [
    (1, "MADRUGADA"),      # 00-06
    (2, "PICO MANHA"),     # 06-10
    (3, "MANHA"),          # 10-12
    (4, "ALMOCO"),         # 12-14
    (5, "TARDE"),          # 14-17
    (6, "PICO TARDE"),     # 17-20
    (7, "NOITE"),          # 20-00
]
dim_tempo_horario = spark.createDataFrame(faixa_horaria_data, ["id_faixa_horaria", "faixa_horaria"])

union_df = union_df.withColumn("hora", F.hour("pickup_datetime"))
union_df = union_df.withColumn("id_faixa_horaria",
    F.when(F.col("hora") < 6, 1)
     .when(F.col("hora") < 10, 2)
     .when(F.col("hora") < 12, 3)
     .when(F.col("hora") < 14, 4)
     .when(F.col("hora") < 17, 5)
     .when(F.col("hora") < 20, 6)
     .otherwise(7)
)

# ============================================================
# 8. DIM_STATUS_GORJETA
# ============================================================
dim_status_gorjeta = spark.createDataFrame([(1, "SIM"), (2, "NAO")], ["id_status_gorjeta", "teve_gorjeta"])

union_df = union_df.withColumn("id_status_gorjeta",
    F.when(F.coalesce(F.col("Tip_amount"), F.lit(0.0)) > 0, 1).otherwise(2)
)

# ============================================================
# 9. DIM_STATUS_CONGESTIONAMENTO
# ============================================================
dim_status_congestionamento = spark.createDataFrame([(1, "SIM"), (2, "NAO")], ["id_status_congestionamento", "incide_taxa_cbd"])

union_df = union_df.withColumn("id_status_congestionamento",
    F.when(F.coalesce(F.col("Congestion_Surcharge"), F.lit(0.0)) > 0, 1).otherwise(2)
)

# ============================================================
# 10. FATO_CORRIDA
# ============================================================
fato = union_df.select(
    F.monotonically_increasing_id().alias("id_corrida"),
    F.col("id_tipo_transporte"),
    F.col("id_empresa"),
    F.col("PULocationID").alias("id_local_embarque"),
    F.col("DOLocationID").alias("id_local_desembarque"),
    F.col("id_faixa_distancia"),
    F.col("id_faixa_horaria"),
    F.col("id_status_gorjeta"),
    F.col("id_status_congestionamento"),
    F.col("pickup_datetime").alias("datahora_embarque"),
    F.coalesce(F.col("duracao_segundos"), F.lit(0)).alias("duracao_segundos"),
    F.col("Trip_distance").alias("distancia_percorrida"),
    F.coalesce(F.col("Fare_amount"), F.lit(0.0)).alias("valor_justo"),
    F.col("Tip_amount").alias("valor_gorjeta"),
    F.col("Tolls_amount").alias("valor_pedagio"),
    F.coalesce(F.col("Improvement_surcharge"), F.lit(0.0)).alias("valor_taxa_modernizacao"),
    F.coalesce(F.col("Airport_fee"), F.lit(0.0)).alias("valor_taxa_aeroporto"),
    F.coalesce(F.col("Congestion_Surcharge"), F.lit(0.0)).alias("valor_taxa_congestionamento"),
    F.coalesce(F.col("cbd_congestion_fee"), F.lit(0.0)).alias("valor_taxa_congestionamento_cbd"),
    F.coalesce(F.col("bfc_amount"), F.lit(0.0)).alias("valor_seguro_bfc"),
    (F.coalesce(F.col("Extra"), F.lit(0.0)) + F.coalesce(F.col("MTA_tax"), F.lit(0.0)) + F.coalesce(F.col("sales_tax"), F.lit(0.0))).alias("valor_demais_taxas"),
    F.lit(1).alias("quantidade_viagem"),
    F.when(F.col("payment_type") == 2, "DINHEIRO").otherwise("DEMAIS").alias("tipo_pagamento"),
    F.date_format("pickup_datetime", "yyyy-MM").alias("ano_mes"),
    F.year("pickup_datetime").alias("year"),
    F.month("pickup_datetime").alias("month"),
)

# Preencher nulos restantes
for c in fato.columns:
    if c not in ["datahora_embarque", "year", "month", "id_corrida"]:
        fato = fato.withColumn(c, F.coalesce(F.col(c), F.lit(0)))

# ============================================================
# 11. Gravacao no Gold (Iceberg)
# Placeholders criados pelo Terraform — substituidos por Iceberg real
# ============================================================
glue_client = boto3.client("glue")

def write_gold(df, table_name, partition_cols=None):
    full_name = f"glue_catalog.{GOLD_DB}.{table_name}"
    # Deletar tabela existente via Glue API (funciona para placeholder e Iceberg)
    try:
        glue_client.delete_table(DatabaseName=GOLD_DB, Name=table_name)
        print(f"  {table_name} | Tabela anterior removida via Glue API")
    except glue_client.exceptions.EntityNotFoundException:
        pass
    except Exception as e:
        print(f"  {table_name} | Aviso ao remover tabela: {e}")
    df.createOrReplaceTempView(f"tmp_{table_name}")
    partition_clause = f"PARTITIONED BY ({', '.join(partition_cols)})" if partition_cols else ""
    spark.sql(f"""
        CREATE TABLE {full_name}
        USING iceberg
        {partition_clause}
        TBLPROPERTIES ('write.spark.fanout.enabled'='true')
        LOCATION 's3://{gold_bucket}/{table_name}/'
        AS SELECT * FROM tmp_{table_name}
    """)
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_name}").collect()[0]["cnt"]
    print(f"OK {full_name}: {count} registros")

# Dimensoes
write_gold(dim_tipo_transporte, "dim_tipo_transporte")
write_gold(dim_empresa, "dim_empresa")
write_gold(dim_local, "dim_local")
write_gold(dim_distancia, "dim_distancia")
write_gold(dim_tempo_horario, "dim_tempo_horario")
write_gold(dim_status_gorjeta, "dim_status_gorjeta")
write_gold(dim_status_congestionamento, "dim_status_congestionamento")

# Fato
write_gold(fato, "fato_corrida", ["year", "month"])

print("Transformacao Silver -> Gold concluida.")
