"""
Glue Job: Bronze -> Silver (Iceberg)
- Remove duplicatas
- Padroniza nomes dos campos entre todos os datasets
- Garante tipagem correta
- Campos texto em maiusculo
- Grava em formato Iceberg particionado por ano/mes no Glue Catalog
"""
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, year, month, upper, trim
from pyspark.sql.types import TimestampType, IntegerType, DoubleType, StringType, LongType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BRONZE_BUCKET", "SILVER_BUCKET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Configuracao Iceberg com Glue Catalog (extensions já vem do --datalake-formats=iceberg)
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{args['SILVER_BUCKET']}/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")
spark.conf.set("spark.sql.parquet.mergeSchema", "true")
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

bronze_bucket = args["BRONZE_BUCKET"]
silver_bucket = args["SILVER_BUCKET"]
SILVER_DB = "nyc_taxi_silver"

# Mapeamento: nome_original -> nome_padronizado para cada dataset
COLUMN_MAP = {
    "yellow": {
        "vendorid":              "vendor_id",
        "tpep_pickup_datetime":  "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "passenger_count":       "passenger_count",
        "trip_distance":         "trip_distance",
        "ratecodeid":            "rate_code_id",
        "store_and_fwd_flag":    "store_and_fwd_flag",
        "pulocationid":          "pu_location_id",
        "dolocationid":          "do_location_id",
        "payment_type":          "payment_type",
        "fare_amount":           "fare_amount",
        "extra":                 "extra",
        "mta_tax":               "mta_tax",
        "tip_amount":            "tip_amount",
        "tolls_amount":          "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "total_amount":          "total_amount",
        "congestion_surcharge":  "congestion_surcharge",
        "airport_fee":           "airport_fee",
    },
    "green": {
        "vendorid":              "vendor_id",
        "lpep_pickup_datetime":  "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
        "passenger_count":       "passenger_count",
        "trip_distance":         "trip_distance",
        "ratecodeid":            "rate_code_id",
        "store_and_fwd_flag":    "store_and_fwd_flag",
        "pulocationid":          "pu_location_id",
        "dolocationid":          "do_location_id",
        "payment_type":          "payment_type",
        "fare_amount":           "fare_amount",
        "extra":                 "extra",
        "mta_tax":               "mta_tax",
        "tip_amount":            "tip_amount",
        "tolls_amount":          "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "total_amount":          "total_amount",
        "trip_type":             "trip_type",
        "congestion_surcharge":  "congestion_surcharge",
    },
    "fhvhv": {
        "hvfhs_license_num":     "license_num",
        "dispatching_base_num":  "dispatching_base_num",
        "originating_base_num":  "originating_base_num",
        "request_datetime":      "request_datetime",
        "on_scene_datetime":     "on_scene_datetime",
        "pickup_datetime":       "pickup_datetime",
        "dropoff_datetime":      "dropoff_datetime",
        "pulocationid":          "pu_location_id",
        "dolocationid":          "do_location_id",
        "trip_miles":            "trip_distance",
        "trip_time":             "trip_time",
        "base_passenger_fare":   "base_passenger_fare",
        "tolls":                 "tolls_amount",
        "bcf":                   "bcf",
        "sales_tax":             "sales_tax",
        "congestion_surcharge":  "congestion_surcharge",
        "airport_fee":           "airport_fee",
        "tips":                  "tip_amount",
        "driver_pay":            "driver_pay",
        "shared_request_flag":   "shared_request_flag",
        "shared_match_flag":     "shared_match_flag",
        "access_a_ride_flag":    "access_a_ride_flag",
        "wav_request_flag":      "wav_request_flag",
        "wav_match_flag":        "wav_match_flag",
    },
    "fhv": {
        "dispatching_base_num":  "dispatching_base_num",
        "pickup_datetime":       "pickup_datetime",
        "dropoff_datetime":      "dropoff_datetime",
        "pulocationid":          "pu_location_id",
        "dolocationid":          "do_location_id",
        "sr_flag":               "sr_flag",
        "affiliated_base_number":"affiliated_base_num",
    },
}

# Tipagem padronizada dos campos
SCHEMA = {
    "vendor_id":              IntegerType(),
    "pickup_datetime":        TimestampType(),
    "dropoff_datetime":       TimestampType(),
    "request_datetime":       TimestampType(),
    "on_scene_datetime":      TimestampType(),
    "passenger_count":        IntegerType(),
    "trip_distance":          DoubleType(),
    "rate_code_id":           IntegerType(),
    "store_and_fwd_flag":     StringType(),
    "pu_location_id":         IntegerType(),
    "do_location_id":         IntegerType(),
    "payment_type":           IntegerType(),
    "fare_amount":            DoubleType(),
    "extra":                  DoubleType(),
    "mta_tax":                DoubleType(),
    "tip_amount":             DoubleType(),
    "tolls_amount":           DoubleType(),
    "improvement_surcharge":  DoubleType(),
    "total_amount":           DoubleType(),
    "congestion_surcharge":   DoubleType(),
    "airport_fee":            DoubleType(),
    "trip_type":              IntegerType(),
    "license_num":            StringType(),
    "dispatching_base_num":   StringType(),
    "originating_base_num":   StringType(),
    "affiliated_base_num":    StringType(),
    "trip_time":              LongType(),
    "base_passenger_fare":    DoubleType(),
    "bcf":                    DoubleType(),
    "sales_tax":              DoubleType(),
    "driver_pay":             DoubleType(),
    "shared_request_flag":    StringType(),
    "shared_match_flag":      StringType(),
    "access_a_ride_flag":     StringType(),
    "wav_request_flag":       StringType(),
    "wav_match_flag":         StringType(),
    "sr_flag":                DoubleType(),
}

YEAR_RANGE = {
    "yellow": (2025, 2025),
    "green":  (2025, 2025),
    "fhvhv":  (2025, 2025),
    "fhv":    (2025, 2025),
}

MONTH_RANGE = (1, 6)

report = []

def read_and_normalize(spark, path, col_map):
    """Le parquet, renomeia e faz cast. Retorna None se falhar."""
    try:
        df = spark.read.parquet(path)
    except Exception:
        return None

    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())

    for old_name, new_name in col_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    mapped_cols = [c for c in col_map.values() if c in df.columns]
    df = df.select(mapped_cols)

    for c in df.columns:
        if c in SCHEMA:
            df = df.withColumn(c, col(c).cast(SCHEMA[c]))

    return df


for name, col_map in COLUMN_MAP.items():
    start_y, end_y = YEAR_RANGE[name]
    print(f"Processando {name} ({start_y}-{end_y})...")

    combined = None
    for y in range(start_y, end_y + 1):
        for m in range(MONTH_RANGE[0], MONTH_RANGE[1] + 1):
            path = f"s3://{bronze_bucket}/{name}/year={y}/month={m:02d}/"
            df = read_and_normalize(spark, path, col_map)
            if df is None:
                continue
            combined = df if combined is None else combined.unionByName(df, allowMissingColumns=True)

    if combined is None:
        print(f"SKIP {name}: nenhuma particao lida com sucesso")
        report.append({"dataset": name, "bronze": 0, "apos_dedup": 0, "apos_filtro_nulos": 0, "silver": 0})
        continue

    # Campos texto em maiusculo
    for c in combined.columns:
        if c in SCHEMA and isinstance(SCHEMA[c], StringType):
            combined = combined.withColumn(c, upper(trim(col(c))))

    # Contagem bronze (antes de qualquer limpeza)
    bronze_count = combined.count()
    print(f"  {name} | Bronze: {bronze_count}")

    # Remove duplicatas
    combined = combined.dropDuplicates()
    dedup_count = combined.count()
    print(f"  {name} | Apos dedup: {dedup_count} (removidos: {bronze_count - dedup_count})")

    # Remove registros sem pickup/dropoff
    combined = combined.filter(col("pickup_datetime").isNotNull() & col("dropoff_datetime").isNotNull())
    clean_count = combined.count()
    print(f"  {name} | Apos filtro nulos: {clean_count} (removidos: {dedup_count - clean_count})")

    # Particiona por ano/mes
    combined = combined.withColumn("year", year(col("pickup_datetime"))) \
                       .withColumn("month", month(col("pickup_datetime")))

    # Grava como Iceberg no Glue Catalog
    table_name = f"glue_catalog.{SILVER_DB}.{name}"

    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")
    except Exception:
        pass

    combined.createOrReplaceTempView(f"tmp_{name}")
    spark.sql(f"""
        CREATE TABLE {table_name}
        USING iceberg
        PARTITIONED BY (year, month)
        TBLPROPERTIES ('write.spark.fanout.enabled'='true')
        LOCATION 's3://{silver_bucket}/{name}/'
        AS SELECT * FROM tmp_{name}
    """)

    # Validacao: contagem silver deve bater com dados limpos
    silver_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]["cnt"]
    if silver_count != clean_count:
        raise Exception(f"ERRO DE VALIDACAO {name}: esperado={clean_count}, silver={silver_count}")
    print(f"  {name} | Silver gravado: {silver_count}")

    report.append({
        "dataset": name,
        "bronze": bronze_count,
        "apos_dedup": dedup_count,
        "apos_filtro_nulos": clean_count,
        "silver": silver_count,
    })

# --- Tabela resumo ---
print("\n" + "=" * 90)
print(f"{'DATASET':<10} | {'BRONZE':>12} | {'APOS DEDUP':>12} | {'APOS NULOS':>12} | {'SILVER':>12} | {'REMOVIDOS':>10}")
print("-" * 90)
for r in report:
    removidos = r["bronze"] - r["silver"]
    pct = f"{100*removidos/max(r['bronze'],1):.2f}%" if r["bronze"] > 0 else "N/A"
    print(f"{r['dataset']:<10} | {r['bronze']:>12,} | {r['apos_dedup']:>12,} | {r['apos_filtro_nulos']:>12,} | {r['silver']:>12,} | {pct:>10}")
print("=" * 90)

print("\nTransformacao Bronze -> Silver concluida.")
