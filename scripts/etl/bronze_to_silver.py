"""
Glue Job: Bronze -> Silver (Iceberg)
- ThreadPool com 10 threads: cada thread processa 1 ano (itera os 12 meses)
- Dedup dentro de cada mes, overwrite da particao
- Padroniza nomes dos campos (de-para Bronze -> Silver)
- Garante tipagem correta e texto em maiusculo
- Remove registros com pickup_datetime fora do ano/mes de referencia
- Grava em formato Iceberg particionado por ano/mes no Glue Catalog
"""
import sys
import threading
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, year as yr, month as mn, upper, trim, lit
from pyspark.sql.types import TimestampType, IntegerType, DoubleType, StringType, LongType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BRONZE_BUCKET", "SILVER_BUCKET"])

# Flag: "true" = full load (reprocessa tudo), "false" = incremental (pula meses ja existentes)
FULL_LOAD = args.get("FULL_LOAD", "false").lower() == "true"
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

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
MAX_WORKERS = 10
glue_client = boto3.client("glue")

# ============================================================
# Mapeamento de nomes: bronze (original) -> silver (padronizado)
# ============================================================
COLUMN_MAP = {
    "yellow": {
        "vendorid":              "Company",
        "tpep_pickup_datetime":  "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "passenger_count":       "passenger_count",
        "trip_distance":         "Trip_distance",
        "ratecodeid":            "rate_code_id",
        "store_and_fwd_flag":    "store_and_fwd_flag",
        "pulocationid":          "PULocationID",
        "dolocationid":          "DOLocationID",
        "payment_type":          "payment_type",
        "fare_amount":           "Fare_amount",
        "extra":                 "Extra",
        "mta_tax":               "MTA_tax",
        "tip_amount":            "Tip_amount",
        "tolls_amount":          "Tolls_amount",
        "improvement_surcharge": "Improvement_surcharge",
        "total_amount":          "Total_amount",
        "congestion_surcharge":  "Congestion_Surcharge",
        "airport_fee":           "Airport_fee",
        "cbd_congestion_fee":    "cbd_congestion_fee",
    },
    "green": {
        "vendorid":              "Company",
        "lpep_pickup_datetime":  "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
        "passenger_count":       "passenger_count",
        "trip_distance":         "Trip_distance",
        "ratecodeid":            "rate_code_id",
        "store_and_fwd_flag":    "store_and_fwd_flag",
        "pulocationid":          "PULocationID",
        "dolocationid":          "DOLocationID",
        "payment_type":          "payment_type",
        "fare_amount":           "Fare_amount",
        "extra":                 "Extra",
        "mta_tax":               "MTA_tax",
        "tip_amount":            "Tip_amount",
        "tolls_amount":          "Tolls_amount",
        "improvement_surcharge": "Improvement_surcharge",
        "total_amount":          "Total_amount",
        "trip_type":             "trip_type",
        "congestion_surcharge":  "Congestion_Surcharge",
        "cbd_congestion_fee":    "cbd_congestion_fee",
    },
    "fhvhv": {
        "hvfhs_license_num":     "Company",
        "dispatching_base_num":  "dispatching_base_num",
        "originating_base_num":  "originating_base_num",
        "request_datetime":      "request_datetime",
        "on_scene_datetime":     "on_scene_datetime",
        "pickup_datetime":       "pickup_datetime",
        "dropoff_datetime":      "dropoff_datetime",
        "pulocationid":          "PULocationID",
        "dolocationid":          "DOLocationID",
        "trip_miles":            "Trip_distance",
        "trip_time":             "trip_time",
        "base_passenger_fare":   "Fare_amount",
        "tolls":                 "Tolls_amount",
        "bcf":                   "bfc_amount",
        "sales_tax":             "sales_tax",
        "congestion_surcharge":  "Congestion_Surcharge",
        "airport_fee":           "Airport_fee",
        "tips":                  "Tip_amount",
        "driver_pay":            "driver_pay",
        "shared_request_flag":   "shared_request_flag",
        "shared_match_flag":     "shared_match_flag",
        "access_a_ride_flag":    "access_a_ride_flag",
        "wav_request_flag":      "wav_request_flag",
        "wav_match_flag":        "wav_match_flag",
        "cbd_congestion_fee":    "cbd_congestion_fee",
    },
    "fhv": {
        "dispatching_base_num":  "dispatching_base_num",
        "pickup_datetime":       "pickup_datetime",
        "dropoff_datetime":      "dropoff_datetime",
        "pulocationid":          "PULocationID",
        "dolocationid":          "DOLocationID",
        "sr_flag":               "sr_flag",
        "affiliated_base_number":"affiliated_base_num",
    },
}

SCHEMA = {
    "Company":                StringType(),
    "pickup_datetime":        TimestampType(),
    "dropoff_datetime":       TimestampType(),
    "request_datetime":       TimestampType(),
    "on_scene_datetime":      TimestampType(),
    "passenger_count":        IntegerType(),
    "Trip_distance":          DoubleType(),
    "rate_code_id":           IntegerType(),
    "store_and_fwd_flag":     StringType(),
    "PULocationID":           IntegerType(),
    "DOLocationID":           IntegerType(),
    "payment_type":           IntegerType(),
    "Fare_amount":            DoubleType(),
    "Extra":                  DoubleType(),
    "MTA_tax":                DoubleType(),
    "Tip_amount":             DoubleType(),
    "Tolls_amount":           DoubleType(),
    "Improvement_surcharge":  DoubleType(),
    "Total_amount":           DoubleType(),
    "Congestion_Surcharge":   DoubleType(),
    "Airport_fee":            DoubleType(),
    "cbd_congestion_fee":     DoubleType(),
    "bfc_amount":             DoubleType(),
    "sales_tax":              DoubleType(),
    "trip_type":              IntegerType(),
    "dispatching_base_num":   StringType(),
    "originating_base_num":   StringType(),
    "affiliated_base_num":    StringType(),
    "trip_time":              LongType(),
    "driver_pay":             DoubleType(),
    "shared_request_flag":    StringType(),
    "shared_match_flag":      StringType(),
    "access_a_ride_flag":     StringType(),
    "wav_request_flag":       StringType(),
    "wav_match_flag":         StringType(),
    "sr_flag":                DoubleType(),
}

YEAR_RANGE = {
    "yellow": (2016, 2026),
    "green":  (2016, 2026),
    "fhvhv":  (2019, 2026),
    "fhv":    (2016, 2026),
}
MONTH_RANGE = (1, 12)

# Lock para operacoes de escrita Iceberg (Spark nao e thread-safe para writes)
write_lock = threading.Lock()


def normalize(df, col_map):
    """Renomeia colunas, seleciona mapeadas, aplica cast e upper em textos."""
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
    for c in df.columns:
        if c in SCHEMA and isinstance(SCHEMA[c], StringType):
            df = df.withColumn(c, upper(trim(col(c))))
    return df


def process_year(name, col_map, y, table_name, location, table_ready_flag):
    """Processa todos os meses de um ano para um dataset."""
    year_stats = {"bronze": 0, "dedup": 0, "clean": 0, "skipped": 0}

    for m in range(MONTH_RANGE[0], MONTH_RANGE[1] + 1):
        # Incremental: pula meses que ja existem na Silver
        if not FULL_LOAD and table_ready_flag[0]:
            try:
                cnt = spark.sql(f"SELECT 1 FROM {table_name} WHERE year = {y} AND month = {m} LIMIT 1").count()
                if cnt > 0:
                    year_stats["skipped"] += 1
                    continue
            except Exception:
                pass

        path = f"s3://{bronze_bucket}/{name}/year={y}/month={m:02d}/"

        try:
            df = spark.read.parquet(path)
        except Exception:
            continue

        df = normalize(df, col_map)

        bronze_count = df.count()
        if bronze_count == 0:
            continue

        df = df.dropDuplicates()
        dedup_count = df.count()

        df = df.filter(col("pickup_datetime").isNotNull() & col("dropoff_datetime").isNotNull())
        df = df.filter((yr(col("pickup_datetime")) == y) & (mn(col("pickup_datetime")) == m))
        clean_count = df.count()

        df = df.withColumn("year", lit(y)).withColumn("month", lit(m))

        # Escrita serializada via lock
        view_name = f"tmp_{name}_{y}_{m}"
        with write_lock:
            df.createOrReplaceTempView(view_name)

            if not table_ready_flag[0]:
                # Primeiro write: cria tabela Iceberg real (placeholder ja removido via Glue API)
                spark.sql(f"""
                    CREATE TABLE {table_name}
                    USING iceberg
                    PARTITIONED BY (year, month)
                    TBLPROPERTIES ('write.spark.fanout.enabled'='true')
                    LOCATION '{location}'
                    AS SELECT * FROM {view_name}
                """)
                table_ready_flag[0] = True
            else:
                # Evolucao de schema: adiciona colunas novas que nao existem na tabela
                table_schema = spark.read.format("iceberg").load(table_name).schema
                table_cols = {f.name for f in table_schema.fields}
                schema_changed = False
                for field in df.schema.fields:
                    if field.name not in table_cols:
                        spark.sql(f"ALTER TABLE {table_name} ADD COLUMN `{field.name}` {field.dataType.simpleString()}")
                        schema_changed = True
                        print(f"  {name} | Schema evolution: adicionada coluna {field.name}")

                # Reler schema atualizado (apos ALTER TABLE ou alteracoes de outras threads)
                table_schema = spark.read.format("iceberg").load(table_name).schema

                # Alinhar DataFrame com schema da tabela (adicionar colunas faltantes como null)
                for tfield in table_schema.fields:
                    if tfield.name not in df.columns:
                        df = df.withColumn(tfield.name, lit(None).cast(tfield.dataType))

                # Reordenar colunas na ordem da tabela
                table_col_order = [f.name for f in table_schema.fields]
                df = df.select(table_col_order)

                df.createOrReplaceTempView(view_name)
                spark.sql(f"""
                    INSERT OVERWRITE {table_name}
                    SELECT * FROM {view_name}
                """)

        year_stats["bronze"] += bronze_count
        year_stats["dedup"] += dedup_count
        year_stats["clean"] += clean_count

        removed = bronze_count - clean_count
        print(f"  {name} | {y}-{m:02d} | bronze={bronze_count:,} dedup={dedup_count:,} clean={clean_count:,} removidos={removed:,}")

    return y, year_stats


report = []

for name, col_map in COLUMN_MAP.items():
    start_y, end_y = YEAR_RANGE[name]
    table_name = f"glue_catalog.{SILVER_DB}.{name}"
    location = f"s3://{silver_bucket}/{name}/"

    # Detecta se tabela Iceberg real ja existe (vs placeholder do Terraform)
    try:
        spark.read.format("iceberg").load(table_name).limit(0)
        table_ready_flag = [True]
        print(f"  {name} | Tabela Iceberg ja existe, usando INSERT OVERWRITE")
    except Exception:
        # Placeholder ou tabela invalida — deletar via Glue API (nao via Spark/Iceberg)
        try:
            glue_client.delete_table(DatabaseName=SILVER_DB, Name=name)
            print(f"  {name} | Placeholder removido via Glue API")
        except glue_client.exceptions.EntityNotFoundException:
            print(f"  {name} | Tabela nao existe, sera criada")
        except Exception as e:
            print(f"  {name} | Erro ao remover placeholder: {e}")
        table_ready_flag = [False]

    total_bronze = 0
    total_dedup = 0
    total_clean = 0

    years = list(range(start_y, end_y + 1))
    print(f"Processando {name} ({start_y}-{end_y}) com {min(MAX_WORKERS, len(years))} threads...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_year, name, col_map, y, table_name, location, table_ready_flag): y
            for y in years
        }

        for future in as_completed(futures):
            y, stats = future.result()
            total_bronze += stats["bronze"]
            total_dedup += stats["dedup"]
            total_clean += stats["clean"]
            print(f"  {name} | Ano {y} concluido: bronze={stats['bronze']:,} clean={stats['clean']:,} skipped={stats.get('skipped',0)}")

    report.append({
        "dataset": name,
        "bronze": total_bronze,
        "apos_dedup": total_dedup,
        "apos_filtro_nulos": total_clean,
        "silver": total_clean,
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
