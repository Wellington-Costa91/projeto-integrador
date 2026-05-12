# --- IAM Role ---
resource "aws_iam_role" "glue" {
  name = "${var.project_name}-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "glue-s3-access"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = flatten([for arn in var.s3_bucket_arns : [arn, "${arn}/*"]])
    }]
  })
}

resource "aws_iam_role_policy" "glue_dynamo" {
  name = "glue-dynamo-access"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["dynamodb:PutItem", "dynamodb:Scan", "dynamodb:DeleteItem"]
      Resource = var.dynamo_table_arn
    }]
  })
}

# --- Upload dos scripts ---
resource "aws_s3_object" "script_ingest" {
  bucket = var.scripts_bucket
  key    = "etl/ingest_to_bronze.py"
  source = "${path.module}/../../scripts/etl/ingest_to_bronze.py"
  etag   = filemd5("${path.module}/../../scripts/etl/ingest_to_bronze.py")
}

resource "aws_s3_object" "script_retry" {
  bucket = var.scripts_bucket
  key    = "etl/retry_failed.py"
  source = "${path.module}/../../scripts/etl/retry_failed.py"
  etag   = filemd5("${path.module}/../../scripts/etl/retry_failed.py")
}

# --- Upload script Zones ---
resource "aws_s3_object" "script_zones" {
  bucket = var.scripts_bucket
  key    = "etl/ingest_zones.py"
  source = "${path.module}/../../scripts/etl/ingest_zones.py"
  etag   = filemd5("${path.module}/../../scripts/etl/ingest_zones.py")
}

# --- Catalog Databases (1 por camada) ---
resource "aws_glue_catalog_database" "bronze" {
  name = "nyc_taxi_bronze"
}

resource "aws_glue_catalog_database" "silver" {
  name = "nyc_taxi_silver"
}

resource "aws_glue_catalog_database" "gold" {
  name = "nyc_taxi_gold"
}

# --- Tabelas Bronze (Glue Catalog) ---
resource "aws_glue_catalog_table" "bronze_yellow" {
  database_name = aws_glue_catalog_database.bronze.name
  name          = "yellow"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification               = "parquet"
    compressionType              = "none"
    "partition_filtering.enabled" = "true"
    typeOfData                   = "file"
  }

  storage_descriptor {
    location      = "s3://${var.bronze_bucket}/yellow/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "vendorid"
      type = "bigint"
    }
    columns {
      name = "tpep_pickup_datetime"
      type = "timestamp"
    }
    columns {
      name = "tpep_dropoff_datetime"
      type = "timestamp"
    }
    columns {
      name = "passenger_count"
      type = "double"
    }
    columns {
      name = "trip_distance"
      type = "double"
    }
    columns {
      name = "ratecodeid"
      type = "double"
    }
    columns {
      name = "store_and_fwd_flag"
      type = "string"
    }
    columns {
      name = "pulocationid"
      type = "bigint"
    }
    columns {
      name = "dolocationid"
      type = "bigint"
    }
    columns {
      name = "payment_type"
      type = "bigint"
    }
    columns {
      name = "fare_amount"
      type = "double"
    }
    columns {
      name = "extra"
      type = "double"
    }
    columns {
      name = "mta_tax"
      type = "double"
    }
    columns {
      name = "tip_amount"
      type = "double"
    }
    columns {
      name = "tolls_amount"
      type = "double"
    }
    columns {
      name = "improvement_surcharge"
      type = "double"
    }
    columns {
      name = "total_amount"
      type = "double"
    }
    columns {
      name = "congestion_surcharge"
      type = "double"
    }
    columns {
      name = "airport_fee"
      type = "double"
    }
    columns {
      name = "cbd_congestion_fee"
      type = "double"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
}

resource "aws_glue_catalog_table" "bronze_green" {
  database_name = aws_glue_catalog_database.bronze.name
  name          = "green"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification               = "parquet"
    compressionType              = "none"
    "partition_filtering.enabled" = "true"
    typeOfData                   = "file"
  }

  storage_descriptor {
    location      = "s3://${var.bronze_bucket}/green/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "vendorid"
      type = "bigint"
    }
    columns {
      name = "lpep_pickup_datetime"
      type = "timestamp"
    }
    columns {
      name = "lpep_dropoff_datetime"
      type = "timestamp"
    }
    columns {
      name = "store_and_fwd_flag"
      type = "string"
    }
    columns {
      name = "ratecodeid"
      type = "double"
    }
    columns {
      name = "pulocationid"
      type = "bigint"
    }
    columns {
      name = "dolocationid"
      type = "bigint"
    }
    columns {
      name = "passenger_count"
      type = "double"
    }
    columns {
      name = "trip_distance"
      type = "double"
    }
    columns {
      name = "fare_amount"
      type = "double"
    }
    columns {
      name = "extra"
      type = "double"
    }
    columns {
      name = "mta_tax"
      type = "double"
    }
    columns {
      name = "tip_amount"
      type = "double"
    }
    columns {
      name = "tolls_amount"
      type = "double"
    }
    columns {
      name = "ehail_fee"
      type = "double"
    }
    columns {
      name = "improvement_surcharge"
      type = "double"
    }
    columns {
      name = "total_amount"
      type = "double"
    }
    columns {
      name = "payment_type"
      type = "double"
    }
    columns {
      name = "trip_type"
      type = "double"
    }
    columns {
      name = "congestion_surcharge"
      type = "double"
    }
    columns {
      name = "cbd_congestion_fee"
      type = "double"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
}

resource "aws_glue_catalog_table" "bronze_fhvhv" {
  database_name = aws_glue_catalog_database.bronze.name
  name          = "fhvhv"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification               = "parquet"
    compressionType              = "none"
    "partition_filtering.enabled" = "true"
    typeOfData                   = "file"
  }

  storage_descriptor {
    location      = "s3://${var.bronze_bucket}/fhvhv/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "hvfhs_license_num"
      type = "string"
    }
    columns {
      name = "dispatching_base_num"
      type = "string"
    }
    columns {
      name = "originating_base_num"
      type = "string"
    }
    columns {
      name = "request_datetime"
      type = "timestamp"
    }
    columns {
      name = "on_scene_datetime"
      type = "timestamp"
    }
    columns {
      name = "pickup_datetime"
      type = "timestamp"
    }
    columns {
      name = "dropoff_datetime"
      type = "timestamp"
    }
    columns {
      name = "pulocationid"
      type = "bigint"
    }
    columns {
      name = "dolocationid"
      type = "bigint"
    }
    columns {
      name = "trip_miles"
      type = "double"
    }
    columns {
      name = "trip_time"
      type = "bigint"
    }
    columns {
      name = "base_passenger_fare"
      type = "double"
    }
    columns {
      name = "tolls"
      type = "double"
    }
    columns {
      name = "bcf"
      type = "double"
    }
    columns {
      name = "sales_tax"
      type = "double"
    }
    columns {
      name = "congestion_surcharge"
      type = "double"
    }
    columns {
      name = "airport_fee"
      type = "double"
    }
    columns {
      name = "tips"
      type = "double"
    }
    columns {
      name = "driver_pay"
      type = "double"
    }
    columns {
      name = "shared_request_flag"
      type = "string"
    }
    columns {
      name = "shared_match_flag"
      type = "string"
    }
    columns {
      name = "access_a_ride_flag"
      type = "string"
    }
    columns {
      name = "wav_request_flag"
      type = "string"
    }
    columns {
      name = "wav_match_flag"
      type = "string"
    }
    columns {
      name = "cbd_congestion_fee"
      type = "double"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
}

resource "aws_glue_catalog_table" "bronze_fhv" {
  database_name = aws_glue_catalog_database.bronze.name
  name          = "fhv"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification               = "parquet"
    compressionType              = "none"
    "partition_filtering.enabled" = "true"
    typeOfData                   = "file"
  }

  storage_descriptor {
    location      = "s3://${var.bronze_bucket}/fhv/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "dispatching_base_num"
      type = "string"
    }
    columns {
      name = "pickup_datetime"
      type = "timestamp"
    }
    columns {
      name = "dropoff_datetime"
      type = "timestamp"
    }
    columns {
      name = "pulocationid"
      type = "double"
    }
    columns {
      name = "dolocationid"
      type = "double"
    }
    columns {
      name = "sr_flag"
      type = "double"
    }
    columns {
      name = "affiliated_base_number"
      type = "string"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
}

# --- Ingest Jobs (1 por dataset) ---
resource "aws_glue_job" "ingest" {
  for_each = toset(["yellow_tripdata", "green_tripdata", "fhvhv_tripdata", "fhv_tripdata"])

  name     = "${var.project_name}-ingest-${split("_", each.key)[0]}"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/etl/ingest_to_bronze.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--TempDir"                         = "s3://${var.scripts_bucket}/tmp/"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TARGET_BUCKET"                   = var.bronze_bucket
    "--DATASET"                         = each.key
    "--DYNAMO_TABLE"                    = var.dynamo_table_name
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 480
  max_retries       = 0

  execution_property {
    max_concurrent_runs = 3
  }

  depends_on = [aws_s3_object.script_ingest]
}

# --- Retry Job ---
resource "aws_glue_job" "retry_failed" {
  name     = "${var.project_name}-retry-failed"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/etl/retry_failed.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--TempDir"                         = "s3://${var.scripts_bucket}/tmp/"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TARGET_BUCKET"                   = var.bronze_bucket
    "--DYNAMO_TABLE"                    = var.dynamo_table_name
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 480
  max_retries       = 0

  execution_property {
    max_concurrent_runs = 3
  }

  depends_on = [aws_s3_object.script_retry]
}

# --- Zones Lookup Job ---
resource "aws_glue_job" "ingest_zones" {
  name     = "${var.project_name}-ingest-zones"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/etl/ingest_zones.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--TempDir"                         = "s3://${var.scripts_bucket}/tmp/"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TARGET_BUCKET"                   = var.bronze_bucket
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60
  max_retries       = 0

  depends_on = [aws_s3_object.script_zones]
}

# --- Upload script Silver ---
resource "aws_s3_object" "script_silver" {
  bucket = var.scripts_bucket
  key    = "etl/bronze_to_silver.py"
  source = "${path.module}/../../scripts/etl/bronze_to_silver.py"
  etag   = filemd5("${path.module}/../../scripts/etl/bronze_to_silver.py")
}

# --- Silver Job (Iceberg) ---
resource "aws_glue_job" "bronze_to_silver" {
  name     = "${var.project_name}-bronze-to-silver"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/etl/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--TempDir"                         = "s3://${var.scripts_bucket}/tmp/"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"         = "true"
    "--datalake-formats"                = "iceberg"
    "--BRONZE_BUCKET"                   = var.bronze_bucket
    "--SILVER_BUCKET"                   = var.silver_bucket
    "--FULL_LOAD"                       = "false"
  }

  glue_version      = "4.0"
  number_of_workers = 5
  worker_type       = "G.2X"
  timeout           = 480
  max_retries       = 0

  execution_property {
    max_concurrent_runs = 3
  }

  depends_on = [aws_s3_object.script_silver]
}

# --- Upload script Gold ---
resource "aws_s3_object" "script_gold" {
  bucket = var.scripts_bucket
  key    = "etl/silver_to_gold.py"
  source = "${path.module}/../../scripts/etl/silver_to_gold.py"
  etag   = filemd5("${path.module}/../../scripts/etl/silver_to_gold.py")
}

# --- Gold Job (Iceberg - Modelo Dimensional) ---
resource "aws_glue_job" "silver_to_gold" {
  name     = "${var.project_name}-silver-to-gold"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/etl/silver_to_gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--TempDir"                         = "s3://${var.scripts_bucket}/tmp/"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"         = "true"
    "--datalake-formats"                = "iceberg"
    "--SILVER_BUCKET"                   = var.silver_bucket
    "--GOLD_BUCKET"                     = var.gold_bucket
    "--BRONZE_BUCKET"                   = var.bronze_bucket
  }

  glue_version      = "4.0"
  number_of_workers = 5
  worker_type       = "G.2X"
  timeout           = 480
  max_retries       = 0

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [aws_s3_object.script_gold]
}

# ============================================================
# TABELAS SILVER (placeholders — Spark recria como Iceberg no primeiro write)
# ============================================================
resource "aws_glue_catalog_table" "silver_yellow" {
  database_name = aws_glue_catalog_database.silver.name
  name          = "yellow"
  storage_descriptor {
    location = "s3://${var.silver_bucket}/yellow/"
    columns {
      name = "pickup_datetime"
      type = "timestamp"
    }
  }
  lifecycle { ignore_changes = all }
}

resource "aws_glue_catalog_table" "silver_green" {
  database_name = aws_glue_catalog_database.silver.name
  name          = "green"
  storage_descriptor {
    location = "s3://${var.silver_bucket}/green/"
    columns {
      name = "pickup_datetime"
      type = "timestamp"
    }
  }
  lifecycle { ignore_changes = all }
}

resource "aws_glue_catalog_table" "silver_fhvhv" {
  database_name = aws_glue_catalog_database.silver.name
  name          = "fhvhv"
  storage_descriptor {
    location = "s3://${var.silver_bucket}/fhvhv/"
    columns {
      name = "pickup_datetime"
      type = "timestamp"
    }
  }
  lifecycle { ignore_changes = all }
}

resource "aws_glue_catalog_table" "silver_fhv" {
  database_name = aws_glue_catalog_database.silver.name
  name          = "fhv"
  storage_descriptor {
    location = "s3://${var.silver_bucket}/fhv/"
    columns {
      name = "pickup_datetime"
      type = "timestamp"
    }
  }
  lifecycle { ignore_changes = all }
}

# ============================================================
# TABELAS GOLD (placeholders — Spark recria como Iceberg no primeiro write)
# ============================================================
resource "aws_glue_catalog_table" "gold_dim_tipo_transporte" {
  database_name = aws_glue_catalog_database.gold.name
  name          = "dim_tipo_transporte"
  storage_descriptor {
    location = "s3://${var.gold_bucket}/dim_tipo_transporte/"
    columns {
      name = "id_tipo_transporte"
      type = "int"
    }
  }
  lifecycle { ignore_changes = all }
}

resource "aws_glue_catalog_table" "gold_dim_empresa" {
  database_name = aws_glue_catalog_database.gold.name
  name          = "dim_empresa"
  storage_descriptor {
    location = "s3://${var.gold_bucket}/dim_empresa/"
    columns {
      name = "id_empresa"
      type = "int"
    }
  }
  lifecycle { ignore_changes = all }
}

resource "aws_glue_catalog_table" "gold_dim_local" {
  database_name = aws_glue_catalog_database.gold.name
  name          = "dim_local"
  storage_descriptor {
    location = "s3://${var.gold_bucket}/dim_local/"
    columns {
      name = "id_local"
      type = "int"
    }
  }
  lifecycle { ignore_changes = all }
}

resource "aws_glue_catalog_table" "gold_dim_distancia" {
  database_name = aws_glue_catalog_database.gold.name
  name          = "dim_distancia"
  storage_descriptor {
    location = "s3://${var.gold_bucket}/dim_distancia/"
    columns {
      name = "id_distancia"
      type = "int"
    }
  }
  lifecycle { ignore_changes = all }
}

resource "aws_glue_catalog_table" "gold_dim_tempo_horario" {
  database_name = aws_glue_catalog_database.gold.name
  name          = "dim_tempo_horario"
  storage_descriptor {
    location = "s3://${var.gold_bucket}/dim_tempo_horario/"
    columns {
      name = "id_faixa_horaria"
      type = "int"
    }
  }
  lifecycle { ignore_changes = all }
}

resource "aws_glue_catalog_table" "gold_dim_status_gorjeta" {
  database_name = aws_glue_catalog_database.gold.name
  name          = "dim_status_gorjeta"
  storage_descriptor {
    location = "s3://${var.gold_bucket}/dim_status_gorjeta/"
    columns {
      name = "id_status_gorjeta"
      type = "int"
    }
  }
  lifecycle { ignore_changes = all }
}

resource "aws_glue_catalog_table" "gold_dim_status_congestionamento" {
  database_name = aws_glue_catalog_database.gold.name
  name          = "dim_status_congestionamento"
  storage_descriptor {
    location = "s3://${var.gold_bucket}/dim_status_congestionamento/"
    columns {
      name = "id_status_congestionamento"
      type = "int"
    }
  }
  lifecycle { ignore_changes = all }
}

resource "aws_glue_catalog_table" "gold_fato_corrida" {
  database_name = aws_glue_catalog_database.gold.name
  name          = "fato_corrida"
  storage_descriptor {
    location = "s3://${var.gold_bucket}/fato_corrida/"
    columns {
      name = "id_corrida"
      type = "bigint"
    }
  }
  lifecycle { ignore_changes = all }
}

# --- Data Quality Rulesets (Silver) ---
resource "aws_glue_data_quality_ruleset" "silver" {
  for_each = {
    yellow = <<-DQDL
      Rules = [
        RowCount > 0,
        Completeness "pickup_datetime" >= 0.99,
        Completeness "dropoff_datetime" >= 0.99,
        Completeness "pu_location_id" >= 0.95,
        Completeness "do_location_id" >= 0.95,
        ColumnValues "fare_amount" >= -50.0,
        ColumnValues "trip_distance" >= 0.0,
        ColumnValues "tip_amount" >= 0.0,
        ColumnValues "total_amount" between -100.0 and 50000.0,
        ColumnValues "passenger_count" between 0 and 9
      ]
    DQDL
    green = <<-DQDL
      Rules = [
        RowCount > 0,
        Completeness "pickup_datetime" >= 0.99,
        Completeness "dropoff_datetime" >= 0.99,
        Completeness "pu_location_id" >= 0.95,
        Completeness "do_location_id" >= 0.95,
        ColumnValues "fare_amount" >= -50.0,
        ColumnValues "trip_distance" >= 0.0,
        ColumnValues "tip_amount" >= 0.0,
        ColumnValues "total_amount" between -100.0 and 50000.0
      ]
    DQDL
    fhvhv = <<-DQDL
      Rules = [
        RowCount > 0,
        Completeness "pickup_datetime" >= 0.99,
        Completeness "dropoff_datetime" >= 0.99,
        Completeness "pu_location_id" >= 0.95,
        Completeness "do_location_id" >= 0.95,
        ColumnValues "trip_distance" >= 0.0,
        ColumnValues "tip_amount" >= 0.0,
        ColumnValues "base_passenger_fare" >= -50.0,
        ColumnValues "trip_time" >= 0
      ]
    DQDL
    fhv = <<-DQDL
      Rules = [
        RowCount > 0,
        Completeness "pickup_datetime" >= 0.99,
        Completeness "dropoff_datetime" >= 0.99,
        Completeness "pu_location_id" >= 0.90,
        Completeness "do_location_id" >= 0.90
      ]
    DQDL
  }

  name    = "${var.project_name}-dq-silver-${each.key}"
  ruleset = each.value

  target_table {
    database_name = aws_glue_catalog_database.silver.name
    table_name    = each.key
  }

  depends_on = [
    aws_glue_catalog_table.silver_yellow,
    aws_glue_catalog_table.silver_green,
    aws_glue_catalog_table.silver_fhvhv,
    aws_glue_catalog_table.silver_fhv,
  ]
}
