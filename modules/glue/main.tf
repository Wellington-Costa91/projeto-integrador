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

# --- Crawler Bronze ---
resource "aws_glue_crawler" "bronze" {
  name          = "${var.project_name}-bronze-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.bronze.name
  s3_target { path = "s3://${var.bronze_bucket}/" }
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
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

# --- Data Quality Rulesets (Silver) ---
# NOTA: As tabelas silver são criadas pelo job bronze_to_silver (Iceberg).
# Os rulesets só podem ser aplicados após a primeira execução do job.
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
}
