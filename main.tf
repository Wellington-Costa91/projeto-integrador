locals {
  s3_bucket_arns = [for b in module.s3.buckets : b.arn]
}

# ---- S3 ----
module "s3" {
  source       = "./modules/s3"
  project_name = var.project_name
  account_id   = var.account_id
}

# ---- DynamoDB ----
module "dynamodb" {
  source       = "./modules/dynamodb"
  project_name = var.project_name
}

# ---- Glue (Ingestão + Retry + Silver) ----
module "glue" {
  source           = "./modules/glue"
  project_name     = var.project_name
  s3_bucket_arns   = local.s3_bucket_arns
  bronze_bucket    = module.s3.buckets["bronze"].bucket
  silver_bucket    = module.s3.buckets["silver"].bucket
  scripts_bucket   = module.s3.buckets["scripts"].bucket
  dynamo_table_name = module.dynamodb.table_name
  dynamo_table_arn  = module.dynamodb.table_arn
}

# ---- Step Functions (Orquestração) ----
module "step_functions" {
  source              = "./modules/step_functions"
  project_name        = var.project_name
  glue_job_names      = module.glue.job_names
  retry_job_name      = module.glue.retry_job_name
  silver_job_name     = module.glue.silver_job_name
  bronze_crawler_name = module.glue.crawler_names["bronze"]
  dq_ruleset_names    = module.glue.dq_ruleset_names
  silver_database     = module.glue.database_names["silver"]
}
