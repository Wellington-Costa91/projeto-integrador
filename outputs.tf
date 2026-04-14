output "s3_buckets" {
  value = { for k, v in module.s3.buckets : k => v.bucket }
}

output "glue_databases" {
  value = module.glue.database_names
}

output "glue_jobs" {
  value = module.glue.job_names
}

output "glue_retry_job" {
  value = module.glue.retry_job_name
}

output "dynamodb_table" {
  value = module.dynamodb.table_name
}

output "step_function_ingest_arn" {
  value = module.step_functions.ingest_state_machine_arn
}

output "step_function_transform_arn" {
  value = module.step_functions.transform_state_machine_arn
}
