output "database_names" {
  value = {
    bronze = aws_glue_catalog_database.bronze.name
    silver = aws_glue_catalog_database.silver.name
    gold   = aws_glue_catalog_database.gold.name
  }
}

output "role_arn" { value = aws_iam_role.glue.arn }

output "job_names" {
  value = { for k, v in aws_glue_job.ingest : k => v.name }
}

output "retry_job_name" {
  value = aws_glue_job.retry_failed.name
}

output "silver_job_name" {
  value = aws_glue_job.bronze_to_silver.name
}

output "crawler_names" {
  value = {
    bronze = aws_glue_crawler.bronze.name
  }
}

output "dq_ruleset_names" {
  value = { for k, v in aws_glue_data_quality_ruleset.silver : k => v.name }
}
