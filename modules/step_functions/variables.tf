variable "project_name" { type = string }
variable "glue_job_names" {
  type = map(string)
}
variable "retry_job_name"      { type = string }
variable "silver_job_name"     { type = string }
variable "bronze_crawler_name" { type = string }
variable "dq_ruleset_names"    { type = map(string) }
variable "silver_database"     { type = string }
