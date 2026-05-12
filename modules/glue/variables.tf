variable "project_name"     { type = string }
variable "s3_bucket_arns"   { type = list(string) }
variable "bronze_bucket"    { type = string }
variable "silver_bucket"    { type = string }
variable "scripts_bucket"   { type = string }
variable "dynamo_table_name" { type = string }
variable "dynamo_table_arn"  { type = string }
variable "gold_bucket"      { type = string }