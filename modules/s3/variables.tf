variable "project_name" { type = string }
variable "account_id" { type = string }
variable "bucket_names" {
  type    = list(string)
  default = ["bronze", "silver", "gold", "scripts", "athena-results"]
}
