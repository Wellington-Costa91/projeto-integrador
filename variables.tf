variable "aws_region" {
  default = "us-east-1"
}

variable "project_name" {
  default = "nyc-taxi-pipeline"
}

variable "account_id" {
  description = "AWS Account ID — informe via -var, terraform.tfvars (não versionado) ou data.aws_caller_identity.current.account_id"
  type        = string
}