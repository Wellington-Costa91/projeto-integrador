resource "aws_dynamodb_table" "failed_downloads" {
  name         = "${var.project_name}-failed-downloads"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "url"

  attribute {
    name = "url"
    type = "S"
  }

  tags = { Project = var.project_name }
}
