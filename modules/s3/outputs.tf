output "buckets" {
  value = { for k, v in aws_s3_bucket.this : k => { id = v.id, arn = v.arn, bucket = v.bucket } }
}
