# --- IAM Role ---
resource "aws_iam_role" "this" {
  name = "${var.project_name}-sfn-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

data "aws_caller_identity" "current" {}

resource "aws_iam_role_policy" "this" {
  name = "sfn-permissions"
  role = aws_iam_role.this.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["glue:StartCrawler", "glue:GetCrawler"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["glue:StartDataQualityRulesetEvaluationRun", "glue:GetDataQualityRulesetEvaluationRun"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetTable", "glue:GetPartitions", "glue:GetDatabase"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.project_name}-glue-role"
      },
      {
        Effect   = "Allow"
        Action   = ["states:StartExecution", "states:DescribeExecution", "states:StopExecution"]
        Resource = aws_sfn_state_machine.transform.arn
      },
      {
        Effect   = "Allow"
        Action   = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
        Resource = "*"
      }
    ]
  })
}

# --- State Machine 1: Ingestao ---
resource "aws_sfn_state_machine" "ingest" {
  name     = "${var.project_name}-ingest-orchestrator"
  role_arn = aws_iam_role.this.arn

  definition = jsonencode({
    Comment = "Ingestao paralela + Retry + Crawler Bronze + chama Transformacao"
    StartAt = "IngestAll"
    States = {
      IngestAll = {
        Type = "Parallel"
        Branches = [
          for name in values(var.glue_job_names) : {
            StartAt = "Run-${name}"
            States = {
              "Run-${name}" = {
                Type     = "Task"
                Resource = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = { JobName = name }
                End = true
              }
            }
          }
        ]
        Next = "RetryFailed"
      }
      RetryFailed = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = { JobName = var.retry_job_name }
        Next = "RunCrawlerBronze"
      }
      RunCrawlerBronze = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = { Name = var.bronze_crawler_name }
        Next = "WaitCrawlerBronze"
      }
      WaitCrawlerBronze = {
        Type    = "Wait"
        Seconds = 30
        Next    = "CheckCrawlerBronze"
      }
      CheckCrawlerBronze = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = { Name = var.bronze_crawler_name }
        Next = "CrawlerBronzeDone?"
      }
      "CrawlerBronzeDone?" = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.Crawler.State"
          StringEquals = "READY"
          Next         = "StartTransform"
        }]
        Default = "WaitCrawlerBronze"
      }
      StartTransform = {
        Type     = "Task"
        Resource = "arn:aws:states:::states:startExecution.sync:2"
        Parameters = {
          StateMachineArn = aws_sfn_state_machine.transform.arn
          Input           = {}
        }
        End = true
      }
    }
  })
}

# --- State Machine 2: Transformacao Iceberg + Data Quality ---
resource "aws_sfn_state_machine" "transform" {
  name     = "${var.project_name}-transform-orchestrator"
  role_arn = aws_iam_role.this.arn

  definition = jsonencode({
    Comment = "Bronze -> Silver (Iceberg) + Data Quality"
    StartAt = "BronzeToSilver"
    States = {
      BronzeToSilver = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = { JobName = var.silver_job_name }
        Next = "DataQualityChecks"
      }
      DataQualityChecks = {
        Type = "Parallel"
        Branches = [
          for dataset, ruleset_name in var.dq_ruleset_names : {
            StartAt = "DQ-${dataset}"
            States = {
              "DQ-${dataset}" = {
                Type     = "Task"
                Resource = "arn:aws:states:::aws-sdk:glue:startDataQualityRulesetEvaluationRun"
                Parameters = {
                  DataSource = {
                    GlueTable = {
                      DatabaseName = var.silver_database
                      TableName    = dataset
                    }
                  }
                  RulesetNames = [ruleset_name]
                  Role         = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.project_name}-glue-role"
                }
                Next = "WaitDQ-${dataset}"
              }
              "WaitDQ-${dataset}" = {
                Type    = "Wait"
                Seconds = 30
                Next    = "CheckDQ-${dataset}"
              }
              "CheckDQ-${dataset}" = {
                Type     = "Task"
                Resource = "arn:aws:states:::aws-sdk:glue:getDataQualityRulesetEvaluationRun"
                Parameters = {
                  "RunId.$" = "$.RunId"
                }
                Next = "DQDone-${dataset}?"
              }
              "DQDone-${dataset}?" = {
                Type = "Choice"
                Choices = [
                  {
                    Variable     = "$.Status"
                    StringEquals = "SUCCEEDED"
                    Next         = "DQComplete-${dataset}"
                  },
                  {
                    Variable     = "$.Status"
                    StringEquals = "FAILED"
                    Next         = "DQComplete-${dataset}"
                  },
                  {
                    Variable     = "$.Status"
                    StringEquals = "ERROR"
                    Next         = "DQComplete-${dataset}"
                  }
                ]
                Default = "WaitDQ-${dataset}"
              }
              "DQComplete-${dataset}" = {
                Type = "Pass"
                End  = true
              }
            }
          }
        ]
        Next = "TransformSuccess"
      }
      TransformSuccess = {
        Type = "Succeed"
      }
    }
  })
}
