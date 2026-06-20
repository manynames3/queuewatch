terraform {
  required_version = ">= 1.6.0"

  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

locals {
  raw_bucket_name = var.raw_bucket_name != "" ? var.raw_bucket_name : "queuewatch-raw-${data.aws_caller_identity.current.account_id}-${random_id.bucket_suffix.hex}"
  common_tags = merge(
    {
      Project     = "QueueWatch"
      ManagedBy   = "Terraform"
      Environment = var.environment
    },
    var.tags
  )
}

variable "aws_region" {
  description = "AWS region for the QueueWatch stack. The default Bedrock model uses a US geo inference profile."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "prod"
}

variable "raw_bucket_name" {
  description = "Optional globally unique bucket name for raw and parsed queue documents. Leave blank to generate one."
  type        = string
  default     = ""
}

variable "state_table_name" {
  description = "DynamoDB table name for queue source metadata and change-detection state."
  type        = string
  default     = "QueueWatch-State"
}

variable "insights_table_name" {
  description = "DynamoDB table name for extracted interconnection queue insights."
  type        = string
  default     = "QueueWatch-Insights"
}

variable "schedule_expression" {
  description = "EventBridge Scheduler expression for the orchestrator Lambda."
  type        = string
  default     = "cron(0 9 * * ? *)"
}

variable "schedule_timezone" {
  description = "Timezone used by EventBridge Scheduler."
  type        = string
  default     = "UTC"
}

variable "bedrock_model_id" {
  description = "Bedrock model ID or inference profile ID used by the parser Lambda."
  type        = string
  default     = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
}

variable "bedrock_policy_resource_arns" {
  description = "Bedrock resources the parser can invoke. Default wildcard supports foundation model IDs and regional/geo inference profile IDs."
  type        = list(string)
  default     = ["*"]
}

variable "tags" {
  description = "Additional tags applied to resources."
  type        = map(string)
  default     = {}
}

variable "alarm_email" {
  description = "Optional email address for CloudWatch alarm notifications. Leave blank to create alarms without an email subscription."
  type        = string
  default     = ""
}

variable "alarm_period_seconds" {
  description = "CloudWatch alarm metric period in seconds."
  type        = number
  default     = 300
}

variable "alarm_evaluation_periods" {
  description = "Number of CloudWatch periods to evaluate before alarming."
  type        = number
  default     = 1
}

variable "lambda_error_alarm_threshold" {
  description = "Lambda error count threshold that triggers an alarm."
  type        = number
  default     = 1
}

variable "dlq_message_alarm_threshold" {
  description = "Visible DLQ message count threshold that triggers an alarm."
  type        = number
  default     = 1
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention period for Lambda log groups."
  type        = number
  default     = 30
}

variable "enable_textract_fallback" {
  description = "Enable Amazon Textract fallback when PDF text extraction is poor or unsupported."
  type        = bool
  default     = true
}

variable "textract_min_pdf_text_chars" {
  description = "Minimum locally extracted PDF text length before skipping Textract fallback."
  type        = number
  default     = 250
}

variable "textract_max_wait_seconds" {
  description = "Maximum time the parser Lambda waits for an async Textract job."
  type        = number
  default     = 240
}

variable "textract_poll_seconds" {
  description = "Polling interval for async Textract jobs."
  type        = number
  default     = 5
}

variable "report_schedule_expression" {
  description = "EventBridge Scheduler expression for the daily buyer-facing report."
  type        = string
  default     = "cron(30 10 * * ? *)"
}

variable "report_recipient_emails" {
  description = "Comma-separated email recipients for QueueWatch daily reports. Leave blank to only write S3 report artifacts."
  type        = string
  default     = ""
}

variable "report_sender_email" {
  description = "Verified SES sender email for daily reports. Leave blank to disable email delivery."
  type        = string
  default     = ""
}

variable "lead_allowed_origin" {
  description = "Allowed CORS origin for QueueWatch pilot lead capture."
  type        = string
  default     = "https://queuewatch.pages.dev"
}

variable "lead_notification_email" {
  description = "Optional email address notified when a new pilot lead is captured. Requires lead_sender_email to be verified in SES."
  type        = string
  default     = ""
}

variable "lead_sender_email" {
  description = "Verified SES sender email for pilot lead notifications."
  type        = string
  default     = ""
}

resource "aws_s3_bucket" "raw_documents" {
  bucket = local.raw_bucket_name
  tags   = local.common_tags
}

resource "aws_s3_bucket_public_access_block" "raw_documents" {
  bucket                  = aws_s3_bucket.raw_documents.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_documents" {
  bucket = aws_s3_bucket.raw_documents.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "raw_documents" {
  bucket = aws_s3_bucket.raw_documents.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_dynamodb_table" "state" {
  name         = var.state_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "QueueID"

  attribute {
    name = "QueueID"
    type = "S"
  }

  server_side_encryption {
    enabled = true
  }

  tags = local.common_tags
}

resource "aws_dynamodb_table" "insights" {
  name         = var.insights_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "QueueID"
  range_key    = "ObservedAt"

  attribute {
    name = "QueueID"
    type = "S"
  }

  attribute {
    name = "ObservedAt"
    type = "S"
  }

  server_side_encryption {
    enabled = true
  }

  tags = local.common_tags
}

resource "aws_dynamodb_table" "leads" {
  name         = "QueueWatch-Leads"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LeadID"

  attribute {
    name = "LeadID"
    type = "S"
  }

  server_side_encryption {
    enabled = true
  }

  tags = local.common_tags
}

resource "aws_sqs_queue" "orchestrator_dlq" {
  name                      = "queuewatch-orchestrator-dlq"
  message_retention_seconds = 1209600
  receive_wait_time_seconds = 20
  sqs_managed_sse_enabled   = true
  tags                      = local.common_tags
}

resource "aws_sqs_queue" "parser_dlq" {
  name                      = "queuewatch-parser-dlq"
  message_retention_seconds = 1209600
  receive_wait_time_seconds = 20
  sqs_managed_sse_enabled   = true
  tags                      = local.common_tags
}

resource "aws_sqs_queue" "reporter_dlq" {
  name                      = "queuewatch-reporter-dlq"
  message_retention_seconds = 1209600
  receive_wait_time_seconds = 20
  sqs_managed_sse_enabled   = true
  tags                      = local.common_tags
}

resource "aws_sns_topic" "alerts" {
  name = "queuewatch-alerts"
  tags = local.common_tags
}

resource "aws_sns_topic_subscription" "alerts_email" {
  count     = var.alarm_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

data "archive_file" "orchestrator_zip" {
  type        = "zip"
  source_file = "${path.module}/orchestrator.py"
  output_path = "${path.module}/orchestrator.zip"
}

data "archive_file" "parser_zip" {
  type        = "zip"
  source_file = "${path.module}/parser.py"
  output_path = "${path.module}/parser.zip"
}

data "archive_file" "reporter_zip" {
  type        = "zip"
  source_file = "${path.module}/reporter.py"
  output_path = "${path.module}/reporter.zip"
}

data "archive_file" "lead_capture_zip" {
  type        = "zip"
  source_file = "${path.module}/lead_capture.py"
  output_path = "${path.module}/lead_capture.zip"
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "orchestrator" {
  name               = "queuewatch-orchestrator-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = local.common_tags
}

resource "aws_iam_role" "parser" {
  name               = "queuewatch-parser-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = local.common_tags
}

resource "aws_iam_role" "reporter" {
  name               = "queuewatch-reporter-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = local.common_tags
}

resource "aws_iam_role" "lead_capture" {
  name               = "queuewatch-lead-capture-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "orchestrator_basic_logs" {
  role       = aws_iam_role.orchestrator.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "parser_basic_logs" {
  role       = aws_iam_role.parser.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "reporter_basic_logs" {
  role       = aws_iam_role.reporter.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lead_capture_basic_logs" {
  role       = aws_iam_role.lead_capture.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "orchestrator" {
  statement {
    sid = "ReadAndUpdateQueueState"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:Scan",
      "dynamodb:UpdateItem"
    ]
    resources = [aws_dynamodb_table.state.arn]
  }

  statement {
    sid = "WriteRawDocuments"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject"
    ]
    resources = [
      aws_s3_bucket.raw_documents.arn,
      "${aws_s3_bucket.raw_documents.arn}/*"
    ]
  }

  statement {
    sid       = "InvokeParserLambda"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.parser.arn]
  }

  statement {
    sid       = "SendToOrchestratorDlq"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.orchestrator_dlq.arn]
  }
}

resource "aws_iam_policy" "orchestrator" {
  name   = "queuewatch-orchestrator-policy"
  policy = data.aws_iam_policy_document.orchestrator.json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "orchestrator" {
  role       = aws_iam_role.orchestrator.name
  policy_arn = aws_iam_policy.orchestrator.arn
}

data "aws_iam_policy_document" "parser" {
  statement {
    sid = "ReadRawAndWriteInsights"
    actions = [
      "s3:GetObject",
      "s3:PutObject"
    ]
    resources = ["${aws_s3_bucket.raw_documents.arn}/*"]
  }

  statement {
    sid = "WriteParsedInsights"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem"
    ]
    resources = [
      aws_dynamodb_table.insights.arn,
      aws_dynamodb_table.state.arn
    ]
  }

  statement {
    sid       = "InvokeBedrockModel"
    actions   = ["bedrock:InvokeModel"]
    resources = var.bedrock_policy_resource_arns
  }

  statement {
    sid = "ExtractPdfTextWithTextract"
    actions = [
      "textract:DetectDocumentText",
      "textract:GetDocumentTextDetection",
      "textract:StartDocumentTextDetection"
    ]
    resources = ["*"]
  }

  statement {
    sid       = "SendToParserDlq"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.parser_dlq.arn]
  }
}

resource "aws_iam_policy" "parser" {
  name   = "queuewatch-parser-policy"
  policy = data.aws_iam_policy_document.parser.json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "parser" {
  role       = aws_iam_role.parser.name
  policy_arn = aws_iam_policy.parser.arn
}

data "aws_iam_policy_document" "reporter" {
  statement {
    sid       = "ReadInsights"
    actions   = ["dynamodb:Scan"]
    resources = [aws_dynamodb_table.insights.arn]
  }

  statement {
    sid       = "WriteReports"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.raw_documents.arn}/*"]
  }

  statement {
    sid       = "SendReportEmail"
    actions   = ["ses:SendEmail", "ses:SendRawEmail"]
    resources = ["*"]
  }

  statement {
    sid       = "SendToReporterDlq"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.reporter_dlq.arn]
  }
}

resource "aws_iam_policy" "reporter" {
  name   = "queuewatch-reporter-policy"
  policy = data.aws_iam_policy_document.reporter.json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "reporter" {
  role       = aws_iam_role.reporter.name
  policy_arn = aws_iam_policy.reporter.arn
}

data "aws_iam_policy_document" "lead_capture" {
  statement {
    sid       = "WriteLeads"
    actions   = ["dynamodb:PutItem"]
    resources = [aws_dynamodb_table.leads.arn]
  }

  statement {
    sid       = "SendLeadNotificationEmail"
    actions   = ["ses:SendEmail", "ses:SendRawEmail"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "lead_capture" {
  name   = "queuewatch-lead-capture-policy"
  policy = data.aws_iam_policy_document.lead_capture.json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "lead_capture" {
  role       = aws_iam_role.lead_capture.name
  policy_arn = aws_iam_policy.lead_capture.arn
}

resource "aws_cloudwatch_log_group" "orchestrator" {
  name              = "/aws/lambda/queuewatch-orchestrator"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "parser" {
  name              = "/aws/lambda/queuewatch-parser"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "reporter" {
  name              = "/aws/lambda/queuewatch-reporter"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "lead_capture" {
  name              = "/aws/lambda/queuewatch-lead-capture"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

resource "aws_lambda_function" "orchestrator" {
  function_name    = "queuewatch-orchestrator"
  description      = "QueueWatch daily source scanner and remote change detector."
  role             = aws_iam_role.orchestrator.arn
  runtime          = "python3.11"
  handler          = "orchestrator.lambda_handler"
  filename         = data.archive_file.orchestrator_zip.output_path
  source_code_hash = data.archive_file.orchestrator_zip.output_base64sha256
  memory_size      = 256
  timeout          = 180

  dead_letter_config {
    target_arn = aws_sqs_queue.orchestrator_dlq.arn
  }

  environment {
    variables = {
      STATE_TABLE_NAME      = aws_dynamodb_table.state.name
      RAW_BUCKET_NAME       = aws_s3_bucket.raw_documents.bucket
      PARSER_FUNCTION_NAME  = aws_lambda_function.parser.function_name
      HTTP_TIMEOUT_SECONDS  = "20"
      MAX_LIGHTWEIGHT_BYTES = "1048576"
      MAX_DOWNLOAD_BYTES    = "52428800"
      QUEUEWATCH_USER_AGENT = "QueueWatch/1.0 (+https://example.com/contact)"
    }
  }

  depends_on = [aws_cloudwatch_log_group.orchestrator]

  tags = local.common_tags
}

resource "aws_lambda_function" "parser" {
  function_name    = "queuewatch-parser"
  description      = "QueueWatch Bedrock parser for changed interconnection queue documents."
  role             = aws_iam_role.parser.arn
  runtime          = "python3.11"
  handler          = "parser.lambda_handler"
  filename         = data.archive_file.parser_zip.output_path
  source_code_hash = data.archive_file.parser_zip.output_base64sha256
  memory_size      = 512
  timeout          = 300

  dead_letter_config {
    target_arn = aws_sqs_queue.parser_dlq.arn
  }

  environment {
    variables = {
      STATE_TABLE_NAME                = aws_dynamodb_table.state.name
      INSIGHTS_TABLE_NAME             = aws_dynamodb_table.insights.name
      BEDROCK_MODEL_ID                = var.bedrock_model_id
      OUTPUT_PREFIX                   = "insights/"
      PROJECT_SNAPSHOT_PREFIX         = "project-snapshots/"
      MAX_SOURCE_BYTES                = "52428800"
      MAX_DOCUMENT_CHARS              = "120000"
      MAX_PROJECT_RECORDS             = "12000"
      MAX_PROJECT_INSIGHTS_PER_SOURCE = "750"
      BASELINE_SAMPLE_LIMIT           = "25"
      BEDROCK_MAX_TOKENS              = "1200"
      ENABLE_TEXTRACT_FALLBACK        = tostring(var.enable_textract_fallback)
      TEXTRACT_MIN_PDF_TEXT_CHARS     = tostring(var.textract_min_pdf_text_chars)
      TEXTRACT_MAX_WAIT_SECONDS       = tostring(var.textract_max_wait_seconds)
      TEXTRACT_POLL_SECONDS           = tostring(var.textract_poll_seconds)
    }
  }

  depends_on = [aws_cloudwatch_log_group.parser]

  tags = local.common_tags
}

resource "aws_lambda_function" "reporter" {
  function_name    = "queuewatch-reporter"
  description      = "QueueWatch daily buyer-facing report generator and optional email sender."
  role             = aws_iam_role.reporter.arn
  runtime          = "python3.11"
  handler          = "reporter.lambda_handler"
  filename         = data.archive_file.reporter_zip.output_path
  source_code_hash = data.archive_file.reporter_zip.output_base64sha256
  memory_size      = 256
  timeout          = 180

  dead_letter_config {
    target_arn = aws_sqs_queue.reporter_dlq.arn
  }

  environment {
    variables = merge(
      {
        INSIGHTS_TABLE_NAME   = aws_dynamodb_table.insights.name
        REPORT_BUCKET_NAME    = aws_s3_bucket.raw_documents.bucket
        REPORT_PREFIX         = "reports/"
        REPORT_LOOKBACK_HOURS = "24"
        PRODUCT_URL           = "https://queuewatch.pages.dev"
      },
      var.report_recipient_emails != null && var.report_recipient_emails != "" ? {
        REPORT_RECIPIENT_EMAILS = var.report_recipient_emails
      } : {},
      var.report_sender_email != null && var.report_sender_email != "" ? {
        REPORT_SENDER_EMAIL = var.report_sender_email
      } : {}
    )
  }

  depends_on = [aws_cloudwatch_log_group.reporter]

  tags = local.common_tags
}

resource "aws_lambda_function" "lead_capture" {
  function_name    = "queuewatch-lead-capture"
  description      = "QueueWatch pilot lead capture endpoint for the Cloudflare Pages landing site."
  role             = aws_iam_role.lead_capture.arn
  runtime          = "python3.11"
  handler          = "lead_capture.lambda_handler"
  filename         = data.archive_file.lead_capture_zip.output_path
  source_code_hash = data.archive_file.lead_capture_zip.output_base64sha256
  memory_size      = 128
  timeout          = 15

  environment {
    variables = merge(
      {
        LEADS_TABLE_NAME = aws_dynamodb_table.leads.name
        ALLOWED_ORIGIN   = var.lead_allowed_origin
      },
      var.lead_notification_email != null && var.lead_notification_email != "" ? {
        LEAD_NOTIFICATION_EMAIL = var.lead_notification_email
      } : {},
      var.lead_sender_email != null && var.lead_sender_email != "" ? {
        LEAD_SENDER_EMAIL = var.lead_sender_email
      } : {}
    )
  }

  depends_on = [aws_cloudwatch_log_group.lead_capture]

  tags = local.common_tags
}

data "aws_iam_policy_document" "scheduler_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "queuewatch-scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "scheduler" {
  statement {
    sid     = "InvokeScheduledLambdas"
    actions = ["lambda:InvokeFunction"]
    resources = [
      aws_lambda_function.orchestrator.arn,
      aws_lambda_function.reporter.arn
    ]
  }

  statement {
    sid     = "SendSchedulerFailuresToDlq"
    actions = ["sqs:SendMessage"]
    resources = [
      aws_sqs_queue.orchestrator_dlq.arn,
      aws_sqs_queue.reporter_dlq.arn
    ]
  }
}

resource "aws_iam_policy" "scheduler" {
  name   = "queuewatch-scheduler-policy"
  policy = data.aws_iam_policy_document.scheduler.json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "scheduler" {
  role       = aws_iam_role.scheduler.name
  policy_arn = aws_iam_policy.scheduler.arn
}

resource "aws_scheduler_schedule" "daily_orchestrator" {
  name                         = "queuewatch-daily-orchestrator"
  description                  = "Daily QueueWatch change detection scan."
  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = var.schedule_timezone

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.orchestrator.arn
    role_arn = aws_iam_role.scheduler.arn

    dead_letter_config {
      arn = aws_sqs_queue.orchestrator_dlq.arn
    }

    retry_policy {
      maximum_event_age_in_seconds = 3600
      maximum_retry_attempts       = 2
    }
  }
}

resource "aws_scheduler_schedule" "daily_report" {
  name                         = "queuewatch-daily-report"
  description                  = "Daily QueueWatch buyer-facing report generation."
  schedule_expression          = var.report_schedule_expression
  schedule_expression_timezone = var.schedule_timezone

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.reporter.arn
    role_arn = aws_iam_role.scheduler.arn

    dead_letter_config {
      arn = aws_sqs_queue.reporter_dlq.arn
    }

    retry_policy {
      maximum_event_age_in_seconds = 3600
      maximum_retry_attempts       = 2
    }
  }
}

resource "aws_lambda_permission" "allow_scheduler" {
  statement_id  = "AllowExecutionFromEventBridgeScheduler"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.orchestrator.function_name
  principal     = "scheduler.amazonaws.com"
  source_arn    = aws_scheduler_schedule.daily_orchestrator.arn
}

resource "aws_lambda_permission" "allow_report_scheduler" {
  statement_id  = "AllowReportExecutionFromEventBridgeScheduler"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.reporter.function_name
  principal     = "scheduler.amazonaws.com"
  source_arn    = aws_scheduler_schedule.daily_report.arn
}

resource "aws_apigatewayv2_api" "lead_capture" {
  name          = "queuewatch-lead-capture-api"
  protocol_type = "HTTP"

  cors_configuration {
    allow_headers = ["content-type"]
    allow_methods = ["OPTIONS", "POST"]
    allow_origins = [var.lead_allowed_origin]
    max_age       = 3600
  }

  tags = local.common_tags
}

resource "aws_apigatewayv2_integration" "lead_capture" {
  api_id                 = aws_apigatewayv2_api.lead_capture.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.lead_capture.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "lead_capture" {
  api_id    = aws_apigatewayv2_api.lead_capture.id
  route_key = "POST /pilot"
  target    = "integrations/${aws_apigatewayv2_integration.lead_capture.id}"
}

resource "aws_apigatewayv2_stage" "lead_capture" {
  api_id      = aws_apigatewayv2_api.lead_capture.id
  name        = "$default"
  auto_deploy = true

  tags = local.common_tags
}

resource "aws_lambda_permission" "allow_lead_api" {
  statement_id  = "AllowExecutionFromLeadApi"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lead_capture.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.lead_capture.execution_arn}/*/*"
}

resource "aws_cloudwatch_metric_alarm" "orchestrator_errors" {
  alarm_name          = "queuewatch-orchestrator-errors"
  alarm_description   = "QueueWatch orchestrator Lambda reported errors."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = var.alarm_evaluation_periods
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = var.alarm_period_seconds
  statistic           = "Sum"
  threshold           = var.lambda_error_alarm_threshold
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = local.common_tags

  dimensions = {
    FunctionName = aws_lambda_function.orchestrator.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "parser_errors" {
  alarm_name          = "queuewatch-parser-errors"
  alarm_description   = "QueueWatch parser Lambda reported errors."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = var.alarm_evaluation_periods
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = var.alarm_period_seconds
  statistic           = "Sum"
  threshold           = var.lambda_error_alarm_threshold
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = local.common_tags

  dimensions = {
    FunctionName = aws_lambda_function.parser.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "reporter_errors" {
  alarm_name          = "queuewatch-reporter-errors"
  alarm_description   = "QueueWatch reporter Lambda reported errors."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = var.alarm_evaluation_periods
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = var.alarm_period_seconds
  statistic           = "Sum"
  threshold           = var.lambda_error_alarm_threshold
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = local.common_tags

  dimensions = {
    FunctionName = aws_lambda_function.reporter.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "lead_capture_errors" {
  alarm_name          = "queuewatch-lead-capture-errors"
  alarm_description   = "QueueWatch lead capture Lambda reported errors."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = var.alarm_evaluation_periods
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = var.alarm_period_seconds
  statistic           = "Sum"
  threshold           = var.lambda_error_alarm_threshold
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = local.common_tags

  dimensions = {
    FunctionName = aws_lambda_function.lead_capture.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "orchestrator_dlq_messages" {
  alarm_name          = "queuewatch-orchestrator-dlq-visible-messages"
  alarm_description   = "QueueWatch orchestrator DLQ has visible messages."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = var.alarm_evaluation_periods
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = var.alarm_period_seconds
  statistic           = "Maximum"
  threshold           = var.dlq_message_alarm_threshold
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = local.common_tags

  dimensions = {
    QueueName = aws_sqs_queue.orchestrator_dlq.name
  }
}

resource "aws_cloudwatch_metric_alarm" "parser_dlq_messages" {
  alarm_name          = "queuewatch-parser-dlq-visible-messages"
  alarm_description   = "QueueWatch parser DLQ has visible messages."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = var.alarm_evaluation_periods
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = var.alarm_period_seconds
  statistic           = "Maximum"
  threshold           = var.dlq_message_alarm_threshold
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = local.common_tags

  dimensions = {
    QueueName = aws_sqs_queue.parser_dlq.name
  }
}

resource "aws_cloudwatch_metric_alarm" "reporter_dlq_messages" {
  alarm_name          = "queuewatch-reporter-dlq-visible-messages"
  alarm_description   = "QueueWatch reporter DLQ has visible messages."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = var.alarm_evaluation_periods
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = var.alarm_period_seconds
  statistic           = "Maximum"
  threshold           = var.dlq_message_alarm_threshold
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = local.common_tags

  dimensions = {
    QueueName = aws_sqs_queue.reporter_dlq.name
  }
}

output "raw_bucket_name" {
  value       = aws_s3_bucket.raw_documents.bucket
  description = "S3 bucket storing raw queue documents and parser output JSON."
}

output "state_table_name" {
  value       = aws_dynamodb_table.state.name
  description = "DynamoDB state table."
}

output "insights_table_name" {
  value       = aws_dynamodb_table.insights.name
  description = "DynamoDB insights table."
}

output "orchestrator_lambda_name" {
  value       = aws_lambda_function.orchestrator.function_name
  description = "Daily change-detection Lambda function name."
}

output "parser_lambda_name" {
  value       = aws_lambda_function.parser.function_name
  description = "Asynchronous parser Lambda function name."
}

output "reporter_lambda_name" {
  value       = aws_lambda_function.reporter.function_name
  description = "Daily report Lambda function name."
}

output "lead_capture_url" {
  value       = "${aws_apigatewayv2_api.lead_capture.api_endpoint}/pilot"
  description = "HTTP API endpoint used by the landing page pilot form."
}

output "alerts_topic_arn" {
  value       = aws_sns_topic.alerts.arn
  description = "SNS topic used by QueueWatch CloudWatch alarms."
}

output "orchestrator_dlq_url" {
  value       = aws_sqs_queue.orchestrator_dlq.url
  description = "SQS DLQ for orchestrator scheduler/Lambda failures."
}

output "parser_dlq_url" {
  value       = aws_sqs_queue.parser_dlq.url
  description = "SQS DLQ for parser asynchronous invocation failures."
}

output "reporter_dlq_url" {
  value       = aws_sqs_queue.reporter_dlq.url
  description = "SQS DLQ for reporter scheduler/Lambda failures."
}
