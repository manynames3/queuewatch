# QueueWatch

Serverless change detection and LLM extraction for public utility interconnection queues.

## Architecture

- EventBridge Scheduler invokes `queuewatch-orchestrator` daily.
- The orchestrator scans active targets in `QueueWatch-State`, fingerprints each URL with `HEAD` or bounded `GET`, downloads changed documents to S3, and asynchronously invokes `queuewatch-parser`.
- The parser deterministically normalizes XLSX, CSV, XML, and HTML queue tables into project rows, snapshots each source to S3 for row-level delta comparison, falls back to Textract/Bedrock for PDF or unstructured inputs, and writes structured output to `QueueWatch-Insights` plus `s3://<bucket>/insights/`.
- The reporter creates daily buyer-facing HTML, CSV, and JSON project-delta reports under `s3://<bucket>/reports/` and can email them with SES when sender and recipients are configured.
- The landing page posts pilot requests to a serverless lead-capture API backed by `QueueWatch-Leads`.
- SQS DLQs capture scheduler/Lambda async failures.
- CloudWatch alarms publish to the `queuewatch-alerts` SNS topic.

## Deploy

```bash
terraform init
terraform plan
terraform apply
```

Optional email alarm subscription:

```bash
terraform apply -var='alarm_email=you@example.com'
```

Optional report email delivery and lead notifications require verified SES sender identities:

```bash
terraform apply \
  -var='report_sender_email=reports@example.com' \
  -var='report_recipient_emails=buyer@example.com,analyst@example.com' \
  -var='lead_sender_email=leads@example.com' \
  -var='lead_notification_email=founder@example.com'
```

If you use a different Bedrock region/model, override the model ID:

```bash
terraform apply \
  -var='aws_region=us-east-1' \
  -var='bedrock_model_id=us.anthropic.claude-haiku-4-5-20251001-v1:0'
```

## Seed Queue Targets

Seed a CSV or JSON file:

```bash
python3 seed_queues.py --file queues.example.csv --region us-east-1
```

Seed the first production source catalog:

```bash
python3 seed_queues.py --file queues.production.csv --region us-east-1
```

Seed one target:

```bash
python3 seed_queues.py \
  --queue-id dominion-nova \
  --source-url 'https://utility.example/interconnection-queue.xlsx' \
  --utility-name 'Dominion Energy' \
  --region us-east-1
```

The seeder updates target fields without deleting runtime fields such as hashes, S3 object keys, parser status, or previous errors.

Production source coverage is documented in `docs/source-coverage.md`. The current catalog monitors official CAISO, MISO, SPP, PJM, NYISO, ERCOT, and ISO-NE queue sources. Structured XLSX, CSV, XML, and HTML sources are normalized into project-row snapshots so future changed-source runs emit `NEW`, `UPDATED`, and `REMOVED` project deltas.

## Smoke Test

```bash
aws lambda invoke \
  --function-name queuewatch-orchestrator \
  --payload '{}' \
  response.json

cat response.json
```

Inspect output:

```bash
aws dynamodb scan --table-name QueueWatch-State
aws dynamodb scan --table-name QueueWatch-Insights
aws s3 ls "s3://$(terraform output -raw raw_bucket_name)/raw/" --recursive
aws s3 ls "s3://$(terraform output -raw raw_bucket_name)/insights/" --recursive
aws s3 ls "s3://$(terraform output -raw raw_bucket_name)/reports/" --recursive
```

Check DLQs:

```bash
aws sqs get-queue-attributes \
  --queue-url "$(terraform output -raw orchestrator_dlq_url)" \
  --attribute-names ApproximateNumberOfMessages

aws sqs get-queue-attributes \
  --queue-url "$(terraform output -raw parser_dlq_url)" \
  --attribute-names ApproximateNumberOfMessages
```

Generate the buyer-facing daily report manually:

```bash
aws lambda invoke \
  --function-name queuewatch-reporter \
  --payload '{}' \
  report-response.json \
  --cli-binary-format raw-in-base64-out

cat report-response.json
```

Check captured pilot leads:

```bash
aws dynamodb scan --table-name QueueWatch-Leads
```

## Landing Page

The end-user marketing site lives in `landing/` and is deployable as a static Cloudflare Pages site.

Local preview:

```bash
python3 -m http.server 8788 --directory landing
```

Deploy:

```bash
npx wrangler@latest pages deploy landing --project-name queuewatch --branch main
```

The current production URL is:

```text
https://queuewatch.pages.dev
```
