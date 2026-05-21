import csv
import datetime as dt
import html
import io
import json
import logging
import os
from decimal import Decimal
from typing import Any

import boto3


logger = logging.getLogger()
logger.setLevel(logging.INFO)

INSIGHTS_TABLE_NAME = os.getenv("INSIGHTS_TABLE_NAME", "QueueWatch-Insights")
REPORT_BUCKET_NAME = os.getenv("REPORT_BUCKET_NAME", "")
REPORT_PREFIX = os.getenv("REPORT_PREFIX", "reports/")
REPORT_LOOKBACK_HOURS = int(os.getenv("REPORT_LOOKBACK_HOURS", "24"))
REPORT_RECIPIENT_EMAILS = [
    email.strip()
    for email in os.getenv("REPORT_RECIPIENT_EMAILS", "").split(",")
    if email.strip()
]
REPORT_SENDER_EMAIL = os.getenv("REPORT_SENDER_EMAIL", "")
PRODUCT_URL = os.getenv("PRODUCT_URL", "https://queuewatch.pages.dev")

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
ses = boto3.client("ses")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    logger.info("Starting QueueWatch report generation")
    items = load_recent_insights(REPORT_LOOKBACK_HOURS)
    generated_at = utc_now_iso()
    report = build_report(items, generated_at)

    output: dict[str, Any] = {
        "generated_at": generated_at,
        "lookback_hours": REPORT_LOOKBACK_HOURS,
        "insight_count": len(items),
        "reports": {},
        "email_sent": False,
    }

    if REPORT_BUCKET_NAME:
        keys = write_report_artifacts(report, generated_at)
        output["reports"] = keys

    if REPORT_SENDER_EMAIL and REPORT_RECIPIENT_EMAILS:
        send_email_report(report)
        output["email_sent"] = True
    else:
        logger.info("Skipping email delivery; sender or recipients are not configured")

    logger.info("QueueWatch report generation completed: %s", output)
    return output


def load_recent_insights(lookback_hours: int) -> list[dict[str, Any]]:
    cutoff = dt.datetime.now(dt.UTC) - dt.timedelta(hours=lookback_hours)
    cutoff_iso = cutoff.isoformat().replace("+00:00", "Z")
    table = dynamodb.Table(INSIGHTS_TABLE_NAME)
    items: list[dict[str, Any]] = []
    scan_kwargs: dict[str, Any] = {}

    while True:
        response = table.scan(**scan_kwargs)
        for item in response.get("Items", []):
            observed_at = str(item.get("ObservedAt", ""))
            if observed_at >= cutoff_iso:
                items.append(item)
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    return sorted(items, key=lambda item: str(item.get("ObservedAt", "")), reverse=True)


def build_report(items: list[dict[str, Any]], generated_at: str) -> dict[str, Any]:
    all_normalized = [normalize_item(item) for item in items]
    project_deltas = [
        item for item in all_normalized if item["insight_type"] == "PROJECT_DELTA"
    ]
    normalized = project_deltas or all_normalized
    summary = {
        "total_signals": len(normalized),
        "project_delta_signals": len(project_deltas),
        "baseline_signals": sum(1 for item in normalized if item["delta_type"] == "BASELINE"),
        "new_signals": sum(1 for item in normalized if item["delta_type"] == "NEW"),
        "updated_signals": sum(1 for item in normalized if item["delta_type"] == "UPDATED"),
        "removed_signals": sum(1 for item in normalized if item["delta_type"] == "REMOVED"),
        "needs_review": sum(1 for item in normalized if item["review_status"] == "NEEDS_REVIEW"),
        "total_capacity_mw": sum(item["capacity_mw"] or 0 for item in normalized),
        "active_signals": sum(1 for item in normalized if item["status"] == "Active"),
        "withdrawn_signals": sum(1 for item in normalized if item["status"] == "Withdrawn"),
        "completed_signals": sum(1 for item in normalized if item["status"] == "Completed"),
    }
    return {
        "generated_at": generated_at,
        "product_url": PRODUCT_URL,
        "summary": summary,
        "signals": normalized,
    }


def normalize_item(item: dict[str, Any]) -> dict[str, Any]:
    extraction = item.get("Extraction") or {}
    return {
        "queue_id": string_value(item.get("QueueID")),
        "source_queue_id": string_value(item.get("SourceQueueID") or item.get("QueueID")),
        "insight_type": string_value(item.get("InsightType") or "SOURCE_EXTRACTION"),
        "delta_type": string_value(item.get("DeltaType")),
        "observed_at": string_value(item.get("ObservedAt")),
        "interconnection_queue_id": string_value(
            item.get("InterconnectionQueueID") or extraction.get("interconnection_queue_id")
        ),
        "project_name": string_value(item.get("ProjectName")),
        "capacity_mw": int_value(item.get("CapacityMW") or extraction.get("capacity_mw")),
        "substation_or_node": string_value(
            item.get("SubstationOrNode") or extraction.get("substation_or_node")
        ),
        "status": string_value(item.get("Status") or extraction.get("status")),
        "developer_name": string_value(item.get("DeveloperName") or extraction.get("developer_name")),
        "source_url": string_value(item.get("SourceURL")),
        "raw_s3_key": string_value(item.get("RawS3Key")),
        "output_s3_key": string_value(item.get("OutputS3Key")),
        "confidence": decimal_value(item.get("ExtractionConfidence")),
        "review_status": string_value(item.get("ReviewStatus")) or "NEEDS_REVIEW",
        "review_reasons": list_value(item.get("ReviewReasons")),
        "changed_fields": list_value(item.get("ChangedFields")),
        "market": string_value(item.get("Market")),
        "utility_name": string_value(item.get("UtilityName")),
    }


def write_report_artifacts(report: dict[str, Any], generated_at: str) -> dict[str, str]:
    timestamp = safe_timestamp(generated_at)
    prefix = REPORT_PREFIX if REPORT_PREFIX.endswith("/") else f"{REPORT_PREFIX}/"
    html_key = f"{prefix}{timestamp}/queuewatch-report.html"
    csv_key = f"{prefix}{timestamp}/queuewatch-signals.csv"
    json_key = f"{prefix}{timestamp}/queuewatch-report.json"

    s3.put_object(
        Bucket=REPORT_BUCKET_NAME,
        Key=html_key,
        Body=render_html_report(report).encode("utf-8"),
        ContentType="text/html; charset=utf-8",
    )
    s3.put_object(
        Bucket=REPORT_BUCKET_NAME,
        Key=csv_key,
        Body=render_csv_report(report).encode("utf-8"),
        ContentType="text/csv; charset=utf-8",
    )
    s3.put_object(
        Bucket=REPORT_BUCKET_NAME,
        Key=json_key,
        Body=json.dumps(report, default=json_default, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return {
        "html": html_key,
        "csv": csv_key,
        "json": json_key,
    }


def render_html_report(report: dict[str, Any]) -> str:
    summary = report["summary"]
    rows = "\n".join(render_signal_row(signal) for signal in report["signals"])
    if not rows:
        rows = """
          <tr>
            <td colspan="11" class="empty">No changed queue signals were detected in this reporting window.</td>
          </tr>
        """

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>QueueWatch Daily Signal Report</title>
  <style>
    body {{ margin: 0; background: #f7f8f2; color: #141714; font-family: Arial, sans-serif; }}
    main {{ max-width: 1100px; margin: 0 auto; padding: 40px 22px; }}
    h1 {{ margin: 0; font-size: 34px; }}
    p {{ color: #4f5750; line-height: 1.5; }}
    .summary {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin: 24px 0; }}
    .metric {{ background: #fff; border: 1px solid #d8ded1; border-radius: 8px; padding: 16px; }}
    .metric span {{ display: block; color: #4f5750; font-size: 12px; text-transform: uppercase; }}
    .metric strong {{ display: block; margin-top: 8px; font-size: 24px; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #d8ded1; }}
    th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #e5e8df; vertical-align: top; }}
    th {{ font-size: 12px; text-transform: uppercase; color: #4f5750; background: #ecefe7; }}
    .empty {{ color: #4f5750; text-align: center; }}
    .needs {{ color: #9a3c31; font-weight: 700; }}
    .auto {{ color: #2f7b63; font-weight: 700; }}
    @media (max-width: 900px) {{ .summary {{ grid-template-columns: 1fr 1fr; }} table {{ font-size: 13px; }} }}
  </style>
</head>
<body>
  <main>
    <p>QueueWatch</p>
    <h1>Daily Signal Report</h1>
    <p>Generated {html.escape(report["generated_at"])}. Source-backed project deltas from public interconnection queue documents.</p>
    <section class="summary">
      <div class="metric"><span>Total signals</span><strong>{summary["total_signals"]}</strong></div>
      <div class="metric"><span>Total capacity</span><strong>{summary["total_capacity_mw"]} MW</strong></div>
      <div class="metric"><span>New / updated</span><strong>{summary["new_signals"]} / {summary["updated_signals"]}</strong></div>
      <div class="metric"><span>Active</span><strong>{summary["active_signals"]}</strong></div>
      <div class="metric"><span>Needs review</span><strong>{summary["needs_review"]}</strong></div>
    </section>
    <table>
      <thead>
        <tr>
          <th>Observed</th>
          <th>Change</th>
          <th>Market</th>
          <th>Queue ID</th>
          <th>Project</th>
          <th>Capacity</th>
          <th>Node</th>
          <th>Status</th>
          <th>Developer</th>
          <th>Review</th>
          <th>Source</th>
        </tr>
      </thead>
      <tbody>
        {rows}
      </tbody>
    </table>
  </main>
</body>
</html>"""


def render_signal_row(signal: dict[str, Any]) -> str:
    review_class = "auto" if signal["review_status"] == "AUTO_REVIEWED" else "needs"
    capacity = "" if signal["capacity_mw"] is None else f"{signal['capacity_mw']} MW"
    source = signal["source_url"] or signal["raw_s3_key"]
    delta_type = signal["delta_type"] or "SOURCE"
    return f"""
      <tr>
        <td>{html.escape(signal["observed_at"])}</td>
        <td>{html.escape(delta_type)}</td>
        <td>{html.escape(signal["market"] or signal["source_queue_id"])}</td>
        <td>{html.escape(signal["interconnection_queue_id"] or signal["queue_id"])}</td>
        <td>{html.escape(signal["project_name"])}</td>
        <td>{html.escape(capacity)}</td>
        <td>{html.escape(signal["substation_or_node"])}</td>
        <td>{html.escape(signal["status"])}</td>
        <td>{html.escape(signal["developer_name"])}</td>
        <td class="{review_class}">{html.escape(signal["review_status"])}<br><small>{html.escape(str(signal["confidence"]))}</small></td>
        <td>{html.escape(source)}</td>
      </tr>
    """


def render_csv_report(report: dict[str, Any]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "observed_at",
            "queue_id",
            "source_queue_id",
            "insight_type",
            "delta_type",
            "market",
            "utility_name",
            "interconnection_queue_id",
            "project_name",
            "capacity_mw",
            "substation_or_node",
            "status",
            "developer_name",
            "changed_fields",
            "confidence",
            "review_status",
            "source_url",
            "raw_s3_key",
            "output_s3_key",
        ],
    )
    writer.writeheader()
    for signal in report["signals"]:
        writer.writerow({key: signal.get(key, "") for key in writer.fieldnames})
    return output.getvalue()


def send_email_report(report: dict[str, Any]) -> None:
    summary = report["summary"]
    subject = (
        f"QueueWatch daily report: {summary['total_signals']} signals, "
        f"{summary['total_capacity_mw']} MW"
    )
    ses.send_email(
        Source=REPORT_SENDER_EMAIL,
        Destination={"ToAddresses": REPORT_RECIPIENT_EMAILS},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {
                "Html": {"Data": render_html_report(report), "Charset": "UTF-8"},
                "Text": {"Data": render_text_report(report), "Charset": "UTF-8"},
            },
        },
    )


def render_text_report(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "QueueWatch Daily Signal Report",
        f"Generated: {report['generated_at']}",
        f"Signals: {summary['total_signals']}",
        f"New: {summary['new_signals']}",
        f"Updated: {summary['updated_signals']}",
        f"Removed: {summary['removed_signals']}",
        f"Total capacity: {summary['total_capacity_mw']} MW",
        "",
    ]
    for signal in report["signals"]:
        lines.append(
            " | ".join(
                [
                    signal["observed_at"],
                    signal["delta_type"] or "SOURCE",
                    signal["market"] or signal["source_queue_id"],
                    signal["interconnection_queue_id"] or signal["queue_id"],
                    signal["project_name"],
                    f"{signal['capacity_mw']} MW" if signal["capacity_mw"] is not None else "capacity unknown",
                    signal["substation_or_node"],
                    signal["status"],
                    signal["review_status"],
                ]
            )
        )
    return "\n".join(lines)


def string_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def int_value(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return int(value)
    return int(value)


def decimal_value(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def list_value(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def safe_timestamp(value: str) -> str:
    return value.replace(":", "-").replace(".", "-")


def utc_now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
