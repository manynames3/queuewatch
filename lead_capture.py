import datetime as dt
import json
import logging
import os
import re
import uuid
from typing import Any

import boto3


logger = logging.getLogger()
logger.setLevel(logging.INFO)

LEADS_TABLE_NAME = os.environ["LEADS_TABLE_NAME"]
ALLOWED_ORIGIN = os.getenv("ALLOWED_ORIGIN", "https://queuewatch.pages.dev")
LEAD_NOTIFICATION_EMAIL = os.getenv("LEAD_NOTIFICATION_EMAIL", "")
LEAD_SENDER_EMAIL = os.getenv("LEAD_SENDER_EMAIL", "")

dynamodb = boto3.resource("dynamodb")
ses = boto3.client("ses")
leads_table = dynamodb.Table(LEADS_TABLE_NAME)

EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    method = (
        event.get("requestContext", {})
        .get("http", {})
        .get("method", event.get("httpMethod", ""))
    )

    if method == "OPTIONS":
        return response(204, {})

    if method != "POST":
        return response(405, {"error": "Method not allowed"})

    try:
        payload = parse_body(event)
        lead = validate_payload(payload)
        leads_table.put_item(Item=lead)
        notify_if_configured(lead)
        logger.info("Captured QueueWatch lead lead_id=%s email=%s", lead["LeadID"], lead["Email"])
        return response(
            202,
            {
                "ok": True,
                "lead_id": lead["LeadID"],
                "message": "Pilot request received.",
            },
        )
    except ValueError as exc:
        return response(400, {"error": str(exc)})
    except Exception as exc:
        logger.exception("Lead capture failed: %s", exc)
        return response(500, {"error": "Lead capture failed"})


def parse_body(event: dict[str, Any]) -> dict[str, Any]:
    body = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        raise ValueError("Base64 request bodies are not supported")
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise ValueError("Request body must be valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("Request body must be a JSON object")
    return payload


def validate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    email = clean(payload.get("email")).lower()
    company = clean(payload.get("company"))
    role = clean(payload.get("role"))
    territories = clean(payload.get("territories"))
    use_case = clean(payload.get("use_case"))
    name = clean(payload.get("name"))

    if not email or not EMAIL_PATTERN.match(email):
        raise ValueError("A valid work email is required")
    if not company:
        raise ValueError("Company is required")
    if not territories:
        raise ValueError("Priority territories are required")
    if not use_case:
        raise ValueError("Use case is required")

    created_at = utc_now_iso()
    return {
        "LeadID": str(uuid.uuid4()),
        "CreatedAt": created_at,
        "Email": email,
        "Name": name,
        "Company": company,
        "Role": role,
        "PriorityTerritories": territories,
        "UseCase": use_case,
        "Status": "NEW",
        "Source": "queuewatch.pages.dev",
    }


def notify_if_configured(lead: dict[str, Any]) -> None:
    if not LEAD_NOTIFICATION_EMAIL or not LEAD_SENDER_EMAIL:
        return

    ses.send_email(
        Source=LEAD_SENDER_EMAIL,
        Destination={"ToAddresses": [LEAD_NOTIFICATION_EMAIL]},
        Message={
            "Subject": {
                "Data": f"New QueueWatch pilot lead: {lead['Company']}",
                "Charset": "UTF-8",
            },
            "Body": {
                "Text": {
                    "Data": "\n".join(
                        [
                            "New QueueWatch pilot lead",
                            f"Lead ID: {lead['LeadID']}",
                            f"Name: {lead['Name']}",
                            f"Email: {lead['Email']}",
                            f"Company: {lead['Company']}",
                            f"Role: {lead['Role']}",
                            f"Territories: {lead['PriorityTerritories']}",
                            f"Use case: {lead['UseCase']}",
                        ]
                    ),
                    "Charset": "UTF-8",
                }
            },
        },
    )


def response(status_code: int, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
            "Access-Control-Allow-Headers": "content-type",
            "Access-Control-Allow-Methods": "OPTIONS,POST",
            "Content-Type": "application/json",
        },
        "body": "" if status_code == 204 else json.dumps(body),
    }


def clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()[:2000]


def utc_now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
