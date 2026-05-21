#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import json
import logging
import os
from pathlib import Path
from typing import Any

import boto3


logger = logging.getLogger("queuewatch.seed")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

DEFAULT_TABLE = os.getenv("QUEUEWATCH_STATE_TABLE", "QueueWatch-State")


def main() -> int:
    args = parse_args()
    records = load_records(args)
    if not records:
        logger.info("No queue records to seed")
        return 0

    dynamodb = boto3.resource("dynamodb", region_name=args.region)
    table = dynamodb.Table(args.table)

    failures = 0
    for record in records:
        try:
            normalized = normalize_record(record, default_active=not args.inactive)
            if args.dry_run:
                logger.info("DRY RUN %s", json.dumps(normalized, sort_keys=True))
                continue
            upsert_queue(table, normalized)
            logger.info("Seeded QueueID=%s", normalized["QueueID"])
        except Exception as exc:
            failures += 1
            logger.error("Failed to seed record %s: %s", record, exc)

    if failures:
        logger.error("Completed with %d failure(s)", failures)
        return 1

    logger.info("Seeded %d queue target(s) into %s", len(records), args.table)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed or update QueueWatch-State target records."
    )
    parser.add_argument("--table", default=DEFAULT_TABLE, help="DynamoDB state table")
    parser.add_argument("--region", default=None, help="AWS region")
    parser.add_argument("--file", type=Path, help="CSV or JSON file of queue targets")
    parser.add_argument("--queue-id", help="Single QueueID to seed")
    parser.add_argument("--source-url", help="Single SourceURL to seed")
    parser.add_argument("--utility-name", help="Optional utility name for single target")
    parser.add_argument(
        "--inactive",
        action="store_true",
        help="Mark single-target records inactive by default",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print without writing")
    return parser.parse_args()


def load_records(args: argparse.Namespace) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    if args.file:
        records.extend(load_records_from_file(args.file))

    if args.queue_id or args.source_url:
        if not args.queue_id or not args.source_url:
            raise ValueError("--queue-id and --source-url must be provided together")
        record = {
            "QueueID": args.queue_id,
            "SourceURL": args.source_url,
        }
        if args.utility_name:
            record["UtilityName"] = args.utility_name
        records.append(record)

    if not records:
        raise ValueError("Provide --file or both --queue-id and --source-url")
    return records


def load_records_from_file(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]

    if suffix == ".json":
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict) and isinstance(payload.get("queues"), list):
            payload = payload["queues"]
        if not isinstance(payload, list):
            raise ValueError("JSON seed file must be a list or an object with queues[]")
        return [dict(item) for item in payload]

    raise ValueError(f"Unsupported seed file type: {path}")


def normalize_record(
    record: dict[str, Any],
    default_active: bool,
) -> dict[str, Any]:
    queue_id = first_value(record, "QueueID", "queue_id", "id")
    source_url = first_value(record, "SourceURL", "source_url", "url", "URL")
    if not queue_id or not source_url:
        raise ValueError("Each record requires QueueID and SourceURL")

    normalized: dict[str, Any] = {
        "QueueID": str(queue_id).strip(),
        "SourceURL": str(source_url).strip(),
        "Active": parse_bool(first_value(record, "Active", "active"), default_active),
    }

    for key, value in record.items():
        canonical = canonical_key(key)
        if canonical in normalized or value is None or str(value).strip() == "":
            continue
        normalized[canonical] = normalize_extra_value(value)

    return normalized


def upsert_queue(table: Any, record: dict[str, Any]) -> None:
    now = utc_now_iso()
    assignments = [
        "#created_at = if_not_exists(#created_at, :created_at)",
        "#updated_at = :updated_at",
    ]
    names = {
        "#created_at": "CreatedAt",
        "#updated_at": "UpdatedAt",
    }
    values: dict[str, Any] = {
        ":created_at": now,
        ":updated_at": now,
    }

    for index, (key, value) in enumerate(record.items()):
        if key == "QueueID":
            continue
        name_token = f"#field_{index}"
        value_token = f":value_{index}"
        assignments.append(f"{name_token} = {value_token}")
        names[name_token] = key
        values[value_token] = value

    table.update_item(
        Key={"QueueID": record["QueueID"]},
        UpdateExpression="SET " + ", ".join(assignments),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def first_value(record: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in record and record[key] not in (None, ""):
            return record[key]
    return None


def canonical_key(value: str) -> str:
    mapping = {
        "queue_id": "QueueID",
        "id": "QueueID",
        "source_url": "SourceURL",
        "url": "SourceURL",
        "active": "Active",
        "utility": "UtilityName",
        "utility_name": "UtilityName",
    }
    stripped = value.strip()
    return mapping.get(stripped.lower(), stripped)


def normalize_extra_value(value: Any) -> Any:
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "false", "yes", "no", "y", "n"}:
            return parse_bool(value, default=False)
        return value.strip()
    return value


def parse_bool(value: Any, default: bool) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "active"}


def utc_now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        logger.error("%s", exc)
        raise SystemExit(1)
