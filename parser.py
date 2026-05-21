import csv
import datetime as dt
import hashlib
import html.parser
import io
import json
import logging
import os
import re
import time
import urllib.parse
import zipfile
import zlib
from decimal import Decimal
from typing import Any
from xml.etree import ElementTree

import boto3


logger = logging.getLogger()
logger.setLevel(logging.INFO)

INSIGHTS_TABLE_NAME = os.environ["INSIGHTS_TABLE_NAME"]
STATE_TABLE_NAME = os.getenv("STATE_TABLE_NAME", "")
BEDROCK_MODEL_ID = os.getenv(
    "BEDROCK_MODEL_ID",
    "us.anthropic.claude-haiku-4-5-20251001-v1:0",
)
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "insights/")
PROJECT_SNAPSHOT_PREFIX = os.getenv("PROJECT_SNAPSHOT_PREFIX", "project-snapshots/")
MAX_SOURCE_BYTES = int(os.getenv("MAX_SOURCE_BYTES", str(50 * 1024 * 1024)))
MAX_DOCUMENT_CHARS = int(os.getenv("MAX_DOCUMENT_CHARS", "120000"))
MAX_PROJECT_RECORDS = int(os.getenv("MAX_PROJECT_RECORDS", "12000"))
MAX_PROJECT_INSIGHTS_PER_SOURCE = int(os.getenv("MAX_PROJECT_INSIGHTS_PER_SOURCE", "750"))
BASELINE_SAMPLE_LIMIT = int(os.getenv("BASELINE_SAMPLE_LIMIT", "25"))
BEDROCK_MAX_TOKENS = int(os.getenv("BEDROCK_MAX_TOKENS", "1200"))
ENABLE_TEXTRACT_FALLBACK = os.getenv("ENABLE_TEXTRACT_FALLBACK", "true").lower() in {
    "1",
    "true",
    "yes",
    "y",
}
TEXTRACT_MIN_PDF_TEXT_CHARS = int(os.getenv("TEXTRACT_MIN_PDF_TEXT_CHARS", "250"))
TEXTRACT_MAX_WAIT_SECONDS = int(os.getenv("TEXTRACT_MAX_WAIT_SECONDS", "240"))
TEXTRACT_POLL_SECONDS = int(os.getenv("TEXTRACT_POLL_SECONDS", "5"))
TEXTRACT_SYNC_MAX_BYTES = 10 * 1024 * 1024

ALLOWED_STATUSES = {"Active", "Withdrawn", "Completed"}
PROJECT_DELTA_INSIGHT_TYPE = "PROJECT_DELTA"
SOURCE_EXTRACTION_INSIGHT_TYPE = "SOURCE_EXTRACTION"
DETERMINISTIC_MODEL_ID = "deterministic-table-parser-v1"

PROJECT_ID_ALIASES = [
    "queuepos",
    "queueposition",
    "queuenumber",
    "generationinterconnectionnumber",
    "projectnumber",
    "projectid",
    "inr",
    "qp",
    "applicationid",
    "ifsqueuenumber",
    "requestid",
    "interconnectionrequestid",
]
PROJECT_NAME_ALIASES = [
    "projectname",
    "commercialname",
    "alternativename",
    "generatingfacility",
    "facilityname",
    "name",
]
DEVELOPER_ALIASES = [
    "developerinterconnectioncustomer",
    "interconnectioncustomer",
    "interconnectioncustomername",
    "interconnectingentity",
    "customer",
    "applicant",
    "owner",
]
NODE_ALIASES = [
    "pointsofinterconnection",
    "pointofinterconnection",
    "poilocation",
    "poiname",
    "poi",
    "substationornode",
    "substation",
    "nearesttownorcounty",
    "bus",
    "node",
]
STATUS_ALIASES = [
    "status",
    "requeststatus",
    "projectstatus",
    "queuestatus",
    "postgiastatus",
    "studyphase",
    "gimstudyphase",
]
WITHDRAWN_DATE_ALIASES = [
    "withdrawaldate",
    "datewithdrawn",
    "wddate",
    "wddate",
]
CAPACITY_HEADER_HINTS = (
    "capacity",
    "mw",
    "maximumfacilityoutput",
    "nameplate",
    "output",
)
CAPACITY_HEADER_EXCLUDES = (
    "mwh",
    "date",
    "duration",
)

s3 = boto3.client("s3")
bedrock = boto3.client("bedrock-runtime")
textract = boto3.client("textract")
dynamodb = boto3.resource("dynamodb")
insights_table = dynamodb.Table(INSIGHTS_TABLE_NAME)
state_table = dynamodb.Table(STATE_TABLE_NAME) if STATE_TABLE_NAME else None

SYSTEM_PROMPT = """You are QueueWatch, an extraction engine for public utility interconnection queue records.

Return only one valid JSON object. Do not include markdown, XML, comments, explanations, code fences, or trailing text.

Required JSON keys:
- interconnection_queue_id: string or null
- capacity_mw: integer or null
- substation_or_node: string or null
- status: one of "Active", "Withdrawn", "Completed"
- developer_name: string or null

Extraction rules:
- Extract only facts supported by the supplied document text and metadata.
- Normalize project capacity to integer MW. Convert GW to MW. Round only when the document itself uses a decimal MW value.
- Prefer queue/project IDs exactly as written in the source.
- For status, use exactly "Active", "Withdrawn", or "Completed". If the document lists a queued project without a more specific status, use "Active".
- Use null for unavailable fields other than status.
- If several records are present, extract the record most directly associated with the supplied QueueID metadata.
"""


class UnsupportedDocumentError(ValueError):
    pass


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    queue_id = str(event.get("queue_id") or event.get("QueueID") or "").strip()
    bucket = str(event.get("s3_bucket") or event.get("bucket") or "").strip()
    key = str(event.get("s3_key") or event.get("key") or "").strip()
    observed_at = str(event.get("observed_at") or utc_now_iso())

    if not queue_id or not bucket or not key:
        raise ValueError("Parser event must include queue_id, s3_bucket, and s3_key")

    logger.info("Starting parser queue_id=%s s3://%s/%s", queue_id, bucket, key)

    try:
        source = load_source_object(bucket, key)
        project_records, project_notes = extract_project_records(
            raw_bytes=source["body"],
            s3_key=key,
            content_type=source.get("content_type"),
            source_queue_id=queue_id,
        )
        if project_records:
            logger.info(
                "Extracted %d deterministic project rows for queue_id=%s",
                len(project_records),
                queue_id,
            )
            previous_snapshot_key, previous_records = load_previous_project_snapshot(
                queue_id=queue_id,
                bucket=bucket,
            )
            project_deltas, delta_summary = compare_project_records(
                previous_records=previous_records,
                current_records=project_records,
            )
            selected_deltas = select_project_deltas(project_deltas, previous_records)
            snapshot_key = write_project_snapshot(
                bucket=bucket,
                queue_id=queue_id,
                observed_at=observed_at,
                records=project_records,
                event=event,
            )
            output_key = write_output_json(
                bucket=bucket,
                queue_id=queue_id,
                observed_at=observed_at,
                payload={
                    "queue_id": queue_id,
                    "observed_at": observed_at,
                    "source_url": event.get("source_url"),
                    "raw_s3_bucket": bucket,
                    "raw_s3_key": key,
                    "content_md5": event.get("content_md5"),
                    "remote_fingerprint": event.get("remote_fingerprint"),
                    "model_id": DETERMINISTIC_MODEL_ID,
                    "parser_mode": "deterministic_project_table",
                    "extraction_notes": project_notes,
                    "project_record_count": len(project_records),
                    "previous_project_snapshot_s3_key": previous_snapshot_key,
                    "project_snapshot_s3_key": snapshot_key,
                    "project_delta_summary": delta_summary,
                    "project_deltas_persisted": len(selected_deltas),
                    "project_deltas_sample": selected_deltas[:50],
                    "parsed_at": utc_now_iso(),
                },
            )
            persist_project_deltas(
                queue_id=queue_id,
                observed_at=observed_at,
                event=event,
                deltas=selected_deltas,
                output_key=output_key,
            )
            mark_parser_status(
                queue_id,
                "SUCCEEDED",
                observed_at,
                output_key=output_key,
                review_status="AUTO_REVIEWED",
                project_snapshot_key=snapshot_key,
                project_record_count=len(project_records),
                project_delta_count=len(project_deltas),
                project_delta_summary=delta_summary,
            )
            logger.info(
                "Deterministic parser completed queue_id=%s records=%d deltas=%d output_key=%s",
                queue_id,
                len(project_records),
                len(project_deltas),
                output_key,
            )
            return {
                "queue_id": queue_id,
                "observed_at": observed_at,
                "status": "SUCCEEDED",
                "parser_mode": "deterministic_project_table",
                "project_record_count": len(project_records),
                "project_delta_count": len(project_deltas),
                "project_deltas_persisted": len(selected_deltas),
                "project_snapshot_s3_key": snapshot_key,
                "output_s3_key": output_key,
            }

        text, extraction_notes = extract_document_text(
            raw_bytes=source["body"],
            s3_bucket=bucket,
            s3_key=key,
            content_type=source.get("content_type"),
            content_length=source.get("content_length"),
        )
        extraction_notes = project_notes + extraction_notes
        if len(text) > MAX_DOCUMENT_CHARS:
            extraction_notes.append(
                f"Document text truncated from {len(text)} to {MAX_DOCUMENT_CHARS} characters"
            )
            text = text[:MAX_DOCUMENT_CHARS]

        model_output = invoke_bedrock(
            queue_id=queue_id,
            source_url=event.get("source_url"),
            s3_key=key,
            content_md5=event.get("content_md5"),
            document_text=text,
            extraction_notes=extraction_notes,
        )
        extraction = validate_extraction(model_output)
        extraction_confidence, review_status, review_reasons = score_extraction(
            extraction,
            extraction_notes,
        )
        output_key = write_output_json(
            bucket=bucket,
            queue_id=queue_id,
            observed_at=observed_at,
            payload={
                "queue_id": queue_id,
                "observed_at": observed_at,
                "source_url": event.get("source_url"),
                "raw_s3_bucket": bucket,
                "raw_s3_key": key,
                "content_md5": event.get("content_md5"),
                "remote_fingerprint": event.get("remote_fingerprint"),
                "model_id": BEDROCK_MODEL_ID,
                "extraction_notes": extraction_notes,
                "extraction_confidence": extraction_confidence,
                "review_status": review_status,
                "review_reasons": review_reasons,
                "extraction": extraction,
                "parsed_at": utc_now_iso(),
            },
        )
        persist_insight(
            queue_id=queue_id,
            observed_at=observed_at,
            event=event,
            extraction=extraction,
            extraction_confidence=extraction_confidence,
            review_status=review_status,
            review_reasons=review_reasons,
            output_key=output_key,
        )
        mark_parser_status(
            queue_id,
            "SUCCEEDED",
            observed_at,
            output_key=output_key,
            extraction_confidence=extraction_confidence,
            review_status=review_status,
        )
        logger.info("Parser completed queue_id=%s output_key=%s", queue_id, output_key)
        return {
            "queue_id": queue_id,
            "observed_at": observed_at,
            "status": "SUCCEEDED",
            "output_s3_key": output_key,
        }
    except Exception as exc:
        logger.exception("Parser failed queue_id=%s: %s", queue_id, exc)
        mark_parser_status(queue_id, "FAILED", observed_at, error=exc)
        raise


def load_source_object(bucket: str, key: str) -> dict[str, Any]:
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read(MAX_SOURCE_BYTES + 1)
    if len(body) > MAX_SOURCE_BYTES:
        body = body[:MAX_SOURCE_BYTES]
        logger.info("Source object truncated to MAX_SOURCE_BYTES=%d", MAX_SOURCE_BYTES)

    return {
        "body": body,
        "content_type": response.get("ContentType", ""),
        "metadata": response.get("Metadata", {}),
        "content_length": response.get("ContentLength"),
    }


def extract_project_records(
    raw_bytes: bytes,
    s3_key: str,
    content_type: str | None,
    source_queue_id: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    lower_key = s3_key.lower()
    normalized_type = (content_type or "").split(";", 1)[0].strip().lower()
    notes: list[str] = []
    records: list[dict[str, Any]] = []

    try:
        if lower_key.endswith(".xlsx") or normalized_type.endswith("spreadsheetml.sheet"):
            records = extract_xlsx_project_records(raw_bytes, source_queue_id)
            notes.append("Extracted project rows from XLSX worksheets")
        elif lower_key.endswith(".xml") or normalized_type in {"text/xml", "application/xml"}:
            records = extract_xml_project_records(raw_bytes, source_queue_id)
            notes.append("Extracted project rows from XML elements")
        elif lower_key.endswith(".html") or lower_key.endswith(".htm") or normalized_type in {
            "text/html",
            "application/xhtml+xml",
        }:
            records = extract_html_project_records(raw_bytes, source_queue_id)
            notes.append("Extracted project rows from HTML tables")
        elif lower_key.endswith(".csv") or normalized_type in {"text/csv", "application/csv"}:
            records = extract_csv_project_records(raw_bytes, source_queue_id)
            notes.append("Extracted project rows from CSV")
    except Exception as exc:
        logger.info("Deterministic project extraction skipped for %s: %s", s3_key, exc)
        return [], [f"Deterministic project extraction skipped: {exc}"]

    records = with_record_fingerprints(dedupe_project_records(records))
    if len(records) > MAX_PROJECT_RECORDS:
        notes.append(
            f"Project rows truncated from {len(records)} to {MAX_PROJECT_RECORDS} records"
        )
        records = records[:MAX_PROJECT_RECORDS]

    return records, notes


def extract_xlsx_project_records(raw_bytes: bytes, source_queue_id: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as workbook:
        shared_strings = read_xlsx_shared_strings(workbook)
        worksheet_names = sorted(
            name
            for name in workbook.namelist()
            if name.startswith("xl/worksheets/") and name.endswith(".xml")
        )
        for worksheet_name in worksheet_names:
            rows = read_xlsx_rows(workbook, worksheet_name, shared_strings)
            records.extend(records_from_matrix(rows, source_queue_id, worksheet_name))
    return records


def read_xlsx_rows(
    workbook: zipfile.ZipFile,
    worksheet_name: str,
    shared_strings: list[str],
) -> list[tuple[int, list[str]]]:
    root = ElementTree.fromstring(workbook.read(worksheet_name))
    rows: list[tuple[int, list[str]]] = []
    for row in root.iterfind(".//{*}row"):
        row_number = int(row.attrib.get("r", len(rows) + 1))
        values: list[str] = []
        for cell in row.iterfind("{*}c"):
            index = xlsx_column_index(cell.attrib.get("r", "A1"))
            while len(values) <= index:
                values.append("")
            values[index] = clean_cell(read_xlsx_cell(cell, shared_strings))
        rows.append((row_number, values))
    return rows


def xlsx_column_index(cell_ref: str) -> int:
    letters = "".join(char for char in cell_ref if char.isalpha()) or "A"
    index = 0
    for char in letters.upper():
        index = index * 26 + (ord(char) - ord("A") + 1)
    return max(0, index - 1)


def extract_csv_project_records(raw_bytes: bytes, source_queue_id: str) -> list[dict[str, Any]]:
    text = decode_text(raw_bytes)
    sample = text[:8192]
    try:
        dialect = csv.Sniffer().sniff(sample)
    except csv.Error:
        dialect = csv.excel
    rows = [
        (line_number, [clean_cell(cell) for cell in row])
        for line_number, row in enumerate(csv.reader(io.StringIO(text), dialect), start=1)
        if any(clean_cell(cell) for cell in row)
    ]
    return records_from_matrix(rows, source_queue_id, "csv")


def extract_xml_project_records(raw_bytes: bytes, source_queue_id: str) -> list[dict[str, Any]]:
    root = ElementTree.fromstring(raw_bytes)
    records: list[dict[str, Any]] = []
    for index, child in enumerate(list(root), start=1):
        row: dict[str, str] = {}
        for element in list(child):
            if list(element):
                continue
            value = clean_cell(element.text or "")
            if value:
                row[local_xml_name(element.tag)] = value
        record = normalize_project_record(
            row,
            source_queue_id=source_queue_id,
            source_location=f"{local_xml_name(child.tag)}[{index}]",
            source_row_number=index,
        )
        if record:
            records.append(record)
    return records


def local_xml_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


class TableRowParser(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_table = False
        self.table_depth = 0
        self.in_cell = False
        self.current_row: list[str] = []
        self.current_cell: list[str] = []
        self.rows: list[tuple[int, list[str]]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag == "table":
            self.in_table = True
            self.table_depth += 1
        elif self.in_table and tag == "tr":
            self.current_row = []
        elif self.in_table and tag in {"td", "th"}:
            self.in_cell = True
            self.current_cell = []

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if self.in_table and tag in {"td", "th"} and self.in_cell:
            self.current_row.append(clean_cell(" ".join(self.current_cell)))
            self.in_cell = False
        elif self.in_table and tag == "tr":
            if any(self.current_row):
                self.rows.append((len(self.rows) + 1, self.current_row))
        elif tag == "table" and self.in_table:
            self.table_depth -= 1
            if self.table_depth <= 0:
                self.in_table = False

    def handle_data(self, data: str) -> None:
        if self.in_cell:
            self.current_cell.append(data)


def extract_html_project_records(raw_bytes: bytes, source_queue_id: str) -> list[dict[str, Any]]:
    parser = TableRowParser()
    parser.feed(decode_text(raw_bytes))
    return records_from_matrix(parser.rows, source_queue_id, "html-table")


def records_from_matrix(
    rows: list[tuple[int, list[str]]],
    source_queue_id: str,
    source_location: str,
) -> list[dict[str, Any]]:
    header_indices = find_header_indices(rows)
    records: list[dict[str, Any]] = []
    for position, header_index in enumerate(header_indices):
        next_header_index = (
            header_indices[position + 1] if position + 1 < len(header_indices) else len(rows)
        )
        header_row_number, header = rows[header_index]
        for row_number, values in rows[header_index + 1 : next_header_index]:
            if not any(values):
                continue
            row = row_dict_from_header(header, values)
            record = normalize_project_record(
                row,
                source_queue_id=source_queue_id,
                source_location=source_location,
                source_row_number=row_number,
                source_header_row_number=header_row_number,
            )
            if record:
                records.append(record)
    return records


def find_header_indices(rows: list[tuple[int, list[str]]]) -> list[int]:
    return [index for index, (_, row) in enumerate(rows) if is_project_header_row(row)]


def is_project_header_row(row: list[str]) -> bool:
    normalized = {normalize_header(value) for value in row if clean_cell(value)}
    if not any(alias in normalized for alias in PROJECT_ID_ALIASES):
        return False
    if any(alias in normalized for alias in NODE_ALIASES + PROJECT_NAME_ALIASES + STATUS_ALIASES):
        return True
    return any(is_capacity_header(name, name) for name in normalized) and len(normalized) >= 5


def row_dict_from_header(header: list[str], values: list[str]) -> dict[str, str]:
    row: dict[str, str] = {}
    for index, header_value in enumerate(header):
        name = clean_cell(header_value)
        if not name:
            continue
        value = clean_cell(values[index]) if index < len(values) else ""
        if not value:
            continue
        key = name
        suffix = 2
        while key in row:
            key = f"{name} {suffix}"
            suffix += 1
        row[key] = value
    return row


def normalize_project_record(
    row: dict[str, Any],
    source_queue_id: str,
    source_location: str,
    source_row_number: int | None = None,
    source_header_row_number: int | None = None,
) -> dict[str, Any] | None:
    lookup = row_lookup(row)
    project_id = get_alias_value(lookup, PROJECT_ID_ALIASES)
    project_name = get_project_name(lookup)
    capacity_mw, capacity_source = choose_capacity(lookup)
    substation_or_node = get_node_value(lookup)
    status = infer_project_status(lookup)
    developer_name = get_alias_value(lookup, DEVELOPER_ALIASES)

    if not project_id and not project_name:
        return None
    if project_id and normalize_header(project_id) in set(PROJECT_ID_ALIASES):
        return None
    if capacity_mw is None and not substation_or_node:
        return None

    if not substation_or_node and get_alias_value(lookup, ["commercialname"]):
        substation_or_node = get_alias_value(lookup, ["name"])

    project_key = build_project_key(project_id, project_name, substation_or_node)
    extraction = {
        "interconnection_queue_id": project_id or project_key,
        "capacity_mw": capacity_mw,
        "substation_or_node": substation_or_node,
        "status": status,
        "developer_name": developer_name,
    }
    confidence, review_status, review_reasons = score_project_record(extraction)
    record = {
        "ProjectKey": project_key,
        "SourceQueueID": source_queue_id,
        "InterconnectionQueueID": extraction["interconnection_queue_id"],
        "ProjectName": project_name,
        "CapacityMW": capacity_mw,
        "CapacitySourceField": capacity_source,
        "SubstationOrNode": substation_or_node,
        "Status": status,
        "DeveloperName": developer_name,
        "County": get_alias_value(lookup, ["county", "projectcounty", "nearesttownorcounty"]),
        "State": get_alias_value(lookup, ["state", "projectstate", "st"]),
        "Zone": get_alias_value(lookup, ["zone", "z", "cdrreportingzone"]),
        "FuelType": get_alias_value(lookup, ["fuel", "fueltype", "typefuel", "generationfuel1"]),
        "Extraction": extraction,
        "ExtractionConfidence": confidence,
        "ReviewStatus": review_status,
        "ReviewReasons": review_reasons,
        "SourceLocation": source_location,
        "SourceRowNumber": source_row_number,
        "SourceHeaderRowNumber": source_header_row_number,
        "SourceRow": compact_source_row(row),
    }
    return drop_none_values(record)


def row_lookup(row: dict[str, Any]) -> dict[str, tuple[str, str]]:
    lookup: dict[str, tuple[str, str]] = {}
    for key, value in row.items():
        cleaned = clean_cell(value)
        if not cleaned:
            continue
        lookup.setdefault(normalize_header(str(key)), (str(key), cleaned))
    return lookup


def get_alias_value(
    lookup: dict[str, tuple[str, str]],
    aliases: list[str],
) -> str | None:
    for alias in aliases:
        match = lookup.get(alias)
        if match:
            value = normalize_nullable_string(match[1])
            if value is not None:
                return value
    return None


def get_project_name(lookup: dict[str, tuple[str, str]]) -> str | None:
    if get_alias_value(lookup, ["commercialname"]):
        return get_alias_value(lookup, ["commercialname"])
    return get_alias_value(lookup, PROJECT_NAME_ALIASES)


def get_node_value(lookup: dict[str, tuple[str, str]]) -> str | None:
    return get_alias_value(lookup, NODE_ALIASES)


def choose_capacity(lookup: dict[str, tuple[str, str]]) -> tuple[int | None, str | None]:
    candidates: list[tuple[float, str]] = []
    for normalized_header, (source_header, value) in lookup.items():
        if not is_capacity_header(normalized_header, source_header):
            continue
        numeric = parse_numeric(value)
        if numeric is not None:
            candidates.append((numeric, source_header))

    if not candidates:
        return None, None
    positive = [candidate for candidate in candidates if candidate[0] > 0]
    numeric, source_header = max(positive or candidates, key=lambda item: item[0])
    return int(round(numeric)), source_header


def is_capacity_header(normalized_header: str, source_header: str) -> bool:
    haystack = f"{normalized_header} {source_header.lower()}"
    if any(token in haystack for token in CAPACITY_HEADER_EXCLUDES):
        return False
    return any(token in haystack for token in CAPACITY_HEADER_HINTS)


def infer_project_status(lookup: dict[str, tuple[str, str]]) -> str:
    if any(get_alias_value(lookup, [alias]) for alias in WITHDRAWN_DATE_ALIASES):
        return "Withdrawn"

    raw_status = get_alias_value(lookup, STATUS_ALIASES)
    if raw_status:
        try:
            return normalize_status(raw_status)
        except ValueError:
            lowered = raw_status.lower()
            if any(token in lowered for token in ("withdraw", "inactive", "ina")):
                return "Withdrawn"
            if any(
                token in lowered
                for token in ("done", "complete", "service", "energized", "commercial operation")
            ):
                return "Completed"
    return "Active"


def score_project_record(extraction: dict[str, Any]) -> tuple[float, str, list[str]]:
    score = 0.64
    reasons: list[str] = []
    if extraction.get("interconnection_queue_id"):
        score += 0.12
    else:
        reasons.append("Missing queue ID")
    if extraction.get("capacity_mw") is not None:
        score += 0.12
    else:
        reasons.append("Missing capacity MW")
    if extraction.get("substation_or_node"):
        score += 0.08
    else:
        reasons.append("Missing substation or node")
    if extraction.get("status") in ALLOWED_STATUSES:
        score += 0.04
    if extraction.get("developer_name"):
        score += 0.04

    score = max(0.0, min(0.99, round(score, 2)))
    blocking_reasons = {
        "Missing queue ID",
        "Missing capacity MW",
        "Missing substation or node",
    }
    review_status = (
        "NEEDS_REVIEW" if any(reason in blocking_reasons for reason in reasons) else "AUTO_REVIEWED"
    )
    return score, review_status, reasons


def build_project_key(
    project_id: str | None,
    project_name: str | None,
    substation_or_node: str | None,
) -> str:
    basis = project_id or "|".join(
        value for value in [project_name, substation_or_node] if value
    )
    if not basis:
        basis = "unknown"
    safe = sanitize_key_part(basis)
    if safe == "unknown":
        digest = hashlib.sha256(basis.encode("utf-8")).hexdigest()[:16]
        safe = f"project-{digest}"
    return safe[:120]


def compact_source_row(row: dict[str, Any]) -> dict[str, str]:
    compact: dict[str, str] = {}
    for key, value in row.items():
        cleaned = clean_cell(value)
        if cleaned:
            compact[str(key)[:80]] = cleaned[:500]
        if len(compact) >= 80:
            break
    return compact


def clean_cell(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()


def normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def parse_numeric(value: Any) -> float | None:
    match = re.search(r"-?\d+(?:\.\d+)?", str(value).replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def dedupe_project_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for record in records:
        key = str(record.get("ProjectKey") or "")
        if not key:
            continue
        existing = deduped.get(key)
        if not existing or project_record_completeness(record) > project_record_completeness(existing):
            deduped[key] = record
    return list(deduped.values())


def project_record_completeness(record: dict[str, Any]) -> int:
    return sum(
        1
        for key in [
            "InterconnectionQueueID",
            "ProjectName",
            "CapacityMW",
            "SubstationOrNode",
            "DeveloperName",
            "County",
            "State",
            "FuelType",
        ]
        if record.get(key) not in (None, "")
    )


def with_record_fingerprints(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for record in records:
        record = dict(record)
        record["RecordFingerprint"] = project_record_fingerprint(record)
        output.append(record)
    return output


def project_record_fingerprint(record: dict[str, Any]) -> str:
    ignored = {
        "RecordFingerprint",
        "DeltaType",
        "ChangedFields",
        "ExtractionConfidence",
        "ReviewStatus",
        "ReviewReasons",
        "SourceHeaderRowNumber",
        "SourceRowNumber",
    }
    payload = {key: value for key, value in record.items() if key not in ignored}
    return hashlib.sha256(
        json.dumps(payload, default=str, sort_keys=True).encode("utf-8")
    ).hexdigest()


def load_previous_project_snapshot(
    queue_id: str,
    bucket: str,
) -> tuple[str | None, list[dict[str, Any]] | None]:
    if state_table is None:
        return None, None
    try:
        response = state_table.get_item(Key={"QueueID": queue_id})
    except Exception:
        logger.exception("Failed to load parser state for queue_id=%s", queue_id)
        return None, None

    item = response.get("Item") or {}
    snapshot_key = item.get("LastProjectSnapshotS3Key")
    if not snapshot_key:
        return None, None

    try:
        response = s3.get_object(Bucket=bucket, Key=snapshot_key)
        payload = json.loads(response["Body"].read().decode("utf-8"))
        records = payload.get("records", [])
        if not isinstance(records, list):
            return str(snapshot_key), None
        return str(snapshot_key), records
    except Exception:
        logger.exception("Failed to load previous project snapshot s3://%s/%s", bucket, snapshot_key)
        return str(snapshot_key), None


def compare_project_records(
    previous_records: list[dict[str, Any]] | None,
    current_records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    if previous_records is None:
        baseline = top_project_records(current_records, BASELINE_SAMPLE_LIMIT)
        deltas = [project_delta(record, "BASELINE", []) for record in baseline]
        return deltas, {
            "baseline_records": len(current_records),
            "baseline_sample": len(deltas),
            "new": 0,
            "updated": 0,
            "removed": 0,
        }

    previous = index_project_records(previous_records)
    current = index_project_records(current_records)
    deltas: list[dict[str, Any]] = []
    summary = {
        "baseline_records": 0,
        "baseline_sample": 0,
        "new": 0,
        "updated": 0,
        "removed": 0,
    }

    for key, record in current.items():
        old = previous.get(key)
        if old is None:
            deltas.append(project_delta(record, "NEW", []))
            summary["new"] += 1
        elif project_record_fingerprint(old) != project_record_fingerprint(record):
            changed_fields = changed_project_fields(old, record)
            deltas.append(project_delta(record, "UPDATED", changed_fields))
            summary["updated"] += 1

    for key, old in previous.items():
        if key not in current:
            removed = dict(old)
            removed["Status"] = "Withdrawn"
            removed["Extraction"] = dict(removed.get("Extraction") or {})
            removed["Extraction"]["status"] = "Withdrawn"
            deltas.append(project_delta(removed, "REMOVED", ["Status"]))
            summary["removed"] += 1

    return deltas, summary


def index_project_records(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(record["ProjectKey"]): record
        for record in records
        if record.get("ProjectKey")
    }


def top_project_records(records: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return sorted(
        records,
        key=lambda record: int(record.get("CapacityMW") or -1),
        reverse=True,
    )[:limit]


def changed_project_fields(old: dict[str, Any], new: dict[str, Any]) -> list[str]:
    fields = [
        "InterconnectionQueueID",
        "ProjectName",
        "CapacityMW",
        "SubstationOrNode",
        "Status",
        "DeveloperName",
        "County",
        "State",
        "Zone",
        "FuelType",
    ]
    changed = [field for field in fields if old.get(field) != new.get(field)]
    return changed or ["SourceRow"]


def project_delta(
    record: dict[str, Any],
    delta_type: str,
    changed_fields: list[str],
) -> dict[str, Any]:
    delta = dict(record)
    delta["DeltaType"] = delta_type
    delta["ChangedFields"] = changed_fields
    return delta


def select_project_deltas(
    deltas: list[dict[str, Any]],
    previous_records: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    if previous_records is None:
        return deltas

    priority = {"REMOVED": 0, "NEW": 1, "UPDATED": 2, "BASELINE": 3}
    return sorted(
        deltas,
        key=lambda delta: (
            priority.get(str(delta.get("DeltaType")), 9),
            -int(delta.get("CapacityMW") or 0),
            str(delta.get("ProjectKey") or ""),
        ),
    )[:MAX_PROJECT_INSIGHTS_PER_SOURCE]


def write_project_snapshot(
    bucket: str,
    queue_id: str,
    observed_at: str,
    records: list[dict[str, Any]],
    event: dict[str, Any],
) -> str:
    safe_queue_id = sanitize_key_part(queue_id)
    safe_observed_at = sanitize_key_part(observed_at)
    prefix = PROJECT_SNAPSHOT_PREFIX if PROJECT_SNAPSHOT_PREFIX.endswith("/") else f"{PROJECT_SNAPSHOT_PREFIX}/"
    key = f"{prefix}{safe_queue_id}/{safe_observed_at}.json"
    payload = {
        "queue_id": queue_id,
        "observed_at": observed_at,
        "source_url": event.get("source_url"),
        "raw_s3_bucket": event.get("s3_bucket"),
        "raw_s3_key": event.get("s3_key"),
        "content_md5": event.get("content_md5"),
        "remote_fingerprint": event.get("remote_fingerprint"),
        "created_at": utc_now_iso(),
        "record_count": len(records),
        "records": records,
    }
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, default=json_default).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def extract_document_text(
    raw_bytes: bytes,
    s3_bucket: str,
    s3_key: str,
    content_type: str | None,
    content_length: int | None,
) -> tuple[str, list[str]]:
    lower_key = s3_key.lower()
    normalized_type = (content_type or "").split(";", 1)[0].strip().lower()
    notes: list[str] = []

    if lower_key.endswith(".xlsx") or normalized_type.endswith(
        "spreadsheetml.sheet"
    ):
        notes.append("Extracted worksheet text from XLSX XML parts")
        return extract_xlsx_text(raw_bytes), notes

    if lower_key.endswith(".html") or lower_key.endswith(".htm") or normalized_type in {
        "text/html",
        "application/xhtml+xml",
    }:
        notes.append("Extracted visible text from HTML")
        return html_to_text(decode_text(raw_bytes)), notes

    if lower_key.endswith(".pdf") or normalized_type == "application/pdf":
        notes.append("Attempted local best-effort PDF text extraction")
        local_error: Exception | None = None
        try:
            local_text = extract_pdf_text(raw_bytes)
            if (
                not ENABLE_TEXTRACT_FALLBACK
                or len(local_text) >= TEXTRACT_MIN_PDF_TEXT_CHARS
            ):
                notes.append("Used local PDF text extraction")
                return local_text, notes
            notes.append(
                "Local PDF text was below threshold; using Amazon Textract fallback"
            )
        except UnsupportedDocumentError as exc:
            local_error = exc
            notes.append(f"Local PDF text extraction failed: {exc}")

        if ENABLE_TEXTRACT_FALLBACK:
            textract_text = extract_pdf_text_with_textract(
                bucket=s3_bucket,
                key=s3_key,
                content_length=content_length,
            )
            notes.append("Extracted PDF text with Amazon Textract")
            return textract_text, notes

        if local_error:
            raise local_error
        raise UnsupportedDocumentError("PDF text extraction was below usable threshold")

    text = decode_text(raw_bytes)
    if not looks_like_text(text):
        raise UnsupportedDocumentError(
            f"Unsupported binary document type for {s3_key}; provide text, CSV, HTML, PDF, or XLSX"
        )
    notes.append("Decoded text payload")
    return text, notes


def decode_text(raw_bytes: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            text = raw_bytes.decode(encoding)
            if looks_like_text(text):
                return text
        except UnicodeDecodeError:
            continue
    return raw_bytes.decode("latin-1", errors="replace")


def looks_like_text(text: str) -> bool:
    sample = text[:10000]
    if not sample:
        return False
    printable = sum(1 for char in sample if char.isprintable() or char.isspace())
    return printable / len(sample) >= 0.85 and "\x00" not in sample


class VisibleTextParser(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self.skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"script", "style", "noscript"}:
            self.skip_depth += 1
        elif tag.lower() in {"br", "p", "div", "tr", "li", "table"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style", "noscript"} and self.skip_depth:
            self.skip_depth -= 1
        elif tag.lower() in {"p", "div", "tr", "li"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self.skip_depth:
            value = data.strip()
            if value:
                self.parts.append(value)


def html_to_text(html: str) -> str:
    parser = VisibleTextParser()
    parser.feed(html)
    text = " ".join(parser.parts)
    return normalize_whitespace(text)


def extract_xlsx_text(raw_bytes: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as workbook:
        shared_strings = read_xlsx_shared_strings(workbook)
        worksheet_names = sorted(
            name
            for name in workbook.namelist()
            if name.startswith("xl/worksheets/") and name.endswith(".xml")
        )
        rows: list[str] = []
        for worksheet_name in worksheet_names:
            rows.append(f"\n# {worksheet_name}")
            root = ElementTree.fromstring(workbook.read(worksheet_name))
            for row in root.iterfind(".//{*}row"):
                values = [
                    read_xlsx_cell(cell, shared_strings)
                    for cell in row.iterfind("{*}c")
                ]
                values = [value for value in values if value != ""]
                if values:
                    rows.append("\t".join(values))
    text = "\n".join(rows)
    if not text.strip():
        raise UnsupportedDocumentError("No readable worksheet text found in XLSX")
    return text


def read_xlsx_shared_strings(workbook: zipfile.ZipFile) -> list[str]:
    try:
        root = ElementTree.fromstring(workbook.read("xl/sharedStrings.xml"))
    except KeyError:
        return []

    strings: list[str] = []
    for item in root.iterfind(".//{*}si"):
        parts = [node.text or "" for node in item.iterfind(".//{*}t")]
        strings.append("".join(parts))
    return strings


def read_xlsx_cell(cell: ElementTree.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join(node.text or "" for node in cell.iterfind(".//{*}t")).strip()

    value_node = cell.find("{*}v")
    if value_node is None or value_node.text is None:
        return ""

    value = value_node.text.strip()
    if cell_type == "s":
        try:
            return shared_strings[int(value)].strip()
        except (IndexError, ValueError):
            return value
    return value


def extract_pdf_text(raw_bytes: bytes) -> str:
    fragments: list[str] = []
    for stream in re.finditer(rb"stream\r?\n(.*?)\r?\nendstream", raw_bytes, re.DOTALL):
        stream_bytes = stream.group(1).strip(b"\r\n")
        for candidate in inflate_candidates(stream_bytes):
            fragments.extend(extract_pdf_string_literals(candidate))

    if not fragments:
        fragments.extend(extract_pdf_string_literals(raw_bytes))

    text = normalize_whitespace(" ".join(fragments))
    if len(text) < 50:
        raise UnsupportedDocumentError(
            "Could not extract enough text from PDF with stdlib-only parser"
        )
    return text


def extract_pdf_text_with_textract(
    bucket: str,
    key: str,
    content_length: int | None,
) -> str:
    if content_length is not None and content_length <= TEXTRACT_SYNC_MAX_BYTES:
        try:
            logger.info("Calling Textract DetectDocumentText for s3://%s/%s", bucket, key)
            response = textract.detect_document_text(
                Document={
                    "S3Object": {
                        "Bucket": bucket,
                        "Name": key,
                    }
                }
            )
            text = textract_lines_to_text(response.get("Blocks", []))
            if text:
                return text
            logger.info("Textract synchronous response contained no LINE blocks")
        except Exception as exc:
            logger.info(
                "Textract synchronous extraction failed for s3://%s/%s: %s",
                bucket,
                key,
                exc,
            )

    return extract_pdf_text_with_async_textract(bucket, key)


def extract_pdf_text_with_async_textract(bucket: str, key: str) -> str:
    token = hashlib.sha256(f"{bucket}/{key}".encode("utf-8")).hexdigest()[:64]
    logger.info("Starting Textract async text detection for s3://%s/%s", bucket, key)
    start_response = textract.start_document_text_detection(
        ClientRequestToken=token,
        DocumentLocation={
            "S3Object": {
                "Bucket": bucket,
                "Name": key,
            }
        },
        JobTag=f"queuewatch-{token[:32]}",
    )
    job_id = start_response["JobId"]
    deadline = time.monotonic() + TEXTRACT_MAX_WAIT_SECONDS

    while True:
        response = textract.get_document_text_detection(JobId=job_id, MaxResults=1)
        status = response.get("JobStatus")
        if status in {"SUCCEEDED", "PARTIAL_SUCCESS"}:
            break
        if status == "FAILED":
            message = response.get("StatusMessage", "Textract job failed")
            raise UnsupportedDocumentError(message)
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Textract job {job_id} did not finish within {TEXTRACT_MAX_WAIT_SECONDS}s"
            )
        time.sleep(TEXTRACT_POLL_SECONDS)

    blocks: list[dict[str, Any]] = []
    next_token: str | None = None
    while True:
        kwargs: dict[str, Any] = {
            "JobId": job_id,
            "MaxResults": 1000,
        }
        if next_token:
            kwargs["NextToken"] = next_token
        page = textract.get_document_text_detection(**kwargs)
        if page.get("JobStatus") == "FAILED":
            message = page.get("StatusMessage", "Textract job failed")
            raise UnsupportedDocumentError(message)
        blocks.extend(page.get("Blocks", []))
        next_token = page.get("NextToken")
        if not next_token:
            break

    text = textract_lines_to_text(blocks)
    if not text:
        raise UnsupportedDocumentError("Textract returned no readable text")
    return text


def textract_lines_to_text(blocks: list[dict[str, Any]]) -> str:
    lines = [
        str(block.get("Text", "")).strip()
        for block in blocks
        if block.get("BlockType") == "LINE" and block.get("Text")
    ]
    return normalize_whitespace("\n".join(lines))


def inflate_candidates(stream_bytes: bytes) -> list[bytes]:
    candidates = [stream_bytes]
    try:
        candidates.append(zlib.decompress(stream_bytes))
    except zlib.error:
        pass
    return candidates


def extract_pdf_string_literals(raw_bytes: bytes) -> list[str]:
    fragments: list[str] = []
    for literal in re.findall(rb"\((?:\\.|[^\\)])*\)", raw_bytes):
        value = literal[1:-1]
        fragments.append(unescape_pdf_literal(value))
    return [fragment for fragment in fragments if fragment.strip()]


def unescape_pdf_literal(value: bytes) -> str:
    replacements = {
        rb"\n": b"\n",
        rb"\r": b"\r",
        rb"\t": b"\t",
        rb"\b": b"\b",
        rb"\f": b"\f",
        rb"\\": b"\\",
        rb"\(": b"(",
        rb"\)": b")",
    }
    for source, target in replacements.items():
        value = value.replace(source, target)
    return value.decode("latin-1", errors="ignore")


def normalize_whitespace(text: str) -> str:
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def invoke_bedrock(
    queue_id: str,
    source_url: str | None,
    s3_key: str,
    content_md5: str | None,
    document_text: str,
    extraction_notes: list[str],
) -> dict[str, Any]:
    user_prompt = {
        "queue_id_metadata": queue_id,
        "source_url": source_url,
        "s3_key": s3_key,
        "content_md5": content_md5,
        "extraction_notes": extraction_notes,
        "document_text": document_text,
    }
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": BEDROCK_MAX_TOKENS,
        "temperature": 0,
        "system": SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Extract the QueueWatch JSON object from this payload:\n"
                            f"{json.dumps(user_prompt, ensure_ascii=False)}"
                        ),
                    }
                ],
            }
        ],
    }

    logger.info("Invoking Bedrock model_id=%s queue_id=%s", BEDROCK_MODEL_ID, queue_id)
    response = bedrock.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        body=json.dumps(body).encode("utf-8"),
        contentType="application/json",
        accept="application/json",
    )
    response_body = json.loads(response["body"].read())
    text_parts = [
        block.get("text", "")
        for block in response_body.get("content", [])
        if block.get("type") == "text"
    ]
    raw_text = "\n".join(text_parts).strip()
    logger.info("Received Bedrock response length=%d", len(raw_text))
    return parse_json_object(raw_text)


def parse_json_object(raw_text: str) -> dict[str, Any]:
    try:
        value = json.loads(raw_text)
    except json.JSONDecodeError:
        start = raw_text.find("{")
        end = raw_text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("Bedrock response did not contain a JSON object")
        value = json.loads(raw_text[start : end + 1])

    if not isinstance(value, dict):
        raise ValueError("Bedrock response JSON must be an object")
    return value


def validate_extraction(raw: dict[str, Any]) -> dict[str, Any]:
    required_keys = {
        "interconnection_queue_id",
        "capacity_mw",
        "substation_or_node",
        "status",
        "developer_name",
    }
    missing = required_keys - set(raw)
    if missing:
        raise ValueError(f"Bedrock response missing required keys: {sorted(missing)}")

    capacity = normalize_capacity(raw.get("capacity_mw"))
    status = normalize_status(raw.get("status"))

    return {
        "interconnection_queue_id": normalize_nullable_string(
            raw.get("interconnection_queue_id")
        ),
        "capacity_mw": capacity,
        "substation_or_node": normalize_nullable_string(raw.get("substation_or_node")),
        "status": status,
        "developer_name": normalize_nullable_string(raw.get("developer_name")),
    }


def score_extraction(
    extraction: dict[str, Any],
    extraction_notes: list[str],
) -> tuple[float, str, list[str]]:
    score = 1.0
    reasons: list[str] = []

    required_signal_fields = {
        "interconnection_queue_id": "Missing queue ID",
        "capacity_mw": "Missing capacity MW",
        "substation_or_node": "Missing substation or node",
    }
    for key, reason in required_signal_fields.items():
        if extraction.get(key) in (None, ""):
            score -= 0.22
            reasons.append(reason)

    if extraction.get("status") not in ALLOWED_STATUSES:
        score -= 0.24
        reasons.append("Status is outside allowed values")

    if extraction.get("developer_name") in (None, ""):
        score -= 0.08
        reasons.append("Missing developer name")

    note_text = " ".join(extraction_notes).lower()
    if "truncated" in note_text:
        score -= 0.12
        reasons.append("Source text was truncated before LLM extraction")
    if "best-effort" in note_text or "failed" in note_text:
        score -= 0.08
        reasons.append("Text extraction required fallback handling")

    score = max(0.0, min(1.0, round(score, 2)))
    review_status = "AUTO_REVIEWED" if score >= 0.88 and not reasons else "NEEDS_REVIEW"
    return score, review_status, reasons


def normalize_capacity(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("capacity_mw must be an integer or null")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(round(value))
    if isinstance(value, str):
        match = re.search(r"-?\d+(?:\.\d+)?", value.replace(",", ""))
        if match:
            return int(round(float(match.group(0))))
    raise ValueError("capacity_mw must be an integer or null")


def normalize_status(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("status must be one of Active, Withdrawn, Completed")
    normalized = value.strip().title()
    aliases = {
        "Done": "Completed",
        "Done ": "Completed",
        "In Service": "Completed",
        "Commercial Operation": "Completed",
        "Commercial Operations": "Completed",
        "Complete": "Completed",
        "Completed": "Completed",
        "Operational": "Completed",
        "Energized": "Completed",
        "Withdraw": "Withdrawn",
        "Withdrawn": "Withdrawn",
        "Inactive": "Withdrawn",
        "Ina": "Withdrawn",
        "Active": "Active",
        "Under Study": "Active",
        "Planned": "Active",
        "Pln": "Active",
        "Queued": "Active",
        "Pending": "Active",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in ALLOWED_STATUSES:
        raise ValueError("status must be one of Active, Withdrawn, Completed")
    return normalized


def normalize_nullable_string(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized or normalized.lower() in {"n/a", "na", "none", "null", "unknown"}:
        return None
    return normalized


def write_output_json(
    bucket: str,
    queue_id: str,
    observed_at: str,
    payload: dict[str, Any],
) -> str:
    safe_queue_id = sanitize_key_part(queue_id)
    safe_observed_at = sanitize_key_part(observed_at)
    prefix = OUTPUT_PREFIX if OUTPUT_PREFIX.endswith("/") else f"{OUTPUT_PREFIX}/"
    output_key = f"{prefix}{safe_queue_id}/{safe_observed_at}.json"
    s3.put_object(
        Bucket=bucket,
        Key=output_key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return output_key


def persist_insight(
    queue_id: str,
    observed_at: str,
    event: dict[str, Any],
    extraction: dict[str, Any],
    extraction_confidence: float,
    review_status: str,
    review_reasons: list[str],
    output_key: str,
) -> None:
    item = {
        "QueueID": queue_id,
        "ObservedAt": observed_at,
        "InsightType": SOURCE_EXTRACTION_INSIGHT_TYPE,
        "ParsedAt": utc_now_iso(),
        "SourceURL": event.get("source_url"),
        "RawS3Bucket": event.get("s3_bucket"),
        "RawS3Key": event.get("s3_key"),
        "OutputS3Key": output_key,
        "ContentMD5": event.get("content_md5"),
        "RemoteFingerprint": event.get("remote_fingerprint"),
        "ModelID": BEDROCK_MODEL_ID,
        "InterconnectionQueueID": extraction["interconnection_queue_id"],
        "CapacityMW": extraction["capacity_mw"],
        "SubstationOrNode": extraction["substation_or_node"],
        "Status": extraction["status"],
        "DeveloperName": extraction["developer_name"],
        "ExtractionConfidence": Decimal(str(extraction_confidence)),
        "ReviewStatus": review_status,
        "ReviewReasons": review_reasons,
        "Extraction": extraction,
    }
    insights_table.put_item(Item=drop_none_values(item))


def persist_project_deltas(
    queue_id: str,
    observed_at: str,
    event: dict[str, Any],
    deltas: list[dict[str, Any]],
    output_key: str,
) -> None:
    queue_metadata = event.get("queue_metadata") or {}
    for delta in deltas:
        extraction = delta.get("Extraction") or {}
        item = {
            "QueueID": build_insight_queue_id(queue_id, str(delta.get("ProjectKey") or "")),
            "ObservedAt": observed_at,
            "InsightType": PROJECT_DELTA_INSIGHT_TYPE,
            "SourceQueueID": queue_id,
            "ProjectKey": delta.get("ProjectKey"),
            "DeltaType": delta.get("DeltaType"),
            "ChangedFields": delta.get("ChangedFields", []),
            "ParsedAt": utc_now_iso(),
            "SourceURL": event.get("source_url"),
            "CatalogSourceURL": event.get("catalog_source_url"),
            "RawS3Bucket": event.get("s3_bucket"),
            "RawS3Key": event.get("s3_key"),
            "OutputS3Key": output_key,
            "ContentMD5": event.get("content_md5"),
            "RemoteFingerprint": event.get("remote_fingerprint"),
            "ModelID": DETERMINISTIC_MODEL_ID,
            "UtilityName": queue_metadata.get("UtilityName"),
            "Market": queue_metadata.get("Market"),
            "Region": queue_metadata.get("Region"),
            "SourceKind": queue_metadata.get("SourceKind"),
            "InterconnectionQueueID": extraction.get("interconnection_queue_id"),
            "ProjectName": delta.get("ProjectName"),
            "CapacityMW": extraction.get("capacity_mw"),
            "CapacitySourceField": delta.get("CapacitySourceField"),
            "SubstationOrNode": extraction.get("substation_or_node"),
            "Status": extraction.get("status"),
            "DeveloperName": extraction.get("developer_name"),
            "County": delta.get("County"),
            "State": delta.get("State"),
            "Zone": delta.get("Zone"),
            "FuelType": delta.get("FuelType"),
            "ExtractionConfidence": Decimal(str(delta.get("ExtractionConfidence", 0.0))),
            "ReviewStatus": delta.get("ReviewStatus", "NEEDS_REVIEW"),
            "ReviewReasons": delta.get("ReviewReasons", []),
            "RecordFingerprint": delta.get("RecordFingerprint"),
            "SourceLocation": delta.get("SourceLocation"),
            "SourceRowNumber": delta.get("SourceRowNumber"),
            "SourceRow": delta.get("SourceRow", {}),
            "Extraction": extraction,
        }
        insights_table.put_item(Item=drop_none_values(item))


def build_insight_queue_id(source_queue_id: str, project_key: str) -> str:
    return sanitize_key_part(f"{source_queue_id}-{project_key}")[:220]


def mark_parser_status(
    queue_id: str,
    status: str,
    observed_at: str,
    output_key: str | None = None,
    extraction_confidence: float | None = None,
    review_status: str | None = None,
    project_snapshot_key: str | None = None,
    project_record_count: int | None = None,
    project_delta_count: int | None = None,
    project_delta_summary: dict[str, int] | None = None,
    error: Exception | None = None,
) -> None:
    if state_table is None:
        return

    expression = (
        "SET ParserStatus = :status, "
        "LastParserObservedAt = :observed_at, "
        "LastParsedAt = :parsed_at"
    )
    values: dict[str, Any] = {
        ":status": status,
        ":observed_at": observed_at,
        ":parsed_at": utc_now_iso(),
    }

    if output_key:
        expression += ", LastInsightS3Key = :output_key, LastParserError = :empty"
        values[":output_key"] = output_key
        values[":empty"] = ""

    if extraction_confidence is not None:
        expression += ", LastExtractionConfidence = :confidence"
        values[":confidence"] = Decimal(str(extraction_confidence))

    if review_status:
        expression += ", LastReviewStatus = :review_status"
        values[":review_status"] = review_status

    if project_snapshot_key:
        expression += ", LastProjectSnapshotS3Key = :project_snapshot_key"
        values[":project_snapshot_key"] = project_snapshot_key

    if project_record_count is not None:
        expression += ", LastProjectRecordCount = :project_record_count"
        values[":project_record_count"] = project_record_count

    if project_delta_count is not None:
        expression += ", LastProjectDeltaCount = :project_delta_count"
        values[":project_delta_count"] = project_delta_count

    if project_delta_summary is not None:
        expression += ", LastProjectDeltaSummary = :project_delta_summary"
        values[":project_delta_summary"] = project_delta_summary

    if error:
        expression += ", LastParserError = :error, LastParserErrorType = :error_type"
        values[":error"] = str(error)[:1000]
        values[":error_type"] = error.__class__.__name__

    try:
        state_table.update_item(
            Key={"QueueID": queue_id},
            UpdateExpression=expression,
            ExpressionAttributeValues=values,
        )
    except Exception:
        logger.exception("Failed to update parser status for queue_id=%s", queue_id)


def drop_none_values(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if item is not None}


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def sanitize_key_part(value: str) -> str:
    value = urllib.parse.unquote(str(value))
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.=T:Z")
    sanitized = "".join(char if char in allowed else "-" for char in value)
    return sanitized.strip("-")[:160] or "unknown"


def utc_now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
