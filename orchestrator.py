import datetime as dt
import hashlib
import json
import logging
import mimetypes
import os
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

import boto3


logger = logging.getLogger()
logger.setLevel(logging.INFO)

STATE_TABLE_NAME = os.environ["STATE_TABLE_NAME"]
RAW_BUCKET_NAME = os.environ["RAW_BUCKET_NAME"]
PARSER_FUNCTION_NAME = os.environ["PARSER_FUNCTION_NAME"]

HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
MAX_LIGHTWEIGHT_BYTES = int(os.getenv("MAX_LIGHTWEIGHT_BYTES", str(1024 * 1024)))
MAX_DOWNLOAD_BYTES = int(os.getenv("MAX_DOWNLOAD_BYTES", str(50 * 1024 * 1024)))
USER_AGENT = os.getenv("QUEUEWATCH_USER_AGENT", "QueueWatch/1.0")

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
lambda_client = boto3.client("lambda")
state_table = dynamodb.Table(STATE_TABLE_NAME)


@dataclass(frozen=True)
class RemoteFingerprint:
    value: str | None
    method: str
    status_code: int | None
    headers: dict[str, str]


@dataclass(frozen=True)
class DownloadedDocument:
    s3_bucket: str
    s3_key: str
    content_md5: str
    byte_count: int
    content_type: str


@dataclass(frozen=True)
class ResolvedSource:
    source_url: str
    filename_hint: str | None = None
    metadata: dict[str, str] | None = None


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    logger.info("Starting QueueWatch orchestrator scan")
    queues = list_active_queues()
    summary = {
        "total_targets": len(queues),
        "changed": 0,
        "unchanged": 0,
        "errors": 0,
        "parser_invocations": 0,
    }

    for queue in queues:
        queue_id = str(queue.get("QueueID", "")).strip()
        source_url = get_source_url(queue)

        if not queue_id or not source_url:
            logger.error("Skipping malformed queue item: %s", safe_json(queue))
            summary["errors"] += 1
            continue

        try:
            result = process_queue(queue, queue_id, source_url)
            summary[result] += 1
            if result == "changed":
                summary["parser_invocations"] += 1
        except Exception as exc:
            summary["errors"] += 1
            logger.exception("Queue processing failed for %s: %s", queue_id, exc)
            mark_queue_error(queue_id, exc)

    logger.info("QueueWatch orchestrator completed: %s", summary)
    return summary


def process_queue(queue: dict[str, Any], queue_id: str, source_url: str) -> str:
    logger.info("Checking queue_id=%s source_url=%s", queue_id, source_url)
    checked_at = utc_now_iso()
    resolved = resolve_source(queue, source_url)
    request_headers = request_headers_for_queue(queue)
    remote = fetch_remote_fingerprint(resolved.source_url, request_headers)
    stored_fingerprint = get_stored_fingerprint(queue)

    if remote.value and remote.value == stored_fingerprint:
        logger.info("No remote change detected for queue_id=%s", queue_id)
        update_queue_unchanged(queue_id, checked_at, remote, resolved)
        return "unchanged"

    logger.info(
        "Change suspected for queue_id=%s old_fingerprint=%s new_fingerprint=%s",
        queue_id,
        stored_fingerprint,
        remote.value,
    )
    downloaded = download_to_s3(queue_id, resolved.source_url, remote, request_headers, resolved.filename_hint)
    content_fingerprint = f"md5:{downloaded.content_md5}"

    if not remote.value and stored_fingerprint in {content_fingerprint, downloaded.content_md5}:
        logger.info("Downloaded content hash unchanged for queue_id=%s", queue_id)
        update_queue_unchanged(
            queue_id,
            checked_at,
            RemoteFingerprint(
                value=content_fingerprint,
                method="full-download-md5",
                status_code=remote.status_code,
                headers=remote.headers,
            ),
            resolved,
        )
        return "unchanged"

    observed_at = utc_now_iso()
    remote_fingerprint = remote.value or content_fingerprint
    update_queue_changed(
        queue_id=queue_id,
        checked_at=checked_at,
        observed_at=observed_at,
        remote=remote,
        remote_fingerprint=remote_fingerprint,
        downloaded=downloaded,
        resolved=resolved,
    )
    invoke_parser(
        {
            "queue_id": queue_id,
            "source_url": resolved.source_url,
            "catalog_source_url": source_url,
            "s3_bucket": downloaded.s3_bucket,
            "s3_key": downloaded.s3_key,
            "content_md5": downloaded.content_md5,
            "content_type": downloaded.content_type,
            "remote_fingerprint": remote_fingerprint,
            "observed_at": observed_at,
            "source_metadata": resolved.metadata or {},
            "queue_metadata": queue_metadata(queue),
        }
    )
    return "changed"


def list_active_queues() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    scan_kwargs: dict[str, Any] = {}

    while True:
        response = state_table.scan(**scan_kwargs)
        for item in response.get("Items", []):
            if is_active_queue(item):
                items.append(item)

        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    logger.info("Loaded %d active queue targets from %s", len(items), STATE_TABLE_NAME)
    return items


def is_active_queue(item: dict[str, Any]) -> bool:
    active = item.get("Active", True)
    if isinstance(active, str):
        return active.strip().lower() in {"1", "true", "yes", "y", "active"}
    return bool(active)


def get_source_url(item: dict[str, Any]) -> str | None:
    for key in ("SourceURL", "source_url", "URL", "Url"):
        value = item.get(key)
        if value:
            return str(value).strip()
    return None


def get_stored_fingerprint(item: dict[str, Any]) -> str | None:
    for key in ("LastRemoteFingerprint", "LastHash", "ETag", "LastContentMD5"):
        value = item.get(key)
        if value:
            value = str(value).strip()
            if key == "LastContentMD5" and not value.startswith("md5:"):
                return f"md5:{value}"
            return value
    return None


def queue_metadata(queue: dict[str, Any]) -> dict[str, str]:
    keys = [
        "UtilityName",
        "Market",
        "Region",
        "SourceKind",
        "RefreshCadence",
        "OfficialSourcePage",
        "Notes",
    ]
    return {
        key: str(queue[key])
        for key in keys
        if queue.get(key) not in (None, "")
    }


def resolve_source(queue: dict[str, Any], source_url: str) -> ResolvedSource:
    resolver = str(queue.get("Resolver") or queue.get("SourceResolver") or "").strip().lower()
    if resolver in {"", "direct"}:
        return ResolvedSource(source_url=source_url)
    if resolver == "ercot_ice_doclist":
        return resolve_ercot_ice_doclist(queue, source_url)
    raise ValueError(f"Unsupported source resolver: {resolver}")


def resolve_ercot_ice_doclist(queue: dict[str, Any], source_url: str) -> ResolvedSource:
    prefix = str(queue.get("DocumentNamePrefix") or "GIS_Report_").strip()
    extension = str(queue.get("DocumentExtension") or "xlsx").strip().lower()
    download_base = str(
        queue.get("DownloadBaseURL")
        or "https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId="
    )

    logger.info("Resolving ERCOT ICE document list for %s", source_url)
    request = urllib.request.Request(
        source_url,
        method="GET",
        headers=base_request_headers(),
    )
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        payload = json.loads(read_limited(response, MAX_LIGHTWEIGHT_BYTES).decode("utf-8"))

    documents = payload.get("ListDocsByRptTypeRes", {}).get("DocumentList", [])
    matches: list[dict[str, Any]] = []
    for entry in documents:
        document = entry.get("Document", {})
        friendly_name = str(document.get("FriendlyName") or "")
        document_extension = str(document.get("Extension") or "").lower()
        if friendly_name.startswith(prefix) and document_extension == extension:
            matches.append(document)

    if not matches:
        raise ValueError(
            f"No ERCOT document matched prefix={prefix!r} extension={extension!r}"
        )

    matches.sort(key=lambda item: str(item.get("PublishDate") or ""), reverse=True)
    latest = matches[0]
    doc_id = str(latest.get("DocID") or "").strip()
    if not doc_id:
        raise ValueError("Latest ERCOT document did not include DocID")

    friendly_name = str(latest.get("FriendlyName") or f"ercot-{doc_id}")
    filename_hint = f"{friendly_name}.{extension}"
    resolved_url = f"{download_base}{urllib.parse.quote(doc_id)}"
    return ResolvedSource(
        source_url=resolved_url,
        filename_hint=filename_hint,
        metadata={
            "resolver": "ercot_ice_doclist",
            "catalog_url": source_url,
            "doc_id": doc_id,
            "friendly_name": friendly_name,
            "publish_date": str(latest.get("PublishDate") or ""),
            "constructed_name": str(latest.get("ConstructedName") or ""),
        },
    )


def request_headers_for_queue(queue: dict[str, Any]) -> dict[str, str]:
    headers: dict[str, str] = {}
    raw_headers = queue.get("RequestHeaders") or queue.get("HTTPHeaders")
    if isinstance(raw_headers, dict):
        headers.update({str(key): str(value) for key, value in raw_headers.items()})
    elif isinstance(raw_headers, str) and raw_headers.strip():
        parsed = json.loads(raw_headers)
        if not isinstance(parsed, dict):
            raise ValueError("RequestHeaders must be a JSON object")
        headers.update({str(key): str(value) for key, value in parsed.items()})

    cookie = queue.get("RequestCookie") or queue.get("Cookie")
    if cookie:
        headers["Cookie"] = str(cookie)

    return headers


def fetch_remote_fingerprint(
    source_url: str,
    extra_headers: dict[str, str] | None = None,
) -> RemoteFingerprint:
    try:
        status, headers = http_request_headers(source_url, method="HEAD", extra_headers=extra_headers)
        fingerprint = fingerprint_from_headers(headers)
        if fingerprint:
            return RemoteFingerprint(fingerprint, "HEAD", status, headers)
        logger.info("HEAD returned no reliable fingerprint; using bounded GET for %s", source_url)
    except urllib.error.HTTPError as exc:
        if exc.code not in {403, 405, 406, 501}:
            raise
        logger.info("HEAD unsupported for %s status=%s; using bounded GET", source_url, exc.code)
    except urllib.error.URLError as exc:
        logger.info("HEAD failed for %s reason=%s; using bounded GET", source_url, exc.reason)

    return fingerprint_with_bounded_get(source_url, extra_headers)


def base_request_headers() -> dict[str, str]:
    return {
        "Accept": "*/*",
        "User-Agent": USER_AGENT,
    }


def merged_request_headers(extra_headers: dict[str, str] | None = None) -> dict[str, str]:
    headers = base_request_headers()
    if extra_headers:
        headers.update(extra_headers)
    return headers


def http_request_headers(
    source_url: str,
    method: str,
    extra_headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str]]:
    request = urllib.request.Request(
        source_url,
        method=method,
        headers=merged_request_headers(extra_headers),
    )
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        return response.status, dict(response.headers.items())


def fingerprint_from_headers(headers: dict[str, str]) -> str | None:
    etag = header_value(headers, "ETag")
    if etag:
        return f"etag:{etag.strip()}"

    last_modified = header_value(headers, "Last-Modified")
    content_length = header_value(headers, "Content-Length")
    if last_modified:
        return f"metadata:last-modified={last_modified};length={content_length or 'unknown'}"

    return None


def fingerprint_with_bounded_get(
    source_url: str,
    extra_headers: dict[str, str] | None = None,
) -> RemoteFingerprint:
    request = urllib.request.Request(
        source_url,
        method="GET",
        headers=merged_request_headers(
            {
            "Range": f"bytes=0-{MAX_LIGHTWEIGHT_BYTES - 1}",
                **(extra_headers or {}),
            }
        ),
    )

    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        body = read_limited(response, MAX_LIGHTWEIGHT_BYTES)
        headers = dict(response.headers.items())
        fingerprint = fingerprint_from_headers(headers)
        if not fingerprint:
            content_length = header_value(headers, "Content-Length")
            content_range = header_value(headers, "Content-Range")
            sample_hash = hashlib.md5(body, usedforsecurity=False).hexdigest()
            fingerprint = (
                f"probe-md5:{sample_hash};bytes={len(body)};"
                f"length={content_length or 'unknown'};range={content_range or 'none'}"
            )
        return RemoteFingerprint(fingerprint, "GET_RANGE", response.status, headers)


def read_limited(response: Any, max_bytes: int) -> bytes:
    chunks: list[bytes] = []
    remaining = max_bytes
    while remaining > 0:
        chunk = response.read(min(64 * 1024, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def download_to_s3(
    queue_id: str,
    source_url: str,
    remote: RemoteFingerprint,
    extra_headers: dict[str, str] | None = None,
    filename_hint: str | None = None,
) -> DownloadedDocument:
    observed_at = dt.datetime.now(dt.UTC)
    s3_key = build_s3_key(queue_id, source_url, observed_at, filename_hint)
    request = urllib.request.Request(
        source_url,
        method="GET",
        headers=merged_request_headers(extra_headers),
    )

    hasher = hashlib.md5(usedforsecurity=False)
    total_bytes = 0

    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        headers = dict(response.headers.items())
        content_type = normalized_content_type(
            header_value(headers, "Content-Type"),
            source_url,
            filename_hint,
        )

        with tempfile.NamedTemporaryFile(mode="wb", dir="/tmp") as tmp_file:
            while True:
                chunk = response.read(256 * 1024)
                if not chunk:
                    break
                total_bytes += len(chunk)
                if total_bytes > MAX_DOWNLOAD_BYTES:
                    raise ValueError(
                        f"Remote document exceeds MAX_DOWNLOAD_BYTES={MAX_DOWNLOAD_BYTES}"
                    )
                hasher.update(chunk)
                tmp_file.write(chunk)

            tmp_file.flush()
            metadata = {
                "queue-id": s3_metadata_value(queue_id, 160),
                "source-url": s3_metadata_value(source_url, 512),
                "content-md5": hasher.hexdigest(),
                "remote-fingerprint": s3_metadata_value(remote.value or "", 512),
                "fingerprint-method": s3_metadata_value(remote.method, 64),
                "observed-at": observed_at.isoformat().replace("+00:00", "Z"),
            }
            s3.upload_file(
                tmp_file.name,
                RAW_BUCKET_NAME,
                s3_key,
                ExtraArgs={
                    "ContentType": content_type,
                    "Metadata": metadata,
                },
            )

    logger.info(
        "Downloaded queue_id=%s bytes=%d md5=%s s3://%s/%s",
        queue_id,
        total_bytes,
        hasher.hexdigest(),
        RAW_BUCKET_NAME,
        s3_key,
    )
    return DownloadedDocument(
        s3_bucket=RAW_BUCKET_NAME,
        s3_key=s3_key,
        content_md5=hasher.hexdigest(),
        byte_count=total_bytes,
        content_type=content_type,
    )


def build_s3_key(
    queue_id: str,
    source_url: str,
    observed_at: dt.datetime,
    filename_hint: str | None = None,
) -> str:
    parsed = urllib.parse.urlparse(source_url)
    filename = filename_hint or os.path.basename(parsed.path) or "queue-document"
    filename = urllib.parse.unquote(filename).strip() or "queue-document"
    filename = sanitize_key_part(filename)
    if "." not in filename:
        filename = f"{filename}.bin"

    timestamp = observed_at.strftime("%Y%m%dT%H%M%SZ")
    safe_queue_id = sanitize_key_part(queue_id)
    return (
        f"raw/{safe_queue_id}/{observed_at:%Y/%m/%d}/"
        f"{timestamp}-{filename}"
    )


def sanitize_key_part(value: str) -> str:
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.=")
    sanitized = "".join(char if char in allowed else "-" for char in value)
    return sanitized.strip("-")[:160] or "unknown"


def s3_metadata_value(value: str, max_len: int) -> str:
    return str(value).encode("ascii", errors="ignore").decode("ascii")[:max_len]


def normalized_content_type(
    content_type: str | None,
    source_url: str,
    filename_hint: str | None = None,
) -> str:
    if content_type:
        return content_type.split(";", 1)[0].strip() or "application/octet-stream"
    guessed, _ = mimetypes.guess_type(filename_hint or source_url)
    return guessed or "application/octet-stream"


def update_queue_unchanged(
    queue_id: str,
    checked_at: str,
    remote: RemoteFingerprint,
    resolved: ResolvedSource | None = None,
) -> None:
    expression = (
        "SET LastCheckedAt = :checked_at, "
        "LastRemoteFingerprint = :fingerprint, "
        "LastFingerprintMethod = :method, "
        "LastHTTPStatus = :status, "
        "LastError = :empty"
    )
    values: dict[str, Any] = {
        ":checked_at": checked_at,
        ":fingerprint": remote.value or "",
        ":method": remote.method,
        ":status": remote.status_code or 0,
        ":empty": "",
    }
    if resolved:
        expression += ", LastResolvedSourceURL = :resolved_source_url"
        values[":resolved_source_url"] = resolved.source_url

    state_table.update_item(
        Key={"QueueID": queue_id},
        UpdateExpression=expression,
        ExpressionAttributeValues=values,
    )


def update_queue_changed(
    queue_id: str,
    checked_at: str,
    observed_at: str,
    remote: RemoteFingerprint,
    remote_fingerprint: str,
    downloaded: DownloadedDocument,
    resolved: ResolvedSource,
) -> None:
    state_table.update_item(
        Key={"QueueID": queue_id},
        UpdateExpression=(
            "SET LastCheckedAt = :checked_at, "
            "LastChangedAt = :changed_at, "
            "LastRemoteFingerprint = :fingerprint, "
            "LastFingerprintMethod = :method, "
            "LastContentMD5 = :content_md5, "
            "LastS3Bucket = :s3_bucket, "
            "LastS3Key = :s3_key, "
            "LastContentType = :content_type, "
            "LastContentBytes = :content_bytes, "
            "LastHTTPStatus = :status, "
            "LastResolvedSourceURL = :resolved_source_url, "
            "ParserStatus = :parser_status, "
            "LastError = :empty"
        ),
        ExpressionAttributeValues={
            ":checked_at": checked_at,
            ":changed_at": observed_at,
            ":fingerprint": remote_fingerprint,
            ":method": remote.method,
            ":content_md5": downloaded.content_md5,
            ":s3_bucket": downloaded.s3_bucket,
            ":s3_key": downloaded.s3_key,
            ":content_type": downloaded.content_type,
            ":content_bytes": downloaded.byte_count,
            ":status": remote.status_code or 0,
            ":resolved_source_url": resolved.source_url,
            ":parser_status": "PENDING",
            ":empty": "",
        },
    )


def mark_queue_error(queue_id: str, exc: Exception) -> None:
    try:
        state_table.update_item(
            Key={"QueueID": queue_id},
            UpdateExpression=(
                "SET LastCheckedAt = :checked_at, "
                "LastError = :error_message, "
                "LastErrorType = :error_type"
            ),
            ExpressionAttributeValues={
                ":checked_at": utc_now_iso(),
                ":error_message": str(exc)[:1000],
                ":error_type": exc.__class__.__name__,
            },
        )
    except Exception:
        logger.exception("Failed to persist queue error for queue_id=%s", queue_id)


def invoke_parser(payload: dict[str, Any]) -> None:
    logger.info(
        "Invoking parser asynchronously for queue_id=%s s3_key=%s",
        payload["queue_id"],
        payload["s3_key"],
    )
    lambda_client.invoke(
        FunctionName=PARSER_FUNCTION_NAME,
        InvocationType="Event",
        Payload=json.dumps(payload).encode("utf-8"),
    )


def header_value(headers: dict[str, str], name: str) -> str | None:
    for key, value in headers.items():
        if key.lower() == name.lower():
            return str(value).strip()
    return None


def utc_now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")


def safe_json(value: Any) -> str:
    try:
        return json.dumps(value, default=str, sort_keys=True)
    except Exception:
        return str(value)
