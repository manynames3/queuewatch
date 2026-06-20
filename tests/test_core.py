import importlib
import io
import json
import os
import re
import unittest
from pathlib import Path
from unittest import mock


os.environ.setdefault("STATE_TABLE_NAME", "QueueWatch-State")
os.environ.setdefault("RAW_BUCKET_NAME", "queuewatch-test")
os.environ.setdefault("PARSER_FUNCTION_NAME", "queuewatch-parser")
os.environ.setdefault("INSIGHTS_TABLE_NAME", "QueueWatch-Insights")
os.environ.setdefault("REPORT_BUCKET_NAME", "queuewatch-test")
os.environ.setdefault("LEADS_TABLE_NAME", "QueueWatch-Leads")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

orchestrator = importlib.import_module("orchestrator")
parser = importlib.import_module("parser")
seed_queues = importlib.import_module("seed_queues")
reporter = importlib.import_module("reporter")
lead_capture = importlib.import_module("lead_capture")


class OrchestratorTests(unittest.TestCase):
    def test_fingerprint_prefers_etag(self) -> None:
        fingerprint = orchestrator.fingerprint_from_headers(
            {
                "ETag": '"abc123"',
                "Last-Modified": "Wed, 20 May 2026 12:00:00 GMT",
                "Content-Length": "100",
            }
        )
        self.assertEqual(fingerprint, 'etag:"abc123"')

    def test_fingerprint_uses_last_modified_and_length(self) -> None:
        fingerprint = orchestrator.fingerprint_from_headers(
            {
                "Last-Modified": "Wed, 20 May 2026 12:00:00 GMT",
                "Content-Length": "100",
            }
        )
        self.assertEqual(
            fingerprint,
            "metadata:last-modified=Wed, 20 May 2026 12:00:00 GMT;length=100",
        )

    def test_s3_key_sanitizes_queue_and_source_filename(self) -> None:
        key = orchestrator.build_s3_key(
            "dominion/nova test",
            "https://example.com/files/interconnection queue.xlsx",
            parser.dt.datetime(2026, 5, 21, 9, 0, tzinfo=parser.dt.UTC),
        )
        self.assertEqual(
            key,
            "raw/dominion-nova-test/2026/05/21/20260521T090000Z-interconnection-queue.xlsx",
        )

    def test_s3_key_uses_resolved_filename_hint(self) -> None:
        key = orchestrator.build_s3_key(
            "ercot-gis-report",
            "https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId=123",
            parser.dt.datetime(2026, 5, 21, 9, 0, tzinfo=parser.dt.UTC),
            filename_hint="GIS Report April 2026.xlsx",
        )
        self.assertEqual(
            key,
            "raw/ercot-gis-report/2026/05/21/20260521T090000Z-GIS-Report-April-2026.xlsx",
        )

    def test_request_headers_for_queue_supports_cookie_and_json_headers(self) -> None:
        headers = orchestrator.request_headers_for_queue(
            {
                "RequestCookie": "AspxAutoDetectCookieSupport=1",
                "RequestHeaders": json.dumps({"Accept": "text/html"}),
            }
        )
        self.assertEqual(headers["Cookie"], "AspxAutoDetectCookieSupport=1")
        self.assertEqual(headers["Accept"], "text/html")

    def test_resolve_ercot_doclist_picks_latest_matching_report(self) -> None:
        payload = {
            "ListDocsByRptTypeRes": {
                "DocumentList": [
                    {
                        "Document": {
                            "FriendlyName": "Co-located_Battery_Identification_Report_April_2026",
                            "Extension": "xlsx",
                            "DocID": "battery",
                            "PublishDate": "2026-05-05T14:31:25-05:00",
                        }
                    },
                    {
                        "Document": {
                            "FriendlyName": "GIS_Report_March2026",
                            "Extension": "xlsx",
                            "DocID": "march",
                            "PublishDate": "2026-04-02T15:47:02-05:00",
                        }
                    },
                    {
                        "Document": {
                            "FriendlyName": "GIS_Report_April2026",
                            "Extension": "xlsx",
                            "DocID": "april",
                            "PublishDate": "2026-05-01T13:53:52-05:00",
                        }
                    },
                ]
            }
        }

        class FakeResponse:
            status = 200
            headers = {}

            def __init__(self) -> None:
                self.body = io.BytesIO(json.dumps(payload).encode("utf-8"))

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self, size: int = -1) -> bytes:
                return self.body.read(size)

        with mock.patch.object(orchestrator.urllib.request, "urlopen", return_value=FakeResponse()):
            resolved = orchestrator.resolve_ercot_ice_doclist(
                {"Resolver": "ercot_ice_doclist"},
                "https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=15933",
            )

        self.assertEqual(
            resolved.source_url,
            "https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId=april",
        )
        self.assertEqual(resolved.filename_hint, "GIS_Report_April2026.xlsx")


class ParserTests(unittest.TestCase):
    def test_validate_extraction_normalizes_supported_fields(self) -> None:
        extraction = parser.validate_extraction(
            {
                "interconnection_queue_id": " QW-123 ",
                "capacity_mw": "50.2 MW",
                "substation_or_node": " Oak Creek 230 ",
                "status": "queued",
                "developer_name": "Example Solar LLC",
            }
        )
        self.assertEqual(
            extraction,
            {
                "interconnection_queue_id": "QW-123",
                "capacity_mw": 50,
                "substation_or_node": "Oak Creek 230",
                "status": "Active",
                "developer_name": "Example Solar LLC",
            },
        )

    def test_extract_document_text_decodes_plain_text(self) -> None:
        raw = Path("tests/fixtures/sample_queue.txt").read_bytes()
        text, notes = parser.extract_document_text(
            raw_bytes=raw,
            s3_bucket="queuewatch-test",
            s3_key="raw/sample.txt",
            content_type="text/plain",
            content_length=len(raw),
        )
        self.assertIn("QW-SMOKE-001", text)
        self.assertEqual(notes, ["Decoded text payload"])

    def test_score_extraction_flags_complete_record(self) -> None:
        extraction = {
            "interconnection_queue_id": "QW-SMOKE-001",
            "capacity_mw": 50,
            "substation_or_node": "Oak Creek 230",
            "status": "Active",
            "developer_name": "Example Solar LLC",
        }
        confidence, review_status, reasons = parser.score_extraction(extraction, [])
        self.assertGreaterEqual(confidence, 0.9)
        self.assertEqual(review_status, "AUTO_REVIEWED")
        self.assertEqual(reasons, [])

    def test_extract_csv_project_records_normalizes_real_queue_columns(self) -> None:
        raw = (
            "Last Updated On,5/21/2026,\n"
            "Generation Interconnection Number,Nearest Town or County,State,Capacity,Fuel Type\n"
            "GI-TC-2024-31,Elbert County,CO,398,Solar/Storage\n"
        ).encode("utf-8")
        records = parser.extract_csv_project_records(raw, "spp-gi-active-requests")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["InterconnectionQueueID"], "GI-TC-2024-31")
        self.assertEqual(records[0]["CapacityMW"], 398)
        self.assertEqual(records[0]["SubstationOrNode"], "Elbert County")
        self.assertEqual(records[0]["Status"], "Active")

    def test_project_delta_comparison_detects_updated_capacity(self) -> None:
        base = parser.normalize_project_record(
            {
                "Queue Number": "2207",
                "Project Name": "Alisa Solar",
                "NET MW POI": "500",
                "POI": "North Gila",
            },
            source_queue_id="caiso-cluster-15-queue",
            source_location="sheet1",
        )
        updated = dict(base)
        updated["CapacityMW"] = 525
        updated["Extraction"] = dict(updated["Extraction"])
        updated["Extraction"]["capacity_mw"] = 525
        updated["RecordFingerprint"] = parser.project_record_fingerprint(updated)
        base["RecordFingerprint"] = parser.project_record_fingerprint(base)

        deltas, summary = parser.compare_project_records([base], [updated])
        self.assertEqual(summary["updated"], 1)
        self.assertEqual(deltas[0]["DeltaType"], "UPDATED")
        self.assertIn("CapacityMW", deltas[0]["ChangedFields"])

    def test_project_delta_comparison_baselines_without_flooding(self) -> None:
        records = []
        for index in range(30):
            record = parser.normalize_project_record(
                {
                    "Queue Pos.": f"{index:04d}",
                    "Project Name": f"Project {index}",
                    "SP (MW)": str(index + 1),
                    "Points of Interconnection": "Oak Creek",
                },
                source_queue_id="nyiso-interconnection-queue",
                source_location="sheet1",
            )
            records.append(record)
        records = parser.with_record_fingerprints(records)

        deltas, summary = parser.compare_project_records(None, records)
        self.assertEqual(summary["baseline_records"], 30)
        self.assertLessEqual(len(deltas), parser.BASELINE_SAMPLE_LIMIT)
        self.assertEqual({delta["DeltaType"] for delta in deltas}, {"BASELINE"})

    def test_write_project_snapshot_serializes_records(self) -> None:
        record = parser.normalize_project_record(
            {
                "Queue Number": "2207",
                "Project Name": "Alisa Solar",
                "NET MW POI": "500",
                "POI": "North Gila",
            },
            source_queue_id="caiso-cluster-15-queue",
            source_location="sheet1",
        )
        record["RecordFingerprint"] = parser.project_record_fingerprint(record)

        with mock.patch.object(parser.s3, "put_object") as put_object:
            key = parser.write_project_snapshot(
                bucket="queuewatch-test",
                queue_id="caiso-cluster-15-queue",
                observed_at="2026-05-21T19:42:32Z",
                records=[record],
                event={"source_url": "https://example.com/source.xlsx"},
            )

        self.assertTrue(key.startswith("project-snapshots/caiso-cluster-15-queue/"))
        payload = json.loads(put_object.call_args.kwargs["Body"].decode("utf-8"))
        self.assertEqual(payload["record_count"], 1)
        self.assertEqual(payload["records"][0]["CapacityMW"], 500)


class SeedQueueTests(unittest.TestCase):
    def test_normalize_record_preserves_extra_metadata(self) -> None:
        record = seed_queues.normalize_record(
            {
                "queue_id": "dominion-nova",
                "url": "https://example.com/queue.xlsx",
                "active": "yes",
                "utility": "Dominion Energy",
                "Territory": "Northern Virginia",
            },
            default_active=False,
        )
        self.assertEqual(record["QueueID"], "dominion-nova")
        self.assertEqual(record["SourceURL"], "https://example.com/queue.xlsx")
        self.assertTrue(record["Active"])
        self.assertEqual(record["UtilityName"], "Dominion Energy")
        self.assertEqual(record["Territory"], "Northern Virginia")


class ReporterTests(unittest.TestCase):
    def test_build_report_summarizes_signals(self) -> None:
        report = reporter.build_report(
            [
                {
                    "QueueID": "queuewatch-smoke-test",
                    "ObservedAt": "2026-05-21T09:08:00Z",
                    "InterconnectionQueueID": "QW-SMOKE-001",
                    "CapacityMW": 50,
                    "SubstationOrNode": "Oak Creek 230",
                    "Status": "Active",
                    "DeveloperName": "Example Solar LLC",
                    "ExtractionConfidence": parser.Decimal("0.95"),
                    "ReviewStatus": "AUTO_REVIEWED",
                }
            ],
            "2026-05-21T10:00:00Z",
        )
        self.assertEqual(report["schema_version"], "queuewatch.report.v1")
        self.assertEqual(report["summary"]["total_signals"], 1)
        self.assertEqual(report["summary"]["total_capacity_mw"], 50)
        self.assertIn("evidence_key", report["signals"][0])
        self.assertIn("parser_status", report["signals"][0])
        self.assertEqual(report["source_health"][0]["source_queue_id"], "queuewatch-smoke-test")
        self.assertIn("limitations", report)
        self.assertIn("QueueWatch Daily Signal Report", reporter.render_text_report(report))

    def test_build_report_prefers_project_delta_rows(self) -> None:
        report = reporter.build_report(
            [
                {
                    "QueueID": "source-only",
                    "ObservedAt": "2026-05-21T09:00:00Z",
                    "InsightType": "SOURCE_EXTRACTION",
                    "InterconnectionQueueID": "source-only",
                    "CapacityMW": 999,
                    "Status": "Active",
                    "ReviewStatus": "NEEDS_REVIEW",
                },
                {
                    "QueueID": "pjm-planning-queues-AE2-417",
                    "SourceQueueID": "pjm-planning-queues",
                    "ObservedAt": "2026-05-21T09:01:00Z",
                    "InsightType": "PROJECT_DELTA",
                    "DeltaType": "UPDATED",
                    "Market": "PJM",
                    "InterconnectionQueueID": "AE2-417",
                    "ProjectName": "Oak Creek",
                    "CapacityMW": 175,
                    "SubstationOrNode": "Oak Creek 230 kV",
                    "Status": "Active",
                    "ReviewStatus": "AUTO_REVIEWED",
                },
            ],
            "2026-05-21T10:00:00Z",
        )
        self.assertEqual(report["summary"]["total_signals"], 1)
        self.assertEqual(report["summary"]["updated_signals"], 1)
        self.assertEqual(report["summary"]["total_capacity_mw"], 175)


class LeadCaptureTests(unittest.TestCase):
    def test_validate_payload_requires_commercial_context(self) -> None:
        lead = lead_capture.validate_payload(
            {
                "email": "buyer@example.com",
                "company": "Example Energy",
                "role": "Power Strategy",
                "territories": "Northern Virginia, PJM",
                "use_case": "Monitor queue withdrawals near target sites",
            }
        )
        self.assertEqual(lead["Email"], "buyer@example.com")
        self.assertEqual(lead["Company"], "Example Energy")
        self.assertEqual(lead["Status"], "NEW")

    def test_validate_payload_rejects_bad_email(self) -> None:
        with self.assertRaises(ValueError):
            lead_capture.validate_payload(
                {
                    "email": "not-an-email",
                    "company": "Example Energy",
                    "territories": "PJM",
                    "use_case": "Monitor changes",
                }
            )


class TerraformSqsTests(unittest.TestCase):
    def test_all_sqs_queues_enable_long_polling(self) -> None:
        terraform = Path("main.tf").read_text()
        queue_blocks = re.findall(
            r'resource "aws_sqs_queue" "[^"]+" \{(.*?)\n\}',
            terraform,
            flags=re.DOTALL,
        )

        self.assertTrue(queue_blocks)
        for queue_block in queue_blocks:
            self.assertRegex(queue_block, r"receive_wait_time_seconds\s*=\s*20")


if __name__ == "__main__":
    unittest.main()
