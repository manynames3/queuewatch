# QueueWatch Source Coverage

`queues.production.csv` contains the first production source catalog for public interconnection queue monitoring. Each row is a source-level target; the parser now normalizes structured XLSX, CSV, XML, and HTML targets into project-level rows and stores source snapshots for delta comparison.

## Active Sources

| QueueID | Market | Source | Format | Notes |
| --- | --- | --- | --- | --- |
| `caiso-public-queue-report` | CAISO | Public Queue Report | PDF | Daily CAISO public queue report. |
| `caiso-cluster-15-queue` | CAISO | Cluster 15 Queue Report | XLSX | Current cluster interconnection requests. |
| `miso-eras-interconnection-requests` | MISO | ERAS Interconnection Requests | XLSX | MISO public interconnection requests spreadsheet. |
| `spp-gi-active-requests` | SPP | GI Active Requests CSV export | CSV | Direct CSV export from SPP OpsPortal. |
| `pjm-planning-queues` | PJM | PlanningQueues XML | XML | Public XML backing PJM Serial Service Request Status. |
| `nyiso-interconnection-queue` | NYISO | Interconnection Queue workbook | XLSX | NYISO queue workbook. |
| `ercot-gis-report` | ERCOT | GIS Report | XLSX | Uses the `ercot_ice_doclist` resolver to select the latest monthly GIS workbook from ERCOT's public document list. |
| `isone-public-queue` | ISO-NE | IRTT Public Queue | HTML | Uses a static ASP.NET cookie to access the public queue page without login. Excel export requires authentication, so the public HTML table is monitored. |

## Operational Notes

- The first run after seeding production sources will download and parse every active source. Later daily runs only invoke the parser when source fingerprints change.
- The deterministic parser currently extracts project rows from the CAISO Cluster 15 XLSX, MISO XLSX, SPP CSV, PJM XML, NYISO XLSX, ERCOT XLSX, and ISO-NE HTML sources.
- Current production baseline: 15,800 project rows snapshotted in S3 across those 7 structured sources, plus 175 high-capacity baseline sample insight rows in `QueueWatch-Insights`.
- Future changed source runs compare the new normalized project snapshot with the prior S3 snapshot and write only `NEW`, `UPDATED`, or `REMOVED` project-delta insight rows.
- The CAISO public queue PDF still uses the text/OCR plus Bedrock fallback path. It remains source-level until a dedicated PDF table parser is added.
- The `RequestCookie` and `Resolver` columns are intentionally stored as DynamoDB metadata so sources can use site-specific access behavior without new infrastructure.

## Seed

```bash
python3 seed_queues.py --file queues.production.csv --region us-east-1
```

Keep the synthetic smoke target in `queues.example.csv`; it is useful for testing the pipeline without touching live utility sources.
