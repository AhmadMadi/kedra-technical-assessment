# WRC Legal Decisions Pipeline

Scrapes decisions and determinations from [Workplace Relations](https://www.workplacerelations.ie/en/search/)
(all four bodies: WRC, Labour Court, Equality Tribunal, Employment Appeals Tribunal),
stores the raw documents and metadata in a landing zone, and transforms them into a
clean, consumable corpus in a curated zone.

**Stack:** Scrapy · MongoDB (metadata) · MinIO (documents, S3-compatible) · Dagster (orchestration) · structlog (JSON logs)

```
scrape (date-partitioned) ──▶ landing: raw files + metadata ──▶ transform ──▶ curated: clean files + metadata
                              (immutable, append-only)                        (derived, recomputable)
```

## Prerequisites

- Docker (with Compose)
- Python 3.11+ (developed on 3.14)

## Setup

```bash
cp .env.example .env          # all connection strings and knobs live here
docker compose up -d          # MongoDB + MinIO, wait ~15s for healthchecks

python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Running with Dagster (recommended)

```bash
dagster dev -f wrc_scraper/orchestration/definitions.py
```

Open http://localhost:3000 → Jobs → `wrc_pipeline` → Launchpad, paste a date range:

```yaml
ops:
  ingest:
    config: {start_date: "2024-01-01", end_date: "2024-02-01"}
  transform:
    config: {start_date: "2024-01-01", end_date: "2024-02-01"}
```

Launch. `ingest` crawls (the run log streams the crawl's JSON events live);
`transform` starts only after ingest succeeds.

Note: the scraper partitions the range monthly (`PARTITION_MONTHS`), so a wide
range crawls every month in it — start with a single month.

## Running from the CLI

The orchestrator wraps two commands that also work standalone:

```bash
scrapy crawl wrc -a start_date=2024-01-01 -a end_date=2024-02-01 -L INFO
python -m wrc_scraper.transform.run --start-date 2024-01-01 --end-date 2024-02-01
```

## Logs

Pipeline events are JSON lines on stdout (Scrapy's own logs stay on stderr):

```bash
scrapy crawl wrc -a start_date=2024-01-01 -a end_date=2024-02-01 -L WARNING | tee run.jsonl
jq 'select(.event=="run_summary")' run.jsonl          # end-of-run ledger
jq 'select(.event=="download_failed")' run.jsonl      # failures with URL + error
```

Every run ends with a `run_summary` event: records found per partition/body,
found vs. scraped, new/updated/unchanged counts, uploads vs. skips, and every
failed download with its reason.

## Verifying a run

Reference ranges with known counts:

| Range | Found | Notes |
|---|---|---|
| 2024-01-01 → 2024-02-01 | 279 (234 WRC + 45 LC) | all HTML decision pages |
| 2010-03-01 → 2010-04-01 | 271 (157 EAT + 86 LC + 28 ET) | EAT decisions are PDFs behind HTML cover pages |

- **MinIO console:** http://localhost:9001 (minioadmin/minioadmin) — `landing` and `curated` buckets
- **Mongo:** `docker exec -it <mongo-container> mongosh wrc`, then
  `db.decisions_landing.countDocuments()` / `db.decisions_curated.countDocuments()`
- **Idempotency:** run the same range twice — the second `run_summary` reports
  `unchanged` for every record, zero uploads, and stores keep the same counts.

## Configuration

Everything is set via `.env` (see `.env.example`) — no hardcoded values:

| Variable | Purpose |
|---|---|
| `MONGO_URI`, `MONGO_DB` | metadata store |
| `MONGO_COLLECTION_LANDING`, `MONGO_COLLECTION_CURATED` | the two metadata collections |
| `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` | object storage (MinIO locally; point at AWS S3 to switch) |
| `S3_BUCKET_LANDING`, `S3_BUCKET_CURATED` | the two zones |
| `SCRAPER_START_URL` | search endpoint |
| `PARTITION_MONTHS` | date partition size |
| `DOWNLOAD_DELAY`, `CONCURRENT_REQUESTS` | politeness knobs (AutoThrottle adapts on top) |

## Project layout

```
wrc_scraper/
├── config.py                  # typed settings from .env (pydantic-settings)
├── logging_setup.py           # structlog: JSON events on stdout
├── scraper/
│   ├── settings.py            # Scrapy politeness/resilience settings
│   ├── spiders/wrc.py         # partitioned crawl, document fetching, PDF attachment hop
│   └── pipelines.py           # normalize+hash → MinIO landing → Mongo upsert
├── transform/run.py           # landing → curated (BeautifulSoup extraction, rename)
└── orchestration/definitions.py  # Dagster: ingest → transform job
```

See `ARCHITECTURE.md` for design decisions.
