# Architecture

## Date partition size

Monthly by default (`PARTITION_MONTHS`, configurable). Per body-month volume on this
site is modest (tens to low hundreds of records), so monthly keeps the request count
low — one listing probe per body per month plus ~1 page per 10 records — while giving
useful resume/retry granularity: a failed backfill loses at most one month of one body,
and every record carries its `partition_date`, so any slice can be re-run independently.
Partitions are half-open `[start, end)` internally; the site's inclusive `to` filter
gets `end − 1 day`, so a boundary-day decision can never land in two partitions.

## Retries and rate limiting

Politeness first: `robots.txt` respected, a real browser User-Agent, a base
`DOWNLOAD_DELAY`, capped concurrency, and AutoThrottle adjusting pace to the server's
observed latency — fast when the server is comfortable, backing off when it strains.
With a single source, the global concurrency cap is the effective per-site limit;
at multiple sources, `CONCURRENT_REQUESTS_PER_DOMAIN` becomes the setting that keeps
each site protected while the global cap rises.

Transient failures retry 3× (timeouts, 5xx). A download that fails all retries is not
a hole: the errback stores a metadata record with a `download_error` field, the JSON
log carries the URL and reason, and the run summary counts it. Because the record
exists but has no file hash, the next run over that range re-attempts it
automatically — failures self-heal on re-runs.

## Deduplication and idempotency

Record identity is `record_url` (the catalog row's own page), not the site's
identifier — EAT-era refNOs are reused across genuinely different cases (one number
spanned nine distinct decisions), which an identifier-keyed upsert silently merges.
A unique Mongo index on `record_url` makes duplicates impossible by construction;
`identifier` stays as indexed metadata.

Files are content-addressed in the landing bucket: `identifier/hash16.ext`. Same
content → same key → upload skipped; changed content → new key, so the landing zone
is append-only and keeps version history without ever overwriting. The hash is
sha256 over the file bytes with HTML comments stripped first — the server injects
volatile comments (render timing, cache markers) that change per fetch, so raw-byte
hashing would defeat change detection. The stored file keeps its raw bytes; only the
hash input is canonicalized.

Landing is the immutable source of truth; curated is a derived view, deterministic
and cheap to rebuild, so the transform simply overwrites — re-running it is the
recovery procedure.

## Supporting 50+ sources

The spider is deliberately the only source-shaped file: partitioning, storage,
hashing, config, and logging are all source-agnostic, and records already carry a
`source` field. Scaling is one spider per source in the same project, feeding the
same pipelines. Beyond that:

- Rename the package to something source-neutral and extract a base spider
  (partition loop, run ledger, summary) once two or three spiders make the shared
  skeleton obvious — not before.
- Raise global concurrency while the per-domain cap keeps each site protected.
- Orchestration: a Dagster job per source (or partitioned assets), with schedules
  for incremental pulls; runs are already date-scoped and idempotent, so overlap
  is safe.
- Stores swap to managed services (S3, Atlas) by changing connection strings —
  nothing in the code assumes MinIO or a local Mongo.
- The JSON event stream ships to a log platform unchanged; found-vs-scraped and
  duplicate-version counts are the natural per-source health metrics.
