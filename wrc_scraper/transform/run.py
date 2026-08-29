"""
Landing -> curated transformation (bronze -> silver).

Reads metadata from Mongo for a date range, fetches the files from the landing
bucket, strips site chrome from HTML documents, renames everything to a
collision-safe identifier-based name, and writes to the curated bucket +
curated collection. The landing zone is never modified.
"""
import argparse
import boto3
import hashlib

from collections import Counter
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from pymongo import MongoClient

from wrc_scraper.config import settings as env
from wrc_scraper.logging_setup import log
from wrc_scraper.scraper.pipelines import HTML_COMMENTS

def extract_content(raw: bytes) -> bytes | None:
    """
    Keep only the decision text. Recon (4 page types, 2005-2024): the whole
    decision lives in a single <div class="content">; everything outside it is
    site chrome. Returns None if the structure isn't recognized.
    """

    soup = BeautifulSoup(raw, "html.parser")
    node = soup.select_one("div.content")

    if node is None:
        return None
    
    return str(node).encode("utf-8")

def curated_name(record: dict, counts: Counter) -> str:

    ident = record["identifier"]
    if counts[ident] > 1:
        return f'{ident}-{record["file_hash"][:8]}{record["file_ext"]}'
    
    return f'{ident}{record["file_ext"]}'

def run_transform(start_date: str, end_date: str) -> dict:
    """The transformation itself — callable from the CLI (main below) or from an
    orchestrator (Dagster op in M6). Returns the run's counters."""
    datetime.strptime(start_date, "%Y-%m-%d")  # fail fast on garbage, any caller
    datetime.strptime(end_date, "%Y-%m-%d")

    mongo = MongoClient(env.mongo_uri)
    landing = mongo[env.mongo_db][env.mongo_collection_landing]
    curated = mongo[env.mongo_db][env.mongo_collection_curated]
    curated.create_index("record_url", unique=True)

    s3 = boto3.client(
        "s3", 
        endpoint_url=env.s3_endpoint_url,
        aws_access_key_id=env.s3_access_key,
        aws_secret_access_key=env.s3_secret_key
        )

    try:
        s3.create_bucket(Bucket=env.s3_bucket_curated)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass

    query = { "partition_date": { "$gte": start_date, "$lt": end_date } }
    landing_records = list(landing.find(query))
    id_counts = Counter(rec["identifier"] for rec in landing_records)
    log.info(
        "transform_started",
        start=start_date,
        end=end_date,
        recordsLength=len(landing_records)
    )

    stats = Counter()
    for rec in landing_records:
        if not rec.get("file_path"):
            stats["skipped_no_file"] += 1
            log.warning(
                "transform_skipped",
                record_url=rec["record_url"],
                reason="No file in landing (Failed Download)"
            )
            continue
            
        bucket, key = rec["file_path"].split("/", 1)
        raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()

        if rec["file_ext"] == ".html":
            content = extract_content(HTML_COMMENTS.sub(b"", raw))
            if content is None:
                content = raw # Structuer is unknown. Keep raw, flag it, and never lose data
                extracted = False
                stats["fallback_raw"] += 1
                log.warning(
                    "extraction_failed",
                    record_url=rec["record_url"],
                    reason="div.content not found; stored raw copy"
                    )
                
            else:
                extracted = True
                stats["extracted"] += 1

        else:
            content = raw # PDFs or Docs. Pass through untouched
            extracted = False
            stats["passthrough"] += 1
        
        name = curated_name(rec, id_counts)
        new_hash = hashlib.sha256(content).hexdigest()
        s3.put_object(
            Bucket=env.s3_bucket_curated,
            Key=name,
            Body=content,
            ContentType=rec.get("content_type") or "application/octet-stream"
            )
        
        doc = { k: v for k, v in rec.items() if k not in  ("_id", "file_path", "file_hash", "unchanged", "file_upload_skipped") }
        doc.update({
            "file_path": f"{env.s3_bucket_curated}/{name}",
            "file_hash": new_hash,
            "landing_file_path": rec["file_path"],
            "landing_file_hash": rec["file_hash"],
            "content_extracted": extracted,
            "transformed_at": datetime.now(timezone.utc)
        })

        curated.update_one(
            { "record_url": rec["record_url"] },
            { "$set": doc },
            upsert=True
        )

        stats["records_written"] += 1

    log.info("transform_summary", **stats)
    mongo.close()
    return dict(stats)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True, help="ISO date - Inclusive")
    parser.add_argument("--end-date", required=True, help="ISO date, exclusive")
    args = parser.parse_args()
    run_transform(args.start_date, args.end_date)


if __name__ == "__main__":
    main()