import hashlib
import re
import boto3

from datetime import datetime, timezone
from urllib.parse import urlparse
from scrapy.exceptions import DropItem
from botocore.exceptions import ClientError
from pymongo import MongoClient

from wrc_scraper.config import settings as env

DOC_EXTENSIONS = [".pdf", ".doc", ".docx"]

# The server injects volatile comments that change between fetches — discovered by
# diffing "versions" of the same page: first "<!-- Elapsed time: 0.58… -->" (render
# timing), then "<!-- cached or not being index.aspx page -->" (cache marker).
# Chasing them individually is whack-a-mole; HTML comments are never *content*, so we
# scrub them ALL from the hash input. The stored file keeps its raw bytes regardless.
HTML_COMMENTS = re.compile(rb"<!--.*?-->", re.DOTALL)

class NormalizeAndHashPipeline:
    """Station 1: canonical identifier, file extension, sha256 of the bytes."""

    def process_item(self, item):
        raw_id = item.get("identifier") or ""
        canonical = re.sub(r"\s+", "", raw_id).upper()
        if not canonical:
            raise DropItem(f"Record has no identifier: {item.get('doc_url')}")

        item["identifier_raw"] = raw_id
        item["identifier"] = canonical

        # Removes query in URL
        path = urlparse(item["doc_url"]).path if item.get("doc_url") else ""

        # Gets the first dot from the right to extract the file's ext
        suffix = path[path.rfind("."):].lower() if "." in path else ""

        item["file_ext"] = suffix if suffix in DOC_EXTENSIONS else ".html"

        content = item.get("file_content")
        if content:
            # Hash answers "is this the same document?" → volatile noise removed first.
            # The stored file answers "what did the server send?" → raw bytes, untouched.
            stable = HTML_COMMENTS.sub(b"", content) if item["file_ext"] == ".html" else content
            item["file_hash"] = hashlib.sha256(stable).hexdigest()

        return item

class MinioLandingPipeline:
    """Station 2: upload file bytes to the landing bucket, content-addressed, append only"""

    def open_spider(self):
        self.s3 = boto3.client(
            "s3",
            endpoint_url=env.s3_endpoint_url,
            aws_access_key_id=env.s3_access_key,
            aws_secret_access_key=env.s3_secret_key
        )

        try:
            self.s3.create_bucket(Bucket=env.s3_bucket_landing)
        except ClientError as e:
            if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise

    def process_item(self, item):
        content = item.pop("file_content", None)
        if not content:
            return item # This is a failed download -> meta only, record continues

        key = f"{item['identifier']}/{item['file_hash'][:16]}{item['file_ext']}"

        try:
            self.s3.head_object(Bucket=env.s3_bucket_landing, Key=key)
            item["file_upload_skipped"] = True # same content already stored
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self.s3.put_object(
                    Bucket=env.s3_bucket_landing,
                    Key=key,
                    Body=content,
                    ContentType=item.get("content_type") or "application/octet-stream"
                )
            
            else:
                raise

        item["file_path"] = f"{env.s3_bucket_landing}/{key}"
        return item

class MongoMetadataPipeline:
    """Station 3: upsert the metadata records, idempotent on the canonical identifier"""

    def open_spider(self):
        self.client = MongoClient(env.mongo_uri)
        self.collection = self.client[env.mongo_db][env.mongo_collection_landing]
        self.collection.create_index("identifier", unique=True)

    def close_spider(self):
        self.client.close()

    def process_item(self, item):
        existing = self.collection.find_one({ "identifier": item["identifier"] })
        if existing and existing.get("file_hash") and existing.get("file_hash") == item.get("file_hash"):
            item["unchanged"] = True # same doc as the last run, change detection done via hash

        now = datetime.now(timezone.utc)
        self.collection.update_one(
            { "identifier": item["identifier"] },
            {
                "$set": { **item, "last_seen_at": now },
                "$setOnInsert": { "first_seen_at": now }
            },
            upsert = True
        )

        return item