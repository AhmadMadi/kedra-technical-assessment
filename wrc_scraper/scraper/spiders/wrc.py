import math
import scrapy

from datetime import datetime, timedelta
from urllib.parse import urlencode
from dateutil.relativedelta import relativedelta
from urllib.parse import urlencode, urlparse
from scrapy.http import TextResponse

from wrc_scraper.config import settings as env
from wrc_scraper.logging_setup import log

BODIES = {
    "workplace-relations-commission": 15376,
    "labour-court": 3,
    "equality-tribunal": 1,
    "employment-appeals-tribunal": 2
}

PAGE_SIZE = 10

# Every WRC page links cookie_policy.pdf and a site-guide PDF
# Without filtering, all records would suddenly "have attachments." 
# Substring match on the lowercased path.
ATTACHMENT_EXCLUDE = ("decisions_information_guide", "cookie_policy")

class WrcSpider(scrapy.Spider):
    name = "wrc"

    def __init__(self, start_date: str, end_date: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        # Run ledger for the end-of-run summary (spec §10):
        self.found = {}      # "partition/body" -> banner count
        self.failures = []   # every failed download: {url, error}

    def _partitions(self):
        """Yield [cursor, upper) windows of PARTITION_MONTHS between the dates"""
        step = relativedelta(months=env.partition_months)
        cursor = self.start_date
        while cursor < self.end_date:
            upper = min(cursor + step, self.end_date)
            yield cursor, upper
            cursor = upper

    def _search_url(self, body_id, frm, to, page):
        qs = urlencode({
            "decisions": 1,
            "from": frm.strftime("%d/%m/%Y"),
            "to": (to - timedelta(days=1)).strftime("%d/%m/%Y"),
            "legislationsub": "",
            "body": body_id,
            "pageNumber": page
        })

        return f"{env.scraper_start_url}?{qs}"

    async def start(self):
        # For each parition, automated next()
        for frm, upper in self._partitions():
            # For each body (checkbox), also an automated next()
            for body_name, body_id in BODIES.items():
                # Scrapy engine queueing the requests
                yield scrapy.Request(
                    self._search_url(body_id, frm, upper, page = 1),
                    callback=self.parse_results,
                    meta={
                        "body": body_name,
                        "body_id": body_id,
                        "partition_date": frm.isoformat(),
                        "frm": frm,
                        "upper": upper,
                        "page": 1
                    }
                )

    def parse_results(self, response):
        meta = response.meta

        for row in response.css("li.each-item"):
            href = row.css("h2.title a::attr(href)").get()
            item = {
                "identifier": (row.css("span.refNO::text").get() or "").strip(),
                "title": (row.css("h2.title::attr(title)").get() or "").strip(),
                "description": (row.css("p.description::attr(title)").get() or "").strip(),
                "decision_date": (row.css("span.date::text").get() or "").strip(),
                "doc_url": response.urljoin(href) if href else None,
                "body": meta["body"],
                "partition_date": meta["partition_date"]
            }

            # The catalog row's own page is the record's true identity — EAT-era
            # refNOs are NOT unique (one number can span many distinct cases).
            item["record_url"] = item["doc_url"] or f"missing-doc:{item['identifier']}"

            if item["doc_url"]:
                yield scrapy.Request(
                    item["doc_url"],
                    callback=self.parse_document,
                    errback=self.on_download_error,
                    meta={ "item": item }
                )
            else:
                log.warning("record_without_doc_link", identifier=item["identifier"],
                            partition=meta["partition_date"], body=meta["body"])
                yield item

         # Fan out only from page 1, other pages would mint duplicate tickets 
         # (scheduler dedup would drop them, but the guard avoids the waste and keeps found= logged once)
        if meta["page"] == 1:
            total_txt = response.css("div.searchhead").re_first(r"of\s+([\d,]+)\s+results")
            total = int(total_txt.replace(",", "")) if total_txt else 0

            self.found[f'{meta["partition_date"]}/{meta["body"]}'] = total
            log.info("partition_scanned", partition=meta["partition_date"],
                     body=meta["body"], found=total)

            for page in range(2, math.ceil(total / PAGE_SIZE) + 1):
                yield scrapy.Request(
                    self._search_url(meta["body_id"], meta["frm"], meta["upper"], page),
                    callback=self.parse_results,
                    meta={
                        "body": meta["body"],
                        "body_id": meta["body_id"],
                        "partition_date": meta["partition_date"],
                        "frm": meta["frm"],
                        "upper": meta["upper"],
                        "page": page,
                    }
                )

    def parse_document(self, response):
        item = response.meta["item"]

        # Old EAT-era case pages are thin HTML wrappers around a PDF/DOC attachment.
        # If this response is HTML and links to a real document, hop once more and
        # store THAT instead (spec 6a: store PDF/DOC files as they are).
        if isinstance(response, TextResponse):
            for href in response.css("a::attr(href)").getall():
                path = urlparse(href).path.lower()
                if path.endswith((".pdf", ".doc", ".docx")) and not any(
                    x in path for x in ATTACHMENT_EXCLUDE
                ):
                    item["page_url"] = response.url
                    item["doc_url"] = response.urljoin(href)
                    yield scrapy.Request(
                        item["doc_url"],
                        callback=self.parse_document,
                        errback=self.on_download_error,
                        meta={"item": item},
                    )
                    return

        item["file_content"] = response.body
        item["content_type"] = (response.headers.get("Content-Type") or b"").decode()
        yield item

    def on_download_error(self, failure):
        item = failure.request.meta["item"]
        error = repr(failure.value)
        log.error("download_failed", url=failure.request.url, error=error,
                  identifier=item.get("identifier"), partition=item.get("partition_date"))
        self.failures.append({"url": failure.request.url, "error": error})

        item["download_error"] = error
        yield item

    def closed(self, reason):
        """Spider lifecycle hook — Scrapy calls this once when the crawl ends.
        Emits the end-of-run summary the spec demands (§10)."""
        stats = self.crawler.stats
        log.info(
            "run_summary",
            reason=reason,
            date_range=f"{self.start_date} -> {self.end_date}",
            found_per_partition_body=self.found,
            total_found=sum(self.found.values()),
            items_scraped=stats.get_value("item_scraped_count", 0),
            records_new=stats.get_value("wrc/records_new", 0),
            records_unchanged=stats.get_value("wrc/records_unchanged", 0),
            records_updated=stats.get_value("wrc/records_updated", 0),
            files_uploaded=stats.get_value("wrc/files_uploaded", 0),
            uploads_skipped=stats.get_value("wrc/uploads_skipped", 0),
            records_dropped=stats.get_value("wrc/records_dropped", 0),
            failed_downloads=len(self.failures),
            failures=self.failures,
        )