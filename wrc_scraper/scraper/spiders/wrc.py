import math
from datetime import datetime, timedelta
from urllib.parse import urlencode

import scrapy
from dateutil.relativedelta import relativedelta

from wrc_scraper.config import settings as env

BODIES = {
    "workplace-relations-commission": 15376,
    "labour-court": 3,
    "equality-tribunal": 1,
    "employment-appeals-tribunal": 2
}

PAGE_SIZE = 10

class WrcSpider(scrapy.Spider):
    name = "wrc"

    def __init__(self, start_date: str, end_date: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

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
        for frm, upper in self._partitions():
            for body_name, body_id in BODIES.items():
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
            yield {
                "identifier": (row.css("span.refNO::text").get() or "").strip(),
                "title": (row.css("h2.title::attr(title)").get() or "").strip(),
                "description": (row.css("p.description::attr(title)").get() or "").strip(),
                "decision_date": (row.css("span.date::text").get() or "").strip(),
                "doc_url": response.urljoin(href) if href else None,
                "body": meta["body"],
                "partition_date": meta["partition_date"]
            }

        # Fan out the remaining pages, but only from page 1, so it happens once
        # per (partition, body) and not once per page
        if meta["page"] == 1:
            total_txt = response.css("div.searchhead").re_first(r"of\s+([\d,]+)\s+results")
            total = int(total_txt.replace(",", "")) if total_txt else 0
            self.logger.info(
                "partition=%s body=%s found=%d",
                meta["partition_date"], meta["body"], total,
            )
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