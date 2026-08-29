"""Dagster orchestration: ingestion and transformation as separate ops with an
explicit dependency.

Run the UI with:  dagster dev -f wrc_scraper/orchestration/definitions.py
"""
import subprocess
import sys
from pathlib import Path

from dagster import Config, Definitions, In, Nothing, Out, OpExecutionContext, job, op

from wrc_scraper.transform.run import run_transform

# scrapy must run from the directory holding scrapy.cfg (the repo root)
REPO_ROOT = Path(__file__).resolve().parents[2]


class DateRange(Config):
    """Run configuration — same contract as the CLI tools."""
    start_date: str  # ISO, inclusive
    end_date: str    # ISO, exclusive


@op(out=Out(Nothing))
def ingest(context: OpExecutionContext, config: DateRange) -> None:
    """Run the crawl as a subprocess: Twisted's reactor can't be restarted
    in-process, and a non-zero exit must fail the op."""
    result = subprocess.run(
        [
            sys.executable, "-m", "scrapy", "crawl", "wrc",
            "-a", f"start_date={config.start_date}",
            "-a", f"end_date={config.end_date}",
            "-L", "INFO",
        ],
        cwd=REPO_ROOT,
        check=True,  # non-zero exit -> op fails -> transform never runs
    )
    context.log.info(f"scrapy crawl finished (exit code {result.returncode})")


@op(ins={"after_ingest": In(Nothing)})
def transform(context: OpExecutionContext, config: DateRange) -> None:
    """Landing -> curated, same function the CLI uses."""
    stats = run_transform(config.start_date, config.end_date)
    context.log.info(f"transform finished: {stats}")


@job
def wrc_pipeline():
    """ingest -> transform. The Nothing edge carries no data, only ordering."""
    transform(after_ingest=ingest())


defs = Definitions(jobs=[wrc_pipeline])
