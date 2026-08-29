"""Structured JSON logging for the whole pipeline (spec §10).

Every event is one JSON line on stdout: machine-parseable (jq/grep-able, shippable
to any log aggregator), while Scrapy's own human-oriented logs stay on stderr —
two streams, two audiences, separable with plain shell redirection.
"""
import logging
import sys

import structlog


def configure_logging() -> None:
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(sys.stdout),
        cache_logger_on_first_use=True,
    )


configure_logging()
log = structlog.get_logger()
