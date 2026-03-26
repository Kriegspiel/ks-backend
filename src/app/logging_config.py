from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def _shared_processors() -> list[Any]:
    return [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]


def configure_logging(environment: str) -> None:
    is_production = environment == "production"

    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout, force=True)

    processors: list[Any] = [
        *_shared_processors(),
        structlog.processors.dict_tracebacks,
    ]

    if is_production:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
