"""Structured JSON logging via structlog.

Every log line is a JSON object with consistent fields:
  - timestamp, level, event, logger
  - correlation_id (when set via context)
  - Any additional key-value pairs passed at the call site
"""

import logging
import sys

import structlog


def setup_logging(log_level: str = "INFO") -> None:
    """Configure structlog for structured JSON output.

    Call once at application startup.
    """
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Bridge stdlib logging to structlog
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper()),
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a named logger instance."""
    return structlog.get_logger(name)  # type: ignore[no-any-return]
