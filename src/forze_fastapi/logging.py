import logging

from forze.base.logging import getLogger
from forze.base.logging.helpers import normalize_level

# ----------------------- #

logger = getLogger(__name__)

# ....................... #


class InterceptHandler(logging.Handler):
    """Redirect selected stdlib logs to forze logger."""

    def emit(self, record: logging.LogRecord) -> None:
        if not (record.name.startswith("uvicorn") or record.name.startswith("fastapi")):
            return

        level = normalize_level(record.levelno)

        scope = "server"
        if record.name == "uvicorn.access":
            scope = "api"

        with logger.contextualize(scope=scope):
            logger.opt(
                depth=6,
                exception=record.exc_info,
            ).log(level, record.getMessage())


# ....................... #

UVICORN_LOG_CONFIG_TEMPLATE = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "forze_intercept": {
            "class": InterceptHandler,
        },
    },
    "loggers": {
        "uvicorn": {
            "handlers": ["forze_intercept"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.error": {
            "handlers": ["forze_intercept"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.access": {
            "handlers": ["forze_intercept"],
            "level": "INFO",
            "propagate": False,
        },
        "fastapi": {
            "handlers": ["forze_intercept"],
            "level": "INFO",
            "propagate": False,
        },
    },
}
