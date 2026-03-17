import logging

from forze.base.logging import getLogger, normalize_level

# ----------------------- #

_log = getLogger(__name__)

api_logger = _log.bind(scope="api")
server_logger = _log.bind(scope="server")

# ....................... #


class InterceptHandler(logging.Handler):
    """Redirect selected stdlib logs to forze logger."""

    def emit(self, record: logging.LogRecord) -> None:
        if not (record.name.startswith("uvicorn") or record.name.startswith("fastapi")):
            return

        level = normalize_level(record.levelno)
        log = server_logger

        if record.name == "uvicorn.access":
            log = api_logger

        kw: dict[str, object] = {}

        if record.exc_info:
            kw["exc_info"] = record.exc_info

        log.log(level, record.getMessage(), sub=None, **kw)


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
