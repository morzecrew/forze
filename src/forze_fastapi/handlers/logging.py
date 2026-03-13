import logging

from forze.base.logging import getLogger
from forze.base.logging.helpers import normalize_level

# ----------------------- #

logger = getLogger(__name__)

# ....................... #


class InterceptHandler(logging.Handler):
    """Redirect standard logging to loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        level = normalize_level(record.levelno)

        with logger.contextualize(scope="uvicorn"):
            logger.opt(
                depth=6,
                exception=record.exc_info,
            ).log(level, record.getMessage())


# ....................... #


def register_uvicorn_logging_interceptor() -> None:
    interceptor = InterceptHandler()

    root = logging.getLogger()
    root.setLevel(logging.NOTSET)

    # Add interceptor to root; child loggers propagate to it
    root.addHandler(interceptor)

    for name in (
        "uvicorn",
        "uvicorn.error",
        "uvicorn.access",
        "fastapi",
    ):
        log = logging.getLogger(name)
        log.handlers.clear()
        log.propagate = True
