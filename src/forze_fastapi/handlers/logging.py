import logging

from forze.base.logging import getLogger
from forze.base.logging.helpers import normalize_level

# ----------------------- #


class InterceptHandler(logging.Handler):
    """Redirect standard logging to loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        logger = getLogger(__name__)
        level = normalize_level(record.levelno)

        with logger.contextualize(scope="uvicorn"):
            logger.opt(
                depth=6,
                exception=record.exc_info,
            ).log(level, record.getMessage())


# ....................... #


def register_uvicorn_logging_interceptor() -> None:
    interceptor = InterceptHandler()

    for name in [
        "uvicorn",
        "uvicorn.access",
        "uvicorn.error",
        "fastapi",
    ]:
        logging.getLogger(name).handlers = [interceptor]
        logging.getLogger(name).propagate = False
