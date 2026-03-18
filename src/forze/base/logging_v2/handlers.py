import structlog

from .processors import (
    filter_by_effective_level,
    render_rich_exception_info,
    resolve_forze_level,
)
from .types import Processor

# ----------------------- #


def common_processors() -> list[Processor]:
    return [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        resolve_forze_level,
        structlog.processors.TimeStamper(fmt="iso"),
        render_rich_exception_info,
        filter_by_effective_level,
    ]


# ....................... #
