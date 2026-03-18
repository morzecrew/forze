import sys
from io import StringIO
from typing import Any

from rich.console import Console
from rich.traceback import Traceback
from structlog.typing import EventDict

from .constants import (
    DEPTH_KEY,
    EXC_INFO_KEY,
    EXCEPTION_KEY,
    FORZE_LEVEL_KEY,
    LEVEL_KEY,
    LOGGER_KEY,
)
from .context import get_depth

# ----------------------- #


def resolve_forze_level(_: Any, __: str, event_dict: EventDict) -> EventDict:
    """Override structlog level when Logger.trace() passes _forze_level."""

    override = event_dict.pop(FORZE_LEVEL_KEY)

    if override is not None:
        event_dict[LEVEL_KEY] = override

    return event_dict


# ....................... #


def resolve_depth_level(_: Any, __: str, event_dict: EventDict) -> EventDict:
    """Resolve the depth level from local context."""

    event_dict[DEPTH_KEY] = get_depth()

    return event_dict


# ....................... #


def render_rich_exception_info(_: Any, __: str, event_dict: EventDict) -> EventDict:
    """Format exception with Rich when colorize; else let format_exc_info handle it."""

    exc_info = event_dict.get(EXC_INFO_KEY)

    if exc_info is None:  # or not get_config().colorize
        return event_dict

    if exc_info is True:
        exc_info = sys.exc_info()

    if exc_info and exc_info[0] is not None:
        exc_type, exc_value, exc_tb = exc_info

        if exc_type is not None and exc_value is not None:
            tb = Traceback.from_exception(exc_type, exc_value, exc_tb)
            buf = StringIO()
            Console(file=buf, force_terminal=True, color_system="auto").print(tb)
            event_dict[EXCEPTION_KEY] = buf.getvalue()
            event_dict.pop(EXC_INFO_KEY, None)

    return event_dict


# ....................... #


def filter_by_effective_level(_: Any, __: str, event_dict: EventDict) -> EventDict:
    """Drop events below effective level for the logger name."""

    name = str(event_dict.get(LOGGER_KEY, "root"))

    if not name.startswith("forze"):
        return event_dict

    return event_dict
