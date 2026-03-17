"""Shared structlog processors."""

from __future__ import annotations

import sys
from typing import Any

import structlog
from rich.console import Console
from rich.traceback import Traceback

from .config import get_config, level_no
from .context import get_depth

_FORZE_LEVEL_KEY = "_forze_level"


def resolve_forze_level(
    logger: Any, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Override structlog level when Logger.trace() passes _forze_level."""
    del logger, method_name
    override = event_dict.pop(_FORZE_LEVEL_KEY, None)
    if override is not None:
        event_dict["level"] = override
    return event_dict


def maybe_rich_exc_info(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Format exception with Rich when colorize; else let format_exc_info handle it."""
    del logger, method_name

    exc_info = event_dict.get("exc_info")
    if exc_info is None or not get_config().colorize:
        return event_dict

    if exc_info is True:
        exc_info = sys.exc_info()

    if exc_info and exc_info[0] is not None:
        exc_type, exc_value, exc_tb = exc_info
        if exc_type is not None and exc_value is not None:
            from io import StringIO

            tb = Traceback.from_exception(exc_type, exc_value, exc_tb)
            buf = StringIO()
            Console(file=buf, force_terminal=True, color_system="auto").print(tb)
            event_dict["exception"] = buf.getvalue()
            event_dict.pop("exc_info", None)

    return event_dict


def add_forze_context(
    logger: Any, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Add depth and default scope to event dict."""
    del method_name
    event_dict["depth"] = get_depth()
    if "scope" not in event_dict or event_dict["scope"] is None:
        event_dict["scope"] = "root"
    return event_dict


def filter_by_level(
    logger: Any, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Drop events below effective level for the logger name.

    Only filters forze loggers; third-party and stdlib loggers pass through
    to avoid DropEvent breaking ProcessorFormatter for e.g. asyncio DEBUG.
    """
    del method_name

    from .config import effective_level_for_name

    name = event_dict.get("logger", "root")
    # Skip filtering for non-forze loggers (stdlib, asyncio, etc.)
    if not name.startswith("forze"):
        return event_dict

    effective = effective_level_for_name(name)
    msg_level = event_dict.get("level", "INFO")
    msg_no = msg_level if isinstance(msg_level, int) else level_no(msg_level)

    if msg_no < level_no(effective):
        raise structlog.DropEvent

    return event_dict
