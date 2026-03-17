"""Renderers: ConsoleRenderer and JSON renderer for structlog."""

from __future__ import annotations

import textwrap
from datetime import datetime
from io import StringIO
from pprint import pformat
from typing import Any, Callable

import structlog
from rich.console import Console
from rich.highlighter import ReprHighlighter

from .config import NO_TO_LEVEL, get_config


def _render_rich_to_str(renderable: Any) -> str:
    """Render a Rich renderable to an ANSI-colored string."""
    buf = StringIO()
    Console(file=buf, force_terminal=True, color_system="auto").print(
        renderable, end=""
    )
    return buf.getvalue()


def _extra_needs_block(extra: dict[str, Any]) -> bool:
    """Use block format (below log, blank line) when nested or many keys."""
    if len(extra) > 5:
        return True
    return any(
        isinstance(v, (dict, list, tuple)) and not _is_simple_tuple(v)
        for v in extra.values()
    )


def _is_simple_tuple(v: Any) -> bool:
    """Tuples of primitives can stay inline."""
    if not isinstance(v, tuple):
        return False
    return len(v) <= 3 and all(  # pyright: ignore[reportUnknownArgumentType]
        isinstance(x, (str, int, float, bool, type(None)))
        for x in v  # pyright: ignore[reportUnknownVariableType]
    )


def _format_ts(ts: Any) -> str:
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return str(ts)


class ConsoleRenderer:
    """Human-readable console renderer with optional colorization."""

    def __init__(
        self,
        *,
        step: str = "  ",
        event_width: int | None = None,
        extra_indent: int = 1,
        prefix_width: int | None = None,
        extra_dim: str | None = None,
        extra_key_sort: Callable[[str], int] | None = None,
        colorize: bool = False,
    ) -> None:
        self.step = step
        self.event_width = event_width
        self.extra_indent = extra_indent
        self.prefix_width = prefix_width
        self.extra_dim = extra_dim
        self.extra_key_sort = extra_key_sort
        self.colorize = colorize

    def __call__(
        self,
        logger: Any,
        method_name: str,
        event_dict: dict[str, Any],
    ) -> str:
        del logger, method_name

        config = get_config()
        step = self.step or config.step
        event_width = self.event_width if self.event_width is not None else config.event_width
        extra_indent = self.extra_indent
        prefix_width = self.prefix_width if self.prefix_width is not None else config.prefix_width
        extra_dim = self.extra_dim if self.extra_dim is not None else config.extra_dim
        extra_key_sort = self.extra_key_sort or config.extra_key_sort
        colorize = self.colorize

        from .context import get_depth

        indent = step * get_depth()

        level = self._level_display(event_dict.get("level", "INFO")).ljust(9)
        ts = event_dict.get("timestamp", "")
        time_str = _format_ts(ts) if ts else ""
        event = event_dict.get("event", "")
        scope = event_dict.get("scope", "root")
        scope_str = f"[{scope}]"

        # Prefix length (time + "   " + level + scope + indent)
        prefix_len = len(time_str) + 3 + len(level) + len(scope_str) + len(indent)
        block_indent = " " * prefix_width
        prefix_padding = " " * max(0, prefix_width - prefix_len)

        standard_keys = {
            "event",
            "level",
            "timestamp",
            "logger",
            "scope",
            "source",
            "depth",
            "exception",
            "exc_info",
        }
        exception_str = event_dict.get("exception", "")
        dim = "\033[2m" if colorize else ""
        rst = "\033[0m" if colorize else ""
        extra_dim_str = (extra_dim or "\033[2m") if colorize else ""

        extra = {
            k: v
            for k, v in event_dict.items()
            if k not in standard_keys and v is not None
        }
        extra_inline_plain = ""
        extra_block_str = ""

        if extra:
            ordered_extra = sorted(
                extra.items(),
                key=lambda item: (
                    extra_key_sort(item[0]) if extra_key_sort is not None else item[0]
                ),
            )
            if _extra_needs_block(extra):
                ordered_dict = dict(ordered_extra)
                formatted = pformat(ordered_dict, width=100)
                extra_content = formatted.rstrip()
                if colorize:
                    extra_content = _render_rich_to_str(
                        ReprHighlighter()(extra_content)
                    )
                extra_content = extra_dim_str + extra_content + rst
                extra_lines = extra_content.split("\n")
                extra_block_str = (
                    "\n\n" + "\n".join(block_indent + ln for ln in extra_lines) + "\n"
                )
            else:
                indent_str = " " * extra_indent
                inline_plain = indent_str + " ".join(
                    f"{k}={v!r}" for k, v in ordered_extra
                )
                extra_inline_plain = inline_plain
                if colorize:
                    # Dim whole inline extra (trace-level color), no syntax highlights
                    extra_inline_plain = extra_dim_str + inline_plain + rst

        colors = {
            "DEBUG": "\033[36m",
            "INFO": "\033[32m",
            "WARNING": "\033[33m",
            "ERROR": "\033[31m",
            "CRITICAL": "\033[35m",
        }
        is_trace = level.strip() == "TRACE"

        # prefix_width + event_width + extra_indent = extra column
        event_display = event
        wrap_width = max(10, event_width)
        if extra_inline_plain:
            lines = textwrap.wrap(event, width=wrap_width, drop_whitespace=False)
            if lines:
                event_padded = lines[0].ljust(wrap_width)
                event_display = event_padded + extra_inline_plain
                if len(lines) > 1:
                    event_display += "\n" + "\n".join(
                        block_indent + ln for ln in lines[1:]
                    )
            else:
                event_display = event + extra_inline_plain
        elif len(event) > event_width:
            lines = textwrap.wrap(event, width=wrap_width, drop_whitespace=False)
            if len(lines) > 1:
                event_display = (
                    lines[0] + "\n" + "\n".join(block_indent + ln for ln in lines[1:])
                )

        if is_trace and colorize:
            line = f"{dim}{time_str}   {level}{scope_str}{indent}{prefix_padding}{event_display}{extra_block_str}{rst}"
        else:
            lvl_style = colors.get(level.strip(), "") if colorize else ""
            line = f"{dim}{time_str}{rst}   {lvl_style}{level}{rst}{dim}{scope_str}{rst}{indent}{prefix_padding}{event_display}{extra_block_str}"

        if exception_str:
            line += (
                "\n\n"
                + "\n".join(
                    block_indent + ln for ln in exception_str.rstrip().split("\n")
                )
                + "\n"
            )

        return line

    @staticmethod
    def _level_display(level: Any) -> str:
        if isinstance(level, int):
            return NO_TO_LEVEL.get(level, "INFO")
        return str(level).upper()


def build_json_renderer() -> Any:
    """Return structlog's JSONRenderer for JSON output."""

    return structlog.processors.JSONRenderer()
