"""Renderers: ConsoleRenderer and JSON renderer for structlog."""

from __future__ import annotations

import textwrap
from datetime import datetime
from io import StringIO
from pprint import pformat
from typing import Any

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
        width: int = 36,
        max_width: int | None = None,
        extra_indent: int = 1,
        colorize: bool = False,
    ) -> None:
        self.step = step
        self.width = width
        self.max_width = max_width
        self.extra_indent = extra_indent
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
        width = self.width or config.width
        max_width = self.max_width or config.max_width
        extra_indent = self.extra_indent
        colorize = self.colorize

        from .context import get_depth

        indent = step * get_depth()

        level = self._level_display(event_dict.get("level", "INFO")).ljust(9)
        ts = event_dict.get("timestamp", "")
        time_str = _format_ts(ts) if ts else ""
        event = event_dict.get("event", "")
        scope = event_dict.get("scope", "root")
        scope_str = f"[{scope}]".ljust(width)

        # Prefix length for alignment (time + "   " + level + scope + indent)
        prefix_len = len(time_str) + 3 + len(level) + len(scope_str) + len(indent)
        block_indent = " " * prefix_len

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

        extra = {
            k: v
            for k, v in event_dict.items()
            if k not in standard_keys and v is not None
        }
        extra_inline_plain = ""
        extra_block_str = ""
        if extra:
            if _extra_needs_block(extra):
                formatted = pformat(extra, width=100)
                extra_content = formatted.rstrip()
                if colorize:
                    extra_content = _render_rich_to_str(
                        ReprHighlighter()(extra_content)
                    )
                extra_lines = extra_content.split("\n")
                extra_block_str = "\n\n" + "\n".join(
                    block_indent + ln for ln in extra_lines
                ) + "\n"
            else:
                indent_str = " " * extra_indent
                inline_plain = indent_str + " ".join(
                    f"{k}={v!r}" for k, v in sorted(extra.items())
                )
                extra_inline_plain = inline_plain
                if colorize:
                    extra_inline_plain = indent_str + _render_rich_to_str(
                        ReprHighlighter()(inline_plain.lstrip())
                    )

        colors = {
            "DEBUG": "\033[36m",
            "INFO": "\033[32m",
            "WARNING": "\033[33m",
            "ERROR": "\033[31m",
            "CRITICAL": "\033[35m",
        }
        is_trace = level.strip() == "TRACE"

        # max_width applies to event (log message) only; not timestamp, level, scope, or extras
        event_display = event
        if extra_inline_plain:
            if max_width:
                # Wrap event at max_width; pad first line so extra aligns
                event_width = max(10, max_width)
                lines = textwrap.wrap(event, width=event_width, drop_whitespace=False)
                if lines:
                    event_padded = lines[0].ljust(event_width)
                    event_display = event_padded + extra_inline_plain
                    if len(lines) > 1:
                        event_display += "\n" + "\n".join(
                            block_indent + ln for ln in lines[1:]
                        )
                else:
                    event_display = event + extra_inline_plain
            else:
                event_display = event + extra_inline_plain
        elif max_width and len(event) > max_width:
            lines = textwrap.wrap(event, width=max_width, drop_whitespace=False)
            if len(lines) > 1:
                event_display = lines[0] + "\n" + "\n".join(
                    block_indent + ln for ln in lines[1:]
                )

        if is_trace and colorize:
            line = (
                f"{dim}{time_str}   {level}{scope_str}{indent}{event_display}{extra_block_str}{rst}"
            )
        else:
            lvl_style = colors.get(level.strip(), "") if colorize else ""
            line = f"{dim}{time_str}{rst}   {lvl_style}{level}{rst}{dim}{scope_str}{rst}{indent}{event_display}{extra_block_str}"

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
