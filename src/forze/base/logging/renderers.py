"""Plain console rendering for structlog event dictionaries."""

from __future__ import annotations

from io import StringIO
from typing import Any, Final

import attrs
from rich.console import Console, Group
from rich.syntax import Syntax
from rich.text import Text
from structlog.typing import EventDict, WrappedLogger

from .constants import ERR_MESSAGE_KEY, ERR_STACK_KEY, ERR_TYPE_KEY

# ----------------------- #

_ID_SHORT_NAMES: Final[dict[str, str]] = {
    "correlation_id": "corr",
    "execution_id": "exec",
    "causation_id": "caus",
    "operation_id": "op",
}
_ID_SHORTEN: Final[set[str]] = {"correlation_id", "execution_id", "causation_id"}
_SEP: Final[str] = "  "


def _rich_level_style(level: str) -> str:
    match level.lower():
        case "trace" | "notset":
            return "dim"

        case "debug":
            return "blue"

        case "info":
            return "green"

        case "warning" | "warn":
            return "yellow"

        case "error" | "critical":
            return "red"

        case _:
            return ""


def _last_six_chars(value: object) -> str:
    s = str(value)
    return s[-6:] if len(s) > 6 else s


def _repr_extra_value(value: Any) -> str:
    if isinstance(value, str) and not any(c in value for c in ' \t\r\n="'):
        return value

    return repr(value)


def _format_extra_pair_plain(key: str, value: Any) -> str:
    display_key = _ID_SHORT_NAMES.get(key, key)

    if key in _ID_SHORT_NAMES:
        if key in _ID_SHORTEN:
            display_val = _last_six_chars(value)

        else:
            display_val = _repr_extra_value(value)

    else:
        display_val = _repr_extra_value(value)

    return f"{display_key}={display_val}"


def _extra_display_value(key: str, value: Any) -> str:
    if key in _ID_SHORT_NAMES:
        if key in _ID_SHORTEN:
            return _last_six_chars(value)
        return _repr_extra_value(value)
    return _repr_extra_value(value)


def _rich_capture_print(text: Text, *, min_width: int) -> str:
    """Render *text* to a string with ANSI styles.

    ``color_system='standard'`` is required: with ``color_system='auto'``, Rich
    treats a :class:`io.StringIO` sink as non-color-capable even when
    ``force_terminal`` is true, so no escape codes would be produced.

    The console width must be large enough for padded columns (see
    :attr:`ForzeConsoleRenderer.event_width`); otherwise Rich wraps the line.
    """

    buf = StringIO()
    width = max(min_width, 4096)
    console = Console(
        file=buf,
        force_terminal=True,
        color_system="standard",
        width=width,
        no_color=False,
        highlight=False,
        legacy_windows=False,
    )
    console.print(
        text,
        end="",
        highlight=False,
        no_wrap=True,
        overflow="ignore",
        crop=False,
    )
    return buf.getvalue()


def _error_header_line(err_type: str | None, err_message: str | None) -> str | None:
    if err_type is None and err_message is None:
        return None
    t = err_type or "Exception"
    m = err_message or ""
    return f"{t}: {m}".rstrip()


def _format_error_block_plain(
    *,
    err_type: str | None,
    err_message: str | None,
    err_stack: str | None,
) -> str:
    header = _error_header_line(err_type, err_message)
    parts: list[str] = []
    if header:
        parts.append(header)
    if err_stack:
        parts.append(err_stack.rstrip("\n"))
    if not parts:
        return ""
    return "\n" + "\n".join(parts) + "\n"


def _format_error_block_rich(
    *,
    err_type: str | None,
    err_message: str | None,
    err_stack: str | None,
    min_width: int,
) -> str:
    header = _error_header_line(err_type, err_message)
    if not header and not err_stack:
        return ""

    buf = StringIO()
    width = max(120, min(min_width, 160))
    console = Console(
        file=buf,
        force_terminal=True,
        color_system="standard",
        width=width,
        no_color=False,
        highlight=False,
        legacy_windows=False,
    )
    group_parts: list[Text | Syntax] = []
    if header:
        group_parts.append(Text(header, style="bold red"))
    if err_stack:
        group_parts.append(
            Syntax(
                err_stack.rstrip("\n"),
                lexer="pytb",
                theme="ansi_dark",
                word_wrap=True,
            )
        )
    console.print(Group(*group_parts))
    return "\n" + buf.getvalue()


# ....................... #


@attrs.define(slots=True, kw_only=True)
class ForzeConsoleRenderer:
    """Render *event_dict* as ``ts  LEVEL  [logger]  event  |  extra``.

    Exception data is expected under ``error.type``, ``error.message``, and
    ``error.stack`` (from :func:`~forze.base.logging.processors.format_exc_info`
    in the common processor chain). When ``colors`` is true, Rich renders the
    stack with the ``pytb`` lexer.

    .. _Rich: https://github.com/Textualize/rich
    """

    colors: bool = True
    logger_name_width: int = 22
    event_width: int = 100

    # ....................... #

    def __call__(self, _: WrappedLogger, __: str, event_dict: EventDict) -> str:
        ed: dict[str, Any] = dict(event_dict)
        stack = ed.pop("stack", None)
        exc_str = ed.pop("exception", None)
        ed.pop("exc_info", None)

        err_type = ed.pop(ERR_TYPE_KEY, None)
        err_message = ed.pop(ERR_MESSAGE_KEY, None)
        err_stack = ed.pop(ERR_STACK_KEY, None)

        ts = str(ed.pop("timestamp", ""))
        level = str(ed.pop("level", ""))
        logger_name = ed.pop("logger", None) or ed.pop("logger_name", None) or ""
        event = str(ed.pop("event", ""))

        extra_keys = sorted(k for k in ed if not k.startswith("_"))

        min_width = self.logger_name_width + self.event_width + 80

        if self.colors:
            level_plain = f"{level:<8}"
            logger_plain = f"[{logger_name}]".ljust(self.logger_name_width)
            event_plain = event.ljust(self.event_width)

            line = Text()

            line.append(ts, style="dim")
            line.append(_SEP)
            line.append(level_plain, style=_rich_level_style(level))
            line.append(_SEP)
            line.append(logger_plain, style="dim")
            line.append(_SEP)
            line.append(event_plain, style="bold")

            if extra_keys:
                line.append(_SEP)
                first = True

                for key in extra_keys:
                    display_key = _ID_SHORT_NAMES.get(key, key)
                    display_val = _extra_display_value(key, ed[key])

                    if not first:
                        line.append(_SEP)

                    first = False
                    line.append(display_key, style="cyan")
                    line.append("=")
                    line.append(display_val, style="magenta")

            main = _rich_capture_print(line, min_width=min_width)

        else:
            extra_parts = [_format_extra_pair_plain(k, ed[k]) for k in extra_keys]
            main = "  ".join((ts, level, f"[{logger_name}]", event))

            if extra_parts:
                main = f"{main}  |  {' '.join(extra_parts)}"

        sio = StringIO()
        sio.write(main)

        if stack is not None:
            sio.write("\n" + stack)

        if err_stack is not None or err_type is not None or err_message is not None:
            if self.colors:
                sio.write(
                    _format_error_block_rich(
                        err_type=err_type,
                        err_message=err_message,
                        err_stack=err_stack,
                        min_width=min_width,
                    )
                )
            else:
                sio.write(
                    _format_error_block_plain(
                        err_type=err_type,
                        err_message=err_message,
                        err_stack=err_stack,
                    )
                )

        elif exc_str:
            sio.write("\n" + exc_str)

        return sio.getvalue()


# Default processor instance.
forze_console_renderer: ForzeConsoleRenderer = ForzeConsoleRenderer()
