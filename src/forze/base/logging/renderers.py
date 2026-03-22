"""Plain console rendering for structlog event dictionaries."""

from __future__ import annotations

import sys
from io import StringIO
from types import TracebackType
from typing import Any, Final, cast

import attrs
from rich.console import Console
from rich.text import Text
from structlog.dev import plain_traceback
from structlog.typing import EventDict, ExcInfo, WrappedLogger

# ----------------------- #

_ID_SHORT_NAMES: Final[dict[str, str]] = {
    "correlation_id": "corr",
    "execution_id": "exec",
    "causation_id": "caus",
    "operation_id": "op",
}
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
        display_val = _last_six_chars(value)

    else:
        display_val = _repr_extra_value(value)

    return f"{display_key}={display_val}"


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


def _normalize_exc_info(raw: Any) -> ExcInfo | None:
    if isinstance(raw, BaseException):
        return (type(raw), raw, raw.__traceback__)

    match raw:
        case (exc_type, exc_val, tb):
            if (
                isinstance(exc_type, type)
                and issubclass(exc_type, BaseException)
                and isinstance(exc_val, BaseException)
                and (tb is None or isinstance(tb, TracebackType))
            ):
                return exc_type, exc_val, tb
        case _:
            pass

    if raw:
        info = sys.exc_info()
        if info != (None, None, None):
            return cast(ExcInfo, info)
    return None


# ....................... #


@attrs.define(slots=True, kw_only=True)
class ForzeConsoleRenderer:
    """Render *event_dict* as ``ts  LEVEL  [logger]  event  |  extra``.

    When ``colors`` is true, styles are applied with Rich_ (a direct
    dependency). No colorama import is required; Rich handles Windows consoles
    that support VT sequences.

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
        exc_raw = ed.pop("exc_info", None)

        ts = str(ed.pop("timestamp", ""))
        level = str(ed.pop("level", ""))
        logger_name = ed.pop("logger", None) or ed.pop("logger_name", None) or ""
        event = str(ed.pop("event", ""))

        extra_keys = sorted(k for k in ed if not k.startswith("_"))

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

                    if key in _ID_SHORT_NAMES:
                        display_val = _last_six_chars(ed[key])

                    else:
                        display_val = _repr_extra_value(ed[key])

                    if not first:
                        line.append(_SEP)

                    first = False
                    line.append(display_key, style="cyan")
                    line.append("=")
                    line.append(display_val, style="magenta")

            min_width = self.logger_name_width + self.event_width + 80
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

        exc_info = _normalize_exc_info(exc_raw)

        if exc_info is not None:
            plain_traceback(sio, exc_info)

        elif exc_str:
            sio.write("\n" + exc_str)

        return sio.getvalue()
