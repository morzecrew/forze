"""Plain console rendering for structlog event dictionaries."""

from __future__ import annotations

import sys
from io import StringIO
from types import TracebackType
from typing import Any, Final, cast

from structlog.dev import plain_traceback
from structlog.typing import EventDict, ExcInfo, WrappedLogger

# ----------------------- #

_ID_SHORT_NAMES: Final[dict[str, str]] = {
    "correlation_id": "corr",
    "execution_id": "exec",
    "causation_id": "caus",
}


def _last_six_chars(value: object) -> str:
    s = str(value)
    return s[-6:] if len(s) > 6 else s


def _repr_extra_value(value: Any) -> str:
    if isinstance(value, str) and not any(c in value for c in ' \t\r\n="'):
        return value
    return repr(value)


def _format_extra_pair(key: str, value: Any) -> str:
    display_key = _ID_SHORT_NAMES.get(key, key)
    if key in _ID_SHORT_NAMES:
        display_val = _last_six_chars(value)
    else:
        display_val = _repr_extra_value(value)
    return f"{display_key}={display_val}"


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


def forze_console_renderer(_: WrappedLogger, __: str, event_dict: EventDict) -> str:
    """Render *event_dict* as ``ts  LEVEL  [logger]  event  |  extra``.

    *correlation_id*, *execution_id*, and *causation_id* are shown as *corr*,
    *exec*, and *caus* with values truncated to the last six characters.
    """

    ed: dict[str, Any] = dict(event_dict)
    stack = ed.pop("stack", None)
    exc_str = ed.pop("exception", None)
    exc_raw = ed.pop("exc_info", None)

    ts = str(ed.pop("timestamp", ""))
    level = str(ed.pop("level", ""))
    logger_name = ed.pop("logger", None) or ed.pop("logger_name", None) or ""
    event = str(ed.pop("event", ""))

    extra_keys = sorted(k for k in ed if not k.startswith("_"))
    extra_parts = [_format_extra_pair(k, ed[k]) for k in extra_keys]

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
