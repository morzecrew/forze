from typing import Any, Final, Literal

import attrs
from structlog.typing import EventDict

from ..constants import ERR_MESSAGE_KEY, ERR_STACK_KEY, ERR_TYPE_KEY

# ----------------------- #

_ID_SHORT_NAMES: Final[dict[str, str]] = {
    "correlation_id": "corr",
    "execution_id": "exec",
    "causation_id": "caus",
    "operation_id": "op",
}
_ID_SHORTEN: Final[set[str]] = {"correlation_id", "execution_id", "causation_id"}

EventKind = Literal["common", "access"]

# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class NormalizedEvent:
    timestamp: str
    level: str
    logger_name: str
    message: str
    extras: tuple[tuple[str, str], ...] = ()
    err_header: str | None = None
    err_stack: str | None = None
    stack: str | None = None
    kind: EventKind = "common"


# ....................... #


def shorten_traceback_text(stack: str, *, max_lines: int = 18) -> str:
    lines = [line.rstrip("\n") for line in stack.splitlines()]

    if len(lines) <= max_lines:
        return "\n".join(lines)

    return "\n".join(["...", *lines[-max_lines:]])


# ....................... #


def is_access_log(event: EventDict) -> bool:
    http = event.get("http")

    return (
        isinstance(http, dict)
        and "method" in http
        and "url" in http
        and "status_code" in http
    )


# ....................... #


def _last_six_chars(value: object) -> str:
    s = str(value)
    return s[-6:] if len(s) > 6 else s


def _repr_extra_value(value: Any) -> str:
    if isinstance(value, str) and not any(c in value for c in ' \t\r\n="'):
        return value

    return repr(value)


def _extra_display_value(key: str, value: Any) -> str:
    if key in _ID_SHORT_NAMES:
        if key in _ID_SHORTEN:
            return _last_six_chars(value)
        return _repr_extra_value(value)
    return _repr_extra_value(value)


# ....................... #


def process_common_log(
    event: EventDict,
    *,
    max_traceback_lines: int = 18,
) -> NormalizedEvent:
    ed = dict(event)

    stack = ed.pop("stack", None)
    exc_str = ed.pop("exception", None)
    ed.pop("exc_info", None)

    err_type = ed.pop(ERR_TYPE_KEY, None)
    err_message = ed.pop(ERR_MESSAGE_KEY, None)
    err_stack = ed.pop(ERR_STACK_KEY, None)

    ts = str(ed.pop("timestamp", ""))
    level = str(ed.pop("level", ""))
    logger_name = ed.pop("logger", None) or ed.pop("logger_name", None) or ""
    message = str(ed.pop("event", ""))

    if err_stack:
        err_stack = shorten_traceback_text(err_stack, max_lines=max_traceback_lines)

    elif exc_str:
        err_stack = shorten_traceback_text(exc_str, max_lines=max_traceback_lines)

    err_header = None

    if err_type is not None or err_message is not None:
        err_header = f"{err_type or 'Exception'}: {err_message or ''}".rstrip()

    extras = tuple(
        (k, _extra_display_value(k, ed[k]))
        for k in sorted(k for k in ed if not k.startswith("_"))
    )

    return NormalizedEvent(
        timestamp=ts,
        level=level,
        logger_name=logger_name,
        message=message,
        err_header=err_header,
        err_stack=err_stack,
        stack=stack,
        extras=extras,
    )


# ....................... #


def process_access_log(
    event: EventDict,
    common_event: NormalizedEvent,
) -> NormalizedEvent:
    ed = dict(event)

    http = ed.pop("http")
    network = ed.pop("network", None)
    duration = ed.pop("duration", None)

    method = str(http.get("method", ""))
    url = str(http.get("url", ""))
    status_code = str(http.get("status_code", ""))

    message = f"{method} {url} {status_code}".strip()
    extras: list[tuple[str, str]] = []

    if duration is not None:
        extras.append(("duration", f"{duration}ms"))

    if isinstance(network, dict):
        client = network.get("client")  # type: ignore

        if isinstance(client, dict):
            ip = client.get("ip")  # type: ignore
            port = client.get("port")  # type: ignore

            if ip is not None and port is not None:
                extras.append(("client", f"{ip}:{port}"))

    for key in sorted(k for k in ed if not k.startswith("_")):
        extras.append((key, _extra_display_value(key, ed[key])))

    return attrs.evolve(
        common_event,
        message=message,
        extras=tuple(extras),
        kind="access",
    )


# ....................... #


def normalize_event_dict(
    event: EventDict,
    *,
    max_traceback_lines: int = 18,
) -> NormalizedEvent:
    ed = dict(event)
    rendered_event = process_common_log(ed, max_traceback_lines=max_traceback_lines)

    if is_access_log(ed):
        rendered_event = process_access_log(ed, rendered_event)

    return rendered_event


# ....................... #
