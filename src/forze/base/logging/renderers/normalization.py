from typing import Any, Literal

import attrs
from structlog.typing import EventDict, ExcInfo

from ..constants import ERR_MESSAGE_KEY, ERR_STACK_KEY, ERR_TYPE_KEY, RICH_EXC_INFO_KEY

# ----------------------- #
#! Yes we leak some information here, but it's for the sake of readability.
#! It's super tricky to maintain strict isolation :/
#! Or we need to put renderer somewhere else.


@attrs.define(slots=True, frozen=True, kw_only=True)
class NormalizedEvent:
    timestamp: str
    level: str
    logger_name: str
    message: str
    extras: tuple[tuple[str, str], ...] = ()
    err_stack: str | None = None
    exc_info: ExcInfo | None = None
    kind: Literal["common", "access"] = "common"


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


def sanitize_extra_value(value: Any) -> str:
    if isinstance(value, str) and not any(c in value for c in ' \t\r\n="'):
        return value

    return repr(value)


# ....................... #


def process_common_log(
    event: EventDict,
    *,
    max_traceback_lines: int = 18,
) -> tuple[dict[str, Any], NormalizedEvent]:
    ed = dict(event)

    exc_str = ed.pop("exception", None)
    ed.pop(ERR_TYPE_KEY, None)
    ed.pop(ERR_MESSAGE_KEY, None)
    err_stack = ed.pop(ERR_STACK_KEY, None)
    exc_info = ed.pop(RICH_EXC_INFO_KEY, None)

    ts = str(ed.pop("timestamp", ""))
    level = str(ed.pop("level", ""))
    logger_name = ed.pop("logger", None) or ed.pop("logger_name", None) or ""
    message = str(ed.pop("event", ""))

    if err_stack:
        err_stack = shorten_traceback_text(err_stack, max_lines=max_traceback_lines)

    elif exc_str:
        err_stack = shorten_traceback_text(exc_str, max_lines=max_traceback_lines)

    extras = tuple(
        (k, sanitize_extra_value(ed[k]))
        for k in sorted(k for k in ed if not k.startswith("_"))
    )

    return ed, NormalizedEvent(
        timestamp=ts,
        level=level,
        logger_name=logger_name,
        message=message,
        err_stack=err_stack,
        extras=extras,
        exc_info=exc_info,
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
    status_code = str(http.get("status_code", "500"))

    message = f"{method} {url}".strip()
    extras: list[tuple[str, str]] = [("status_code", status_code)]

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
        extras.append((key, sanitize_extra_value(ed[key])))

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

    # We chain `ed` here to avoid keeping error keys in the event dict.
    ed, rendered_event = process_common_log(
        ed,
        max_traceback_lines=max_traceback_lines,
    )

    if is_access_log(ed):
        rendered_event = process_access_log(ed, rendered_event)

    return rendered_event
