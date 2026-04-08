"""Unit tests for log event normalization helpers."""

from forze.base.logging.constants import (
    ERR_MESSAGE_KEY,
    ERR_STACK_KEY,
    ERR_TYPE_KEY,
    RICH_EXC_INFO_KEY,
)
from forze.base.logging.renderers.normalization import (
    is_access_log,
    normalize_event_dict,
    process_access_log,
    process_common_log,
    sanitize_extra_value,
    shorten_traceback_text,
)


def test_shorten_traceback_text_unchanged_when_short() -> None:
    stack = "line1\nline2\nline3"
    assert shorten_traceback_text(stack, max_lines=10) == stack


def test_shorten_traceback_text_truncates_long_stack() -> None:
    lines = [f"  File \"x.py\", line {i}" for i in range(30)]
    stack = "\n".join(lines)
    out = shorten_traceback_text(stack, max_lines=5)
    assert out.startswith("...")
    tail = "\n".join(lines[-5:])
    assert out.endswith(tail)


def test_is_access_log_positive() -> None:
    assert is_access_log(
        {
            "http": {
                "method": "GET",
                "url": "/api",
                "status_code": 200,
            }
        }
    )


def test_is_access_log_negative() -> None:
    assert not is_access_log({"http": {"method": "GET"}})
    assert not is_access_log({})


def test_sanitize_extra_value_plain_token() -> None:
    assert sanitize_extra_value("abc-123") == "abc-123"


def test_sanitize_extra_value_uses_repr_when_needed() -> None:
    assert sanitize_extra_value("a b") == repr("a b")
    assert sanitize_extra_value('x"y') == repr('x"y')


def test_process_common_log_strips_error_keys_and_shortens_stack() -> None:
    long_tb = "\n".join([f"  line {i}" for i in range(40)])
    event = {
        "timestamp": "t",
        "level": "error",
        "logger": "app",
        "event": "boom",
        ERR_TYPE_KEY: "ValueError",
        ERR_MESSAGE_KEY: "bad",
        ERR_STACK_KEY: long_tb,
        "extra_field": 42,
    }
    remainder, norm = process_common_log(event, max_traceback_lines=8)
    assert ERR_TYPE_KEY not in remainder and ERR_MESSAGE_KEY not in remainder
    assert norm.err_stack is not None
    assert norm.err_stack.count("\n") <= 8 + 1  # ellipsis + 8 lines
    assert ("extra_field", "42") in norm.extras


def test_process_common_log_uses_exception_string_when_no_stack() -> None:
    event = {
        "timestamp": "t",
        "level": "error",
        "logger": "app",
        "event": "boom",
        "exception": "short err",
    }
    _, norm = process_common_log(event)
    assert norm.err_stack == "short err"


def test_process_access_log_adds_duration_and_client() -> None:
    _, common = process_common_log(
        {
            "timestamp": "t",
            "level": "info",
            "logger": "uvicorn",
            "event": "ignored",
        }
    )
    access_event = {
        "http": {
            "method": "POST",
            "url": "/items",
            "status_code": 201,
        },
        "network": {"client": {"ip": "10.0.0.1", "port": 12345}},
        "duration": 12,
        "trace_id": "abc",
    }
    out = process_access_log(access_event, common)
    assert out.kind == "access"
    assert out.message == "POST /items"
    extra = dict(out.extras)
    assert extra["status_code"] == "201"
    assert extra["duration"] == "12ms"
    assert extra["client"] == "10.0.0.1:12345"
    assert extra["trace_id"] == "abc"


def test_normalize_event_dict_routes_to_access_renderer() -> None:
    ev = {
        "timestamp": "t",
        "level": "info",
        "logger": "uvicorn.access",
        "event": "request",
        "http": {"method": "GET", "url": "/", "status_code": 200},
    }
    norm = normalize_event_dict(ev)
    assert norm.kind == "access"
    assert "GET /" in norm.message


def test_process_common_log_accepts_logger_name_key() -> None:
    event = {
        "timestamp": "t",
        "level": "debug",
        "logger_name": "other",
        "event": "ping",
    }
    _, norm = process_common_log(event)
    assert norm.logger_name == "other"


def test_process_common_log_keeps_rich_exc_info() -> None:
    exc_info = (ValueError, ValueError("x"), None)
    event = {
        "timestamp": "t",
        "level": "error",
        "logger": "app",
        "event": "e",
        RICH_EXC_INFO_KEY: exc_info,
    }
    _, norm = process_common_log(event)
    assert norm.exc_info == exc_info
