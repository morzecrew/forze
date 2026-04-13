import attrs
import pytest

from forze.base.logging.renderers.normalization import (
    NormalizedEvent,
    is_access_log,
    normalize_event_dict,
    process_access_log,
    process_common_log,
    sanitize_extra_value,
    shorten_traceback_text,
)


class TestNormalizedEvent:
    def test_instantiation_required_only(self) -> None:
        event = NormalizedEvent(
            timestamp="2024-01-01T00:00:00Z",
            level="info",
            logger_name="test.logger",
            message="test message",
        )
        assert event.timestamp == "2024-01-01T00:00:00Z"
        assert event.level == "info"
        assert event.logger_name == "test.logger"
        assert event.message == "test message"
        assert event.extras == ()
        assert event.err_stack is None
        assert event.exc_info is None
        assert event.kind == "common"

    def test_instantiation_all_fields(self) -> None:
        event = NormalizedEvent(
            timestamp="2024-01-01T00:00:00Z",
            level="error",
            logger_name="test.logger",
            message="test error",
            extras=(("foo", "bar"),),
            err_stack="traceback",
            exc_info=(ValueError, ValueError("boom"), None),
            kind="access",
        )
        assert event.timestamp == "2024-01-01T00:00:00Z"
        assert event.level == "error"
        assert event.logger_name == "test.logger"
        assert event.message == "test error"
        assert event.extras == (("foo", "bar"),)
        assert event.err_stack == "traceback"
        assert event.exc_info[0] is ValueError
        assert event.kind == "access"

    def test_immutability(self) -> None:
        event = NormalizedEvent(
            timestamp="ts",
            level="info",
            logger_name="logger",
            message="msg",
        )
        with pytest.raises(attrs.exceptions.FrozenInstanceError):
            event.message = "new message"  # type: ignore


class TestUtilityFunctions:
    def test_shorten_traceback_text_short(self) -> None:
        stack = "line 1\nline 2\nline 3"
        assert shorten_traceback_text(stack, max_lines=5) == stack

    def test_shorten_traceback_text_long(self) -> None:
        stack = "\n".join(f"line {i}" for i in range(10))
        shortened = shorten_traceback_text(stack, max_lines=5)
        lines = shortened.splitlines()
        assert lines[0] == "..."
        assert len(lines) == 6  # ... + 5 lines
        assert lines[-1] == "line 9"
        assert lines[1] == "line 5"

    def test_is_access_log_true(self) -> None:
        event = {
            "http": {
                "method": "GET",
                "url": "/",
                "status_code": 200,
            }
        }
        assert is_access_log(event) is True

    def test_is_access_log_false(self) -> None:
        assert is_access_log({"event": "msg"}) is False
        assert is_access_log({"http": "not a dict"}) is False
        assert is_access_log({"http": {"method": "GET"}}) is False

    def test_sanitize_extra_value_simple(self) -> None:
        assert sanitize_extra_value("simple") == "simple"
        assert sanitize_extra_value("with-dash") == "with-dash"

    def test_sanitize_extra_value_quoting(self) -> None:
        assert sanitize_extra_value("with space") == "'with space'"
        assert sanitize_extra_value('with"quote') == "'with\"quote'"
        assert sanitize_extra_value("with=equal") == "'with=equal'"

    def test_sanitize_extra_value_non_string(self) -> None:
        assert sanitize_extra_value(123) == "123"
        assert sanitize_extra_value(None) == "None"
        assert sanitize_extra_value({"a": 1}) == "{'a': 1}"


class TestProcessingFunctions:
    def test_process_common_log_basic(self) -> None:
        event = {
            "timestamp": "2024-01-01T00:00:00Z",
            "level": "info",
            "logger": "test.logger",
            "event": "hello",
            "foo": "bar",
            "_private": "secret",
        }
        remaining, normalized = process_common_log(event)

        assert normalized.timestamp == "2024-01-01T00:00:00Z"
        assert normalized.level == "info"
        assert normalized.logger_name == "test.logger"
        assert normalized.message == "hello"
        assert normalized.extras == (("foo", "bar"),)
        assert normalized.kind == "common"

        # Remaining dict should not have timestamp, level, logger, event
        assert "timestamp" not in remaining
        assert "level" not in remaining
        assert "logger" not in remaining
        assert "event" not in remaining
        assert remaining["foo"] == "bar"
        assert remaining["_private"] == "secret"

    def test_process_common_log_with_error(self) -> None:
        event = {
            "timestamp": "ts",
            "level": "error",
            "event": "fail",
            "exception": "Value Error Traceback",
        }
        _, normalized = process_common_log(event)
        assert normalized.err_stack == "Value Error Traceback"

    def test_process_access_log(self) -> None:
        common_event = NormalizedEvent(
            timestamp="ts",
            level="info",
            logger_name="logger",
            message="original",
        )
        event = {
            "http": {
                "method": "POST",
                "url": "/submit",
                "status_code": 201,
            },
            "network": {
                "client": {
                    "ip": "127.0.0.1",
                    "port": 8080,
                }
            },
            "duration": 15.5,
            "extra_key": "val",
        }

        normalized = process_access_log(event, common_event)

        assert normalized.message == "POST /submit"
        assert normalized.kind == "access"
        # extras are sorted by key, but process_access_log puts status_code, duration, client first then others sorted.
        assert ("status_code", "201") in normalized.extras
        assert ("duration", "15.5ms") in normalized.extras
        assert ("client", "127.0.0.1:8080") in normalized.extras
        assert ("extra_key", "val") in normalized.extras

    def test_normalize_event_dict_common(self) -> None:
        event = {
            "timestamp": "ts",
            "level": "info",
            "event": "msg",
        }
        normalized = normalize_event_dict(event)
        assert normalized.kind == "common"
        assert normalized.message == "msg"

    def test_normalize_event_dict_access(self) -> None:
        event = {
            "timestamp": "ts",
            "level": "info",
            "event": "msg",
            "http": {
                "method": "GET",
                "url": "/api",
                "status_code": 200,
            },
        }
        normalized = normalize_event_dict(event)
        assert normalized.kind == "access"
        assert normalized.message == "GET /api"
