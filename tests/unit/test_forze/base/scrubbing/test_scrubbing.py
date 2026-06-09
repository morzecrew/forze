"""Unit tests for forze.base.scrubbing."""

from collections.abc import Iterator

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import BaseModel, EmailStr, SecretStr, ValidationError

from tests.support.hypothesis_strategies import integration_hypothesis_settings

from forze.base.scrubbing import (
    SECRET_PLACEHOLDER,
    dump_bound_args_for_errors,
    dump_for_error_context,
    register_sensitive_patterns,
    sanitize,
    sanitize_pydantic_errors,
)
from forze.base.scrubbing.policy import scrub_log_string

# ----------------------- #


class _SecretModel(BaseModel):
    password: str
    api_key: SecretStr


class TestSanitizeEgress:
    def test_secret_str_masked(self) -> None:
        assert sanitize(SecretStr("hunter2"), context="egress") == SECRET_PLACEHOLDER

    def test_nested_mapping_sensitive_key(self) -> None:
        data = {"user": {"password": "hunter2"}, "id": "1"}
        assert sanitize(data, context="egress") == {
            "user": {"password": SECRET_PLACEHOLDER},
            "id": "1",
        }

    def test_egress_does_not_scrub_email_in_note(self) -> None:
        data = {"note": "contact alice@example.com"}
        assert sanitize(data, context="egress") == data


class TestSanitizeLog:
    def test_masks_sensitive_keys(self) -> None:
        data = {"password": "hunter2", "id": "1"}
        assert sanitize(data, context="log") == {
            "password": SECRET_PLACEHOLDER,
            "id": "1",
        }

    def test_scrubs_email_in_string_when_text_scrub_enabled(self) -> None:
        data = {"note": "contact alice@example.com"}
        result = sanitize(data, context="log", text_scrub=True)
        assert "alice@example.com" not in str(result["note"])
        assert SECRET_PLACEHOLDER in result["note"]

    def test_scrubs_bearer_in_note(self) -> None:
        data = {"note": "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9"}
        result = sanitize(data, context="log", text_scrub=True)
        assert "eyJ" not in result["note"]
        assert SECRET_PLACEHOLDER in result["note"]

    def test_scrubs_password_substring_in_note(self) -> None:
        data = {"note": "failed: password=hunter2"}
        result = sanitize(data, context="log", text_scrub=True)
        assert "hunter2" not in result["note"]
        assert SECRET_PLACEHOLDER in result["note"]

    def test_session_in_key_masks_whole_value(self) -> None:
        data = {"session_id": "abc"}
        assert sanitize(data, context="log") == {"session_id": SECRET_PLACEHOLDER}

    def test_text_scrub_can_be_disabled(self) -> None:
        data = {"note": "contact alice@example.com"}
        assert sanitize(data, context="log", text_scrub=False) == data

    def test_scrubs_postgresql_dsn_in_string(self) -> None:
        text = "connect failed: postgresql://user:secret@db.example.com:5432/app"
        result = scrub_log_string(text)
        assert "postgresql://" not in result
        assert SECRET_PLACEHOLDER in result

    def test_scrubs_inline_private_key_json_fragment(self) -> None:
        text = 'config {"private_key": "-----BEGIN PRIVATE KEY-----\\nabc"}'
        result = scrub_log_string(text)
        assert "-----BEGIN PRIVATE KEY-----" not in result
        assert SECRET_PLACEHOLDER in result


class TestSanitizePydanticErrors:
    def test_strips_input_and_ctx(self) -> None:
        class M(BaseModel):
            email: EmailStr

        with pytest.raises(ValidationError) as exc_info:
            M.model_validate({"email": "not-an-email"})

        sanitized = sanitize_pydantic_errors(exc_info.value.errors())
        assert sanitized
        assert "input" not in sanitized[0]
        assert "ctx" not in sanitized[0]
        assert "loc" in sanitized[0]
        assert "msg" in sanitized[0]


class TestDumpForErrorContext:
    def test_masks_secret_str_and_plain_password_field(self) -> None:
        model = _SecretModel(password="plain-secret", api_key=SecretStr("key-secret"))
        dumped = dump_for_error_context(model)
        assert dumped["password"] == SECRET_PLACEHOLDER
        assert dumped["api_key"] == SECRET_PLACEHOLDER


class TestDumpBoundArgsForErrors:
    def test_dumps_base_model_args(self) -> None:
        model = _SecretModel(password="x", api_key=SecretStr("y"))
        ctx = dump_bound_args_for_errors({"dto": model, "limit": 10})
        assert ctx["limit"] == 10
        assert ctx["dto"]["password"] == SECRET_PLACEHOLDER


@integration_hypothesis_settings
@given(
    key=st.text(min_size=1, max_size=12, alphabet=st.characters(blacklist_categories=("Cs",))),
    value=st.text(min_size=0, max_size=24),
)
def test_sanitize_log_masks_nested_sensitive_keys(key: str, value: str) -> None:
    sensitive_key = f"user_{key}_password"
    data = {"outer": {sensitive_key: value, "safe": "ok"}}
    result = sanitize(data, context="log")
    assert result["outer"]["safe"] == "ok"
    assert result["outer"][sensitive_key] == SECRET_PLACEHOLDER


class TestRegisterSensitivePatterns:
    @pytest.fixture(autouse=True)
    def _restore_policy(self) -> Iterator[None]:
        from forze.base.scrubbing import policy

        keys = list(policy._EXTRA_SENSITIVE_KEY_PATTERNS)
        logs = list(policy._EXTRA_LOG_STRING_PATTERNS)
        key_re = policy._sensitive_key_re
        log_re = policy._log_string_re

        try:
            yield

        finally:
            policy._EXTRA_SENSITIVE_KEY_PATTERNS[:] = keys
            policy._EXTRA_LOG_STRING_PATTERNS[:] = logs
            policy._sensitive_key_re = key_re
            policy._log_string_re = log_re

    def test_custom_key_pattern_masks_value(self) -> None:
        field = "acme_widget_handle"

        assert sanitize({field: "v"}, context="log") == {field: "v"}

        register_sensitive_patterns(keys=[r"widget[._ -]?handle"])

        assert sanitize({field: "v"}, context="log") == {field: SECRET_PLACEHOLDER}

    def test_custom_log_string_pattern_is_scrubbed(self) -> None:
        text = "issued ACME-TOKEN-abc123"

        assert scrub_log_string(text) == text

        register_sensitive_patterns(log_strings=[r"ACME-TOKEN-\S+"])
        result = scrub_log_string(text)

        assert "abc123" not in result
        assert SECRET_PLACEHOLDER in result

    def test_empty_patterns_are_ignored(self) -> None:
        register_sensitive_patterns(keys=[""], log_strings=[""])

        # An empty fragment would otherwise match everything; it must be dropped.
        assert sanitize({"plain_field": "v"}, context="log") == {"plain_field": "v"}
        assert scrub_log_string("nothing sensitive here") == "nothing sensitive here"
