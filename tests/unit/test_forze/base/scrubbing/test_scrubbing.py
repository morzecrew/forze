"""Unit tests for forze.base.scrubbing."""

import pytest
from pydantic import BaseModel, EmailStr, SecretStr, ValidationError

from forze.base.scrubbing import (
    SECRET_PLACEHOLDER,
    dump_bound_args_for_errors,
    dump_for_error_context,
    sanitize,
    sanitize_pydantic_errors,
)

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
