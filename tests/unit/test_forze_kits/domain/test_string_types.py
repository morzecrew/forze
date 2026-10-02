"""A kits text field refuses a value that is not text with a validation error, not a crash.

``String`` and ``LongString`` normalize their input before pydantic validates it as a string,
and the metadata mixin trims ``display_name`` and ``description`` before that. Both called
string methods on whatever arrived, so a JSON number, list, object or boolean raised a raw
exception and a route answered 500 instead of 422.
"""

from __future__ import annotations

from typing import Annotated, Any

import pytest
from pydantic import ConfigDict, Strict, ValidationError

from forze.domain.models import BaseDTO
from forze_kits.domain.base.types import LongString, String
from forze_kits.domain.metadata import MetadataCreateCmdMixin, MetadataUpdateCmdMixin


class _Body(BaseDTO):
    name: String
    note: LongString | None = None


class _Metadata(MetadataUpdateCmdMixin): ...


_NOT_TEXT: list[Any] = [5, 5.5, True, [], ["x"], {}, {"a": 1}]


class TestAValueThatIsNotText:
    @pytest.mark.parametrize("value", _NOT_TEXT)
    @pytest.mark.parametrize("field", ["name", "note"])
    def test_is_a_validation_error(self, field: str, value: Any) -> None:
        body = {"name": "ok", field: value}

        with pytest.raises(ValidationError) as raised:
            _Body.model_validate(body)

        assert [error["loc"] for error in raised.value.errors()] == [(field,)]

    @pytest.mark.parametrize("value", _NOT_TEXT)
    @pytest.mark.parametrize("field", ["display_name", "description"])
    def test_is_a_validation_error_in_the_metadata_fields(self, field: str, value: Any) -> None:
        with pytest.raises(ValidationError) as raised:
            _Metadata.model_validate({field: value})

        assert [error["loc"] for error in raised.value.errors()] == [(field,)]

    def test_is_answered_422_by_a_route(self) -> None:
        pytest.importorskip("fastapi")

        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from forze_fastapi.exceptions import register_exception_handlers

        app = FastAPI()
        register_exception_handlers(app)

        @app.post("/things")
        def create(body: _Body) -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
            return {"name": body.name}

        client = TestClient(app, raise_server_exceptions=False)

        assert client.post("/things", json={"name": 5}).status_code == 422
        assert client.post("/things", json={"name": "a  b"}).json() == {"name": "a b"}


class TestBytes:
    """Pydantic reads UTF-8 bytes as a string, so they are decoded and normalized like one."""

    @pytest.mark.parametrize("value", [b"ab  cd", bytearray(b"ab  cd")])
    def test_are_normalized(self, value: bytes | bytearray) -> None:
        assert _Body(name=value).name == "ab cd"  # type: ignore[arg-type]

    def test_that_are_not_utf8_are_a_validation_error(self) -> None:
        with pytest.raises(ValidationError, match="string_unicode"):
            _Body(name=b"\xff\xfe")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "value",
        # Blank once normalized: an invisible or control character alone is no content either.
        [b"   ", bytearray(b" "), "   ", "﻿", "​ ​", "\x00", "\n \n", b"\xef\xbb\xbf"],
    )
    @pytest.mark.parametrize("field", ["display_name", "description"])
    def test_that_are_blank_leave_a_metadata_field_unset(self, field: str, value: Any) -> None:
        assert getattr(_Metadata.model_validate({field: value}), field) is None
        assert getattr(MetadataCreateCmdMixin.model_validate({"name": "ok", field: value}), field) is None


class _StrictBody(BaseDTO):
    model_config = ConfigDict(strict=True)

    name: String


class _StrictMetadata(MetadataUpdateCmdMixin):
    model_config = ConfigDict(strict=True)


class TestAStrictModel:
    @pytest.mark.parametrize("value", [b"ab", bytearray(b"ab")])
    def test_refuses_bytes(self, value: bytes | bytearray) -> None:
        with pytest.raises(ValidationError, match="string_type"):
            _StrictBody.model_validate({"name": value})

        with pytest.raises(ValidationError, match="string_type"):
            _StrictMetadata.model_validate({"display_name": value})

    def test_still_normalizes_text(self) -> None:
        assert _StrictBody.model_validate({"name": "a  b"}).name == "a b"

    def test_with_a_field_level_strict_still_decodes_bytes(self) -> None:
        # A before-validator sees the model's config, not a field's own `Strict()` or a
        # `model_validate(..., strict=True)` call, so bytes are decoded there as in lax mode.
        class _FieldStrict(BaseDTO):
            name: Annotated[String, Strict()]

        assert _FieldStrict.model_validate({"name": b"a  b"}).name == "a b"
        assert _Body.model_validate({"name": b"a  b"}, strict=True).name == "a b"
