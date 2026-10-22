"""A 422 at the HTTP edge says which field failed, never what the caller sent.

A validator's ``raise ValueError(f"bad key {value!r}")`` puts the rejected value into pydantic's
``msg``. Both edge paths, FastAPI's own request validation and the manually assembled payloads
behind ``validate_payload``, render that ``msg`` to the client, so both must scrub it.
"""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI
from pydantic import BaseModel, field_validator
from starlette.testclient import TestClient

from forze.base.exceptions import CoreException
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.routes._attach import validate_payload
from forze_kits.dto.querying import OptionalFilterExpression

pytestmark = pytest.mark.unit

NEEDLE = "pasted-secret-7f3a"


class Body(BaseModel):
    key: str

    @field_validator("key")
    @classmethod
    def _reject(cls, value: str) -> str:
        raise ValueError(f"bad key {value!r}")


class ListBody(BaseModel):
    filters: OptionalFilterExpression = None


class TestValidationMessages:
    def test_request_validation_does_not_echo_a_validator_message(self) -> None:
        app = FastAPI()

        @app.post("/echo")
        def echo(body: Body) -> dict[str, str]:
            return {"key": body.key}

        register_exception_handlers(app)
        response = TestClient(app).post("/echo", json={"key": NEEDLE})

        assert response.status_code == 422
        assert NEEDLE not in response.text
        errors = response.json()["context"]["errors"]
        # Still actionable: the field that failed and the rule it broke.
        assert [(e["loc"][-1], e["type"]) for e in errors] == [("key", "value_error")]

    def test_a_query_refusal_keeps_the_parsers_guidance(self) -> None:
        """The parser names the key at fault, never a value, so its summary is not scrubbed."""

        app = FastAPI()

        @app.post("/list")
        def list_(body: ListBody) -> dict[str, str]:
            return {}

        register_exception_handlers(app)
        response = TestClient(app).post("/list", json={"filters": {"$bogus": 1}})

        assert response.status_code == 422
        (error,) = response.json()["context"]["errors"]
        assert error["type"] == "query_expression_invalid"
        assert "Unknown filter key $bogus" in error["msg"]

    def test_validate_payload_does_not_echo_a_validator_message(self) -> None:
        with pytest.raises(CoreException) as ei:
            validate_payload(Body, {"key": NEEDLE}, "widgets.update")

        assert ei.value.details is not None
        assert NEEDLE not in repr(ei.value.details)
        errors = ei.value.details["errors"]
        assert [(e["loc"][-1], e["type"]) for e in errors] == [("key", "value_error")]
