"""Tests for the :class:`AuthnRequirement` value object and ``apply_authn_requirement``."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import attrs
import pytest
from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient

from forze.application.contracts.authn import AuthnIdentity
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import CoreError
from forze_fastapi.endpoints.http import (
    AuthnRequirement,
    HttpEndpointSpec,
    HttpSpec,
    apply_authn_requirement,
    build_authn_requirement_dependency,
)
from forze_fastapi.endpoints.http.composition import build_http_endpoint_spec
from forze_fastapi.endpoints.http.features.security import RequireAuthnFeature
from forze_fastapi.endpoints.http.mapping import EmptyMapper

# ----------------------- #

pytestmark = pytest.mark.unit


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _NoopHandler:
    async def __call__(self, args: None) -> None:
        return None


def _stub_endpoint_spec() -> HttpEndpointSpec[Any, Any, Any, Any, Any, None, None, None]:
    http: HttpSpec = {"method": "GET", "path": "/dummy"}

    return build_http_endpoint_spec(
        "dummy.noop",
        http=http,
        request_mapper=EmptyMapper(),
    )


# ....................... #


class TestAuthnRequirement:
    def test_token_header_branch(self) -> None:
        r = AuthnRequirement(authn_route="main", token_header="Authorization")
        assert r.scheme_kind == "bearer"
        assert r.scheme_name == "forze_authn__main__bearer"

    def test_token_cookie_branch(self) -> None:
        r = AuthnRequirement(authn_route="main", token_cookie="access_token")
        assert r.scheme_kind == "cookie"
        assert r.scheme_name == "forze_authn__main__cookie"

    def test_api_key_header_branch(self) -> None:
        r = AuthnRequirement(authn_route="main", api_key_header="X-API-Key")
        assert r.scheme_kind == "api_key"
        assert r.scheme_name == "forze_authn__main__api_key"

    def test_mutually_exclusive_transports_raises(self) -> None:
        with pytest.raises(CoreError, match="exactly one of"):
            AuthnRequirement(
                authn_route="main",
                token_header="Authorization",
                token_cookie="access_token",
            )

    def test_no_transport_raises(self) -> None:
        with pytest.raises(CoreError, match="exactly one"):
            AuthnRequirement(authn_route="main")


class TestApplyAuthnRequirement:
    def test_prepends_require_authn_feature(self) -> None:
        spec = _stub_endpoint_spec()
        req = AuthnRequirement(authn_route="main", token_header="Authorization")
        out = apply_authn_requirement(spec, req)
        assert out.features is not None
        assert len(out.features) == 1
        assert isinstance(out.features[0], RequireAuthnFeature)

    def test_merges_openapi_security(self) -> None:
        spec = _stub_endpoint_spec()
        req = AuthnRequirement(authn_route="main", token_header="Authorization")
        out = apply_authn_requirement(spec, req)
        assert out.metadata is not None
        assert "openapi_extra" in out.metadata
        assert "security" in out.metadata["openapi_extra"]
