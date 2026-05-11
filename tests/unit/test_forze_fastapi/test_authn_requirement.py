"""Tests for the :class:`AuthnRequirement` value object and ``apply_authn_requirement``."""

from __future__ import annotations

from typing import Any

import attrs
import pytest

from forze.application.execution import Usecase, UsecasesFacade, facade_op
from forze.base.errors import CoreError
from forze_fastapi.endpoints.http import (
    AuthnRequirement,
    HttpEndpointSpec,
    HttpSpec,
    apply_authn_requirement,
)
from forze_fastapi.endpoints.http.composition import build_http_endpoint_spec
from forze_fastapi.endpoints.http.features.security import RequireAuthnFeature
from forze_fastapi.endpoints.http.mapping import EmptyMapper

# ----------------------- #


pytestmark = pytest.mark.unit


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _NoopUsecase(Usecase[None, None]):
    async def main(self, args: None) -> None:
        return None


@attrs.define(slots=True, kw_only=True, frozen=True)
class _DummyFacade(UsecasesFacade):
    noop = facade_op("dummy.noop", uc=_NoopUsecase)


def _stub_endpoint_spec() -> HttpEndpointSpec[Any, Any, Any, Any, Any, None, None, None, _DummyFacade]:
    http: HttpSpec = {"method": "GET", "path": "/dummy"}

    return build_http_endpoint_spec(
        _DummyFacade,
        _DummyFacade.noop,  # type: ignore[arg-type]
        http=http,
        mapper=EmptyMapper(),
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

    def test_no_transport_rejected(self) -> None:
        with pytest.raises(CoreError, match="exactly one"):
            AuthnRequirement(authn_route="main")

    def test_multiple_transports_rejected(self) -> None:
        with pytest.raises(CoreError, match="exactly one"):
            AuthnRequirement(
                authn_route="main",
                token_header="Authorization",
                token_cookie="access_token",
            )

    def test_empty_route_rejected(self) -> None:
        with pytest.raises(CoreError, match="non-empty"):
            AuthnRequirement(authn_route="", token_header="Authorization")


# ....................... #


class TestApplyAuthnRequirement:
    def test_none_is_passthrough(self) -> None:
        spec = _stub_endpoint_spec()
        out = apply_authn_requirement(spec, None)
        assert out is spec

    def test_prepends_require_authn_feature(self) -> None:
        spec = _stub_endpoint_spec()
        req = AuthnRequirement(authn_route="main", token_header="Authorization")

        out = apply_authn_requirement(spec, req)

        assert out.features is not None
        assert any(isinstance(f, RequireAuthnFeature) for f in out.features)

    def test_injects_security_scheme(self) -> None:
        spec = _stub_endpoint_spec()
        req = AuthnRequirement(authn_route="main", token_cookie="access_token")

        out = apply_authn_requirement(spec, req)

        assert out.metadata is not None
        extra = out.metadata.get("openapi_extra")
        assert extra is not None
        assert "components" in extra
        schemes = extra["components"]["securitySchemes"]
        assert req.scheme_name in schemes
        assert schemes[req.scheme_name]["type"] == "apiKey"
        assert schemes[req.scheme_name]["in"] == "cookie"

        security = extra["security"]
        assert {req.scheme_name: []} in security

    def test_api_key_header_security_scheme(self) -> None:
        spec = _stub_endpoint_spec()
        req = AuthnRequirement(authn_route="main", api_key_header="X-API-Key")

        out = apply_authn_requirement(spec, req)

        assert out.metadata is not None
        extra = out.metadata["openapi_extra"]
        scheme = extra["components"]["securitySchemes"][req.scheme_name]
        assert scheme["type"] == "apiKey"
        assert scheme["in"] == "header"
        assert scheme["name"] == "X-API-Key"

    def test_bearer_security_scheme(self) -> None:
        spec = _stub_endpoint_spec()
        req = AuthnRequirement(
            authn_route="main",
            token_header="Authorization",
            bearer_format="Opaque",
            description="Forze access token",
        )

        out = apply_authn_requirement(spec, req)

        assert out.metadata is not None
        extra = out.metadata["openapi_extra"]
        scheme = extra["components"]["securitySchemes"][req.scheme_name]
        assert scheme["type"] == "http"
        assert scheme["scheme"] == "bearer"
        assert scheme["bearerFormat"] == "Opaque"
        assert scheme["description"] == "Forze access token"

    def test_preserves_existing_features(self) -> None:
        @attrs.define(slots=True, frozen=True, kw_only=True)
        class _NoopFeature:
            def wrap(self, handler):  # type: ignore[no-untyped-def]
                return handler

        spec = attrs.evolve(_stub_endpoint_spec(), features=(_NoopFeature(),))  # type: ignore[arg-type]

        req = AuthnRequirement(authn_route="main", token_header="Authorization")

        out = apply_authn_requirement(spec, req)

        assert out.features is not None
        # RequireAuthn prepended, original feature preserved.
        assert isinstance(out.features[0], RequireAuthnFeature)
        assert any(type(f).__name__ == "_NoopFeature" for f in out.features)
