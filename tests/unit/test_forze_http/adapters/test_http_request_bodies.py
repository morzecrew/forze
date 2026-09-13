"""Form-encoded bodies, Basic client auth, and declared error responses, over real httpx.

Every assertion here reads the `httpx.Request` the transport was handed — the bytes and the
headers that actually went out — rather than what the adapter meant to send. That
distinction is the point of the file: an encoding bug is exactly the kind that passes a
test written against the adapter's own inputs.
"""

from __future__ import annotations

from urllib.parse import parse_qs

import httpx
import pytest
from pydantic import BaseModel

from forze.application.contracts.http import (
    RESPONSE_ERROR_DETAIL,
    HttpOperationSpec,
    HttpServiceSpec,
)
from forze.base.exceptions import CoreException, error_envelope
from forze_http.adapters.http_service import HttpServiceAdapter
from forze_http.execution.deps.configs import HttpAuthConfig, HttpServiceConfig
from forze_http.kernel.client import HttpClient

pytestmark = pytest.mark.unit

# ----------------------- #

_BASE = "https://provider.example"


class TokenArgs(BaseModel):
    grant_type: str
    code: str | None = None
    offline: bool = False


class TokenReply(BaseModel):
    access_token: str


class TokenError(BaseModel):
    error: str
    error_description: str | None = None


class SecretArgs(BaseModel):
    client_id: str
    client_secret: str
    code: str


class NestedArgs(BaseModel):
    grant_type: str
    extra: dict[str, str]


def _spec(
    *,
    encoding: str = "form",
    error_type: type[BaseModel] | None = None,
    args_type: type[BaseModel] = TokenArgs,
) -> HttpServiceSpec:
    return HttpServiceSpec(
        name="provider",
        operations={
            "token": HttpOperationSpec(
                name="token",
                method="POST",
                path="/oauth/token",
                args_type=args_type,
                return_type=TokenReply,
                body_encoding=encoding,  # type: ignore[arg-type]
                error_type=error_type,
            ),
            # A JSON sibling on the same service, which is the case per-operation encoding
            # exists for: one provider, one base URL, two content types.
            "profile": HttpOperationSpec(
                name="profile",
                method="POST",
                path="/v1/profile",
                args_type=TokenArgs,
                return_type=TokenReply,
            ),
        },
    )


async def _adapter(
    handler,
    *,
    spec: HttpServiceSpec | None = None,
    auth: HttpAuthConfig | None = None,
) -> HttpServiceAdapter:
    client = HttpClient()
    await client.initialize(base_url=_BASE, transport=httpx.MockTransport(handler))

    return HttpServiceAdapter(
        client=client,
        config=HttpServiceConfig(base_url=_BASE, auth=auth),
        spec=spec or _spec(),
    )


# ....................... #


class TestFormEncoding:
    async def test_a_form_operation_sends_urlencoded_bytes(self) -> None:
        seen: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["type"] = request.headers.get("content-type")
            seen["body"] = request.content.decode()

            return httpx.Response(200, json={"access_token": "at"})

        adapter = await _adapter(handler)
        result = await adapter.invoke("token", TokenArgs(grant_type="authorization_code", code="c"))

        assert isinstance(result, TokenReply)
        assert seen["type"] == "application/x-www-form-urlencoded"
        # Parsed rather than string-compared: field order is httpx's business, the pairs
        # are the contract.
        # `exclude_unset` is the plane's existing rule: a field the caller never set is
        # not sent. `offline` has a default and was not passed, so it is absent — which is
        # also why the boolean spelling is pinned in the parts tests, where it is reachable.
        assert parse_qs(str(seen["body"])) == {
            "grant_type": ["authorization_code"],
            "code": ["c"],
        }

    async def test_a_json_operation_on_the_same_service_is_untouched(self) -> None:
        seen: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["type"] = request.headers.get("content-type")
            seen["body"] = request.content.decode()

            return httpx.Response(200, json={"access_token": "at"})

        adapter = await _adapter(handler)
        await adapter.invoke("profile", TokenArgs(grant_type="x"))

        assert seen["type"] == "application/json"
        assert seen["body"] == '{"grant_type":"x"}'

    async def test_a_nested_field_is_refused_before_the_request(self) -> None:
        calls: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(str(request.url))

            return httpx.Response(200, json={"access_token": "at"})

        adapter = await _adapter(handler, spec=_spec(args_type=NestedArgs))

        with pytest.raises(CoreException) as raised:
            await adapter.invoke("token", NestedArgs(grant_type="x", extra={"a": "b"}))

        assert "extra" in str(raised.value)
        # Refused *before* the request: a half-encoded body must never reach a provider.
        assert calls == []

    async def test_an_explicit_boolean_is_sent_lowercase(self) -> None:
        seen: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["body"] = request.content.decode()

            return httpx.Response(200, json={"access_token": "at"})

        adapter = await _adapter(handler)
        await adapter.invoke("token", TokenArgs(grant_type="x", offline=True))

        # "True" is what Python would render and what a provider rejects.
        assert parse_qs(str(seen["body"]))["offline"] == ["true"]


class TestTheSecretInAFormBody:
    """RFC 0024's token POST puts a client secret in a form field, so this is the seam
    where that secret could reach a log. It does not, and that is pinned here rather than
    inferred from the scrubber's existence."""

    async def test_a_client_secret_is_redacted_in_exception_details(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, json={"error": "boom"})

        spec = _spec(args_type=SecretArgs)
        adapter = await _adapter(handler, spec=spec)

        with pytest.raises(CoreException) as raised:
            await adapter.invoke(
                "token",
                SecretArgs(client_id="cid", client_secret="SUPERSECRET", code="c"),
            )

        details = raised.value.details or {}
        rendered = repr(details)

        assert "SUPERSECRET" not in rendered
        assert "cid" in rendered
        # The failure is server-side, so nothing is exposed to a caller at all.
        assert error_envelope(raised.value).context is None


class TestBasicAuth:
    async def test_the_authorization_header_is_basic_and_exact(self) -> None:
        seen: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["auth"] = request.headers.get("authorization")

            return httpx.Response(200, json={"access_token": "at"})

        adapter = await _adapter(
            handler,
            auth=HttpAuthConfig(kind="basic", username="client-id", password="client-secret"),
        )
        await adapter.invoke("token", TokenArgs(grant_type="authorization_code"))

        # base64("client-id:client-secret"), computed by hand so the test fails if the
        # composition changes rather than tracking whatever the code happens to build.
        assert seen["auth"] == "Basic Y2xpZW50LWlkOmNsaWVudC1zZWNyZXQ="

    async def test_the_secret_is_not_in_the_config_repr(self) -> None:
        config = HttpAuthConfig(kind="basic", username="client-id", password="client-secret")

        assert "client-secret" not in repr(config)
        assert "client-id" in repr(config)

    def test_a_half_declared_credential_is_refused_at_wiring(self) -> None:
        # Sending no header at all would surface at the counterparty as a rejected
        # request rather than here as the wiring mistake it is.
        with pytest.raises(CoreException) as raised:
            HttpAuthConfig(kind="basic", username="client-id")

        assert "username and password" in str(raised.value)

    def test_an_absent_token_still_means_no_auth(self) -> None:
        # The token kinds keep their old behaviour — and this is the assertion that would
        # have caught `Authorization: Bearer None`.
        assert HttpAuthConfig().auth_headers() == {}
        assert HttpAuthConfig(kind="api_key").auth_headers() == {}


class TestDeclaredErrorResponses:
    async def test_a_declared_body_reaches_details(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                400,
                json={"error": "invalid_grant", "error_description": "code expired"},
            )

        adapter = await _adapter(handler, spec=_spec(error_type=TokenError))

        with pytest.raises(CoreException) as raised:
            await adapter.invoke("token", TokenArgs(grant_type="authorization_code"))

        declared = (raised.value.details or {})[RESPONSE_ERROR_DETAIL]

        assert declared == {"error": "invalid_grant", "error_description": "code expired"}

    async def test_an_undeclared_operation_is_unchanged(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(400, json={"error": "invalid_grant"})

        adapter = await _adapter(handler)

        with pytest.raises(CoreException) as raised:
            await adapter.invoke("token", TokenArgs(grant_type="authorization_code"))

        assert RESPONSE_ERROR_DETAIL not in (raised.value.details or {})
        assert "HTTP client error (400)" in raised.value.summary

    async def test_a_body_that_does_not_match_changes_nothing(self) -> None:
        # An error response that is itself broken must not replace the error the caller
        # was already being told about.
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(400, text="<html>gateway says no</html>")

        adapter = await _adapter(handler, spec=_spec(error_type=TokenError))

        with pytest.raises(CoreException) as raised:
            await adapter.invoke("token", TokenArgs(grant_type="authorization_code"))

        assert RESPONSE_ERROR_DETAIL not in (raised.value.details or {})
        assert "HTTP client error (400)" in raised.value.summary

    async def test_a_success_carries_no_error_detail(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"access_token": "at"})

        adapter = await _adapter(handler, spec=_spec(error_type=TokenError))
        result = await adapter.invoke("token", TokenArgs(grant_type="authorization_code"))

        assert isinstance(result, TokenReply)
