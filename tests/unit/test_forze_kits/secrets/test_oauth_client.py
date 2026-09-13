"""The token client's own judgement, tested where the store cannot show it.

The acquisition battery asserts what the store *did* with a rejection — a burn notice, or a
credential left alone. This file asserts what the client *said*, because the mapping from a
provider's error code to a permanent-or-transient verdict is the client's decision and the
one that destroys credentials when it is wrong.
"""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from forze.application.contracts.secrets import (
    INVALID_GRANT_CODE,
    SecretRef,
    SecretsDepKey,
)
from forze.application.execution import ExecutionRuntime
from forze.application.execution.deps import DepsRegistry
from forze.base.exceptions import CoreException
from forze_http import HttpClient, HttpDepsModule
from forze_http.execution.deps.configs import HttpServiceConfig
from forze_kits.integrations.secrets import (
    OAuth2ProviderConfig,
    OAuth2TokenClient,
    OAuth2TokenResponse,
)
from forze_mock import MockDepsModule

pytestmark = pytest.mark.unit

# ----------------------- #

_BASE = "https://provider.example"
_SECRET_REF = SecretRef(path="providers/demo/client_secret")


async def _client_over(
    handler,
    *,
    token_auth: str = "post",
    client_secret: SecretRef | None = _SECRET_REF,
    auth=None,
):
    """A client wired over *handler*, plus the scope it runs in."""

    runtime: ExecutionRuntime | None = None
    client = OAuth2TokenClient(
        config=OAuth2ProviderConfig(
            name="demo",
            token_endpoint="/oauth/token",
            client_id="client-id",
            client_secret=client_secret,
            token_auth=token_auth,  # type: ignore[arg-type]
        ),
        ctx_factory=lambda: runtime.get_context(),  # type: ignore[union-attr]
    )

    http_client = HttpClient()
    await http_client.initialize(base_url=_BASE, transport=httpx.MockTransport(handler))
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(
            MockDepsModule(),
            HttpDepsModule(
                client=http_client,
                services={"demo": HttpServiceConfig(base_url=_BASE, auth=auth)},
            ),
        ).freeze()
    )

    return client, runtime, http_client


def _ok(**overrides: object) -> httpx.Response:
    body: dict[str, object] = {
        "access_token": "at",
        "refresh_token": "rt",
        "token_type": "Bearer",
    }
    body.update(overrides)

    return httpx.Response(200, json=body)


def _error(code: str, status: int = 400) -> httpx.Response:
    return httpx.Response(status, json={"error": code, "error_description": "scripted"})


# ....................... #


class TestClassification:
    """Which provider codes may burn a grant, and which may never."""

    @pytest.mark.parametrize("code", ["invalid_grant", "unauthorized_client"])
    async def test_a_dead_grant_is_permanent(self, code: str) -> None:
        client, runtime, http = await _client_over(lambda _r: _error(code))

        try:
            async with runtime.scope():
                await _seed(runtime)

                with pytest.raises(CoreException) as raised:
                    await client.exchange(SecretRef(path="g"), refresh_token="rt", metadata={})

            assert raised.value.code == INVALID_GRANT_CODE

        finally:
            await http.aclose()

    @pytest.mark.parametrize(
        ("code", "status"),
        [
            ("invalid_client", 401),
            ("invalid_request", 400),
            ("temporarily_unavailable", 503),
            ("slow_down", 400),
            ("something_the_provider_invented", 400),
        ],
    )
    async def test_everything_else_is_transient(self, code: str, status: int) -> None:
        # The asymmetry is the exchanger port's: a transient reported as permanent is
        # unrecoverable, a permanent reported as transient costs one wasted retry. So a
        # code nobody has seen before must not burn anything — and `invalid_client` is
        # *our* misconfiguration, not the grant's death.
        client, runtime, http = await _client_over(lambda _r: _error(code, status))

        try:
            async with runtime.scope():
                await _seed(runtime)

                with pytest.raises(CoreException) as raised:
                    await client.exchange(SecretRef(path="g"), refresh_token="rt", metadata={})

            assert raised.value.code != INVALID_GRANT_CODE, code

        finally:
            await http.aclose()

    async def test_a_rejection_with_no_body_is_transient(self) -> None:
        # An HTML error page from a proxy in front of the provider. Nothing says the grant
        # is dead, so nothing may burn it.
        client, runtime, http = await _client_over(
            lambda _r: httpx.Response(400, text="<html>go away</html>")
        )

        try:
            async with runtime.scope():
                await _seed(runtime)

                with pytest.raises(CoreException) as raised:
                    await client.exchange(SecretRef(path="g"), refresh_token="rt", metadata={})

            assert raised.value.code != INVALID_GRANT_CODE

        finally:
            await http.aclose()


class TestClientCredentialPlacement:
    async def test_post_places_them_in_the_body(self) -> None:
        seen: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            for pair in request.content.decode().split("&"):
                key, _, value = pair.partition("=")
                seen[key] = value

            return _ok()

        client, runtime, http = await _client_over(handler)

        try:
            async with runtime.scope():
                await _seed(runtime)
                await client.exchange(SecretRef(path="g"), refresh_token="rt", metadata={})

            assert seen["client_id"] == "client-id"
            assert seen["client_secret"] == "the-secret"

        finally:
            await http.aclose()

    async def test_basic_leaves_the_body_alone(self) -> None:
        # The credential belongs to the connection there, and the service's own auth config
        # sends it. Putting it in the body as well would present two credentials and let a
        # provider pick which to honour.
        seen: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["body"] = request.content.decode()
            seen["auth"] = request.headers.get("authorization", "")

            return _ok()

        from forze_http.execution.deps.configs import HttpAuthConfig

        client, runtime, http = await _client_over(
            handler,
            token_auth="basic",
            auth=HttpAuthConfig(kind="basic", username="client-id", password="the-secret"),
        )

        try:
            async with runtime.scope():
                await _seed(runtime)
                await client.exchange(SecretRef(path="g"), refresh_token="rt", metadata={})

            assert "client_secret" not in seen["body"]
            assert "client_id" not in seen["body"]
            assert seen["auth"] == "Basic Y2xpZW50LWlkOnRoZS1zZWNyZXQ="

        finally:
            await http.aclose()

    async def test_a_public_client_posts_no_secret(self) -> None:
        seen: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["body"] = request.content.decode()

            return _ok()

        client, runtime, http = await _client_over(handler, client_secret=None)

        try:
            async with runtime.scope():
                await client.exchange(SecretRef(path="g"), refresh_token="rt", metadata={})

            assert "client_secret" not in seen["body"]
            assert "client_id=client-id" in seen["body"]

        finally:
            await http.aclose()


class TestTheProjectedCredential:
    async def test_expires_in_becomes_an_absolute_instant(self) -> None:
        client, runtime, http = await _client_over(lambda _r: _ok(expires_in=3600))

        try:
            async with runtime.scope():
                await _seed(runtime)
                credential = await client.exchange(
                    SecretRef(path="g"), refresh_token="rt", metadata={}
                )

            assert credential.expires_at is not None
            ahead = (credential.expires_at - datetime.now(UTC)).total_seconds()
            assert 3500 < ahead <= 3600

        finally:
            await http.aclose()

    @pytest.mark.parametrize("expires_in", [None, 0, -1])
    async def test_a_useless_expiry_is_dropped(self, expires_in: int | None) -> None:
        # A credential born already expired would be refreshed on its first use for no
        # reason; a provider sending 0 is saying nothing, not saying "now".
        client, runtime, http = await _client_over(
            lambda _r: _ok(**({} if expires_in is None else {"expires_in": expires_in}))
        )

        try:
            async with runtime.scope():
                await _seed(runtime)
                credential = await client.exchange(
                    SecretRef(path="g"), refresh_token="rt", metadata={}
                )

            assert credential.expires_at is None

        finally:
            await http.aclose()

    async def test_an_acquisition_without_a_refresh_token_is_refused(self) -> None:
        # A grant that cannot be rotated has no business in a rotating store, and the
        # refusal names the provider — where storing an empty token would surface later as
        # an invalid_grant nobody can explain.
        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"access_token": "at", "token_type": "Bearer"})

        client, runtime, http = await _client_over(handler)

        try:
            async with runtime.scope():
                await _seed(runtime)

                with pytest.raises(CoreException) as raised:
                    await client.exchange_code(
                        code="c", code_verifier="v", redirect_uri="https://app/cb"
                    )

            assert "refresh_token" in str(raised.value)

        finally:
            await http.aclose()

    async def test_a_malformed_success_is_transient_not_a_dead_grant(self) -> None:
        # A 200 whose body is not a token response is a provider or proxy bug, and the
        # grant is untouched by it — so it must not burn anything.
        client, runtime, http = await _client_over(
            lambda _r: httpx.Response(200, json={"token_type": "Bearer"})
        )

        try:
            async with runtime.scope():
                await _seed(runtime)

                with pytest.raises(CoreException) as raised:
                    await client.exchange(SecretRef(path="g"), refresh_token="rt", metadata={})

            assert raised.value.code != INVALID_GRANT_CODE

        finally:
            await http.aclose()

    async def test_the_context_is_asked_for_on_every_exchange(self) -> None:
        # The factory contract: a client outlives any one scope, so caching a context
        # would leave a long-lived client exchanging through a dead one.
        calls: list[int] = []
        client, runtime, http = await _client_over(lambda _r: _ok())
        counting = OAuth2TokenClient(
            config=client.config,
            ctx_factory=lambda: (calls.append(1), runtime.get_context())[1],  # type: ignore[union-attr]
        )

        try:
            async with runtime.scope():
                await _seed(runtime)
                await counting.exchange(SecretRef(path="g"), refresh_token="rt", metadata={})
                await counting.exchange(SecretRef(path="g"), refresh_token="rt", metadata={})

            assert len(calls) == 2

        finally:
            await http.aclose()

    async def test_the_tokens_stay_out_of_every_repr(self) -> None:
        # The response model holds both tokens; a repr that printed them would put them in
        # any log that formats an unexpected value.
        reply = OAuth2TokenResponse(access_token="at-secret", refresh_token="rt-secret")

        assert "at-secret" not in repr(reply)
        assert "rt-secret" not in repr(reply)


# ....................... #


async def _seed(runtime: ExecutionRuntime) -> None:
    ctx = runtime.get_context()
    await ctx.deps.provide(SecretsDepKey).put(_SECRET_REF, "the-secret")
