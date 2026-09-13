"""The acquisition scenario over a real `forze_http` service — the other half of the pair.

The scenario is the one the in-process leg drives, imported rather than restated. What runs
here is the transport: a form-encoded POST goes out over httpx, and a rejected response's
declared error body comes back through the plane's own error path. That is the half the
in-process leg cannot prove, and the half the whole classification rests on — without the
body, a dead grant and a bad minute are both a bare 400.

It also wires the service the way the docs say to, with `egress_sensitive` and its
acknowledgement, so the one thing the kit cannot check for itself is checked here.
"""

from __future__ import annotations

from urllib.parse import parse_qs
from uuid import uuid4

import httpx
import pytest

from forze.application.contracts.secrets import (
    RotatingCredentialsDepKey,
    SecretsDepKey,
)
from forze.application.execution import ExecutionRuntime, InvocationMetadata
from forze.application.execution.deps import DepsRegistry
from forze_http import HttpClient, HttpDepsModule
from forze_http.execution.deps.configs import HttpServiceConfig
from forze_kits.integrations.secrets import OAuth2ProviderConfig, OAuth2TokenClient
from forze_mock import MockDepsModule, MockRouteConfig
from tests.support.oauth_acquisition import (
    CLIENT_ID,
    CLIENT_SECRET,
    CLIENT_SECRET_REF,
    OAUTH2_ACQUISITION_BATTERY,
    PROVIDER_NAME,
    TOKEN_PATH,
    Check,
    FailablePutStore,
    OAuth2AcquisitionHarness,
    ScriptedProvider,
    battery_is_populated,
)

pytestmark = pytest.mark.unit

# ----------------------- #

_BASE_URL = "https://provider.example"


def _token_service() -> HttpServiceConfig:
    """The wiring the docs prescribe for a token endpoint.

    The egress flags are the point: the request carries a code and a client secret out of
    the trust boundary and the response carries a token back, so the route says so — and
    declaring the first without the second fails here rather than shipping the data.
    """

    return HttpServiceConfig(
        base_url=_BASE_URL,
        egress_sensitive=True,
        acknowledge_data_egress=True,
    )


def _transport(provider: ScriptedProvider) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["content-type"] == "application/x-www-form-urlencoded"
        assert str(request.url) == f"{_BASE_URL}{TOKEN_PATH}"

        # The secret travels in the body and nowhere else: a query string ends up in
        # every proxy log between here and the provider.
        assert "client_secret" not in str(request.url)
        assert "code=" not in str(request.url)

        form = {k: v[0] for k, v in parse_qs(request.content.decode()).items()}
        status, body = provider.answer(form)

        return httpx.Response(status, json=body)

    return httpx.MockTransport(handler)


# ....................... #


@pytest.mark.parametrize("check", OAUTH2_ACQUISITION_BATTERY, ids=lambda c: c.__name__)
async def test_oauth2_acquisition_battery(check: Check) -> None:
    provider = ScriptedProvider()
    runtime: ExecutionRuntime | None = None

    client = OAuth2TokenClient(
        config=OAuth2ProviderConfig(
            name=PROVIDER_NAME,
            token_endpoint=TOKEN_PATH,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET_REF,
        ),
        ctx_factory=lambda: runtime.get_context(),  # type: ignore[union-attr]
    )

    http_client = HttpClient()
    await http_client.initialize(base_url=_BASE_URL, transport=_transport(provider))

    registry = DepsRegistry.from_modules(
        MockDepsModule(
            rotating_credentials=client,
            routes={"rotating_credentials": MockRouteConfig(tenant_aware=True)},
        ),
        HttpDepsModule(client=http_client, services={PROVIDER_NAME: _token_service()}),
    ).freeze()
    runtime = ExecutionRuntime(deps=registry)

    try:
        async with runtime.scope():
            ctx = runtime.get_context()
            await ctx.deps.provide(SecretsDepKey).put(CLIENT_SECRET_REF, CLIENT_SECRET)

            harness = OAuth2AcquisitionHarness(
                ctx=ctx,
                client=client,
                store=FailablePutStore(
                    inner=ctx.deps.resolve_simple(ctx, RotatingCredentialsDepKey)
                ),
                provider=provider,
                backend="httpx",
            )

            with ctx.inv_ctx.bind(
                metadata=InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4()),
                tenant=harness.tenant,
            ):
                await check(harness)

    finally:
        # `finally`, so a failed assertion still closes the pool rather than leaking a
        # client into whatever runs next.
        await http_client.aclose()


# ....................... #


def test_the_battery_still_has_its_checks() -> None:
    battery_is_populated()


# ....................... #


class TestTheWiringTheKitCannotCheck:
    def test_the_token_service_declares_acknowledged_egress(self) -> None:
        config = _token_service()

        assert config.egress_sensitive is True
        assert config.acknowledge_data_egress is True

    def test_declaring_the_egress_without_accepting_it_fails_to_wire(self) -> None:
        # `forze_kits` cannot import `forze_http`, so the kit cannot enforce this on the
        # application's behalf. This is where the claim is held instead.
        from forze.base.exceptions import CoreException

        with pytest.raises(CoreException) as raised:
            HttpServiceConfig(base_url=_BASE_URL, egress_sensitive=True)

        assert raised.value.code == "http_egress_unacknowledged"
