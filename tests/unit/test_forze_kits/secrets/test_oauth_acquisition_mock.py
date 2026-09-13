"""The acquisition scenario with the token endpoint answered in-process.

One half of the mock-equals-real pair. The scenario lives in
:mod:`tests.support.oauth_acquisition` and the httpx leg drives the same body, so a
divergence between "a handler returns a dict" and "a server returns bytes over a real
transport" fails here rather than drifting. What this module owns is the wiring: a mock
HTTP registry answering the provider's token service, a mock rotating store whose
exchanger *is* the client, and a seeded client secret.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.http import RESPONSE_ERROR_DETAIL
from forze.application.contracts.secrets import (
    RotatingCredentialsDepKey,
    SecretsDepKey,
)
from forze.application.execution import ExecutionRuntime, InvocationMetadata
from forze.application.execution.deps import DepsRegistry
from forze.base.exceptions import CoreException, exc
from forze_kits.integrations.secrets import OAuth2ProviderConfig, OAuth2TokenClient
from forze_mock import MockDepsModule, MockHttpRegistry, MockRouteConfig
from tests.support.oauth_acquisition import (
    CLIENT_ID,
    CLIENT_SECRET,
    CLIENT_SECRET_REF,
    OAUTH2_ACQUISITION_BATTERY,
    PROVIDER_NAME,
    TOKEN_PATH,
    Check,
    OAuth2AcquisitionHarness,
    ScriptedProvider,
    battery_is_populated,
)

pytestmark = pytest.mark.unit

# ----------------------- #


def _provider_refusal(status: int, body: dict[str, object]) -> CoreException:
    """What the real transport raises for a rejected token request.

    The declared `error_type` is why the body survives, and this leg has to reproduce that
    contract or the two legs measure different things: without the detail, both a dead
    grant and a bad minute arrive as a bare failure. That the *transport* really attaches
    it is the httpx leg's job to prove; this states the shape the client is written against.
    """

    return exc.infrastructure(
        f"HTTP client error ({status}).",
        details={RESPONSE_ERROR_DETAIL: body},
    )


# ....................... #


def _provider_config() -> OAuth2ProviderConfig:
    return OAuth2ProviderConfig(
        name=PROVIDER_NAME,
        token_endpoint=TOKEN_PATH,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET_REF,
    )


# ....................... #


@pytest.mark.parametrize("check", OAUTH2_ACQUISITION_BATTERY, ids=lambda c: c.__name__)
async def test_oauth2_acquisition_battery(check: Check) -> None:
    provider = ScriptedProvider()
    runtime: ExecutionRuntime | None = None

    def answer(args: object) -> dict[str, object]:
        # The mock port hands the validated args model, so the form the client built is
        # read off it — the same mapping the httpx leg sees on the wire.
        form = args.model_dump(exclude_unset=True) if args is not None else {}
        status, body = provider.answer({k: str(v) for k, v in form.items()})

        if status >= 400:
            raise _provider_refusal(status, body)

        return body

    client = OAuth2TokenClient(
        config=_provider_config(),
        # Returns the runtime's own context, which is what the field asks for: contexts are
        # one per scope, and this one is created when the scope opens below.
        ctx_factory=lambda: runtime.get_context(),  # type: ignore[union-attr]
    )
    module = MockDepsModule(
        http=MockHttpRegistry().on(PROVIDER_NAME, "token", answer),
        rotating_credentials=client,
        routes={"rotating_credentials": MockRouteConfig(tenant_aware=True)},
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        await ctx.deps.provide(SecretsDepKey).put(CLIENT_SECRET_REF, CLIENT_SECRET)

        harness = OAuth2AcquisitionHarness(
            ctx=ctx,
            client=client,
            store=ctx.deps.resolve_simple(ctx, RotatingCredentialsDepKey),
            provider=provider,
            backend="mock-http",
        )

        with ctx.inv_ctx.bind(
            metadata=InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4()),
            tenant=harness.tenant,
        ):
            await check(harness)


# ....................... #


def test_the_battery_still_has_its_checks() -> None:
    battery_is_populated()
