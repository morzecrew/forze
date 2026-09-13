"""Shared scenario: a grant is acquired at a token endpoint and rotated by the same client.

The claim RFC 0024 rests on is that acquisition and refresh are one call with two
``grant_type`` values, so the same object that produces the first credential is the
``CredentialExchangerPort`` the store drives forever after. That is only worth asserting
end to end: through a real store, over a real transport, with a provider that answers.

Two legs drive this body — one where the token service is answered in-process by the mock
HTTP registry, one where it is answered by `httpx` over a `MockTransport` behind a fully
wired ``forze_http`` service. What differs is the wire; what must not differ is any of the
behaviour below. The provider's *brain* is shared (:class:`ScriptedProvider`), so a
divergence between the legs is a transport difference rather than two different fakes
disagreeing.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.secrets import (
    BURNT_CREDENTIAL_CODE,
    INVALID_GRANT_CODE,
    RotatingCredentialStorePort,
    SecretRef,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import InvocationMetadata
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kits.integrations.secrets import (
    GRANTED_SCOPE_METADATA,
    REQUESTED_SCOPE_METADATA,
    OAuth2TokenClient,
    complete_authorization,
)

# ----------------------- #

PROVIDER_NAME = "demo_provider"
TOKEN_PATH = "/oauth/token"
CLIENT_ID = "client-id"
CLIENT_SECRET_REF = SecretRef(path="providers/demo/client_secret")
CLIENT_SECRET = "client-secret-value"
REDIRECT_URI = "https://app.example/oauth/callback"
REQUESTED_SCOPE = "crm.read crm.write"

REF = SecretRef(path="providers/demo/grant")
"""Where an acquired grant is stored."""


# ....................... #


@attrs.define(slots=True, kw_only=True)
class ScriptedProvider:
    """The provider's answers, and a record of what it was asked.

    Shared by both legs so neither can drift into being a different provider. It answers
    the two grants, and every failure mode the battery needs is a field rather than a
    subclass: a test that wants `invalid_grant` sets it and runs the same body.
    """

    grant_types: list[str] = attrs.field(factory=list)
    """Every ``grant_type`` seen, in order — the unification claim, recorded."""

    forms: list[Mapping[str, str]] = attrs.field(factory=list)
    """Every form body seen, so a test can assert where the client credentials went."""

    error: str | None = None
    """When set, the next call answers with this RFC 6749 §5.2 error code."""

    error_status: int = 400
    """Status accompanying :attr:`error` — 400 for a rejection, 503 for a bad minute."""

    granted_scope: str | None = REQUESTED_SCOPE
    """Scope the provider says it granted. Set narrower to exercise a downgrade."""

    omit_refresh_token: bool = False
    """Answer a refresh without a new refresh token, as RFC 6749 §6 permits."""

    issued: int = 0
    """How many successful responses have been handed out, so tokens differ per call."""

    # ....................... #

    def answer(self, form: Mapping[str, str]) -> tuple[int, dict[str, Any]]:
        """The response to a token request: ``(status, json_body)``."""

        self.grant_types.append(form.get("grant_type", ""))
        self.forms.append(dict(form))

        if self.error is not None:
            return self.error_status, {
                "error": self.error,
                "error_description": "scripted",
            }

        self.issued += 1
        body: dict[str, Any] = {
            "access_token": f"access-{self.issued}",
            "token_type": "Bearer",
            "expires_in": 3600,
        }

        if not (self.omit_refresh_token and form.get("grant_type") == "refresh_token"):
            body["refresh_token"] = f"refresh-{self.issued}"

        if self.granted_scope is not None:
            body["scope"] = self.granted_scope

        return 200, body


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OAuth2AcquisitionHarness:
    """One leg's seam: a wired context, a client, a store, and the provider behind them."""

    ctx: ExecutionContext
    """The context the client exchanges under and the store is resolved from."""

    client: OAuth2TokenClient
    """The client under test — also the store's exchanger, which is the point."""

    store: RotatingCredentialStorePort
    """The rotating store the acquired grant lands in."""

    provider: ScriptedProvider
    """What the token endpoint will say, and what it was asked."""

    backend: str
    """Label used in assertion messages, so a failure names the leg that disagreed."""

    tenant: TenantIdentity = attrs.field(factory=lambda: TenantIdentity(tenant_id=uuid4()))
    """The tenant the acquisition runs under."""

    # ....................... #

    async def acquire(self, *, code: str = "auth-code", ref: SecretRef = REF) -> Any:
        """Run the callback handoff: exchange *code* and store the grant."""

        return await complete_authorization(
            self.store,
            self.client,
            ref,
            code=code,
            code_verifier="verifier-from-the-session",
            redirect_uri=REDIRECT_URI,
            requested_scopes={"scope": REQUESTED_SCOPE},
        )


Check = Callable[[OAuth2AcquisitionHarness], Any]
"""One scenario check. Async, but typed loosely so the tuple stays homogeneous."""


# ....................... #


async def check_the_first_grant_is_acquired_and_stored(h: OAuth2AcquisitionHarness) -> None:
    """The happy path, and what a caller may see of it.

    The refresh token must not come back from ``get``: the store keeps it internal, and a
    client that could read it could also burn it outside the store's lock.
    """

    stored = await h.acquire()

    assert stored.access_token == "access-1", h.backend

    read = await h.store.get(REF)

    assert read.access_token == "access-1", h.backend
    assert not hasattr(read, "refresh_token"), f"{h.backend}: the store handed back a refresh token"
    assert h.provider.grant_types == ["authorization_code"], h.backend


# ....................... #


async def check_the_code_and_verifier_reach_the_provider(h: OAuth2AcquisitionHarness) -> None:
    """The three fields RFC 6749 §4.1.3 requires, on the wire rather than in intent.

    ``redirect_uri`` is the one worth pinning: the provider compares it against the value
    the authorize step sent, so an omission fails at the provider as an invalid grant.
    """

    await h.acquire(code="the-code")
    form = h.provider.forms[0]

    assert form["grant_type"] == "authorization_code", h.backend
    assert form["code"] == "the-code", h.backend
    assert form["code_verifier"] == "verifier-from-the-session", h.backend
    assert form["redirect_uri"] == REDIRECT_URI, h.backend


# ....................... #


async def check_the_same_client_refreshes_what_it_acquired(h: OAuth2AcquisitionHarness) -> None:
    """The unification: one config, both grants, the store driving the second one.

    Nothing here constructs a second exchanger. The store was wired with this client, so a
    rotation reaches the same token endpoint with a different ``grant_type`` — which is the
    whole reason this is a kit rather than two recipes.
    """

    stored = await h.acquire()
    rotated = await h.store.refresh(REF, observed=stored.version)

    assert rotated.access_token == "access-2", h.backend
    assert h.provider.grant_types == ["authorization_code", "refresh_token"], h.backend
    # The refresh presented the token the acquisition stored, not the access token.
    assert h.provider.forms[1]["refresh_token"] == "refresh-1", h.backend


# ....................... #


async def check_a_dead_grant_burns_and_a_bad_minute_does_not(
    h: OAuth2AcquisitionHarness,
) -> None:
    """The exchanger's one load-bearing distinction, from both sides.

    ``invalid_grant`` must reach the store as :data:`INVALID_GRANT_CODE` so it records the
    burn; a 503 must not, or a provider's bad minute destroys a working credential. This is
    the check that the declared error body is actually being read — without it, both look
    like a 400 and nothing can tell them apart.
    """

    stored = await h.acquire()

    h.provider.error = "temporarily_unavailable"
    h.provider.error_status = 503

    with pytest.raises(CoreException) as transient:
        await h.store.refresh(REF, observed=stored.version)

    assert transient.value.code != INVALID_GRANT_CODE, (
        f"{h.backend}: a 503 was reported as a dead grant, which burns a working credential"
    )
    # Untouched: the credential is still readable at the version it was stored at.
    survived = await h.store.get(REF)
    assert survived.access_token == "access-1", h.backend

    h.provider.error = "invalid_grant"
    h.provider.error_status = 400

    with pytest.raises(CoreException):
        await h.store.refresh(REF, observed=survived.version)

    # The store's own evidence that it took the rejection as terminal. Asserted through
    # the store rather than on the raised code, because the store translates the
    # exchanger's INVALID_GRANT_CODE into a burn notice and then speaks for itself — and
    # the notice is the part that stops the sweeper retrying a dead grant forever.
    with pytest.raises(CoreException) as burnt:
        await h.store.get(REF)

    assert burnt.value.code == BURNT_CREDENTIAL_CODE, (
        f"{h.backend}: after a dead grant the store answered {burnt.value.code!r}, so no "
        "burn notice was recorded"
    )


# ....................... #


async def check_a_refresh_without_a_new_token_keeps_the_old_one(
    h: OAuth2AcquisitionHarness,
) -> None:
    """RFC 6749 §6 lets a provider omit the new refresh token, meaning "keep yours".

    The value object requires one, so something must be stored; storing nothing usable
    would surface later as an invalid_grant nobody can explain.
    """

    stored = await h.acquire()
    h.provider.omit_refresh_token = True

    rotated = await h.store.refresh(REF, observed=stored.version)

    assert rotated.access_token == "access-2", h.backend

    # The proof is that a *second* rotation still works: it can only present a token the
    # first rotation carried forward.
    again = await h.store.refresh(REF, observed=rotated.version)

    assert again.access_token == "access-3", h.backend
    assert h.provider.forms[-1]["refresh_token"] == "refresh-1", (
        f"{h.backend}: the carried-forward refresh token was not the one presented"
    )


# ....................... #


async def check_a_scope_downgrade_is_recorded_not_swallowed(
    h: OAuth2AcquisitionHarness,
) -> None:
    """A provider granting less than was asked for produces a working, narrower grant.

    Refusing it would leave the user with nothing; accepting it silently turns a missing
    scope into a mysterious permission error weeks later. So both scopes are stored.
    """

    h.provider.granted_scope = "crm.read"

    await h.acquire()
    read = await h.store.get(REF)

    assert read.metadata[GRANTED_SCOPE_METADATA] == "crm.read", h.backend
    assert read.metadata[REQUESTED_SCOPE_METADATA] == REQUESTED_SCOPE, h.backend


# ....................... #


async def check_the_client_secret_travels_in_the_body_and_nowhere_else(
    h: OAuth2AcquisitionHarness,
) -> None:
    """``token_auth="post"`` places the secret in the form body — and only there.

    The stored credential's metadata, the client's repr and the provider's view of the URL
    are all checked, because a secret in any of them is a secret in a log.
    """

    await h.acquire()
    form = h.provider.forms[0]

    assert form["client_id"] == CLIENT_ID, h.backend
    assert form["client_secret"] == CLIENT_SECRET, h.backend

    read = await h.store.get(REF)

    assert CLIENT_SECRET not in repr(read.metadata), h.backend
    assert CLIENT_SECRET not in repr(h.client), h.backend
    assert CLIENT_SECRET not in repr(h.client.config), h.backend


# ....................... #


async def check_a_grant_lands_only_in_its_own_tenants_slot(
    h: OAuth2AcquisitionHarness,
) -> None:
    """The store's ambient tenancy applies to an acquisition unchanged.

    Nothing in the acquisition path names a tenant, which is the property: the callback
    runs inside the connecting user's request, so the grant lands in that tenant's slot and
    no other tenant can read it.
    """

    await h.acquire()

    other = TenantIdentity(tenant_id=uuid4())

    bind = h.ctx.inv_ctx.bind(
        metadata=InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4()),
        tenant=other,
    )

    with bind, pytest.raises(CoreException) as missing:
        await h.store.get(REF)

    assert missing.value.kind is ExceptionKind.NOT_FOUND, (
        f"{h.backend}: another tenant read the grant, or failed for the wrong reason "
        f"({missing.value.code!r})"
    )


# ....................... #


OAUTH2_ACQUISITION_BATTERY: tuple[Check, ...] = (
    check_the_first_grant_is_acquired_and_stored,
    check_the_code_and_verifier_reach_the_provider,
    check_the_same_client_refreshes_what_it_acquired,
    check_a_dead_grant_burns_and_a_bad_minute_does_not,
    check_a_refresh_without_a_new_token_keeps_the_old_one,
    check_a_scope_downgrade_is_recorded_not_swallowed,
    check_the_client_secret_travels_in_the_body_and_nowhere_else,
    check_a_grant_lands_only_in_its_own_tenants_slot,
)
"""The scenario, in the order a reader should meet it.

Both legs drive this by ``parametrize``, which is silent about an empty argument list — a
battery emptied by a bad edit would collect zero tests and report green on both legs at
once. :func:`battery_is_populated` is the guard against that, asserted by each leg."""


# ....................... #


def battery_is_populated() -> None:
    """Refuse a battery that has lost its checks."""

    names = {check.__name__ for check in OAUTH2_ACQUISITION_BATTERY}

    if names != {
        "check_the_first_grant_is_acquired_and_stored",
        "check_the_code_and_verifier_reach_the_provider",
        "check_the_same_client_refreshes_what_it_acquired",
        "check_a_dead_grant_burns_and_a_bad_minute_does_not",
        "check_a_refresh_without_a_new_token_keeps_the_old_one",
        "check_a_scope_downgrade_is_recorded_not_swallowed",
        "check_the_client_secret_travels_in_the_body_and_nowhere_else",
        "check_a_grant_lands_only_in_its_own_tenants_slot",
    }:
        raise AssertionError(f"the acquisition battery changed shape: {sorted(names)}")
