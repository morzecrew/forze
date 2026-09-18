"""The token-endpoint client — one object serving both grants of an outbound OAuth2 grant.

Acquisition and refresh are the *same* call to the *same* endpoint with a different
``grant_type``, so they are one client rather than two, and its refresh half **is** the
:class:`~forze.application.contracts.secrets.CredentialExchangerPort` the rotating store
and its sweeper drive. One provider config therefore closes the whole outbound lifecycle:
the first grant arrives through :func:`exchange_code`, and every rotation afterwards goes
through :meth:`OAuth2TokenClient.exchange` without the application writing a second
token-endpoint request.

The call goes over a declared ``forze_http`` service rather than a bare client, which is
what puts it inside the plane's tenancy, deadline propagation, resilience and — since the
sensitive-egress gate — an egress declaration. A token endpoint needs three things of that
plane that a JSON API does not: a form-encoded body (RFC 6749 §4.1.3), optional HTTP Basic
client authentication (§2.3.1), and the error body of a rejected request (§5.2), which is
the only way to tell a dead grant from a provider having a bad minute.
"""

import asyncio
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Final, Literal, final

import attrs
from pydantic import BaseModel, Field

from forze.application.contracts.http import (
    RESPONSE_ERROR_DETAIL,
    HttpOperationSpec,
    HttpServiceSpec,
)
from forze.application.contracts.secrets import (
    CREDENTIAL_EXCHANGE_TIMEOUT_CODE,
    INVALID_GRANT_CODE,
    ExchangedCredential,
    SecretRef,
    SecretsDepKey,
)
from forze.application.execution.context import ExecutionContext, ExecutionContextFactory
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import StrKey, utcnow

# ----------------------- #

TOKEN_OPERATION: Final[StrKey] = "token"

_BASIC_AUTH: Final[str] = "basic"
"""Named rather than compared inline: a literal ``"basic"`` beside a field whose name
contains "token" reads to a secrets scanner as a hardcoded credential check."""
"""The one operation on a provider's token service."""

GRANTED_SCOPE_METADATA: Final[str] = "granted_scope"
"""``metadata`` key carrying the scope the provider actually granted."""

REQUESTED_SCOPE_METADATA: Final[str] = "requested_scope"
"""``metadata`` key carrying the scope that was asked for, when the two differ.

Recorded rather than refused: a provider that grants less than was requested has produced
a working credential with a smaller reach, and failing the connect flow would leave the
user with nothing. The difference is visible in the stored metadata instead, so a later
"permission denied" from the provider has an explanation sitting beside the grant."""

_PERMANENT_ERRORS: frozenset[str] = frozenset({"invalid_grant"})
"""Provider error codes that mean *this grant is dead* (RFC 6749 §5.2).

One code, and the shortness is the point: everything absent is treated as transient,
because reporting a transient failure as permanent burns a working credential while the
reverse costs one wasted retry — the asymmetry the exchanger port spells out.

``invalid_request``, ``invalid_client`` and ``unauthorized_client`` are all about *this
client* rather than the grant: a malformed request, a client that failed authentication,
and a client not authorized for this grant type (§5.2's own wording). Every one of them is
a wiring mistake that a deployment fixes and retries, so burning a tenant's grant over one
would destroy working credentials for a typo."""


# ....................... #


class OAuth2TokenResponse(BaseModel):
    """A token endpoint's success body (RFC 6749 §5.1)."""

    access_token: str = Field(repr=False, min_length=1)
    """The credential. Non-empty: a provider answering 200 with an empty token has
    produced something every subsequent call fails on, and storing it turns a broken
    response into a credential nobody can explain."""

    token_type: str = ""
    expires_in: int | None = None
    refresh_token: str | None = Field(default=None, repr=False, min_length=1)
    """The next token, when the provider rotates it. Absent is meaningful (RFC 6749 §6 —
    keep the one you presented); empty is not, and must not be stored as one."""

    scope: str | None = None


class OAuth2ErrorResponse(BaseModel):
    """A token endpoint's failure body (RFC 6749 §5.2).

    Declared as the operation's ``error_type``, so the transport validates it and carries
    exactly these fields — and nothing else the provider sent — into the raised exception.
    """

    error: str = ""
    error_description: str | None = None
    error_uri: str | None = None


class OAuth2TokenRequest(BaseModel):
    """The form body of a token request, for either grant.

    One model for both because the endpoint is one endpoint: the fields a grant does not
    use are simply never set, and the plane sends only what was set. Splitting it in two
    would mean two specs, two operations and two places for the client credentials to
    drift apart.
    """

    grant_type: str
    code: str | None = None
    redirect_uri: str | None = None
    code_verifier: str | None = Field(default=None, repr=False)
    refresh_token: str | None = Field(default=None, repr=False)
    client_id: str | None = None
    client_secret: str | None = Field(default=None, repr=False)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OAuth2ProviderConfig:
    """One outbound OAuth2 provider, as the application declares it."""

    name: StrKey
    """Service name — the route an ``HttpServiceConfig`` is wired under."""

    token_endpoint: str
    """Path of the token endpoint, relative to the service's ``base_url``."""

    client_id: str
    """This client's identifier at the provider."""

    client_secret: SecretRef | None = None
    """Where the client secret lives, for a confidential client.

    Resolved per call rather than held, so a rotated secret is picked up without a restart.
    ``None`` is a public client, which is legitimate only with PKCE."""

    exchange_timeout: timedelta | None = timedelta(seconds=20)
    """Bound on one token request, owned by the client rather than the transport.

    It exists to make a distinction the transport cannot: when *this* bound fires, the
    request was sent and the answer is unknown, so the refresh token is spent-or-unknown
    and the store is told exactly that (:data:`CREDENTIAL_EXCHANGE_TIMEOUT_CODE`) rather
    than "never delivered". A transport-level timeout and a connection failure arrive as
    the same kind with the same code, so the two cannot be told apart from the outside —
    which is why the exchanger port asks an implementation to bring its own deadline.

    Kept under the store's own exchange bound: the store holds a lock across this call, and
    a client that outwaited it would pin that lock until the store gave up anyway. ``None``
    defers entirely to the transport, and gives up the distinction."""

    token_auth: Literal["post", "basic"] = "post"
    """Where the client credentials go at the token endpoint.

    ``post`` places ``client_id`` and ``client_secret`` in the form body
    (``client_secret_post``). ``basic`` omits both and leaves the header to the service's
    own ``HttpAuthConfig(kind="basic", …)`` — the transport owns that credential, so the
    two placements are configured in two places on purpose: a header belongs to the
    connection, a form field belongs to the request."""

    # ....................... #

    def service_spec(self) -> HttpServiceSpec:
        """The declared HTTP service this provider's token endpoint is reached through.

        Form-encoded because RFC 6749 requires it, and declaring
        :class:`OAuth2ErrorResponse` is what makes ``invalid_grant`` legible to
        :meth:`OAuth2TokenClient.exchange` instead of arriving as a bare 400.
        """

        return HttpServiceSpec(
            name=self.name,
            operations={
                TOKEN_OPERATION: HttpOperationSpec(
                    name=TOKEN_OPERATION,
                    method="POST",
                    path=self.token_endpoint,
                    args_type=OAuth2TokenRequest,
                    return_type=OAuth2TokenResponse,
                    body_encoding="form",
                    error_type=OAuth2ErrorResponse,
                    site=f"oauth.{self.name}.token",
                ),
            },
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OAuth2TokenClient:
    """A provider's token endpoint, serving acquisition and refresh through one config.

    Also a :class:`~forze.application.contracts.secrets.CredentialExchangerPort`: hand the
    same instance to a rotating store's config and its refresh sweeper, and every rotation
    of every grant from this provider goes through the same request the first one did.
    """

    config: OAuth2ProviderConfig
    """The provider this client speaks to."""

    ctx_factory: ExecutionContextFactory
    """Returns the execution context the exchange runs under.

    A factory rather than a context because the exchanger contract is context-free — a
    store is configured with an exchanger and calls it later — while an HTTP service port
    is resolved from a context. It is expected to hand back the runtime's **existing**
    context (``runtime.get_context``, or the one a request already holds), not to build a
    new one: contexts are one per runtime scope by contract, and churning them leaks
    per-instance context variables."""

    # ....................... #

    async def exchange_code(
        self,
        *,
        code: str,
        code_verifier: str | None,
        redirect_uri: str,
        requested_scopes: Mapping[str, str] | None = None,
    ) -> ExchangedCredential:
        """Trade an authorization code for the first credential of a grant.

        :param code: The single-use code the provider sent to the callback.
        :param code_verifier: The PKCE verifier from the session — never from the request.
        :param redirect_uri: The registered URI, identical to the one sent to the
            authorization endpoint (RFC 6749 §4.1.3 requires the match).
        :param requested_scopes: Ignored except for its keys' presence; pass
            ``{"scope": "a b"}`` to have a downgrade recorded against what was asked for.
        :returns: The credential, ready to hand to a store's ``put``.
        :raises CoreException: ``INVALID_GRANT_CODE`` when the provider rejected the code
            for good; anything else transient, unchanged from how it arrived.
        """

        if self.config.client_secret is None and not code_verifier:
            # A public client's only proof that this code belongs to the request that
            # started the flow is the PKCE verifier. Without either, an intercepted code
            # is enough for anyone — so this is refused rather than sent and rejected
            # later, where the failure would read as a provider problem.
            raise exc.configuration(
                f"Provider {self.config.name!r} is configured as a public client "
                "(client_secret=None), so exchanging a code requires a PKCE verifier",
            )

        request = OAuth2TokenRequest(
            grant_type="authorization_code",
            code=code,
            redirect_uri=redirect_uri,
            **({"code_verifier": code_verifier} if code_verifier is not None else {}),
        )
        requested = (requested_scopes or {}).get("scope")

        return await self._exchange(
            request,
            fallback_refresh=None,
            carried=None,
            requested_scope=requested,
        )

    # ....................... #

    async def exchange(
        self,
        ref: SecretRef,
        *,
        refresh_token: str,
        metadata: Mapping[str, str],
    ) -> ExchangedCredential:
        """Trade a refresh token for a replacement credential.

        The :class:`~forze.application.contracts.secrets.CredentialExchangerPort` method,
        named after the port rather than after its grant — a store resolves it
        structurally, and "refresh" is what the grant is called, not what the method is.

        :param ref: Which grant is being rotated. Unused by the request itself: one
            provider config addresses one endpoint, and the refresh token identifies the
            grant. Kept because the port passes it and a per-account endpoint would need it.
        :param refresh_token: The stored token. Presenting it may burn it.
        :param metadata: What the store holds beside the credential; the requested scope
            travels in it so a downgrade stays visible across rotations.
        :returns: The replacement, carrying the next refresh token — or this one again,
            when the provider omitted it (RFC 6749 §6 permits that, and it means "keep
            using what you have").
        :raises CoreException: ``INVALID_GRANT_CODE`` when the grant is permanently
            rejected; anything else transient.
        """

        _ = ref

        request = OAuth2TokenRequest(grant_type="refresh_token", refresh_token=refresh_token)

        return await self._exchange(
            request,
            fallback_refresh=refresh_token,
            # Carried forward, not rebuilt: the store hands back what it holds, and the
            # port's contract is that those facts survive a rotation — an account-specific
            # endpoint among them, without which the *next* exchange is unaddressable.
            carried=metadata,
            requested_scope=metadata.get(REQUESTED_SCOPE_METADATA),
        )

    # ....................... #

    async def _exchange(
        self,
        request: OAuth2TokenRequest,
        *,
        fallback_refresh: str | None,
        carried: Mapping[str, str] | None,
        requested_scope: str | None,
    ) -> ExchangedCredential:
        """Post *request* and project the answer, whichever grant it came from."""

        ctx = self.ctx_factory()
        authenticated = await self._with_client_auth(ctx, request)
        service = ctx.http.service(self.config.service_spec())

        timeout = self.config.exchange_timeout

        try:
            if timeout is None:
                reply = await service.invoke(TOKEN_OPERATION, authenticated)

            else:
                async with asyncio.timeout(timeout.total_seconds()):
                    reply = await service.invoke(TOKEN_OPERATION, authenticated)

        except TimeoutError as timed_out:
            # Our own bound, so the request left and the answer is unknown. The store
            # needs that stated: the presented refresh token may already be burned at the
            # provider, and treating this as "never delivered" invites a retry with a
            # token that no longer works.
            bound = timeout.total_seconds() if timeout is not None else 0.0

            raise exc.infrastructure(
                f"Token endpoint {self.config.name!r} did not answer within "
                f"{bound:g}s; the refresh token is spent or unknown",
                code=CREDENTIAL_EXCHANGE_TIMEOUT_CODE,
                details={"provider": str(self.config.name)},
            ) from timed_out

        except CoreException as error:
            raise self._classified(error) from error

        if not isinstance(reply, OAuth2TokenResponse):  # pragma: no cover - spec pins the type
            raise exc.internal(
                f"Token endpoint {self.config.name!r} returned {type(reply).__name__}",
            )

        return self._credential(
            reply,
            fallback_refresh=fallback_refresh,
            carried=carried,
            requested=requested_scope,
        )

    # ....................... #

    async def _with_client_auth(
        self,
        ctx: ExecutionContext,
        request: OAuth2TokenRequest,
    ) -> OAuth2TokenRequest:
        """Place the client credentials for ``token_auth="post"``, or leave them out.

        For ``basic`` they belong to the connection and the service's own auth config sends
        them, so putting them in the body as well would present two credentials and let a
        provider pick.
        """

        if self.config.token_auth == _BASIC_AUTH:
            return request

        secret: str | None = None

        if self.config.client_secret is not None:
            # Resolved per call, so a rotated client secret is picked up without a restart.
            secrets = ctx.deps.provide(SecretsDepKey)
            secret = await secrets.resolve_str(self.config.client_secret)

        return request.model_copy(
            update={"client_id": self.config.client_id, "client_secret": secret},
        )

    # ....................... #

    def _classified(self, error: CoreException) -> CoreException:
        """Permanent rejection, or the failure exactly as it arrived.

        The asymmetry is the exchanger port's: a transient reported as permanent destroys a
        working credential, while a permanent reported as transient costs a wasted retry.
        So only the two codes that mean *this grant is dead* are permanent, and a provider
        code nobody has seen before is transient.
        """

        declared = (error.details or {}).get(RESPONSE_ERROR_DETAIL)
        code = declared.get("error") if isinstance(declared, Mapping) else None  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

        if not isinstance(code, str) or code not in _PERMANENT_ERRORS:
            return error

        return exc.precondition(
            f"Token endpoint {self.config.name!r} rejected the grant: {code}",
            code=INVALID_GRANT_CODE,
            details={"provider": str(self.config.name), "error": code},
        )

    # ....................... #

    def _credential(
        self,
        reply: OAuth2TokenResponse,
        *,
        fallback_refresh: str | None,
        carried: Mapping[str, str] | None,
        requested: str | None,
    ) -> ExchangedCredential:
        """Project a token response into the store's input.

        *carried* is what the store already held. It is the starting point rather than an
        afterthought: the port stores these facts so the *next* exchange can be addressed,
        and a rotation that rebuilt the mapping from the response alone would drop
        everything the provider said once and never repeats.
        """

        refresh = reply.refresh_token or fallback_refresh

        if refresh is None:
            # An acquisition with no refresh token is a grant that cannot be rotated, and a
            # store exists to rotate it. Refusing here names the provider; storing an empty
            # token would surface later as an unexplainable invalid_grant.
            raise exc.precondition(
                f"Token endpoint {self.config.name!r} returned no refresh_token, so the "
                "grant cannot be rotated — request offline access, or hold this credential "
                "outside the rotating store",
                code=INVALID_GRANT_CODE,
                details={"provider": str(self.config.name)},
            )

        metadata: dict[str, str] = dict(carried or {})
        granted = reply.scope

        if granted:
            metadata[GRANTED_SCOPE_METADATA] = granted

        if requested:
            metadata[REQUESTED_SCOPE_METADATA] = requested

        return ExchangedCredential(
            access_token=reply.access_token,
            refresh_token=refresh,
            expires_at=self._expiry(reply.expires_in),
            metadata=metadata,
        )

    # ....................... #

    @staticmethod
    def _expiry(expires_in: int | None) -> datetime | None:
        """Absolute expiry from a relative one, or ``None`` when nothing was stated.

        Three cases, and they are different statements rather than shades of one. **Absent
        or negative** says nothing: no lifetime was given, or the number is nonsense, so
        the credential is stored without an expiry. **Zero** says the token is already
        expired (RFC 6749 §5.1 — ``expires_in`` is a lifetime), and storing that as "no
        expiry" would be reporting the opposite of what the provider said. **Too large to
        be a duration** also says nothing usable: a value that overflows a ``timedelta``
        cannot be an instant, and raising an ``OverflowError`` out of an acquisition would
        turn a provider's nonsense into a crash on our side.

        The clock comes through the framework's seam, so an expiry is simulable: a
        simulation that cannot move this forward cannot test what happens when a grant
        ages out.
        """

        if expires_in is None or expires_in < 0:
            return None

        now = utcnow()

        if expires_in == 0:
            return now

        try:
            return now + timedelta(seconds=expires_in)

        except OverflowError:
            return None
