"""Configurable authn dependency factories resolving document ports from execution context."""

from typing import cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.authn import (
    ApiKeyLifecyclePort,
    ApiKeyStatesPort,
    ApiKeyVerifierDepKey,
    ApiKeyVerifierPort,
    AuthnEventEmitter,
    AuthnEventSink,
    AuthnPort,
    AuthnSpec,
    PasswordAccountProvisioningPort,
    PasswordLifecyclePort,
    PasswordResetPort,
    PasswordVerifierDepKey,
    PasswordVerifierPort,
    PrincipalDeactivationPort,
    PrincipalEligibilityDepKey,
    PrincipalEligibilityPort,
    PrincipalEpochDepKey,
    PrincipalResolverDepKey,
    PrincipalResolverPort,
    RevocationStatesDepKey,
    RevocationStatesPort,
    RevocationStatesSpec,
    TokenLifecycleDepKey,
    TokenLifecyclePort,
    TokenVerifierDepKey,
    TokenVerifierPort,
    resolve_authn_event_emitter,
)
from forze.application.contracts.authz import AuthzSpec
from forze.application.contracts.counter import CounterSpec
from forze.application.contracts.deps import backend_of, implements
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentCommandHookDepKey,
    DocumentCommandPort,
    DocumentQueryDepKey,
    DocumentQueryPort,
    DocumentSpec,
)
from forze.application.execution import ExecutionContext
from forze.application.integrations.authn import (
    LOCKOUT_COUNTER_ROUTE,
    LockoutConfig,
    LoggingAuthnEventSink,
    LoginLockoutGuard,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze.domain.models import BaseDTO, Document
from forze_identity.authz.application import policy_principal_spec

from ...adapters import (
    ApiKeyLifecycleAdapter,
    PasswordAccountProvisioningAdapter,
    PasswordLifecycleAdapter,
    PasswordResetAdapter,
    TokenLifecycleAdapter,
)
from ...adapters.credential_deactivation import AuthnCredentialDeactivationHelper
from ...adapters.principal_deactivation import PrincipalDeactivationAdapter
from ...adapters.principal_eligibility import (
    AdmittedTokenEligibilityAdapter,
    AllowAllPrincipalEligibilityAdapter,
    PolicyPrincipalEligibilityAdapter,
)
from ...application.specs import (
    api_key_account_spec,
    identity_mapping_spec,
    password_account_spec,
    password_invite_spec,
    password_reset_spec,
    session_spec,
)
from ...orchestrator import AuthnOrchestrator
from ...resolvers import (
    DeterministicUuidResolver,
    JwtNativeUuidResolver,
    MappingTableResolver,
)
from ...services import (
    ApiKeyRevocation,
    PolicyPrincipalEpochHook,
    RevocationStates,
    SessionRevocation,
)
from ...verifiers import (
    Argon2PasswordVerifier,
    ForzeJwtTokenVerifier,
    HmacApiKeyVerifier,
)
from .configs import AuthnSharedServices

# ----------------------- #


def _resolve_eligibility(
    ctx: ExecutionContext,
    spec: AuthnSpec,
) -> PrincipalEligibilityPort:
    return ctx.deps.provide(PrincipalEligibilityDepKey, route=spec.name)(ctx, spec)


# ....................... #


def _session_revocation(
    ctx: ExecutionContext,
    shared: AuthnSharedServices | None,
) -> SessionRevocation | None:
    """The fast path's reads and writes for this request, or ``None`` when not configured."""

    config = shared.revocation_states if shared is not None else None

    if config is None:
        return None

    return SessionRevocation(
        config=config,
        states=_revocation_states(ctx, config),
        epochs=ctx.deps.provide(PrincipalEpochDepKey)(ctx),
        sessions=ctx.doc.query(session_spec),
        in_transaction=lambda: ctx.tx_ctx.depth() > 0,
        defer=ctx.tx_ctx.run_or_defer,
    )


def _api_key_revocation(
    ctx: ExecutionContext,
    shared: AuthnSharedServices | None,
) -> ApiKeyRevocation | None:
    """The API-key fast path's reads and writes, or ``None`` without ``key_mac``."""

    config = shared.revocation_states if shared is not None else None

    if config is None or config.key_mac is None:
        return None

    states = _revocation_states(ctx, config)

    if not implements(states, ApiKeyStatesPort):
        raise exc.configuration(
            f"RevocationStates.key_mac verifies API keys from cached states, but the states "
            f"store on route {config.route!r} does not implement ApiKeyStatesPort.",
            code="authn_api_key_states_unsupported",
        )

    return ApiKeyRevocation(
        config=config,
        states=cast(ApiKeyStatesPort, states),
        accounts=ctx.doc.query(api_key_account_spec),
        in_transaction=lambda: ctx.tx_ctx.depth() > 0,
        defer=ctx.tx_ctx.run_or_defer,
    )


def _caller_key_revocation(
    ctx: ExecutionContext,
    shared: AuthnSharedServices | None,
) -> ApiKeyRevocation | None:
    """The re-check a mint runs on its caller's API key, or ``None`` when no key can be admitted
    from a state here: without ``key_mac``, or without an API-key store to read.

    It only reads the caller's key row, so the store's query port is enough; without
    ``key_mac`` the store is never probed.
    """

    config = shared.revocation_states if shared is not None else None

    if config is None or config.key_mac is None:
        return None

    if not ctx.deps.resolvable(DocumentQueryDepKey, route=api_key_account_spec.name):
        return None

    return _api_key_revocation(ctx, shared)


def _revocation_states(ctx: ExecutionContext, config: RevocationStates) -> RevocationStatesPort:
    return ctx.deps.resolve_configurable(
        ctx,
        RevocationStatesDepKey,
        RevocationStatesSpec(name=config.route),
        route=config.route,
    )


# ....................... #


def _resolve_events(
    ctx: ExecutionContext,
    spec: AuthnSpec,
) -> AuthnEventEmitter | None:
    """Resolve the route's optional event emitter (``None`` = emission off)."""

    return resolve_authn_event_emitter(ctx, spec)


# ----------------------- #
# Verifier factories


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableArgon2PasswordVerifier:
    """Build :class:`Argon2PasswordVerifier` from the shared password service + ``ctx`` document port."""

    shared: AuthnSharedServices

    rehash_on_login: bool = attrs.field(default=False)
    """When ``True``, wire the password account command port so stored hashes are
    upgraded to current Argon2 parameters after a successful login."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PasswordVerifierPort:
        _ = spec

        if self.shared.password_svc is None:
            raise exc.internal(
                "Password verifier requires kernel.password",
            )

        pa_cmd = ctx.doc.command(password_account_spec) if self.rehash_on_login else None

        return Argon2PasswordVerifier(
            password_svc=self.shared.password_svc,
            pa_qry=ctx.doc.query(password_account_spec),
            pa_cmd=pa_cmd,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableForzeJwtTokenVerifier:
    """Build :class:`ForzeJwtTokenVerifier` from the shared access-token service.

    Wires :attr:`ForzeJwtTokenVerifier.session_qry` so lifecycle-issued access JWTs
    (``sid`` claim) are invalidated on logout and refresh rotation.
    """

    shared: AuthnSharedServices

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> TokenVerifierPort:
        _ = spec

        if self.shared.access_svc is None:
            raise exc.internal(
                "Forze JWT token verifier requires kernel.access_token_secret",
            )

        return ForzeJwtTokenVerifier(
            access_svc=self.shared.access_svc,
            session_qry=ctx.doc.query(session_spec),
            revocation=_session_revocation(ctx, self.shared),
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableHmacApiKeyVerifier:
    """Build :class:`HmacApiKeyVerifier` from the shared API key service + ``ctx`` document port."""

    shared: AuthnSharedServices

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> ApiKeyVerifierPort:
        _ = spec

        if self.shared.api_key_svc is None:
            raise exc.internal(
                "API key verifier requires kernel.api_key_pepper",
            )

        return HmacApiKeyVerifier(
            api_key_svc=self.shared.api_key_svc,
            ak_qry=ctx.doc.query(api_key_account_spec),
            revocation=_api_key_revocation(ctx, self.shared),
        )


# ....................... #
# Resolver factories


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableJwtNativeUuidResolver:
    """Build :class:`JwtNativeUuidResolver` (no execution-context dependencies)."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PrincipalResolverPort:
        _ = ctx, spec
        return JwtNativeUuidResolver()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableDeterministicUuidResolver:
    """Build :class:`DeterministicUuidResolver` (no execution-context dependencies)."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PrincipalResolverPort:
        _ = ctx, spec
        return DeterministicUuidResolver()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMappingTableResolver:
    """Build :class:`MappingTableResolver` from the identity-mapping document ports."""

    provision_on_first_sight: bool = False
    """Whether the resolver mints new principal ids on unknown subjects."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PrincipalResolverPort:
        _ = spec

        cmd = ctx.doc.command(identity_mapping_spec) if self.provision_on_first_sight else None

        return MappingTableResolver(
            qry=ctx.doc.query(identity_mapping_spec),
            cmd=cmd,
            provision_on_first_sight=self.provision_on_first_sight,
        )


# ....................... #
# Orchestrator factory


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableAuthn:
    """Build :class:`AuthnOrchestrator` by composing per-method verifiers + resolver from deps."""

    enabled_methods: frozenset[str]
    """Snapshot of :attr:`AuthnSpec.enabled_methods`; pre-baked at registration time."""

    actor_claim: str | None = attrs.field(default=None)
    """Token claim carrying the RFC 8693 delegation actor; ``None`` ignores delegation."""

    lockout: LockoutConfig | None = attrs.field(default=None)
    """Optional fixed-window login lockout policy for the password path; ``None`` disables it."""

    lockout_counter_route: StrKey = attrs.field(default=LOCKOUT_COUNTER_ROUTE)
    """Counter route the lockout guard resolves its ``CounterPort`` from (plain
    registrations match any route, so a single app-wide counter backend suffices)."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> AuthnPort:
        # Re-validate against the live spec to guard against route name reuse with
        # mismatched method sets (the registration snapshot wins).
        if frozenset(spec.enabled_methods) != self.enabled_methods:
            raise exc.internal(
                "AuthnSpec.enabled_methods does not match the enabled_methods registered "
                f"for route '{spec.name}'",
            )

        password_v: PasswordVerifierPort | None = None
        token_v: TokenVerifierPort | None = None
        api_key_v: ApiKeyVerifierPort | None = None

        if "password" in self.enabled_methods:
            password_v = ctx.deps.provide(PasswordVerifierDepKey, route=spec.name)(ctx, spec)

        if "token" in self.enabled_methods:
            token_v = ctx.deps.provide(TokenVerifierDepKey, route=spec.name)(ctx, spec)

        if "api_key" in self.enabled_methods:
            api_key_v = ctx.deps.provide(ApiKeyVerifierDepKey, route=spec.name)(ctx, spec)

        resolver = ctx.deps.provide(PrincipalResolverDepKey, route=spec.name)(ctx, spec)
        eligibility = _resolve_eligibility(ctx, spec)

        guard: LoginLockoutGuard | None = None

        if self.lockout is not None:
            guard = LoginLockoutGuard(
                counter=ctx.counter(CounterSpec(name=self.lockout_counter_route)),
                config=self.lockout,
            )

        return AuthnOrchestrator(
            resolver=resolver,
            eligibility=eligibility,
            enabled_methods=self.enabled_methods,
            password_verifier=password_v,
            token_verifier=token_v,
            api_key_verifier=api_key_v,
            actor_claim=self.actor_claim,
            events=_resolve_events(ctx, spec),
            lockout=guard,
        )


# ....................... #


_SKIP_BACKEND = "forze_postgres"
"""The backend whose locking read of the policy principal excludes a concurrent deactivation,
so an issue serializes with it: the skip's precondition. The mock's locking read only claims
the row, so on the mock (as on Mongo) every verification reads."""


def _deactivations_raise_epochs(ctx: ExecutionContext, config: RevocationStates) -> bool:
    """Whether every write that may deactivate a policy principal raises its epoch here.

    :raises CoreException: ``configuration`` when the policy principal's hook carries another
        revocation configuration: its raises would be written where this verifier never reads.
    """

    route = policy_principal_spec.name

    if not ctx.deps.exists(DocumentCommandHookDepKey, route=route):
        return False

    hook = ctx.deps.provide(DocumentCommandHookDepKey, route=route)

    if not isinstance(hook, PolicyPrincipalEpochHook):
        return False

    if hook.config != config:
        raise exc.configuration(
            "The authn and authz kernels carry different revocation states. Pass the same "
            "RevocationStates to both: the verifier reads the epochs deactivations raise.",
            code="authn_revocation_states_mismatch",
        )

    return True


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePolicyPrincipalEligibility:
    """Build :class:`PolicyPrincipalEligibilityAdapter` from policy principal documents.

    With revocation states on both the authn and the authz kernel, signed under ``key_mac``,
    and the policy principal on Postgres, the read is skipped right after the fast path
    admitted a token of the same principal (:class:`AdmittedTokenEligibilityAdapter`).
    """

    shared: AuthnSharedServices | None = None
    """Shared services; their revocation states enable the skip."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PrincipalEligibilityPort:
        _ = spec
        principal_qry = ctx.doc.query(policy_principal_spec)
        adapter = PolicyPrincipalEligibilityAdapter(
            principal_qry=principal_qry,
            in_transaction=lambda: ctx.tx_ctx.depth() > 0,
        )
        config = self.shared.revocation_states if self.shared is not None else None

        if (
            config is None
            or not _deactivations_raise_epochs(ctx, config)
            # Unsigned, a cached epoch can be restored by whoever writes the states store. The
            # authz kernel refuses that configuration; a hook registered by hand reaches here.
            or config.key_mac is None
            or type(backend_of(principal_qry)).__module__.split(".")[0] != _SKIP_BACKEND
        ):
            return adapter

        return AdmittedTokenEligibilityAdapter(inner=adapter)


# ....................... #


class ConfigurableAllowAllEligibility:
    """Build :class:`AllowAllPrincipalEligibilityAdapter` — no documents required."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PrincipalEligibilityPort:
        _ = ctx, spec
        return AllowAllPrincipalEligibilityAdapter()


# ....................... #


def _credential_store[R: BaseModel, D: Document, C: BaseDTO, U: BaseDTO](
    ctx: ExecutionContext,
    spec: DocumentSpec[R, D, C, U],
) -> tuple[DocumentQueryPort[R] | None, DocumentCommandPort[R, D, C, U] | None]:
    """The ports of a credential store the application wires, or ``(None, None)``.

    Read from the composed dependencies, not from one module's routes: an application may
    compose several authn modules, or serve a credential family from a store of its own, and a
    key any of them issues must be revoked. A store is wired when the document plane would
    resolve it: a route under the spec's name or the plain fallback, which a declared spec
    inventory also admits.

    :raises CoreException: ``configuration`` for a store whose query port is wired and command
        port is not (or the reverse): deactivation could see its credentials but not close them.
    """

    wired = [
        ctx.deps.resolvable(key, route=spec.name)
        for key in (DocumentQueryDepKey, DocumentCommandDepKey)
    ]

    if not any(wired):
        return None, None

    if not all(wired):
        raise exc.configuration(
            f"Credential store {spec.name!r} has a document query port but no command port, or "
            "the reverse; principal deactivation needs both to close its credentials.",
        )

    return ctx.doc.query(spec), ctx.doc.command(spec)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePrincipalDeactivation:
    """Build :class:`PrincipalDeactivationAdapter` for cascaded offboarding."""

    authz_route: StrKey
    """Authz route name used to resolve :class:`PrincipalRegistryPort`."""

    shared: AuthnSharedServices | None = None
    """Shared services; their revocation states make deactivation refuse to run outside a
    transaction."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PrincipalDeactivationPort:
        registry = ctx.authz.principal_registry(AuthzSpec(name=self.authz_route))
        token_lifecycle = ctx.deps.provide(TokenLifecycleDepKey, route=spec.name)(
            ctx,
            spec,
        )
        pa_qry, pa_cmd = _credential_store(ctx, password_account_spec)
        ak_qry, ak_cmd = _credential_store(ctx, api_key_account_spec)
        credentials = AuthnCredentialDeactivationHelper(
            pa_qry=pa_qry,
            pa_cmd=pa_cmd,
            ak_qry=ak_qry,
            ak_cmd=ak_cmd,
            key_revocation=_api_key_revocation(ctx, self.shared) if ak_qry is not None else None,
        )
        revocation = _session_revocation(ctx, self.shared)
        stores = (
            (pa_cmd, ak_cmd, ctx.doc.command(policy_principal_spec), ctx.doc.command(session_spec))
            if revocation is not None
            else ()
        )

        return PrincipalDeactivationAdapter(
            principal_registry=registry,
            token_lifecycle=token_lifecycle,
            credentials=credentials,
            events=_resolve_events(ctx, spec),
            revocation=revocation,
            stores=tuple(store for store in stores if store is not None),
        )


# ....................... #
# Lifecycle / provisioning factories (unchanged shapes)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableTokenLifecycle:
    """Build :class:`TokenLifecycleAdapter`."""

    shared: AuthnSharedServices

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> TokenLifecyclePort:
        _ = spec

        if self.shared.access_svc is None or self.shared.refresh_svc is None:
            raise exc.internal(
                "Token lifecycle requires kernel.access_token_secret and kernel.refresh_token_pepper",
            )

        return TokenLifecycleAdapter(
            access_svc=self.shared.access_svc,
            refresh_svc=self.shared.refresh_svc,
            session_qry=ctx.doc.query(session_spec),
            session_cmd=ctx.doc.command(session_spec),
            eligibility=_resolve_eligibility(ctx, spec),
            events=_resolve_events(ctx, spec),
            revocation=_session_revocation(ctx, self.shared),
            key_revocation=_caller_key_revocation(ctx, self.shared),
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePasswordLifecycle:
    """Build :class:`PasswordLifecycleAdapter`."""

    shared: AuthnSharedServices

    revoke_sessions_on_password_change: bool = attrs.field(default=True)
    """Whether a successful password change revokes all of the principal's sessions
    ("log out everywhere"); wires the session document ports when enabled."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PasswordLifecyclePort:
        _ = spec

        if self.shared.password_svc is None:
            raise exc.internal("Password lifecycle requires kernel.password")

        revoke = self.revoke_sessions_on_password_change

        return PasswordLifecycleAdapter(
            password_svc=self.shared.password_svc,
            pa_qry=ctx.doc.query(password_account_spec),
            pa_cmd=ctx.doc.command(password_account_spec),
            eligibility=_resolve_eligibility(ctx, spec),
            session_qry=ctx.doc.query(session_spec) if revoke else None,
            session_cmd=ctx.doc.command(session_spec) if revoke else None,
            revoke_sessions_on_password_change=revoke,
            events=_resolve_events(ctx, spec),
            revocation=_session_revocation(ctx, self.shared) if revoke else None,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePasswordReset:
    """Build :class:`PasswordResetAdapter`."""

    shared: AuthnSharedServices

    revoke_sessions_on_reset: bool = attrs.field(default=True)
    """Whether a successful reset revokes all of the principal's sessions
    ("log out everywhere"); wires the session document ports when enabled."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PasswordResetPort:
        _ = spec

        if self.shared.password_svc is None:
            raise exc.internal("Password reset requires kernel.password")

        if self.shared.reset_svc is None:
            raise exc.internal("Password reset requires kernel.reset_token_pepper")

        revoke = self.revoke_sessions_on_reset

        return PasswordResetAdapter(
            password_svc=self.shared.password_svc,
            reset_svc=self.shared.reset_svc,
            pa_qry=ctx.doc.query(password_account_spec),
            pa_cmd=ctx.doc.command(password_account_spec),
            reset_qry=ctx.doc.query(password_reset_spec),
            reset_cmd=ctx.doc.command(password_reset_spec),
            eligibility=_resolve_eligibility(ctx, spec),
            session_qry=ctx.doc.query(session_spec) if revoke else None,
            session_cmd=ctx.doc.command(session_spec) if revoke else None,
            revoke_sessions_on_reset=revoke,
            events=_resolve_events(ctx, spec),
            revocation=_session_revocation(ctx, self.shared) if revoke else None,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableApiKeyLifecycle:
    """Build :class:`ApiKeyLifecycleAdapter`."""

    shared: AuthnSharedServices

    authz_route: StrKey | None = None
    """Authz route whose principal registry checks a delegation key's agent is a service
    principal; ``None`` leaves only the eligibility gate."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> ApiKeyLifecyclePort:
        if self.shared.api_key_svc is None:
            raise exc.internal("API key lifecycle requires kernel.api_key_pepper")

        return ApiKeyLifecycleAdapter(
            api_key_svc=self.shared.api_key_svc,
            ak_qry=ctx.doc.query(api_key_account_spec),
            ak_cmd=ctx.doc.command(api_key_account_spec),
            eligibility=_resolve_eligibility(ctx, spec),
            principal_registry=(
                ctx.authz.principal_registry(AuthzSpec(name=self.authz_route))
                if self.authz_route is not None
                else None
            ),
            events=_resolve_events(ctx, spec),
            caller=ctx.inv_ctx.get_authn,
            revocation=_session_revocation(ctx, self.shared),
            key_revocation=_api_key_revocation(ctx, self.shared),
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePasswordAccountProvisioning:
    """Build :class:`PasswordAccountProvisioningAdapter`."""

    shared: AuthnSharedServices

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PasswordAccountProvisioningPort:
        _ = spec

        if self.shared.password_svc is None:
            raise exc.internal("Password provisioning requires kernel.password")

        invite_svc = self.shared.invite_svc
        invite_qry = ctx.doc.query(password_invite_spec) if invite_svc is not None else None
        invite_cmd = ctx.doc.command(password_invite_spec) if invite_svc is not None else None

        return PasswordAccountProvisioningAdapter(
            password_svc=self.shared.password_svc,
            password_account_qry=ctx.doc.query(password_account_spec),
            password_account_cmd=ctx.doc.command(password_account_spec),
            eligibility=_resolve_eligibility(ctx, spec),
            invite_svc=invite_svc,
            invite_qry=invite_qry,
            invite_cmd=invite_cmd,
            revocation=_session_revocation(ctx, self.shared),
            key_revocation=_caller_key_revocation(ctx, self.shared),
        )


# ....................... #
# Event sink factory


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableLoggingAuthnEventSink:
    """Build the core :class:`LoggingAuthnEventSink` (no execution-context dependencies).

    Pass as ``AuthnDepsModule(events=ConfigurableLoggingAuthnEventSink())`` for
    one structured log line per authn event.
    """

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> AuthnEventSink:
        _ = ctx, spec
        return LoggingAuthnEventSink()
