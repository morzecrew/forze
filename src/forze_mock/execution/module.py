"""Dependency module registering all mock adapters."""

from __future__ import annotations

from typing import Any, Iterable, Literal, final

import attrs

from forze.application.contracts.analytics import (
    AnalyticsIngestDepKey,
    AnalyticsQueryDepKey,
    AnalyticsSpec,
)
from forze.application.contracts.authn import (
    ApiKeyLifecycleDepKey,
    ApiKeyVerifierDepKey,
    AuthnDepKey,
    AuthnEventSinkDepKey,
    AuthnSpec,
    PasswordAccountProvisioningDepKey,
    PasswordLifecycleDepKey,
    PasswordResetDepKey,
    PasswordVerifierDepKey,
    PrincipalDeactivationDepKey,
    PrincipalEligibilityDepKey,
    PrincipalResolverDepKey,
    TokenLifecycleDepKey,
    TokenVerifierDepKey,
    resolve_authn_event_emitter,
)
from forze.application.contracts.authz import (
    AuthzDecisionDepKey,
    AuthzScopeDepKey,
    DelegationDepKey,
    DelegationGrantDepKey,
    GrantQueryDepKey,
    PrincipalRegistryDepKey,
    RoleAssignmentDepKey,
)
from forze.application.contracts.cache import CacheDepKey, CachePort, CacheSpec
from forze.application.contracts.counter import CounterDepKey, CounterPort, CounterSpec
from forze.application.contracts.deps import DepKey
from forze.application.contracts.dlock import (
    DistributedLockCommandDepKey,
    DistributedLockQueryDepKey,
    DistributedLockSpec,
)
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.domain import DomainEventDispatcherDepKey
from forze.application.contracts.durable.function import (
    DurableFunctionEventCommandDepKey,
    DurableFunctionEventSpec,
    DurableFunctionStepDepKey,
)
from forze.application.contracts.durable.workflow import (
    DurableWorkflowCommandDepKey,
    DurableWorkflowQueryDepKey,
    DurableWorkflowScheduleCommandDepKey,
    DurableWorkflowScheduleQueryDepKey,
    DurableWorkflowSpec,
)
from forze.application.contracts.embeddings import (
    EmbeddingsProviderDepKey,
    EmbeddingsSpec,
)
from forze.application.contracts.graph import (
    GraphCommandDepKey,
    GraphModuleSpec,
    GraphQueryDepKey,
    GraphRawQueryDepKey,
)
from forze.application.contracts.idempotency import (
    IdempotencyDepKey,
    IdempotencyPort,
    IdempotencySpec,
)
from forze.application.contracts.inbox import InboxDepKey, InboxPort, InboxSpec
from forze.application.contracts.outbox import (
    OutboxCommandDepKey,
    OutboxQueryDepKey,
    OutboxSpec,
)
from forze.application.contracts.pubsub import (
    PubSubCommandDepKey,
    PubSubQueryDepKey,
    PubSubSpec,
)
from forze.application.contracts.queue import (
    QueueCommandDepKey,
    QueueQueryDepKey,
    QueueSpec,
)
from forze.application.contracts.resilience import ResilienceExecutorDepKey
from forze.application.contracts.search import (
    FederatedSearchQueryDepKey,
    FederatedSearchSpec,
    HubSearchQueryDepKey,
    HubSearchSpec,
    SearchCommandDepKey,
    SearchQueryDepKey,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.contracts.secrets import SecretsDepKey
from forze.application.contracts.storage import (
    StorageCommandDepKey,
    StorageCommandPort,
    StorageQueryDepKey,
    StorageQueryPort,
    StorageSpec,
)
from forze.application.contracts.stream import (
    StreamCommandDepKey,
    StreamGroupQueryDepKey,
    StreamQueryDepKey,
)
from forze.application.contracts.stream.specs import StreamSpec
from forze.application.contracts.tenancy import (
    TenantManagementDepKey,
    TenantProviderPort,
    TenantResolverDepKey,
)
from forze.application.contracts.transaction import (
    TransactionManagerDepKey,
    TransactionManagerPort,
)
from forze.application.execution import (
    Deps,
    DepsModule,
    DomainEventRegistry,
    ExecutionContext,
    InProcessDomainEventDispatcher,
    InProcessResilienceExecutor,
    builtin_default_policies,
)
from forze.application.execution.domain import domain_dispatcher_provider
from forze.application.execution.outbox import build_staging_outbox_command_for_store
from forze.application.integrations.authn import (
    LOCKOUT_COUNTER_ROUTE,
    AuthnOrchestrator,
    LockoutConfig,
    LoginLockoutGuard,
)
from forze.application.integrations.outbox import StagingOutboxCommand
from forze.application.integrations.search import SearchResultSnapshot
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKey, StrKeyMapping
from forze_mock.adapters import (
    MockAnalyticsAdapter,
    MockCacheAdapter,
    MockCounterAdapter,
    MockDistributedLockAdapter,
    MockDocumentAdapter,
    MockDurableFunctionEventAdapter,
    MockDurableFunctionStepAdapter,
    MockDurableWorkflowCommandAdapter,
    MockDurableWorkflowQueryAdapter,
    MockDurableWorkflowScheduleCommandAdapter,
    MockDurableWorkflowScheduleQueryAdapter,
    MockFederatedSearchAdapter,
    MockGraphAdapter,
    MockHubSearchAdapter,
    MockIdempotencyAdapter,
    MockInboxAdapter,
    MockPubSubAdapter,
    MockQueueAdapter,
    MockSearchAdapter,
    MockSearchCommandAdapter,
    MockSearchResultSnapshotAdapter,
    MockState,
    MockStorageAdapter,
    MockStreamAdapter,
    MockStreamGroupAdapter,
    MockStrictTxManagerAdapter,
    MockTxManagerAdapter,
)
from forze_mock.adapters.events import RecordingAuthnEventSink
from forze_mock.adapters.identity import (
    MockApiKeyLifecyclePort,
    MockApiKeyVerifierPort,
    MockAuthzDecisionPort,
    MockAuthzScopePort,
    MockDelegationGrantPort,
    MockDelegationPort,
    MockGrantQueryPort,
    MockPasswordAccountProvisioningPort,
    MockPasswordLifecyclePort,
    MockPasswordResetPort,
    MockPasswordVerifierPort,
    MockPrincipalDeactivationPort,
    MockPrincipalEligibilityPort,
    MockPrincipalRegistryPort,
    MockPrincipalResolverPort,
    MockRoleAssignmentPort,
    MockSecretsPort,
    MockTenantManagementPort,
    MockTenantResolverPort,
    MockTokenLifecyclePort,
    MockTokenVerifierPort,
)
from forze_mock.embeddings import MockHashEmbeddingsProvider
from forze_mock.outbox_adapter import MockOutboxStore
from forze_mock.tenancy import MockRoutedStateRegistry, resolve_mock_namespace_sync
from forze_mock.tenancy.routed import MockRoutedStateDepKey

from ..resilience import PassthroughResilienceExecutor
from .configs import MockRouteConfig
from .keys import MockStateDepKey

# ----------------------- #

DocSpec = DocumentSpec[Any, Any, Any, Any]


def mock_txmanager(context: ExecutionContext) -> TransactionManagerPort:
    return MockTxManagerAdapter(state=context.deps.provide(MockStateDepKey))


def mock_strict_txmanager(context: ExecutionContext) -> TransactionManagerPort:
    return MockStrictTxManagerAdapter(state=context.deps.provide(MockStateDepKey))


def _tenant_provider(ctx: ExecutionContext) -> TenantProviderPort:
    return ctx.inv_ctx.get_tenant


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MockIdentityConfig:
    """Optional seed data for mock identity stubs."""

    authn_routes: frozenset[StrKey] = frozenset({"main"})
    authz_routes: frozenset[StrKey] = frozenset({"main"})
    tenancy_routes: frozenset[StrKey] = frozenset({"main"})


@attrs.define(slots=True, kw_only=True)
class _MockFactoryBase:
    module: "MockDepsModule"

    def _state(self, ctx: ExecutionContext) -> MockState:
        return ctx.deps.provide(MockStateDepKey)

    def _route(self, spec_name: StrKey) -> MockRouteConfig | None:
        routes = self.module.routes
        if routes is None:
            return None
        return routes.get(spec_name)

    def _namespace_for(
        self,
        ctx: ExecutionContext,
        spec_name: StrKey,
        *,
        default: str,
    ) -> str:
        cfg = self._route(spec_name)
        if cfg is None:
            return default

        tenant = ctx.inv_ctx.get_tenant()
        tenant_id = tenant.tenant_id if tenant is not None else None

        return resolve_mock_namespace_sync(
            default=default,
            relation=cfg.relation,
            namespace=cfg.namespace,
            tenant_id=tenant_id,
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockDocument(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: DocSpec,
    ) -> MockDocumentAdapter[Any, Any, Any, Any]:
        state = self._state(context)
        cfg = self._route(spec.name)
        domain_model = None
        if spec.write is not None:
            domain_model = spec.write["domain"]
        return MockDocumentAdapter[Any, Any, Any, Any](
            spec=spec,
            state=state,
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
            read_model=spec.read,
            domain_model=domain_model,
            dispatcher_provider=domain_dispatcher_provider(context),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockAnalytics(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: AnalyticsSpec[Any, Any],
    ) -> MockAnalyticsAdapter[Any, Any]:
        cfg = self._route(spec.name)
        return MockAnalyticsAdapter(
            state=self._state(context),
            spec=spec,
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockSearch(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: SearchSpec[Any],
    ) -> MockSearchAdapter[Any]:
        cfg = self._route(spec.name)
        snap = None
        if spec.snapshot is not None:
            snap_port = context.deps.provide(SearchResultSnapshotDepKey)(
                context, spec.snapshot
            )
            snap = SearchResultSnapshot(store=snap_port)
        return MockSearchAdapter(
            state=self._state(context),
            spec=spec,
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
            result_snapshot=snap,
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockSearchCommand(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: SearchSpec[Any],
    ) -> MockSearchCommandAdapter:
        cfg = self._route(spec.name)
        return MockSearchCommandAdapter(
            state=self._state(context),
            spec=spec,
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockSearchSnapshot(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: SearchResultSnapshotSpec,
    ) -> MockSearchResultSnapshotAdapter:
        cfg = self._route(spec.name)
        return MockSearchResultSnapshotAdapter(
            state=self._state(context),
            spec=spec,
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockHubSearch(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: HubSearchSpec[Any],
    ) -> MockHubSearchAdapter[Any]:
        legs: list[tuple[str, MockSearchAdapter[Any]]] = []
        search_factory = ConfigurableMockSearch(module=self.module)
        for member in spec.members:
            legs.append((member.name, search_factory(context, member)))
        snap = None
        if spec.snapshot is not None:
            snap_port = context.deps.provide(SearchResultSnapshotDepKey)(
                context, spec.snapshot
            )
            snap = SearchResultSnapshot(store=snap_port)
        return MockHubSearchAdapter(
            hub_spec=spec,
            legs=legs,
            result_snapshot=snap,
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockFederatedSearch(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: FederatedSearchSpec[Any],
    ) -> MockFederatedSearchAdapter[Any]:
        legs: list[tuple[str, MockSearchAdapter[Any]]] = []
        search_factory = ConfigurableMockSearch(module=self.module)
        for member in spec.members:
            if isinstance(member, HubSearchSpec):
                raise exc.configuration(
                    "Mock federated search supports SearchSpec legs only",
                )
            legs.append((member.name, search_factory(context, member)))
        snap = None
        if spec.snapshot is not None:
            snap_port = context.deps.provide(SearchResultSnapshotDepKey)(
                context, spec.snapshot
            )
            snap = SearchResultSnapshot(store=snap_port)
        return MockFederatedSearchAdapter(
            federated_spec=spec,
            legs=legs,
            result_snapshot=snap,
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockCounter(_MockFactoryBase):
    def __call__(self, context: ExecutionContext, spec: CounterSpec) -> CounterPort:
        cfg = self._route(spec.name)
        return MockCounterAdapter(
            state=self._state(context),
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockCache(_MockFactoryBase):
    def __call__(self, context: ExecutionContext, spec: CacheSpec) -> CachePort:
        cfg = self._route(spec.name)
        return MockCacheAdapter(
            state=self._state(context),
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockIdempotency(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: IdempotencySpec,
    ) -> IdempotencyPort:
        cfg = self._route(spec.name)
        return MockIdempotencyAdapter(
            state=self._state(context),
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
            ttl=spec.ttl,
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockInbox(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: InboxSpec,
    ) -> InboxPort:
        cfg = self._route(spec.name)
        return MockInboxAdapter(
            state=self._state(context),
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockStorageQuery(_MockFactoryBase):
    def __call__(
        self, context: ExecutionContext, spec: StorageSpec
    ) -> StorageQueryPort:
        cfg = self._route(spec.name)
        return MockStorageAdapter(
            state=self._state(context),
            bucket=self._namespace_for(context, spec.name, default=str(spec.name)),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockStorageCommand(_MockFactoryBase):
    def __call__(
        self, context: ExecutionContext, spec: StorageSpec
    ) -> StorageCommandPort:
        cfg = self._route(spec.name)
        return MockStorageAdapter(
            state=self._state(context),
            bucket=self._namespace_for(context, spec.name, default=str(spec.name)),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockGraph(_MockFactoryBase):
    def __call__(
        self, context: ExecutionContext, spec: GraphModuleSpec
    ) -> MockGraphAdapter:
        cfg = self._route(spec.name)
        return MockGraphAdapter(
            spec=spec,
            state=self._state(context),
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockQueue(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: QueueSpec[Any],
    ) -> MockQueueAdapter[Any]:
        cfg = self._route(spec.name)
        return MockQueueAdapter(
            state=self._state(context),
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
            codec=spec.codec,
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockPubSub(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: PubSubSpec[Any],
    ) -> MockPubSubAdapter[Any]:
        cfg = self._route(spec.name)
        return MockPubSubAdapter(
            state=self._state(context),
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
            codec=spec.codec,
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockStream(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: StreamSpec[Any],
    ) -> MockStreamAdapter[Any]:
        cfg = self._route(spec.name)
        return MockStreamAdapter(
            state=self._state(context),
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
            codec=spec.codec,
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockStreamGroup(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: StreamSpec[Any],
    ) -> MockStreamGroupAdapter[Any]:
        stream = ConfigurableMockStream(module=self.module)(context, spec)
        return MockStreamGroupAdapter(
            stream=stream,
            state=self._state(context),
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockOutboxQuery(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: OutboxSpec[Any],
    ) -> MockOutboxStore[Any]:
        cfg = self._route(spec.name)
        return MockOutboxStore(
            spec=spec,
            state=self._state(context),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockOutboxCommand(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: OutboxSpec[Any],
    ) -> StagingOutboxCommand[Any]:
        cfg = self._route(spec.name)
        store = MockOutboxStore(
            spec=spec,
            state=self._state(context),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )
        return build_staging_outbox_command_for_store(context, spec, store)


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockDistributedLock(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: DistributedLockSpec,
    ) -> MockDistributedLockAdapter:
        cfg = self._route(spec.name)
        return MockDistributedLockAdapter(
            spec=spec,
            state=self._state(context),
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockEmbeddings(_MockFactoryBase):
    dimensions: int = 8

    def __call__(
        self,
        context: ExecutionContext,
        spec: EmbeddingsSpec,
    ) -> MockHashEmbeddingsProvider:
        _ = context, spec
        return MockHashEmbeddingsProvider(dimensions=self.dimensions)


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockDurableWorkflowCommand(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: DurableWorkflowSpec[Any, Any],
    ) -> MockDurableWorkflowCommandAdapter[Any, Any]:
        return MockDurableWorkflowCommandAdapter(spec=spec, state=self._state(context))


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockDurableWorkflowQuery(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: DurableWorkflowSpec[Any, Any],
    ) -> MockDurableWorkflowQueryAdapter[Any, Any]:
        return MockDurableWorkflowQueryAdapter(spec=spec, state=self._state(context))


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockDurableWorkflowScheduleCommand(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: DurableWorkflowSpec[Any, Any],
    ) -> MockDurableWorkflowScheduleCommandAdapter[Any]:
        return MockDurableWorkflowScheduleCommandAdapter(
            spec=spec,
            state=self._state(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockDurableWorkflowScheduleQuery(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: DurableWorkflowSpec[Any, Any],
    ) -> MockDurableWorkflowScheduleQueryAdapter[Any]:
        return MockDurableWorkflowScheduleQueryAdapter(
            spec=spec,
            state=self._state(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockDurableFunctionEvent(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: DurableFunctionEventSpec[Any],
    ) -> MockDurableFunctionEventAdapter[Any]:
        return MockDurableFunctionEventAdapter(spec=spec, state=self._state(context))


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConstantMockPortFactory:
    """Adapt a pre-built mock identity port to the configurable-factory protocol.

    Identity deps are registered route-keyed as ``(ctx, spec) -> port`` factories —
    the same shape the real identity modules use — so they resolve through
    ``Deps.resolve_configurable`` (``ctx.authn`` / ``ctx.authz`` convenience
    accessors and the kit handler factories). The mock stubs are pre-built per
    route, so this factory simply hands the instance back.
    """

    port: Any
    """Pre-built mock port returned for every resolution on this route."""

    def __call__(self, context: ExecutionContext, spec: Any) -> Any:
        _ = context, spec
        return self.port


# ....................... #


def _route_eligibility(
    context: ExecutionContext,
    spec: AuthnSpec,
) -> Any:
    """Resolve the route's eligibility port (mirrors the identity-plane helper)."""

    return context.deps.provide(PrincipalEligibilityDepKey, route=spec.name)(
        context,
        spec,
    )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockAuthn(_MockFactoryBase):
    """Compose the core :class:`AuthnOrchestrator` over the route's mock ports.

    Replaces the old raising ``MockAuthnPort`` stub under ``AuthnDepKey``:
    password/token/API-key authentication runs for real against the seeded mock
    verifiers, principal resolver, and eligibility gate of the same route, with
    the spec's ``enabled_methods`` gating each credential family — exactly like
    the identity plane's ``ConfigurableAuthn``, minus the crypto. The optional
    event emitter and login lockout guard wire in the same way the identity
    factory wires them (events resolve from ``AuthnEventSinkDepKey``; lockout
    comes from the module's ``lockout`` config over the mock counter).
    """

    def __call__(
        self,
        context: ExecutionContext,
        spec: AuthnSpec,
    ) -> AuthnOrchestrator:
        def port(key: DepKey[Any]) -> Any:
            return context.deps.provide(key, route=spec.name)(context, spec)

        methods = frozenset(spec.enabled_methods)

        guard: LoginLockoutGuard | None = None

        if self.module.lockout is not None and "password" in methods:
            guard = LoginLockoutGuard(
                counter=context.counter(CounterSpec(name=LOCKOUT_COUNTER_ROUTE)),
                config=self.module.lockout,
            )

        return AuthnOrchestrator(
            resolver=port(PrincipalResolverDepKey),
            eligibility=port(PrincipalEligibilityDepKey),
            enabled_methods=methods,
            password_verifier=(
                port(PasswordVerifierDepKey) if "password" in methods else None
            ),
            token_verifier=port(TokenVerifierDepKey) if "token" in methods else None,
            api_key_verifier=(
                port(ApiKeyVerifierDepKey) if "api_key" in methods else None
            ),
            events=resolve_authn_event_emitter(context, spec),
            lockout=guard,
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockTokenLifecycle(_MockFactoryBase):
    """Build the state-backed mock token lifecycle gated by the route's eligibility."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: AuthnSpec,
    ) -> MockTokenLifecyclePort:
        return MockTokenLifecyclePort(
            state=self._state(context),
            route=str(spec.name),
            eligibility=_route_eligibility(context, spec),
            events=resolve_authn_event_emitter(context, spec),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockPasswordLifecycle(_MockFactoryBase):
    """Build the state-backed mock password lifecycle gated by the route's eligibility."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: AuthnSpec,
    ) -> MockPasswordLifecyclePort:
        return MockPasswordLifecyclePort(
            state=self._state(context),
            route=str(spec.name),
            eligibility=_route_eligibility(context, spec),
            events=resolve_authn_event_emitter(context, spec),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockPasswordReset(_MockFactoryBase):
    """Build the state-backed mock password reset gated by the route's eligibility."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: AuthnSpec,
    ) -> MockPasswordResetPort:
        return MockPasswordResetPort(
            state=self._state(context),
            route=str(spec.name),
            eligibility=_route_eligibility(context, spec),
            events=resolve_authn_event_emitter(context, spec),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockPrincipalDeactivation(_MockFactoryBase):
    """Build the mock principal deactivation stub with the route's event emitter."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: AuthnSpec,
    ) -> MockPrincipalDeactivationPort:
        return MockPrincipalDeactivationPort(
            events=resolve_authn_event_emitter(context, spec),
        )


# ....................... #


def _route_stubs(
    cls: type[Any],
    routes: Iterable[StrKey],
    *,
    state: MockState | None = None,
) -> dict[StrKey, Any]:
    """Build a ``{route: factory}`` map of constant configurable factories.

    Stateful adapters receive ``state``/``route``; stateless ones (``state`` omitted)
    are constructed with no arguments. Each pre-built adapter is wrapped in
    :class:`ConstantMockPortFactory` so the registration resolves through
    ``Deps.resolve_configurable`` like the real identity modules.
    """

    if state is None:
        return {route: ConstantMockPortFactory(port=cls()) for route in routes}

    return {
        route: ConstantMockPortFactory(port=cls(state=state, route=str(route)))
        for route in routes
    }


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MockDepsModule(DepsModule):
    """Register all in-memory mock contract adapters and identity stubs."""

    state: MockState = attrs.field(factory=MockState)
    routes: StrKeyMapping[MockRouteConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    identity: MockIdentityConfig | None = attrs.field(default=None)
    routed_state: MockRoutedStateRegistry | None = attrs.field(default=None)
    embeddings_dimensions: int = 8
    resilience: Literal["passthrough", "real"] = "passthrough"
    domain_events: DomainEventRegistry | None = attrs.field(default=None)

    strict_tx: bool = attrs.field(default=False)
    """Opt into :class:`~forze_mock.adapters.tx.MockStrictTxManagerAdapter`.

    When true, transaction rollbacks restore the DB-backed mock stores
    (documents, outbox, inbox, document-backed identity), root transactions on
    the same state serialize, and writes inside a read-only root raise
    (``code="read_only_tx"``). Default ``False`` keeps the documented no-op
    transaction manager — zero behavior change.
    """

    authn_events: bool = attrs.field(default=False)
    """Register :class:`~forze_mock.adapters.events.RecordingAuthnEventSink`
    for every authn route (optional — like the real module's ``events`` knob,
    nothing is recorded by default). Recorded events land on
    ``state.authn_events`` for seed-style test inspection. A custom sink can
    still be merged under ``AuthnEventSinkDepKey`` after the module's output."""

    lockout: LockoutConfig | None = attrs.field(default=None)
    """Optional fixed-window login lockout for password authn routes, backed by
    the in-memory mock counter (route ``authn_lockout``). ``None`` disables it,
    mirroring the real :class:`~forze_identity.authn.AuthnDepsModule`."""

    def __call__(self) -> Deps:
        document = ConfigurableMockDocument(module=self)
        dlock = ConfigurableMockDistributedLock(module=self)
        graph = ConfigurableMockGraph(module=self)
        secrets = MockSecretsPort(state=self.state)

        resilience_executor = (
            PassthroughResilienceExecutor()
            if self.resilience == "passthrough"
            else InProcessResilienceExecutor(policies=builtin_default_policies())
        )

        domain_registry = self.domain_events or DomainEventRegistry()

        def _domain_dispatcher(ctx: ExecutionContext) -> InProcessDomainEventDispatcher:
            return InProcessDomainEventDispatcher(registry=domain_registry, ctx=ctx)

        deps: dict[DepKey[Any], Any] = {
            MockStateDepKey: self.state,
            ResilienceExecutorDepKey: resilience_executor,
            DomainEventDispatcherDepKey: _domain_dispatcher,
            DocumentQueryDepKey: document,
            DocumentCommandDepKey: document,
            SearchQueryDepKey: ConfigurableMockSearch(module=self),
            SearchCommandDepKey: ConfigurableMockSearchCommand(module=self),
            SearchResultSnapshotDepKey: ConfigurableMockSearchSnapshot(module=self),
            HubSearchQueryDepKey: ConfigurableMockHubSearch(module=self),
            FederatedSearchQueryDepKey: ConfigurableMockFederatedSearch(module=self),
            AnalyticsQueryDepKey: ConfigurableMockAnalytics(module=self),
            AnalyticsIngestDepKey: ConfigurableMockAnalytics(module=self),
            CounterDepKey: ConfigurableMockCounter(module=self),
            CacheDepKey: ConfigurableMockCache(module=self),
            IdempotencyDepKey: ConfigurableMockIdempotency(module=self),
            InboxDepKey: ConfigurableMockInbox(module=self),
            StorageQueryDepKey: ConfigurableMockStorageQuery(module=self),
            StorageCommandDepKey: ConfigurableMockStorageCommand(module=self),
            GraphQueryDepKey: graph,
            GraphCommandDepKey: graph,
            GraphRawQueryDepKey: graph,
            TransactionManagerDepKey: (
                mock_strict_txmanager if self.strict_tx else mock_txmanager
            ),
            QueueQueryDepKey: ConfigurableMockQueue(module=self),
            QueueCommandDepKey: ConfigurableMockQueue(module=self),
            PubSubCommandDepKey: ConfigurableMockPubSub(module=self),
            PubSubQueryDepKey: ConfigurableMockPubSub(module=self),
            StreamQueryDepKey: ConfigurableMockStream(module=self),
            StreamCommandDepKey: ConfigurableMockStream(module=self),
            StreamGroupQueryDepKey: ConfigurableMockStreamGroup(module=self),
            OutboxCommandDepKey: ConfigurableMockOutboxCommand(module=self),
            OutboxQueryDepKey: ConfigurableMockOutboxQuery(module=self),
            DistributedLockQueryDepKey: dlock,
            DistributedLockCommandDepKey: dlock,
            EmbeddingsProviderDepKey: ConfigurableMockEmbeddings(
                module=self,
                dimensions=self.embeddings_dimensions,
            ),
            DurableWorkflowCommandDepKey: ConfigurableMockDurableWorkflowCommand(
                module=self
            ),
            DurableWorkflowQueryDepKey: ConfigurableMockDurableWorkflowQuery(
                module=self
            ),
            DurableWorkflowScheduleCommandDepKey: ConfigurableMockDurableWorkflowScheduleCommand(
                module=self
            ),
            DurableWorkflowScheduleQueryDepKey: ConfigurableMockDurableWorkflowScheduleQuery(
                module=self
            ),
            DurableFunctionEventCommandDepKey: ConfigurableMockDurableFunctionEvent(
                module=self
            ),
            DurableFunctionStepDepKey: MockDurableFunctionStepAdapter(state=self.state),
            SecretsDepKey: secrets,
        }

        if self.routed_state is not None:
            deps[MockRoutedStateDepKey] = self.routed_state

        id_cfg = self.identity or MockIdentityConfig()
        authn_keys = id_cfg.authn_routes
        authz_keys = id_cfg.authz_routes
        tenancy_keys = id_cfg.tenancy_routes

        authn_factory = ConfigurableMockAuthn(module=self)
        token_lifecycle = ConfigurableMockTokenLifecycle(module=self)
        password_lifecycle = ConfigurableMockPasswordLifecycle(module=self)
        password_reset = ConfigurableMockPasswordReset(module=self)

        # Identity deps are registered ROUTED with configurable factories — the
        # same shape the real identity modules use — so kit handlers, hooks, and
        # the ctx.authn / ctx.authz accessors resolve real working ports.
        identity_routed: dict[DepKey[Any], dict[StrKey, Any]] = {
            AuthnDepKey: {route: authn_factory for route in authn_keys},
            PasswordVerifierDepKey: _route_stubs(
                MockPasswordVerifierPort, authn_keys, state=self.state
            ),
            TokenVerifierDepKey: _route_stubs(
                MockTokenVerifierPort, authn_keys, state=self.state
            ),
            ApiKeyVerifierDepKey: _route_stubs(
                MockApiKeyVerifierPort, authn_keys, state=self.state
            ),
            PrincipalResolverDepKey: _route_stubs(
                MockPrincipalResolverPort, authn_keys, state=self.state
            ),
            PrincipalEligibilityDepKey: _route_stubs(
                MockPrincipalEligibilityPort, authn_keys
            ),
            PrincipalDeactivationDepKey: {
                route: ConfigurableMockPrincipalDeactivation(module=self)
                for route in authn_keys
            },
            TokenLifecycleDepKey: {route: token_lifecycle for route in authn_keys},
            PasswordLifecycleDepKey: {
                route: password_lifecycle for route in authn_keys
            },
            PasswordResetDepKey: {route: password_reset for route in authn_keys},
            ApiKeyLifecycleDepKey: _route_stubs(MockApiKeyLifecyclePort, authn_keys),
            PasswordAccountProvisioningDepKey: _route_stubs(
                MockPasswordAccountProvisioningPort, authn_keys
            ),
            PrincipalRegistryDepKey: _route_stubs(
                MockPrincipalRegistryPort, authz_keys, state=self.state
            ),
            RoleAssignmentDepKey: _route_stubs(MockRoleAssignmentPort, authz_keys),
            GrantQueryDepKey: _route_stubs(MockGrantQueryPort, authz_keys),
            DelegationGrantDepKey: _route_stubs(
                MockDelegationGrantPort, authz_keys, state=self.state
            ),
            DelegationDepKey: _route_stubs(
                MockDelegationPort, authz_keys, state=self.state
            ),
            AuthzDecisionDepKey: _route_stubs(
                MockAuthzDecisionPort, authz_keys, state=self.state
            ),
            AuthzScopeDepKey: _route_stubs(MockAuthzScopePort, authz_keys),
            TenantResolverDepKey: _route_stubs(
                MockTenantResolverPort, tenancy_keys, state=self.state
            ),
            TenantManagementDepKey: _route_stubs(
                MockTenantManagementPort, tenancy_keys, state=self.state
            ),
        }

        if self.authn_events:
            recording = RecordingAuthnEventSink(state=self.state)
            identity_routed[AuthnEventSinkDepKey] = {
                route: ConstantMockPortFactory(port=recording) for route in authn_keys
            }

        return Deps.merge(Deps.plain(deps), Deps.routed(identity_routed))
