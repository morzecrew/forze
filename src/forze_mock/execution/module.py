"""Dependency module registering all mock adapters."""

from __future__ import annotations

from typing import Any, Mapping, final

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
    AuthnSpec,
    PasswordAccountProvisioningDepKey,
    PasswordLifecycleDepKey,
    PasswordVerifierDepKey,
    PrincipalDeactivationDepKey,
    PrincipalEligibilityDepKey,
    PrincipalResolverDepKey,
    TokenLifecycleDepKey,
    TokenVerifierDepKey,
)
from forze.application.contracts.authz import (
    AuthzDecisionDepKey,
    AuthzScopeDepKey,
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
from forze.application.contracts.idempotency import (
    IdempotencyDepKey,
    IdempotencyPort,
    IdempotencySpec,
)
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
from forze.application.contracts.storage import StorageDepKey, StoragePort, StorageSpec
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
from forze.application.execution import Deps, DepsModule, ExecutionContext
from forze.application.integrations.search import SearchResultSnapshot
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
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
    MockHubSearchAdapter,
    MockIdempotencyAdapter,
    MockPubSubAdapter,
    MockQueueAdapter,
    MockSearchAdapter,
    MockSearchCommandAdapter,
    MockSearchResultSnapshotAdapter,
    MockState,
    MockStorageAdapter,
    MockStreamAdapter,
    MockStreamGroupAdapter,
    MockTxManagerAdapter,
)
from forze_mock.adapters.identity import (
    MockApiKeyLifecyclePort,
    MockApiKeyVerifierPort,
    MockAuthnPort,
    MockAuthzDecisionPort,
    MockAuthzScopePort,
    MockGrantQueryPort,
    MockPasswordAccountProvisioningPort,
    MockPasswordLifecyclePort,
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
from forze.application.integrations.outbox import StagingOutboxCommand
from forze.application.execution.outbox import build_staging_outbox_command_for_store
from forze_mock.outbox_adapter import MockOutboxStore
from forze_mock.tenancy import MockRoutedStateRegistry, resolve_mock_namespace_sync
from forze_mock.tenancy.routed import MockRoutedStateDepKey

from .configs import MockRouteConfig
from .keys import MockStateDepKey

# ----------------------- #

DocSpec = DocumentSpec[Any, Any, Any, Any]


def mock_txmanager(context: ExecutionContext) -> TransactionManagerPort:
    del context
    return MockTxManagerAdapter()


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
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockStorage(_MockFactoryBase):
    def __call__(self, context: ExecutionContext, spec: StorageSpec) -> StoragePort:
        cfg = self._route(spec.name)
        return MockStorageAdapter(
            state=self._state(context),
            bucket=self._namespace_for(context, spec.name, default=str(spec.name)),
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


def _identity_route_factory(
    cls: type[Any],
    *,
    state: MockState,
    route: StrKey,
) -> Any:
    return cls(state=state, route=str(route))


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MockDepsModule(DepsModule):
    """Register all in-memory mock contract adapters and identity stubs."""

    state: MockState = attrs.field(factory=MockState)
    routes: Mapping[StrKey, MockRouteConfig] | None = attrs.field(default=None)
    identity: MockIdentityConfig | None = attrs.field(default=None)
    routed_state: MockRoutedStateRegistry | None = attrs.field(default=None)
    embeddings_dimensions: int = 8

    def __call__(self) -> Deps:
        document = ConfigurableMockDocument(module=self)
        dlock = ConfigurableMockDistributedLock(module=self)
        secrets = MockSecretsPort(state=self.state)

        deps: dict[DepKey[Any], Any] = {
            MockStateDepKey: self.state,
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
            StorageDepKey: ConfigurableMockStorage(module=self),
            TransactionManagerDepKey: mock_txmanager,
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
        authn_routes: dict[StrKey, Any] = {
            route: MockAuthnPort(
                spec=AuthnSpec(name=route, enabled_methods=frozenset({"token"})),
            )
            for route in id_cfg.authn_routes
        }
        password_routes = {
            route: _identity_route_factory(
                MockPasswordVerifierPort,
                state=self.state,
                route=route,
            )
            for route in id_cfg.authn_routes
        }
        token_routes = {
            route: _identity_route_factory(
                MockTokenVerifierPort,
                state=self.state,
                route=route,
            )
            for route in id_cfg.authn_routes
        }
        api_key_routes = {
            route: _identity_route_factory(
                MockApiKeyVerifierPort,
                state=self.state,
                route=route,
            )
            for route in id_cfg.authn_routes
        }
        resolver_routes = {
            route: _identity_route_factory(
                MockPrincipalResolverPort,
                state=self.state,
                route=route,
            )
            for route in id_cfg.authn_routes
        }
        deps.update(
            {
                AuthnDepKey: authn_routes,
                PasswordVerifierDepKey: password_routes,
                TokenVerifierDepKey: token_routes,
                ApiKeyVerifierDepKey: api_key_routes,
                PrincipalResolverDepKey: resolver_routes,
                PrincipalEligibilityDepKey: {
                    route: MockPrincipalEligibilityPort()
                    for route in id_cfg.authn_routes
                },
                PrincipalDeactivationDepKey: {
                    route: MockPrincipalDeactivationPort()
                    for route in id_cfg.authn_routes
                },
                TokenLifecycleDepKey: {
                    route: MockTokenLifecyclePort() for route in id_cfg.authn_routes
                },
                PasswordLifecycleDepKey: {
                    route: MockPasswordLifecyclePort() for route in id_cfg.authn_routes
                },
                ApiKeyLifecycleDepKey: {
                    route: MockApiKeyLifecyclePort() for route in id_cfg.authn_routes
                },
                PasswordAccountProvisioningDepKey: {
                    route: MockPasswordAccountProvisioningPort()
                    for route in id_cfg.authn_routes
                },
                PrincipalRegistryDepKey: {
                    route: _identity_route_factory(
                        MockPrincipalRegistryPort,
                        state=self.state,
                        route=route,
                    )
                    for route in id_cfg.authz_routes
                },
                RoleAssignmentDepKey: {
                    route: MockRoleAssignmentPort() for route in id_cfg.authz_routes
                },
                GrantQueryDepKey: {
                    route: MockGrantQueryPort() for route in id_cfg.authz_routes
                },
                AuthzDecisionDepKey: {
                    route: MockAuthzDecisionPort() for route in id_cfg.authz_routes
                },
                AuthzScopeDepKey: {
                    route: MockAuthzScopePort() for route in id_cfg.authz_routes
                },
                TenantResolverDepKey: {
                    route: _identity_route_factory(
                        MockTenantResolverPort,
                        state=self.state,
                        route=route,
                    )
                    for route in id_cfg.tenancy_routes
                },
                TenantManagementDepKey: {
                    route: _identity_route_factory(
                        MockTenantManagementPort,
                        state=self.state,
                        route=route,
                    )
                    for route in id_cfg.tenancy_routes
                },
            }
        )

        return Deps.plain(deps)
