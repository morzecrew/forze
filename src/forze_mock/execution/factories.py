"""Configurable per-spec factories that build mock adapters for MockDepsModule."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, final

import attrs

from forze.application.contracts.analytics import AnalyticsSpec
from forze.application.contracts.authn import (
    ApiKeyVerifierDepKey,
    AuthnSpec,
    PasswordVerifierDepKey,
    PrincipalEligibilityDepKey,
    PrincipalResolverDepKey,
    TokenVerifierDepKey,
    resolve_authn_event_emitter,
)
from forze.application.contracts.cache import CachePort, CacheSpec
from forze.application.contracts.counter import (
    CounterAdminPort,
    CounterPort,
    CounterSpec,
)
from forze.application.contracts.crypto import KeyringDepKey
from forze.application.contracts.deps import DepKey
from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.durable.function import DurableFunctionEventSpec
from forze.application.contracts.durable.workflow import DurableWorkflowSpec
from forze.application.contracts.embeddings import EmbeddingsSpec
from forze.application.contracts.graph import GraphModuleSpec
from forze.application.contracts.http import HttpServiceSpec
from forze.application.contracts.idempotency import IdempotencyPort, IdempotencySpec
from forze.application.contracts.inbox import InboxPort, InboxSpec
from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.procedure import ProcedureSpec
from forze.application.contracts.pubsub import PubSubCommandPort, PubSubSpec
from forze.application.contracts.queue import QueueCommandPort, QueueSpec
from forze.application.contracts.search import (
    FederatedSearchSpec,
    HubSearchSpec,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.contracts.storage import (
    StorageCommandPort,
    StorageQueryPort,
    StorageSpec,
    StorageUploadSessionPort,
)
from forze.application.contracts.stream import StreamCommandPort
from forze.application.contracts.stream.specs import StreamSpec
from forze.application.contracts.tenancy import TenantProviderPort
from forze.application.contracts.transaction import TransactionManagerPort
from forze.application.execution import ExecutionContext
from forze.application.execution.crypto import enforce_required_reach
from forze.application.execution.domain import domain_dispatcher_provider
from forze.application.execution.outbox import build_staging_outbox_command_for_store
from forze.application.integrations.authn import (
    LOCKOUT_COUNTER_ROUTE,
    AuthnOrchestrator,
    LoginLockoutGuard,
)
from forze.application.integrations.outbox import StagingOutboxCommand
from forze.application.integrations.pubsub import encrypting_pubsub_command
from forze.application.integrations.queue import encrypting_queue_command
from forze.application.integrations.search import SearchResultSnapshot
from forze.application.integrations.stream import encrypting_stream_command
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze_mock.adapters import (
    MockAckStreamGroupAdapter,
    MockAckStreamGroupAdminAdapter,
    MockAnalyticsAdapter,
    MockCacheAdapter,
    MockCommitStreamGroupAdapter,
    MockCommitStreamGroupAdminAdapter,
    MockCounterAdapter,
    MockCounterAdminAdapter,
    MockDistributedLockAdapter,
    MockDocumentAdapter,
    MockDurableFunctionEventAdapter,
    MockDurableFunctionStepAdapter,
    MockDurableRunStore,
    MockDurableScheduleStore,
    MockDurableWorkflowCommandAdapter,
    MockDurableWorkflowQueryAdapter,
    MockDurableWorkflowScheduleCommandAdapter,
    MockDurableWorkflowScheduleQueryAdapter,
    MockFederatedSearchAdapter,
    MockGraphAdapter,
    MockHttpRegistry,
    MockHttpServiceAdapter,
    MockHubSearchAdapter,
    MockIdempotencyAdapter,
    MockInboxAdapter,
    MockJournalTxManagerAdapter,
    MockProcedureAdapter,
    MockProcedureRegistry,
    MockPubSubAdapter,
    MockQueueAdapter,
    MockSearchAdapter,
    MockSearchCommandAdapter,
    MockSearchManagementAdapter,
    MockSearchResultSnapshotAdapter,
    MockState,
    MockStorageAdapter,
    MockStreamAdapter,
    MockStrictTxManagerAdapter,
    MockTxManagerAdapter,
)
from forze_mock.adapters.embeddings import MockHashEmbeddingsProvider
from forze_mock.adapters.identity import (
    MockPasswordLifecyclePort,
    MockPasswordResetPort,
    MockPrincipalDeactivationPort,
    MockTokenLifecyclePort,
)
from forze_mock.adapters.outbox import MockOutboxStore
from forze_mock.tenancy import resolve_mock_namespace_sync

from .configs import MockRouteConfig
from .keys import MockStateDepKey

if TYPE_CHECKING:
    from forze_mock.execution.module import MockDepsModule

# ----------------------- #

DocSpec = DocumentSpec[Any, Any, Any, Any]

# ....................... #


def mock_txmanager(context: ExecutionContext) -> TransactionManagerPort:
    return MockTxManagerAdapter(state=context.deps.provide(MockStateDepKey))


# ....................... #


def mock_strict_txmanager(context: ExecutionContext) -> TransactionManagerPort:
    return MockStrictTxManagerAdapter(state=context.deps.provide(MockStateDepKey))


# ....................... #


def mock_journal_txmanager(context: ExecutionContext) -> TransactionManagerPort:
    return MockJournalTxManagerAdapter(state=context.deps.provide(MockStateDepKey))


# ....................... #


def _tenant_provider(ctx: ExecutionContext) -> TenantProviderPort:
    return ctx.inv_ctx.get_tenant


# ....................... #


@attrs.define(slots=True, kw_only=True)
class _MockFactoryBase:
    module: MockDepsModule

    # ....................... #

    def _state(self, ctx: ExecutionContext) -> MockState:
        return ctx.deps.provide(MockStateDepKey)

    def _route(self, spec_name: StrKey) -> MockRouteConfig | None:
        routes = self.module.routes

        return None if routes is None else routes.get(spec_name)

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


# ....................... #


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
        domain_model = spec.write["domain"] if spec.write is not None else None
        sources = self.module.query_param_sources
        query_params_source = sources.source_for(str(spec.name)) if sources is not None else None

        return MockDocumentAdapter[Any, Any, Any, Any](
            spec=spec,
            state=state,
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
            read_model=spec.read,
            domain_model=domain_model,
            dispatcher_provider=domain_dispatcher_provider(context),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
            query_params_source=query_params_source,
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockHttpService(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: HttpServiceSpec,
    ) -> MockHttpServiceAdapter:
        # Stateless except the programmable handler registry on the module; an
        # absent registry yields an empty one, so every op is "unprogrammed"
        # (loud error) until a handler is registered.
        return MockHttpServiceAdapter(
            spec=spec,
            registry=self.module.http or MockHttpRegistry(),
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
class ConfigurableMockProcedure(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: ProcedureSpec[Any, Any],
    ) -> MockProcedureAdapter[Any, Any]:
        cfg = self._route(spec.name)
        return MockProcedureAdapter(
            state=self._state(context),
            spec=spec,
            registry=self.module.procedures or MockProcedureRegistry(),
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
            snap_port = context.deps.provide(SearchResultSnapshotDepKey)(context, spec.snapshot)
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
class ConfigurableMockSearchManagement(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: SearchSpec[Any],
    ) -> MockSearchManagementAdapter:
        cfg = self._route(spec.name)
        return MockSearchManagementAdapter(
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

        legs.extend((member.name, search_factory(context, member)) for member in spec.members)

        snap = None
        if spec.snapshot is not None:
            snap_port = context.deps.provide(SearchResultSnapshotDepKey)(context, spec.snapshot)
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
            snap_port = context.deps.provide(SearchResultSnapshotDepKey)(context, spec.snapshot)
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
class ConfigurableMockCounterAdmin(_MockFactoryBase):
    def __call__(self, context: ExecutionContext, spec: CounterSpec) -> CounterAdminPort:
        cfg = self._route(spec.name)
        return MockCounterAdminAdapter(
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
    def __call__(self, context: ExecutionContext, spec: StorageSpec) -> StorageQueryPort:
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
    def __call__(self, context: ExecutionContext, spec: StorageSpec) -> StorageCommandPort:
        cfg = self._route(spec.name)
        return MockStorageAdapter(
            state=self._state(context),
            bucket=self._namespace_for(context, spec.name, default=str(spec.name)),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockStorageUploads(_MockFactoryBase):
    def __call__(self, context: ExecutionContext, spec: StorageSpec) -> StorageUploadSessionPort:
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
    def __call__(self, context: ExecutionContext, spec: GraphModuleSpec) -> MockGraphAdapter:
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
    command: bool = False
    """When ``True`` (the command-side registration), wrap writes with payload encryption
    for an ``end_to_end`` route; the query-side registration stays unwrapped so consumers
    keep the full receive/consume surface."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: QueueSpec[Any],
    ) -> MockQueueAdapter[Any] | QueueCommandPort[Any]:
        cfg = self._route(spec.name)
        adapter = MockQueueAdapter(
            state=self._state(context),
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
            codec=spec.codec,
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )

        enforce_required_reach(
            context.deps,
            route=str(spec.name),
            declared=spec.encryption,
            kind="queue",
            supports_at_rest=False,
        )

        if not self.command:
            return adapter

        cipher = context.deps.provide(KeyringDepKey) if context.deps.exists(KeyringDepKey) else None
        return encrypting_queue_command(
            adapter, spec, cipher=cipher, tenant_provider=_tenant_provider(context)
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockPubSub(_MockFactoryBase):
    command: bool = False
    """When ``True`` (the command-side registration), wrap publishes with encryption for an
    ``end_to_end`` route; the query-side registration stays unwrapped."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: PubSubSpec[Any],
    ) -> MockPubSubAdapter[Any] | PubSubCommandPort[Any]:
        cfg = self._route(spec.name)
        adapter = MockPubSubAdapter(
            state=self._state(context),
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
            codec=spec.codec,
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )

        enforce_required_reach(
            context.deps,
            route=str(spec.name),
            declared=spec.encryption,
            kind="pubsub",
            supports_at_rest=False,
        )

        if not self.command:
            return adapter

        cipher = context.deps.provide(KeyringDepKey) if context.deps.exists(KeyringDepKey) else None
        return encrypting_pubsub_command(
            adapter, spec, cipher=cipher, tenant_provider=_tenant_provider(context)
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockStream(_MockFactoryBase):
    command: bool = False
    """When ``True`` (the command-side registration), wrap appends with encryption for an
    ``end_to_end`` route; the query/group-side registrations stay unwrapped."""

    def _adapter(self, context: ExecutionContext, spec: StreamSpec[Any]) -> MockStreamAdapter[Any]:
        cfg = self._route(spec.name)
        return MockStreamAdapter(
            state=self._state(context),
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
            codec=spec.codec,
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )

    def __call__(
        self,
        context: ExecutionContext,
        spec: StreamSpec[Any],
    ) -> MockStreamAdapter[Any] | StreamCommandPort[Any]:
        adapter = self._adapter(context, spec)

        enforce_required_reach(
            context.deps,
            route=str(spec.name),
            declared=spec.encryption,
            kind="stream",
            supports_at_rest=False,
        )

        if not self.command:
            return adapter

        cipher = context.deps.provide(KeyringDepKey) if context.deps.exists(KeyringDepKey) else None
        return encrypting_stream_command(
            adapter, spec, cipher=cipher, tenant_provider=_tenant_provider(context)
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockAckStreamGroup(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: StreamSpec[Any],
    ) -> MockAckStreamGroupAdapter[Any]:
        enforce_required_reach(
            context.deps,
            route=str(spec.name),
            declared=spec.encryption,
            kind="stream",
            supports_at_rest=False,
        )
        stream = ConfigurableMockStream(module=self.module)._adapter(  # pyright: ignore[reportPrivateUsage]
            context, spec
        )
        return MockAckStreamGroupAdapter(
            stream=stream,
            state=self._state(context),
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockAckStreamGroupAdmin(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: StreamSpec[Any],
    ) -> MockAckStreamGroupAdminAdapter[Any]:
        stream = ConfigurableMockStream(module=self.module)._adapter(  # pyright: ignore[reportPrivateUsage]
            context, spec
        )
        return MockAckStreamGroupAdminAdapter(stream=stream, state=self._state(context))


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockCommitStreamGroup(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: StreamSpec[Any],
    ) -> MockCommitStreamGroupAdapter[Any]:
        enforce_required_reach(
            context.deps,
            route=str(spec.name),
            declared=spec.encryption,
            kind="stream",
            supports_at_rest=False,
        )
        stream = ConfigurableMockStream(module=self.module)._adapter(  # pyright: ignore[reportPrivateUsage]
            context, spec
        )
        return MockCommitStreamGroupAdapter(
            stream=stream,
            state=self._state(context),
            namespace=self._namespace_for(context, spec.name, default=str(spec.name)),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockCommitStreamGroupAdmin(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: StreamSpec[Any],
    ) -> MockCommitStreamGroupAdminAdapter[Any]:
        stream = ConfigurableMockStream(module=self.module)._adapter(  # pyright: ignore[reportPrivateUsage]
            context, spec
        )
        return MockCommitStreamGroupAdminAdapter(stream=stream, state=self._state(context))


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
        cfg = self._route(spec.name)
        return MockDurableWorkflowCommandAdapter(
            spec=spec,
            state=self._state(context),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockDurableWorkflowQuery(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: DurableWorkflowSpec[Any, Any],
    ) -> MockDurableWorkflowQueryAdapter[Any, Any]:
        cfg = self._route(spec.name)
        return MockDurableWorkflowQueryAdapter(
            spec=spec,
            state=self._state(context),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockDurableWorkflowScheduleCommand(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: DurableWorkflowSpec[Any, Any],
    ) -> MockDurableWorkflowScheduleCommandAdapter[Any]:
        cfg = self._route(spec.name)
        return MockDurableWorkflowScheduleCommandAdapter(
            spec=spec,
            state=self._state(context),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockDurableWorkflowScheduleQuery(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: DurableWorkflowSpec[Any, Any],
    ) -> MockDurableWorkflowScheduleQueryAdapter[Any]:
        cfg = self._route(spec.name)
        return MockDurableWorkflowScheduleQueryAdapter(
            spec=spec,
            state=self._state(context),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockDurableFunctionEvent(_MockFactoryBase):
    def __call__(
        self,
        context: ExecutionContext,
        spec: DurableFunctionEventSpec[Any],
    ) -> MockDurableFunctionEventAdapter[Any]:
        cfg = self._route(spec.name)
        return MockDurableFunctionEventAdapter(
            spec=spec,
            state=self._state(context),
            tenant_aware=cfg.tenant_aware if cfg else False,
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockDurableFunctionStep(_MockFactoryBase):
    """Build the mock durable step port (a ``SimpleDepPort``: ``ctx`` only, no spec).

    Resolves state per scope via ``MockStateDepKey`` like every other mock factory, so it
    is coherent under DST — a shared instance would bypass ``resolve_simple``'s per-scope
    resolution and tracing.
    """

    def __call__(
        self,
        context: ExecutionContext,
    ) -> MockDurableFunctionStepAdapter:
        return MockDurableFunctionStepAdapter(state=self._state(context))


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockDurableRunStore(_MockFactoryBase):
    """Build the mock durable run store (a ``SimpleDepPort``: ``ctx`` only, no spec)."""

    def __call__(
        self,
        context: ExecutionContext,
    ) -> MockDurableRunStore:
        return MockDurableRunStore(
            state=self._state(context),
            tenant_provider=_tenant_provider(context),
        )


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableMockDurableSchedule(_MockFactoryBase):
    """Build the mock durable schedule store (a ``SimpleDepPort``: ``ctx`` only, no spec)."""

    def __call__(
        self,
        context: ExecutionContext,
    ) -> MockDurableScheduleStore:
        return MockDurableScheduleStore(
            state=self._state(context),
            tenant_provider=_tenant_provider(context),
        )


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
            password_verifier=(port(PasswordVerifierDepKey) if "password" in methods else None),
            token_verifier=port(TokenVerifierDepKey) if "token" in methods else None,
            api_key_verifier=(port(ApiKeyVerifierDepKey) if "api_key" in methods else None),
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


def route_stubs(
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
        route: ConstantMockPortFactory(port=cls(state=state, route=str(route))) for route in routes
    }
