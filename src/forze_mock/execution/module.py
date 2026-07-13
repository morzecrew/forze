"""Dependency module registering all mock adapters."""

from __future__ import annotations

from typing import Any, Literal, final

import attrs

from forze.application.contracts.analytics import (
    AnalyticsIngestDepKey,
    AnalyticsQueryDepKey,
)
from forze.application.contracts.authn import (
    ApiKeyLifecycleDepKey,
    ApiKeyVerifierDepKey,
    AuthnDepKey,
    AuthnEventSinkDepKey,
    PasswordAccountProvisioningDepKey,
    PasswordLifecycleDepKey,
    PasswordResetDepKey,
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
    DelegationDepKey,
    DelegationGrantDepKey,
    GrantQueryDepKey,
    PrincipalRegistryDepKey,
    RoleAssignmentDepKey,
)
from forze.application.contracts.cache import CacheDepKey
from forze.application.contracts.counter import CounterDepKey
from forze.application.contracts.crypto import (
    AeadDepKey,
    AesGcmAead,
    DeterministicCipherDepKey,
    KeyDirectoryDepKey,
    KeyManagementDepKey,
    KeyRef,
    KeyringDepKey,
    StaticKeyDirectory,
)
from forze.application.contracts.deps import DepKey
from forze.application.contracts.dlock import (
    DistributedLockCommandDepKey,
    DistributedLockQueryDepKey,
)
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.domain import DomainEventDispatcherDepKey
from forze.application.contracts.durable.function import (
    DurableFunctionEventCommandDepKey,
    DurableFunctionStepDepKey,
    DurableRunAdminDepKey,
    DurableRunStoreDepKey,
    DurableScheduleStoreDepKey,
)
from forze.application.contracts.durable.workflow import (
    DurableWorkflowCommandDepKey,
    DurableWorkflowQueryDepKey,
    DurableWorkflowScheduleCommandDepKey,
    DurableWorkflowScheduleQueryDepKey,
)
from forze.application.contracts.embeddings import (
    EmbeddingsProviderDepKey,
)
from forze.application.contracts.graph import (
    GraphCommandDepKey,
    GraphQueryDepKey,
    GraphRawQueryDepKey,
)
from forze.application.contracts.hlc import HlcCheckpointDepKey
from forze.application.contracts.http import HttpServiceDepKey
from forze.application.contracts.idempotency import (
    IdempotencyDepKey,
)
from forze.application.contracts.inbox import InboxDepKey
from forze.application.contracts.outbox import (
    OutboxAdminDepKey,
    OutboxCommandDepKey,
    OutboxQueryDepKey,
)
from forze.application.contracts.procedure import ProcedureCommandDepKey
from forze.application.contracts.pubsub import (
    PubSubCommandDepKey,
    PubSubQueryDepKey,
)
from forze.application.contracts.queue import (
    QueueCommandDepKey,
    QueueQueryDepKey,
)
from forze.application.contracts.resilience import ResilienceExecutorDepKey
from forze.application.contracts.search import (
    FederatedSearchQueryDepKey,
    HubSearchQueryDepKey,
    SearchCommandDepKey,
    SearchManagementDepKey,
    SearchQueryDepKey,
    SearchResultSnapshotDepKey,
)
from forze.application.contracts.secrets import SecretsDepKey
from forze.application.contracts.storage import (
    StorageCommandDepKey,
    StorageQueryDepKey,
    StorageUploadSessionDepKey,
)
from forze.application.contracts.stream import (
    AckStreamGroupAdminDepKey,
    AckStreamGroupQueryDepKey,
    CommitStreamGroupAdminDepKey,
    CommitStreamGroupQueryDepKey,
    StreamCommandDepKey,
    StreamQueryDepKey,
)
from forze.application.contracts.tenancy import (
    TenantManagementDepKey,
    TenantResolverDepKey,
)
from forze.application.contracts.transaction import (
    TransactionManagerDepKey,
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
from forze.application.integrations.authn import (
    LockoutConfig,
)
from forze.application.integrations.crypto import DeterministicFieldCipher, Keyring
from forze.base.primitives import MappingConverter, StrKey, StrKeyMapping
from forze_mock.adapters import (
    MockHttpRegistry,
    MockKeyManagement,
    MockProcedureRegistry,
    MockQueryParamsRegistry,
    MockState,
)
from forze_mock.adapters.events import RecordingAuthnEventSink
from forze_mock.adapters.hlc_checkpoint import MockHlcCheckpointAdapter
from forze_mock.adapters.identity import (
    MockApiKeyLifecyclePort,
    MockApiKeyVerifierPort,
    MockAuthzDecisionPort,
    MockAuthzScopePort,
    MockDelegationGrantPort,
    MockDelegationPort,
    MockGrantQueryPort,
    MockPasswordAccountProvisioningPort,
    MockPasswordVerifierPort,
    MockPrincipalEligibilityPort,
    MockPrincipalRegistryPort,
    MockPrincipalResolverPort,
    MockRoleAssignmentPort,
    MockSecretsPort,
    MockTenantManagementPort,
    MockTenantResolverPort,
    MockTokenVerifierPort,
)
from forze_mock.adapters.resilience import PassthroughResilienceExecutor
from forze_mock.execution.factories import (
    ConfigurableMockAckStreamGroup,
    ConfigurableMockAckStreamGroupAdmin,
    ConfigurableMockAnalytics,
    ConfigurableMockAuthn,
    ConfigurableMockCache,
    ConfigurableMockCommitStreamGroup,
    ConfigurableMockCommitStreamGroupAdmin,
    ConfigurableMockCounter,
    ConfigurableMockDistributedLock,
    ConfigurableMockDocument,
    ConfigurableMockDurableFunctionEvent,
    ConfigurableMockDurableFunctionStep,
    ConfigurableMockDurableRunStore,
    ConfigurableMockDurableSchedule,
    ConfigurableMockDurableWorkflowCommand,
    ConfigurableMockDurableWorkflowQuery,
    ConfigurableMockDurableWorkflowScheduleCommand,
    ConfigurableMockDurableWorkflowScheduleQuery,
    ConfigurableMockEmbeddings,
    ConfigurableMockFederatedSearch,
    ConfigurableMockGraph,
    ConfigurableMockHttpService,
    ConfigurableMockHubSearch,
    ConfigurableMockIdempotency,
    ConfigurableMockInbox,
    ConfigurableMockOutboxCommand,
    ConfigurableMockOutboxQuery,
    ConfigurableMockPasswordLifecycle,
    ConfigurableMockPasswordReset,
    ConfigurableMockPrincipalDeactivation,
    ConfigurableMockProcedure,
    ConfigurableMockPubSub,
    ConfigurableMockQueue,
    ConfigurableMockSearch,
    ConfigurableMockSearchCommand,
    ConfigurableMockSearchManagement,
    ConfigurableMockSearchSnapshot,
    ConfigurableMockStorageCommand,
    ConfigurableMockStorageQuery,
    ConfigurableMockStorageUploads,
    ConfigurableMockStream,
    ConfigurableMockTokenLifecycle,
    ConstantMockPortFactory,
    mock_journal_txmanager,
    mock_strict_txmanager,
    mock_txmanager,
    route_stubs,
)
from forze_mock.tenancy import MockRoutedStateRegistry
from forze_mock.tenancy.routed import MockRoutedStateDepKey

from .configs import MockRouteConfig
from .keys import MockStateDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MockIdentityConfig:
    """Optional seed data for mock identity stubs."""

    authn_routes: frozenset[StrKey] = frozenset({"main"})
    authz_routes: frozenset[StrKey] = frozenset({"main"})
    tenancy_routes: frozenset[StrKey] = frozenset({"main"})


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
    http: MockHttpRegistry | None = attrs.field(default=None)
    """Programmable in-memory responses for outbound ``HttpServicePort`` calls.

    ``None`` registers the port but leaves every operation unprogrammed (any call
    raises ``code="mock.http.unprogrammed"``); pass a :class:`MockHttpRegistry`
    with handlers to answer HTTP ops in-process with zero external services."""

    procedures: MockProcedureRegistry | None = attrs.field(default=None)
    """Programmable in-memory handlers for the ``ProcedurePort`` (governed parametrized
    commands/compute).

    ``None`` registers the port but leaves every procedure unprogrammed (any call raises
    ``code="mock.procedures.unprogrammed"``); pass a :class:`MockProcedureRegistry` with handlers
    that model each procedure's effect on :class:`MockState`."""

    query_param_sources: MockQueryParamsRegistry | None = attrs.field(default=None)
    """Programmable sources modelling parametrized document reads (``with_parameters``).

    ``None`` leaves a ``query_params`` document read unprogrammed (a bound read raises
    ``code="mock.query_parameters.unprogrammed"``); pass a :class:`MockQueryParamsRegistry` whose
    sources produce rows from the bound params + :class:`MockState`, over which the DSL composes."""

    resilience: Literal["passthrough", "real"] = "passthrough"
    domain_events: DomainEventRegistry | None = attrs.field(default=None)

    hlc_checkpoint: bool = False
    """Wire the in-memory HLC high-water-mark store (default off).

    When ``True``, the outbox flush persists the node's clock mark into :class:`MockState`,
    so ``hlc_checkpoint_recovery_lifecycle_step`` can resume a rebuilt clock above its prior
    emissions — letting a test exercise restart monotonicity in-process. Off by default so
    existing scenarios are unperturbed (the outbox flush then resolves no checkpoint)."""

    transactions: Literal["journal", "none", "strict"] = attrs.field(default="journal")
    """Which mock transaction manager to wire (default ``"journal"``):

    - ``"journal"`` (default) — :class:`~forze_mock.adapters.tx.MockJournalTxManagerAdapter`:
      concurrency-preserving **atomicity** via a per-transaction undo journal (a failed
      operation leaves no partial writes), with row-``rev`` optimistic concurrency and
      read-only-root enforcement. The faithful default — makes simulation/DST findings
      trustworthy without serializing concurrent transactions.
    - ``"none"`` — the legacy no-op manager: writes inside a rolled-back transaction
      **persist**. Only for tests that deliberately assert that unfaithful behavior.
    - ``"strict"`` — :class:`~forze_mock.adapters.tx.MockStrictTxManagerAdapter`: whole-store
      snapshot/restore atomicity, but **serializes** root transactions (no interleaving).
    """

    strict_tx: bool = attrs.field(default=False)
    """Back-compat alias: when true, forces ``transactions="strict"``."""

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

    def _txmanager_factory(self) -> Any:
        """Pick the transaction-manager factory (``strict_tx`` forces strict for back-compat)."""

        mode = "strict" if self.strict_tx else self.transactions
        return {
            "journal": mock_journal_txmanager,
            "none": mock_txmanager,
            "strict": mock_strict_txmanager,
        }[mode]

    def __call__(self) -> Deps:
        document = ConfigurableMockDocument(module=self)
        dlock = ConfigurableMockDistributedLock(module=self)
        graph = ConfigurableMockGraph(module=self)
        secrets = MockSecretsPort(state=self.state)

        crypto_kms = MockKeyManagement()
        crypto_aead = AesGcmAead()
        crypto_directory = StaticKeyDirectory(KeyRef(key_id="mock-cmk"))
        crypto_keyring = Keyring(
            kms=crypto_kms,
            aead=crypto_aead,
            directory=crypto_directory,
        )
        crypto_deterministic = DeterministicFieldCipher(root=b"mock-deterministic-root-secret!!")

        resilience_executor = (
            PassthroughResilienceExecutor()
            if self.resilience == "passthrough"
            else InProcessResilienceExecutor(policies=builtin_default_policies())
        )

        domain_registry = self.domain_events or DomainEventRegistry()

        def _domain_dispatcher(ctx: ExecutionContext) -> InProcessDomainEventDispatcher:
            return InProcessDomainEventDispatcher(registry=domain_registry, ctx=ctx)

        def _hlc_checkpoint(ctx: ExecutionContext) -> MockHlcCheckpointAdapter:
            return MockHlcCheckpointAdapter(state=self.state)

        deps: dict[DepKey[Any], Any] = {
            MockStateDepKey: self.state,
            ResilienceExecutorDepKey: resilience_executor,
            DomainEventDispatcherDepKey: _domain_dispatcher,
            DocumentQueryDepKey: document,
            DocumentCommandDepKey: document,
            SearchQueryDepKey: ConfigurableMockSearch(module=self),
            SearchCommandDepKey: ConfigurableMockSearchCommand(module=self),
            SearchManagementDepKey: ConfigurableMockSearchManagement(module=self),
            SearchResultSnapshotDepKey: ConfigurableMockSearchSnapshot(module=self),
            HubSearchQueryDepKey: ConfigurableMockHubSearch(module=self),
            FederatedSearchQueryDepKey: ConfigurableMockFederatedSearch(module=self),
            AnalyticsQueryDepKey: ConfigurableMockAnalytics(module=self),
            AnalyticsIngestDepKey: ConfigurableMockAnalytics(module=self),
            ProcedureCommandDepKey: ConfigurableMockProcedure(module=self),
            CounterDepKey: ConfigurableMockCounter(module=self),
            CacheDepKey: ConfigurableMockCache(module=self),
            IdempotencyDepKey: ConfigurableMockIdempotency(module=self),
            InboxDepKey: ConfigurableMockInbox(module=self),
            StorageQueryDepKey: ConfigurableMockStorageQuery(module=self),
            StorageCommandDepKey: ConfigurableMockStorageCommand(module=self),
            StorageUploadSessionDepKey: ConfigurableMockStorageUploads(module=self),
            GraphQueryDepKey: graph,
            GraphCommandDepKey: graph,
            GraphRawQueryDepKey: graph,
            HttpServiceDepKey: ConfigurableMockHttpService(module=self),
            TransactionManagerDepKey: self._txmanager_factory(),
            QueueQueryDepKey: ConfigurableMockQueue(module=self),
            QueueCommandDepKey: ConfigurableMockQueue(module=self, command=True),
            PubSubCommandDepKey: ConfigurableMockPubSub(module=self, command=True),
            PubSubQueryDepKey: ConfigurableMockPubSub(module=self),
            StreamQueryDepKey: ConfigurableMockStream(module=self),
            StreamCommandDepKey: ConfigurableMockStream(module=self, command=True),
            AckStreamGroupQueryDepKey: ConfigurableMockAckStreamGroup(module=self),
            AckStreamGroupAdminDepKey: ConfigurableMockAckStreamGroupAdmin(module=self),
            CommitStreamGroupQueryDepKey: ConfigurableMockCommitStreamGroup(module=self),
            CommitStreamGroupAdminDepKey: ConfigurableMockCommitStreamGroupAdmin(module=self),
            OutboxCommandDepKey: ConfigurableMockOutboxCommand(module=self),
            OutboxQueryDepKey: ConfigurableMockOutboxQuery(module=self),
            # The store serves both protocols; the admin key is separate so a read-only
            # QUERY can acquire the depth probes without the claim/mark port.
            OutboxAdminDepKey: ConfigurableMockOutboxQuery(module=self),
            DistributedLockQueryDepKey: dlock,
            DistributedLockCommandDepKey: dlock,
            EmbeddingsProviderDepKey: ConfigurableMockEmbeddings(
                module=self,
                dimensions=self.embeddings_dimensions,
            ),
            DurableWorkflowCommandDepKey: ConfigurableMockDurableWorkflowCommand(module=self),
            DurableWorkflowQueryDepKey: ConfigurableMockDurableWorkflowQuery(module=self),
            DurableWorkflowScheduleCommandDepKey: ConfigurableMockDurableWorkflowScheduleCommand(
                module=self
            ),
            DurableWorkflowScheduleQueryDepKey: ConfigurableMockDurableWorkflowScheduleQuery(
                module=self
            ),
            DurableFunctionEventCommandDepKey: ConfigurableMockDurableFunctionEvent(module=self),
            DurableFunctionStepDepKey: ConfigurableMockDurableFunctionStep(module=self),
            DurableRunStoreDepKey: ConfigurableMockDurableRunStore(module=self),
            DurableRunAdminDepKey: ConfigurableMockDurableRunStore(module=self),
            DurableScheduleStoreDepKey: ConfigurableMockDurableSchedule(module=self),
            SecretsDepKey: secrets,
            KeyManagementDepKey: crypto_kms,
            AeadDepKey: crypto_aead,
            KeyDirectoryDepKey: crypto_directory,
            KeyringDepKey: crypto_keyring,
            DeterministicCipherDepKey: crypto_deterministic,
        }

        if self.routed_state is not None:
            deps[MockRoutedStateDepKey] = self.routed_state

        if self.hlc_checkpoint:
            deps[HlcCheckpointDepKey] = _hlc_checkpoint

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
            AuthnDepKey: dict.fromkeys(authn_keys, authn_factory),
            PasswordVerifierDepKey: route_stubs(
                MockPasswordVerifierPort, authn_keys, state=self.state
            ),
            TokenVerifierDepKey: route_stubs(MockTokenVerifierPort, authn_keys, state=self.state),
            ApiKeyVerifierDepKey: route_stubs(MockApiKeyVerifierPort, authn_keys, state=self.state),
            PrincipalResolverDepKey: route_stubs(
                MockPrincipalResolverPort, authn_keys, state=self.state
            ),
            PrincipalEligibilityDepKey: route_stubs(MockPrincipalEligibilityPort, authn_keys),
            PrincipalDeactivationDepKey: {
                route: ConfigurableMockPrincipalDeactivation(module=self) for route in authn_keys
            },
            TokenLifecycleDepKey: dict.fromkeys(authn_keys, token_lifecycle),
            PasswordLifecycleDepKey: dict.fromkeys(authn_keys, password_lifecycle),
            PasswordResetDepKey: dict.fromkeys(authn_keys, password_reset),
            ApiKeyLifecycleDepKey: route_stubs(MockApiKeyLifecyclePort, authn_keys),
            PasswordAccountProvisioningDepKey: route_stubs(
                MockPasswordAccountProvisioningPort, authn_keys
            ),
            PrincipalRegistryDepKey: route_stubs(
                MockPrincipalRegistryPort, authz_keys, state=self.state
            ),
            RoleAssignmentDepKey: route_stubs(MockRoleAssignmentPort, authz_keys),
            GrantQueryDepKey: route_stubs(MockGrantQueryPort, authz_keys),
            DelegationGrantDepKey: route_stubs(
                MockDelegationGrantPort, authz_keys, state=self.state
            ),
            DelegationDepKey: route_stubs(MockDelegationPort, authz_keys, state=self.state),
            AuthzDecisionDepKey: route_stubs(MockAuthzDecisionPort, authz_keys, state=self.state),
            AuthzScopeDepKey: route_stubs(MockAuthzScopePort, authz_keys),
            TenantResolverDepKey: route_stubs(
                MockTenantResolverPort, tenancy_keys, state=self.state
            ),
            TenantManagementDepKey: route_stubs(
                MockTenantManagementPort, tenancy_keys, state=self.state
            ),
        }

        if self.authn_events:
            recording = RecordingAuthnEventSink(state=self.state)
            identity_routed[AuthnEventSinkDepKey] = {
                route: ConstantMockPortFactory(port=recording) for route in authn_keys
            }

        return Deps.merge(Deps.plain(deps), Deps.routed(identity_routed))
