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
from forze.application.contracts.http import HttpServiceDepKey
from forze.application.contracts.idempotency import (
    IdempotencyDepKey,
)
from forze.application.contracts.inbox import InboxDepKey
from forze.application.contracts.outbox import (
    OutboxCommandDepKey,
    OutboxQueryDepKey,
)
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
    StreamCommandDepKey,
    StreamGroupQueryDepKey,
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
    MockDurableFunctionStepAdapter,
    MockHttpRegistry,
    MockKeyManagement,
    MockState,
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
    ConfigurableMockAnalytics,
    ConfigurableMockAuthn,
    ConfigurableMockCache,
    ConfigurableMockCounter,
    ConfigurableMockDistributedLock,
    ConfigurableMockDocument,
    ConfigurableMockDurableFunctionEvent,
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
    ConfigurableMockPubSub,
    ConfigurableMockQueue,
    ConfigurableMockSearch,
    ConfigurableMockSearchCommand,
    ConfigurableMockSearchSnapshot,
    ConfigurableMockStorageCommand,
    ConfigurableMockStorageQuery,
    ConfigurableMockStorageUploads,
    ConfigurableMockStream,
    ConfigurableMockStreamGroup,
    ConfigurableMockTokenLifecycle,
    ConstantMockPortFactory,
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

        crypto_kms = MockKeyManagement()
        crypto_aead = AesGcmAead()
        crypto_directory = StaticKeyDirectory(KeyRef(key_id="mock-cmk"))
        crypto_keyring = Keyring(
            kms=crypto_kms,
            aead=crypto_aead,
            directory=crypto_directory,
        )
        crypto_deterministic = DeterministicFieldCipher(
            root=b"mock-deterministic-root-secret!!"
        )

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
            StorageUploadSessionDepKey: ConfigurableMockStorageUploads(module=self),
            GraphQueryDepKey: graph,
            GraphCommandDepKey: graph,
            GraphRawQueryDepKey: graph,
            HttpServiceDepKey: ConfigurableMockHttpService(module=self),
            TransactionManagerDepKey: (
                mock_strict_txmanager if self.strict_tx else mock_txmanager
            ),
            QueueQueryDepKey: ConfigurableMockQueue(module=self),
            QueueCommandDepKey: ConfigurableMockQueue(module=self, command=True),
            PubSubCommandDepKey: ConfigurableMockPubSub(module=self, command=True),
            PubSubQueryDepKey: ConfigurableMockPubSub(module=self),
            StreamQueryDepKey: ConfigurableMockStream(module=self),
            StreamCommandDepKey: ConfigurableMockStream(module=self, command=True),
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
            KeyManagementDepKey: crypto_kms,
            AeadDepKey: crypto_aead,
            KeyDirectoryDepKey: crypto_directory,
            KeyringDepKey: crypto_keyring,
            DeterministicCipherDepKey: crypto_deterministic,
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
            PasswordVerifierDepKey: route_stubs(
                MockPasswordVerifierPort, authn_keys, state=self.state
            ),
            TokenVerifierDepKey: route_stubs(
                MockTokenVerifierPort, authn_keys, state=self.state
            ),
            ApiKeyVerifierDepKey: route_stubs(
                MockApiKeyVerifierPort, authn_keys, state=self.state
            ),
            PrincipalResolverDepKey: route_stubs(
                MockPrincipalResolverPort, authn_keys, state=self.state
            ),
            PrincipalEligibilityDepKey: route_stubs(
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
            DelegationDepKey: route_stubs(
                MockDelegationPort, authz_keys, state=self.state
            ),
            AuthzDecisionDepKey: route_stubs(
                MockAuthzDecisionPort, authz_keys, state=self.state
            ),
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
