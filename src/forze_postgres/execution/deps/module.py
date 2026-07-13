"""Postgres dependency module for the application kernel."""

from collections.abc import Callable
from datetime import timedelta
from functools import partial
from typing import Any, cast, final

import attrs

from forze.application.contracts.analytics import (
    AnalyticsIngestDepKey,
    AnalyticsQueryDepKey,
)
from forze.application.contracts.crypto import EncryptionTier
from forze.application.contracts.deps import (
    DepKey,
    Deps,
    DepsModule,
    merge_deps,
    routed_from_mapping,
)
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.durable.function import (
    DurableFunctionStepDepKey,
    DurableRunAdminDepKey,
    DurableRunStoreDepKey,
    DurableScheduleStoreDepKey,
)
from forze.application.contracts.hlc import HlcCheckpointDepKey
from forze.application.contracts.idempotency import IdempotencyDepKey
from forze.application.contracts.inbox import InboxDepKey
from forze.application.contracts.outbox import OutboxCommandDepKey, OutboxQueryDepKey
from forze.application.contracts.procedure import ProcedureCommandDepKey
from forze.application.contracts.search import (
    FederatedSearchQueryDepKey,
    HubSearchQueryDepKey,
    SearchQueryDepKey,
)
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKey, StrKeyMapping

from ...kernel.catalog.introspect import PostgresIntrospector
from ...kernel.catalog.validation.validate_relation_specs import (
    warn_dynamic_relation_with_tenant_aware,
)
from ...kernel.catalog.validation.validate_tenancy import (
    PostgresTenancyRouteSpec,
    PostgresTenantIsolationMode,
    validate_postgres_tenancy_wiring,
)
from ...kernel.client import PostgresClientPort, RoutedPostgresClient
from .configs import (
    PostgresAnalyticsConfig,
    PostgresDocumentConfig,
    PostgresDurableRunConfig,
    PostgresDurableScheduleConfig,
    PostgresDurableStepConfig,
    PostgresFederatedSearchConfig,
    PostgresFederatedSearchLegHub,
    PostgresHlcCheckpointConfig,
    PostgresHubSearchConfig,
    PostgresIdempotencyConfig,
    PostgresInboxConfig,
    PostgresOutboxConfig,
    PostgresProcedureConfig,
    PostgresReadOnlyDocumentConfig,
    PostgresSearchConfig,
)
from .factories import (
    ConfigurablePostgresAnalytics,
    ConfigurablePostgresDocument,
    ConfigurablePostgresDurableRun,
    ConfigurablePostgresDurableSchedule,
    ConfigurablePostgresDurableStep,
    ConfigurablePostgresFederatedSearch,
    ConfigurablePostgresHlcCheckpoint,
    ConfigurablePostgresHubSearch,
    ConfigurablePostgresIdempotency,
    ConfigurablePostgresInbox,
    ConfigurablePostgresOutboxCommand,
    ConfigurablePostgresOutboxQuery,
    ConfigurablePostgresProcedure,
    ConfigurablePostgresReadOnlyDocument,
    ConfigurablePostgresSearch,
    postgres_txmanager,
)
from .keys import PostgresClientDepKey, PostgresIntrospectorDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresDepsModule(DepsModule):
    """Dependency module that registers Postgres clients and adapters."""

    client: PostgresClientPort
    """Pre-constructed Postgres client (single-DSN or routed). For :class:`~forze_postgres.kernel.client.RoutedPostgresClient`, set :attr:`introspector_cache_partition_key` to match tenant routing."""

    introspector_cache_partition_key: Callable[[], str | None] | None = attrs.field(
        default=None,
    )
    """When set, :class:`PostgresIntrospector` cache keys include this partition (e.g. tenant id).

    Required for correct catalog caching with database-per-tenant routing.
    """

    introspector_cache_ttl: timedelta | None = attrs.field(default=None)
    """Optional TTL for :class:`PostgresIntrospector` catalog caches (``None`` = no expiry)."""

    required_tenant_isolation: PostgresTenantIsolationMode | None = attrs.field(
        default=None,
    )
    """Declared minimum tenant isolation for this deployment (``None`` = no floor).

    When set, wiring fails closed if the derived isolation (routed client / per-route
    ``tenant_aware`` / relation resolvers) is weaker than the requirement. Set to
    ``"dedicated"`` to refuse any shared-store (``tagged``/``namespace``) wiring — the only
    model safe for untrusted callers.
    """

    required_encryption: EncryptionTier | None = attrs.field(default=None)
    """Declared minimum document field-encryption coverage (``None`` = no floor).

    When set, a document spec served by this module whose derived coverage is weaker
    is refused at resolution: ``"field"`` requires every document to declare a non-empty
    ``encryption`` policy (``FieldEncryption.encrypted``/``searchable``) and have a keyring
    wired. Documents can only ever provide per-``field`` coverage.
    """

    ro_documents: StrKeyMapping[PostgresReadOnlyDocumentConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from read-only document names to their Postgres-specific configurations."""

    rw_documents: StrKeyMapping[PostgresDocumentConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from read-write document names to their Postgres-specific configurations."""

    searches: StrKeyMapping[PostgresSearchConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from search names to their Postgres-specific configurations."""

    hub_searches: StrKeyMapping[PostgresHubSearchConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from hub search names to their Postgres-specific configurations."""

    federated_searches: StrKeyMapping[PostgresFederatedSearchConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from federated search names to their Postgres-specific configurations."""

    tx: set[StrKey] | None = attrs.field(default=None)
    """Set of transaction routes to register."""

    analytics: StrKeyMapping[PostgresAnalyticsConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from analytics route names to their Postgres-specific configurations."""

    procedures: StrKeyMapping[PostgresProcedureConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from procedure route names to their Postgres-specific configurations."""

    outboxes: StrKeyMapping[PostgresOutboxConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from outbox route names to their Postgres-specific configurations."""

    inboxes: StrKeyMapping[PostgresInboxConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from inbox route names to their Postgres-specific configurations."""

    idempotencies: StrKeyMapping[PostgresIdempotencyConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from idempotency route names to their Postgres-specific configurations.

    Co-located store: the result record commits inside the business transaction, so a
    duplicate cannot re-execute after a crash between the business commit and the record."""

    hlc_checkpoint: PostgresHlcCheckpointConfig | None = attrs.field(default=None)
    """Optional Postgres HLC high-water-mark store (node-global; default unwired).

    When set, the outbox flush persists the runtime's clock mark in the business
    transaction so ``hlc_checkpoint_recovery_lifecycle_step`` can resume the clock above its
    prior emissions after a restart, keeping HLC monotonicity across process boundaries.
    Unset leaves the clock resuming from ``(0, 0)`` (the prior behavior)."""

    durable_step: PostgresDurableStepConfig | None = attrs.field(default=None)
    """Optional Postgres durable-function step-memo journal (execution-scoped).

    When set, registers the ``DurableFunctionStepPort`` over a ``durable_step`` table so
    steps memoize their results (exactly-once effect on replay), enabling the self-hosted
    durable saga executor. Unset leaves durability delegated to Temporal/Inngest."""

    durable_run: PostgresDurableRunConfig | None = attrs.field(default=None)
    """Optional Postgres durable-run store (execution-scoped).

    When set, registers the ``DurableRunStorePort`` over a ``durable_run`` table so run
    instances persist and a crashed run can be re-claimed and resumed. Pairs with
    :attr:`durable_step` and the ``forze_kits`` durable-function runner / recovery scanner."""

    durable_schedule: PostgresDurableScheduleConfig | None = attrs.field(default=None)
    """Optional Postgres durable-schedule store (execution-scoped).

    When set, registers the ``DurableScheduleStorePort`` over a ``durable_schedule`` table
    so recurring cron triggers fire runs on a cadence (driven by the ``forze_kits`` durable
    scheduler lifecycle step)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        routes: list[PostgresTenancyRouteSpec] = []

        if (
            self.introspector_cache_ttl is not None
            and self.introspector_cache_ttl.total_seconds() <= 0
        ):
            raise exc.configuration("Introspector cache TTL must be positive")

        if self.ro_documents:
            for name, cfg in self.ro_documents.items():
                routes.append(
                    PostgresTenancyRouteSpec(
                        name=str(name),
                        tenant_aware=cfg.tenant_aware,
                        kind="document",
                        has_namespace_routing=callable(cfg.read),
                    ),
                )
                warn_dynamic_relation_with_tenant_aware(
                    route_name=str(name),
                    kind="document",
                    tenant_aware=cfg.tenant_aware,
                    fields=[("read", cfg.read)],
                )

        if self.rw_documents:
            for name, cfg in self.rw_documents.items():
                routes.append(
                    PostgresTenancyRouteSpec(
                        name=str(name),
                        tenant_aware=cfg.tenant_aware,
                        kind="document",
                        has_namespace_routing=callable(cfg.read) or callable(cfg.write),
                    ),
                )
                warn_dynamic_relation_with_tenant_aware(
                    route_name=str(name),
                    kind="document",
                    tenant_aware=cfg.tenant_aware,
                    fields=[
                        ("read", cfg.read),
                        ("write", cfg.write),
                        ("history", cfg.history),
                    ],
                )

        if self.searches:
            for name, search_cfg in self.searches.items():
                routes.append(
                    PostgresTenancyRouteSpec(
                        name=str(name),
                        tenant_aware=search_cfg.tenant_aware,
                        kind="search",
                        has_namespace_routing=callable(search_cfg.index),
                    ),
                )
                warn_dynamic_relation_with_tenant_aware(
                    route_name=str(name),
                    kind="search",
                    tenant_aware=search_cfg.tenant_aware,
                    fields=[
                        ("index", search_cfg.index),
                        ("read", search_cfg.read),
                        ("heap", search_cfg.heap_relation),
                    ],
                )

        if self.hub_searches:
            for name, hub_search_cfg in self.hub_searches.items():
                routes.append(
                    PostgresTenancyRouteSpec(
                        name=str(name),
                        tenant_aware=hub_search_cfg.tenant_aware,
                        kind="hub_search",
                    ),
                )
                warn_dynamic_relation_with_tenant_aware(
                    route_name=str(name),
                    kind="hub_search",
                    tenant_aware=hub_search_cfg.tenant_aware,
                    fields=[("hub", hub_search_cfg.hub)],
                )

                for member_name, leg in hub_search_cfg.members.items():
                    warn_dynamic_relation_with_tenant_aware(
                        route_name=f"{name}.{member_name}",
                        kind="hub_search",
                        tenant_aware=hub_search_cfg.tenant_aware,
                        fields=[
                            ("index", leg.index),
                            ("read", leg.read),
                            ("heap", leg.heap_relation),
                        ],
                    )

        if self.federated_searches:
            for name, federated_search_cfg in self.federated_searches.items():
                routes.append(
                    PostgresTenancyRouteSpec(
                        name=str(name),
                        tenant_aware=federated_search_cfg.tenant_aware,
                        kind="federated_search",
                    ),
                )

                for member_name, fed_leg in federated_search_cfg.members.items():
                    if isinstance(fed_leg, PostgresFederatedSearchLegHub):
                        hub_cfg = fed_leg.hub
                        warn_dynamic_relation_with_tenant_aware(
                            route_name=f"{name}.{member_name}",
                            kind="federated_search",
                            tenant_aware=federated_search_cfg.tenant_aware,
                            fields=[("hub", hub_cfg.hub)],
                        )

                        for hub_member_name, hub_leg in hub_cfg.members.items():
                            warn_dynamic_relation_with_tenant_aware(
                                route_name=f"{name}.{member_name}.{hub_member_name}",
                                kind="federated_search",
                                tenant_aware=federated_search_cfg.tenant_aware,
                                fields=[
                                    ("index", hub_leg.index),
                                    ("read", hub_leg.read),
                                    ("heap", hub_leg.heap_relation),
                                ],
                            )

                    else:
                        search_cfg = fed_leg.search

                        warn_dynamic_relation_with_tenant_aware(
                            route_name=f"{name}.{member_name}",
                            kind="federated_search",
                            tenant_aware=federated_search_cfg.tenant_aware,
                            fields=[
                                ("index", search_cfg.index),
                                ("read", search_cfg.read),
                                ("heap", search_cfg.heap_relation),
                            ],
                        )

        if self.analytics:
            for name, analytics_cfg in self.analytics.items():
                routes.append(
                    PostgresTenancyRouteSpec(
                        name=str(name),
                        tenant_aware=analytics_cfg.tenant_aware,
                        kind="analytics",
                        has_namespace_routing=callable(analytics_cfg.query_schema),
                    ),
                )

        if self.procedures:
            for name, procedure_cfg in self.procedures.items():
                routes.append(
                    PostgresTenancyRouteSpec(
                        name=str(name),
                        tenant_aware=procedure_cfg.tenant_aware,
                        kind="procedures",
                        has_namespace_routing=callable(procedure_cfg.query_schema),
                    ),
                )

        if self.outboxes:
            for name, outbox_cfg in self.outboxes.items():
                routes.append(
                    PostgresTenancyRouteSpec(
                        name=str(name),
                        tenant_aware=outbox_cfg.tenant_aware,
                        kind="outbox",
                        has_namespace_routing=callable(outbox_cfg.relation),
                    ),
                )

        if self.inboxes:
            for name, inbox_cfg in self.inboxes.items():
                routes.append(
                    PostgresTenancyRouteSpec(
                        name=str(name),
                        tenant_aware=inbox_cfg.tenant_aware,
                        kind="inbox",
                        has_namespace_routing=callable(inbox_cfg.relation),
                    ),
                )

        # Namespace tier is now tracked per route (a DYNAMIC per-tenant relation /
        # query_schema resolver on that route) so a declared floor is enforced route by route.
        validate_postgres_tenancy_wiring(
            client_is_routed=isinstance(self.client, RoutedPostgresClient),
            introspector_cache_partition_key_set=(
                self.introspector_cache_partition_key is not None
            ),
            routes=routes,
            required_isolation=self.required_tenant_isolation,
        )

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Postgres-backed ports."""

        plain_deps = Deps.plain(
            {
                PostgresClientDepKey: self.client,
                PostgresIntrospectorDepKey: PostgresIntrospector(
                    client=self.client,
                    cache_partition_key=self.introspector_cache_partition_key,
                    cache_ttl=self.introspector_cache_ttl,
                ),
            }
        )

        search_deps = Deps()
        hub_search_deps = Deps()
        federated_search_deps = Deps()
        tx_deps = Deps()
        analytics_deps = Deps()
        procedures_deps = Deps()
        outbox_deps = Deps()

        # ``cast`` erases the factories' generic parameters (``partial`` would otherwise
        # leak them as Unknown); the encryption floor is partial-applied so the route
        # builder only deals in plain ``factory(config=...)`` callables. A read-write
        # config is also a read-only config, so the query side reuses the RO factory.
        ro_query_factory = cast(
            Callable[..., Any],
            partial(
                ConfigurablePostgresReadOnlyDocument,
                required_encryption=self.required_encryption,
            ),
        )
        rw_command_factory = cast(
            Callable[..., Any],
            partial(
                ConfigurablePostgresDocument,
                required_encryption=self.required_encryption,
            ),
        )

        doc_deps = merge_deps(
            routed_from_mapping(
                self.ro_documents,
                bindings=[(DocumentQueryDepKey, ro_query_factory)],
            ),
            routed_from_mapping(
                self.rw_documents,
                bindings=[
                    (DocumentQueryDepKey, ro_query_factory),
                    (DocumentCommandDepKey, rw_command_factory),
                ],
            ),
        )

        if self.searches:
            search_deps = search_deps.merge(
                Deps.routed(
                    {
                        SearchQueryDepKey: {
                            name: ConfigurablePostgresSearch(config=config)
                            for name, config in self.searches.items()
                        }
                    }
                )
            )

        if self.hub_searches:
            hub_search_deps = hub_search_deps.merge(
                Deps.routed(
                    {
                        HubSearchQueryDepKey: {
                            name: ConfigurablePostgresHubSearch(config=config)
                            for name, config in self.hub_searches.items()
                        }
                    }
                )
            )

        if self.federated_searches:
            federated_search_deps = federated_search_deps.merge(
                Deps.routed(
                    {
                        FederatedSearchQueryDepKey: {
                            name: ConfigurablePostgresFederatedSearch(config=config)
                            for name, config in self.federated_searches.items()
                        }
                    }
                )
            )

        if self.tx:
            tx_deps = tx_deps.merge(
                Deps.routed({TransactionManagerDepKey: dict.fromkeys(self.tx, postgres_txmanager)})
            )

        if self.analytics:
            factory = ConfigurablePostgresAnalytics
            analytics_deps = analytics_deps.merge(
                Deps.routed(
                    {
                        AnalyticsQueryDepKey: {
                            name: factory(config=config) for name, config in self.analytics.items()
                        },
                        AnalyticsIngestDepKey: {
                            name: factory(config=config) for name, config in self.analytics.items()
                        },
                    }
                )
            )

        if self.procedures:
            procedures_deps = procedures_deps.merge(
                Deps.routed(
                    {
                        ProcedureCommandDepKey: {
                            name: ConfigurablePostgresProcedure(config=config)
                            for name, config in self.procedures.items()
                        },
                    }
                )
            )

        if self.outboxes:
            outbox_deps = outbox_deps.merge(
                Deps.routed(
                    {
                        OutboxCommandDepKey: {
                            name: ConfigurablePostgresOutboxCommand(config=config)
                            for name, config in self.outboxes.items()
                        },
                        OutboxQueryDepKey: {
                            name: ConfigurablePostgresOutboxQuery(config=config)
                            for name, config in self.outboxes.items()
                        },
                    }
                )
            )

        inbox_deps = Deps()

        if self.inboxes:
            inbox_deps = Deps.routed(
                {
                    InboxDepKey: {
                        name: ConfigurablePostgresInbox(config=config)
                        for name, config in self.inboxes.items()
                    },
                }
            )

        idempotency_deps = Deps()

        if self.idempotencies:
            idempotency_deps = Deps.routed(
                {
                    IdempotencyDepKey: {
                        name: ConfigurablePostgresIdempotency(config=config)
                        for name, config in self.idempotencies.items()
                    },
                }
            )

        hlc_checkpoint_deps = Deps()

        if self.hlc_checkpoint is not None:
            # Node-global singleton (SimpleDepPort), so a plain registration — not routed.
            hlc_checkpoint_deps = Deps.plain(
                {
                    HlcCheckpointDepKey: ConfigurablePostgresHlcCheckpoint(
                        config=self.hlc_checkpoint
                    ),
                }
            )

        durable_step_deps = Deps()

        if self.durable_step is not None:
            # Execution-scoped step port (SimpleDepPort): plain registration, resolved per
            # scope; the active run is read from the ambient DurableRunContext, not a route.
            durable_step_deps = Deps.plain(
                {
                    DurableFunctionStepDepKey: ConfigurablePostgresDurableStep(
                        config=self.durable_step
                    ),
                }
            )

        durable_run_deps = Deps()

        if self.durable_run is not None:
            # The dual-port store implements both the run store and the admin/list port; the
            # same factory registers under both keys when admin listing is opted in.
            run_factory = ConfigurablePostgresDurableRun(config=self.durable_run)
            run_registrations: dict[DepKey[Any], Any] = {DurableRunStoreDepKey: run_factory}

            if self.durable_run.admin:
                run_registrations[DurableRunAdminDepKey] = run_factory

            durable_run_deps = Deps.plain(run_registrations)

        durable_schedule_deps = Deps()

        if self.durable_schedule is not None:
            durable_schedule_deps = Deps.plain(
                {
                    DurableScheduleStoreDepKey: ConfigurablePostgresDurableSchedule(
                        config=self.durable_schedule
                    ),
                }
            )

        return plain_deps.merge(
            doc_deps,
            search_deps,
            tx_deps,
            hub_search_deps,
            federated_search_deps,
            analytics_deps,
            procedures_deps,
            outbox_deps,
            inbox_deps,
            idempotency_deps,
            hlc_checkpoint_deps,
            durable_step_deps,
            durable_run_deps,
            durable_schedule_deps,
        )
