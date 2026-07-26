"""Tests for Postgres deps module and analytics config validation."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
    IngestSpec,
)
from forze.base.exceptions import CoreException
from forze_postgres import PostgresClient, PostgresDepsModule
from forze_postgres.execution.deps.configs import (
    PostgresAnalyticsConfig,
    PostgresQueryConfig,
)
from tests.support.execution_context import (
    context_from_deps,
)


class _Row(BaseModel):
    value: int


class _Params(BaseModel):
    pass


def _spec() -> AnalyticsSpec[_Row, _Row]:
    return AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
        ingest=_Row,
    )


def test_validate_missing_query_key() -> None:
    spec = _spec()
    config = PostgresAnalyticsConfig(
        queries={},
        ingest=IngestSpec(("public", "t")),
    )
    with pytest.raises(CoreException, match="missing query keys"):
        config.validate_against_spec(spec)


def test_deps_module_registers_analytics_keys() -> None:
    client = PostgresClient()
    module = PostgresDepsModule(
        client=client,
        analytics={
            "events": PostgresAnalyticsConfig(
                queries={"counts": PostgresQueryConfig(sql="SELECT 1 AS value")},
                ingest=IngestSpec(("public", "events")),
            ),
        },
    )
    deps = module()
    ctx = context_from_deps(deps)
    spec = _spec()
    assert ctx.analytics.query(spec) is not None
    assert ctx.analytics.ingest(spec) is not None


def test_required_dedicated_isolation_rejects_shared_client_with_analytics() -> None:
    # A tenant-aware analytics route on a shared client derives "tagged"; a declared
    # "dedicated" floor rejects it at wiring.
    with pytest.raises(CoreException, match="postgres_tenancy_validation_failed"):
        PostgresDepsModule(
            client=PostgresClient(),
            required_tenant_isolation="dedicated",
            analytics={
                "events": PostgresAnalyticsConfig(
                    tenant_aware=True,
                    queries={
                        "counts": PostgresQueryConfig(
                            sql="SELECT 1 AS value WHERE tenant_id = %(tenant)s",
                        ),
                    },
                ),
            },
        )


def test_rotating_credentials_route_is_validated_against_isolation_floor() -> None:
    # The floor is enforced per route, so an unscoped sibling must not hide behind a
    # compliant one. A tenant-aware document satisfies "tagged" on its own; the credential
    # store next to it does not, and omitting it from validation is exactly how every
    # tenant's third-party grants end up sharing one unfiltered table under a declared floor.
    from collections.abc import Mapping

    from forze.application.contracts.secrets import ExchangedCredential, SecretRef
    from forze_postgres.execution.deps.configs import (
        PostgresDocumentConfig,
        PostgresRotatingCredentialsConfig,
    )

    class _Exchanger:
        async def exchange(
            self,
            ref: SecretRef,
            *,
            refresh_token: str,
            metadata: Mapping[str, str],
        ) -> ExchangedCredential:  # pragma: no cover — wiring never calls it
            raise NotImplementedError

    def _module(**extra: object) -> PostgresDepsModule:
        return PostgresDepsModule(
            client=PostgresClient(),
            required_tenant_isolation="tagged",
            rw_documents={
                "orders": PostgresDocumentConfig(
                    read=("public", "orders"),
                    write=("public", "orders"),
                    bookkeeping_strategy="application",
                    tenant_aware=True,
                ),
            },
            **extra,  # type: ignore[arg-type]
        )

    # The compliant sibling alone passes the floor.
    assert _module() is not None

    with pytest.raises(CoreException, match="postgres_tenancy_validation_failed"):
        _module(
            rotating_credentials=PostgresRotatingCredentialsConfig(
                relation=("public", "rotating_credentials"),
                exchanger=_Exchanger(),
                tenant_aware=False,
            ),
        )


def test_outbox_route_is_validated_against_isolation_floor() -> None:
    # A tenant-aware outbox route is now included in tenancy validation (was excluded):
    # a "dedicated" floor on a shared client rejects it.
    from forze_postgres.execution.deps.configs import PostgresOutboxConfig

    with pytest.raises(CoreException, match="postgres_tenancy_validation_failed"):
        PostgresDepsModule(
            client=PostgresClient(),
            required_tenant_isolation="dedicated",
            outboxes={
                "events": PostgresOutboxConfig(
                    relation=("public", "outbox"),
                    tenant_aware=True,
                ),
            },
        )


def test_ingest_coerces_raw_relation_tuple() -> None:
    # The natural migration from the old ingest_relation=(...) must not crash:
    # a raw relation spec is coerced into an IngestSpec.
    config = PostgresAnalyticsConfig(queries={}, ingest=("public", "events"))
    assert config.resolved_ingest_relation() == ("public", "events")
