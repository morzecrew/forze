"""A route reaches the ``namespace`` tier only when every relation it names resolves per tenant.

One static relation holds every tenant's rows however the others are split, so it drops the
route to the tier its ``tenant_aware`` flag gives, and a declared ``namespace`` floor refuses it.

# covers: PostgresDepsModule.__attrs_post_init__
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from forze.base.exceptions import CoreException
from forze_postgres import PostgresClient, PostgresDepsModule
from forze_postgres.execution.deps.configs import (
    PostgresAnalyticsConfig,
    PostgresDocumentConfig,
    PostgresFederatedSearchConfig,
    PostgresFederatedSearchLegHub,
    PostgresFederatedSearchLegSearch,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresIdempotencyConfig,
    PostgresInboxConfig,
    PostgresQueryConfig,
    PostgresReadOnlyDocumentConfig,
    PostgresSearchConfig,
)

# ----------------------- #


def _relation(table: str, *, static: bool) -> Any:
    if static:
        return ("public", table)

    return lambda tenant_id: (f"tenant_{tenant_id}", table)


def _document(static: str | None) -> dict[str, Any]:
    return {
        "rw_documents": {
            "orders": PostgresDocumentConfig(
                read=_relation("orders_view", static=static == "read"),
                write=_relation("orders", static=static == "write"),
                history=_relation("orders_history", static=static == "history"),
                bookkeeping_strategy="application",
            ),
        },
    }


def _search(static: str | None) -> dict[str, Any]:
    return {
        "searches": {
            "orders": PostgresSearchConfig(
                index=_relation("orders_idx", static=static == "index"),
                read=_relation("orders_view", static=static == "read"),
                heap=_relation("orders", static=static == "heap"),
                engine="pgroonga",
            ),
        },
    }


def _analytics(static: str | None) -> dict[str, Any]:
    return {
        "analytics": {
            "events": PostgresAnalyticsConfig(
                queries={"counts": PostgresQueryConfig(sql="SELECT 1 AS value")},
                # Unset, the queries read whatever their SQL names: one place for every tenant.
                query_schema=None if static == "query_schema" else lambda t: f"tenant_{t}",
                ingest=_relation("events", static=static == "ingest"),
            ),
        },
    }


# These two resolve their table with the tenant only when they are tenant-aware, and keep a
# shared table apart by a tenant prefix on each key: the ``tagged`` tier, not ``namespace``.


def _idempotency(static: str | None, *, tenant_aware: bool = True) -> dict[str, Any]:
    return {
        "idempotencies": {
            "orders": PostgresIdempotencyConfig(
                relation=_relation("idempotency", static=static == "relation"),
                tenant_aware=tenant_aware,
            ),
        },
    }


def _inbox(static: str | None, *, tenant_aware: bool = True) -> dict[str, Any]:
    return {
        "inboxes": {
            "orders": PostgresInboxConfig(
                relation=_relation("inbox", static=static == "relation"),
                tenant_aware=tenant_aware,
            ),
        },
    }


_ROUTES: dict[str, tuple[Callable[[str | None], dict[str, Any]], tuple[str, ...]]] = {
    "document": (_document, ("read", "write", "history")),
    "search": (_search, ("index", "read", "heap")),
    "analytics": (_analytics, ("query_schema", "ingest")),
    "idempotency": (_idempotency, ("relation",)),
    "inbox": (_inbox, ("relation",)),
}

_STATIC_CASES = [
    pytest.param(build, field, id=f"{kind}-{field}")
    for kind, (build, fields) in _ROUTES.items()
    for field in fields
]


def _module(*, floor: str | None, **routes: Any) -> PostgresDepsModule:
    # A compliant sibling, so a refusal is the route's and never an empty module's.
    return PostgresDepsModule(
        client=PostgresClient(),
        required_tenant_isolation=floor,  # type: ignore[arg-type]
        ro_documents={
            "baseline": PostgresReadOnlyDocumentConfig(read=_relation("b", static=False))
        },
        **routes,
    )


# ....................... #


@pytest.mark.parametrize("kind", list(_ROUTES))
def test_a_route_resolving_every_relation_per_tenant_meets_a_namespace_floor(kind: str) -> None:
    build, _ = _ROUTES[kind]

    assert _module(floor="namespace", **build(None)) is not None


@pytest.mark.parametrize(("build", "field"), _STATIC_CASES)
def test_one_static_relation_drops_the_route_below_a_namespace_floor(
    build: Callable[[str | None], dict[str, Any]],
    field: str,
) -> None:
    with pytest.raises(CoreException, match="postgres_tenancy_validation_failed"):
        _module(floor="namespace", **build(field))

    # The control: with no floor declared the same wiring stands, so the refusal is the floor's.
    assert _module(floor=None, **build(field)) is not None


def test_an_unset_optional_relation_does_not_count() -> None:
    per_tenant = _relation("orders", static=False)

    module = _module(
        floor="namespace",
        rw_documents={
            "orders": PostgresDocumentConfig(
                read=per_tenant,
                write=per_tenant,
                bookkeeping_strategy="application",
            ),
        },
        analytics={
            "events": PostgresAnalyticsConfig(
                queries={"counts": PostgresQueryConfig(sql="SELECT 1 AS value")},
                query_schema=lambda t: f"tenant_{t}",
            ),
        },
    )

    assert module is not None


@pytest.mark.parametrize("build", [_idempotency, _inbox], ids=["idempotency", "inbox"])
def test_a_store_resolving_its_table_without_the_tenant_is_not_namespace(
    build: Callable[..., dict[str, Any]],
) -> None:
    # Not tenant-aware, the store calls its resolver with no tenant: one table for all.
    with pytest.raises(CoreException, match="postgres_tenancy_validation_failed"):
        _module(floor="namespace", **build(None, tenant_aware=False))


@pytest.mark.parametrize("build", [_idempotency, _inbox], ids=["idempotency", "inbox"])
def test_a_shared_store_meets_a_tagged_floor_only_when_tenant_aware(
    build: Callable[..., dict[str, Any]],
) -> None:
    with pytest.raises(CoreException, match="postgres_tenancy_validation_failed"):
        _module(floor="tagged", **build("relation", tenant_aware=False))

    assert _module(floor="tagged", **build("relation", tenant_aware=True)) is not None


def _search_leg(*, static: bool, tenant_aware: bool) -> PostgresFederatedSearchLegSearch:
    return PostgresFederatedSearchLegSearch(
        search=PostgresSearchConfig(
            index=_relation("help_idx", static=static),
            read=_relation("help", static=static),
            engine="pgroonga",
            tenant_aware=tenant_aware,
        ),
    )


_HUB_LEG = PostgresFederatedSearchLegHub(
    hub=PostgresHubSearchConfig(
        hub=_relation("hub", static=True),
        members={
            "m": PostgresHubSearchMemberConfig(
                index=_relation("m_idx", static=True),
                read=_relation("m", static=True),
                engine="pgroonga",
                hub_fk="hub_id",
            ),
        },
    ),
)


def _federated(leg: Any, *, tenant_aware: bool = True) -> dict[str, Any]:
    return {
        "federated_searches": {
            "fed": PostgresFederatedSearchConfig(
                tenant_aware=tenant_aware,
                members={"docs": _search_leg(static=True, tenant_aware=True), "other": leg},
            ),
        },
    }


@pytest.mark.parametrize(
    ("wiring", "meets_tagged"),
    [
        pytest.param(
            _federated(_search_leg(static=True, tenant_aware=True)), True, id="tagged-leg"
        ),
        pytest.param(
            _federated(_search_leg(static=False, tenant_aware=False)), True, id="per-tenant-leg"
        ),
        # The hub takes the federation's flag for every leg it runs.
        pytest.param(_federated(_HUB_LEG), True, id="hub-leg"),
        pytest.param(
            _federated(_search_leg(static=True, tenant_aware=False)), False, id="untagged-leg"
        ),
        pytest.param(
            _federated(_search_leg(static=True, tenant_aware=True), tenant_aware=False),
            False,
            id="untagged-federation",
        ),
    ],
)
def test_a_federated_search_is_tagged_only_when_every_leg_is_scoped(
    wiring: dict[str, Any], meets_tagged: bool
) -> None:
    if meets_tagged:
        assert _module(floor="tagged", **wiring) is not None
    else:
        with pytest.raises(CoreException) as caught:
            _module(floor="tagged", **wiring)

        assert caught.value.details["derived_isolation"] == "none"
        assert "federated_search route 'fed'" in caught.value.summary

    # The control: with no floor declared every wiring stands, so a refusal is the floor's.
    assert _module(floor=None, **wiring) is not None


def test_a_federated_search_never_claims_namespace() -> None:
    wiring = _federated(_search_leg(static=False, tenant_aware=False))

    with pytest.raises(CoreException) as caught:
        _module(floor="namespace", **wiring)

    assert caught.value.details["derived_isolation"] == "tagged"
