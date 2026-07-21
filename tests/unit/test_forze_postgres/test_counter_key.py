"""The Postgres counter's route/suffix key encoding and namespace-tier getter — no DB.

The data-isolation guarantees are asserted against a real backend (the differential leg in
``tests/integration/test_forze_postgres/test_postgres_counter.py``); these pin the pure
logic the mock could never exercise: the length-prefixed route fold and the resolution
getter.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze_postgres.adapters.counter import (
    PostgresCounterAdapter,
    _decode_suffix,  # pyright: ignore[reportPrivateUsage]
    _encode_suffix,  # pyright: ignore[reportPrivateUsage]
    _route_prefix,  # pyright: ignore[reportPrivateUsage]
)
from forze_postgres.execution.deps.configs import PostgresCounterConfig

# ----------------------- #


@pytest.mark.parametrize("suffix", [None, "", "2026", "s:already-prefixed", "with:colons"])
@pytest.mark.parametrize("route", ["orders", "invoices", "a", "route:with:colons", "42"])
def test_suffix_round_trips_under_the_route_fold(route: str, suffix: str | None) -> None:
    stored = _encode_suffix(route, suffix)

    assert stored.startswith(_route_prefix(route))
    assert _decode_suffix(route, stored) == suffix


def test_two_routes_never_produce_the_same_stored_key() -> None:
    # The silent-merge bug: without the route in the key, ("", suffix) collides across
    # specs. The length prefix makes each route's keyspace disjoint.
    seen: dict[str, tuple[str, str | None]] = {}

    for route in ("a", "ab", "abc", "orders", "orders2", "1", "12"):
        for suffix in (None, "", "x", "abc"):
            key = _encode_suffix(route, suffix)
            assert key not in seen, f"{(route, suffix)} collides with {seen.get(key)}"
            seen[key] = (route, suffix)


def test_one_routes_prefix_never_covers_another_routes_rows() -> None:
    # ``list_counters`` filters with ``starts_with(suffix, _route_prefix(route))``; a
    # route's prefix must match its own rows and no other route's.
    for route in ("a", "ab", "orders", "orders2"):
        prefix = _route_prefix(route)
        own = _encode_suffix(route, "x")
        assert own.startswith(prefix)

        for other in ("a", "ab", "orders", "orders2"):
            if other != route:
                assert not _encode_suffix(other, "x").startswith(prefix)


@pytest.mark.asyncio
async def test_namespace_tier_resolution_uses_the_bound_tenant() -> None:
    """``_table`` must resolve through ``_tenant_id_for_resolve`` — the bound tenant reaches
    a per-tenant relation resolver even without tagged ``tenant_aware``. The old getter
    (``require_tenant_if_aware``) returned ``None`` here, folding every tenant onto one
    table."""

    tenant = uuid4()
    seen_tenants: list[object] = []

    def _resolver(tid: object) -> tuple[str, str]:
        seen_tenants.append(tid)
        return ("public", f"counters_{tid}")

    adapter = PostgresCounterAdapter(
        client=object(),  # never touched — _table only resolves the name
        config=PostgresCounterConfig(relation=_resolver),
        route="orders",
        tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
    )

    qname = await adapter._table()  # pyright: ignore[reportPrivateUsage]

    assert seen_tenants == [tenant]  # the resolver saw the bound tenant, not None
    assert qname.name == f"counters_{tenant}"


@pytest.mark.asyncio
async def test_tagged_tier_missing_tenant_fails_closed() -> None:
    adapter = PostgresCounterAdapter(
        client=object(),
        config=PostgresCounterConfig(relation=("public", "counters"), tenant_aware=True),
        route="orders",
        tenant_aware=True,
        tenant_provider=lambda: None,
    )

    with pytest.raises(CoreException):
        await adapter._table()  # pyright: ignore[reportPrivateUsage]
