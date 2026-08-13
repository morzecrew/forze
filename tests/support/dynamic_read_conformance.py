"""Shared ``DynamicReadPort`` conformance battery: the governance shell, on every engine.

This plane splits cleanly in two, and the split is the reason the battery is scoped the way it
is. Half of the contract is refusals only a *database* can make — a write rejected by a
read-only transaction, a second command rejected by the wire protocol, a cross-schema read
rejected by role grants. The mock cannot detect a write in a string and does not pretend to, so
those live in the real-Postgres suite and are named there.

The other half is the governance shell: the byte cap, the row cap and its refuse-don't-truncate
rule, per-call clamping that only ever tightens, fail-closed tenancy, the advisory tenant
parameter, and row validation on ``select``. That half is shared code, and shared code is
exactly what a differential must pin — because "the mock agrees" is the claim the whole DST
oracle rests on, and a shell that behaved differently on one engine would quietly invalidate
every proof run against the other.

What each check pins:

1. A result exactly at the cap comes back whole — the cap is a ceiling, not an off-by-one.
2. One row over the cap **raises**. No truncated page is ever returned: a short page reads as a
   complete one, and a dashboard rendered from it is confidently wrong.
3. A per-call ``row_cap`` clamps down.
4. A per-call ``row_cap`` cannot clamp *up* — the wiring's ceiling is not a suggestion.
5. An oversized statement is refused before any connection is touched.
6. An empty statement is refused.
7. A tenant-aware route with no bound tenant fails closed, ahead of the engine.
8. Two tenants running the identical statement read disjoint containers — the namespace proof,
   and the reason this plane is admissible on the namespace tier at all.
9. A referenced tenant placeholder is bound to the bound tenant.
10. A statement that never mentions the placeholder still runs — the parameter is advisory
    convenience, and the container remains the boundary.
11. ``select`` validates rows as the requested type, and says so when they do not match.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.dynamic_read import DynamicReadPort, DynamicReadSpec
from forze.application.contracts.tenancy import TenantProviderPort
from forze.application.integrations.dynamic_read import (
    ROW_CAP_EXCEEDED_CODE,
    STATEMENT_TOO_LARGE_CODE,
)
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #

ROUTE = "battery_dynamic_read"
"""Route name every check uses; isolation comes from per-check containers instead."""


class Row(BaseModel):
    """The seeded row shape — one integer column named ``n``."""

    n: int


class WrongRow(BaseModel):
    """A shape the seeded rows do not satisfy, for the ``select`` mismatch check."""

    missing: str


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DynamicReadHarness:
    """One engine's seam for the battery."""

    backend: str
    """Label used in assertion messages, so a failure names the engine that disagreed."""

    build: Callable[[DynamicReadSpec, TenantProviderPort | None, bool], DynamicReadPort]
    """``(spec, tenant_provider, tenant_aware) -> port``.

    The route is always wired with per-tenant namespace routing, because that is the tier this
    plane is admissible on; ``tenant_aware`` toggles the tagged-tier *flag* the fail-closed
    check turns on.
    """

    seed: Callable[[UUID | None, int], Awaitable[None]]
    """``(tenant, count) -> None``: put rows ``0 … count-1`` in that tenant's container.

    Called more than once per tenant in a check, so it must **replace** rather than append —
    otherwise check 1 leaves rows behind and check 2 passes for the wrong reason.
    """

    rows_statement: str
    """Selects every row in the current container, ordered, as a single column ``n``."""

    tenant_statement: str
    """Selects the bound tenant id as a single column ``t``, via the tenant placeholder."""


Check = Callable[[DynamicReadHarness], Any]
"""One battery check."""


# ....................... #


def _spec(**overrides: Any) -> DynamicReadSpec:
    return DynamicReadSpec(name=ROUTE, **overrides)


def _tenant_provider(tenant_id: UUID | None) -> TenantProviderPort:
    from forze.application.contracts.tenancy import TenantIdentity

    def provider() -> TenantIdentity | None:
        return None if tenant_id is None else TenantIdentity(tenant_id=tenant_id)

    return provider


def _port(
    harness: DynamicReadHarness,
    spec: DynamicReadSpec,
    *,
    tenant: UUID | None = None,
    tenant_aware: bool = False,
) -> DynamicReadPort:
    return harness.build(spec, _tenant_provider(tenant), tenant_aware)


# ....................... #


async def check_a_result_at_the_cap_returns_whole(h: DynamicReadHarness) -> None:
    """Exactly ``row_cap`` rows is a legal answer, not a boundary failure."""

    await h.seed(None, 5)
    rows = await _port(h, _spec(row_cap=5)).run(h.rows_statement)

    assert [row["n"] for row in rows] == [0, 1, 2, 3, 4], h.backend


async def check_one_row_over_the_cap_is_refused(h: DynamicReadHarness) -> None:
    """The cap raises rather than truncating — the plane's loudest promise."""

    await h.seed(None, 6)

    with pytest.raises(CoreException) as ei:
        await _port(h, _spec(row_cap=5)).run(h.rows_statement)

    assert ei.value.code == ROW_CAP_EXCEEDED_CODE, h.backend
    assert ei.value.kind == ExceptionKind.PRECONDITION, h.backend


async def check_a_call_option_clamps_the_cap_down(h: DynamicReadHarness) -> None:
    """A caller may tighten its own ceiling for one call."""

    await h.seed(None, 4)
    port = _port(h, _spec(row_cap=10))

    assert len(await port.run(h.rows_statement, options={"row_cap": 4})) == 4, h.backend

    with pytest.raises(CoreException) as ei:
        await port.run(h.rows_statement, options={"row_cap": 3})

    assert ei.value.code == ROW_CAP_EXCEEDED_CODE, h.backend


async def check_a_call_option_cannot_raise_the_cap(h: DynamicReadHarness) -> None:
    """The wiring's ceiling is not negotiable from the call site.

    The direction that matters: a call able to raise its own cap would let a caller undo the
    limit a reviewer approved, which is the whole point of the limit living in the wiring.
    """

    await h.seed(None, 6)

    with pytest.raises(CoreException) as ei:
        await _port(h, _spec(row_cap=5)).run(h.rows_statement, options={"row_cap": 1_000})

    assert ei.value.code == ROW_CAP_EXCEEDED_CODE, h.backend


async def check_an_oversized_statement_is_refused(h: DynamicReadHarness) -> None:
    """A statement over the byte cap never reaches the engine."""

    await h.seed(None, 1)

    with pytest.raises(CoreException) as ei:
        await _port(h, _spec(max_statement_bytes=4)).run(h.rows_statement)

    assert ei.value.code == STATEMENT_TOO_LARGE_CODE, h.backend
    assert ei.value.kind == ExceptionKind.VALIDATION, h.backend


async def check_an_empty_statement_is_refused(h: DynamicReadHarness) -> None:
    """Whitespace is not a statement."""

    with pytest.raises(CoreException) as ei:
        await _port(h, _spec()).run("   \n  ")

    assert ei.value.kind == ExceptionKind.VALIDATION, h.backend


async def check_a_tenant_aware_route_without_a_tenant_fails_closed(
    h: DynamicReadHarness,
) -> None:
    """No bound tenant on a tenant-aware route is a refusal, not an unscoped read."""

    with pytest.raises(CoreException) as ei:
        await _port(h, _spec(), tenant=None, tenant_aware=True).run(h.rows_statement)

    assert ei.value.code == "tenant_required", h.backend
    assert ei.value.kind == ExceptionKind.AUTHENTICATION, h.backend


async def check_two_tenants_read_disjoint_containers(h: DynamicReadHarness) -> None:
    """The namespace proof: same statement, different container, disjoint rows.

    This is the property that makes the namespace tier admissible for statements nobody
    reviewed. The statement carries no tenancy of its own — it does not have to, because the
    container it runs in decides what it can see.
    """

    first, second = uuid4(), uuid4()

    await h.seed(first, 2)
    await h.seed(second, 4)

    spec = _spec(row_cap=100)
    first_rows = await _port(h, spec, tenant=first, tenant_aware=True).run(h.rows_statement)
    second_rows = await _port(h, spec, tenant=second, tenant_aware=True).run(h.rows_statement)

    assert [row["n"] for row in first_rows] == [0, 1], h.backend
    assert [row["n"] for row in second_rows] == [0, 1, 2, 3], h.backend


async def check_a_referenced_tenant_parameter_is_bound(h: DynamicReadHarness) -> None:
    """A statement that mentions the placeholder gets the bound tenant, as a string."""

    tenant = uuid4()
    await h.seed(tenant, 1)

    rows = await _port(h, _spec(), tenant=tenant, tenant_aware=True).run(h.tenant_statement)

    assert [str(row["t"]) for row in rows] == [str(tenant)], h.backend


async def check_a_statement_ignoring_the_placeholder_still_runs(h: DynamicReadHarness) -> None:
    """The tenant parameter is advisory; the container is the boundary.

    A statement that never mentions it is *not* an error — refusing one would be a promise the
    framework cannot keep anyway, since referencing the placeholder proves reference and never
    scope.
    """

    tenant = uuid4()
    await h.seed(tenant, 3)

    rows = await _port(h, _spec(), tenant=tenant, tenant_aware=True).run(h.rows_statement)

    assert [row["n"] for row in rows] == [0, 1, 2], h.backend


async def check_select_validates_rows_as_the_return_type(h: DynamicReadHarness) -> None:
    """``select`` types the rows at the call site, and refuses a shape they do not fit."""

    await h.seed(None, 2)
    port = _port(h, _spec())

    typed = await port.select(Row, h.rows_statement)

    assert [row.n for row in typed] == [0, 1], h.backend

    with pytest.raises(CoreException) as ei:
        await port.select(WrongRow, h.rows_statement)

    assert ei.value.code == "dynamic_read_row_type_mismatch", h.backend
    assert ei.value.kind == ExceptionKind.VALIDATION, h.backend


# ....................... #

DYNAMIC_READ_BATTERY: tuple[Check, ...] = (
    check_a_result_at_the_cap_returns_whole,
    check_one_row_over_the_cap_is_refused,
    check_a_call_option_clamps_the_cap_down,
    check_a_call_option_cannot_raise_the_cap,
    check_an_oversized_statement_is_refused,
    check_an_empty_statement_is_refused,
    check_a_tenant_aware_route_without_a_tenant_fails_closed,
    check_two_tenants_read_disjoint_containers,
    check_a_referenced_tenant_parameter_is_bound,
    check_a_statement_ignoring_the_placeholder_still_runs,
    check_select_validates_rows_as_the_return_type,
)
