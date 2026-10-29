"""Unit tests for :func:`postgres_counter_purge_lifecycle_step` over fake admin ports.

# covers: postgres_counter_purge_lifecycle_step
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.counter import CounterAdminDepKey, CounterSpec
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.testing import context_from_deps
from forze_postgres import POSTGRES_CLIENT_CAPABILITY, postgres_counter_purge_lifecycle_step

pytestmark = pytest.mark.unit

# ----------------------- #

ORDERS = CounterSpec(name="orders")
LOCKOUT = CounterSpec(name="authn_lockout")


class _Purger:
    """Stands in for ``PostgresCounterAdminAdapter``: records each purge and its tenant."""

    def __init__(
        self,
        ctx: ExecutionContext,
        route: str,
        calls: list[tuple[str, UUID | None]],
        *,
        tenant_aware: bool = False,
        fail_for: UUID | None = None,
    ) -> None:
        self.ctx = ctx
        self.route = route
        self.calls = calls
        self.tenant_aware = tenant_aware
        self.fail_for = fail_for

    async def purge_expired(self) -> int:
        tenant = self.ctx.inv_ctx.get_tenant()
        tenant_id = tenant.tenant_id if tenant is not None else None

        if self.fail_for is not None and tenant_id == self.fail_for:
            raise exc.infrastructure("database unavailable")

        self.calls.append((self.route, tenant_id))
        return 1


def _context(
    calls: list[tuple[str, UUID | None]],
    *,
    tenant_aware: bool = False,
    fail_for: UUID | None = None,
) -> ExecutionContext:
    return context_from_deps(
        Deps.routed(
            {
                CounterAdminDepKey: {
                    str(spec.name): (
                        lambda ctx, spec: _Purger(
                            ctx,
                            str(spec.name),
                            calls,
                            tenant_aware=tenant_aware,
                            fail_for=fail_for,
                        )
                    )
                    for spec in (ORDERS, LOCKOUT)
                },
            }
        )
    )


# ....................... #


class TestFactory:
    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"specs": []}, "at least one counter spec"),
            ({"specs": [ORDERS], "interval": timedelta(0)}, "interval must be positive"),
            ({"specs": [ORDERS], "jitter": 1.0}, "Jitter"),
        ],
        ids=["no-specs", "zero-interval", "jitter-out-of-range"],
    )
    def test_refuses_a_bad_configuration(self, kwargs: dict[str, object], match: str) -> None:
        with pytest.raises(CoreException, match=match) as error:
            postgres_counter_purge_lifecycle_step(**kwargs)  # type: ignore[arg-type]

        assert error.value.kind is ExceptionKind.CONFIGURATION

    def test_runs_after_the_pool_in_a_long_running_process(self) -> None:
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS])

        assert step.id == "postgres_counter_purge"
        assert step.requires == (POSTGRES_CLIENT_CAPABILITY,)
        assert step.requires_long_running is True


class TestStartup:
    async def test_the_loop_purges_every_route_and_stops_on_shutdown(self) -> None:
        calls: list[tuple[str, UUID | None]] = []
        ctx = _context(calls)
        step = postgres_counter_purge_lifecycle_step(
            specs=[ORDERS, LOCKOUT], interval=timedelta(hours=1)
        )

        await step.startup(ctx)

        try:
            async with asyncio.timeout(5):
                while len(calls) < 2:
                    await asyncio.sleep(0)
        finally:
            await step.shutdown(ctx)

        assert calls == [("orders", None), ("authn_lockout", None)]
        assert step.startup.task is not None and step.startup.task.done()  # type: ignore[attr-defined]

    async def test_a_route_that_is_not_a_postgres_counter_is_refused(self) -> None:
        ctx = context_from_deps(
            Deps.routed({CounterAdminDepKey: {"orders": lambda _ctx, _spec: object()}})
        )
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS])

        with pytest.raises(CoreException, match="not a Postgres counter") as error:
            await step.startup(ctx)

        assert error.value.kind is ExceptionKind.CONFIGURATION
        assert step.startup.task is None  # type: ignore[attr-defined]

    async def test_without_tenants_a_tenant_aware_route_is_refused(self) -> None:
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS])

        with pytest.raises(CoreException) as error:
            await step.startup(_context([], tenant_aware=True))

        assert error.value.code == "tenant_global_required"


class TestTick:
    async def test_each_tenant_is_bound_in_turn(self) -> None:
        calls: list[tuple[str, UUID | None]] = []
        ctx = _context(calls, tenant_aware=True)
        t1, t2 = uuid4(), uuid4()
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS], tenants=lambda: [t1, t2])

        await step.startup._purge_tick(ctx, [t1, t2])  # type: ignore[attr-defined]

        assert calls == [("orders", t1), ("orders", t2)]

    async def test_one_tenant_failing_does_not_stop_the_others(self) -> None:
        calls: list[tuple[str, UUID | None]] = []
        t1, t2 = uuid4(), uuid4()
        ctx = _context(calls, fail_for=t1)
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS], tenants=lambda: [t1, t2])

        await step.startup._purge_tick(ctx, [t1, t2])  # type: ignore[attr-defined]

        assert calls == [("orders", t2)]

    async def test_a_configuration_error_ends_the_tick(self) -> None:
        ctx = context_from_deps(
            Deps.routed({CounterAdminDepKey: {"orders": lambda _ctx, _spec: object()}})
        )
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS], tenants=lambda: [uuid4()])

        with pytest.raises(CoreException) as error:
            await step.startup._purge_tick(ctx, [uuid4()])  # type: ignore[attr-defined]

        assert error.value.kind is ExceptionKind.CONFIGURATION
