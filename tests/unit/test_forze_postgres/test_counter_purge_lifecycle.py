"""Unit tests for :func:`postgres_counter_purge_lifecycle_step`.

The admin adapters are real, so the step's tenancy checks read real wiring; only the
``purge_expired`` statement is replaced, to record which route and tenant each purge ran for.

# covers: postgres_counter_purge_lifecycle_step
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.counter import CounterAdminDepKey, CounterSpec
from forze.application.contracts.resolution import RelationSpec
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.testing import context_from_deps
from forze_postgres import (
    POSTGRES_CLIENT_CAPABILITY,
    PostgresClient,
    postgres_counter_purge_lifecycle_step,
)
from forze_postgres.adapters.counter import PostgresCounterAdminAdapter
from forze_postgres.execution.deps.configs import PostgresCounterConfig
from forze_postgres.execution.lifecycle import counter_purge as counter_purge_module

pytestmark = pytest.mark.unit

# ----------------------- #

ORDERS = CounterSpec(name="orders")
LOCKOUT = CounterSpec(name="authn_lockout")


@attrs.define
class _Purges:
    calls: list[tuple[str, UUID | None]] = attrs.field(factory=list)
    fail_for: UUID | None = None
    fail_route: str | None = None
    error: BaseException = attrs.field(factory=lambda: exc.infrastructure("database unavailable"))
    purged: int = 1


@pytest.fixture
def purges(monkeypatch: pytest.MonkeyPatch) -> _Purges:
    record = _Purges()

    async def purge_expired(self: PostgresCounterAdminAdapter) -> int:
        tenant = self.tenant_provider() if self.tenant_provider is not None else None
        tenant_id = tenant.tenant_id if tenant is not None else None

        if (tenant_id is not None and tenant_id == record.fail_for) or (
            self.route == record.fail_route
        ):
            raise record.error

        record.calls.append((self.route, tenant_id))
        return record.purged

    monkeypatch.setattr(PostgresCounterAdminAdapter, "purge_expired", purge_expired)
    return record


def _per_tenant_table(tenant_id: object) -> tuple[str, str]:
    return ("public", f"counters_{tenant_id}")


def _context(
    *, tenant_aware: bool = False, relation: RelationSpec = ("public", "counters")
) -> ExecutionContext:
    def build(ctx: ExecutionContext, spec: CounterSpec) -> PostgresCounterAdminAdapter:
        return PostgresCounterAdminAdapter(
            client=PostgresClient(),
            config=PostgresCounterConfig(relation=relation),
            route=str(spec.name),
            tenant_aware=tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )

    return context_from_deps(
        Deps.routed({CounterAdminDepKey: {str(spec.name): build for spec in (ORDERS, LOCKOUT)}})
    )


def _misrouted() -> ExecutionContext:
    return context_from_deps(
        Deps.routed({CounterAdminDepKey: {"orders": lambda _ctx, _spec: object()}})
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
    async def test_the_loop_purges_every_route_and_stops_on_shutdown(self, purges: _Purges) -> None:
        ctx = _context()
        step = postgres_counter_purge_lifecycle_step(
            specs=[ORDERS, LOCKOUT], interval=timedelta(hours=1)
        )

        await step.startup(ctx)

        try:
            async with asyncio.timeout(5):
                while len(purges.calls) < 2:
                    await asyncio.sleep(0)
        finally:
            await step.shutdown(ctx)

        assert purges.calls == [("orders", None), ("authn_lockout", None)]
        assert step.startup.task is not None and step.startup.task.done()  # type: ignore[attr-defined]

    async def test_a_second_startup_is_ignored(self, purges: _Purges) -> None:
        ctx = _context()
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS], interval=timedelta(hours=1))

        await step.startup(ctx)
        task = step.startup.task  # type: ignore[attr-defined]

        try:
            await step.startup(ctx)

            assert step.startup.task is task  # type: ignore[attr-defined]
            name = step.startup.loop_name  # type: ignore[attr-defined]
            assert name == "postgres_counter_purge"
        finally:
            await step.shutdown(ctx)

    async def test_a_route_that_is_not_a_postgres_counter_is_refused(self) -> None:
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS])

        with pytest.raises(CoreException, match="not a Postgres counter") as error:
            await step.startup(_misrouted())

        assert error.value.kind is ExceptionKind.CONFIGURATION
        assert step.startup.task is None  # type: ignore[attr-defined]

    async def test_without_tenants_a_tenant_aware_route_is_refused(self) -> None:
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS])

        with pytest.raises(CoreException) as error:
            await step.startup(_context(tenant_aware=True))

        assert error.value.code == "tenant_global_required"

    async def test_with_tenants_a_tenant_global_route_is_refused(self) -> None:
        # Each tenant's purge would walk the same untagged rows again.
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS], tenants=lambda: [uuid4()])

        with pytest.raises(CoreException, match="tenant-global") as error:
            await step.startup(_context())

        assert error.value.kind is ExceptionKind.CONFIGURATION
        assert step.startup.task is None  # type: ignore[attr-defined]

    async def test_with_tenants_a_table_per_tenant_route_is_accepted(self, purges: _Purges) -> None:
        # Not tenant_aware, yet per tenant: its table resolves per tenant.
        tenant = uuid4()
        ctx = _context(relation=_per_tenant_table)
        step = postgres_counter_purge_lifecycle_step(
            specs=[ORDERS], tenants=lambda: [tenant], interval=timedelta(hours=1)
        )

        await step.startup(ctx)

        try:
            async with asyncio.timeout(5):
                while not purges.calls:
                    await asyncio.sleep(0)
        finally:
            await step.shutdown(ctx)

        assert purges.calls == [("orders", tenant)]

    async def test_an_empty_shard_is_accepted_and_reported(
        self, purges: _Purges, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # More replicas than tenants leaves a shard empty; it must not fail startup.
        warnings: list[str] = []
        monkeypatch.setattr(
            counter_purge_module,
            "logger",
            SimpleNamespace(
                warning=lambda event, **_extras: warnings.append(event),
                info=lambda *_args, **_extras: None,
                exception=lambda *_args, **_extras: None,
            ),
        )
        ctx = _context(tenant_aware=True)
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS], tenants=lambda: [])

        await step.startup(ctx)
        await step.shutdown(ctx)

        assert purges.calls == []
        assert len(warnings) == 1 and "no tenants" in warnings[0]


class TestTick:
    async def test_each_tenant_is_bound_in_turn(self, purges: _Purges) -> None:
        ctx = _context(tenant_aware=True)
        t1, t2 = uuid4(), uuid4()
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS], tenants=lambda: [t1, t2])

        await step.startup._purge_tick(ctx, [t1, t2])  # type: ignore[attr-defined]

        assert purges.calls == [("orders", t1), ("orders", t2)]

    async def test_one_tenant_failing_does_not_stop_the_others(self, purges: _Purges) -> None:
        t1, t2 = uuid4(), uuid4()
        purges.fail_for = t1
        ctx = _context(tenant_aware=True)
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS], tenants=lambda: [t1, t2])

        await step.startup._purge_tick(ctx, [t1, t2])  # type: ignore[attr-defined]

        assert purges.calls == [("orders", t2)]

    @pytest.mark.parametrize("per_tenant", [False, True], ids=["unbound", "per-tenant"])
    async def test_one_route_failing_does_not_stop_the_others(
        self, purges: _Purges, per_tenant: bool
    ) -> None:
        purges.fail_route = str(ORDERS.name)
        tenants = [uuid4(), uuid4()] if per_tenant else None
        ctx = _context(tenant_aware=per_tenant)
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS, LOCKOUT])

        await step.startup._purge_tick(ctx, tenants)  # type: ignore[attr-defined]

        assert purges.calls == [("authn_lockout", tenant) for tenant in tenants or [None]]

    async def test_a_cancelled_purge_is_not_swallowed(self, purges: _Purges) -> None:
        purges.fail_route, purges.error = str(ORDERS.name), asyncio.CancelledError()
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS, LOCKOUT])

        with pytest.raises(asyncio.CancelledError):
            await step.startup._purge_tick(_context(), None)  # type: ignore[attr-defined]

        assert purges.calls == []

    @pytest.mark.parametrize(("purged", "logged"), [(1, 1), (0, 0)])
    async def test_only_a_purge_that_deleted_rows_is_logged(
        self, purges: _Purges, monkeypatch: pytest.MonkeyPatch, purged: int, logged: int
    ) -> None:
        infos: list[str] = []
        monkeypatch.setattr(
            counter_purge_module,
            "logger",
            SimpleNamespace(info=lambda event, **_extras: infos.append(event)),
        )
        purges.purged = purged
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS])

        await step.startup._purge_tick(_context(), None)  # type: ignore[attr-defined]

        assert len(infos) == logged

    async def test_a_configuration_error_ends_the_tick(self) -> None:
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS], tenants=lambda: [uuid4()])

        with pytest.raises(CoreException) as error:
            await step.startup._purge_tick(_misrouted(), [uuid4()])  # type: ignore[attr-defined]

        assert error.value.kind is ExceptionKind.CONFIGURATION


class TestLoop:
    async def test_a_failed_tick_is_retried_and_a_configuration_error_stops_the_loop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        outcomes = iter(
            [exc.infrastructure("down"), RuntimeError("bug"), exc.configuration("miswired")]
        )
        step = postgres_counter_purge_lifecycle_step(
            specs=[ORDERS], interval=timedelta(milliseconds=1), jitter=0.0
        )

        async def tick(*_args: object) -> None:
            raise next(outcomes)

        monkeypatch.setattr(type(step.startup), "_purge_tick", tick)
        await step.startup(_context())

        async with asyncio.timeout(5):
            await step.startup.task  # type: ignore[attr-defined]  # ends without a shutdown

        assert next(outcomes, None) is None, "every tick ran"

    async def test_a_cancelled_tick_ends_the_loop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        entered = asyncio.Event()
        step = postgres_counter_purge_lifecycle_step(specs=[ORDERS])

        async def tick(*_args: object) -> None:
            entered.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(type(step.startup), "_purge_tick", tick)
        await step.startup(_context())
        await entered.wait()
        step.startup.task.cancel()  # type: ignore[attr-defined]

        with pytest.raises(asyncio.CancelledError):
            await step.startup.task  # type: ignore[attr-defined]
