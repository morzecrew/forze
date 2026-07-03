"""Per-tenant durable recovery: bound recovery is scoped to a tenant; execution re-binds it.

# covers: DurableFunctionRunner.recover
# covers: DurableScheduler.tick
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from forze.application.contracts.durable.function import DurableRunStatus
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from tests.support.execution_context import context_from_modules

from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    DurableScheduler,
    resolve_durable_run_store,
)
from forze_mock import MockDepsModule, MockState

# ----------------------- #

UTC = timezone.utc


def _tenant_recording_registry(seen: dict[str, UUID | None]) -> DurableFunctionRegistry:
    registry = DurableFunctionRegistry()

    async def handler(ctx: ExecutionContext, input_json: dict | None) -> dict:
        tenant = ctx.inv_ctx.get_tenant()
        seen[(input_json or {})["run"]] = tenant.tenant_id if tenant else None
        return {"ok": True}

    registry.register("fn", handler)
    return registry


def _bind(ctx: ExecutionContext, tenant: UUID):
    return ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant))


# ....................... #


class TestPerTenantRecovery:
    async def test_bound_recovery_is_scoped_and_execution_rebinds_tenant(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        tenant_a, tenant_b = uuid4(), uuid4()
        seen: dict[str, UUID | None] = {}
        runner = DurableFunctionRunner(registry=_tenant_recording_registry(seen))
        store = resolve_durable_run_store(ctx)

        with _bind(ctx, tenant_a):
            run_a = await runner.enqueue(ctx, "fn", {"run": "a"})
        with _bind(ctx, tenant_b):
            run_b = await runner.enqueue(ctx, "fn", {"run": "b"})

        # Each run is tagged with the tenant it was enqueued under.
        assert (await store.load(run_a.run_id)).tenant_id == tenant_a  # type: ignore[union-attr]
        assert (await store.load(run_b.run_id)).tenant_id == tenant_b  # type: ignore[union-attr]

        # A scanner bound to tenant A recovers only A's run — and A's body runs under A.
        with _bind(ctx, tenant_a):
            assert await runner.recover(ctx) == 1

        assert (await store.load(run_a.run_id)).status is DurableRunStatus.COMPLETED  # type: ignore[union-attr]
        assert (await store.load(run_b.run_id)).status is DurableRunStatus.PENDING  # type: ignore[union-attr]
        assert seen == {"a": tenant_a}

        # An unbound sweep recovers B; the runner re-binds B's tenant to execute it, so B's
        # body still sees its own tenant even though the scanner was unbound.
        assert await runner.recover(ctx) == 1

        assert (await store.load(run_b.run_id)).status is DurableRunStatus.COMPLETED  # type: ignore[union-attr]
        assert seen == {"a": tenant_a, "b": tenant_b}

    async def test_scheduler_bound_to_a_tenant_fires_only_its_schedules(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        tenant_a, tenant_b = uuid4(), uuid4()
        scheduler = DurableScheduler()
        put_at = datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC)

        with _bind(ctx, tenant_a):
            await scheduler.put(ctx, "sa", "fn", "* * * * *", now=put_at)
        with _bind(ctx, tenant_b):
            await scheduler.put(ctx, "sb", "fn", "* * * * *", now=put_at)

        due = datetime(2026, 1, 1, 0, 1, 5, tzinfo=UTC)
        with _bind(ctx, tenant_a):
            assert await scheduler.tick(ctx, now=due) == 1

        # Only tenant A's schedule fired → one run, tagged with tenant A.
        runs = list(state.durable_runs.values())
        assert len(runs) == 1
        assert runs[0]["tenant_id"] == tenant_a

    async def test_idempotency_key_convergence_is_scoped_per_tenant(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        tenant_a, tenant_b = uuid4(), uuid4()
        store = resolve_durable_run_store(ctx)

        with _bind(ctx, tenant_a):
            a1 = await store.enqueue("fn", input_json={"n": 1}, idempotency_key="k")
            a2 = await store.enqueue("fn", input_json={"n": 2}, idempotency_key="k")
        with _bind(ctx, tenant_b):
            b1 = await store.enqueue("fn", input_json={"n": 3}, idempotency_key="k")

        # Same tenant + key converges; a different tenant reusing the key is its own run.
        assert a1.run_id == a2.run_id
        assert b1.run_id != a1.run_id
        assert a1.idempotency_key == "k" and b1.idempotency_key == "k"
        assert a1.tenant_id == tenant_a and b1.tenant_id == tenant_b
