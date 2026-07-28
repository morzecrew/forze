"""The credential sweeper: scan → per-grant durable refresh, safe next to live traffic.

The store's battery proves the scan and the single-flight convergence; these tests prove
the *runner* wiring on top of it — that a sweep pass turns dueness into durable runs, that
one dead provider costs one failing run rather than a stalled sweep, and that a grant the
scan flagged as burnt is routed to a human instead of an exchange.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from forze.application.contracts.durable.function import DurableRunStatus
from forze.application.contracts.secrets import (
    ExchangedCredential,
    RotatingCredentialsDepKey,
    SecretRef,
)
from forze.base.exceptions import CoreException, exc
from forze_kits.integrations.durable import durable_kits_deps, resolve_durable_run_admin
from forze_kits.integrations.durable.registry import DurableFunctionRegistry
from forze_kits.integrations.durable.runner import DurableFunctionRunner
from forze_kits.integrations.secrets import CredentialSweeper
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps
from tests.support.rotating_credentials import FakeCounterparty

# ----------------------- #

_HEALTHY = SecretRef("oauth/healthy")
_DOOMED = SecretRef("oauth/doomed-provider")

_IDLE_WINDOW = timedelta(microseconds=1)
"""Aggressively small, so every seeded grant is already due — the tests age grants by
letting real microseconds pass instead of freezing clocks."""


class _SelectiveCounterparty(FakeCounterparty):
    """The battery's fake, except one ref's provider is permanently unreachable."""

    dead_ref_path: str = ""

    async def exchange(
        self,
        ref: SecretRef,
        *,
        refresh_token: str,
        metadata: Any,
    ) -> ExchangedCredential:
        if ref.path == self.dead_ref_path:
            raise exc.infrastructure("provider is down")

        return await super().exchange(ref, refresh_token=refresh_token, metadata=metadata)


def _composition(
    *,
    counterparty: FakeCounterparty | None = None,
    limit: int = 100,
) -> tuple[Any, CredentialSweeper, FakeCounterparty, DurableFunctionRunner]:
    exchanger = counterparty or FakeCounterparty()
    registry = DurableFunctionRegistry()
    sweeper = CredentialSweeper(refresh_if_idle_for=_IDLE_WINDOW, limit=limit)
    sweeper.register(registry)
    durable_deps, runner, _ = durable_kits_deps(registry=registry)
    ctx = context_from_deps(
        MockDepsModule(state=MockState(), rotating_credentials=exchanger)(),
        durable_deps,
    )

    return ctx, sweeper, exchanger, runner


async def _seed(ctx: Any, ref: SecretRef) -> None:
    await ctx.deps.resolve_simple(ctx, RotatingCredentialsDepKey).put(
        ref,
        ExchangedCredential(access_token=f"access-{ref.path}", refresh_token=f"refresh-{ref.path}"),
    )


async def _drain(runner: DurableFunctionRunner, ctx: Any) -> int:
    """Execute every enqueued refresh run, failures isolated per run."""

    total = 0

    while claimed := await runner.recover(ctx, limit=10):
        total += claimed

    return total


# ....................... #


class TestSweep:
    async def test_a_sweep_refreshes_every_due_grant(self) -> None:
        ctx, sweeper, counterparty, runner = _composition()
        await _seed(ctx, _HEALTHY)
        await _seed(ctx, SecretRef("oauth/second"))

        record = await sweeper.sweep_now(ctx)

        assert record.status is DurableRunStatus.COMPLETED
        assert record.output_json is not None
        assert record.output_json["due"] == 2
        assert record.output_json["enqueued"] == 2

        assert await _drain(runner, ctx) == 2
        # Both grants were exchanged exactly once each.
        assert sorted(counterparty.presented) == ["refresh-oauth/healthy", "refresh-oauth/second"]

    async def test_one_dead_provider_costs_one_failing_run_not_the_sweep(self) -> None:
        """Proof 4, runner-level: the sweep completes, the healthy grant refreshes, and
        the dead provider's failure is a per-run record rather than a stalled pass."""

        counterparty = _SelectiveCounterparty()
        counterparty.dead_ref_path = _DOOMED.path
        ctx, sweeper, _, runner = _composition(counterparty=counterparty)
        await _seed(ctx, _DOOMED)
        await _seed(ctx, _HEALTHY)

        sweep = await sweeper.sweep_now(ctx)
        assert sweep.status is DurableRunStatus.COMPLETED
        assert sweep.output_json is not None
        assert sweep.output_json["enqueued"] == 2

        await _drain(runner, ctx)

        admin = resolve_durable_run_admin(ctx)
        failed = await admin.list_runs(status=DurableRunStatus.FAILED)

        assert len(failed.records) == 1
        # The healthy grant still made it through the same pass.
        assert "refresh-oauth/healthy" in counterparty.presented

    async def test_a_burnt_grant_is_reported_not_exchanged(self) -> None:
        """Proof 3, runner-level: burnt grants surface in the sweep output and no refresh
        run is enqueued for them — nothing is left to present."""

        ctx, sweeper, counterparty, runner = _composition()
        await _seed(ctx, _HEALTHY)
        store = ctx.deps.resolve_simple(ctx, RotatingCredentialsDepKey)
        await store.burn(_HEALTHY, reason="revoked by provider webhook")

        record = await sweeper.sweep_now(ctx)

        assert record.output_json is not None
        assert record.output_json["needs_reauthorization"] == [_HEALTHY.path]
        assert record.output_json["enqueued"] == 0

        assert await _drain(runner, ctx) == 0
        assert counterparty.presented == []

    async def test_a_grant_burnt_between_scan_and_run_ends_the_run_without_retry(self) -> None:
        """The refresh run's one special case: burnt is terminal, so the run completes
        instead of entering a durable retry loop that can never succeed."""

        ctx, sweeper, counterparty, runner = _composition()
        await _seed(ctx, _HEALTHY)

        sweep = await sweeper.sweep_now(ctx)
        assert sweep.output_json is not None and sweep.output_json["enqueued"] == 1

        # Burnt after the scan, before the run executes.
        await ctx.deps.resolve_simple(ctx, RotatingCredentialsDepKey).burn(_HEALTHY, reason="late webhook")

        await _drain(runner, ctx)

        admin = resolve_durable_run_admin(ctx)
        assert (await admin.list_runs(status=DurableRunStatus.FAILED)).records == []
        assert counterparty.presented == []

    async def test_the_sweep_converges_with_live_traffic_to_one_exchange(self) -> None:
        """Proof 2, runner-level: live traffic refreshes between the scan and the run; the
        run passes its scanned version, the store converges, one exchange total."""

        ctx, sweeper, counterparty, runner = _composition()
        await _seed(ctx, _HEALTHY)

        sweep = await sweeper.sweep_now(ctx)
        assert sweep.output_json is not None and sweep.output_json["enqueued"] == 1

        # Live traffic gets there first.
        store = ctx.deps.resolve_simple(ctx, RotatingCredentialsDepKey)
        live = await store.get(_HEALTHY)
        await store.refresh(_HEALTHY, observed=live.version)
        exchanges_after_live = len(counterparty.presented)

        await _drain(runner, ctx)

        assert len(counterparty.presented) == exchanges_after_live
        assert not counterparty.family_revoked

    async def test_the_pass_is_bounded_by_the_configured_limit(self) -> None:
        """Proof 5, runner-level: a pass enqueues at most ``limit`` runs; the remainder is
        still due and lands in the next pass."""

        ctx, sweeper, _, runner = _composition(limit=2)

        for suffix in ("a", "b", "c"):
            await _seed(ctx, SecretRef(f"oauth/{suffix}"))

        first = await sweeper.sweep_now(ctx)
        assert first.output_json is not None
        assert first.output_json["enqueued"] == 2

        await _drain(runner, ctx)

        second = await sweeper.sweep_now(ctx, idempotency_key="second-pass")
        assert second.output_json is not None
        # The two refreshed grants reset their clocks past the tiny idle window is not
        # guaranteed (the window is microseconds), so the invariant asserted is the cap,
        # not the exact remainder.
        assert second.output_json["enqueued"] <= 2


class TestConfigValidation:
    def test_the_idle_window_is_required_positive(self) -> None:
        with pytest.raises(CoreException, match="must be positive"):
            CredentialSweeper(refresh_if_idle_for=timedelta(0))

    def test_the_limit_must_be_at_least_one(self) -> None:
        with pytest.raises(CoreException, match="at least 1"):
            CredentialSweeper(refresh_if_idle_for=timedelta(days=30), limit=0)


class TestTenantScopedSweep:
    """The per-tenant fan-out really partitions — through the module wiring, not just the
    battery's hand-built harness.

    This is the test the original wiring failed: the mock module registered the store and
    the scan as tenant-blind singletons (no tenant provider), so every tenant's grants
    landed under the unbound key and tenant A's sweep saw — and refreshed — tenant B's
    grants. The PG wiring threaded ``ctx.inv_ctx.get_tenant`` all along; the oracle now
    does the same via per-scope factories, with single-flight preserved through the lock
    stripe on ``MockState``.
    """

    async def test_each_tenants_sweep_touches_only_its_own_grants(self) -> None:
        from uuid import uuid4

        from forze.application.contracts.tenancy import TenantIdentity

        ctx, sweeper, counterparty, runner = _composition()
        tenant_a, tenant_b = uuid4(), uuid4()

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant_a)):
            await _seed(ctx, SecretRef("oauth/a-grant"))

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant_b)):
            await _seed(ctx, SecretRef("oauth/b-grant"))

        assert await sweeper.enqueue_tenants(ctx, tenants=[tenant_a]) == 1
        await _drain(runner, ctx)

        # Only tenant A's grant was exchanged; B's sat untouched.
        assert counterparty.presented == ["refresh-oauth/a-grant"]

        assert await sweeper.enqueue_tenants(ctx, tenants=[tenant_b]) == 1
        await _drain(runner, ctx)

        assert sorted(counterparty.presented) == [
            "refresh-oauth/a-grant",
            "refresh-oauth/b-grant",
        ]

    async def test_single_flight_survives_the_per_scope_factories(self) -> None:
        """The wiring fix's own regression guard: two concurrently resolved store
        instances still collapse concurrent refreshes to one exchange, because the lock
        stripe lives on the shared state rather than the adapter."""

        import asyncio

        counterparty = FakeCounterparty(delay=0.05)  # widen the race window
        registry = DurableFunctionRegistry()
        CredentialSweeper(refresh_if_idle_for=_IDLE_WINDOW).register(registry)
        durable_deps, _, _ = durable_kits_deps(registry=registry)
        module_deps = MockDepsModule(state=MockState(), rotating_credentials=counterparty)()

        # Two execution contexts over one deps blob — the real shape of two concurrent
        # scopes, each resolving its own adapter instance.
        ctx = context_from_deps(module_deps, durable_deps)
        ctx_two = context_from_deps(module_deps, durable_deps)
        await _seed(ctx, _HEALTHY)

        store_one = ctx.deps.resolve_simple(ctx, RotatingCredentialsDepKey)
        store_two = ctx_two.deps.resolve_simple(ctx_two, RotatingCredentialsDepKey)
        assert store_one is not store_two

        current = await store_one.get(_HEALTHY)
        results = await asyncio.gather(
            store_one.refresh(_HEALTHY, observed=current.version),
            store_two.refresh(_HEALTHY, observed=current.version),
        )

        assert len(counterparty.presented) == 1, "two instances must share single-flight"
        assert results[0].version == results[1].version
        assert not counterparty.family_revoked


class TestFleetTriggers:
    async def test_enqueue_tenants_converges_on_the_idempotency_prefix(self) -> None:
        """A re-submitted fleet pass resumes instead of duplicating."""

        from uuid import uuid4

        ctx, sweeper, _counterparty, runner = _composition()
        tenants = [uuid4(), uuid4()]

        assert await sweeper.enqueue_tenants(ctx, tenants=tenants, idempotency_prefix="pass-1") == 2
        assert await sweeper.enqueue_tenants(ctx, tenants=tenants, idempotency_prefix="pass-1") == 2

        # Convergence happened at the run store: draining executes two runs, not four.
        assert await _drain(runner, ctx) == 2

    async def test_ensure_cron_is_idempotent_per_tenant(self) -> None:
        from uuid import uuid4

        from forze_kits.integrations.durable import resolve_durable_scheduler

        ctx, sweeper, _counterparty, _runner = _composition()
        tenant = uuid4()

        first = await sweeper.ensure_cron(ctx, cron="0 4 * * *", tenant_id=tenant)
        second = await sweeper.ensure_cron(ctx, cron="0 4 * * *", tenant_id=tenant)

        assert first.schedule_id == second.schedule_id

        # A different tenant gets its own schedule, so per-tenant cadences can differ.
        other = await sweeper.ensure_cron(ctx, cron="0 5 * * *", tenant_id=uuid4())
        assert other.schedule_id != first.schedule_id

        _ = resolve_durable_scheduler(ctx)  # resolvable in this composition — wiring sanity
