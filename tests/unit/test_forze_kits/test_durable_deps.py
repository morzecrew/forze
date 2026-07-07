"""Container wiring for the durable runner + scheduler singletons.

# covers: DurableKitsDepsModule
# covers: durable_kits_deps
# covers: resolve_durable_runner
# covers: resolve_durable_scheduler
"""

from __future__ import annotations

from datetime import timedelta

from forze.testing import context_from_deps, context_from_modules

from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    DurableKitsDepsModule,
    DurableScheduler,
    durable_kits_deps,
    resolve_durable_runner,
    resolve_durable_scheduler,
)

# ----------------------- #


class TestDurableKitsDepsModule:
    def test_registers_prebuilt_runner_and_scheduler(self) -> None:
        runner = DurableFunctionRunner(registry=DurableFunctionRegistry())
        scheduler = DurableScheduler()

        ctx = context_from_modules(
            DurableKitsDepsModule(runner=runner, scheduler=scheduler)
        )

        assert resolve_durable_runner(ctx) is runner
        assert resolve_durable_scheduler(ctx) is scheduler


class TestDurableKitsDeps:
    def test_builds_and_registers_in_one_call(self) -> None:
        registry = DurableFunctionRegistry()

        deps, runner, scheduler = durable_kits_deps(registry=registry)

        # The returned instances are exactly what the deps register (so the same objects can
        # also be handed to the recovery / scheduler lifecycle steps).
        ctx = context_from_deps(deps)
        assert resolve_durable_runner(ctx) is runner
        assert resolve_durable_scheduler(ctx) is scheduler

    def test_forwards_runner_configuration(self) -> None:
        registry = DurableFunctionRegistry()

        _, runner, _ = durable_kits_deps(
            registry=registry,
            lease_for=timedelta(seconds=90),
            heartbeat_divisor=4,
        )

        assert runner.registry is registry
        assert runner.lease_for == timedelta(seconds=90)
        assert runner.heartbeat_divisor == 4
