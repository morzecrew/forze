"""The durable rotator: step ordering, verify-before-promote, crash resume, no leaks."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest

from forze.application.contracts.durable.function import (
    DurableRunContext,
    DurableRunStatus,
    bind_durable_run,
    reset_durable_run,
)
from forze.application.contracts.pubsub import PubSubQueryDepKey
from forze.application.contracts.secrets import (
    PendingCredential,
    SecretRef,
    SecretsAdminDepKey,
    SecretsDepKey,
)
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze_kits.integrations.durable import (
    durable_kits_deps,
    resolve_durable_run_admin,
    resolve_durable_run_store,
)
from forze_kits.integrations.durable.registry import DurableFunctionRegistry
from forze_kits.integrations.outbox import OutboxRelay
from forze_kits.integrations.secrets import (
    PubSubSecretsChangeSource,
    SecretRotator,
    secret_rotated_outbox_spec,
    secret_rotated_pubsub_spec,
)
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps

# ----------------------- #

_REF = SecretRef("db/dsn")


class _RecordingTarget:
    """Rotation target double: counts calls, optionally fails verification."""

    def __init__(self, *, fail_verify: int = 0) -> None:
        self.calls: list[str] = []
        self.fail_verify = fail_verify
        self.minted: list[str] = []

    async def compose(self, tenant_id: UUID | None, *, current: str, minted: str) -> str:
        self.calls.append("compose")
        self.minted.append(minted)
        return f"dsn-for-{minted}"

    async def apply(self, tenant_id: UUID | None, pending: PendingCredential) -> None:
        self.calls.append("apply")

    async def verify(self, tenant_id: UUID | None, pending: PendingCredential) -> None:
        self.calls.append("verify")

        if self.fail_verify > 0:
            self.fail_verify -= 1
            raise RuntimeError("backend refused the pending credential")


def _composition(
    target: _RecordingTarget,
    *,
    state: MockState | None = None,
    publish: bool = True,
) -> tuple[Any, SecretRotator, MockState]:
    state = state or MockState()
    registry = DurableFunctionRegistry()
    rotator = SecretRotator(
        target=target,
        publish_spec=secret_rotated_outbox_spec() if publish else None,
    )
    rotator.register(registry)
    durable_deps, _, _ = durable_kits_deps(registry=registry)
    ctx = context_from_deps(MockDepsModule(state=state)(), durable_deps)

    return ctx, rotator, state


async def _seed(ctx: Any, value: str) -> None:
    await ctx.deps.provide(SecretsAdminDepKey).put(_REF, value)


class TestFourStepRotation:
    async def test_happy_path_stages_verifies_and_promotes(self) -> None:
        target = _RecordingTarget()
        ctx, rotator, _ = _composition(target)
        await _seed(ctx, "dsn-old")

        record = await rotator.rotate_now(ctx, _REF)

        assert record.status is DurableRunStatus.COMPLETED
        assert target.calls == ["compose", "apply", "verify"]

        secrets = ctx.deps.provide(SecretsDepKey)
        staged = await secrets.resolve_str(SecretRef("db/dsn.pending"))
        promoted = await secrets.resolve_str(_REF)

        assert staged == promoted == f"dsn-for-{target.minted[0]}"
        assert promoted != "dsn-old"

    async def test_journal_and_output_carry_refs_and_versions_only(self) -> None:
        target = _RecordingTarget()
        ctx, rotator, state = _composition(target)
        await _seed(ctx, "dsn-old")

        record = await rotator.rotate_now(ctx, _REF)

        secret_text = f"dsn-for-{target.minted[0]}"
        assert record.output_json is not None
        assert secret_text not in str(record.output_json)
        assert target.minted[0] not in str(record.output_json)

        # The step journal (what a crashed run replays from) is equally clean.
        journal_blob = str(state.durable_runs) + str(getattr(state, "durable_steps", ""))
        assert target.minted[0] not in journal_blob

    async def test_failed_verify_halts_before_promote(self) -> None:
        target = _RecordingTarget(fail_verify=99)
        ctx, rotator, _ = _composition(target)
        await _seed(ctx, "dsn-old")

        with pytest.raises(Exception, match="refused the pending credential"):
            await rotator.rotate_now(ctx, _REF)

        secrets = ctx.deps.provide(SecretsDepKey)
        assert await secrets.resolve_str(_REF) == "dsn-old"

        admin = resolve_durable_run_admin(ctx)
        page = await admin.list_runs(status=DurableRunStatus.FAILED)
        assert len(page.records) == 1

    async def test_fails_closed_on_read_only_admin_store(self) -> None:
        """An admin store that declares no writes stops the run before any minting."""

        from forze.application.contracts.deps import Deps
        from forze_kits.adapters.secrets import MappingSecrets

        target = _RecordingTarget()
        rotator = SecretRotator(target=target, publish_spec=None, lock=None)

        class _ReadOnlyAdmin:
            async def put(self, ref: SecretRef, value: str) -> None:  # pragma: no cover
                raise AssertionError("must not be reached")

        ctx = context_from_deps(
            Deps.plain(
                {
                    SecretsDepKey: MappingSecrets(data={"db/dsn": "dsn-old"}),
                    SecretsAdminDepKey: _ReadOnlyAdmin(),
                }
            )
        )

        with pytest.raises(Exception, match="not supported"):
            await rotator.handler(ctx, {"ref_path": _REF.path, "tenant_id": None})

        assert target.calls == []


class TestCrashResume:
    async def test_resume_replays_create_without_reminting(self) -> None:
        """Crash after *test* fails transiently → recovery resumes, never re-mints."""

        target = _RecordingTarget(fail_verify=1)
        state = MockState()
        registry = DurableFunctionRegistry()
        rotator = SecretRotator(target=target, publish_spec=None)
        rotator.register(registry)
        durable_deps, runner, _ = durable_kits_deps(registry=registry)
        ctx = context_from_deps(MockDepsModule(state=state)(), durable_deps)
        await _seed(ctx, "dsn-old")

        store = resolve_durable_run_store(ctx)
        record = await store.enqueue(
            rotator.function_name, input_json={"ref_path": _REF.path, "tenant_id": None}
        )
        assert await store.begin(record.run_id, lease_for=timedelta(minutes=5))

        # Drive the handler by hand and "crash" at the failing verify: steps
        # create/set are journaled, no terminal state is written.
        token = bind_durable_run(
            DurableRunContext(run_id=record.run_id, name=rotator.function_name)
        )
        try:
            with pytest.raises(RuntimeError, match="refused"):
                await rotator.handler(ctx, {"ref_path": _REF.path, "tenant_id": None})
        finally:
            reset_durable_run(token)

        assert target.calls == ["compose", "apply", "verify"]

        # Reclaim after lease expiry: create + set replay from the journal, verify
        # runs live (and now passes), finish promotes exactly the staged value.
        state.durable_runs[record.run_id]["leased_until"] = utcnow() - timedelta(hours=1)
        assert await runner.recover(ctx) == 1

        reloaded = await store.load(record.run_id)
        assert reloaded is not None
        assert reloaded.status is DurableRunStatus.COMPLETED
        assert target.calls == ["compose", "apply", "verify", "verify"]
        assert len(target.minted) == 1  # the crash never re-minted

        secrets = ctx.deps.provide(SecretsDepKey)
        assert await secrets.resolve_str(_REF) == f"dsn-for-{target.minted[0]}"


class TestPromoteFence:
    async def test_restaged_ref_after_verify_refuses_promotion(self) -> None:
        """The lost-lock race: a competing rotator overwrites the staging ref after
        this run's verify passed. The version fence must refuse the promote — an
        unverified credential never becomes active."""

        class _InterferingTarget(_RecordingTarget):
            """After verify succeeds, simulate the competing worker restaging."""

            admin: Any = None

            async def verify(self, tenant_id: UUID | None, pending: PendingCredential) -> None:
                await super().verify(tenant_id, pending)
                await self.admin.put(pending.ref, "intruder-unverified-value")

        target = _InterferingTarget()
        ctx, rotator, _ = _composition(target, publish=False)
        target.admin = ctx.deps.provide(SecretsAdminDepKey)
        await _seed(ctx, "dsn-old")

        with pytest.raises(Exception, match="refusing to promote"):
            await rotator.rotate_now(ctx, _REF)

        secrets = ctx.deps.provide(SecretsDepKey)
        # Neither our verified value nor the intruder's unverified one was promoted.
        assert await secrets.resolve_str(_REF) == "dsn-old"

    async def test_fails_closed_on_unversioned_data_store(self) -> None:
        """The promote fence needs versioned reads — an unversioned store is
        refused at the start of the run, before anything is minted."""

        from forze.application.contracts.deps import Deps
        from forze_kits.adapters.secrets import MappingSecrets

        class _UnversionedSecrets:
            async def resolve_str(self, ref: SecretRef) -> str:  # pragma: no cover
                return "x"

            async def exists(self, ref: SecretRef) -> bool:  # pragma: no cover
                return True

        target = _RecordingTarget()
        rotator = SecretRotator(target=target, publish_spec=None, lock=None)
        ctx = context_from_deps(
            Deps.plain(
                {
                    SecretsDepKey: _UnversionedSecrets(),
                    SecretsAdminDepKey: MappingSecrets(data={"db/dsn": "dsn-old"}),
                }
            )
        )

        with pytest.raises(Exception, match="not supported"):
            await rotator.handler(ctx, {"ref_path": _REF.path, "tenant_id": None})

        assert target.calls == []


class TestPublication:
    async def test_finish_publishes_a_rotation_event_end_to_end(self) -> None:
        target = _RecordingTarget()
        ctx, rotator, _ = _composition(target, publish=True)
        await _seed(ctx, "dsn-old")

        pubsub_spec = secret_rotated_pubsub_spec()
        query = ctx.deps.resolve_configurable(
            ctx, PubSubQueryDepKey, pubsub_spec, route=pubsub_spec.name
        )
        source = PubSubSecretsChangeSource(query=query)
        received: list[str] = []

        async def _drain() -> None:
            async for change in source.subscribe():
                received.append(change.ref.path)
                return

        task = asyncio.create_task(_drain())
        await asyncio.sleep(0)

        try:
            await rotator.rotate_now(ctx, _REF)
            relayed = await OutboxRelay(outbox_spec=rotator.publish_spec).to_pubsub(
                ctx, pubsub_spec
            )
            assert relayed.published == 1

            await asyncio.wait_for(task, timeout=2)

        finally:
            task.cancel()

        assert received == [_REF.path]


class TestConfig:
    def test_rejects_weak_entropy(self) -> None:
        with pytest.raises(CoreException, match="at least 16 bytes"):
            SecretRotator(target=_RecordingTarget(), minted_bytes=8)

    def test_rejects_empty_suffix(self) -> None:
        with pytest.raises(CoreException, match="suffix"):
            SecretRotator(target=_RecordingTarget(), pending_suffix="")
