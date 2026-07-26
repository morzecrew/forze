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
        # The trailing verify is the post-promote backend confirmation.
        assert target.calls == ["compose", "apply", "verify", "verify"]

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
        # Replayed create/set, live verify, then the post-promote confirmation.
        assert target.calls == ["compose", "apply", "verify", "verify", "verify"]
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

    async def test_completed_competing_rotation_is_not_clobbered(self) -> None:
        """The lost-lock stale-promote race: a competitor finishes a WHOLE rotation
        (its promote advanced the primary) while this run is between its staging
        fence read and its promote write. The CAS promote must fail instead of
        overwriting the newer credential — staging alone can't catch this."""

        class _CompetingTarget(_RecordingTarget):
            """After verify passes, simulate the competitor's completed promote —
            the primary advances, the staging ref is left untouched."""

            admin: Any = None

            async def verify(self, tenant_id: UUID | None, pending: PendingCredential) -> None:
                await super().verify(tenant_id, pending)
                await self.admin.put(_REF, "competitor-promoted-dsn")

        target = _CompetingTarget()
        ctx, rotator, _ = _composition(target, publish=False)
        target.admin = ctx.deps.provide(SecretsAdminDepKey)
        await _seed(ctx, "dsn-old")

        with pytest.raises(Exception, match="changed since it was last observed"):
            await rotator.rotate_now(ctx, _REF)

        secrets = ctx.deps.provide(SecretsDepKey)
        # The competitor's promotion survives; the stale staged value never landed.
        assert await secrets.resolve_str(_REF) == "competitor-promoted-dsn"

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


class _ConfirmScriptedTarget(_RecordingTarget):
    """Distinguishes the post-promote confirmation (canonical = primary ref) from
    the pre-promote verify, and scripts confirmation failures."""

    def __init__(self, *, fail_confirms: int = 0) -> None:
        super().__init__()
        self.fail_confirms = fail_confirms

    async def verify(self, tenant_id: UUID | None, pending: PendingCredential) -> None:
        if pending.ref == _REF:
            self.calls.append("confirm")

            if self.fail_confirms > 0:
                self.fail_confirms -= 1
                raise RuntimeError("backend disagrees with the promoted credential")

            return

        await super().verify(tenant_id, pending)

    async def apply(self, tenant_id: UUID | None, pending: PendingCredential) -> None:
        if pending.ref == _REF:
            self.calls.append("converge")
            return

        await super().apply(tenant_id, pending)


class TestBackendConvergence:
    async def test_stale_backend_write_is_converged_by_the_winner(self) -> None:
        """The unfenceable-write race: a competitor's stale ALTER lands after our
        promote. The winner's confirmation catches the disagreement and re-applies
        the canonical (promoted) credential."""

        target = _ConfirmScriptedTarget(fail_confirms=1)
        ctx, rotator, _ = _composition(target, publish=False)
        await _seed(ctx, "dsn-old")

        record = await rotator.rotate_now(ctx, _REF)

        assert record.status is DurableRunStatus.COMPLETED
        assert target.calls == [
            "compose",
            "apply",
            "verify",
            "confirm",  # backend disagrees (the stale write landed)
            "converge",  # re-assert the canonical credential
            "confirm",  # and prove it took
        ]

    async def test_unconvergeable_backend_fails_the_run_loudly(self) -> None:
        target = _ConfirmScriptedTarget(fail_confirms=2)
        ctx, rotator, _ = _composition(target, publish=False)
        await _seed(ctx, "dsn-old")

        with pytest.raises(Exception, match="disagrees"):
            await rotator.rotate_now(ctx, _REF)

        # Promoted but unusable is a paged failure, never a silent publish; the
        # primary intentionally keeps the promoted value (it IS canonical).
        secrets = ctx.deps.provide(SecretsDepKey)
        assert await secrets.resolve_str(_REF) == f"dsn-for-{target.minted[0]}"

    async def test_finish_schedules_the_delayed_reconfirmation(self) -> None:
        target = _RecordingTarget()
        ctx, rotator, _ = _composition(target, publish=False)
        await _seed(ctx, "dsn-old")

        await rotator.rotate_now(ctx, _REF)

        admin = resolve_durable_run_admin(ctx)
        page = await admin.list_runs(name=rotator.confirm_function_name)
        assert len(page.records) == 1
        assert page.records[0].status is DurableRunStatus.PENDING

    async def test_reconfirmation_can_be_disabled(self) -> None:
        target = _RecordingTarget()
        state = MockState()
        registry = DurableFunctionRegistry()
        rotator = SecretRotator(target=target, publish_spec=None, reconfirm_after=None)
        rotator.register(registry)
        durable_deps, _, _ = durable_kits_deps(registry=registry)
        ctx = context_from_deps(MockDepsModule(state=state)(), durable_deps)
        await _seed(ctx, "dsn-old")

        await rotator.rotate_now(ctx, _REF)

        admin = resolve_durable_run_admin(ctx)
        page = await admin.list_runs(name=rotator.confirm_function_name)
        assert page.records == []

    async def test_delayed_reconfirmation_converges_and_chains_until_quiet(self) -> None:
        """The latecomer beyond the in-run confirm: a stale ALTER commits after the
        rotation completed. The delayed run converges — and because drift is
        evidence of a recently active stale writer, it schedules another round;
        the chain rests only after a full quiet window."""

        target = _ConfirmScriptedTarget()
        state = MockState()
        registry = DurableFunctionRegistry()
        rotator = SecretRotator(
            target=target,
            publish_spec=None,
            reconfirm_after=timedelta(0),  # due immediately, picked up by recover
        )
        rotator.register(registry)
        durable_deps, runner, _ = durable_kits_deps(registry=registry)
        ctx = context_from_deps(MockDepsModule(state=state)(), durable_deps)
        await _seed(ctx, "dsn-old")

        await rotator.rotate_now(ctx, _REF)
        assert target.calls[-1] == "confirm"  # in-run confirmation passed

        # The stale statement commits AFTER the rotation finished: the backend no
        # longer agrees with the promoted credential.
        target.fail_confirms = 1

        assert await runner.recover(ctx) == 1  # round 1: drift → converge
        assert target.calls[-3:] == ["confirm", "converge", "confirm"]

        assert await runner.recover(ctx) == 1  # round 2: quiet → chain ends
        assert target.calls[-1] == "confirm"
        assert await runner.recover(ctx) == 0  # nothing further scheduled

        admin = resolve_durable_run_admin(ctx)
        page = await admin.list_runs(name=rotator.confirm_function_name)
        assert len(page.records) == 2
        assert all(record.status is DurableRunStatus.COMPLETED for record in page.records)

    async def test_reconfirmation_rounds_are_capped(self) -> None:
        """Perpetual drift must not chain forever — the cap ends the churn (with a
        critical log) instead of re-enqueueing indefinitely."""

        from forze_kits.integrations.secrets.rotator import MAX_RECONFIRM_ROUNDS

        class _AlwaysDriftingTarget(_ConfirmScriptedTarget):
            """Every round's first confirm drifts; the converge-confirm passes —
            fresh drift each window, forever."""

            def __init__(self) -> None:
                super().__init__()
                self._confirms = 0

            async def verify(
                self, tenant_id: UUID | None, pending: PendingCredential
            ) -> None:
                if pending.ref == _REF:
                    self._confirms += 1
                    self.fail_confirms = 1 if self._confirms % 2 == 1 else 0

                await super().verify(tenant_id, pending)

        target = _AlwaysDriftingTarget()
        state = MockState()
        registry = DurableFunctionRegistry()
        rotator = SecretRotator(
            target=target, publish_spec=None, reconfirm_after=timedelta(0)
        )
        rotator.register(registry)
        durable_deps, runner, _ = durable_kits_deps(registry=registry)
        ctx = context_from_deps(MockDepsModule(state=state)(), durable_deps)
        await _seed(ctx, "dsn-old")

        await rotator.rotate_now(ctx, _REF)

        for _ in range(MAX_RECONFIRM_ROUNDS + 3):
            if await runner.recover(ctx) == 0:
                break

        admin = resolve_durable_run_admin(ctx)
        page = await admin.list_runs(name=rotator.confirm_function_name)
        assert len(page.records) == MAX_RECONFIRM_ROUNDS
        assert all(record.status is DurableRunStatus.COMPLETED for record in page.records)

    async def test_reconfirmation_never_rests_on_a_superseded_canonical(self) -> None:
        """A concurrent rotation promotes a newer credential while the corrective
        apply asserts the older one (lock-less wiring): the post-apply recheck
        sees the advance and the chained round converges to the NEW canonical."""

        class _SupersedingTarget(_ConfirmScriptedTarget):
            admin: Any = None
            superseded: bool = False

            async def apply(self, tenant_id: UUID | None, pending: PendingCredential) -> None:
                await super().apply(tenant_id, pending)

                if pending.ref == _REF and not self.superseded:
                    # The concurrent rotation's promote lands mid-corrective-apply.
                    self.superseded = True
                    await self.admin.put(_REF, "newer-canonical-dsn")

        target = _SupersedingTarget()
        state = MockState()
        registry = DurableFunctionRegistry()
        rotator = SecretRotator(
            target=target,
            publish_spec=None,
            lock=None,  # the exposed wiring — the recheck is the remaining net
            reconfirm_after=timedelta(0),
        )
        rotator.register(registry)
        durable_deps, runner, _ = durable_kits_deps(registry=registry)
        ctx = context_from_deps(MockDepsModule(state=state)(), durable_deps)
        target.admin = ctx.deps.provide(SecretsAdminDepKey)
        await _seed(ctx, "dsn-old")

        await rotator.rotate_now(ctx, _REF)
        target.fail_confirms = 1  # round 1 drifts → corrective apply → superseded

        assert await runner.recover(ctx) == 1  # round 1: converge + recheck + chain
        assert await runner.recover(ctx) == 1  # round 2: quiet, on the NEW canonical
        assert await runner.recover(ctx) == 0

        secrets = ctx.deps.provide(SecretsDepKey)
        canonical_now = await secrets.resolve_versioned(_REF)
        assert canonical_now.text == "newer-canonical-dsn"

        admin = resolve_durable_run_admin(ctx)
        page = await admin.list_runs(name=rotator.confirm_function_name)
        assert len(page.records) == 2
        # The chained round asserted the newer canonical, not the superseded one.
        newest = max(page.records, key=lambda record: record.created_at)
        assert newest.output_json is not None
        assert newest.output_json["version_token"] == canonical_now.version.token

    async def test_quiet_verify_raced_by_a_promote_still_chains(self) -> None:
        """The symmetric race: verify passes quietly, but a concurrent rotation
        promoted a newer canonical during the round. The post-verify recheck marks
        drift and the chained round converges to the new canonical."""

        class _RacingTarget(_ConfirmScriptedTarget):
            admin: Any = None
            _ref_verifies: int = 0

            async def verify(self, tenant_id: UUID | None, pending: PendingCredential) -> None:
                await super().verify(tenant_id, pending)

                if pending.ref == _REF:
                    self._ref_verifies += 1

                    # Call 1 is the in-run finish confirm; the promote lands
                    # during the DELAYED round's quiet verify (call 2).
                    if self._ref_verifies == 2:
                        await self.admin.put(_REF, "newer-canonical-dsn")

        target = _RacingTarget()
        state = MockState()
        registry = DurableFunctionRegistry()
        rotator = SecretRotator(
            target=target, publish_spec=None, lock=None, reconfirm_after=timedelta(0)
        )
        rotator.register(registry)
        durable_deps, runner, _ = durable_kits_deps(registry=registry)
        ctx = context_from_deps(MockDepsModule(state=state)(), durable_deps)
        target.admin = ctx.deps.provide(SecretsAdminDepKey)
        await _seed(ctx, "dsn-old")

        await rotator.rotate_now(ctx, _REF)

        assert await runner.recover(ctx) == 1  # round 1: quiet verify, recheck drifts
        assert await runner.recover(ctx) == 1  # round 2: quiet on the new canonical
        assert await runner.recover(ctx) == 0

        secrets = ctx.deps.provide(SecretsDepKey)
        canonical_now = await secrets.resolve_versioned(_REF)
        assert canonical_now.text == "newer-canonical-dsn"

        admin = resolve_durable_run_admin(ctx)
        page = await admin.list_runs(name=rotator.confirm_function_name)
        assert len(page.records) == 2
        newest = max(page.records, key=lambda record: record.created_at)
        assert newest.output_json is not None
        assert newest.output_json["version_token"] == canonical_now.version.token

    def test_confirm_round_must_be_positive(self) -> None:
        from pydantic import ValidationError

        from forze_kits.integrations.secrets import RotationInput

        with pytest.raises(ValidationError):
            RotationInput(ref_path="db/dsn", confirm_round=0)

        with pytest.raises(ValidationError):
            RotationInput(ref_path="db/dsn", confirm_round=-3)

        assert RotationInput(ref_path="db/dsn").confirm_round == 1

    async def test_reconfirm_window_must_exceed_the_targets_latency_bound(self) -> None:
        """Fail closed at wiring: a reconfirmation that fires while a stale apply
        can still commit proves nothing."""

        class _BoundedTarget(_RecordingTarget):
            @property
            def apply_latency_bound(self) -> timedelta:
                return timedelta(seconds=30)

        with pytest.raises(CoreException, match="must exceed"):
            SecretRotator(
                target=_BoundedTarget(),
                publish_spec=None,
                reconfirm_after=timedelta(seconds=10),
            )

        # Strictly above the bound (or disabled entirely) wires fine.
        SecretRotator(
            target=_BoundedTarget(), publish_spec=None, reconfirm_after=timedelta(seconds=60)
        )
        SecretRotator(target=_BoundedTarget(), publish_spec=None, reconfirm_after=None)

    async def test_finish_retry_after_own_promote_is_idempotent(self) -> None:
        """A crash between promote and publish makes the retry's CAS trip on OUR
        OWN earlier write — it must converge, not fail as a competitor."""

        target = _ConfirmScriptedTarget(fail_confirms=2)  # both confirms of attempt 1
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

        # Attempt 1 "crashes" after the promote landed (both confirms fail).
        token = bind_durable_run(
            DurableRunContext(run_id=record.run_id, name=rotator.function_name)
        )
        try:
            with pytest.raises(RuntimeError, match="disagrees"):
                await rotator.handler(ctx, {"ref_path": _REF.path, "tenant_id": None})
        finally:
            reset_durable_run(token)

        # Reclaim: finish re-runs, its CAS trips on our own promote, recognizes the
        # primary already holds our staged text, and completes.
        state.durable_runs[record.run_id]["leased_until"] = utcnow() - timedelta(hours=1)
        assert await runner.recover(ctx) == 1

        reloaded = await store.load(record.run_id)
        assert reloaded is not None
        assert reloaded.status is DurableRunStatus.COMPLETED
        assert len(target.minted) == 1

        secrets = ctx.deps.provide(SecretsDepKey)
        assert await secrets.resolve_str(_REF) == f"dsn-for-{target.minted[0]}"


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


class TestTriggers:
    async def test_enqueue_fleet_enqueue_and_cron(self) -> None:
        from uuid import uuid4

        target = _RecordingTarget()
        ctx, rotator, _ = _composition(target, publish=False)
        await _seed(ctx, "dsn-old")

        record = await rotator.enqueue(ctx, _REF)
        assert record.status is DurableRunStatus.PENDING

        tenant_a, tenant_b = uuid4(), uuid4()
        refs = {
            tenant_a: SecretRef(f"tenants/{tenant_a}/dsn"),
            tenant_b: SecretRef(f"tenants/{tenant_b}/dsn"),
        }

        count = await rotator.enqueue_tenants(
            ctx,
            tenants=(tenant_a, tenant_b),
            ref_for_tenant=refs,
            idempotency_prefix="cycle-2026-07",
        )
        assert count == 2

        # Re-submitting the same fleet pass converges on the same runs (per-key
        # idempotency), instead of duplicating work.
        assert (
            await rotator.enqueue_tenants(
                ctx,
                tenants=(tenant_a, tenant_b),
                ref_for_tenant=refs,
                idempotency_prefix="cycle-2026-07",
            )
            == 2
        )

        schedule = await rotator.ensure_cron(ctx, _REF, cron="0 4 * * 0")
        assert schedule.cron == "0 4 * * 0"
        assert schedule.name == rotator.function_name


class TestConfig:
    def test_rejects_weak_entropy(self) -> None:
        with pytest.raises(CoreException, match="at least 16 bytes"):
            SecretRotator(target=_RecordingTarget(), minted_bytes=8)

    def test_rejects_empty_suffix(self) -> None:
        with pytest.raises(CoreException, match="suffix"):
            SecretRotator(target=_RecordingTarget(), pending_suffix="")
