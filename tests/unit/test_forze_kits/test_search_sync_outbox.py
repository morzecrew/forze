"""Durable search-index maintenance through the kit's dedicated outbox route (mock).

With ``AggregateKit(search=…, search_delivery=OutboxSearchSync())`` each committed write
stages an identity-only marker **in the write's transaction**; the standard relay carries
it to the sync queue at-least-once, and the consumer re-reads the row's committed state
and applies *that* to the index with inbox dedup. These tests drive the relay and the
consumer one-shot (no background loops), on the strict mock so a failing apply really
rolls its inbox mark back — the semantics broker redelivery retries ride on.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest

from forze import build_runtime
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.invariants import ReadSet, SumOf, SystemInvariant
from forze.application.contracts.queue import QueueMessage
from forze.application.contracts.search import SearchSpec
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import CreateDocumentCmd, ReadDocument
from forze_kits.aggregates import AggregateKit
from forze_kits.aggregates.document import DocumentIdDTO, DocumentUpdateDTO
from forze_kits.aggregates.document.dto import DocumentIdRevDTO
from forze_kits.aggregates.document.operations import DocumentKernelOp
from forze_kits.aggregates.search import (
    SEARCH_SYNC_EVENT_TYPE,
    OutboxSearchSync,
    SearchSyncMarker,
)
from forze_kits.aggregates.soft_deletion import SoftDeletionKernelOp
from forze_kits.domain.soft_deletion import (
    DocWithSoftDeletion,
    UpdateCmdWithSoftDeletion,
)
from forze_kits.integrations.outbox import OutboxRelay, RelayBinding
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.adapters import MockSearchCommandAdapter

# ----------------------- #

_TX = "mock"
_IDLE = timedelta(milliseconds=250)
_SYNC_ROUTE = "gizmos_index_sync"


class Gizmo(DocWithSoftDeletion):
    name: str
    qty: int = 0


class GizmoCreate(CreateDocumentCmd):
    name: str
    qty: int = 0


class GizmoUpdate(UpdateCmdWithSoftDeletion):
    name: str | None = None
    qty: int | None = None


class GizmoRead(ReadDocument):
    name: str
    qty: int = 0
    is_deleted: bool = False


GIZMO_SPEC = DocumentSpec(
    name="gizmos",
    read=GizmoRead,
    write=DocumentWriteTypes(
        domain=Gizmo, create_cmd=GizmoCreate, update_cmd=GizmoUpdate
    ),
)
GIZMO_INDEX = SearchSpec(
    name="gizmos_index",
    model_type=GizmoRead,
    fields=["name"],
    facetable_fields={"is_deleted"},
)

# A per-name qty cap — gives the staging tests a write that rolls back post-handler.
NAME_CAP = SystemInvariant(
    name="gizmo_name_cap",
    read_set=ReadSet(spec=GIZMO_SPEC, scope_keys=("name",)),
    aggregate=SumOf("qty"),
    holds=lambda total: total <= 10,
)


def _kit(**overrides: Any) -> AggregateKit[GizmoRead, Gizmo, GizmoCreate, GizmoUpdate]:
    config: dict[str, Any] = {
        "spec": GIZMO_SPEC,
        "soft_delete": True,
        "search": GIZMO_INDEX,
        "search_delivery": OutboxSearchSync(),
    }
    config.update(overrides)
    return AggregateKit(**config)


def _key(op: Any) -> str:
    return GIZMO_SPEC.default_namespace.key(op)


def _index(runtime: Any) -> dict[UUID, Any]:
    return (
        runtime.get_context()
        .deps.provide(MockStateDepKey)
        .documents.get("gizmos_index", {})
    )


async def _create(reg: Any, ctx: Any, name: str, qty: int = 1) -> GizmoRead:
    return await run_operation(
        reg, _key(DocumentKernelOp.CREATE), GizmoCreate(name=name, qty=qty), ctx
    )


async def _drain(kit: Any, ctx: Any) -> Any:
    """One relay pass + one one-shot consumer run over the kit's sync route."""

    wiring = kit.search_sync_wiring()
    await OutboxRelay(outbox_spec=wiring.outbox_spec).to_queue(ctx, wiring.queue_spec)
    return await wiring.queue_consumer(ctx, tx_route=_TX).run(ctx, timeout=_IDLE)


# ....................... #


class TestDeclaration:
    def test_search_delivery_without_search_is_rejected(self) -> None:
        with pytest.raises(CoreException) as ei:
            AggregateKit(spec=GIZMO_SPEC, search_delivery=OutboxSearchSync())
        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_soft_delete_with_unfilterable_index_fails_closed(self) -> None:
        # The kit's search reads must be able to filter `is_deleted`; a spec that never
        # provisions it filterable (no facetable declaration) is rejected at construction.
        bare = SearchSpec(name="gizmos_bare", model_type=GizmoRead, fields=["name"])

        with pytest.raises(CoreException) as ei:
            AggregateKit(spec=GIZMO_SPEC, soft_delete=True, search=bare)
        assert ei.value.kind is ExceptionKind.CONFIGURATION
        assert "is_deleted" in str(ei.value)

    def test_backend_requirements_report_the_sync_route(self) -> None:
        assert _kit().backend_requirements(tx_route=_TX).search_sync_route == _SYNC_ROUTE

        # Default (after-commit) delivery wires no sync route.
        default = _kit(search_delivery=None)
        assert default.backend_requirements(tx_route=_TX).search_sync_route is None

    def test_lifecycle_steps_carry_the_relay_and_the_consumer(self) -> None:
        ids = [step.id for step in _kit().lifecycle_steps(tx_route=_TX)]

        assert f"search_sync_relay:{_SYNC_ROUTE}" in ids
        assert f"search_sync_consumer:{_SYNC_ROUTE}" in ids

    def test_out_of_process_opt_outs_drop_the_steps(self) -> None:
        kit = _kit(search_delivery=OutboxSearchSync(relay=None, consume=False))
        assert kit.lifecycle_steps(tx_route=_TX) == ()


# ....................... #


class TestInTxStaging:
    async def test_committed_write_stages_a_marker_and_defers_the_index(self) -> None:
        kit = _kit()
        runtime = build_runtime(MockDepsModule(strict_tx=True))
        reg = kit.registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await _create(reg, ctx, "alpha")

            # Nothing hit the index at commit time — maintenance rides the outbox now.
            assert _index(runtime) == {}

            claims = await ctx.outbox.query(kit.search_sync_wiring().outbox_spec).claim_pending()
            assert len(claims) == 1
            assert claims[0].payload == {"document_id": str(row.id)}

    async def test_rolled_back_write_stages_nothing(self) -> None:
        kit = _kit(invariants=(NAME_CAP,))
        runtime = build_runtime(MockDepsModule(strict_tx=True))
        reg = kit.registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()

            with pytest.raises(CoreException):
                await _create(reg, ctx, "alpha", qty=20)  # over the cap — rolled back

            claims = await ctx.outbox.query(kit.search_sync_wiring().outbox_spec).claim_pending()
            assert claims == []  # the staged marker rolled back with the write

    def test_the_route_declares_its_atomicity_precondition(self) -> None:
        # Staging atomically with the write is the whole difference between this route and
        # the best-effort after-commit sync, so the route must *declare* that rather than
        # merely happen to get it from how the kit wires the hook.
        assert _kit().search_sync_wiring().outbox_spec.require_transaction is True

    async def test_staging_outside_a_transaction_is_refused_not_silently_dual_written(
        self,
    ) -> None:
        # A hook attached without bind_tx() flushes after the write has already committed.
        # That is a dual-write: the row is durable, the marker is not covered by it, and the
        # index can diverge with nothing to alert on. It must fail loudly instead.
        kit = _kit()
        runtime = build_runtime(MockDepsModule(strict_tx=True))
        wiring = kit.search_sync_wiring()

        async with runtime.scope():
            ctx = runtime.get_context()
            command = ctx.outbox.command(wiring.outbox_spec)
            await command.stage(
                SEARCH_SYNC_EVENT_TYPE,
                SearchSyncMarker(document_id=str(uuid4())),
            )

            with pytest.raises(CoreException) as caught:
                await command.flush()  # no open transaction

        assert caught.value.code == "core.outbox.flush_outside_transaction"


# ....................... #


class TestEndToEnd:
    async def test_create_converges_through_relay_and_consumer(self) -> None:
        kit = _kit()
        runtime = build_runtime(MockDepsModule(strict_tx=True))
        reg = kit.registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await _create(reg, ctx, "alpha")

            result = await _drain(kit, ctx)

            assert result.processed == 1
            assert _index(runtime)[row.id]["name"] == "alpha"

    async def test_transient_index_failure_retries_via_redelivery(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        kit = _kit()
        runtime = build_runtime(MockDepsModule(strict_tx=True))
        reg = kit.registry(tx_route=_TX)

        original = MockSearchCommandAdapter.upsert
        calls = {"n": 0}

        async def _flaky(self: Any, documents: Any) -> None:
            calls["n"] += 1
            if calls["n"] == 1:
                raise ConnectionError("index unreachable")
            await original(self, documents)

        monkeypatch.setattr(MockSearchCommandAdapter, "upsert", _flaky)

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await _create(reg, ctx, "alpha")

            # First apply fails → the inbox mark rolls back with its transaction and the
            # message is nacked for redelivery; the redelivery converges the index.
            result = await _drain(kit, ctx)

            assert result.failed == 1
            assert result.processed == 1
            assert row.id in _index(runtime)

    async def test_soft_delete_and_restore_converge(self) -> None:
        kit = _kit()
        runtime = build_runtime(MockDepsModule(strict_tx=True))
        reg = kit.registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await _create(reg, ctx, "alpha")
            await _drain(kit, ctx)
            assert row.id in _index(runtime)

            deleted = await run_operation(
                reg,
                _key(SoftDeletionKernelOp.DELETE),
                DocumentIdRevDTO(id=row.id, rev=row.rev),
                ctx,
            )
            await _drain(kit, ctx)
            assert row.id not in _index(runtime)  # re-read saw the soft-deleted state

            await run_operation(
                reg,
                _key(SoftDeletionKernelOp.RESTORE),
                DocumentIdRevDTO(id=row.id, rev=deleted.rev),
                ctx,
            )
            await _drain(kit, ctx)
            assert row.id in _index(runtime)  # re-read saw the restored live state

    async def test_kill_removes_the_index_entry(self) -> None:
        kit = _kit()
        runtime = build_runtime(MockDepsModule(strict_tx=True))
        reg = kit.registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await _create(reg, ctx, "alpha")
            await _drain(kit, ctx)
            assert row.id in _index(runtime)

            await run_operation(
                reg, _key(DocumentKernelOp.KILL), DocumentIdDTO(id=row.id), ctx
            )
            await _drain(kit, ctx)
            assert row.id not in _index(runtime)  # re-read 404s → delete from the index

    async def test_background_lifecycle_steps_converge_without_manual_draining(
        self,
    ) -> None:
        # The kit's own emitted steps — sync relay + consumer — close the loop when
        # registered on the runtime, with no one-shot draining from the test.
        kit = _kit(
            search_delivery=OutboxSearchSync(
                relay=RelayBinding(interval=timedelta(milliseconds=20), jitter=0.0),
            ),
        )
        runtime = build_runtime(
            MockDepsModule(strict_tx=True),
            lifecycle_steps=kit.lifecycle_steps(tx_route=_TX),
        )
        reg = kit.registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await _create(reg, ctx, "alpha")

            for _ in range(400):
                if row.id in _index(runtime):
                    break
                await asyncio.sleep(0.01)

            assert row.id in _index(runtime)

    async def test_default_delivery_still_syncs_after_commit(self) -> None:
        # Without the knob the pre-existing best-effort contract is untouched: the index
        # reflects the write at commit time and no sync-route rows are ever staged.
        kit = _kit(search_delivery=None)
        runtime = build_runtime(MockDepsModule(strict_tx=True))
        reg = kit.registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await _create(reg, ctx, "alpha")

            assert row.id in _index(runtime)
            state = ctx.deps.provide(MockStateDepKey)
            assert _SYNC_ROUTE not in state.outbox_rows


# ....................... #


class TestReorderSafety:
    async def test_a_stale_upsert_marker_cannot_resurrect_a_deleted_row(self) -> None:
        """Markers carry no action: application re-reads the committed state.

        Stage an upsert-time marker, then soft-delete the row (staging its own marker).
        Apply the markers in the adverse order — the delete-time one first, the *stale*
        upsert-time one last. Both re-read the deleted state, so the last-applied marker
        deletes again instead of resurrecting the ghost.
        """

        kit = _kit()
        runtime = build_runtime(MockDepsModule(strict_tx=True))
        reg = kit.registry(tx_route=_TX)
        wiring = kit.search_sync_wiring()

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await _create(reg, ctx, "alpha")
            await run_operation(
                reg,
                _key(DocumentKernelOp.UPDATE),
                DocumentUpdateDTO(
                    id=row.id, rev=row.rev, dto=GizmoUpdate(is_deleted=True)
                ),
                ctx,
            )

            # Seed the index with the pre-delete state — the divergence a lost/lagging
            # delete would leave behind.
            index = _index(runtime)
            index[row.id] = {"id": str(row.id), "name": "alpha", "is_deleted": False}

            apply = wiring.apply_handler(ctx)
            marker = SearchSyncMarker(document_id=str(row.id))

            # Delete-time marker first, stale upsert-time marker last (worst-case order).
            await apply(QueueMessage(queue=_SYNC_ROUTE, id="delete-time", payload=marker))
            await apply(QueueMessage(queue=_SYNC_ROUTE, id="upsert-time", payload=marker))

            assert row.id not in _index(runtime)
