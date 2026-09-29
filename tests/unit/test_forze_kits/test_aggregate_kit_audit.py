"""`AggregateKit(audit=...)` binds the audit hooks to the operations it generates.

A write's row has to ride the write's transaction — that is what makes a failed audit write
roll the write back — and a generated write has no transaction until something binds one, so
the legs assert the write and the row together. A read gets no transaction it did not have.
"""

from __future__ import annotations

from typing import Any, Final

import attrs
import pytest

from forze import build_runtime
from forze.application.contracts.audit import (
    AuditDepKey,
    AuditEntry,
    AuditObjectRef,
    AuditOutcome,
    AuditSpec,
)
from forze.application.contracts.deps import Deps, DepsModule
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import run_operation
from forze.application.hooks.audit import Audited
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_kits.aggregates import AggregateKit
from forze_kits.aggregates.document import DocumentIdDTO, DocumentUpdateDTO
from forze_kits.aggregates.document.dto import written_read_model
from forze_kits.aggregates.document.operations import DocumentKernelOp
from forze_kits.aggregates.soft_deletion import SoftDeletionKernelOp
from forze_kits.integrations.audit import AuditDepsModule, AuditRecord, audit_record_spec
from forze_mock import MockDepsModule

# ----------------------- #

_TX: Final = "mock"
TRAIL: Final = audit_record_spec()


class Gadget(Document):
    name: str


class GadgetCreate(BaseDTO):
    name: str


class GadgetUpdate(BaseDTO):
    name: str | None = None


class GadgetRead(ReadDocument):
    name: str


GADGETS = DocumentSpec(
    name="gadgets",
    read=GadgetRead,
    write=DocumentWriteTypes(domain=Gadget, create_cmd=GadgetCreate, update_cmd=GadgetUpdate),
)


def _audited(action: str) -> Audited:
    return Audited(
        spec=AuditSpec(action=action),
        # An update returns its row wrapped with the diff; the unwrap reads either shape.
        object_ref=lambda args, result: (
            AuditObjectRef(type="gadget", id=str(written_read_model(result).id))
            if result is not None
            else None
        ),
    )


def _kit(**audit: Audited) -> AggregateKit[GadgetRead, Gadget, GadgetCreate, GadgetUpdate]:
    return AggregateKit(spec=GADGETS, audit=audit)


def _key(op: str) -> str:
    return GADGETS.default_namespace.key(op)


async def _rows(ctx: ExecutionContext) -> list[AuditRecord]:
    return list((await ctx.document.query(TRAIL).find_many()).hits)


@attrs.define(slots=True, kw_only=True, frozen=True)
class _Unavailable:
    async def record(self, entry: AuditEntry) -> None:
        raise exc.infrastructure("audit store unavailable")


class _UnavailableTrail(DepsModule):
    def __call__(self) -> Deps:
        return Deps.plain({AuditDepKey: lambda ctx: _Unavailable()})


# ....................... #


class TestWrites:
    async def test_create_and_update_each_leave_one_allowed_row(self) -> None:
        kit = _kit(
            **{
                DocumentKernelOp.CREATE: _audited("gadget.create"),
                DocumentKernelOp.UPDATE: _audited("gadget.update"),
            }
        )
        runtime = build_runtime([MockDepsModule(), AuditDepsModule(tx_route=_TX)])
        reg = kit.registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            made = await run_operation(
                reg, _key(DocumentKernelOp.CREATE), GadgetCreate(name="a"), ctx
            )
            await run_operation(
                reg,
                _key(DocumentKernelOp.UPDATE),
                DocumentUpdateDTO(id=made.id, rev=made.rev, dto=GadgetUpdate(name="b")),
                ctx,
            )

            rows = await _rows(ctx)

        assert sorted((r.action, r.outcome, r.object_id) for r in rows) == [
            ("gadget.create", AuditOutcome.ALLOWED, str(made.id)),
            ("gadget.update", AuditOutcome.ALLOWED, str(made.id)),
        ]

    async def test_a_write_whose_row_cannot_be_written_does_not_commit(self) -> None:
        # The generated create has no transaction of its own; the arm binds one on the kit's
        # route, so the failed row takes the write with it.
        kit = _kit(**{DocumentKernelOp.CREATE: _audited("gadget.create")})
        runtime = build_runtime([MockDepsModule(), _UnavailableTrail()])
        reg = kit.registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()

            with pytest.raises(CoreException) as caught:
                await run_operation(reg, _key(DocumentKernelOp.CREATE), GadgetCreate(name="a"), ctx)

            assert caught.value.kind is ExceptionKind.INFRASTRUCTURE
            assert await ctx.document.query(GADGETS).count() == 0


class TestReads:
    async def test_a_read_is_recorded_without_gaining_a_transaction(self) -> None:
        seen: list[int] = []

        def _get(ctx: ExecutionContext) -> Any:
            async def _handler(args: DocumentIdDTO) -> GadgetRead:
                seen.append(ctx.tx_ctx.depth())
                return await ctx.document.query(GADGETS).get(args.id)

            return _handler

        kit = AggregateKit(
            spec=GADGETS,
            audit={DocumentKernelOp.GET: _audited("gadget.read")},
            handlers={DocumentKernelOp.GET: _get},
        )
        runtime = build_runtime([MockDepsModule(), AuditDepsModule(tx_route=_TX)])
        reg = kit.registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            made = await ctx.document.command(GADGETS).create(GadgetCreate(name="a"))
            await run_operation(reg, _key(DocumentKernelOp.GET), DocumentIdDTO(id=made.id), ctx)

            [row] = await _rows(ctx)

        assert (row.action, row.outcome, row.object_id) == (
            "gadget.read",
            AuditOutcome.ALLOWED,
            str(made.id),
        )
        assert seen == [0], "the arm opened a transaction around a read"


class TestTheDeclaration:
    @pytest.mark.parametrize(
        "op", ["no_such_op", SoftDeletionKernelOp.DELETE], ids=["unknown", "not-composed"]
    )
    def test_an_operation_the_kit_does_not_compose_is_refused(self, op: str) -> None:
        kit = _kit(**{op: _audited("gadget.gone")})

        with pytest.raises(CoreException) as caught:
            kit.registry(tx_route=_TX)

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert repr(op) in caught.value.summary
