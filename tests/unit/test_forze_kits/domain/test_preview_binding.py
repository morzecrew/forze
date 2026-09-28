"""A confirmation refuses when what it confirms is no longer what was shown.

The legs that matter pair the refusal with the write it stops: a stale confirmation that raises
after its handler already wrote would pass a test that only looks for the error. And the
fingerprint is compared across processes in production — a preview served by one replica is
confirmed on another — so its determinism is asserted across hash seeds, not within one run.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Final
from uuid import UUID

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze.testing import context_from_modules
from forze_kits.domain.preview import (
    FINGERPRINT_PREFIX,
    PREVIEW_CHANGED,
    PreviewBinding,
    ReviewedCommand,
    canonical_fingerprint,
)
from forze_mock import MockDepsModule

# ----------------------- #


class _Quote(Document):
    label: str
    amount: int
    hint: str = ""


class _QuoteCreate(BaseDTO):
    label: str
    amount: int
    hint: str = ""


class _QuoteUpdate(BaseDTO):
    label: str | None = None
    amount: int | None = None
    hint: str | None = None


class _QuoteRead(ReadDocument):
    label: str
    amount: int
    hint: str = ""


QUOTES = DocumentSpec(
    name="quotes",
    read=_QuoteRead,
    write=DocumentWriteTypes(domain=_Quote, create_cmd=_QuoteCreate, update_cmd=_QuoteUpdate),
)


class _Order(Document):
    quote_id: UUID


class _OrderCreate(BaseDTO):
    quote_id: UUID


class _OrderRead(ReadDocument):
    quote_id: UUID


ORDERS = DocumentSpec(
    name="orders",
    read=_OrderRead,
    write=DocumentWriteTypes(domain=_Order, create_cmd=_OrderCreate),
)


class _Projection(BaseModel):
    label: str
    amount: int
    hint: str


class _QuoteArgs(BaseDTO):
    quote_id: UUID


class _Confirm(_QuoteArgs, ReviewedCommand):
    pass


DEPTHS: Final[list[int]] = []


async def _project(ctx: ExecutionContext, args: _QuoteArgs) -> _Projection:
    DEPTHS.append(ctx.tx_ctx.depth())
    quote = await ctx.document.query(QUOTES).get(args.quote_id)

    return _Projection(label=quote.label, amount=quote.amount, hint=quote.hint)


QUOTE_PREVIEW = PreviewBinding(name="quote", projector=_project, exclude=frozenset({"hint"}))


@attrs.define(slots=True, kw_only=True, frozen=True)
class _PlaceOrder:
    ctx: ExecutionContext

    async def __call__(self, args: _Confirm) -> None:
        await self.ctx.document.command(ORDERS).create(_OrderCreate(quote_id=args.quote_id))


def _registry(*, tx: bool = True, isolation: IsolationLevel = IsolationLevel.SNAPSHOT) -> Any:
    binder = OperationRegistry(handlers={"confirm": lambda ctx: _PlaceOrder(ctx=ctx)}).bind(
        "confirm"
    )

    if tx:
        binder = binder.bind_tx().set_route("mock").finish()

    return QUOTE_PREVIEW.bind(binder, isolation=isolation).finish().freeze()


async def _quote(ctx: ExecutionContext) -> _QuoteRead:
    return await ctx.document.command(QUOTES).create(
        _QuoteCreate(label="12 widgets", amount=1200, hint="rendered 09:00")
    )


async def _orders(ctx: ExecutionContext) -> int:
    return await ctx.document.query(ORDERS).count()


async def _edit(ctx: ExecutionContext, quote: _QuoteRead, **update: Any) -> None:
    current = await ctx.document.query(QUOTES).get(quote.id)
    await ctx.document.command(QUOTES).update(quote.id, current.rev, _QuoteUpdate(**update))


# ....................... #


class TestTheRoundTrip:
    async def test_an_unchanged_preview_confirms(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        quote = await _quote(ctx)

        preview = await QUOTE_PREVIEW.reviewed(ctx, _QuoteArgs(quote_id=quote.id))
        await run_operation(
            _registry(),
            "confirm",
            _Confirm(quote_id=quote.id, fingerprint=preview.fingerprint),
            ctx,
        )

        assert preview.data == _Projection(label="12 widgets", amount=1200, hint="rendered 09:00")
        assert preview.fingerprint.startswith(f"{FINGERPRINT_PREFIX}:")
        assert await _orders(ctx) == 1

    @pytest.mark.parametrize("change", [{"label": "20 widgets"}, {"amount": 2000}])
    async def test_a_changed_projection_refuses_and_nothing_is_written(
        self, change: dict[str, Any]
    ) -> None:
        ctx = context_from_modules(MockDepsModule())
        quote = await _quote(ctx)
        preview = await QUOTE_PREVIEW.reviewed(ctx, _QuoteArgs(quote_id=quote.id))

        await _edit(ctx, quote, **change)

        with pytest.raises(CoreException) as caught:
            await run_operation(
                _registry(),
                "confirm",
                _Confirm(quote_id=quote.id, fingerprint=preview.fingerprint),
                ctx,
            )

        assert (caught.value.kind, caught.value.code) == (
            ExceptionKind.PRECONDITION,
            PREVIEW_CHANGED,
        )
        assert "'quote'" in caught.value.summary
        # Stale, not what changed: neither the old value nor the new one is named.
        for value in ("12 widgets", "20 widgets", "1200", "2000"):
            assert value not in str(caught.value)
        assert await _orders(ctx) == 0

    async def test_a_change_to_an_excluded_field_confirms(self) -> None:
        # Both sides read one declaration, so the field the preview ignored is the field the
        # check ignores.
        ctx = context_from_modules(MockDepsModule())
        quote = await _quote(ctx)
        preview = await QUOTE_PREVIEW.reviewed(ctx, _QuoteArgs(quote_id=quote.id))

        await _edit(ctx, quote, hint="rendered 09:05")
        await run_operation(
            _registry(),
            "confirm",
            _Confirm(quote_id=quote.id, fingerprint=preview.fingerprint),
            ctx,
        )

        assert await _orders(ctx) == 1

    async def test_a_tampered_fingerprint_only_fails_its_own_confirmation(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        quote = await _quote(ctx)

        with pytest.raises(CoreException) as caught:
            await run_operation(
                _registry(), "confirm", _Confirm(quote_id=quote.id, fingerprint="sha256-c1:0"), ctx
            )

        assert caught.value.code == PREVIEW_CHANGED
        assert await _orders(ctx) == 0

    async def test_the_check_reads_the_state_before_the_handler_changes_it(self) -> None:
        # A confirmation that marks what it confirms (here: relabels the quote) must be checked
        # against what the caller saw, not against what the handler just wrote.
        @attrs.define(slots=True, kw_only=True, frozen=True)
        class _MarkAndOrder:
            ctx: ExecutionContext

            async def __call__(self, args: _Confirm) -> None:
                current = await self.ctx.document.query(QUOTES).get(args.quote_id)
                await self.ctx.document.command(QUOTES).update(
                    args.quote_id, current.rev, _QuoteUpdate(label="confirmed")
                )
                await self.ctx.document.command(ORDERS).create(_OrderCreate(quote_id=args.quote_id))

        ctx = context_from_modules(MockDepsModule())
        quote = await _quote(ctx)
        preview = await QUOTE_PREVIEW.reviewed(ctx, _QuoteArgs(quote_id=quote.id))
        binder = (
            OperationRegistry(handlers={"confirm": lambda c: _MarkAndOrder(ctx=c)})
            .bind("confirm")
            .bind_tx()
            .set_route("mock")
            .finish()
        )
        reg = QUOTE_PREVIEW.bind(binder).finish().freeze()

        await run_operation(
            reg, "confirm", _Confirm(quote_id=quote.id, fingerprint=preview.fingerprint), ctx
        )

        assert await _orders(ctx) == 1

    async def test_the_check_runs_inside_the_transaction(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        quote = await _quote(ctx)
        preview = await QUOTE_PREVIEW.reviewed(ctx, _QuoteArgs(quote_id=quote.id))
        DEPTHS.clear()

        await run_operation(
            _registry(),
            "confirm",
            _Confirm(quote_id=quote.id, fingerprint=preview.fingerprint),
            ctx,
        )

        assert DEPTHS == [1]


class TestBinding:
    @pytest.mark.parametrize("level", [IsolationLevel.SNAPSHOT, IsolationLevel.SERIALIZABLE])
    def test_the_confirmation_runs_at_snapshot_isolation_or_stronger(
        self, level: IsolationLevel
    ) -> None:
        # Below snapshot, a write between the check and the handler's own reads is visible to
        # the handler: it would act on a state the caller never saw.
        ctx = context_from_modules(MockDepsModule())

        assert _registry(isolation=level).resolve("confirm", ctx).plan.tx.isolation is level

    def test_read_committed_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            _registry(isolation=IsolationLevel.READ_COMMITTED)

        assert caught.value.code == "preview_isolation_too_weak"

    def test_a_confirmation_without_a_transaction_is_refused_at_freeze(self) -> None:
        with pytest.raises(CoreException):
            _registry(tx=False)

    async def test_a_command_without_a_fingerprint_is_refused(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        quote = await _quote(ctx)

        with pytest.raises(CoreException) as caught:
            await run_operation(_registry(), "confirm", _QuoteArgs(quote_id=quote.id), ctx)

        assert caught.value.code == "preview_command_unbound"

    def test_an_exclusion_that_names_nothing_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            canonical_fingerprint(_Projection(label="a", amount=1, hint=""), exclude={"hnit"})

        assert caught.value.code == "preview_exclusion_unknown"

    @pytest.mark.parametrize(
        "build",
        [
            lambda: PreviewBinding(name=" ", projector=_project),
            lambda: PreviewBinding(name="q", projector=_project, exclude="hint"),  # type: ignore[arg-type]
        ],
        ids=["blank-name", "bare-string-exclude"],
    )
    def test_a_declaration_nobody_meant_is_refused(self, build: Any) -> None:
        with pytest.raises(CoreException):
            build()


# ....................... #


class _Colour(Enum):
    RED = "red"


class _Everything(BaseModel):
    text: str = "x"
    count: int = 3
    ratio: float = 0.5
    flag: bool = True
    when: datetime = datetime(2026, 9, 28, 12, 0, tzinfo=UTC)
    day: date = date(2026, 9, 28)
    span: timedelta = timedelta(minutes=5)
    ref: UUID = UUID("00000000-0000-4000-8000-000000000001")
    money: Decimal = Decimal("12.50")
    colour: _Colour = _Colour.RED
    tags: set[str] = {"alpha", "beta", "gamma", "delta"}
    lines: dict[str, int] = {"a": 1, "b": 2}
    steps: list[str] = ["first", "second"]
    nothing: None = None


class TestTheCanonicalForm:
    def test_the_form_is_pinned(self) -> None:
        # The form is the fingerprint's contract: RFC 0055 stores fingerprints and recomputes
        # them later. Any change to how a value renders must take a new prefix, and this digest
        # is what notices one that did not.
        assert canonical_fingerprint(_Everything()) == (
            "sha256-c1:cf268c9c5fea71d8cbcb489c5472d0b47dc675b7ed00570b5c8903bf4e61833b"
        )

    def test_key_and_set_order_do_not_move_it(self) -> None:
        one = _Everything(tags={"alpha", "beta", "gamma", "delta"}, lines={"a": 1, "b": 2})
        two = _Everything(tags={"delta", "gamma", "beta", "alpha"}, lines={"b": 2, "a": 1})

        assert canonical_fingerprint(one) == canonical_fingerprint(two)

    @pytest.mark.parametrize(
        "change",
        [
            {"text": "y"},
            {"count": 4},
            {"ratio": 0.25},
            {"flag": False},
            {"when": datetime(2026, 9, 28, 12, 1, tzinfo=UTC)},
            {"day": date(2026, 9, 29)},
            {"span": timedelta(minutes=6)},
            {"ref": UUID("00000000-0000-4000-8000-000000000002")},
            {"money": Decimal("12.51")},
            {"tags": {"alpha"}},
            {"lines": {"a": 1, "b": 3}},
            {"steps": ["second", "first"]},  # a list's order is data, unlike a set's
        ],
    )
    def test_every_value_type_moves_it_when_it_changes(self, change: dict[str, Any]) -> None:
        assert canonical_fingerprint(_Everything(**change)) != canonical_fingerprint(_Everything())

    def test_a_value_with_no_canonical_form_is_refused(self) -> None:
        class _Opaque(BaseModel):
            model_config = {"arbitrary_types_allowed": True}

            thing: object = object()

        with pytest.raises(CoreException) as caught:
            canonical_fingerprint(_Opaque())

        assert caught.value.code == "preview_projection_unrenderable"

    def test_the_same_projection_fingerprints_identically_in_every_process(self) -> None:
        # A preview served by one replica is confirmed on another; a hash-seeded set order
        # would refuse every such confirmation.
        script = textwrap.dedent(
            """
            from pydantic import BaseModel
            from forze_kits.domain.preview import canonical_fingerprint

            class Shown(BaseModel):
                tags: set[str]
                nested: dict[str, frozenset[str]]

            print(canonical_fingerprint(Shown(
                tags={"admin", "billing", "support", "auditor", "owner"},
                nested={"k": frozenset({"x", "y", "z", "w"})},
            )))
            """
        )

        digests = {
            seed: subprocess.run(
                [sys.executable, "-c", script],
                env={**os.environ, "PYTHONHASHSEED": seed},
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            for seed in ("0", "1", "12345", "99999")
        }

        assert len(set(digests.values())) == 1, f"fingerprint varies by hash seed: {digests}"
        assert next(iter(digests.values())).startswith("sha256-c1:")
