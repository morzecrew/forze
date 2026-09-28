"""An audited operation leaves one row, written where its outcome can be trusted.

The hooks split by outcome — an admitted operation's row rides its transaction, a failed or
denied one's is written after the rollback, a read's after it completes — so the legs that
matter assert the row *and* the business write together: a row for a write that rolled back,
or a committed write with no row, is the failure the split exists to prevent.
"""

from __future__ import annotations

from typing import Any, Final
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.audit import (
    AUDIT_DECLARATION,
    AUDIT_METADATA_REFUSED,
    AuditDepKey,
    AuditEntry,
    AuditObjectRef,
    AuditOutcome,
    AuditSpec,
)
from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.deps import Deps, DepsModule
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import BeforeStep
from forze.application.execution import ExecutionContext, check_wiring
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.hooks.audit import Audited
from forze.application.hooks.audit import plans as audit_plans
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze.testing import context_from_modules
from forze_kits.integrations.audit import AuditDepsModule, AuditRecord, audit_record_spec
from forze_mock import MockDepsModule

# ----------------------- #

USER: Final = uuid4()
AGENT: Final = uuid4()
OTHER: Final = uuid4()


class _Thing(Document):
    label: str


class _ThingCreate(BaseDTO):
    label: str


class _ThingRead(ReadDocument):
    label: str


THINGS = DocumentSpec(
    name="things",
    read=_ThingRead,
    write=DocumentWriteTypes(domain=_Thing, create_cmd=_ThingCreate),
)
TRAIL = audit_record_spec()
SPEC = AuditSpec(action="thing.create", allowed_metadata=frozenset({"label", "count"}))


class _Args(BaseModel):
    label: str = "a"
    actor: UUID | None = None
    boom: bool = False
    owner: UUID | None = None


@attrs.define(slots=True, kw_only=True, frozen=True)
class _Create:
    ctx: ExecutionContext

    async def __call__(self, args: _Args) -> _ThingRead:
        thing = await self.ctx.document.command(THINGS).create(_ThingCreate(label=args.label))

        if args.boom:
            raise RuntimeError("handler failed after its write")

        return thing


@attrs.define(slots=True, kw_only=True, frozen=True)
class _Read:
    ctx: ExecutionContext

    async def __call__(self, args: _Args) -> _Args:
        if args.boom:
            raise exc.not_found("no such thing")

        return args


def _guard(kind: ExceptionKind) -> BeforeStep:
    def _factory(_ctx: ExecutionContext) -> Any:
        async def _before(_args: Any) -> None:
            raise exc.authorization("no") if kind is ExceptionKind.AUTHORIZATION else (
                exc.authentication("who?")
            )

        return _before

    return BeforeStep(id="guard", factory=_factory)


def _registry(
    audited: Audited,
    *,
    read: bool = False,
    guard: ExceptionKind | None = None,
    transactional: bool = True,
    tx: bool = True,
) -> Any:
    handler = _Read if read else _Create
    binder = OperationRegistry(handlers={"op": lambda c: handler(ctx=c)}).bind("op")

    if read:
        binder = binder.as_query()

    if tx:
        binder = binder.bind_tx().set_route("mock").finish()

    if guard is not None:
        binder = binder.bind_outer().before(_guard(guard)).finish()

    return audited.bind(binder, transactional=transactional).finish().freeze()


def _ctx(*extra: DepsModule) -> ExecutionContext:
    return context_from_modules(MockDepsModule(), *(extra or (AuditDepsModule(tx_route="mock"),)))


async def _run(
    reg: Any,
    ctx: ExecutionContext,
    args: _Args | None = None,
    identity: AuthnIdentity | None = AuthnIdentity(principal_id=USER),
) -> Any:
    with ctx.inv_ctx.bind_identity(authn=identity):
        return await run_operation(reg, "op", args if args is not None else _Args(), ctx)


async def _rows(ctx: ExecutionContext) -> list[AuditRecord]:
    return list((await ctx.document.query(TRAIL).find_many()).hits)


async def _things(ctx: ExecutionContext) -> int:
    return await ctx.document.query(THINGS).count()


# ....................... #


class TestTheAllowlist:
    async def test_declared_metadata_is_recorded_as_scalars(self) -> None:
        ctx = _ctx()
        ref = uuid4()
        audited = Audited(
            spec=AuditSpec(
                action="thing.create",
                allowed_metadata=frozenset({"label", "count", "ratio", "flag", "ref", "none"}),
            ),
            metadata=lambda args, result: {
                "label": args.label,
                "count": 3,
                "ratio": 0.5,
                "flag": True,
                "ref": ref,
                "none": None,
            },
        )

        await _run(_registry(audited), ctx, _Args(label="x"))

        [row] = await _rows(ctx)
        assert row.metadata == {
            "label": "x",
            "count": 3,
            "ratio": 0.5,
            "flag": True,
            "ref": str(ref),
            "none": None,
        }

    async def test_an_undeclared_key_raises_and_nothing_commits(self) -> None:
        ctx = _ctx()
        audited = Audited(spec=SPEC, metadata=lambda args, result: {"comment": "free text"})

        with pytest.raises(CoreException) as caught:
            await _run(_registry(audited), ctx)

        assert caught.value.code == AUDIT_METADATA_REFUSED
        assert "comment" in caught.value.summary
        assert await _things(ctx) == 0

    @pytest.mark.parametrize(
        "value",
        [{"nested": 1}, [1], float("nan"), object()],
        ids=["mapping", "list", "nan", "object"],
    )
    def test_a_value_that_is_not_a_scalar_is_refused_by_type(self, value: object) -> None:
        with pytest.raises(CoreException) as caught:
            SPEC.check_metadata({"label": value})

        assert caught.value.code == AUDIT_METADATA_REFUSED
        assert type(value).__name__ in caught.value.summary

    def test_a_refusal_names_the_key_never_the_value(self) -> None:
        with pytest.raises(CoreException) as caught:
            SPEC.check_metadata({"label": {"card": "4111-1111"}})

        assert "4111" not in str(caught.value) and "4111" not in repr(caught.value.details)

    @pytest.mark.parametrize(
        "build",
        [
            lambda: AuditSpec(action="a", allowed_metadata=frozenset({"password"})),
            lambda: AuditSpec(action="a", allowed_metadata=frozenset({"api_key"})),
            lambda: AuditSpec(action="a", allowed_metadata="purpose"),  # type: ignore[arg-type]
            lambda: AuditSpec(action="a", allowed_metadata=frozenset({" "})),
            lambda: AuditSpec(action=" "),
            lambda: AuditSpec(action="a", audit_reads="Never"),  # type: ignore[arg-type]
            lambda: AuditSpec(action="a", on_failure="failclosed"),  # type: ignore[arg-type]
        ],
        ids=[
            "sensitive",
            "api-key",
            "bare-string",
            "blank-key",
            "blank-action",
            "reads-typo",
            "policy-typo",
        ],
    )
    def test_a_declaration_nobody_meant_is_refused(self, build: Any) -> None:
        with pytest.raises(CoreException) as caught:
            build()

        assert caught.value.code == AUDIT_DECLARATION


class TestEveryOutcome:
    async def test_an_admitted_write_commits_with_its_row(self) -> None:
        ctx = _ctx()
        audited = Audited(
            spec=SPEC, object_ref=lambda args, result: AuditObjectRef(type="thing", id=str(result.id))
        )

        thing = await _run(_registry(audited), ctx)

        [row] = await _rows(ctx)
        assert (row.action, row.outcome, row.object_type, row.object_id) == (
            "thing.create",
            AuditOutcome.ALLOWED,
            "thing",
            str(thing.id),
        )
        assert await _things(ctx) == 1

    async def test_a_failed_write_rolls_back_and_its_row_survives(self) -> None:
        # The failed row is written after the rollback; written inside the transaction it
        # would have been rolled back with the write it describes.
        ctx = _ctx()

        with pytest.raises(RuntimeError, match="handler failed"):
            await _run(_registry(Audited(spec=SPEC)), ctx, _Args(boom=True))

        assert [row.outcome for row in await _rows(ctx)] == [AuditOutcome.FAILED]
        assert await _things(ctx) == 0

    @pytest.mark.parametrize(
        ("kind", "identity"),
        [
            (ExceptionKind.AUTHORIZATION, AuthnIdentity(principal_id=USER)),
            (ExceptionKind.AUTHENTICATION, None),
        ],
        ids=["authz", "authn"],
    )
    async def test_a_guard_denial_is_recorded(
        self, kind: ExceptionKind, identity: AuthnIdentity | None
    ) -> None:
        # The Finally claim: a guard refuses before the handler and before the transaction,
        # and only the outer finally hook sees it.
        ctx = _ctx()

        with pytest.raises(CoreException) as caught:
            await _run(_registry(Audited(spec=SPEC), guard=kind), ctx, identity=identity)

        assert caught.value.kind is kind
        [row] = await _rows(ctx)
        assert row.outcome is AuditOutcome.DENIED
        assert row.actor_id == (identity.principal_id if identity else None)
        assert await _things(ctx) == 0

    async def test_without_a_transaction_the_row_is_written_after_return(self) -> None:
        ctx = _ctx()

        await _run(_registry(Audited(spec=SPEC), transactional=False, tx=False), ctx)

        assert [row.outcome for row in await _rows(ctx)] == [AuditOutcome.ALLOWED]

    def test_a_transactional_binding_without_a_route_is_refused(self) -> None:
        with pytest.raises(CoreException):
            _registry(Audited(spec=SPEC), tx=False)


class TestTheActor:
    async def test_the_payload_cannot_name_the_actor(self) -> None:
        ctx = _ctx()

        await _run(_registry(Audited(spec=SPEC)), ctx, _Args(actor=OTHER))

        [row] = await _rows(ctx)
        assert (row.actor_id, row.subject_id) == (USER, USER)

    async def test_a_delegated_call_records_the_agent_and_the_user(self) -> None:
        ctx = _ctx()
        delegated = AuthnIdentity(principal_id=USER, actor=AuthnIdentity(principal_id=AGENT))

        await _run(_registry(Audited(spec=SPEC)), ctx, identity=delegated)

        [row] = await _rows(ctx)
        assert (row.actor_id, row.subject_id) == (AGENT, USER)


class TestReads:
    READ = AuditSpec(action="thing.read")

    @staticmethod
    def _audited(spec: AuditSpec | None = None, *, owned: bool = True) -> Audited:
        return Audited(
            spec=spec or TestReads.READ,
            owner=(lambda args, result: result.owner) if owned else None,
        )

    @pytest.mark.parametrize(
        ("owner", "identity", "recorded"),
        [
            (OTHER, AuthnIdentity(principal_id=USER), True),
            (USER, AuthnIdentity(principal_id=USER), False),
            (USER, AuthnIdentity(principal_id=USER, actor=AuthnIdentity(principal_id=AGENT)), True),
        ],
        ids=["someone-elses", "own", "own-but-delegated"],
    )
    async def test_an_admitted_read_is_recorded_unless_it_is_ones_own(
        self, owner: UUID, identity: AuthnIdentity, recorded: bool
    ) -> None:
        ctx = _ctx()

        await _run(_registry(self._audited(), read=True), ctx, _Args(owner=owner), identity)

        rows = await _rows(ctx)
        assert [row.outcome for row in rows] == ([AuditOutcome.ALLOWED] if recorded else [])

    async def test_a_read_resolved_inside_another_read_is_recorded(self) -> None:
        # The audit port is built when the operation is resolved. Resolved inside another
        # read, that happens under the read-only flag, and the audit collection's write port
        # is the one it may still take.
        ctx = _ctx()
        inner = _registry(self._audited(owned=False), read=True)

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class _Outer:
            ctx: ExecutionContext

            async def __call__(self, args: _Args) -> Any:
                return await run_operation(inner, "op", args, self.ctx)

        outer = (
            OperationRegistry(handlers={"outer": lambda c: _Outer(ctx=c)})
            .bind("outer")
            .as_query()
            .finish()
            .freeze()
        )

        with ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=USER)):
            await run_operation(outer, "outer", _Args(), ctx)

        assert [row.action for row in await _rows(ctx)] == ["thing.read"]

    async def test_without_an_owner_every_admitted_read_is_recorded(self) -> None:
        ctx = _ctx()

        await _run(_registry(self._audited(owned=False), read=True), ctx, _Args(owner=USER))

        assert len(await _rows(ctx)) == 1

    @pytest.mark.parametrize("case", ["refused", "failed", "never"])
    async def test_a_refused_failed_or_undeclared_read_records_nothing(self, case: str) -> None:
        ctx = _ctx()
        spec = AuditSpec(action="thing.read", audit_reads="never") if case == "never" else None
        reg = _registry(
            self._audited(spec, owned=False),
            read=True,
            guard=ExceptionKind.AUTHORIZATION if case == "refused" else None,
        )

        try:
            await _run(reg, ctx, _Args(boom=case == "failed"))

        except CoreException:
            assert case != "never"

        assert await _rows(ctx) == []


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _BrokenAudit:
    async def record(self, entry: AuditEntry) -> None:
        raise exc.infrastructure("audit store down")


@attrs.define(slots=True, kw_only=True, frozen=True)
class _BrokenAuditModule(DepsModule):
    def __call__(self) -> Deps:
        return Deps.plain({AuditDepKey: lambda ctx: _BrokenAudit()})


class TestWhenTheAuditWriteFails:
    async def test_by_default_the_operation_fails_and_its_write_rolls_back(self) -> None:
        ctx = _ctx(_BrokenAuditModule())

        with pytest.raises(CoreException, match="audit store down"):
            await _run(_registry(Audited(spec=SPEC)), ctx)

        assert await _things(ctx) == 0

    async def test_ignore_commits_the_write_and_warns(self, monkeypatch: pytest.MonkeyPatch) -> None:
        spy = MagicMock()
        monkeypatch.setattr(audit_plans, "logger", spy)
        ctx = _ctx(_BrokenAuditModule())
        spec = AuditSpec(action="thing.create", on_failure="ignore")

        await _run(_registry(Audited(spec=spec)), ctx)

        assert await _things(ctx) == 1
        spy.warning.assert_called_once()
        assert spy.warning.call_args.args == ("audit.write_failed",)

    async def test_ignore_covers_a_callable_that_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        spy = MagicMock()
        monkeypatch.setattr(audit_plans, "logger", spy)
        ctx = _ctx()

        def _broken(args: Any, result: Any) -> dict[str, object]:
            raise KeyError("label")

        spec = AuditSpec(action="thing.create", on_failure="ignore")
        await _run(_registry(Audited(spec=spec, metadata=_broken)), ctx)

        assert await _things(ctx) == 1
        spy.warning.assert_called_once()

    async def test_ignore_never_covers_a_metadata_refusal(self) -> None:
        ctx = _ctx()
        spec = AuditSpec(action="thing.create", on_failure="ignore")
        audited = Audited(spec=spec, metadata=lambda args, result: {"comment": "free text"})

        with pytest.raises(CoreException) as caught:
            await _run(_registry(audited), ctx)

        assert caught.value.code == AUDIT_METADATA_REFUSED
        assert await _things(ctx) == 0

    async def test_a_denial_keeps_its_row_when_the_metadata_needs_a_result(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The RFC's own example reads the result; a denial has none. The event is kept.
        spy = MagicMock()
        monkeypatch.setattr(audit_plans, "logger", spy)
        ctx = _ctx()
        audited = Audited(spec=SPEC, metadata=lambda args, result: {"label": result.label})

        with pytest.raises(CoreException):
            await _run(_registry(audited, guard=ExceptionKind.AUTHORIZATION), ctx)

        [row] = await _rows(ctx)
        assert (row.outcome, row.metadata) == (AuditOutcome.DENIED, {})
        assert spy.error.call_args.args == ("audit.metadata_failed",)

    async def test_a_failing_operation_keeps_its_own_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The failed row is written from a finally hook while the operation's exception is in
        # flight; an audit error raised there would replace it.
        spy = MagicMock()
        monkeypatch.setattr(audit_plans, "logger", spy)
        ctx = _ctx(_BrokenAuditModule())

        with pytest.raises(RuntimeError, match="handler failed"):
            await _run(_registry(Audited(spec=SPEC)), ctx, _Args(boom=True))

        spy.error.assert_called_once()


class TestWiring:
    @pytest.mark.parametrize("transactional", [True, False])
    def test_an_unwired_audit_port_fails_the_wiring_check(self, transactional: bool) -> None:
        reg = _registry(Audited(spec=SPEC), transactional=transactional, tx=transactional)
        report = check_wiring(reg, lambda: context_from_modules(MockDepsModule()))

        assert not report.ok
        assert [failure.op for failure in report.failures] == ["op"]

    def test_the_collection_refuses_sealing_what_the_trail_is_queried_by(self) -> None:
        with pytest.raises(CoreException, match="action"):
            audit_record_spec(encryption=FieldEncryption(encrypted=frozenset({"action"})))

        audit_record_spec(encryption=FieldEncryption(encrypted=frozenset({"metadata"})))
