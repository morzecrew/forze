"""Dry-run wiring check: resolve every operation up front to catch missing deps.

These exercise :func:`check_wiring`, which resolves each registered operation against a
throwaway context so a missing/misrouted dependency surfaces at test/startup time rather
than on the operation's first live call.
"""

from __future__ import annotations

import attrs
import pytest

from forze.application.contracts.deps import DepKey
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import check_wiring
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockState
from forze_mock.execution import MockStateDepKey
from tests.support.execution_context import context_from_modules

# ----------------------- #


class Thing(Document):
    name: str = "x"


class ThingCreate(CreateDocumentCmd):
    name: str = "x"


class ThingUpdate(BaseDTO):
    name: str | None = None


class ThingRead(ReadDocument):
    name: str


SPEC = DocumentSpec(
    name="things",
    read=ThingRead,
    write=DocumentWriteTypes(
        domain=Thing, create_cmd=ThingCreate, update_cmd=ThingUpdate
    ),
)

_UNREGISTERED: DepKey[str] = DepKey("not-wired")


@attrs.define(slots=True)
class _HoldsDep(Handler[None, str]):
    # Holds a dependency acquired eagerly in the factory (the common kit pattern).
    dep: object

    async def __call__(self, _args: None) -> str:
        return "built"


def _mock_ctx() -> ExecutionContext:
    return context_from_modules(MockDepsModule(state=MockState()))


# ....................... #


class TestCheckWiring:
    def test_all_registered_deps_resolve_cleanly(self) -> None:
        reg = OperationRegistry(
            handlers={
                "a": lambda c: _HoldsDep(dep=c.deps.provide(MockStateDepKey)),
                "b": lambda c: _HoldsDep(dep=c.document.query(SPEC)),
            }
        ).freeze()

        report = check_wiring(reg, _mock_ctx)

        assert report.ok is True
        assert report.failures == ()
        assert set(report.checked) == {"a", "b"}

    def test_missing_dep_is_reported_as_a_configuration_failure(self) -> None:
        reg = OperationRegistry(
            handlers={"a": lambda c: _HoldsDep(dep=c.deps.provide(_UNREGISTERED))},
        ).freeze()

        report = check_wiring(reg, _mock_ctx)

        assert report.ok is False
        assert len(report.failures) == 1
        failure = report.failures[0]
        assert failure.op == "a"
        assert failure.kind is ExceptionKind.CONFIGURATION
        assert failure.is_wiring is True
        assert "not-wired" in failure.message

    def test_every_broken_op_is_reported_not_just_the_first(self) -> None:
        reg = OperationRegistry(
            handlers={
                "ok": lambda c: _HoldsDep(dep=c.deps.provide(MockStateDepKey)),
                "bad-1": lambda c: _HoldsDep(dep=c.deps.provide(_UNREGISTERED)),
                "bad-2": lambda c: _HoldsDep(dep=c.deps.provide(_UNREGISTERED)),
            }
        ).freeze()

        report = check_wiring(reg, _mock_ctx)

        assert {f.op for f in report.failures} == {"bad-1", "bad-2"}

    def test_query_op_acquiring_a_write_port_is_caught(self) -> None:
        # A QUERY handler that eagerly acquires a command (write) port trips the
        # read-only guard at resolve time — check_wiring surfaces it because it
        # routes through resolve(), which builds QUERY handlers read-only.
        reg = (
            OperationRegistry(
                handlers={"q": lambda c: _HoldsDep(dep=c.document.command(SPEC))}
            )
            .bind("q")
            .as_query()
            .finish()
            .freeze()
        )

        report = check_wiring(reg, _mock_ctx)

        assert report.ok is False
        assert len(report.failures) == 1
        assert report.failures[0].op == "q"
        assert report.failures[0].kind is ExceptionKind.PRECONDITION

    def test_raise_if_failed_aggregates_into_one_error(self) -> None:
        reg = OperationRegistry(
            handlers={
                "bad-1": lambda c: _HoldsDep(dep=c.deps.provide(_UNREGISTERED)),
                "bad-2": lambda c: _HoldsDep(dep=c.deps.provide(_UNREGISTERED)),
            }
        ).freeze()

        report = check_wiring(reg, _mock_ctx)

        with pytest.raises(CoreException) as ei:
            report.raise_if_failed()

        assert ei.value.kind is ExceptionKind.CONFIGURATION
        assert "bad-1" in ei.value.summary
        assert "bad-2" in ei.value.summary

    def test_raise_if_failed_is_a_noop_when_clean(self) -> None:
        reg = OperationRegistry(
            handlers={"a": lambda c: _HoldsDep(dep=c.deps.provide(MockStateDepKey))},
        ).freeze()

        check_wiring(reg, _mock_ctx).raise_if_failed()  # does not raise

    def test_ops_argument_limits_the_checked_set(self) -> None:
        reg = OperationRegistry(
            handlers={
                "a": lambda c: _HoldsDep(dep=c.deps.provide(MockStateDepKey)),
                "bad": lambda c: _HoldsDep(dep=c.deps.provide(_UNREGISTERED)),
            }
        ).freeze()

        report = check_wiring(reg, _mock_ctx, ops=["a"])

        assert report.checked == ("a",)
        assert report.ok is True
