"""Unit tests for forze.application.execution.operations.facade."""

from typing import Any

import attrs
import pytest

from forze.application.contracts.execution import Handler
from forze.application.execution.operations.facade import (
    OperationFacade,
    OperationFacadeFactory,
    facade_op,
)
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException
from forze.base.primitives import StrKeyNamespace
from tests.support.execution_context import context_from_deps

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class StubHandler(Handler[str, str]):
    async def __call__(self, args: str) -> str:
        return f"ok:{args}"


_OPS = StrKeyNamespace(prefix="test")


class MinimalFacade(OperationFacade):
    namespace_required = True
    namespace: StrKeyNamespace
    get = facade_op("get", uc=StubHandler)


class TestOperationFacade:
    def test_resolve_returns_operation(self, stub_ctx) -> None:
        reg = OperationRegistry(
            handlers={_OPS.key("get"): lambda _ctx: StubHandler()},
        ).freeze()
        facade = MinimalFacade(ctx=stub_ctx, registry=reg, namespace=_OPS)
        op = facade.resolve("get")
        assert op is not None

    def test_facade_op_returns_resolved_on_instance(self, stub_ctx) -> None:
        reg = OperationRegistry(
            handlers={_OPS.key("get"): lambda _ctx: StubHandler()},
        ).freeze()
        facade = MinimalFacade(ctx=stub_ctx, registry=reg, namespace=_OPS)
        op = facade.get
        assert op is not None

    def test_class_access_returns_descriptor(self) -> None:
        assert isinstance(MinimalFacade.get, facade_op)
        assert MinimalFacade.get.op == "get"

    def test_missing_namespace_raises(self, stub_ctx) -> None:
        class MissingNamespaceFacade(OperationFacade):
            namespace_required = True
            get = facade_op("get", uc=StubHandler)

        reg = OperationRegistry(
            handlers={"get": lambda _ctx: StubHandler()},
        ).freeze()

        with pytest.raises(CoreException, match="requires namespace"):
            MissingNamespaceFacade(ctx=stub_ctx, registry=reg)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _StatefulFacade(OperationFacade):
    calls: list[str] = attrs.field(factory=list)
    """A field a subclass might fill per call."""


class TestOperationFacadeFactory:
    def _registry(self) -> Any:
        return OperationRegistry(
            handlers={_OPS.key("get"): lambda _ctx: StubHandler()},
        ).freeze()

    def test_reuses_the_facade_while_the_context_stays_current(self, stub_ctx) -> None:
        factory = OperationFacadeFactory(MinimalFacade, self._registry(), lambda: stub_ctx, _OPS)

        first = factory()

        assert factory() is first
        assert (first.ctx, first.namespace) == (stub_ctx, _OPS)

    def test_builds_a_new_facade_for_another_context(self, stub_deps) -> None:
        contexts = [context_from_deps(stub_deps), context_from_deps(stub_deps)]
        current = iter([*contexts, contexts[0]])
        factory = OperationFacadeFactory(
            MinimalFacade, self._registry(), lambda: next(current), _OPS
        )

        built = [factory(), factory(), factory()]

        assert [f.ctx for f in built] == [contexts[0], contexts[1], contexts[0]]
        assert built[0] is not built[2]

    def test_a_facade_with_its_own_fields_is_never_reused(self, stub_ctx) -> None:
        factory = OperationFacadeFactory(_StatefulFacade, self._registry(), lambda: stub_ctx)

        assert factory() is not factory()

    def test_factory_equality_ignores_the_reused_facade(self, stub_ctx) -> None:
        a = OperationFacadeFactory(MinimalFacade, self._registry(), lambda: stub_ctx, _OPS)
        b = attrs.evolve(a)
        a()

        assert a == b
        assert "_last" not in repr(a)

    def test_missing_namespace_raises_on_every_call(self, stub_ctx) -> None:
        factory = OperationFacadeFactory(MinimalFacade, self._registry(), lambda: stub_ctx)

        for _ in range(2):
            with pytest.raises(CoreException, match="requires namespace"):
                factory()
