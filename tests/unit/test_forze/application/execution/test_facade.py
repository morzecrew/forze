"""Unit tests for forze.application.execution.facade."""

import attrs
import pytest

from forze.application.contracts.execution import Handler
from forze.application.execution.facade import OperationFacade, facade_op
from forze.application.execution.registry import OperationRegistry
from forze.base.errors import CoreError
from forze.base.primitives import StrKeyNamespace

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

        with pytest.raises(CoreError, match="requires namespace"):
            MissingNamespaceFacade(ctx=stub_ctx, registry=reg)
