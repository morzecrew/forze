"""Unit tests for ExecutionContext.dep() resolution."""

import pytest

from forze.application.contracts.deps import DepKey
from forze.application.execution import Deps
from forze.application.execution import ExecutionContext

# ----------------------- #


class TestExecutionContextDep:
    """Tests for ExecutionContext.dep() dependency resolution."""

    def test_dep_resolves_registered(self) -> None:
        deps = Deps(deps={DepKey[str]("foo"): "bar"})
        ctx = ExecutionContext(deps=deps)
        assert ctx.dep(DepKey[str]("foo")) == "bar"

    def test_dep_resolves_typed(self) -> None:
        deps = Deps(deps={DepKey[int]("num"): 42})
        ctx = ExecutionContext(deps=deps)
        result: int = ctx.dep(DepKey[int]("num"))
        assert result == 42

    def test_dep_missing_raises(self) -> None:
        from forze.base.errors import CoreError

        ctx = ExecutionContext(deps=Deps())
        with pytest.raises(CoreError, match="not found"):
            ctx.dep(DepKey[str]("missing"))
