"""Unit tests for :class:`FrozenDeps` resolution."""

import pytest

from forze.application.contracts.deps import DepKey
from forze.application.execution import Deps, DepsRegistry, ExecutionContext
from forze.application.execution.deps import DepsResolutionTrace
from forze.application.execution.deps.resolution import frame_for
from forze.application.execution.deps.store import ProviderStore
from forze.base.exceptions import CoreException
from tests.support.execution_context import context_from_deps

_A = DepKey[str]("a")
_B = DepKey[str]("b")
_R = DepKey[str]("r")
_CLIENT = DepKey[str]("client")


class _NamedSpec:
    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name


_SPEC_A = _NamedSpec("a")
_SPEC_B = _NamedSpec("b")


def _resolve(*registration: Deps, **freeze_kw) -> "FrozenDeps":
    from forze.application.execution import FrozenDeps

    return DepsRegistry.from_deps(*registration).freeze(**freeze_kw).resolve()


class TestFrozenDepsProvide:
    def test_routed_group_expands_provider_across_routes(self) -> None:
        reg = Deps.routed_group({_A: "p"}, routes=frozenset({"x", "y"}))
        resolved = _resolve(reg)

        assert resolved.provide(_A, route="x") == "p"
        assert resolved.provide(_A, route="y") == "p"

    def test_plain_not_found_raises(self) -> None:
        with pytest.raises(CoreException, match="Plain dependency"):
            _resolve(Deps.plain({})).provide(_A)

    def test_routed_not_found_fallback_to_plain(self) -> None:
        resolved = _resolve(
            Deps.plain({_A: "plain"}).merge(Deps.routed({_R: {"z": "z"}})),
        )

        assert resolved.provide(_A, route="missing", fallback_to_plain=True) == "plain"

    def test_same_key_plain_and_routed_in_one_store(self) -> None:
        reg = Deps(
            store=ProviderStore(
                plain_deps={_A: "plain"},
                routed_deps={_A: {"z": "routed"}},
            ),
        )
        resolved = _resolve(reg)

        assert resolved.provide(_A, route="missing", fallback_to_plain=True) == "plain"


class TestFrozenDepsCycleDetection:
    def test_provide_same_frame_while_scope_active_raises(self) -> None:
        resolved = _resolve(Deps.plain({_A: "value"}))

        with resolved.resolution_scope(_A):
            with pytest.raises(CoreException, match="Cyclic dependency resolution"):
                resolved.provide(_A)

    def test_factory_chain_a_to_b_to_a_raises(self) -> None:
        def factory_a(ctx: ExecutionContext, spec: _NamedSpec) -> str:
            ctx.deps.resolve_configurable(ctx, _B, _SPEC_B, route=_SPEC_B.name)
            return "a"

        def factory_b(ctx: ExecutionContext, spec: _NamedSpec) -> str:
            ctx.deps.resolve_configurable(ctx, _A, _SPEC_A, route=_SPEC_A.name)
            return "b"

        reg = Deps.routed(
            {
                _A: {_SPEC_A.name: factory_a},
                _B: {_SPEC_B.name: factory_b},
            },
        )
        resolved = _resolve(reg)
        ctx = context_from_deps(reg)

        with pytest.raises(CoreException, match="Cyclic dependency resolution"):
            resolved.resolve_configurable(ctx, _A, _SPEC_A, route="a")


class TestFrozenDepsResolutionTrace:
    def test_trace_records_scope_and_provide_edges(self) -> None:
        resolved = (
            DepsRegistry.from_deps(Deps.plain({_A: "outer", _B: "inner"}))
            .with_tracing(resolution=True)
            .freeze()
            .resolve()
        )
        frame_a = frame_for(_A, None)
        frame_b = frame_for(_B, None)

        with resolved.resolution_scope(_A):
            resolved.provide(_B)

        trace = resolved.resolution_trace()

        assert trace is not None
        assert (frame_a, frame_b) in trace.edges
