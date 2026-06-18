"""Unit tests for :class:`FrozenDeps` resolution."""

import attrs
import pytest

from forze.application.contracts.deps import DepKey
from forze.application.execution import Deps, DepsRegistry, ExecutionContext, FrozenDeps
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


@attrs.define(slots=True, frozen=True)
class _ValueSpec:
    """Spec with value equality, like the real frozen-attrs spec types."""

    name: str


def _resolve(*registration: Deps, **freeze_kw) -> "FrozenDeps":

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


class TestFrozenDepsPortCache:
    def _counting_reg(self) -> tuple[Deps, list[int]]:
        calls = [0]

        def factory(ctx: ExecutionContext, spec: _NamedSpec) -> object:
            calls[0] += 1
            return object()

        return Deps.routed({_A: {_SPEC_A.name: factory}}), calls

    def _ctx(
        self,
        reg: Deps,
        *,
        cache_ports: bool = True,
        tracing: bool = False,
    ) -> ExecutionContext:
        fd = (
            DepsRegistry.from_deps(reg)
            .with_tracing(resolution=tracing)
            .freeze()
            .resolve()
        )

        return ExecutionContext(deps=fd, cache_ports=cache_ports)

    def test_caching_on_reuses_resolved_port(self) -> None:
        reg, calls = self._counting_reg()
        ctx = self._ctx(reg)

        first = ctx.deps.resolve_configurable(ctx, _A, _SPEC_A, route=_SPEC_A.name)
        second = ctx.deps.resolve_configurable(ctx, _A, _SPEC_A, route=_SPEC_A.name)

        assert first is second
        assert calls[0] == 1

    def test_caching_off_rebuilds_each_time(self) -> None:
        reg, calls = self._counting_reg()
        ctx = self._ctx(reg, cache_ports=False)

        first = ctx.deps.resolve_configurable(ctx, _A, _SPEC_A, route=_SPEC_A.name)
        second = ctx.deps.resolve_configurable(ctx, _A, _SPEC_A, route=_SPEC_A.name)

        assert first is not second
        assert calls[0] == 2

    def test_different_spec_on_same_route_rebuilds(self) -> None:
        # _NamedSpec compares by identity, so a fresh instance is a non-equal
        # spec on the same key and must rebuild.
        reg, calls = self._counting_reg()
        ctx = self._ctx(reg)
        other_spec = _NamedSpec("a")  # same route name, different object

        first = ctx.deps.resolve_configurable(ctx, _A, _SPEC_A, route=_SPEC_A.name)
        second = ctx.deps.resolve_configurable(ctx, _A, other_spec, route=_SPEC_A.name)

        assert first is not second
        assert calls[0] == 2

    def test_structurally_equal_spec_hits_cache(self) -> None:
        # Per-call-constructed specs with value equality must reuse the cached
        # port instead of rebuilding on every resolve.
        calls = [0]

        def factory(ctx: ExecutionContext, spec: _ValueSpec) -> object:
            calls[0] += 1
            return object()

        reg = Deps.routed({_A: {"a": factory}})
        ctx = self._ctx(reg)

        first = ctx.deps.resolve_configurable(ctx, _A, _ValueSpec(name="a"), route="a")
        second = ctx.deps.resolve_configurable(ctx, _A, _ValueSpec(name="a"), route="a")

        assert first is second
        assert calls[0] == 1

    def test_structurally_different_value_spec_rebuilds(self) -> None:
        calls = [0]

        def factory(ctx: ExecutionContext, spec: _ValueSpec) -> object:
            calls[0] += 1
            return object()

        reg = Deps.routed({_A: {"a": factory}})
        ctx = self._ctx(reg)

        first = ctx.deps.resolve_configurable(ctx, _A, _ValueSpec(name="a"), route="a")
        second = ctx.deps.resolve_configurable(ctx, _A, _ValueSpec(name="b"), route="a")

        assert first is not second
        assert calls[0] == 2

    def test_real_spec_types_have_value_equality(self) -> None:
        # Locks the assumption behind equality-based port caching: structurally
        # identical spec literals compare equal.
        from pydantic import BaseModel

        from forze.application.contracts.document import DocumentSpec

        class _Read(BaseModel):
            id: str

        assert DocumentSpec(name="doc", read=_Read) == DocumentSpec(
            name="doc",
            read=_Read,
        )
        assert DocumentSpec(name="doc", read=_Read) != DocumentSpec(
            name="other",
            read=_Read,
        )

    def test_resolution_tracing_bypasses_cache(self) -> None:
        reg, calls = self._counting_reg()
        ctx = self._ctx(reg, tracing=True)

        first = ctx.deps.resolve_configurable(ctx, _A, _SPEC_A, route=_SPEC_A.name)
        second = ctx.deps.resolve_configurable(ctx, _A, _SPEC_A, route=_SPEC_A.name)

        assert first is not second
        assert calls[0] == 2

    def test_ambient_interceptors_bypass_cache(self) -> None:
        # A port resolved + cached BEFORE an ambient interceptor chain is bound would otherwise
        # be reused bare and skip the chain (the DST cooperative/fault/partition foot-gun).
        # While a chain is bound the cache is bypassed, so each resolve rebuilds and rewraps.
        import attrs

        from forze.application.execution.interception import (
            PortCall,
            PortNext,
            bind_interceptors,
        )

        @attrs.define(slots=True, frozen=True)
        class _Noop:
            async def around(self, call: PortCall, nxt: PortNext) -> object:
                return await nxt(call)

        reg, calls = self._counting_reg()
        ctx = self._ctx(reg)

        ctx.deps.resolve_configurable(ctx, _A, _SPEC_A, route=_SPEC_A.name)  # cached bare
        assert calls[0] == 1

        with bind_interceptors(_Noop()):
            first = ctx.deps.resolve_configurable(ctx, _A, _SPEC_A, route=_SPEC_A.name)
            second = ctx.deps.resolve_configurable(ctx, _A, _SPEC_A, route=_SPEC_A.name)

            assert first is not second  # the stale pre-binding entry is not reused
            assert calls[0] == 3  # both rebuilt under the binding

        # Outside the binding the cache is in force again (zero cost in production) — it serves
        # the original cached entry, no rebuild.
        third = ctx.deps.resolve_configurable(ctx, _A, _SPEC_A, route=_SPEC_A.name)
        fourth = ctx.deps.resolve_configurable(ctx, _A, _SPEC_A, route=_SPEC_A.name)
        assert third is fourth and calls[0] == 3

    def test_cache_is_per_scope(self) -> None:
        reg, _ = self._counting_reg()
        ctx_a = self._ctx(reg)
        ctx_b = self._ctx(reg)

        port_a = ctx_a.deps.resolve_configurable(ctx_a, _A, _SPEC_A, route=_SPEC_A.name)
        port_b = ctx_b.deps.resolve_configurable(ctx_b, _A, _SPEC_A, route=_SPEC_A.name)

        assert port_a is not port_b


class TestFrozenDepsSimpleCache:
    """Per-scope memoization of ``resolve_simple`` (gated by the same ``cache_ports`` flag)."""

    def _counting_reg(self) -> tuple[Deps, list[int]]:
        calls = [0]

        def factory(ctx: ExecutionContext) -> object:
            calls[0] += 1
            return object()

        return Deps.plain({_CLIENT: factory}), calls

    def _ctx(
        self,
        reg: Deps,
        *,
        cache_ports: bool = True,
        resolution_tracing: bool = False,
        runtime_tracing: bool = False,
    ) -> ExecutionContext:
        fd = (
            DepsRegistry.from_deps(reg)
            .with_tracing(resolution=resolution_tracing, runtime=runtime_tracing)
            .freeze()
            .resolve()
        )

        return ExecutionContext(deps=fd, cache_ports=cache_ports)

    def test_caching_on_reuses_resolved_dep(self) -> None:
        reg, calls = self._counting_reg()
        ctx = self._ctx(reg)

        first = ctx.deps.resolve_simple(ctx, _CLIENT)
        second = ctx.deps.resolve_simple(ctx, _CLIENT)

        assert first is second
        assert calls[0] == 1

    def test_caching_off_rebuilds_each_time(self) -> None:
        reg, calls = self._counting_reg()
        ctx = self._ctx(reg, cache_ports=False)

        first = ctx.deps.resolve_simple(ctx, _CLIENT)
        second = ctx.deps.resolve_simple(ctx, _CLIENT)

        assert first is not second
        assert calls[0] == 2

    def test_resolution_tracing_bypasses_cache(self) -> None:
        reg, calls = self._counting_reg()
        ctx = self._ctx(reg, resolution_tracing=True)

        first = ctx.deps.resolve_simple(ctx, _CLIENT)
        second = ctx.deps.resolve_simple(ctx, _CLIENT)

        assert first is not second
        assert calls[0] == 2

    def test_runtime_tracer_records_on_cache_hit(self) -> None:
        reg, calls = self._counting_reg()
        ctx = self._ctx(reg, runtime_tracing=True)

        first = ctx.deps.resolve_simple(ctx, _CLIENT)
        second = ctx.deps.resolve_simple(ctx, _CLIENT)

        # Still cached (factory built once), but the runtime tracer records each access —
        # including the cache hit.
        assert first is second
        assert calls[0] == 1

        trace = ctx.deps.runtime_trace()
        assert trace is not None
        assert [e.op for e in trace.events].count("resolve") == 2

    def test_cache_is_per_scope(self) -> None:
        reg, _ = self._counting_reg()
        ctx_a = self._ctx(reg)
        ctx_b = self._ctx(reg)

        a = ctx_a.deps.resolve_simple(ctx_a, _CLIENT)
        b = ctx_b.deps.resolve_simple(ctx_b, _CLIENT)

        assert a is not b


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
