"""Unit tests for authoring :class:`DepsRegistry`."""

import pytest

from forze.application.contracts.deps import DepKey
from forze.application.execution import Deps, DepsRegistry, FrozenDepsRegistry
from forze.base.exceptions import CoreException

_A = DepKey[str]("a")
_B = DepKey[str]("b")


class TestDepsRegistryAuthoring:
    def test_freeze_empty_returns_empty_frozen_registry(self) -> None:
        frozen = DepsRegistry().freeze()

        assert isinstance(frozen, FrozenDepsRegistry)
        assert frozen.store.empty()

    def test_with_modules_appends(self) -> None:
        p0 = DepsRegistry()
        p1 = p0.with_modules(lambda: Deps.plain({_A: "x"}))

        assert len(p0.modules) == 0
        assert len(p1.modules) == 1

    def test_freeze_merges_modules(self) -> None:
        frozen = DepsRegistry.from_modules(
            lambda: Deps.plain({_A: 1}),
            lambda: Deps.plain({_B: 2}),
        ).freeze()
        resolved = frozen.resolve()

        assert resolved.provide(_A) == 1
        assert resolved.provide(_B) == 2

    def test_build_is_alias_for_freeze(self) -> None:
        with pytest.warns(DeprecationWarning, match="freeze"):
            frozen = DepsRegistry.from_deps(Deps.plain({_A: 1})).build()

        assert isinstance(frozen, FrozenDepsRegistry)

    def test_freeze_enables_trace_from_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("FORZE_DEPS_TRACE", "1")
        resolved = DepsRegistry.from_modules(lambda: Deps.plain({_A: 1})).freeze().resolve()

        assert resolved.trace_resolution is True

    def test_with_tracing_on_registry(self) -> None:
        resolved = (
            DepsRegistry.from_modules(lambda: Deps.plain({_A: 1}))
            .with_tracing(
                resolution=True,
                runtime=False,
            )
            .freeze(trace_resolution=False)
            .resolve()
        )

        assert resolved.trace_resolution is True
        assert resolved.trace_runtime is False

    def test_merge_conflict_at_freeze(self) -> None:
        with pytest.raises(CoreException, match="Conflicting plain"):
            DepsRegistry.from_modules(
                lambda: Deps.plain({_A: 1}),
                lambda: Deps.plain({_A: 2}),
            ).freeze()
