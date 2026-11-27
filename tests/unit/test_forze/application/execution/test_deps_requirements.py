"""A provider's declared requirements are checked when the registry is frozen."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from unittest.mock import MagicMock, Mock

import attrs
import pytest

from forze.application.contracts.deps import (
    DeclaresRequirements,
    DepKey,
    Deps,
    ProviderStore,
    Requirement,
)
from forze.application.execution import DepsRegistry, WiringFinding
from forze.base.exceptions import CoreException, ExceptionKind

pytestmark = pytest.mark.unit

# ----------------------- #

NEEDED = DepKey[Any]("needed")
GATE = DepKey[Any]("gate")
REMEDY = "Register 'needed' on route 'store'."


@attrs.define(frozen=True)
class _Needs:
    """A provider that resolves ``needed`` on route ``store`` when called."""

    route: str = "store"
    remedy: str = REMEDY

    def __call__(self, ctx: Any) -> object:
        return object()

    def requirements(self) -> Iterable[Requirement]:
        return (Requirement(key=NEEDED, route=self.route, remedy=self.remedy),)


@attrs.define(frozen=True)
class _Reports:
    """A provider that reports a posture finding."""

    target: str = "thing:main"

    def posture_findings(self) -> Iterable[WiringFinding]:
        return (WiringFinding(source="test", target=self.target, rule="is unsafe"),)


class _Module:
    """A module reporting the same finding as the provider it registers."""

    def __call__(self) -> Deps:
        return Deps.routed({GATE: {"main": _Reports()}})

    def posture_findings(self) -> Iterable[WiringFinding]:
        return (WiringFinding(source="test", target="thing:main", rule="is unsafe"),)


def _refusal(deps: Deps) -> CoreException:
    with pytest.raises(CoreException) as caught:
        DepsRegistry.from_deps(deps).freeze()

    return caught.value


# ....................... #


def test_the_store_yields_every_provider_once() -> None:
    plain, routed_a, routed_b = object(), object(), object()
    store = ProviderStore.merge(
        Deps.plain({NEEDED: plain}).store,
        Deps.routed({GATE: {"a": routed_a, "b": routed_b}}).store,
    )

    assert list(store.providers()) == [
        (NEEDED, None, plain),
        (GATE, "a", routed_a),
        (GATE, "b", routed_b),
    ]


def test_a_provider_needing_an_unregistered_route_refuses_the_freeze() -> None:
    error = _refusal(Deps.routed({GATE: {"main": _Needs()}}))

    assert error.kind is ExceptionKind.CONFIGURATION
    assert error.code == "dependency_requirement_unbound"
    assert error.summary == (
        "'gate' on route 'main' resolves 'needed' on route 'store', which is not registered and "
        f"has no plain fallback. {REMEDY}"
    )


@pytest.mark.parametrize(
    "store",
    [Deps.routed({NEEDED: {"store": object()}}), Deps.plain({NEEDED: object()})],
    ids=["routed", "plain-fallback"],
)
def test_the_route_or_a_plain_fallback_satisfies_it(store: Deps) -> None:
    DepsRegistry.from_deps(Deps.routed({GATE: {"main": _Needs()}}), store).freeze()


def test_every_unbound_requirement_is_named_in_one_refusal() -> None:
    deps = Deps.routed({GATE: {"b": _Needs(), "a": _Needs()}}).merge(
        Deps.plain({DepKey[Any]("other"): _Needs(route="elsewhere", remedy="Or this.")})
    )

    summary = _refusal(deps).summary

    assert summary.index("'gate' on route 'a'") < summary.index("'gate' on route 'b'")
    assert "'other' as a plain registration resolves 'needed' on route 'elsewhere'" in summary
    # Each remedy once, after every frame it answers.
    assert summary.count(REMEDY) == 1
    assert summary.count("Or this.") == 1
    assert summary.rindex("has no plain fallback.") < summary.index(REMEDY)


@pytest.mark.parametrize("double", [Mock, MagicMock], ids=["mock", "magic-mock"])
def test_a_mock_declares_nothing(double: type) -> None:
    # The protocol check reads attributes statically, so a mock's dynamic ones do not count.
    provider = double()

    assert not isinstance(provider, DeclaresRequirements)

    DepsRegistry.from_deps(Deps.routed({GATE: {"main": provider}})).freeze()


def test_a_class_registered_as_a_factory_declares_nothing() -> None:
    # The class carries the method unbound; calling it would fail for want of an instance.
    assert isinstance(_Needs, DeclaresRequirements)

    DepsRegistry.from_deps(Deps.routed({GATE: {"main": _Needs}})).freeze()


def test_a_providers_findings_are_collected_from_built_deps() -> None:
    frozen = DepsRegistry.from_deps(Deps.routed({GATE: {"main": _Reports()}})).freeze()

    assert [f.render() for f in frozen.posture_findings] == ["thing:main: is unsafe"]


def test_a_module_and_its_provider_report_one_finding_once() -> None:
    frozen = DepsRegistry.from_modules(_Module()).freeze()

    assert [f.render() for f in frozen.posture_findings] == ["thing:main: is unsafe"]


def _lookup(name: str) -> tuple[str, ...]:
    return (name,)


@attrs.define
class _Settings:
    requirements: tuple[str, ...] = ("a",)
    posture_findings: tuple[str, ...] = ()


@attrs.define
class _Hooks:
    requirements: Any = _lookup
    posture_findings: Any = _lookup


@pytest.mark.parametrize("settings", [_Settings(), _Hooks()], ids=["value", "callable"])
def test_a_provider_with_a_field_of_a_declaring_name_freezes(settings: object) -> None:
    # The protocols are checked by attribute name; a field declares nothing, even one that
    # holds a callable, and must not be called.
    DepsRegistry.from_deps(Deps.plain({DepKey[object]("settings"): settings})).freeze()
