"""Unit tests for integration DepsModule registration builders."""

from __future__ import annotations

from forze.application.contracts.deps import DepKey
from forze.application.execution import Deps
from forze.application.execution.deps.builders import (
    merge_deps,
    routed_constant,
    routed_from_mapping,
    routed_shared_factories,
)

# ----------------------- #

_CLIENT = DepKey[str]("client")
_QUERY = DepKey[str]("query")
_COMMAND = DepKey[str]("command")


def test_merge_deps_empty() -> None:
    deps = merge_deps()

    assert deps.empty()


def test_merge_deps_plain_only() -> None:
    deps = merge_deps(plain={_CLIENT: "c1"})

    assert deps.exists(_CLIENT)
    assert deps.plain_deps[_CLIENT] == "c1"


def test_merge_deps_sections_only() -> None:
    routed = Deps.routed({_QUERY: {"a": "q"}})
    deps = merge_deps(routed)

    assert deps.exists(_QUERY, route="a")


def test_merge_deps_plain_and_sections() -> None:
    routed = routed_from_mapping(
        {"route-a": "cfg"},
        bindings=[(_QUERY, lambda *, config: f"query-{config}")],
    )
    deps = merge_deps(
        routed,
        plain={_CLIENT: "client"},
    )

    assert deps.exists(_CLIENT)
    assert deps.exists(_QUERY, route="route-a")
    assert deps.routed_deps[_QUERY]["route-a"] == "query-cfg"


def test_routed_from_mapping_none_returns_empty() -> None:
    deps = routed_from_mapping(
        None,
        bindings=[(_QUERY, lambda *, config: config)],
    )

    assert deps.empty()


def test_routed_from_mapping_empty_returns_empty() -> None:
    deps = routed_from_mapping(
        {},
        bindings=[(_QUERY, lambda *, config: config)],
    )

    assert deps.empty()


def test_routed_from_mapping_single_key() -> None:
    deps = routed_from_mapping(
        {"users": "users-cfg"},
        bindings=[(_QUERY, lambda *, config: f"q:{config}")],
    )

    assert deps.routed_deps[_QUERY]["users"] == "q:users-cfg"


def test_routed_from_mapping_multi_key() -> None:
    deps = routed_from_mapping(
        {"events": "events-cfg"},
        bindings=[
            (_QUERY, lambda *, config: f"q:{config}"),
            (_COMMAND, lambda *, config: f"c:{config}"),
        ],
    )

    assert deps.routed_deps[_QUERY]["events"] == "q:events-cfg"
    assert deps.routed_deps[_COMMAND]["events"] == "c:events-cfg"


def test_routed_constant_matches_manual_expansion() -> None:
    provider = object()
    manual = Deps.routed({_QUERY: {name: provider for name in ("a", "b")}})
    built = routed_constant(
        {"a", "b"},
        bindings=[(_QUERY, provider)],
    )

    assert built.routed_deps == manual.routed_deps


def test_routed_constant_none_returns_empty() -> None:
    deps = routed_constant(None, bindings=[(_QUERY, object())])

    assert deps.empty()


def test_routed_shared_factories_multi_key() -> None:
    deps = routed_shared_factories(
        {"events": "cfg"},
        dep_keys=[_QUERY, _COMMAND],
        factory=lambda *, config: f"f:{config}",
    )

    assert deps.routed_deps[_QUERY]["events"] == "f:cfg"
    assert deps.routed_deps[_COMMAND]["events"] == "f:cfg"


def test_routed_shared_factories_none_returns_empty() -> None:
    deps = routed_shared_factories(
        None,
        dep_keys=[_QUERY, _COMMAND],
        factory=lambda *, config: config,
    )

    assert deps.empty()
