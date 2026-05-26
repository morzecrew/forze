"""Tests for operation registry plan patches."""

from enum import StrEnum

import pytest

from forze.base.exceptions import CoreException

from forze.application.contracts.execution import BeforeStep, DispatchStep
from forze.application.execution.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.base.primitives import StrKeySelector, str_key_selector

# ----------------------- #


def _noop_before_factory(_ctx):
    async def _before(_args) -> None:
        return None

    return _before


def _freeze_with_tx_patch(
    reg: OperationRegistry,
    selector: StrKeySelector.Spec,
    *,
    tx_route: str = "mock",
) -> FrozenOperationRegistry:
    """Apply a tx-route patch for ``selector`` and freeze."""

    return reg.patch(selector).bind_tx().set_route(tx_route).finish(deep=True).freeze()


def _tx_routes(frozen: FrozenOperationRegistry) -> dict[str, str | None]:
    """Map operation key to frozen tx route (``None`` when unset)."""

    return {str(op): frozen.plans[op].tx.route for op in frozen.handlers}


def test_patch_all_keys_applies_tx_route_to_all_handlers() -> None:
    reg = OperationRegistry(
        handlers={
            "a": lambda _ctx: None,
            "b": lambda _ctx: None,
        },
    )
    frozen = (
        reg.patch(str_key_selector.all_keys())
        .bind_tx()
        .set_route("mock")
        .finish(deep=True)
        .freeze()
    )

    for op in ("a", "b"):
        assert frozen.plans[op].tx.route == "mock"


def test_patch_then_bind_overlay_merges_explicit_plan_last() -> None:
    step = BeforeStep(id="b1", factory=_noop_before_factory)
    reg = (
        OperationRegistry(handlers={"op": lambda _ctx: None})
        .patch(str_key_selector.all_keys())
        .bind_tx()
        .set_route("mock")
        .finish(deep=True)
        .bind("op")
        .bind_outer()
        .before(step)
        .finish(deep=True)
    )
    frozen = reg.freeze()

    assert frozen.plans["op"].tx.route == "mock"
    assert len(frozen.plans["op"].outer.before.steps) == 1


def test_patch_with_no_handlers_raises_orphan_patch() -> None:
    reg = (
        OperationRegistry()
        .patch(str_key_selector.all_keys())
        .bind_tx()
        .set_route("mock")
        .finish(deep=True)
    )

    with pytest.raises(CoreException, match="Orphan plan patch"):
        reg.freeze()


def test_orphan_patch_exact_selector_raises() -> None:
    reg = (
        OperationRegistry(handlers={"other": lambda _ctx: None})
        .patch(str_key_selector.exact("missing"))
        .bind_tx()
        .set_route("mock")
        .finish(deep=True)
    )

    with pytest.raises(CoreException, match="Orphan plan patch"):
        reg.freeze()


def test_two_patches_same_selector_merge_plan() -> None:
    step = BeforeStep(id="b1", factory=_noop_before_factory)
    reg = (
        OperationRegistry(handlers={"op": lambda _ctx: None})
        .patch(str_key_selector.all_keys())
        .bind_tx()
        .set_route("first")
        .finish(deep=True)
        .patch(str_key_selector.all_keys())
        .bind_outer()
        .before(step)
        .finish(deep=True)
    )

    assert len(reg.get_patches()) == 1
    frozen = reg.freeze()

    assert frozen.plans["op"].tx.route == "first"
    assert len(frozen.plans["op"].outer.before.steps) >= 1


def test_equal_specificity_patch_route_conflict_raises() -> None:
    reg = (
        OperationRegistry(handlers={"op": lambda _ctx: None})
        .patch(str_key_selector.when(lambda k: k.startswith("o")))
        .bind_tx()
        .set_route("a")
        .finish(deep=True)
        .patch(str_key_selector.when(lambda k: "p" in k))
        .bind_tx()
        .set_route("b")
        .finish(deep=True)
    )

    with pytest.raises(CoreException, match="Conflicting plan patches"):
        reg.freeze()


def test_tx_dispatch_without_route_raises_at_freeze() -> None:
    reg = (
        OperationRegistry(
            handlers={
                "main": lambda _ctx: None,
                "target": lambda _ctx: None,
            },
        )
        .patch(str_key_selector.exact("main"))
        .bind_tx()
        .dispatch(
            DispatchStep(id="d1", target="target", mapper=lambda a, r: r),
        )
        .finish(deep=True)
    )

    with pytest.raises(CoreException, match="no transaction route"):
        reg.freeze()


def test_tx_dispatch_after_commit_without_route_raises_at_freeze() -> None:
    reg = (
        OperationRegistry(
            handlers={
                "main": lambda _ctx: None,
                "target": lambda _ctx: None,
            },
        )
        .patch(str_key_selector.exact("main"))
        .bind_tx()
        .dispatch_after_commit(
            DispatchStep(id="d1", target="target", mapper=lambda a, r: r),
        )
        .finish(deep=True)
    )

    with pytest.raises(CoreException, match="no transaction route"):
        reg.freeze()


def test_dispatch_in_patch_validates_at_freeze() -> None:
    reg = (
        OperationRegistry(handlers={"main": lambda _ctx: None})
        .patch(str_key_selector.exact("main"))
        .bind_outer()
        .dispatch(
            DispatchStep(id="d1", target="missing", mapper=lambda a, r: r),
        )
        .finish(deep=True)
    )

    with pytest.raises(CoreException, match="Dispatch target"):
        reg.freeze()


def test_merge_detects_patch_selector_conflict() -> None:
    left = (
        OperationRegistry(handlers={"op": lambda _ctx: None})
        .patch(str_key_selector.all_keys())
        .bind_tx()
        .set_route("a")
        .finish(deep=True)
    )
    right = (
        OperationRegistry(handlers={"other": lambda _ctx: None})
        .patch(str_key_selector.all_keys())
        .bind_tx()
        .set_route("b")
        .finish(deep=True)
    )

    with pytest.raises(CoreException, match="Conflicting operation plan patches"):
        OperationRegistry.merge(left, right)


# ....................... #
# Selector-specific patch resolution


class TestPatchSelectorExact:
    def test_exact_matches_str_enum_operation_keys(self) -> None:
        class Op(StrEnum):
            CREATE = "projects.create"

        reg = OperationRegistry(handlers={Op.CREATE: lambda _ctx: None})
        frozen = _freeze_with_tx_patch(
            reg,
            str_key_selector.exact(Op.CREATE),
            tx_route="enum",
        )

        assert _tx_routes(frozen)["projects.create"] == "enum"

    def test_exact_applies_only_to_listed_operations(self) -> None:
        reg = OperationRegistry(
            handlers={
                "projects.create": lambda _ctx: None,
                "projects.update": lambda _ctx: None,
                "projects.get": lambda _ctx: None,
            },
        )
        frozen = _freeze_with_tx_patch(
            reg,
            str_key_selector.exact("projects.create", "projects.update"),
            tx_route="write",
        )

        routes = _tx_routes(frozen)
        assert routes["projects.create"] == "write"
        assert routes["projects.update"] == "write"
        assert routes["projects.get"] is None


class TestPatchSelectorPrefix:
    def test_prefix_matches_startswith_only(self) -> None:
        reg = OperationRegistry(
            handlers={
                "projects.create": lambda _ctx: None,
                "projects-create": lambda _ctx: None,
                "orders.create": lambda _ctx: None,
            },
        )
        frozen = _freeze_with_tx_patch(
            reg,
            str_key_selector.prefix("projects"),
            tx_route="pg",
        )

        routes = _tx_routes(frozen)
        assert routes["projects.create"] == "pg"
        assert routes["projects-create"] == "pg"
        assert routes["orders.create"] is None


class TestPatchSelectorSuffix:
    def test_suffix_matches_endswith_only(self) -> None:
        reg = OperationRegistry(
            handlers={
                "projects.create": lambda _ctx: None,
                "orders-create": lambda _ctx: None,
                "projects.get": lambda _ctx: None,
            },
        )
        frozen = _freeze_with_tx_patch(
            reg,
            str_key_selector.suffix(".create"),
            tx_route="dot_create",
        )

        routes = _tx_routes(frozen)
        assert routes["projects.create"] == "dot_create"
        assert routes["orders-create"] is None
        assert routes["projects.get"] is None

    def test_suffix_with_dash_separator(self) -> None:
        reg = OperationRegistry(
            handlers={
                "orders-create": lambda _ctx: None,
                "orders.get": lambda _ctx: None,
            },
        )
        frozen = _freeze_with_tx_patch(
            reg,
            str_key_selector.suffix("-create"),
            tx_route="dash",
        )

        assert _tx_routes(frozen)["orders-create"] == "dash"
        assert _tx_routes(frozen)["orders.get"] is None


class TestPatchSelectorGlob:
    def test_glob_fnmatch_on_full_key(self) -> None:
        reg = OperationRegistry(
            handlers={
                "projects.create": lambda _ctx: None,
                "projects.update": lambda _ctx: None,
                "projects.create.v2": lambda _ctx: None,
                "other.create": lambda _ctx: None,
            },
        )
        frozen = _freeze_with_tx_patch(
            reg,
            str_key_selector.glob("projects.*"),
            tx_route="pg",
        )

        routes = _tx_routes(frozen)
        assert routes["projects.create"] == "pg"
        assert routes["projects.update"] == "pg"
        assert routes["projects.create.v2"] == "pg"
        assert routes["other.create"] is None


class TestPatchSelectorWhen:
    def test_when_applies_via_custom_predicate(self) -> None:
        reg = OperationRegistry(
            handlers={
                "document.list": lambda _ctx: None,
                "document.list_cursor": lambda _ctx: None,
                "document.get": lambda _ctx: None,
            },
        )
        frozen = _freeze_with_tx_patch(
            reg,
            str_key_selector.when(lambda k: "cursor" in k),
            tx_route="cursor",
        )

        routes = _tx_routes(frozen)
        assert routes["document.list_cursor"] == "cursor"
        assert routes["document.list"] is None
        assert routes["document.get"] is None


class TestPatchSelectorSpecificity:
    def test_more_specific_patch_merges_after_broader_patch(self) -> None:
        """``exact`` patch layers on top of ``prefix`` without conflicting tx routes."""

        step = BeforeStep(id="narrow", factory=_noop_before_factory)
        reg = (
            OperationRegistry(
                handlers={
                    "projects.create": lambda _ctx: None,
                    "projects.get": lambda _ctx: None,
                    "other.create": lambda _ctx: None,
                },
            )
            .patch(str_key_selector.prefix("projects."))
            .bind_tx()
            .set_route("pg")
            .finish(deep=True)
            .patch(str_key_selector.exact("projects.create"))
            .bind_outer()
            .before(step)
            .finish(deep=True)
        )
        frozen = reg.freeze()

        routes = _tx_routes(frozen)
        assert routes["projects.create"] == "pg"
        assert routes["projects.get"] == "pg"
        assert routes["other.create"] is None

        assert len(frozen.plans["projects.create"].outer.before.steps) == 1
        assert len(frozen.plans["projects.get"].outer.before.steps) == 0

    def test_all_keys_then_suffix_both_apply_to_matching_ops(self) -> None:
        reg = (
            OperationRegistry(
                handlers={
                    "projects.create": lambda _ctx: None,
                    "projects.get": lambda _ctx: None,
                },
            )
            .patch(str_key_selector.all_keys())
            .bind_tx()
            .set_route("base")
            .finish(deep=True)
            .patch(str_key_selector.suffix(".create"))
            .bind_outer()
            .before(BeforeStep(id="create_only", factory=_noop_before_factory))
            .finish(deep=True)
        )
        frozen = reg.freeze()

        assert _tx_routes(frozen)["projects.create"] == "base"
        assert _tx_routes(frozen)["projects.get"] == "base"
        assert len(frozen.plans["projects.create"].outer.before.steps) == 1
        assert len(frozen.plans["projects.get"].outer.before.steps) == 0
