"""Freeze-time catalog derivation: idempotency-key support + declared authz keys.

Both facts are detected *structurally* via marker protocols
(``ProvidesIdempotency`` / ``DeclaresAuthz``), mirroring the hedge-gate pattern —
no import coupling between the registry freeze path and the hook implementations.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.authz import AuthzSpec
from forze.application.contracts.execution import (
    BeforeStep,
    DeclaresAuthz,
    MiddlewareStep,
)
from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.contracts.resilience import HedgeSafety
from forze.application.execution.operations import OperationKind
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.hooks.authz import (
    AuthzBeforeAuthorize,
    AuthzDocumentScopeWrap,
)
from forze.application.hooks.idempotency import IdempotencyWrap
from forze.application.hooks.resilience import HedgeWrap
from forze.base.exceptions import CoreException

# ----------------------- #


class _Result(BaseModel):
    pass


def _handler_factory(_ctx: Any) -> Any:
    async def handler(_args: Any) -> None:  # pragma: no cover - never invoked
        return None

    return handler


def _idempotency_step() -> MiddlewareStep:
    return IdempotencyWrap(
        op="op", spec=IdempotencySpec(name="s"), result_type=_Result
    ).to_step()


def _authz_before_step(action: str, *, step_id: str = "authz") -> BeforeStep:
    return AuthzBeforeAuthorize(spec=AuthzSpec(name="z"), action=action).to_step(
        step_id=step_id,
        requires=(),
    )


def _authz_scope_step(action: str | None, *, step_id: str = "scope") -> MiddlewareStep:
    return AuthzDocumentScopeWrap(
        spec=AuthzSpec(name="z"),
        document_name="doc",
        operation="list",
        action=action,
    ).to_step(step_id=step_id)


def _registry() -> OperationRegistry:
    return OperationRegistry(handlers={"op": _handler_factory})


# ....................... #


class TestIdempotencyDerivation:
    def test_op_with_idempotency_wrap_flags_true(self) -> None:
        reg = (
            _registry().bind("op").bind_outer().wrap(_idempotency_step()).finish(deep=True)
        )

        entry = reg.freeze().catalog()["op"]

        assert entry.supports_idempotency_key is True

    def test_op_without_wrap_flags_false(self) -> None:
        entry = _registry().freeze().catalog()["op"]

        assert entry.supports_idempotency_key is False
        assert entry.required_permissions == ()

    def test_tx_scope_wrap_is_detected(self) -> None:
        reg = (
            _registry()
            .bind("op")
            .bind_tx()
            .set_route("pg")
            .wrap(_idempotency_step())
            .finish(deep=True)
        )

        assert reg.freeze().catalog()["op"].supports_idempotency_key is True

    def test_custom_structural_wrap_is_detected_without_import_coupling(self) -> None:
        # A hook the framework has never seen: only the marker protocol matters.
        class _CustomGuard:
            def __call__(self, ctx: Any) -> Any:  # pragma: no cover - never resolved
                raise AssertionError("not resolved at freeze")

            def provides_idempotency(self) -> bool:
                return True

        step = MiddlewareStep(id="custom", factory=_CustomGuard())
        reg = _registry().bind("op").bind_outer().wrap(step).finish(deep=True)

        assert reg.freeze().catalog()["op"].supports_idempotency_key is True

    def test_marker_returning_false_does_not_flag(self) -> None:
        class _DisabledGuard:
            def __call__(self, ctx: Any) -> Any:  # pragma: no cover - never resolved
                raise AssertionError("not resolved at freeze")

            def provides_idempotency(self) -> bool:
                return False

        step = MiddlewareStep(id="custom", factory=_DisabledGuard())
        reg = _registry().bind("op").bind_outer().wrap(step).finish(deep=True)

        assert reg.freeze().catalog()["op"].supports_idempotency_key is False

    def test_hedge_gate_behavior_untouched(self) -> None:
        # Hedge without guard still rejected; with the idempotency sibling still passes.
        hedged = (
            _registry()
            .bind("op")
            .bind_outer()
            .wrap(HedgeWrap(policy="p").to_step())
            .finish(deep=True)
        )

        with pytest.raises(CoreException, match="hedged"):
            hedged.freeze()

        guarded = (
            _registry()
            .bind("op")
            .bind_outer()
            .wrap(HedgeWrap(policy="p").to_step(), _idempotency_step())
            .finish(deep=True)
        )

        assert guarded.freeze().catalog()["op"].supports_idempotency_key is True

        declared_safe = (
            _registry()
            .bind("op")
            .bind_outer()
            .wrap(HedgeWrap(policy="p", safety=HedgeSafety.READ_ONLY).to_step())
            .finish(deep=True)
        )

        assert declared_safe.freeze().catalog()["op"].supports_idempotency_key is False


# ....................... #


class TestAuthzDerivation:
    def test_before_hook_permission_lands_on_entry(self) -> None:
        reg = (
            _registry()
            .bind("op")
            .bind_outer()
            .before(_authz_before_step("notes.read"))
            .finish(deep=True)
        )

        assert reg.freeze().catalog()["op"].required_permissions == ("notes.read",)

    def test_union_across_before_and_wrap_hooks_sorted_deduped(self) -> None:
        reg = (
            _registry()
            .bind("op")
            .bind_outer()
            .before(
                _authz_before_step("notes.write", step_id="authz1"),
                _authz_before_step("notes.admin", step_id="authz2"),
            )
            .wrap(_authz_scope_step("notes.write"))
            .finish(deep=True)
        )

        assert reg.freeze().catalog()["op"].required_permissions == (
            "notes.admin",
            "notes.write",
        )

    def test_scope_wrap_without_action_declares_no_keys(self) -> None:
        reg = (
            _registry()
            .bind("op")
            .bind_outer()
            .wrap(_authz_scope_step(None))
            .finish(deep=True)
        )

        # Honest empty: the wrap still scopes access, it just names no permission key.
        assert reg.freeze().catalog()["op"].required_permissions == ()

    def test_custom_structural_hook_is_detected_without_import_coupling(self) -> None:
        class _CustomAuthz:
            def __call__(self, ctx: Any) -> Any:  # pragma: no cover - never resolved
                raise AssertionError("not resolved at freeze")

            def permission_keys(self) -> tuple[str, ...]:
                return ("custom.perm",)

        step = BeforeStep(id="custom", factory=_CustomAuthz())
        reg = _registry().bind("op").bind_outer().before(step).finish(deep=True)

        assert reg.freeze().catalog()["op"].required_permissions == ("custom.perm",)


# ....................... #


class TestMarkers:
    def test_authz_before_declares_its_action(self) -> None:
        hook = AuthzBeforeAuthorize(spec=AuthzSpec(name="z"), action="x.read")

        assert isinstance(hook, DeclaresAuthz)
        assert hook.permission_keys() == ("x.read",)

    def test_scope_wrap_declares_action_only_when_set(self) -> None:
        with_action = AuthzDocumentScopeWrap(
            spec=AuthzSpec(name="z"), document_name="d", operation="list", action="d.list"
        )
        without_action = AuthzDocumentScopeWrap(
            spec=AuthzSpec(name="z"), document_name="d", operation="list"
        )

        assert isinstance(with_action, DeclaresAuthz)
        assert with_action.permission_keys() == ("d.list",)
        assert without_action.permission_keys() == ()

    def test_idempotency_wrap_is_not_authz(self) -> None:
        wrap = IdempotencyWrap(
            op="op", spec=IdempotencySpec(name="s"), result_type=_Result
        )

        assert not isinstance(wrap, DeclaresAuthz)


# ....................... #


class TestCombinedDerivation:
    def test_idempotency_and_authz_on_one_operation(self) -> None:
        reg = (
            _registry()
            .bind("op")
            .bind_outer()
            .before(_authz_before_step("notes.write"))
            .wrap(_idempotency_step())
            .finish(deep=True)
        )

        entry = reg.freeze().catalog()["op"]

        assert entry.kind is OperationKind.COMMAND
        assert entry.supports_idempotency_key is True
        assert entry.required_permissions == ("notes.write",)
