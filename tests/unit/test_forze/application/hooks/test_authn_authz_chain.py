"""The documented authn → tenant → authz chain freezes, and authorization waits for authentication.

``AuthzBeforeAuthorize.to_step()`` requires the ``authn.principal`` capability by default, so the
recipe's chain resolves only if ``AuthnRequired.to_step()`` provides it.
"""

from __future__ import annotations

from typing import Any

import pytest

from forze.application.contracts.authz import AuthzSpec
from forze.application.contracts.execution import steps_graph_from_sequence
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.hooks.authn import AuthnRequired
from forze.application.hooks.authz import AuthzBeforeAuthorize
from forze.application.hooks.tenancy import TenantRequired
from forze.base.exceptions import CoreException
from forze.base.primitives import AbstractSequence, str_key_selector

pytestmark = pytest.mark.unit

AUTHZ = AuthzSpec(name="api")
OP = "orders.create"


def _recipe_steps() -> tuple[Any, ...]:
    return (
        AuthnRequired().to_step(),
        TenantRequired().to_step(step_id="tenant.required"),
        AuthzBeforeAuthorize(spec=AUTHZ, action="orders:create").to_step(step_id="authz.create"),
    )


def _registry(*steps: Any) -> Any:
    async def _handler(_args: Any) -> str:
        return "ok"

    return (
        OperationRegistry(handlers={OP: lambda _ctx: _handler})
        .patch(str_key_selector.exact(OP))
        .bind_outer()
        .before(*steps)
        .finish(deep=True)
    )


def test_the_documented_chain_freezes() -> None:
    _registry(*_recipe_steps()).freeze()


def test_authorization_runs_after_authentication() -> None:
    # Without the capability edge the authz step (priority 50) would sort ahead of authn (10).
    graph = steps_graph_from_sequence(AbstractSequence(items=_recipe_steps()))
    wave = {step_id: i for i, ids in enumerate(graph.waves) for step_id in ids}

    assert wave["authn.principal"] < wave["authz.create"]


def test_authorization_without_authentication_still_refuses_to_freeze() -> None:
    authz = AuthzBeforeAuthorize(spec=AUTHZ, action="orders:create").to_step()

    with pytest.raises(CoreException, match="authn.principal"):
        _registry(authz).freeze()
