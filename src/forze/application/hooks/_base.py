"""Shared building blocks for hook plan factories (authn, authz, tenancy)."""

from collections.abc import Callable
from typing import Any

from forze.application.contracts.execution import Before, BeforeFactory, BeforeStep
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

# ----------------------- #


def authentication_guard_before(
    ctx: ExecutionContext,
    check: Callable[[ExecutionContext], object | None],
    *,
    message: str,
    code: str,
) -> Before[Any]:
    """Build a before-hook that raises ``exc.authentication`` when *check* yields ``None``."""

    async def _before(args: Any) -> None:
        _ = args

        if check(ctx) is None:
            raise exc.authentication(message, code=code)

    return _before


# ....................... #


def required_guard_step(
    factory: BeforeFactory,
    *,
    step_id: StrKey,
    requires: tuple[StrKey, ...] = (),
    depends_on: tuple[StrKey, ...] = (),
    priority: int = 0,
) -> BeforeStep:
    """Build a :class:`BeforeStep` wrapping a before-hook *factory*."""

    return BeforeStep(
        id=step_id,
        factory=factory,
        requires=requires,
        depends_on=depends_on,
        priority=priority,
    )
