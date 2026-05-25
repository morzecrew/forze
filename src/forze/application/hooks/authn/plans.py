"""Wire authentication requirements into operation plans."""

from __future__ import annotations

from typing import Any, final

import attrs

from forze.application.contracts.execution import BeforeStep
from forze.application.contracts.execution.protocols import Before, BeforeFactory
from forze.application.execution.context import ExecutionContext
from forze.base.errors import AuthenticationError

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class AuthnRequired(BeforeFactory):
    """Before-hook factory that requires a bound :class:`~forze.application.contracts.authn.AuthnIdentity`."""

    message: str = "Authentication required"
    code: str = "auth_required"

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> Before[Any]:
        async def _before(args: Any) -> None:
            _ = args

            if ctx.inv.get_authn() is None:
                raise AuthenticationError(self.message, code=self.code)

        return _before

    # ....................... #

    def to_before_step(
        self,
        *,
        step_id: str,
        requires: tuple[str, ...] = (),
        priority: int = 10,
    ) -> BeforeStep:
        """Build a :class:`BeforeStep` using this factory."""

        return BeforeStep(
            id=step_id,
            factory=self,
            requires=requires,
            priority=priority,
        )


# ....................... #


def authn_required_before_step(
    *,
    step_id: str = "authn_required",
    requires: tuple[str, ...] = (),
    priority: int = 10,
    message: str = "Authentication required",
    code: str = "auth_required",
) -> BeforeStep:
    """Ready-made :class:`BeforeStep` that fails when no principal is bound on ``ctx.inv``."""

    return AuthnRequired(message=message, code=code).to_before_step(
        step_id=step_id,
        requires=requires,
        priority=priority,
    )
