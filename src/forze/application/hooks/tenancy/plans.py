"""Wire tenant context requirements into operation plans."""

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
class TenantRequired(BeforeFactory):
    """Before-hook factory that requires a bound :class:`~forze.application.contracts.tenancy.TenantIdentity`."""

    message: str = "Tenant identity is required"
    code: str = "tenant_required"

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> Before[Any]:
        async def _before(args: Any) -> None:
            _ = args

            if ctx.inv.get_tenant() is None:
                raise AuthenticationError(self.message, code=self.code)

        return _before

    # ....................... #

    def to_before_step(
        self,
        *,
        step_id: str,
        requires: tuple[str, ...] = (),
        priority: int = 20,
    ) -> BeforeStep:
        """Build a :class:`BeforeStep` using this factory."""

        return BeforeStep(
            id=step_id,
            factory=self,
            requires=requires,
            priority=priority,
        )


# ....................... #


def tenant_required_before_step(
    *,
    step_id: str = "tenant_required",
    requires: tuple[str, ...] = (),
    priority: int = 20,
    message: str = "Tenant identity is required",
    code: str = "tenant_required",
) -> BeforeStep:
    """Ready-made :class:`BeforeStep` that fails when no tenant is bound on ``ctx.inv``."""

    return TenantRequired(message=message, code=code).to_before_step(
        step_id=step_id,
        requires=requires,
        priority=priority,
    )
