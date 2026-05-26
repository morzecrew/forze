"""Wire tenant context requirements into operation plans."""

from typing import Any, final

import attrs

from forze.application.contracts.execution import Before, BeforeFactory, BeforeStep
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TenantRequired(BeforeFactory):
    """Before-hook factory that requires a bound :class:`~forze.application.contracts.tenancy.TenantIdentity`."""

    def __call__(self, ctx: ExecutionContext) -> Before[Any]:
        async def _before(args: Any) -> None:
            _ = args

            if ctx.inv.get_tenant() is None:
                raise exc.authentication(
                    "Tenant identity is required",
                    code="tenant_required",
                )

        return _before

    # ....................... #

    def to_step(
        self,
        *,
        step_id: StrKey,
        requires: tuple[StrKey, ...] = (),
        depends_on: tuple[StrKey, ...] = (),
        priority: int = 20,
    ) -> BeforeStep:
        """Build a :class:`BeforeStep` using this factory."""

        return BeforeStep(
            id=step_id,
            factory=self,
            requires=requires,
            depends_on=depends_on,
            priority=priority,
        )
