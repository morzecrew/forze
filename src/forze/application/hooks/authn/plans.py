"""Wire authentication requirements into operation plans."""

from typing import Any, final

import attrs

from forze.application.contracts.execution import Before, BeforeFactory, BeforeStep
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnRequired(BeforeFactory):
    """Before-hook factory that requires a bound :class:`~forze.application.contracts.authn.AuthnIdentity`."""

    def __call__(self, ctx: ExecutionContext) -> Before[Any]:
        async def _before(args: Any) -> None:
            _ = args

            if ctx.inv_ctx.get_authn() is None:
                raise exc.authentication(
                    "Authentication required",
                    code="auth_required",
                )

        return _before

    # ....................... #

    def to_step(
        self,
        *,
        step_id: StrKey = "authn.principal",
        requires: tuple[StrKey, ...] = (),
        depends_on: tuple[StrKey, ...] = (),
        priority: int = 10,
    ) -> BeforeStep:
        """Build a :class:`BeforeStep` using this factory."""

        return BeforeStep(
            id=step_id,
            factory=self,
            requires=requires,
            depends_on=depends_on,
            priority=priority,
        )
