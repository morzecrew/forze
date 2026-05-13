"""Reference guards for capability-driven authentication wiring."""

from __future__ import annotations

from typing import Any

import attrs

from ..execution import CapabilitySkip, ExecutionContext, Guard
from ..execution.plan import GuardFactory


@attrs.define(slots=True, kw_only=True, frozen=True)
class _AuthnPrincipalCapabilityGuard(Guard[Any]):
    """Skips downstream capability work when no principal is present."""

    ctx: ExecutionContext
    skip_reason: str | None

    async def __call__(self, args: Any) -> Any:  # noqa: ARG002
        if self.ctx.get_authn_identity() is None:
            return CapabilitySkip(reason=self.skip_reason)

        return None


def authn_principal_capability_guard_factory(
    *,
    skip_reason: str | None = "skip: authentication required",
) -> GuardFactory:
    """Return a guard that yields :class:`~forze.application.execution.capabilities.CapabilitySkip` without identity.

    Declare ``provides`` (for example ``{AUTHN_PRINCIPAL}``) on the enclosing
    :class:`~forze.application.execution.plan.UsecasePlan` step so downstream
    guards can ``require`` the same capability key.
    """

    def factory(ctx: ExecutionContext) -> Guard[Any]:
        return _AuthnPrincipalCapabilityGuard(ctx=ctx, skip_reason=skip_reason)

    return factory
