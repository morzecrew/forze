"""Usecase-level authorization guards built on :class:`~forze.application.contracts.authz.AuthzPort`."""

from typing import Any, Mapping
from uuid import UUID

import attrs

from forze.base.errors import AuthenticationError, AuthorizationError

from .._logger import logger
from ..contracts.authz import AuthzDepKey, AuthzSpec
from ..execution import ExecutionContext, Guard
from ..execution.capability_keys import (
    AUTHN_PRINCIPAL,
    authz_permits_capability,
)
from ..execution.plan import GuardFactory

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzPermissionRequirement:
    """Declarative permission check shared by usecase guards and HTTP/OpenAPI hints.

    Use the same frozen instance when registering a :class:`~forze.application.execution.plan.UsecasePlan`
    guard and when documenting the matching HTTP route (for example ``openapi_extra``),
    so tests can assert both stay aligned.
    """

    permission_key: str
    """Catalog permission key for :meth:`~forze.application.contracts.authz.AuthzPort.permits`."""

    resource: str | None = None
    """Optional ``resource`` argument to ``permits``."""

    context: Mapping[str, Any] | None = None
    """Optional ``context`` mapping to ``permits``."""

    use_tenant_from_context: bool = True
    """When ``True``, pass ``tenant_id`` from tenancy identity when present."""

    require_authn_identity: bool = True
    """When ``True``, missing :class:`~forze.application.contracts.authn.AuthnIdentity` raises :class:`~forze.base.errors.AuthenticationError`."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzPermissionGuard(Guard[Any]):
    """Guard implementation: ``permits`` for the current principal."""

    ctx: ExecutionContext
    """Execution context."""

    spec: AuthzSpec
    """Authorization spec."""

    requirement: AuthzPermissionRequirement
    """Permission requirement."""

    # ....................... #

    async def __call__(self, args: Any) -> None:  # noqa: ARG002
        ident = self.ctx.get_authn_identity()

        if ident is None and self.requirement.require_authn_identity:
            raise AuthenticationError(message="Authentication required")

        if ident is None:
            return

        authz_dep = self.ctx.dep(AuthzDepKey, route=self.spec.name)
        authz = authz_dep(self.ctx, self.spec)
        tenant_id = self._tenant_id()

        allowed = await authz.permits(
            ident.principal_id,
            self.requirement.permission_key,
            tenant_id=tenant_id,
            resource=self.requirement.resource,
            context=self.requirement.context,
        )

        if not allowed:
            logger.warning(
                "Authorization denied (permission=%s, op_context=usecase_guard)",
                self.requirement.permission_key,
            )

            raise AuthorizationError(
                message="Principal is not permitted for this operation",
            )

    # ....................... #

    def _tenant_id(self) -> UUID | None:
        if not self.requirement.use_tenant_from_context:
            return None

        ten = self.ctx.get_tenancy_identity()

        return None if ten is None else ten.tenant_id


# ....................... #


def authz_permission_guard_factory(
    spec: AuthzSpec,
    requirement: AuthzPermissionRequirement,
) -> GuardFactory:
    """Return a :class:`~forze.application.execution.plan.GuardFactory` for :meth:`UsecasePlan.before`.

    The guard resolves :class:`~forze.application.contracts.authz.AuthzPort` from ``ctx`` using
    ``requirement.authz_route`` and calls :meth:`~forze.application.contracts.authz.AuthzPort.permits`
    with the current :class:`~forze.application.contracts.authn.AuthnIdentity` (when required).

    :param spec: Authorization spec whose ``name`` matches the configured ``AuthzDepKey`` route family.
    :param requirement: Permission and routing details; reuse the same object for HTTP ``openapi_extra`` / ``dependencies``.
    """

    def factory(ctx: ExecutionContext) -> Guard[Any]:
        return AuthzPermissionGuard(ctx=ctx, spec=spec, requirement=requirement)

    return factory


# ....................... #


def authz_permission_capability_keys(
    requirement: AuthzPermissionRequirement,
) -> tuple[frozenset[str], frozenset[str]]:
    """Return ``(requires, provides)`` for :meth:`UsecasePlan.before` capability fields.

    Use with :attr:`UsecasePlan.use_capability_engine` so authorization guards run
    only after another step in the **same bucket** marks :data:`~forze.application.execution.capability_keys.AUTHN_PRINCIPAL` ready (for example a guard that
        returns ``CapabilitySkip`` when
    no identity is present instead of raising).

    Example:

    .. code-block:: python

        req, prov = authz_permission_capability_keys(requirement)
        plan = (
            UsecasePlan(use_capability_engine=True)
            .before("op", identity_guard, provides=frozenset({AUTHN_PRINCIPAL}))
            .before("op", authz_permission_guard_factory(spec, requirement), requires=req, provides=prov)
        )

    :param requirement: Same instance passed to :func:`authz_permission_guard_factory`.
    """

    requires: frozenset[str] = (
        frozenset({str(AUTHN_PRINCIPAL)})
        if requirement.require_authn_identity
        else frozenset()
    )

    provides: frozenset[str] = frozenset(
        {str(authz_permits_capability(requirement.permission_key))}
    )

    return requires, provides
