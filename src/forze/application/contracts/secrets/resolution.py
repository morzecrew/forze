"""Helpers built on :class:`SecretsPort`."""

from collections.abc import Callable, Mapping
from typing import TypeVar
from uuid import UUID

import attrs
from pydantic import BaseModel, ValidationError

from forze.base.exceptions import exc
from forze.base.scrubbing import sanitize_pydantic_errors

from .ports import SecretsPort
from .value_objects import SecretRef

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


def secret_ref_for_tenant(
    ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef],
    tenant_id: UUID,
) -> SecretRef:
    """Resolve a :class:`SecretRef` for *tenant_id* from a callable or mapping."""

    if callable(ref_for_tenant):
        return ref_for_tenant(tenant_id)

    return ref_for_tenant[tenant_id]


# ....................... #


async def resolve_str_for_tenant(
    secrets: SecretsPort,
    ref: SecretRef,
    *,
    tenant_id: UUID,
    backend: str,
) -> str:
    """Resolve a string secret for *tenant_id*, wrapping unexpected errors."""

    try:
        return await secrets.resolve_str(ref)

    except exc:
        raise

    except Exception as e:
        raise exc.internal(
            f"Failed to resolve {backend} secret for tenant {tenant_id}: {e}",
        ) from e


# ....................... #


async def resolve_structured(
    secrets: SecretsPort,
    ref: SecretRef,
    model_type: type[T],
) -> T:
    """Fetch a secret and validate it as JSON into a :class:`~pydantic.BaseModel`.

    :param secrets: Secrets backend.
    :param ref: Secret reference.
    :param model_type: Pydantic model type.
    :returns: Validated model instance.
    """

    raw = await secrets.resolve_str(ref)

    try:
        return model_type.model_validate_json(raw)

    except ValidationError as e:
        raise exc.internal(
            f"Secret at {ref.path!r} is not valid for {model_type.__name__}.",
            code="secret_invalid",
            details={"errors": sanitize_pydantic_errors(e.errors())},
        ) from e


# ....................... #


@attrs.define(frozen=True, slots=True, kw_only=True)
class TenantSecretResolver:
    """Binds a :class:`SecretsPort`, a per-tenant :class:`SecretRef` lookup, and a backend
    label so a tenant's secret resolves in one call — instead of threading the three
    through every call site.

    Holds no per-tenant state; *tenant_id* is the only per-call argument. The DSN path
    (:meth:`resolve_str`) and the structured-credentials path (:meth:`resolve_structured`)
    both wrap unexpected backend errors with the bound backend/tenant context.
    """

    secrets: SecretsPort
    """Backend used to resolve secrets."""

    ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    """Per-tenant :class:`SecretRef` lookup (callable or mapping)."""

    backend: str
    """Backend label used in error context."""

    # ....................... #

    def ref(self, tenant_id: UUID) -> SecretRef:
        """The :class:`SecretRef` bound to *tenant_id*."""

        return secret_ref_for_tenant(self.ref_for_tenant, tenant_id)

    # ....................... #

    async def resolve_str(self, tenant_id: UUID) -> str:
        """Resolve *tenant_id*'s secret as a string (e.g. a DSN), wrapping unexpected errors.

        The :meth:`ref` lookup is wrapped too (a missing mapping entry or a faulty
        resolver callable), so this path raises the same domain failure as
        :meth:`resolve_structured` rather than a raw ``KeyError``.
        """

        try:
            return await resolve_str_for_tenant(
                self.secrets,
                self.ref(tenant_id),
                tenant_id=tenant_id,
                backend=self.backend,
            )

        except exc:
            raise

        except Exception as e:
            raise exc.internal(
                f"Failed to resolve {self.backend} secret for tenant {tenant_id}: {e}",
            ) from e

    # ....................... #

    async def resolve_structured[CredsT: BaseModel](
        self, creds_type: type[CredsT], tenant_id: UUID
    ) -> CredsT:
        """Resolve *tenant_id*'s secret and validate it into *creds_type*, wrapping unexpected errors."""

        try:
            return await resolve_structured(
                self.secrets,
                self.ref(tenant_id),
                creds_type,
            )

        except exc:
            raise

        except Exception as e:
            raise exc.internal(
                f"Failed to resolve {self.backend} secret for tenant {tenant_id}: {e}",
            ) from e
