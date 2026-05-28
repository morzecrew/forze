"""Helpers built on :class:`SecretsPort`."""

from collections.abc import Callable, Mapping
from typing import TypeVar
from uuid import UUID

from pydantic import BaseModel, ValidationError

from forze.base.exceptions import exc
from forze.base.scrubbing import sanitize_pydantic_errors

from .ports import SecretsPort
from .value_objects import SecretRef

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


def secret_ref_for_tenant(
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef],
    tenant_id: UUID,
) -> SecretRef:
    """Resolve a :class:`SecretRef` for *tenant_id* from a callable or mapping."""

    if callable(secret_ref_for_tenant):
        return secret_ref_for_tenant(tenant_id)

    return secret_ref_for_tenant[tenant_id]


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
            f"Secret at {ref.path!r} is not valid for {model_type.__name__}: {e}",
            code="secret_invalid",
            details={"errors": sanitize_pydantic_errors(e.errors())},
        ) from e
