"""Helpers built on :class:`SecretsPort`."""

from typing import TypeVar

from pydantic import BaseModel, ValidationError

from forze.base.exceptions import exc
from forze.base.scrubbing import sanitize_pydantic_errors

from .ports import SecretsPort
from .value_objects import SecretRef

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

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
