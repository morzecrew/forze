"""Serialization and utility helpers around Pydantic models."""

import hashlib
from decimal import Decimal
from functools import lru_cache
from typing import Any, Literal, Sequence, TypedDict

import orjson
from pydantic import BaseModel, TypeAdapter

from ..logging import getLogger

# ----------------------- #

logger = getLogger(__name__).bind(scope="serialization")

# ....................... #


@lru_cache(maxsize=128)
def _list_adapter[M: BaseModel](cls: type[M]) -> TypeAdapter[list[M]]:
    return TypeAdapter(list[cls])  # type: ignore[valid-type]


# ....................... #


def pydantic_validate[M: BaseModel](
    cls: type[M],
    data: dict[str, Any],
    *,
    forbid_extra: bool = False,
) -> M:
    """Validate raw ``data`` into a Pydantic model instance.

    :param cls: Pydantic model class to validate against.
    :param data: Raw input mapping.
    :param forbid_extra: When true, extra keys are forbidden instead of ignored. Defaults to False.
    :returns: Validated model instance.
    """

    logger.trace(
        "Validating data into {model} (forbid_extra={forbid_extra})",
        sub={
            "model": cls.__name__,
            "forbid_extra": forbid_extra,
        },
    )

    return cls.model_validate(data, extra="forbid" if forbid_extra else "ignore")


# ....................... #


def pydantic_validate_many[M: BaseModel](
    cls: type[M],
    data: Sequence[dict[str, Any]],
    *,
    forbid_extra: bool = False,
) -> list[M]:
    logger.trace(
        "Validating {count} data items into list[{model}] (forbid_extra={forbid_extra})",
        sub={
            "count": len(data),
            "model": cls.__name__,
            "forbid_extra": forbid_extra,
        },
    )
    adapter = _list_adapter(cls)

    return adapter.validate_python(
        list(data),
        extra="forbid" if forbid_extra else "ignore",
    )


# ....................... #


class _PydanticDumpExcludeOptions(TypedDict, total=False):
    """Options controlling which fields to exclude from :func:`pydantic_dump`."""

    unset: bool
    """Exclude fields that were never explicitly set."""

    none: bool
    """Exclude fields whose value is ``None``."""

    defaults: bool
    """Exclude fields still equal to their default value."""

    computed_fields: bool
    """Exclude computed (derived) fields."""


# ....................... #


def pydantic_dump(
    obj: BaseModel,
    *,
    mode: Literal["json", "python"] = "python",
    exclude: _PydanticDumpExcludeOptions = {},
) -> dict[str, Any]:
    """Dump a Pydantic model into a JSON-compatible ``dict``.

    :param obj: Model instance to serialize.
    :param exclude: Fine-grained control over which fields are omitted.
    :param mode: Serialization mode.
    :returns: JSON-ready dictionary representation.
    """

    logger.trace(
        "Dumping {model} (mode={mode}, exclude={exclude})",
        sub={
            "model": type(obj).__name__,
            "mode": mode,
            "exclude": exclude,
        },
    )

    return obj.model_dump(
        exclude_unset=exclude.get("unset", False),
        exclude_none=exclude.get("none", False),
        exclude_defaults=exclude.get("defaults", False),
        exclude_computed_fields=exclude.get("computed_fields", False),
        mode=mode,
    )


# ....................... #


def pydantic_dump_many(
    objs: Sequence[BaseModel],
    *,
    mode: Literal["json", "python"] = "python",
    exclude: _PydanticDumpExcludeOptions = {},
) -> list[dict[str, Any]]:
    """Dump a list of Pydantic models into a list of JSON-compatible ``dict``.

    :param objs: List of models to serialize.
    :param mode: Serialization mode.
    :param exclude: Fine-grained control over which fields are omitted.
    :returns: List of JSON-ready dictionary representations.
    """

    if not objs:
        return []

    cls = type(objs[0])

    logger.trace(
        "Dumping {count} models into list[dict[str, Any]] (mode={mode}, exclude={exclude})",
        sub={
            "count": len(objs),
            "mode": mode,
            "exclude": exclude,
        },
    )

    adapter = _list_adapter(cls)
    dumped = adapter.dump_python(
        list(objs),
        mode=mode,
        exclude_unset=exclude.get("unset", False),
        exclude_none=exclude.get("none", False),
        exclude_defaults=exclude.get("defaults", False),
        exclude_computed_fields=exclude.get("computed_fields", False),
    )

    return dumped


# ....................... #


def pydantic_field_names(
    cls: type[BaseModel],
    *,
    include_computed: bool = True,
) -> frozenset[str]:
    """Return the set of field names defined on a Pydantic model class.

    Results are cached per ``(cls, include_computed)`` combination via
    :func:`_pydantic_field_names_cached` to avoid repeated introspection of
    the same model class.

    :param cls: Pydantic model class.
    :param include_computed: Whether to include computed fields.
    :returns: Frozen set of field names on the model.
    """

    return _pydantic_field_names_cached(cls, include_computed)


@lru_cache(maxsize=256)
def _pydantic_field_names_cached(
    cls: type[BaseModel],
    include_computed: bool,
) -> frozenset[str]:
    """Cached implementation of :func:`pydantic_field_names`."""

    model_fields = set(cls.model_fields.keys())

    if include_computed:
        model_fields |= set(cls.model_computed_fields.keys())

    return frozenset(model_fields)


# ....................... #


def _normalize_for_hashing(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _normalize_for_hashing(v) for k, v in value.items()}  # type: ignore[return-value]

    if isinstance(value, list | tuple | set):
        return [_normalize_for_hashing(v) for v in value]  # type: ignore[arg-type]

    if isinstance(value, Decimal):
        return str(value)

    return value


# ....................... #


def pydantic_model_hash(
    model: BaseModel,
    *,
    exclude: _PydanticDumpExcludeOptions = {},
) -> str:
    """Return a stable SHA-256 hash for the serialized model.

    :param model: Model instance to hash.
    :param exclude: Options forwarded to :func:`pydantic_dump`.
    :returns: Hex-encoded SHA-256 digest of the sorted JSON representation.
    """

    logger.trace(
        "Hashing Pydantic model {model} (exclude={exclude})",
        sub={
            "model": type(model).__name__,
            "exclude": exclude,
        },
    )

    data = pydantic_dump(model, exclude=exclude)
    norm_data = _normalize_for_hashing(data)
    raw = orjson.dumps(norm_data, option=orjson.OPT_SORT_KEYS)
    digest = hashlib.sha256(raw).hexdigest()

    return digest
