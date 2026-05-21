"""Serialization and utility helpers around Pydantic models."""

import hashlib
from collections.abc import Iterator, Sequence
from decimal import Decimal
from functools import lru_cache
from typing import Any, Final, Literal

import orjson
from pydantic import BaseModel, SecretStr, TypeAdapter

from .._logger import logger
from ..primitives import JsonDict
from .model_codec import RecordMappingDumpExcludeOptions

# ----------------------- #


@lru_cache(maxsize=128)
def _list_adapter[M: BaseModel](cls: type[M]) -> TypeAdapter[list[M]]:
    return TypeAdapter(list[cls])  # type: ignore[valid-type]


# ....................... #


def pydantic_validate[M: BaseModel](
    cls: type[M],
    data: JsonDict,
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
        "Validating data into %s (forbid_extra=%s)",
        cls.__name__,
        forbid_extra,
    )

    return cls.model_validate(data, extra="forbid" if forbid_extra else "ignore")


# ....................... #


def _sequence_as_list[T](seq: Sequence[T]) -> list[T]:
    """Return ``seq`` as a ``list`` without copying when already a list."""

    return seq if isinstance(seq, list) else list(seq)


# ....................... #


def pydantic_validate_many[M: BaseModel](
    cls: type[M],
    data: Sequence[JsonDict],
    *,
    forbid_extra: bool = False,
) -> list[M]:
    logger.trace(
        "Validating %s data items into list[%s] (forbid_extra=%s)",
        len(data),
        cls.__name__,
        forbid_extra,
    )
    adapter = _list_adapter(cls)
    payload = _sequence_as_list(data)

    return adapter.validate_python(
        payload,
        extra="forbid" if forbid_extra else "ignore",
    )


# ....................... #


def pydantic_validate_many_batched[M: BaseModel](
    cls: type[M],
    data: Sequence[JsonDict],
    *,
    batch_size: int = 2000,
    forbid_extra: bool = False,
) -> Iterator[list[M]]:
    """Validate row dicts in fixed-size chunks to cap peak memory.

    Total validation work is similar to :func:`pydantic_validate_many`, but
    only one chunk of models exists at a time in memory.

    :param cls: Pydantic model class for each row.
    :param data: Raw row mappings in global order.
    :param batch_size: Maximum rows per yielded chunk (must be >= 1).
    :param forbid_extra: Forwarded to :class:`~pydantic.TypeAdapter` validation.
    :yields: Consecutive ``list[M]`` chunks covering all of ``data``.
    """

    if batch_size < 1:
        msg = "batch_size must be >= 1"
        raise ValueError(msg)

    seq = _sequence_as_list(data)
    if not seq:
        return

    adapter = _list_adapter(cls)

    for start in range(0, len(seq), batch_size):
        chunk = seq[start : start + batch_size]
        if forbid_extra:
            yield adapter.validate_python(chunk, extra="forbid")
        else:
            yield adapter.validate_python(chunk, extra="ignore")


# ....................... #


def pydantic_dump(
    obj: BaseModel,
    *,
    mode: Literal["json", "python"] = "python",
    exclude: RecordMappingDumpExcludeOptions = {},
) -> JsonDict:
    """Dump a Pydantic model into a JSON-compatible ``dict``.

    :param obj: Model instance to serialize.
    :param exclude: Fine-grained control over which fields are omitted.
    :param mode: Serialization mode.
    :returns: JSON-ready dictionary representation.
    """

    logger.trace(
        "Dumping %s (mode=%s, exclude=%s)",
        type(obj).__name__,
        mode,
        exclude,
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
    exclude: RecordMappingDumpExcludeOptions = {},
) -> list[JsonDict]:
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
        "Dumping %s models into list[dict[str, Any]] (mode=%s, exclude=%s)",
        len(objs),
        mode,
        exclude,
    )

    adapter = _list_adapter(cls)
    dumped = adapter.dump_python(
        _sequence_as_list(objs),
        mode=mode,
        exclude_unset=exclude.get("unset", False),
        exclude_none=exclude.get("none", False),
        exclude_defaults=exclude.get("defaults", False),
        exclude_computed_fields=exclude.get("computed_fields", False),
    )

    return dumped


# ....................... #


def pydantic_dump_many_batched(
    objs: Sequence[BaseModel],
    *,
    batch_size: int = 2000,
    mode: Literal["json", "python"] = "python",
    exclude: RecordMappingDumpExcludeOptions = {},
) -> Iterator[list[JsonDict]]:
    """Dump models in fixed-size chunks to cap peak memory.

    :param objs: Homogeneous sequence of model instances (same concrete type).
    :param batch_size: Maximum models per yielded chunk (must be >= 1).
    :param mode: Serialization mode forwarded to :class:`~pydantic.TypeAdapter`.
    :param exclude: Fine-grained field omission options.
    :yields: Consecutive ``list[JsonDict]`` chunks in original order.
    """

    if batch_size < 1:
        msg = "batch_size must be >= 1"
        raise ValueError(msg)

    if not objs:
        return

    seq = _sequence_as_list(objs)
    cls = type(seq[0])
    adapter = _list_adapter(cls)
    exclude_unset = exclude.get("unset", False)
    exclude_none = exclude.get("none", False)
    exclude_defaults = exclude.get("defaults", False)
    exclude_computed_fields = exclude.get("computed_fields", False)

    for start in range(0, len(seq), batch_size):
        chunk = seq[start : start + batch_size]
        yield adapter.dump_python(
            chunk,
            mode=mode,
            exclude_unset=exclude_unset,
            exclude_none=exclude_none,
            exclude_defaults=exclude_defaults,
            exclude_computed_fields=exclude_computed_fields,
        )


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
    exclude: RecordMappingDumpExcludeOptions = {},
) -> str:
    """Return a stable SHA-256 hash for the serialized model.

    :param model: Model instance to hash.
    :param exclude: Options forwarded to :func:`pydantic_dump`.
    :returns: Hex-encoded SHA-256 digest of the sorted JSON representation.
    """

    logger.trace(
        "Hashing Pydantic model %s (exclude=%s)",
        type(model).__name__,
        exclude,
    )

    data = pydantic_dump(model, exclude=exclude)
    norm_data = _normalize_for_hashing(data)
    raw = orjson.dumps(norm_data, option=orjson.OPT_SORT_KEYS)
    digest = hashlib.sha256(raw).hexdigest()

    return digest


# ....................... #

_CACHE_EXCLUDE_OPTS: Final[RecordMappingDumpExcludeOptions] = (
    RecordMappingDumpExcludeOptions(
        none=True,
        defaults=True,
        computed_fields=True,
    )
)


def pydantic_cache_dump(obj: BaseModel) -> JsonDict:
    """Convenience helper for dumping a Pydantic model for cache storage."""

    return pydantic_dump(obj, exclude=_CACHE_EXCLUDE_OPTS, mode="json")


def pydantic_cache_dump_many(objs: Sequence[BaseModel]) -> list[JsonDict]:
    """Convenience helper for dumping a list of Pydantic models for cache storage."""

    return pydantic_dump_many(objs, exclude=_CACHE_EXCLUDE_OPTS, mode="json")


# ....................... #


def pydantic_transform[Out: BaseModel](
    cls: type[Out],
    model: BaseModel,
    *,
    mode: Literal["json", "python"] = "python",
    exclude: RecordMappingDumpExcludeOptions = {"unset": True},
) -> Out:
    """Convenience helper for model-to-model transformations."""

    dump = pydantic_dump(model, mode=mode, exclude=exclude)

    return pydantic_validate(cls, dump)


# ....................... #


def pydantic_transform_many[Out: BaseModel](
    cls: type[Out],
    models: Sequence[BaseModel],
    *,
    mode: Literal["json", "python"] = "python",
    exclude: RecordMappingDumpExcludeOptions = {"unset": True},
) -> list[Out]:
    """Batch model-to-model transformation.

    This runs **two full passes**: :func:`pydantic_dump_many` on all inputs,
    then :func:`pydantic_validate_many` on the resulting dict list. Peak memory
    holds both the dumped dicts and the output models. For very large batches,
    prefer :func:`pydantic_validate_many_batched` / :func:`pydantic_dump_many_batched`
    or a domain-specific streaming pipeline.
    """

    dumps = pydantic_dump_many(models, mode=mode, exclude=exclude)

    return pydantic_validate_many(cls, dumps)


# ....................... #


def pydantic_secret_converter(v: str | SecretStr) -> SecretStr:
    if isinstance(v, SecretStr):
        return v

    return SecretStr(v)
