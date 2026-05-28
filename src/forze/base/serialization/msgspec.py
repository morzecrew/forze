"""Serialization helpers around msgspec structs."""

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from functools import lru_cache
from types import NoneType, UnionType
from typing import (
    Annotated,
    Any,
    Final,
    Iterator,
    Mapping,
    NoReturn,
    Sequence,
    Union,
    get_args,
    get_origin,
)
from uuid import UUID

import msgspec
from pydantic import BaseModel

from .._logger import logger
from ..exceptions import exc
from ..primitives import JsonDict
from .model_codec import EncodeMode, RecordMappingDumpExcludeOptions
from .pydantic import pydantic_dump

# ----------------------- #

_PYTHON_MODE_BUILTIN_TYPES: Final = (
    bytes,
    bytearray,
    memoryview,
    datetime,
    date,
    time,
    timedelta,
    UUID,
    Decimal,
)

# ....................... #


@lru_cache(maxsize=128)
def _struct_fields_cached(
    cls: type[msgspec.Struct],
) -> tuple[msgspec.structs.FieldInfo, ...]:
    return msgspec.structs.fields(cls)


# ....................... #


@lru_cache(maxsize=256)
def _struct_field_names_cached(cls: type[msgspec.Struct]) -> frozenset[str]:
    return frozenset(field.encode_name for field in _struct_fields_cached(cls))


# ....................... #


def _sequence_as_list[T](seq: Sequence[T]) -> list[T]:
    """Return ``seq`` as a ``list`` without copying when already a list."""

    return seq if isinstance(seq, list) else list(seq)


# ....................... #


def _strip_annotated(tp: Any) -> Any:
    while get_origin(tp) is Annotated:
        tp = get_args(tp)[0]

    return tp


# ....................... #


def _is_msgspec_struct_type(tp: Any) -> bool:
    return isinstance(tp, type) and issubclass(tp, msgspec.Struct)


# ....................... #


def _type_may_contain_struct(tp: Any) -> bool:
    tp = _strip_annotated(tp)

    if _is_msgspec_struct_type(tp):
        return True

    origin = get_origin(tp)

    if origin in (Union, UnionType):
        return any(_type_may_contain_struct(arg) for arg in get_args(tp))

    if origin in (list, set, frozenset, tuple):
        args = get_args(tp)
        if len(args) == 2 and args[1] is Ellipsis:
            return _type_may_contain_struct(args[0])

        return any(_type_may_contain_struct(arg) for arg in args)

    if origin in (dict, Mapping):
        args = get_args(tp)
        return len(args) == 2 and _type_may_contain_struct(args[1])

    return False


# ....................... #


def _object_path(path: str, key: str) -> str:
    return f"{path}.{key}"


def _index_path(path: str, index: int) -> str:
    return f"{path}[{index}]"


def _mapping_path(path: str, key: Any) -> str:
    return f"{path}[{key!r}]"


# ....................... #


def _raise_unknown_field(name: str, path: str) -> NoReturn:
    msg = f"Object contains unknown field `{name}`"
    if path != "$":
        msg += f" - at `{path}`"

    raise msgspec.ValidationError(msg)


# ....................... #


def _validate_union_no_unknown_fields(
    value: Any,
    tp: Any,
    path: str,
) -> None:
    args = get_args(tp)
    non_none = [arg for arg in args if arg is not NoneType]

    if len(non_none) == 1 and len(non_none) != len(args):
        _validate_no_unknown_fields(value, non_none[0], path)
        return

    structish = [arg for arg in args if _type_may_contain_struct(arg)]

    if not structish:
        return

    errors: list[msgspec.ValidationError] = []
    for arg in structish:
        try:
            _validate_no_unknown_fields(value, arg, path)
        except msgspec.ValidationError as exc:
            errors.append(exc)
        else:
            return

    if len(structish) == len(args) and errors:
        raise errors[0]


# ....................... #


def _validate_no_unknown_fields(
    value: Any,
    expected_type: Any,
    path: str = "$",
) -> None:
    tp = _strip_annotated(expected_type)

    if value is None:
        return

    if _is_msgspec_struct_type(tp):
        if not isinstance(value, Mapping):
            return

        field_map = {field.encode_name: field for field in _struct_fields_cached(tp)}

        for key, child in value.items():  # pyright: ignore[reportUnknownVariableType]
            key_str = str(key)  # pyright: ignore[reportUnknownArgumentType]
            field = field_map.get(key_str)

            if field is None:
                _raise_unknown_field(key_str, path)

            _validate_no_unknown_fields(
                child,
                field.type,
                _object_path(path, field.encode_name),
            )

        return

    origin = get_origin(tp)

    if origin in (Union, UnionType):
        _validate_union_no_unknown_fields(value, tp, path)
        return

    if origin in (list, set, frozenset):
        args = get_args(tp)
        if not args or not isinstance(value, Sequence | set | frozenset):
            return

        inner = args[0]
        if not _type_may_contain_struct(inner):
            return

        for index, item in enumerate(value):  # type: ignore[unknown-type]
            _validate_no_unknown_fields(item, inner, _index_path(path, index))

        return

    if origin is tuple:
        args = get_args(tp)
        if not args or not isinstance(value, Sequence):
            return

        if len(args) == 2 and args[1] is Ellipsis:
            inner = args[0]
            if not _type_may_contain_struct(inner):
                return

            for index, item in enumerate(value):  # type: ignore[unknown-type]
                _validate_no_unknown_fields(item, inner, _index_path(path, index))

            return

        for index, (item, inner) in enumerate(  # type: ignore[unknown-type]
            zip(  # type: ignore[unknown-type]
                value,  # pyright: ignore[reportUnknownArgumentType]
                args,
                strict=False,
            )
        ):
            if _type_may_contain_struct(inner):
                _validate_no_unknown_fields(item, inner, _index_path(path, index))

        return

    if origin in (dict, Mapping):
        args = get_args(tp)
        if len(args) != 2 or not isinstance(value, Mapping):
            return

        inner = args[1]
        if not _type_may_contain_struct(inner):
            return

        for key, item in value.items():  # type: ignore[unknown-type]
            _validate_no_unknown_fields(item, inner, _mapping_path(path, key))


# ....................... #


def _ensure_unset_not_requested(exclude: RecordMappingDumpExcludeOptions) -> None:
    if exclude.get("unset", False):
        raise exc.internal(
            "msgspec codec does not support exclude={'unset': True}; strip unset fields before crossing the application boundary",
        )


# ....................... #


def _default_value_for_field(field: msgspec.structs.FieldInfo) -> Any:
    if field.default_factory is not msgspec.NODEFAULT:
        return field.default_factory()

    return field.default


# ....................... #


def _dump_leaf(value: Any, *, mode: EncodeMode) -> Any:
    if mode == "json":
        return msgspec.to_builtins(value)

    return msgspec.to_builtins(
        value,
        builtin_types=_PYTHON_MODE_BUILTIN_TYPES,
    )


# ....................... #


def _dump_value(
    value: Any,
    *,
    mode: EncodeMode,
    exclude: RecordMappingDumpExcludeOptions,
) -> Any:
    if isinstance(value, msgspec.Struct):
        return _dump_struct(value, mode=mode, exclude=exclude)

    if isinstance(value, dict):
        return {
            key: _dump_value(item, mode=mode, exclude=exclude)  # type: ignore[unknown-type]
            for key, item in value.items()  # type: ignore[unknown-type]
        }

    if isinstance(value, list):
        return [_dump_value(item, mode=mode, exclude=exclude) for item in value]  # type: ignore[unknown-type]

    if isinstance(value, tuple):
        items = [_dump_value(item, mode=mode, exclude=exclude) for item in value]  # type: ignore[unknown-type]
        return tuple(items) if mode == "python" else items

    if isinstance(value, set | frozenset):
        return [_dump_value(item, mode=mode, exclude=exclude) for item in value]  # type: ignore[unknown-type]

    return _dump_leaf(value, mode=mode)


# ....................... #


def _dump_struct(
    obj: msgspec.Struct,
    *,
    mode: EncodeMode,
    exclude: RecordMappingDumpExcludeOptions,
) -> JsonDict:
    out: JsonDict = {}

    for field in _struct_fields_cached(type(obj)):
        value = getattr(obj, field.name)

        if exclude.get("none", False) and value is None:
            continue

        if exclude.get("defaults", False) and not field.required:
            if value == _default_value_for_field(field):
                continue

        out[field.encode_name] = _dump_value(
            value,
            mode=mode,
            exclude=exclude,
        )

    return out


# ....................... #


def msgspec_validate[T: msgspec.Struct](
    cls: type[T],
    data: JsonDict,
    *,
    forbid_extra: bool = False,
) -> T:
    """Validate raw ``data`` into a msgspec struct instance."""

    logger.trace(
        "Validating data into %s (forbid_extra=%s)",
        cls.__name__,
        forbid_extra,
    )

    if forbid_extra:
        _validate_no_unknown_fields(data, cls)

    return msgspec.convert(data, cls, strict=False)


# ....................... #


def msgspec_validate_many[T: msgspec.Struct](
    cls: type[T],
    data: Sequence[JsonDict],
    *,
    forbid_extra: bool = False,
) -> list[T]:
    """Validate raw mapping rows into a list of msgspec structs."""

    payload = _sequence_as_list(data)

    logger.trace(
        "Validating %s data items into list[%s] (forbid_extra=%s)",
        len(payload),
        cls.__name__,
        forbid_extra,
    )

    if forbid_extra:
        for index, item in enumerate(payload):
            _validate_no_unknown_fields(item, cls, _index_path("$", index))

    return msgspec.convert(payload, list[cls], strict=False)  # type: ignore[valid-type]


# ....................... #


def msgspec_validate_many_batched[T: msgspec.Struct](
    cls: type[T],
    data: Sequence[JsonDict],
    *,
    batch_size: int = 2000,
    forbid_extra: bool = False,
) -> Iterator[list[T]]:
    """Validate raw mapping rows into msgspec structs in fixed-size chunks."""

    if batch_size < 1:
        msg = "batch_size must be >= 1"
        raise ValueError(msg)

    seq = _sequence_as_list(data)

    if not seq:
        return

    for start in range(0, len(seq), batch_size):
        chunk = seq[start : start + batch_size]

        yield msgspec_validate_many(
            cls,
            chunk,
            forbid_extra=forbid_extra,
        )


# ....................... #


def msgspec_dump(
    obj: msgspec.Struct,
    *,
    mode: EncodeMode = "python",
    exclude: RecordMappingDumpExcludeOptions = {},
) -> JsonDict:
    """Dump a msgspec struct into a JSON-compatible ``dict``."""

    _ensure_unset_not_requested(exclude)

    logger.trace(
        "Dumping %s (mode=%s, exclude=%s)",
        type(obj).__name__,
        mode,
        exclude,
    )

    return _dump_struct(obj, mode=mode, exclude=exclude)


# ....................... #


def msgspec_dump_many(
    objs: Sequence[msgspec.Struct],
    *,
    mode: EncodeMode = "python",
    exclude: RecordMappingDumpExcludeOptions = {},
) -> list[JsonDict]:
    """Dump a list of msgspec structs into JSON-compatible dicts."""

    _ensure_unset_not_requested(exclude)
    if not objs:
        return []

    logger.trace(
        "Dumping %s msgspec models into list[JsonDict] (mode=%s, exclude=%s)",
        len(objs),
        mode,
        exclude,
    )

    return [msgspec_dump(obj, mode=mode, exclude=exclude) for obj in objs]


# ....................... #


def msgspec_dump_many_batched(
    objs: Sequence[msgspec.Struct],
    *,
    batch_size: int = 2000,
    mode: EncodeMode = "python",
    exclude: RecordMappingDumpExcludeOptions = {},
) -> Iterator[list[JsonDict]]:
    """Dump msgspec structs in fixed-size chunks."""

    _ensure_unset_not_requested(exclude)

    if batch_size < 1:
        msg = "batch_size must be >= 1"
        raise ValueError(msg)

    seq = _sequence_as_list(objs)

    if not seq:
        return

    for start in range(0, len(seq), batch_size):
        chunk = seq[start : start + batch_size]

        yield msgspec_dump_many(chunk, mode=mode, exclude=exclude)


# ....................... #


def msgspec_encode_json_bytes(
    obj: msgspec.Struct,
    *,
    exclude: RecordMappingDumpExcludeOptions = {},
) -> bytes:
    """Serialize a msgspec struct to JSON UTF-8 bytes for wire transport."""

    _ensure_unset_not_requested(exclude)

    logger.trace(
        "Encoding %s to JSON bytes (exclude=%s)",
        type(obj).__name__,
        exclude,
    )

    if exclude.get("none") or exclude.get("defaults") or exclude.get("computed_fields"):
        return msgspec.json.encode(msgspec_dump(obj, mode="json", exclude=exclude))

    return msgspec.json.encode(obj)


# ....................... #


def msgspec_decode_json_bytes[T: msgspec.Struct](
    cls: type[T],
    raw: bytes | str,
    *,
    forbid_extra: bool = False,
    encoding: str = "utf-8",
) -> T:
    """Deserialize JSON UTF-8 bytes or text into a msgspec struct instance."""

    if isinstance(raw, str):
        raw = raw.encode(encoding)

    logger.trace(
        "Decoding JSON bytes into %s (forbid_extra=%s)",
        cls.__name__,
        forbid_extra,
    )

    if forbid_extra:
        data = msgspec.json.decode(raw)
        if not isinstance(data, dict):
            msg = f"Expected object at $, got {type(data).__name__}"
            raise msgspec.ValidationError(msg)

        _validate_no_unknown_fields(data, cls)
        return msgspec.convert(data, cls, strict=False)

    return msgspec.json.decode(raw, type=cls)


# ....................... #


def msgspec_field_names(
    cls: type[msgspec.Struct],
    *,
    include_computed: bool = True,
) -> frozenset[str]:
    """Return the encoded field names defined on a msgspec struct class."""

    del include_computed
    return _struct_field_names_cached(cls)


# ....................... #


def msgspec_transform[T: msgspec.Struct](
    cls: type[T],
    model: msgspec.Struct | BaseModel,
    *,
    mode: EncodeMode = "python",
    exclude: RecordMappingDumpExcludeOptions = {},
) -> T:
    """Transform a Pydantic model or msgspec struct into a msgspec struct."""

    _ensure_unset_not_requested(exclude)

    if isinstance(model, BaseModel):
        dump = pydantic_dump(model, mode=mode, exclude=exclude)

    else:
        dump = msgspec_dump(model, mode=mode, exclude=exclude)

    return msgspec_validate(cls, dump)


# ....................... #


def msgspec_transform_many[T: msgspec.Struct](
    cls: type[T],
    models: Sequence[msgspec.Struct | BaseModel],
    *,
    mode: EncodeMode = "python",
    exclude: RecordMappingDumpExcludeOptions = {},
) -> list[T]:
    """Transform many Pydantic models or msgspec structs into msgspec structs."""

    _ensure_unset_not_requested(exclude)

    return [
        msgspec_transform(cls, model, mode=mode, exclude=exclude) for model in models
    ]
