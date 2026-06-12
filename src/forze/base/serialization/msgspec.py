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

import attrs
import msgspec
from pydantic import BaseModel

from .._logger import logger
from ..exceptions import exc
from ..primitives import JsonDict
from ._common import sequence_as_list, validate_batch_size
from .model_codec import EncodeMode, ModelDumpExcludeOptions
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


# ....................... #
# Unknown-field pre-pass: precompiled scan plans.
#
# Per struct type we compile (once, cached) a plan holding the allowed
# encode-names and, for each field, which child plan (if any) must be walked
# (direct struct, list-of, dict-of, tuple, union). The per-row runtime walk
# uses only the plan — no ``get_origin`` / ``get_args`` / field-dict rebuilds
# per row. Rejection semantics and error messages are byte-identical to the
# previous recursive introspection.

_UNKNOWN_FIELD: Final = object()
"""Sentinel distinguishing "unknown field" from "known field, no recursion needed"."""

# ....................... #


@attrs.define(slots=True)
class _StructScan:
    """Scan plan for one struct type.

    ``fields`` maps allowed encode-names to the child scan plan, or ``None``
    when the field type cannot contain a struct (no recursion needed).
    """

    fields: dict[str, Any] = attrs.field(factory=dict)

    # ....................... #

    def scan(self, value: Any, path: str) -> None:
        if value is None or not isinstance(value, Mapping):
            return

        fields = self.fields

        for key, child in value.items():  # pyright: ignore[reportUnknownVariableType]
            key_str = str(key)  # pyright: ignore[reportUnknownArgumentType]
            sub = fields.get(key_str, _UNKNOWN_FIELD)

            if sub is _UNKNOWN_FIELD:
                _raise_unknown_field(key_str, path)

            if sub is not None:
                sub.scan(child, _object_path(path, key_str))


# ....................... #


@attrs.define(slots=True)
class _OptionalScan:
    """Union with exactly one non-``None`` member: skip ``None``, walk the member."""

    inner: Any

    # ....................... #

    def scan(self, value: Any, path: str) -> None:
        if value is None:
            return

        self.inner.scan(value, path)


# ....................... #


@attrs.define(slots=True)
class _UnionScan:
    """Multi-member union: first member that accepts wins.

    The collected error is re-raised only when *every* union member is
    struct-ish (mirrors the previous walker).
    """

    members: list[Any]
    raise_when_all_fail: bool = attrs.field(kw_only=True)

    # ....................... #

    def scan(self, value: Any, path: str) -> None:
        if value is None:
            return

        first_error: msgspec.ValidationError | None = None

        for member in self.members:
            try:
                member.scan(value, path)
            except msgspec.ValidationError as error:
                if first_error is None:
                    first_error = error
            else:
                return

        if self.raise_when_all_fail and first_error is not None:
            raise first_error


# ....................... #


@attrs.define(slots=True)
class _ItemsScan:
    """Homogeneous collection (list/set/frozenset, or variadic tuple) of struct-ish items."""

    inner: Any
    include_sets: bool = attrs.field(kw_only=True)

    # ....................... #

    def scan(self, value: Any, path: str) -> None:
        if value is None:
            return

        if self.include_sets:
            if not isinstance(value, Sequence | set | frozenset):
                return
        elif not isinstance(value, Sequence):
            return

        inner = self.inner

        for index, item in enumerate(value):  # type: ignore[unknown-type]
            inner.scan(item, _index_path(path, index))


# ....................... #


@attrs.define(slots=True)
class _TupleFixedScan:
    """Fixed-shape tuple: positional member plans (``None`` entries are skipped)."""

    members: list[Any]

    # ....................... #

    def scan(self, value: Any, path: str) -> None:
        if value is None or not isinstance(value, Sequence):
            return

        for index, (
            item,  # pyright: ignore[reportUnknownVariableType]
            member,
        ) in enumerate(
            zip(
                value,  # pyright: ignore[reportUnknownArgumentType]
                self.members,
                strict=False,
            )
        ):
            if member is not None:
                member.scan(item, _index_path(path, index))


# ....................... #


@attrs.define(slots=True)
class _MappingScan:
    """Mapping with struct-ish values: walk each value under its repr'd key path."""

    inner: Any

    # ....................... #

    def scan(self, value: Any, path: str) -> None:
        if value is None or not isinstance(value, Mapping):
            return

        inner = self.inner

        for key, item in value.items():  # type: ignore[unknown-type]
            inner.scan(item, _mapping_path(path, key))


# ....................... #

_STRUCT_SCAN_CACHE: dict[type[msgspec.Struct], _StructScan] = {}
"""Module-level plan cache: one compiled :class:`_StructScan` per struct type."""

_STRUCT_SCAN_CACHE_MAX: Final = 1024
"""Bound on the plan cache; on overflow the cache is cleared and plans recompile lazily."""

# ....................... #


def _compile_struct_scan(
    cls: type[msgspec.Struct],
    memo: dict[type[msgspec.Struct], _StructScan],
) -> _StructScan:
    """Compile (or fetch the cached) scan plan for ``cls``.

    ``memo`` holds in-progress plans for the current compilation so recursive
    struct graphs resolve to the same node instead of recursing forever.
    """

    cached = _STRUCT_SCAN_CACHE.get(cls)
    if cached is not None:
        return cached

    in_progress = memo.get(cls)
    if in_progress is not None:
        return in_progress

    node = _StructScan()
    memo[cls] = node

    try:
        node.fields = {
            field.encode_name: _compile_scan(field.type, memo)
            for field in _struct_fields_cached(cls)
        }

    finally:
        del memo[cls]

    if len(_STRUCT_SCAN_CACHE) >= _STRUCT_SCAN_CACHE_MAX:
        _STRUCT_SCAN_CACHE.clear()

    _STRUCT_SCAN_CACHE[cls] = node

    return node


# ....................... #


def _compile_scan(
    tp: Any,
    memo: dict[type[msgspec.Struct], _StructScan],
) -> Any | None:
    """Compile a scan plan for ``tp``; ``None`` when nothing reachable can reject."""

    tp = _strip_annotated(tp)

    if _is_msgspec_struct_type(tp):
        return _compile_struct_scan(tp, memo)

    origin = get_origin(tp)

    if origin in (Union, UnionType):
        args = get_args(tp)
        non_none = [arg for arg in args if arg is not NoneType]

        if len(non_none) == 1 and len(non_none) != len(args):
            inner = _compile_scan(non_none[0], memo)
            return _OptionalScan(inner) if inner is not None else None

        structish = [arg for arg in args if _type_may_contain_struct(arg)]

        if not structish:
            return None

        members = [_compile_scan(arg, memo) for arg in structish]

        if any(member is None for member in members):
            # A member that can never reject always "accepts", and the walker
            # returns on the first accepting member — the union never raises.
            return None

        return _UnionScan(members, raise_when_all_fail=len(structish) == len(args))

    if origin in (list, set, frozenset):
        args = get_args(tp)

        if not args:
            return None

        inner = _compile_scan(args[0], memo)
        return _ItemsScan(inner, include_sets=True) if inner is not None else None

    if origin is tuple:
        args = get_args(tp)

        if not args:
            return None

        if len(args) == 2 and args[1] is Ellipsis:
            inner = _compile_scan(args[0], memo)
            return _ItemsScan(inner, include_sets=False) if inner is not None else None

        members = [_compile_scan(arg, memo) for arg in args]

        if all(member is None for member in members):
            return None

        return _TupleFixedScan(members)

    if origin in (dict, Mapping):
        args = get_args(tp)

        if len(args) != 2:
            return None

        inner = _compile_scan(args[1], memo)
        return _MappingScan(inner) if inner is not None else None

    return None


# ....................... #


def _validate_no_unknown_fields(
    value: Any,
    expected_type: Any,
    path: str = "$",
) -> None:
    """Reject mappings carrying keys not declared on the target struct tree.

    Backed by per-struct precompiled scan plans (allowed encode-names plus
    which fields need recursion), compiled once per struct type and cached.
    Error messages are byte-identical to msgspec's native
    ``forbid_unknown_fields`` enforcement; prefer setting
    ``forbid_unknown_fields=True`` on your Structs so this pre-pass can be
    skipped entirely.
    """

    plan = _compile_scan(expected_type, {})

    if plan is not None:
        plan.scan(value, path)


# ....................... #


def _collect_struct_types(tp: Any, out: set[type[msgspec.Struct]]) -> None:
    """Collect struct types syntactically reachable from a type expression."""

    tp = _strip_annotated(tp)

    if _is_msgspec_struct_type(tp):
        out.add(tp)
        return

    origin = get_origin(tp)

    if origin in (Union, UnionType, list, set, frozenset, tuple, dict, Mapping):
        for arg in get_args(tp):
            if arg is not Ellipsis:
                _collect_struct_types(arg, out)


# ....................... #


@lru_cache(maxsize=256)
def _struct_tree_forbids_unknown_fields(cls: type[msgspec.Struct]) -> bool:
    """True when ``cls`` and every reachable child struct forbid unknown fields natively.

    When true, ``msgspec.convert`` already enforces ``forbid_extra`` with
    byte-identical error messages at zero extra cost, so the Python pre-pass
    is skipped. The rule is conservative: a mixed tree (any reachable struct
    without ``forbid_unknown_fields=True``) keeps the full Python pre-pass
    for the whole tree.
    """

    seen: set[type[msgspec.Struct]] = set()
    stack: list[type[msgspec.Struct]] = [cls]

    while stack:
        struct = stack.pop()

        if struct in seen:
            continue

        seen.add(struct)

        if not struct.__struct_config__.forbid_unknown_fields:
            return False

        children: set[type[msgspec.Struct]] = set()

        for field in _struct_fields_cached(struct):
            _collect_struct_types(field.type, children)

        stack.extend(children)

    return True


# ....................... #


def _ensure_unset_not_requested(exclude: ModelDumpExcludeOptions) -> None:
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
    exclude: ModelDumpExcludeOptions,
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
    exclude: ModelDumpExcludeOptions,
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
    """Validate raw ``data`` into a msgspec struct instance.

    Tip: set ``forbid_unknown_fields=True`` on your Structs for free
    ``forbid_extra`` enforcement — when the target struct and every reachable
    child struct forbid unknown fields natively, the Python pre-pass is
    skipped and ``msgspec.convert`` rejects unknowns with identical error
    messages. A mixed tree conservatively keeps the full Python pre-pass.
    """

    logger.trace(
        "Validating data into %s (forbid_extra=%s)",
        cls.__name__,
        forbid_extra,
    )

    if forbid_extra and not _struct_tree_forbids_unknown_fields(cls):
        _validate_no_unknown_fields(data, cls)

    return msgspec.convert(data, cls, strict=False)


# ....................... #


def msgspec_convert[M: msgspec.Struct](
    cls: type[M],
    data: JsonDict,
) -> M:
    """Convert a trusted mapping into a struct without unknown-field scanning."""

    logger.trace("Msgspec convert into %s", cls.__name__)

    return msgspec.convert(data, cls, strict=False)


# ....................... #


def msgspec_convert_many[T: msgspec.Struct](
    cls: type[T],
    data: Sequence[JsonDict],
) -> list[T]:
    """Bulk convert trusted rows via one ``msgspec.convert`` (no ``forbid_extra`` walk)."""

    payload = sequence_as_list(data)

    if not payload:
        return []

    logger.trace(
        "Msgspec convert %s rows into list[%s]",
        len(payload),
        cls.__name__,
    )

    return msgspec.convert(payload, list[cls], strict=False)  # type: ignore[valid-type]


# ....................... #


def msgspec_convert_many_batched[T: msgspec.Struct](
    cls: type[T],
    data: Sequence[JsonDict],
    *,
    batch_size: int = 2000,
) -> Iterator[list[T]]:
    """Yield struct chunks using trusted bulk convert only."""

    validate_batch_size(batch_size)

    seq = sequence_as_list(data)

    if not seq:
        return

    for start in range(0, len(seq), batch_size):
        chunk = seq[start : start + batch_size]
        yield msgspec_convert_many(cls, chunk)


# ....................... #


def msgspec_validate_many[T: msgspec.Struct](
    cls: type[T],
    data: Sequence[JsonDict],
    *,
    forbid_extra: bool = False,
) -> list[T]:
    """Validate raw mapping rows into a list of msgspec structs.

    Tip: set ``forbid_unknown_fields=True`` on your Structs for free
    ``forbid_extra`` enforcement (the per-row Python pre-pass is skipped when
    the whole struct tree forbids unknown fields natively; see
    :func:`msgspec_validate`).
    """

    payload = sequence_as_list(data)

    logger.trace(
        "Validating %s data items into list[%s] (forbid_extra=%s)",
        len(payload),
        cls.__name__,
        forbid_extra,
    )

    if forbid_extra and not _struct_tree_forbids_unknown_fields(cls):
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

    validate_batch_size(batch_size)

    seq = sequence_as_list(data)

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
    exclude: ModelDumpExcludeOptions = {},
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
    exclude: ModelDumpExcludeOptions = {},
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
    exclude: ModelDumpExcludeOptions = {},
) -> Iterator[list[JsonDict]]:
    """Dump msgspec structs in fixed-size chunks."""

    _ensure_unset_not_requested(exclude)

    validate_batch_size(batch_size)

    seq = sequence_as_list(objs)

    if not seq:
        return

    for start in range(0, len(seq), batch_size):
        chunk = seq[start : start + batch_size]

        yield msgspec_dump_many(chunk, mode=mode, exclude=exclude)


# ....................... #


def msgspec_encode_json_bytes(
    obj: msgspec.Struct,
    *,
    exclude: ModelDumpExcludeOptions = {},
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
    """Deserialize JSON UTF-8 bytes or text into a msgspec struct instance.

    Tip: set ``forbid_unknown_fields=True`` on your Structs for free
    ``forbid_extra`` enforcement (the Python pre-pass is skipped when the
    whole struct tree forbids unknown fields natively; see
    :func:`msgspec_validate`).
    """

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

        if not _struct_tree_forbids_unknown_fields(cls):
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
    exclude: ModelDumpExcludeOptions = {},
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
    exclude: ModelDumpExcludeOptions = {},
) -> list[T]:
    """Transform many Pydantic models or msgspec structs into msgspec structs."""

    _ensure_unset_not_requested(exclude)

    return [
        msgspec_transform(cls, model, mode=mode, exclude=exclude) for model in models
    ]
