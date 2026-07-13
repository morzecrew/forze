"""Resolve a (possibly dotted) sort/field path against a read model's types."""

from collections.abc import Mapping
from types import UnionType
from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel

from forze.base.serialization import stored_field_names_for

# ----------------------- #

_MISSING: Any = object()


# ....................... #


def read_fields_for_model(model: type[BaseModel]) -> frozenset[str]:
    """Pydantic field names on *model* (excludes computed fields)."""

    return stored_field_names_for(model, include_computed=False)


# ....................... #


def _unwrap_optional(annotation: Any) -> Any:
    if get_origin(annotation) in (Union, UnionType):
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _is_basemodel(annotation: Any) -> bool:
    return isinstance(annotation, type) and issubclass(annotation, BaseModel)


def _is_any_like(annotation: Any) -> bool:
    # ``Any``, a bare object, or a wide union we cannot meaningfully walk into.
    if annotation is Any or annotation is object:
        return True
    return get_origin(annotation) in (Union, UnionType) and len(get_args(annotation)) > 2


def _str_keyed_mapping_value(annotation: Any) -> Any:
    """Value annotation of a ``str``-keyed mapping, ``None`` for an untyped one,
    or ``_MISSING`` when *annotation* is not a string-keyed mapping."""

    # ``get_origin`` is the parameterized origin (``dict[str, X]`` -> ``dict``);
    # a bare ``dict``/``Mapping`` annotation has no origin, so fall back to it.
    origin = get_origin(annotation) or annotation

    if origin not in (dict, Mapping) and not (
        isinstance(origin, type) and issubclass(origin, Mapping)
    ):
        return _MISSING

    args = get_args(annotation)
    if not args:
        return None  # untyped mapping → walkable for any value path

    if _unwrap_optional(args[0]) in (str, Any):
        return _unwrap_optional(args[1]) if len(args) == 2 else None

    return _MISSING


def _subpath_resolves(annotation: Any, segments: list[str]) -> bool:
    if not segments:
        return True

    if _is_basemodel(annotation):
        info = annotation.model_fields.get(segments[0])
        if info is None:
            return False
        return _subpath_resolves(_unwrap_optional(info.annotation), segments[1:])

    val = _str_keyed_mapping_value(annotation)
    if val is not _MISSING:
        # A dynamic-key hop; an untyped value is walkable for any remaining path.
        return True if val is None else _subpath_resolves(_unwrap_optional(val), segments[1:])

    # A scalar leaf with path left over is invalid; an ``Any``/wide type cannot
    # be disproved, so allow it (avoid false rejections).
    return _is_any_like(annotation)


def field_path_resolves(
    model: type[BaseModel],
    field: str,
    *,
    materialized: frozenset[str] = frozenset(),
) -> bool:
    """Whether a (possibly dotted) sort/field path resolves on *model*.

    Validates the top-level segment against the model and walks nested Pydantic
    models and ``str``-keyed mappings for dotted paths. ``Any``/untyped
    intermediates are treated as walkable (can't be disproved), so this catches
    the common typo / wrong-field case without rejecting genuine dynamic paths.

    *materialized* names computed fields persisted for this spec; a single-segment
    path naming one resolves (it is a stored, sortable scalar).
    """

    segments = field.split(".")
    head = segments[0]

    if len(segments) == 1 and head in materialized:
        return True

    if not head or head not in model.model_fields:
        return False

    if len(segments) == 1:
        return True

    return _subpath_resolves(_unwrap_optional(model.model_fields[head].annotation), segments[1:])


def _sort_field_resolves(  # pyright: ignore[reportUnusedFunction]
    field: str,
    *,
    read_fields: frozenset[str],
    model: type[BaseModel] | None,
) -> bool:
    """Whether *field* (possibly dotted) is a legal sort key.

    A single-segment key uses flat *read_fields* membership (which already covers
    materialized computed fields). A dotted key needs *model* to walk nested Pydantic
    models and ``str``-keyed mappings via :func:`field_path_resolves`, matching how filters
    resolve nested paths; without a *model* the legacy flat check stands (a dotted key is
    rejected), so callers opt into nested sorts by threading their read model.
    """

    if "." not in field or model is None:
        return field in read_fields

    # A dotted key's root segment must be in the allow-set too, so a nested sort
    # cannot bypass a field excluded from read_fields (a lenient/non-stored field)
    # the way the flat key above is gated — matching validate_runtime_sort_fields.
    root = field.split(".", 1)[0]

    if root not in read_fields:
        return False

    materialized = read_fields - read_fields_for_model(model)
    return field_path_resolves(model, field, materialized=materialized)
