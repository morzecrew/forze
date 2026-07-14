"""The portable shape of a catalogued spec, rendered so it can be hashed."""

from __future__ import annotations

from collections.abc import Mapping, Sequence, Set
from datetime import timedelta
from enum import Enum
from typing import Any

import attrs
from pydantic import BaseModel

from forze.base.exceptions import exc
from forze.base.primitives import stable_json_bytes

from .value_objects import SpecRegistryEntry

# ----------------------- #


def _model_schema(model: type[BaseModel], *, at: str) -> Any:
    try:
        return model.model_json_schema()

    except Exception as error:
        raise exc.configuration(
            f"Cannot fingerprint {at}: {model.__qualname__} has no JSON schema ({error}). "
            f"A spec's models must be schema-able — that schema is the portable shape."
        ) from error


# ....................... #


def _is_codec(value: Any) -> bool:
    # ``ModelCodec`` is a Protocol, so a codec need not be any particular class. The shipped
    # one is attrs and is walked as such (picking up its ``materialized`` set); this catches an
    # application's own implementation, whose only guaranteed portable member is ``model_type``.
    return hasattr(value, "model_type") and hasattr(value, "encode_mapping")


# ....................... #


def _attrs_shape(instance: Any, *, at: str) -> dict[str, Any]:
    """Render an attrs object over exactly the fields attrs itself compares.

    Skipping ``eq=False`` is what keeps the fingerprint honest rather than merely convenient:
    those fields are the derived codec caches (``DocumentSpec.codecs``, ``SearchSpec.read_codec``,
    ``AnalyticsSpec.read_codec``/``ingest_codec``) and the derived kind indexes
    (``GraphModuleSpec``), and attrs already declares them to be no part of the object's value.
    Hashing them would make two *equal* specs fingerprint differently. Nothing is lost: every
    model behind a derived codec is also reachable through a compared field — ``read`` and
    ``write`` on a document, ``model_type`` on a search index, ``read``/``ingest`` on an
    analytics table.

    So spec equality and spec shape agree, which is the property the inventory needs: it is the
    same rule ``SpecRegistry.register_entry`` already dedupes a re-derived spec on.
    """

    return {
        field.name: _shape(getattr(instance, field.name), at=f"{at}.{field.name}")
        for field in attrs.fields(type(instance))  # pyright: ignore[reportUnknownArgumentType]
        if field.eq
    }


# ....................... #


def _shape(value: Any, *, at: str) -> Any:
    """Render *value* as JSON primitives, or refuse.

    **Never falls back to ``str()``, and that is the whole point.** The hash underneath
    (``stable_json_bytes``) serializes with ``default=str``, so an object orjson cannot encode
    is hashed as ``<... object at 0x7f…>`` — a *memory address*, identical within one process
    and different in the next. A fingerprint built that way is stable across every in-process
    test and worthless for the one thing it exists to do: compare two processes. The same trap
    sits under ``frozenset``, which orjson also cannot encode, and whose ``str()`` renders in
    iteration order — and string hashing is seeded per process, so a set of two or more field
    names would hash differently in every run.

    Every value is therefore rendered explicitly, sets are sorted, and an unrecognized one
    raises rather than being quietly stringified.
    """

    if value is None:
        return None

    if isinstance(value, Enum):  # before `str`: a StrEnum *is* a str
        return value.value

    if isinstance(value, bool | int | float | str):
        return value

    if isinstance(value, timedelta):
        return value.total_seconds()

    if isinstance(value, type):
        if issubclass(value, BaseModel):
            return _model_schema(value, at=at)

        raise exc.configuration(
            f"Cannot fingerprint {at}: {value.__qualname__} is not a pydantic model, so it has "
            f"no portable shape to hash."
        )

    if attrs.has(
        type(value)  # pyright: ignore[reportUnknownArgumentType]
    ):  # nested specs, the shipped codec, encryption + query policies
        return _attrs_shape(value, at=at)

    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")

    if _is_codec(value):
        return {"codec": _shape(value.model_type, at=f"{at}.model_type")}

    if isinstance(value, Mapping):  # a document's `write` types are a TypedDict — a dict at runtime
        return {str(key): _shape(item, at=f"{at}[{key}]") for key, item in value.items()}  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]

    if isinstance(value, Set):  # sorted by canonical bytes: iteration order is hash-seeded
        return sorted((_shape(item, at=f"{at}{{}}") for item in value), key=stable_json_bytes)  # pyright: ignore[reportUnknownVariableType]

    if isinstance(value, Sequence) and not isinstance(value, bytes):  # order is meaning; keep it
        return [_shape(item, at=f"{at}[{index}]") for index, item in enumerate(value)]  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]

    raise exc.configuration(
        f"Cannot fingerprint {at}: {type(value).__qualname__} has no portable rendering. "
        f"Hashing it would fall back to its repr, which carries a memory address and changes "
        f"every process."
    )


# ....................... #


def entry_shape(entry: SpecRegistryEntry) -> dict[str, Any]:
    """The portable shape of one catalogued spec.

    :attr:`~forze.application.contracts.inventory.SpecRegistryEntry.source` is deliberately
    absent. Who registered a spec — the author, a kit, the framework — is diagnostic, and moving
    a registration out of an application and into a kit changes nothing an export or an import
    can observe.

    :attr:`~forze.application.contracts.inventory.SpecRegistryEntry.disposition` very much is
    present: it is what an export *does* with the plane, so an analytics table whose provenance
    flips ``PROJECTED`` → ``SYSTEM_OF_RECORD`` turns a plane that was rebuilt on the target into
    one an export must refuse — the same rows, an entirely different artifact.
    """

    return {
        "plane": entry.plane.value,
        "name": entry.name,
        "disposition": entry.disposition.value,
        "spec": _shape(entry.spec, at=entry.ref.label()),
    }
