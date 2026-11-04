"""Document aggregate codec bundle."""

from __future__ import annotations

import functools
from collections.abc import Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from re import Pattern
from typing import Any, Generic, TypeVar, cast, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application._logger import logger
from forze.base.primitives import FrozenTimeSource, JsonDict, bind_time_source, utcnow
from forze.base.serialization import ModelCodec, default_model_codec, model_codec_for
from forze.base.serialization.pydantic import pydantic_dump_many
from forze.domain.constants import ID_FIELD
from forze.domain.models import BaseDTO, Document, DocumentHistory

from .write_types import DocumentWriteTypes

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentCodecs(Generic[R, D, C, U]):
    """Codecs for read, write, and history models on a document aggregate."""

    read: ModelCodec[R, Any]
    """Read-model codec."""

    domain: ModelCodec[D, Any] | None = None
    """Domain-model codec when the aggregate is writable."""

    create: ModelCodec[D, Any] | None = None
    """Codec for transforming create commands into domain models."""

    update: ModelCodec[U, Any] | None = None
    """Update-command codec."""

    history: ModelCodec[Any, Any] | None = None
    """History-row codec when history is enabled."""


# ....................... #


def _history_codec_for_domain(domain_model: type[Document]) -> ModelCodec[Any, Any]:
    history_type = DocumentHistory[domain_model]  # type: ignore[valid-type]

    return default_model_codec(history_type)


# ....................... #


def document_codecs_for_spec(
    *,
    read: type[R],
    write: DocumentWriteTypes[D, C, U] | None,
    history_enabled: bool,
    materialized: frozenset[str] = frozenset(),
) -> DocumentCodecs[R, D, C, U]:
    """Build default codecs from document spec model types.

    *materialized* names ``@computed_field`` members on the read and domain models
    that are persisted (and thus queryable); they are threaded into the read,
    domain, and create codecs so those fields are written to storage. Create/update
    commands never carry materialized fields (a derived value cannot be set directly).
    """

    read_codec = model_codec_for(read, materialized=materialized)
    domain: ModelCodec[D, Any] | None = None
    create: ModelCodec[D, Any] | None = None
    update: ModelCodec[U, Any] | None = None
    history: ModelCodec[Any, Any] | None = None

    if write is not None:
        domain_type = write["domain"]
        domain = model_codec_for(domain_type, materialized=materialized)
        create = model_codec_for(domain_type, materialized=materialized)

        if "update_cmd" in write:
            update = default_model_codec(write["update_cmd"])

        if history_enabled:
            history = _history_codec_for_domain(domain_type)

    return DocumentCodecs(
        read=read_codec,
        domain=domain,
        create=create,
        update=update,
        history=history,
    )


# ....................... #


def document_codecs_for_write_types(
    write_types: DocumentWriteTypes[D, C, U],
    *,
    read: type[R] | None = None,
    history_enabled: bool = False,
) -> DocumentCodecs[R, D, C, U]:
    """Build codecs from write types (read defaults to domain when omitted)."""

    read_type = read if read is not None else cast(type[R], write_types["domain"])

    return document_codecs_for_spec(
        read=read_type,
        write=write_types,
        history_enabled=history_enabled,
    )


# ....................... #


def domains_from_create_payloads(
    codec: ModelCodec[Any, Any],
    payloads: Sequence[Any],
    ids: Sequence[UUID] | None = None,
) -> Sequence[Any]:
    """Transform create payloads into domain models, stamping caller-supplied ids.

    ``ids`` is how a write gateway keeps a batch's generated keys aligned with its
    payloads — the position in ``ids`` is the position in ``payloads``, which is why
    the zip is strict.

    :param codec: Create-DTO codec for the document.
    :param payloads: Create payloads to transform.
    :param ids: Per-payload ids to stamp, positionally aligned with ``payloads``.
    :returns: The domain models, with ids applied when given.
    """

    if ids is None:
        return list(codec.transform_many(payloads))

    domain: type[BaseModel] = codec.model_type

    # A ``UUID`` validates to itself; the copy keeps any other id as it is, validation not.
    if _reads_no_id(domain) and all(type(i) is UUID for i in ids):
        # Validated with its id, so no default id is drawn only to be replaced, and no copy.
        return list(codec.transform_many(payloads, update=[{ID_FIELD: i} for i in ids]))

    models = list(codec.transform_many(payloads))

    # Shallow: each model was just built from a dump of its payload and is thrown away
    # here, so nothing else holds its nested values; a deep copy only doubled the cost.
    return [m.model_copy(update={ID_FIELD: i}) for m, i in zip(models, ids, strict=True)]


def rows_from_create_payloads(
    domain: type[BaseModel],
    payloads: Sequence[BaseModel],
    ids: Sequence[UUID],
    *,
    now: datetime,
) -> list[JsonDict] | None:
    """The mappings the *domain* models built from *payloads* would dump to, without building
    them: each payload's dump, its id from *ids* (positionally), the first revision, and *now*
    for both timestamps, which the domain draws from the same clock.

    :param domain: The document's domain model.
    :param payloads: Create payloads, positionally aligned with *ids*.
    :param ids: Per-payload ids.
    :param now: The instant stamped as the domain's timestamp defaults would stamp it, on both
        timestamps of every row alike.
    :returns: The mappings; ``None`` unless every payload is of one type the domain adds nothing
        to (:func:`_create_row_stamps`) and holds every field it requires, for the caller to
        build the domain models.
    """

    create = type(payloads[0])
    plan = _create_row_stamps(domain, create)

    if plan is None or _VERDICTS.get((domain, create)) is False:
        return None

    stamps, required = plan

    # A payload built with ``model_construct`` can lack a required field the domain refuses.
    if any(type(p) is not create or not required <= p.model_fields_set for p in payloads):
        return None

    fixed = {name: now if value is _NOW else value for name, value in stamps}
    rows = pydantic_dump_many(payloads, mode="python")

    for row, pk in zip(rows, ids, strict=True):
        row.update(fixed)
        row[ID_FIELD] = pk

    if (domain, create) not in _VERDICTS and not _confirmed(
        domain, create, payloads[0], ids[0], rows[0], now
    ):
        return None

    return rows


_VERDICTS: dict[tuple[type[BaseModel], type[BaseModel]], bool] = {}
"""Per (domain, create) pair, whether its first row matched the domain path's (:func:`_confirmed`):
checked once per process, and a mismatch keeps the pair on the domain path."""


def _confirmed(
    domain: type[BaseModel],
    create: type[BaseModel],
    payload: BaseModel,
    pk: UUID,
    row: JsonDict,
    now: datetime,
) -> bool:
    """Whether *row*, dumped from *payload*, is the mapping the domain path makes of it: the
    first time a pair is dumped in a process, a check the rule cannot fail to make. A mismatch
    is logged and keeps the pair on the domain path; a payload the domain refuses decides
    nothing, and is left to the domain path to refuse."""

    codec = model_codec_for(domain)

    try:
        with bind_time_source(FrozenTimeSource(instant=now)):
            models = domains_from_create_payloads(codec, [payload], [pk])
            (expected,) = codec.encode_persistence_mapping_many(models)

    except Exception:
        return False

    differing = sorted(
        k for k in expected.keys() | row.keys() if not _same_value(expected.get(k), row.get(k))
    )
    _VERDICTS[(domain, create)] = not differing

    if differing:
        logger.warning(
            "Set-based rows for %s from %s differ from the domain path's in %s; that pair "
            "builds its domain models from now on",
            domain.__qualname__,
            create.__qualname__,
            differing,
        )

    return not differing


def _same_value(a: Any, b: Any) -> bool:
    """Whether two dumped values are the same, types included, a NaN being itself."""

    if type(a) is not type(b):
        return False

    if isinstance(a, dict):
        return a.keys() == b.keys() and all(_same_value(a[k], b[k]) for k in a)

    if isinstance(a, list | tuple):
        return len(a) == len(b) and all(_same_value(x, y) for x, y in zip(a, b, strict=True))

    if isinstance(a, float) and a != a:
        return b != b

    if isinstance(a, Decimal) and a.is_nan():
        return b.is_nan()

    return bool(a == b)


_NOW = object()
"""Stands for the write's instant in :func:`_create_row_stamps`."""

_PLAIN_FACTORIES: tuple[Any, ...] = (list, dict, set, frozenset, tuple)
"""Default factories that build the same value whenever they are called."""

_JSON_ONLY = frozenset({"json", "json-unless-none"})
"""When a serializer runs for JSON output only."""

_FIELD_KEYS = frozenset({"type", "schema", "metadata", "frozen"})
"""What a model field may declare besides its schema: no alias, exclusion or the like."""

_RENAMING_KEYS = frozenset(
    {
        "validation_alias",
        "serialization_alias",
        "serialization_exclude",
        "serialization_exclude_if",
        "discriminator",
    }
)
"""What, on a field anywhere down a schema, makes a dump not validate back to the same value:
an alias (the dump names the field, validation looks for the alias), an exclusion (the dump
drops it), or a union's discriminator (an unset tag is dropped from the dump the domain path
validates)."""

_UNSAFE_TYPES = frozenset({"json", "tagged-union"})
"""Schemas whose Python dump does not validate back: ``Json[T]`` dumps the value it parsed, a
tagged union needs its tag."""


def _plain_schema(node: Any) -> bool:
    """Whether a core-schema *node* validates and dumps a value without running code of its own:
    no validator, serializer, post-init, custom ``__init__`` or ``_missing_`` hook, and no
    default factory but an empty container's. Data only, otherwise; a class is allowed where a
    schema names the model or enum it builds."""

    if isinstance(node, dict):
        if node.get("type") in _UNSAFE_TYPES or _RENAMING_KEYS & node.keys():
            return False

        for key, value in node.items():
            if key in ("metadata", "ref", "cls"):
                continue

            if key == "default_factory":
                if value not in _PLAIN_FACTORIES or node.get("default_factory_takes_data"):
                    return False

                continue

            # A serializer for JSON alone never runs on the Python dump a row is.
            if (
                key == "serialization"
                and isinstance(value, dict)
                and value.get("when_used") in _JSON_ONLY
            ):
                continue

            if key in ("function", "serialization", "post_init", "custom_init", "missing"):
                if value:
                    return False

                continue

            if not _plain_schema(value):
                return False

        return True

    if isinstance(node, list | tuple):
        return all(_plain_schema(v) for v in node)

    return node is None or isinstance(node, (*_SCHEMA_DATA, UUID, Pattern))


def _same_schema(a: Any, b: Any) -> bool:
    """Whether two core-schema nodes validate and dump alike: equal, but for ``metadata``,
    which holds JSON-schema hooks pydantic builds afresh for every model."""

    if isinstance(a, dict) and isinstance(b, dict):
        if type(a) is not type(b):
            return False

        keys = set(a) - {"metadata"}

        return keys == set(b) - {"metadata"} and all(_same_schema(a[k], b[k]) for k in keys)

    if type(a) is not type(b):
        # ``1``, ``1.0`` and ``True`` are equal, and ``set()`` and ``frozenset()``, but a
        # default of one is not dumped as the other.
        return False

    if isinstance(a, list | tuple):
        return len(a) == len(b) and all(_same_schema(x, y) for x, y in zip(a, b, strict=True))

    if isinstance(a, set | frozenset):
        return {(type(x), x) for x in a} == {(type(x), x) for x in b}

    if callable(a):
        return a is b

    return bool(a == b)


def _model_schema(model: type[BaseModel]) -> tuple[JsonDict, JsonDict, JsonDict] | None:
    """*model*'s ``model`` schema, its ``model-fields`` schema and its definitions by ref, when
    it is a plain model (nothing wraps or replaces its fields); ``None`` otherwise."""

    schema: Any = model.__pydantic_core_schema__
    definitions: JsonDict = {}

    if schema.get("type") == "definitions":
        definitions = {d["ref"]: d for d in schema["definitions"]}
        schema = schema["schema"]

        if schema.get("type") == "definition-ref":
            schema = definitions.get(schema["schema_ref"], {})

    if schema.get("type") != "model" or any(
        schema.get(k) for k in ("serialization", "post_init", "custom_init", "root_model")
    ):
        return None

    fields = schema.get("schema", {})

    if (
        fields.get("type") != "model-fields"
        or fields.get("computed_fields")
        or fields.get("extras_schema")
        or fields.get("extra_behavior") == "allow"
        or schema.get("config", {}).get("extra_fields_behavior") == "allow"
    ):
        return None

    return schema, fields, definitions


@functools.cache
def _bare_document_fields() -> JsonDict:
    """``Document``'s own fields' schemas, as a subclass declaring nothing has them."""

    class _Bare(Document):
        pass

    shape = _model_schema(_Bare)

    return {} if shape is None else shape[1]["fields"]


@functools.cache
def _create_row_stamps(
    domain: type[BaseModel], create: type[BaseModel]
) -> tuple[tuple[tuple[str, Any], ...], frozenset[str]] | None:
    """What :func:`rows_from_create_payloads` sets on a *create* payload's dump to make the
    *domain*'s mapping (the id aside, set per row), with the fields a payload must hold:
    ``None`` unless a *create* payload's dump, with the id and these, is the mapping the
    *domain* model validated from it dumps to.

    Decided from the two models' core schemas, once per pair, and refused (``None``) for
    anything not proven alike:

    - the domain is a :class:`~forze.domain.models.Document` whose id nothing reads during
      validation (no invariant, model validator, post-init or validator handed the data);
    - both are plain models: no model validator or serializer, computed field, custom
      ``__init__`` or extra fields kept, and the same core config;
    - the domain's fields are the create's plus ``Document``'s own bookkeeping fields, each
      create field validated and dumped by the very same schema in both;
    - and that schema, nested models and items included, runs no code and renames nothing: no
      validator, serializer, alias, exclusion, discriminated union or ``Json[T]``, and no
      default factory but an empty container, so validating the value it produced and dumping
      it again gives that value, and a default the payload took (compared with its type) is
      the one the domain would take.

    :func:`rows_from_create_payloads` adds two checks per call: every payload holds the fields
    its type requires (``model_construct`` can leave one out), and a pair's first row in a
    process is compared with the domain path's (:func:`_confirmed`).
    """

    if not issubclass(domain, Document) or not _reads_no_id(domain):
        return None

    domain_shape, create_shape = _model_schema(domain), _model_schema(create)

    if domain_shape is None or create_shape is None:
        return None

    (domain_model, domain_fields, domain_defs) = domain_shape
    (create_model, create_fields, create_defs) = create_shape
    bare = _bare_document_fields()

    def config(model: JsonDict) -> JsonDict:
        return {k: v for k, v in model.get("config", {}).items() if k != "title"}

    dfields, cfields = domain_fields["fields"], create_fields["fields"]

    if (
        config(domain_model) != config(create_model)
        or not _same_schema(domain_defs, create_defs)
        or not bare
        or set(dfields) != set(cfields) | set(bare)
        or set(cfields) & set(bare)
        or not all(_same_schema(dfields[name], bare[name]) for name in bare)
        or not all(_same_schema(dfields[name], field) for name, field in cfields.items())
        or any(set(field) - _FIELD_KEYS for field in cfields.values())
        or not all(_plain_schema(field) for field in cfields.values())
        or not all(_plain_schema(definition) for definition in create_defs.values())
    ):
        return None

    stamps: list[tuple[str, Any]] = []

    for name in bare:
        if name == ID_FIELD:
            continue

        default = bare[name]["schema"]

        if default.get("default_factory") is utcnow:
            stamps.append((name, _NOW))

        elif "default" in default and isinstance(default["default"], int):
            stamps.append((name, default["default"]))

        else:
            return None

    required = frozenset(name for name, field in create.model_fields.items() if field.is_required())

    return tuple(stamps), required


_SCHEMA_DATA: tuple[type, ...] = (str, bytes, int, float, Decimal, Enum, date, time, timedelta)
"""What a core schema holds as data (names, bounds, defaults, choices), beside ``None``,
``UUID``s, patterns and code."""


def _handed_data(node: Any) -> bool:
    """Whether a core-schema *node* runs code handed the model's data, or might: a validator
    taking ``info``, a default factory taking the data, a post-init or a custom ``__init__``,
    or anything not recognized as data or code."""

    if isinstance(node, dict):
        if (
            node.get("type") == "with-info"
            or node.get("default_factory_takes_data")
            or node.get("post_init")
            or node.get("custom_init")
        ):
            return True

        return any(_handed_data(v) for v in node.values())

    if isinstance(node, list | tuple):
        return any(_handed_data(v) for v in node)

    # Code runs as the node holding it says, which is read above.
    return not (node is None or isinstance(node, (*_SCHEMA_DATA, UUID, Pattern)) or callable(node))


@functools.cache
def _reads_no_id(domain: type[BaseModel]) -> bool:
    """Whether *domain* validated with its ``id`` is the model a copy stamps afterwards: the id
    a plain ``UUID`` field under its own name (no alias of any kind) that no validator looks
    at, and nothing else run during validation able to read it (an invariant, a model
    validator or post-init of its own, a validator or default factory handed the data)."""

    from .specs import _own_decorators  # pyright: ignore[reportPrivateUsage]

    if not issubclass(domain, Document):
        return False

    field = domain.model_fields.get(ID_FIELD)
    decorators = domain.__pydantic_decorators__
    validated = {n for d in decorators.field_validators.values() for n in d.info.fields} | {
        n for d in decorators.validators.values() for n in d.info.fields
    }

    return (
        field is not None
        # Validation drops an ``id`` the field does not take under that name.
        and field.alias is None
        and field.validation_alias is None
        and field.serialization_alias is None
        and domain.model_config.get("alias_generator") is None
        # The copy never validates the caller's id; a validator of it, or its type's, would.
        and field.annotation is UUID
        and not field.metadata
        and not validated & {ID_FIELD, "*"}
        and not domain._invariants_  # pyright: ignore[reportPrivateUsage]
        and not _own_decorators(domain, "model_validators")
        and not _own_decorators(domain, "root_validators")
        and not _handed_data(domain.__pydantic_core_schema__)
    )
