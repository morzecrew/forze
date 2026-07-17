"""Base gateway for Meilisearch search adapters."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from functools import cache
from types import UnionType
from typing import TYPE_CHECKING, Annotated, Any, Union, cast, get_args, get_origin
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.querying import (
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QueryFilterLimits,
)
from forze.application.contracts.resolution import (
    is_static_named_resource,
    resolve_scoped_namespace,
)
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.application.contracts.tenancy.mixins import TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import OnceCell
from forze.domain.constants import ID_FIELD
from forze_meilisearch.kernel.relation import resolve_meilisearch_index_uid

from ._filter_render import (
    MeilisearchFilterRenderer,
    _datetime_text,  # pyright: ignore[reportPrivateUsage]
    format_literal,
    safe_attribute,
)

if TYPE_CHECKING:
    from forze_meilisearch.execution.deps.configs import MeilisearchSearchConfig

# ----------------------- #

_CONTAINER_ORIGINS = (list, set, frozenset, tuple, dict, Mapping)

# Leaf types whose json-mode dump representation is rewritten before indexing: a Decimal
# becomes a JSON number, an aware datetime normalizes to the UTC-``Z`` form the filter
# literals render (see ``_canonicalize_leaves``).
_CANONICAL_LEAF_TYPES = (Decimal, datetime)

# ....................... #


def _ann_may_hold_canonical_leaf(ann: Any, seen: set[type]) -> bool:
    """Whether *ann* can reach a canonicalized leaf (conservative: unknowable → ``True``)."""

    if ann is Any or ann is None:
        return True

    origin = get_origin(ann)

    if origin is Annotated:
        return _ann_may_hold_canonical_leaf(get_args(ann)[0], seen)

    if origin is Union or origin is UnionType:
        return any(_ann_may_hold_canonical_leaf(a, seen) for a in get_args(ann))

    if origin in _CONTAINER_ORIGINS:
        args = [a for a in get_args(ann) if a is not Ellipsis]

        return any(_ann_may_hold_canonical_leaf(a, seen) for a in args) if args else True

    if origin is not None:
        return True

    if not isinstance(ann, type):
        return True

    if issubclass(ann, _CANONICAL_LEAF_TYPES):
        return True

    if issubclass(ann, Enum):
        # A plain (non-mixin) enum dumps as its *value* — canonical only when a member
        # holds a canonical leaf (e.g. a Decimal-valued enum).
        return any(isinstance(m.value, _CANONICAL_LEAF_TYPES) for m in ann)

    if issubclass(ann, BaseModel):
        if ann in seen:
            return False
        seen.add(ann)
        field_anns = [f.annotation for f in ann.model_fields.values()]
        field_anns += [c.return_type for c in ann.model_computed_fields.values()]
        return any(_ann_may_hold_canonical_leaf(a, seen) for a in field_anns)

    return False


# ....................... #


@cache
def _model_may_hold_canonical_leaf(model_cls: type[BaseModel]) -> bool:
    return _ann_may_hold_canonical_leaf(model_cls, set())


# ....................... #


def _decimal_number(value: Decimal) -> float | None:
    """The Decimal as a finite JSON number, or ``None`` when f64 cannot represent it.

    An explicit ``NaN``/``Infinity`` and a finite value whose magnitude overflows f64
    (``float(Decimal("1e1000"))`` is ``inf``) are not valid JSON numbers — indexing one
    would fail the whole upsert, so such a value keeps its string form instead.
    """

    if not value.is_finite():
        return None

    converted = float(value)

    return converted if math.isfinite(converted) else None


def _pydantic_json_datetime(value: datetime) -> str:
    """The string a json-mode pydantic dump emits for *value* (``Z`` for UTC offsets)."""

    return value.isoformat().replace("+00:00", "Z")


_UNMATCHABLE = object()


def _pydantic_json_twin(value: Any) -> Any:
    """Best-effort mirror of the json-mode dump of a hashable set member.

    Used only to *pair* a set member with the dumped element it serialized to;
    ``_UNMATCHABLE`` marks types the mirror cannot reproduce (model instances, custom
    scalars) — those members keep their dumped form.
    """

    if value is None or isinstance(value, bool):
        return value

    if isinstance(value, Enum):
        return _pydantic_json_twin(value.value)

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, datetime):
        return _pydantic_json_datetime(value)

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, UUID):
        return str(value)

    if isinstance(value, (int, float, str)):
        return value

    if isinstance(value, (tuple, frozenset, set)):
        twins = [_pydantic_json_twin(x) for x in value]  # pyright: ignore[reportUnknownVariableType]

        return _UNMATCHABLE if any(t is _UNMATCHABLE for t in twins) else twins

    return _UNMATCHABLE


def _match_key(twin: Any) -> str | None:
    """Hashable pairing key for a json-form value, or ``None`` when it has none."""

    if twin is _UNMATCHABLE:
        return None

    try:
        return json.dumps(twin, sort_keys=True)
    except (TypeError, ValueError):
        return None


def _canonicalize_unordered(source: set[Any] | frozenset[Any], dumped: list[Any]) -> list[Any]:
    """Canonicalize a set's dump by *value* — set iteration order cannot pair positionally.

    Each member claims (at most once) the dumped element it serialized to, keyed by its
    json-form twin, and the matched pair recurses through the ordinary walk — so compound
    members (a tuple holding a Decimal, a nested frozenset) canonicalize their inner
    leaves too. Unmatched elements, and members the twin cannot mirror, stay as dumped.
    """

    buckets: dict[str, list[Any]] = {}

    for member in source:
        key = _match_key(_pydantic_json_twin(member))

        if key is not None:
            buckets.setdefault(key, []).append(member)

    out: list[Any] = []

    for item in dumped:
        key = _match_key(item)
        queue = buckets.get(key) if key is not None else None

        out.append(_canonicalize_leaves(queue.pop(), item) if queue else item)

    return out


def _canonicalize_leaves(source: Any, dumped: Any) -> Any:
    """Mirror-walk a python-mode dump against its json-mode twin, rewriting the leaves
    whose stringified form Meilisearch cannot compare.

    A ``Decimal`` becomes a JSON number (numeric filters and sorts; a string would
    range-filter to nothing and sort lexically) unless f64 cannot represent it; an aware
    ``datetime`` normalizes to the UTC-``Z`` text the filter literals render, so a
    non-UTC-offset timestamp still matches its ``$eq``/range operand. Driven by the live
    values, so a ``str`` field that merely *looks* numeric or temporal is never touched.
    Any structural mismatch keeps the json-mode value as-is.
    """

    if isinstance(source, Enum):
        # A plain Enum's json form is its *value* (a Decimal-valued enum dumps as the
        # decimal's string); canonicalize through the value, mirroring format_literal.
        return _canonicalize_leaves(source.value, dumped)

    if isinstance(source, Decimal):
        number = _decimal_number(source)

        return dumped if number is None else number

    if isinstance(source, datetime):
        return _datetime_text(source)

    if isinstance(source, dict) and isinstance(dumped, dict):
        source_map = cast(dict[Any, Any], source)  # type: ignore[redundant-cast]
        dumped_map = cast(dict[Any, Any], dumped)  # type: ignore[redundant-cast]

        if len(source_map) != len(dumped_map):
            # Keys collided under json stringification (e.g. 1 alongside "1") —
            # pairing would be a guess, so keep the dumped values.
            return dumped  # pyright: ignore[reportUnknownVariableType]

        # The json-mode dump stringifies mapping keys (``1`` → ``"1"``) but preserves
        # the same insertion order as the python-mode dump of the same mapping, so pair
        # entries positionally, guarded per-entry by key correspondence.
        out: dict[Any, Any] = {}

        for (sk, sv), (dk, dv) in zip(source_map.items(), dumped_map.items(), strict=True):
            corresponds = sk == dk or (isinstance(dk, str) and str(sk) == dk)
            out[dk] = _canonicalize_leaves(sv, dv) if corresponds else dv

        return out

    if isinstance(source, (set, frozenset)) and isinstance(dumped, list):
        return _canonicalize_unordered(source, dumped)  # pyright: ignore[reportUnknownArgumentType]

    if isinstance(source, (list, tuple)) and isinstance(dumped, list):
        items = cast(list[Any], list(source))  # type: ignore[redundant-cast]
        dumped_items = cast(list[Any], dumped)  # type: ignore[redundant-cast]

        if len(items) != len(dumped_items):
            return dumped  # pyright: ignore[reportUnknownVariableType]

        return [_canonicalize_leaves(s, d) for s, d in zip(items, dumped_items, strict=True)]

    return dumped


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MeilisearchSearchGateway[M: BaseModel](TenancyMixin):
    """Shared index mapping and filter rendering for Meilisearch search."""

    spec: SearchSpec[M]
    """Logical search specification."""

    config: MeilisearchSearchConfig
    """Physical Meilisearch mapping."""

    # ....................... #

    filter_parser: QueryFilterExpressionParser = attrs.field(
        factory=lambda: QueryFilterExpressionParser(limits=QueryFilterLimits()),
        init=False,
    )

    _index_uid_cell: OnceCell[str] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    # The logical->physical field map (and its inverse) derive from a frozen config, so
    # resolve them once instead of rebuilding the dict on every ``physical_path`` (per
    # indexed field) and every ``from_hit`` (per search hit). Private and read-only — the
    # public ``field_map`` property still returns a fresh copy.
    _field_map_cache: dict[str, str] = attrs.field(
        default=attrs.Factory(
            lambda self: dict(self.config.field_map or {}),
            takes_self=True,
        ),
        init=False,
        eq=False,
        repr=False,
    )

    _inv_field_map_cache: dict[str, str] = attrs.field(
        default=attrs.Factory(
            lambda self: {
                v: k
                for k, v in (  # pyright: ignore[reportUnknownVariableType]
                    self.config.field_map or {}
                ).items()
            },
            takes_self=True,
        ),
        init=False,
        eq=False,
        repr=False,
    )

    # ....................... #

    async def _resolved_index_uid(self) -> str:
        return await resolve_scoped_namespace(
            self.config.index_uid,
            tenant_id=self._tenant_id_for_resolve(),
            cell=self._index_uid_cell,
            resolver=resolve_meilisearch_index_uid,
        )

    # ....................... #

    @property
    def index_uid(self) -> str:
        """Best-effort sync access when config ``index_uid`` is static."""

        spec = self.config.index_uid

        if is_static_named_resource(spec):
            return spec

        resolved = self._index_uid_cell.peek()

        if resolved is not None:
            return resolved

        raise exc.internal(
            "index_uid is only available for static index UIDs; await _resolved_index_uid()",
        )

    @property
    def primary_key(self) -> str:
        return self.config.primary_key

    @property
    def field_map(self) -> dict[str, str]:
        return dict(self.config.field_map or {})

    @property
    def filter_renderer(self) -> MeilisearchFilterRenderer:
        return MeilisearchFilterRenderer(field_map=self.field_map)

    # ....................... #

    def physical_path(self, field: str) -> str:
        return self._field_map_cache.get(field, field)

    def physical_paths(self, fields: Sequence[str]) -> list[str]:
        return [self.physical_path(f) for f in fields]

    # ....................... #

    def build_filter(
        self,
        filters: QueryFilterExpression | None,
    ) -> str | None:
        from forze_meilisearch.adapters.search._search_params import (
            merge_filter_strings,
        )

        base = self.filter_renderer.render_filters(filters)
        tenant = self._tenant_filter()
        return merge_filter_strings(base, tenant)

    # ....................... #

    def _tenant_filter(self) -> str | None:
        if not self.tenant_aware:
            return None

        tenant_id = self.require_tenant_if_aware()

        if tenant_id is None:
            return None

        attr = safe_attribute(self.physical_path(TENANT_ID_FIELD))
        return f"{attr} = {format_literal(tenant_id)}"

    # ....................... #

    @property
    def _encrypts(self) -> bool:
        # The factory wraps the read codec with an EncryptingModelCodec (which exposes
        # ``prepare_encrypt``) when the spec declares an ``encryption`` policy.
        return hasattr(self.spec.resolved_read_codec, "prepare_encrypt")

    async def prepare_encrypt(self) -> None:
        """Warm the keyring before a synchronous encrypting encode (no-op if plaintext)."""

        prepare = getattr(self.spec.resolved_read_codec, "prepare_encrypt", None)
        if prepare is not None:
            await prepare()

    def to_index_document(self, model: M) -> dict[str, Any]:
        codec = self.spec.resolved_read_codec
        # Encrypting routes must go through the persistence encode to seal the configured
        # fields (``encode_mapping`` is the plaintext passthrough). But that path defaults to
        # excluding pydantic ``@computed_field`` values, which the plain index path keeps —
        # so re-enable them here to index the same field set, just with the encrypted ones
        # sealed. Plain routes use ``encode_mapping`` directly (computed fields already in).
        #
        # ``mode="json"`` on both, because Meilisearch is a JSON-over-HTTP store and this is
        # the boundary: the SDK hands the mapping straight to ``json.dumps``, which cannot
        # serialize the Python objects the default ``mode="python"`` preserves. A ``UUID`` id
        # and ``datetime`` timestamps are exactly what a standard ``ReadDocument`` carries —
        # i.e. what the index-sync path feeds this on every committed write — so the plain
        # encode raised ``TypeError`` for the framework's own document read model.
        data = (
            codec.encode_persistence_mapping(model, mode="json", exclude={"computed_fields": False})
            if self._encrypts
            else codec.encode_mapping(model, mode="json")
        )
        data = self._canonicalize_index_values(model, data)
        out: dict[str, Any] = {}

        for key, value in data.items():
            phys = self.physical_path(key)
            out[phys] = value

        pk = self.primary_key
        pk_val = out.get(pk, data.get(ID_FIELD, data.get("id")))

        if pk_val is not None:
            out[pk] = pk_val

        # Tagged tenancy: stamp the tenant discriminator so tenant-filtered reads and
        # tenant-scoped deletes can isolate this document (a shared index otherwise
        # mixes every tenant's rows). Fails closed if tenant-aware but no tenant bound.
        if self.tenant_aware:
            tenant_id = self.require_tenant_if_aware()
            out[self.physical_path(TENANT_ID_FIELD)] = str(tenant_id)

        return out

    # ....................... #

    def _canonicalize_index_values(self, model: M, data: dict[str, Any]) -> dict[str, Any]:
        """Rewrite dump leaves the filter literals must be able to match (see
        ``_canonicalize_leaves``): Decimal → JSON number, aware datetime → UTC-``Z``.

        Guided by the live model's values via a second, python-mode dump, skipped entirely
        for models that cannot hold such a leaf. Sealed (field-encrypted) roots are left
        untouched — their dumped value is a ciphertext envelope, not the plaintext scalar.
        """

        if not _model_may_hold_canonical_leaf(type(model)):
            return data

        codec = self.spec.resolved_read_codec
        plain = codec.encode_mapping(
            model,
            mode="python",
            exclude={"computed_fields": False} if self._encrypts else None,
        )

        encryption = self.spec.encryption
        sealed_roots: frozenset[str] = (
            frozenset()
            if encryption is None
            else frozenset(
                f.split(".", 1)[0] for f in (encryption.encrypted | encryption.searchable)
            )
        )

        return {
            k: v if k in sealed_roots else _canonicalize_leaves(plain.get(k), v)
            for k, v in data.items()
        }

    # ....................... #

    def from_hit(self, hit: dict[str, Any]) -> dict[str, Any]:
        inv = self._inv_field_map_cache
        out: dict[str, Any] = {}

        for key, value in hit.items():
            if key.startswith("_"):
                continue

            logical = inv.get(key, key)
            out[logical] = value

        return out
