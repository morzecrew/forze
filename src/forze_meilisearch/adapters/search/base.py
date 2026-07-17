"""Base gateway for Meilisearch search adapters."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from functools import cache
from types import UnionType
from typing import Annotated, Any, Union, cast, get_args, get_origin

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
from forze_meilisearch.adapters.search._filter_render import (
    MeilisearchFilterRenderer,
    format_literal,
    safe_attribute,
)
from forze_meilisearch.execution.deps.configs import MeilisearchSearchConfig
from forze_meilisearch.kernel.relation import resolve_meilisearch_index_uid

# ----------------------- #

_CONTAINER_ORIGINS = (list, set, frozenset, tuple, dict, Mapping)

# ....................... #


def _ann_may_hold_decimal(ann: Any, seen: set[type]) -> bool:
    """Whether *ann* can reach a ``Decimal`` value (conservative: unknowable → ``True``)."""

    if ann is Any or ann is None:
        return True

    origin = get_origin(ann)

    if origin is Annotated:
        return _ann_may_hold_decimal(get_args(ann)[0], seen)

    if origin is Union or origin is UnionType:
        return any(_ann_may_hold_decimal(a, seen) for a in get_args(ann))

    if origin in _CONTAINER_ORIGINS:
        args = [a for a in get_args(ann) if a is not Ellipsis]

        return any(_ann_may_hold_decimal(a, seen) for a in args) if args else True

    if origin is not None:
        return True

    if not isinstance(ann, type):
        return True

    if issubclass(ann, Decimal):
        return True

    if issubclass(ann, BaseModel):
        if ann in seen:
            return False
        seen.add(ann)
        field_anns = [f.annotation for f in ann.model_fields.values()]
        field_anns += [c.return_type for c in ann.model_computed_fields.values()]
        return any(_ann_may_hold_decimal(a, seen) for a in field_anns)

    return False


# ....................... #


@cache
def _model_may_hold_decimal(model_cls: type[BaseModel]) -> bool:
    return _ann_may_hold_decimal(model_cls, set())


# ....................... #


def _decimals_to_numbers(source: Any, dumped: Any) -> Any:
    """Mirror-walk a python-mode dump against its json-mode twin, numbering the decimals.

    Wherever the python-mode value is a ``Decimal``, the json-mode dump stringified it —
    replace with ``float`` so Meilisearch indexes a number (numeric filters and sorts;
    a string would range-filter to nothing and sort lexically). Driven by the live values,
    so a ``str`` field that merely *looks* numeric is never touched. Any structural
    mismatch keeps the json-mode value as-is.
    """

    if isinstance(source, Decimal):
        return float(source)

    if isinstance(source, dict) and isinstance(dumped, dict):
        return {
            k: _decimals_to_numbers(source[k], v) if k in source else v
            for k, v in dumped.items()  # pyright: ignore[reportUnknownVariableType]
        }

    if isinstance(source, (list, tuple, set, frozenset)) and isinstance(dumped, list):
        items = cast(list[Any], list(source))  # type: ignore[redundant-cast]
        dumped = cast(list[Any], dumped)  # type: ignore[redundant-cast]
        dumped_items = dumped

        if len(items) != len(dumped_items):
            return dumped

        return [_decimals_to_numbers(s, d) for s, d in zip(items, dumped_items, strict=True)]

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
        data = self._number_decimal_fields(model, data)
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

    def _number_decimal_fields(self, model: M, data: dict[str, Any]) -> dict[str, Any]:
        """Re-index the json-mode dump's Decimal leaves as JSON numbers.

        ``mode="json"`` stringifies a ``Decimal``, and Meilisearch cannot range-filter or
        numerically sort a string — so Decimal-valued fields index as floats (bounded by
        f64 like every Meilisearch number; the document plane keeps the exact value).
        Guided by the live model's values via a second, python-mode dump, skipped entirely
        for models that cannot hold a Decimal. Sealed (field-encrypted) roots are left
        untouched — their dumped value is a ciphertext envelope, not the plaintext scalar.
        """

        if not _model_may_hold_decimal(type(model)):
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
            k: v if k in sealed_roots else _decimals_to_numbers(plain.get(k), v)
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
