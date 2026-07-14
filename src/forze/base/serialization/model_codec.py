"""Model codec protocol: the serialization seam for record models.

A ``ModelCodec`` is the single place that owns how a record model becomes a
storage/wire mapping (and JSON bytes) and back: dump modes, batched and
trusted-decode fast paths, materialized (computed) fields, and the
persistence-mapping path. Call sites — document cache, the persistence gateway,
outbox staging, query scans — go through this protocol instead of each
re-deriving serialization options, and it is the decoration seam that
``EncryptingModelCodec`` wraps to add field-level encryption transparently.

Pydantic is the model family for record contracts; ``PydanticModelCodec`` (over
``forze.base.serialization.pydantic``) is the implementation. The protocol earns
its keep as the wrapping/single-source seam — not as a switch between model
libraries.
"""

from collections.abc import Iterator, Sequence
from typing import Literal, Protocol, TypedDict

from ..primitives import JsonDict

# ----------------------- #

EncodeMode = Literal["json", "python"]
"""Mode for encoding record mappings."""

# ....................... #


class ModelDumpExcludeOptions(TypedDict, total=False):
    """Options controlling which fields to exclude from model dumps."""

    unset: bool
    """Exclude fields that were never explicitly set."""

    none: bool
    """Exclude fields whose value is ``None``."""

    defaults: bool
    """Exclude fields still equal to their default value."""

    computed_fields: bool
    """Exclude computed (derived) fields."""


# ....................... #


class ModelCodec[T, TSource](Protocol):
    """Codec protocol for mapping-based model serialization and transforms."""

    @property
    def model_type(self) -> type[T]: ...

    def decode_mapping(
        self,
        data: JsonDict,
        *,
        forbid_extra: bool = False,
        trust_source: bool = False,
    ) -> T: ...

    def decode_mapping_many(
        self,
        data: Sequence[JsonDict],
        *,
        forbid_extra: bool = False,
        trust_source: bool = False,
    ) -> list[T]: ...

    def decode_mapping_many_batched(
        self,
        data: Sequence[JsonDict],
        *,
        batch_size: int = 2000,
        forbid_extra: bool = False,
        trust_source: bool = False,
    ) -> Iterator[list[T]]: ...

    def encode_mapping(
        self,
        obj: T,
        *,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions | None = None,
    ) -> JsonDict:
        """Encode *obj* to a property map. **Pick `mode` from what will consume the map.**

        The default ``"python"`` keeps a ``UUID``, a ``datetime`` and a ``Decimal`` as Python
        objects. The return type says ``JsonDict``, and in that mode it is not one — the
        annotation describes the *shape*, not the values, and reading it as a promise of
        JSON-safety is how this goes wrong. There are exactly two kinds of consumer, and the
        wrong choice fails loudly on one of them and lossily on the other:

        - **A driver that binds values natively** — psycopg, PyMongo, clickhouse-connect, the
          Neo4j driver — wants ``"python"``. It maps those objects straight onto a ``uuid`` /
          ``timestamptz`` / ``Decimal`` column. Handing it strings pushes every value back
          through the column's own parser for nothing.
        - **Anything that serializes the map to JSON** — an HTTP API (Meilisearch, BigQuery's
          ``insertAll``, Inngest), a ``JSONB`` parameter (psycopg wraps it in ``Jsonb``, which
          calls ``json.dumps``), a broker envelope — needs ``"json"``. ``json.dumps`` cannot
          encode any of those three types, so the default mode raises ``TypeError`` before the
          value ever leaves the process.

        Every one of those JSON consumers has been caught getting this wrong at least once, and
        the failure is invisible in tests: a payload model of ``str`` and ``int`` fields never
        asks the serializer to encode anything it cannot. Cover a new adapter with a model that
        carries a ``UUID``, a ``datetime`` and a ``Decimal``, against the real backend.
        """
        ...

    def encode_mapping_many(
        self,
        objs: Sequence[T],
        *,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions | None = None,
    ) -> list[JsonDict]: ...

    def encode_mapping_many_batched(
        self,
        objs: Sequence[T],
        *,
        batch_size: int = 2000,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions | None = None,
    ) -> Iterator[list[JsonDict]]: ...

    def transform(
        self,
        source: TSource,
        *,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions | None = None,
    ) -> T: ...

    def transform_many(
        self,
        sources: Sequence[TSource],
        *,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions | None = None,
    ) -> list[T]: ...

    @property
    def materialized(self) -> frozenset[str]:
        """Computed field names opted into persistence (and thus query).

        Empty for codecs without materialized derived fields. Members are
        ``@computed_field`` names that are written to storage by
        :meth:`encode_persistence_mapping` and reported by
        :meth:`persisted_field_names`, so they can be filtered/sorted on.
        """
        ...

    def stored_field_names(
        self,
        *,
        include_computed: bool = True,
    ) -> frozenset[str]: ...

    def persisted_field_names(self) -> frozenset[str]:
        """Field names actually written to storage: declared fields + materialized.

        The single source of truth for "what is persisted" (and therefore
        queryable). Equals the declared (non-computed) field set unless the
        codec carries :attr:`materialized` names.
        """
        ...

    def encode_json_bytes(
        self,
        obj: T,
        *,
        exclude: ModelDumpExcludeOptions | None = None,
    ) -> bytes: ...

    def encode_persistence_mapping(
        self,
        obj: T,
        *,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions | None = None,
    ) -> JsonDict: ...

    def encode_persistence_mapping_many(
        self,
        objs: Sequence[T],
        *,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions | None = None,
    ) -> list[JsonDict]: ...

    def decode_json_bytes(
        self,
        raw: bytes | str,
        *,
        forbid_extra: bool = False,
        encoding: str = "utf-8",
    ) -> T: ...
