"""Mongo search dep factory."""

from typing import Any, final

import attrs

from forze.application.contracts.embeddings import EmbeddingsSpec
from forze.application.contracts.search import (
    SearchQueryDepPort,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.contracts.crypto import (
    DeterministicCipherDepKey,
    KeyringDepKey,
)
from forze.application.contracts.search.ports import SearchQueryPort
from forze.application.execution import ExecutionContext
from forze.application.integrations.search import (
    SearchResultSnapshot,
    resolve_search_read_codec_spec,
    resolve_snapshot_cipher,
    search_spec_encrypts,
)
from forze.base.exceptions import exc

from ....adapters import (
    MongoAtlasSearchAdapter,
    MongoTextSearchAdapter,
    MongoVectorSearchAdapter,
)
from ..configs import MongoSearchConfig
from ..keys import MongoClientDepKey

# ----------------------- #


def _resolve_result_snapshot(
    context: ExecutionContext,
    spec: SearchResultSnapshotSpec | None,
) -> Any:
    if spec is None:
        return None

    if not (
        context.deps.exists(SearchResultSnapshotDepKey, route=spec.name)
        or context.deps.exists(SearchResultSnapshotDepKey)
    ):
        return None

    return context.deps.provide(SearchResultSnapshotDepKey, route=spec.name)(
        context,
        spec,
    )


# ....................... #


def _result_snapshot(
    context: ExecutionContext,
    spec: SearchResultSnapshotSpec | None,
    *,
    encrypted: bool = False,
) -> SearchResultSnapshot | None:
    port = _resolve_result_snapshot(context, spec)

    if port is None:
        return None

    cipher = resolve_snapshot_cipher(
        encrypted=encrypted,
        keyring=(
            context.deps.provide(KeyringDepKey)
            if context.deps.exists(KeyringDepKey)
            else None
        ),
    )

    return SearchResultSnapshot(
        store=port, cipher=cipher, cipher_tenant=context.inv_ctx.get_tenant
    )


# ....................... #


def _mongo_search_port_for_config(
    context: ExecutionContext,
    member_spec: SearchSpec[Any],
    c: MongoSearchConfig,
) -> (
    MongoTextSearchAdapter[Any]
    | MongoAtlasSearchAdapter[Any]
    | MongoVectorSearchAdapter[Any]
):
    c.validate_against_spec(member_spec)

    # Decrypt encrypted document fields out of in-place search results (the collection was
    # written encrypted by the document gateway; the wrapped codec reproduces its config).
    member_spec = resolve_search_read_codec_spec(
        member_spec,
        keyring=(
            context.deps.provide(KeyringDepKey)
            if context.deps.exists(KeyringDepKey)
            else None
        ),
        deterministic=(
            context.deps.provide(DeterministicCipherDepKey)
            if context.deps.exists(DeterministicCipherDepKey)
            else None
        ),
        tenant_provider=context.inv_ctx.get_tenant,
    )

    field_map = dict(c.field_map or {})
    result_snapshot = _result_snapshot(
        context, member_spec.snapshot, encrypted=search_spec_encrypts(member_spec)
    )
    client = context.deps.provide(MongoClientDepKey)
    tenant_aware = c.tenant_aware

    match c.engine:
        case "text":
            return MongoTextSearchAdapter(
                spec=member_spec,
                codec=member_spec.resolved_read_codec,
                model_type=member_spec.model_type,
                lenient_read_fields=member_spec.resolved_lenient_read_fields,
                relation=c.read,
                client=client,
                field_map=field_map,
                tenant_provider=context.inv_ctx.get_tenant,
                tenant_aware=tenant_aware,
                result_snapshot=result_snapshot,
            )

        case "atlas":
            index_name = c.index_name

            if index_name is None:
                raise exc.configuration("index_name is required for atlas engine.")

            return MongoAtlasSearchAdapter(
                spec=member_spec,
                codec=member_spec.resolved_read_codec,
                model_type=member_spec.model_type,
                lenient_read_fields=member_spec.resolved_lenient_read_fields,
                relation=c.read,
                client=client,
                field_map=field_map,
                tenant_provider=context.inv_ctx.get_tenant,
                tenant_aware=tenant_aware,
                result_snapshot=result_snapshot,
                index_name=index_name,
            )

        case "vector":
            en = c.embeddings_name
            ed = c.embedding_dimensions
            vpath = c.vector_path
            index_name = c.index_name

            if en is None or ed is None or vpath is None or index_name is None:
                raise exc.configuration(
                    "vector engine requires embeddings_name, embedding_dimensions, "
                    "vector_path, and index_name.",
                )

            es = EmbeddingsSpec(name=en, dimensions=ed)

            return MongoVectorSearchAdapter(
                spec=member_spec,
                codec=member_spec.resolved_read_codec,
                model_type=member_spec.model_type,
                lenient_read_fields=member_spec.resolved_lenient_read_fields,
                relation=c.read,
                client=client,
                field_map=field_map,
                tenant_provider=context.inv_ctx.get_tenant,
                tenant_aware=tenant_aware,
                result_snapshot=result_snapshot,
                embedder=context.embeddings.provider(es),
                embedding_dimensions=ed,
                vector_path=vpath,
                index_name=index_name,
            )

        case _:  # pyright: ignore[reportUnnecessaryComparison]
            raise exc.configuration(f"Unsupported Mongo search engine: {c.engine!r}.")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMongoSearch(SearchQueryDepPort):
    """Configurable Mongo search adapter factory."""

    config: MongoSearchConfig = attrs.field(
        validator=attrs.validators.instance_of(MongoSearchConfig),
    )
    """Mongo-specific search configuration."""

    # ....................... #

    def __call__(
        self,
        context: ExecutionContext,
        spec: SearchSpec[Any],
    ) -> SearchQueryPort[Any]:
        return _mongo_search_port_for_config(context, spec, self.config)
