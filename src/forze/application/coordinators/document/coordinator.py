"""Document coordinator orchestrating query/command ports over gateways."""

from functools import cached_property
from typing import Sequence
from uuid import UUID

import attrs

from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    DocumentSpec,
)
from forze.application.contracts.document.gateways import (
    DocumentReadGatewayPort,
    DocumentWriteGatewayPort,
)
from forze.application.contracts.querying import (
    QuerySortExpression,
    read_fields_for_model,
    resolve_effective_sorts,
)
from forze.base.exceptions import exc
from forze.base.serialization import (
    pydantic_persistence_dump,
    pydantic_persistence_dump_many,
    pydantic_validate,
    pydantic_validate_many,
)

from ..._logger import logger
from ..cache import DocumentCacheCoordinator
from ._command import DocumentCommandMixin
from ._query import DocumentQueryMixin
from ._types import C, D, R, U


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentCoordinator(
    DocumentQueryMixin[R],
    DocumentCommandMixin[R, D, C, U],
    DocumentQueryPort[R],
    DocumentCommandPort[R, D, C, U],
):
    """Orchestrate document query/command ports over pluggable persistence gateways.

    MRO places :class:`~._query.DocumentQueryMixin` before
    :class:`~._command.DocumentCommandMixin` so command helpers (e.g.
    :meth:`~._command.DocumentCommandMixin.update_matching_strict`) can call
    :meth:`~._query.DocumentQueryMixin.project_many`.
    """

    spec: DocumentSpec[R, D, C, U]
    read_gw: DocumentReadGatewayPort[R]
    write_gw: DocumentWriteGatewayPort[D, C, U] | None = attrs.field(default=None)
    cache_coord: DocumentCacheCoordinator[R]
    batch_size: int = 200
    enforce_primary_key_cursor_sort: bool = False
    hydrate_from_write: bool = False

    def __attrs_post_init__(self) -> None:
        """Check compatibility of cache coordinator with read gateway and specification."""

        if self.cache_coord.read_model_type is not self.read_gw.model_type:
            raise exc.configuration(
                "Document cache coordinator read model type mismatches read gateway model type."
            )

        if self.cache_coord.document_name != self.spec.name:
            raise exc.configuration(
                "Document cache coordinator name mismatches document specification name."
            )

    # ....................... #

    @cached_property
    def _read_fields(self) -> frozenset[str]:  # type: ignore[override]
        return read_fields_for_model(self.spec.read)

    # ....................... #

    def _resolve_sorts(
        self,
        sorts: QuerySortExpression | None,
    ) -> QuerySortExpression:
        return resolve_effective_sorts(
            sorts=sorts,
            default_sort=self.spec.default_sort,
            read_fields=self._read_fields,
            spec_name=self.spec.name,
        )

    # ....................... #

    @cached_property
    def eff_batch_size(self) -> int:  # type: ignore[override]
        if self.batch_size < 10:
            logger.warning("Batch size is too small, using default value of 200")

            return 200

        if self.batch_size > 20000:
            logger.warning("Batch size is too large, using default value of 200")

            return 200

        return self.batch_size

    # ....................... #

    def _eff_stream_chunk_size(self, chunk_size: int) -> int:
        if chunk_size < 10:
            logger.warning("Stream chunk size is too small, using default value of 500")
            return 500

        if chunk_size > 20000:
            logger.warning("Stream chunk size is too large, using default value of 500")
            return 500

        return chunk_size

    # ....................... #

    async def _to_read(self, domain: D | None, *, pk: UUID | None = None) -> R:
        if self.hydrate_from_write and domain is not None:
            return pydantic_validate(
                self.read_gw.model_type,
                pydantic_persistence_dump(domain),
            )

        doc_pk = domain.id if domain is not None else pk

        if doc_pk is None:
            raise exc.internal(
                "Cannot load read model after write: domain row missing and no primary key.",
                code="document_hydration_failed",
            )

        return await self.read_gw.get(doc_pk)

    # ....................... #

    async def _to_read_many(
        self,
        domains: Sequence[D | None],
        *,
        pks: Sequence[UUID] | None = None,
    ) -> Sequence[R]:
        if not domains:
            if pks:
                return await self.read_gw.get_many(list(pks))

            return []

        if self.hydrate_from_write and all(d is not None for d in domains):
            return pydantic_validate_many(
                self.read_gw.model_type,
                pydantic_persistence_dump_many(domains),  # type: ignore[arg-type]
            )

        if pks is not None and len(pks) != len(domains):
            raise exc.internal(
                "Primary keys length must match domain rows for read-back.",
                code="document_hydration_failed",
            )

        keys: list[UUID] = []

        for i, domain in enumerate(domains):
            if domain is not None:
                keys.append(domain.id)
            elif pks is not None:
                keys.append(pks[i])
            else:
                raise exc.internal(
                    "Cannot load read models after write: domain row missing and no primary key.",
                    code="document_hydration_failed",
                )

        return await self.read_gw.get_many(keys)

    # ....................... #

    def _require_write(self) -> DocumentWriteGatewayPort[D, C, U]:
        if self.write_gw is None:
            raise exc.configuration("Write gateway is not configured")

        return self.write_gw

    # ....................... #

    async def _finalize_single_write(
        self,
        domain: D,
        *,
        return_new: bool,
        pk: UUID | None = None,
    ) -> R | None:
        if not return_new:
            return None
        res = await self._to_read(domain, pk=pk)
        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_one(res)
        )
        return res

    async def _finalize_bulk_write(
        self,
        domains: Sequence[D],
        *,
        return_new: bool,
        pks: Sequence[UUID] | None = None,
    ) -> Sequence[R] | None:
        if not return_new:
            return None
        res = await self._to_read_many(domains, pks=pks)
        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_many(res)
        )
        return res
