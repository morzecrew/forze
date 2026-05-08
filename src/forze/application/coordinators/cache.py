"""Document read-through cache coordination (versioned keys, deferred warm, invalidation)."""

from typing import Awaitable, Callable, Protocol, Sequence, cast, runtime_checkable
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.cache import CachePort
from forze.application.contracts.tx import AfterCommitPort
from forze.base.primitives import JsonDict
from forze.base.serialization import (
    pydantic_cache_dump,
    pydantic_cache_dump_many,
    pydantic_validate,
    pydantic_validate_many,
)
from forze.domain.constants import ID_FIELD, REV_FIELD

from .._logger import logger

# ----------------------- #


@runtime_checkable
class _ReadModelWithIdAndRev(Protocol):
    id: UUID
    rev: int


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentCacheCoordinator[R: BaseModel]:
    """Coordinates versioned cache reads/writes and post-commit deferral for documents.

    Used when :class:`~forze.application.contracts.cache.CachePort` backs read models
    keyed by ``id`` with optimistic ``rev``. Writes and :meth:`after_commit_or_now` stay
    no-ops without :attr:`cache`.

    Reader paths use :meth:`read_through_eligible`; full-document reads delegate to
    :meth:`get_read_through` and :meth:`get_many_read_through`. When caching is inactive,
    callers should hit persistence directly rather than invoking read-through helpers.

    See :class:`~forze.application.contracts.tx.AfterCommitPort`.
    """

    read_model_type: type[R]
    """Read model used to validate presence of ``id`` and ``rev`` fields."""

    document_name: str
    """Document kind used in logs (typically :attr:`~forze.application.contracts.document.DocumentSpec.name`)."""

    cache: CachePort | None = None
    """Optional cache backend."""

    after_commit: AfterCommitPort | None = None
    """Optional deferral aligned with execution-context commit."""

    # ....................... #

    def read_through_eligible(
        self,
        *,
        skip_cache: bool,
        return_fields: Sequence[str] | None,
    ) -> bool:
        """Whether read-through caching participates (full-document reads only)."""

        return self.cache is not None and return_fields is None and not skip_cache

    # ....................... #

    def id_rev_capable(self) -> bool:
        """Whether :attr:`read_model_type` has ``id`` and ``rev`` for versioned caching."""

        fields = set(self.read_model_type.model_fields.keys())
        return {ID_FIELD, REV_FIELD}.issubset(fields)

    # ....................... #

    async def after_commit_or_now(self, fn: Callable[[], Awaitable[None]]) -> None:
        """Queue or run cache side effects."""

        if self.cache is None:
            return

        if self.after_commit is None:
            await fn()

            return

        await self.after_commit(fn)

    # ....................... #

    async def invalidate_keys_now(self, *pks: UUID) -> None:
        """Invalidate cache entries for primary keys."""

        if self.cache is None:
            return

        await self.clear(*pks)

    # ....................... #

    async def set_one(self, doc: R) -> None:
        """Store one read-model snapshot when versioned caching is permitted."""

        if self.cache is None:
            return

        if not self.id_rev_capable():
            logger.warning(
                "Cannot cache document of type '%s' as it does not have an id and rev",
                type(self.read_model_type).__name__,
            )

            return

        try:
            casted_doc = cast(_ReadModelWithIdAndRev, doc)

            dump = pydantic_cache_dump(doc)

            await self.cache.set_versioned(
                str(casted_doc.id), str(casted_doc.rev), dump
            )

            logger.trace("Cache set successfully")

        except Exception:
            logger.exception("Cache set failed, continuing")

    # ....................... #

    async def set_many(self, docs: Sequence[R]) -> None:
        """Bulk versioned writes for cache warm."""

        if self.cache is None or not docs:
            return

        if not self.id_rev_capable():
            logger.warning(
                "Cannot cache documents of type '%s' as they do not have an id and rev",
                type(self.read_model_type).__name__,
            )

            return

        docs_casted = [cast(_ReadModelWithIdAndRev, x) for x in docs]

        try:
            dumps = pydantic_cache_dump_many(docs)
            versioned_mapping = {
                (str(x.id), str(x.rev)): y
                for x, y in zip(docs_casted, dumps, strict=True)
            }

            await self.cache.set_many_versioned(versioned_mapping)

        except Exception:
            logger.debug(
                "Cache set failed for %s '%s' document(s), continuing",
                len(docs),
                self.document_name,
                exc_info=True,
            )

    # ....................... #

    async def clear(self, *pks: UUID) -> None:
        """Hard-delete cache keys for ``pks``."""

        if self.cache is None:
            return

        if not self.id_rev_capable():
            logger.warning(
                "Cannot clear cache for documents of type '%s' as they do not have an id and rev",
                type(self.read_model_type).__name__,
            )

            return

        try:
            await self.cache.delete_many([str(pk) for pk in pks], hard=True)

        except Exception:
            logger.debug(
                "Cache clear failed for %s '%s' document(s), continuing",
                len(pks),
                self.document_name,
                exc_info=True,
            )

    # ....................... #

    async def get_read_through(
        self,
        pk: UUID,
        *,
        fetch_on_cache_fault: Callable[[], Awaitable[R | JsonDict]],
        fetch_on_miss_without_lock: Callable[[], Awaitable[R]],
    ) -> R | JsonDict:
        """Read-through *pk*: cache layer, resilient fallback on transport errors."""

        if self.cache is None:
            return await fetch_on_cache_fault()

        try:
            cached = await self.cache.get(str(pk))

        except Exception:
            logger.debug(
                "Cache get failed for 1 '%s' document, falling back to read gateway",
                self.document_name,
                exc_info=True,
            )

            return await fetch_on_cache_fault()

        if cached is not None:
            logger.trace("Retrieved 1 cached '%s' document", self.document_name)
            return pydantic_validate(self.read_model_type, cached)

        logger.debug(
            "Fetching 1 '%s' document from database (cache miss)", self.document_name
        )

        res = await fetch_on_miss_without_lock()

        await self.after_commit_or_now(lambda: self.set_one(res))

        return res

    # ....................... #

    async def get_many_read_through(
        self,
        pks: Sequence[UUID],
        *,
        fetch_many_on_cache_fault: Callable[[], Awaitable[Sequence[R | JsonDict]]],
        fetch_misses_many: Callable[[list[str]], Awaitable[Sequence[R]]],
    ) -> Sequence[R]:
        """Read-through for ordered ``pks`` (full rows only).

        Uses :meth:`fetch_misses_many` with string cache miss keys converted by the caller.
        """

        if self.cache is None:
            return cast(
                Sequence[R],
                await fetch_many_on_cache_fault(),
            )

        try:
            hits, misses = await self.cache.get_many([str(pk) for pk in pks])

            if hits:
                logger.trace(
                    "Retrieved %s cached '%s' document(s)",
                    len(hits),
                    self.document_name,
                )

        except Exception as exc:
            logger.debug(
                "Cache get failed for %s '%s' document(s), falling back to read gateway",
                len(pks),
                self.document_name,
                exc_info=True,
            )

            logger.trace("Cache exception: %s", exc)

            return cast(
                Sequence[R],
                await fetch_many_on_cache_fault(),
            )

        miss_res: list[R] = []

        if misses:
            logger.debug(
                "Fetching %s '%s' document(s) from database (cache miss)",
                len(misses),
                self.document_name,
            )

            miss_res = list(await fetch_misses_many(misses))

            await self.after_commit_or_now(lambda: self.set_many(miss_res))

        hits_validated = pydantic_validate_many(
            self.read_model_type,
            list(hits.values()),
        )
        hits_validated_cast = [cast(_ReadModelWithIdAndRev, x) for x in hits_validated]
        miss_res_cast = [cast(_ReadModelWithIdAndRev, x) for x in miss_res]

        by_pk = {x.id: x for x in hits_validated_cast}
        by_pk.update({x.id: x for x in miss_res_cast})

        return [cast(R, by_pk[pk]) for pk in pks]
