"""Redis-backed :class:`~forze.application.contracts.idempotency.IdempotencyPort` adapter."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

import hashlib
from datetime import timedelta
from typing import Any, Final, TypedDict, final

import attrs

from forze.application.contracts.idempotency import (
    ClaimOwnerMixin,
    IdempotencyPort,
    IdempotencyRecord,
)
from forze.base.exceptions import exc

from ..kernel.scripts import IDEMPOTENCY_COMMIT, IDEMPOTENCY_RELEASE
from ._logger import logger
from .base import RedisBaseAdapter
from .codecs import default_json_codec

# ----------------------- #

_PENDING: Final[str] = "P"
_DONE: Final[str] = "D"
_IDEMPOTENCY_SCOPE: Final[str] = "idempotency"
# The result body lives under its own top-level scope, not a ``:body`` suffix on
# the metadata path. A distinct scope segment makes body and metadata keys
# structurally incapable of colliding regardless of the caller-supplied key.
_BODY_SCOPE: Final[str] = "idempotency-body"


# ....................... #


class _MetaPayload(TypedDict, total=False):
    """JSON metadata stored at the primary idempotency key (no raw result)."""

    st: str
    ph: str
    own: str
    """Invocation holding a pending claim; absent when no owner was wired."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisIdempotencyAdapter(IdempotencyPort, RedisBaseAdapter, ClaimOwnerMixin):
    """Redis implementation of :class:`~forze.application.contracts.idempotency.IdempotencyPort`.

    Uses ``SET NX`` on a small JSON metadata key for :meth:`begin`, and stores
    the serialized result as raw bytes on a separate result-body key.
    :meth:`commit` is a fenced compare-and-set (metadata flips to DONE and the
    body is written **only** if the current claim is still the caller's own
    pending claim), and :meth:`fail` a compare-and-delete of that same pending
    claim so a retry of a failed request can re-execute before the TTL expires.

    The claim metadata carries the invocation that took it (``own``), so the
    byte-exact compare also separates two *duplicates of one request*: their
    ``op``, key and payload hash are identical, and without the owner the one
    whose claim lapsed would match the reclaimer's live claim and overwrite it.
    An ownerless claim — nothing wired, or one written before this field — still
    matches, so the fence is additive rather than a flag day.

    The caller-supplied idempotency key is untrusted (an ``Idempotency-Key``
    header): it is SHA-256 hashed before it enters any Redis key, and the body
    lives under its own scope segment, so no caller value can collide the
    metadata and body key spaces or one caller's keys with another's.
    """

    ttl: timedelta = timedelta(hours=24)
    """TTL for the idempotency keys."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if int(self.ttl.total_seconds()) < 1:
            raise exc.configuration("TTL must be at least 1 second")

    # ....................... #

    @property
    def commits_in_transaction(self) -> bool:
        """Always ``False``: Redis is not co-located with the business transaction."""

        return False

    # ....................... #

    @staticmethod
    def __key_digest(key: str) -> str:
        """Hex SHA-256 of the untrusted caller key.

        Fixed-length, separator-free, and non-empty, so it cannot alias another
        caller's key path nor smuggle the key separator into the built key.
        """

        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    def __meta_key(self, op: str, key: str) -> str:
        return self.construct_key(_IDEMPOTENCY_SCOPE, op, self.__key_digest(key))

    def __body_key(self, op: str, key: str) -> str:
        return self.construct_key(_BODY_SCOPE, op, self.__key_digest(key))

    # ....................... #

    def __pending_meta(self, payload_hash: str) -> tuple[_MetaPayload, _MetaPayload]:
        """The pending-claim metadata this caller writes, and its ownerless form.

        Both because the fence is a byte-exact compare: the first is what ``begin`` stores
        and ``commit`` / ``fail`` must match, the second is what a claim taken before the
        owner existed looks like and is still the caller's to finish. One helper for all
        three call sites, so no site can build a payload the others would fail to match.
        """

        legacy: _MetaPayload = {"st": _PENDING, "ph": payload_hash}
        owner = self.claim_owner()

        if owner is None:
            return legacy, legacy

        return {"st": _PENDING, "ph": payload_hash, "own": str(owner)}, legacy

    # ....................... #

    async def __acquire_meta(self, meta_key: str, p: _MetaPayload) -> bool:
        return await self.client.set(
            meta_key,
            default_json_codec.dumps(p),
            ex=int(self.ttl.total_seconds()),
            nx=True,
        )

    # ....................... #

    async def begin(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
    ) -> IdempotencyRecord | None:
        if not key:
            logger.debug("Idempotency key is not provided for op '%s', skipping", op)
            return None

        await self._prepare_keys()
        logger.debug("Beginning idempotency for op '%s', key '%s'", op, key[:9] + "...")

        meta_k = self.__meta_key(op, key)
        idem_p, _ = self.__pending_meta(payload_hash)

        if await self.__acquire_meta(meta_k, idem_p):
            logger.debug("Idempotency key is acquired")
            return None

        raw = await self.client.get(meta_k)

        if raw is None:
            if await self.__acquire_meta(meta_k, idem_p):
                logger.debug("Idempotency key is acquired")
                return None

            raise exc.conflict("Idempotency is in progress (not readable)")

        data: dict[str, Any] = default_json_codec.loads(raw)

        data_st = str(data.get("st", ""))
        data_ph = str(data.get("ph", ""))

        if data_ph != payload_hash:
            # conflict, not precondition: the key is already bound to another payload, which
            # is a clash with stored state rather than a malformed request. The kinds are not
            # interchangeable at the boundary — they render as 409 and 400 — so raising the
            # other one here made the same client mistake answer differently depending on
            # which idempotency store the deployment happened to wire.
            raise exc.conflict("Payload hash mismatch")

        if data_st == _PENDING:
            raise exc.conflict("Idempotency is in progress (pending)")

        if data_st != _DONE:
            raise exc.conflict("Idempotency is in progress (unknown state)")

        result_raw = await self.client.get(self.__body_key(op, key))

        if result_raw is None:
            raise exc.conflict("Idempotency is in progress (done without result)")

        result = (
            result_raw
            if isinstance(result_raw, (bytes, bytearray))  # pyright: ignore[reportUnnecessaryIsInstance]
            else str(result_raw).encode("utf-8")
        )

        return IdempotencyRecord(result=bytes(result))

    # ....................... #

    async def commit(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
        record: IdempotencyRecord,
    ) -> None:
        if not key:
            logger.debug("Idempotency key is not provided for op '%s', skipping", op)
            return None

        await self._prepare_keys()
        logger.debug(
            "Committing idempotency for op '%s', key '%s'",
            op,
            key[:9] + "...",
        )

        meta_k = self.__meta_key(op, key)
        body_k = self.__body_key(op, key)
        ex = int(self.ttl.total_seconds())

        # Fenced compare-and-set: flip metadata to DONE and write the body only
        # if the current claim is byte-for-byte our own pending claim. ``SET XX``
        # alone asserted merely that *some* metadata exists, so a stale owner
        # whose claim had lapsed and been re-acquired by another writer could
        # overwrite that writer's claim. The ``own`` field is what extends that to
        # a reclaim by a *duplicate of this same request*, whose ``ph`` matches.
        pending_meta, legacy_meta = self.__pending_meta(payload_hash)
        done_meta: _MetaPayload = {"st": _DONE, "ph": payload_hash}

        committed = await self.client.run_script(
            IDEMPOTENCY_COMMIT,
            [meta_k, body_k],
            [
                default_json_codec.dumps(pending_meta),
                default_json_codec.dumps(done_meta),
                record.result,
                ex,
                default_json_codec.dumps(legacy_meta),
            ],
        )

        if committed != "1":
            raise exc.conflict("Idempotency commit failed (claim missing, expired, or not owned)")

    # ....................... #

    async def fail(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
    ) -> None:
        if not key:
            logger.debug("Idempotency key is not provided for op '%s', skipping", op)
            return

        await self._prepare_keys()
        logger.debug(
            "Releasing idempotency claim for op '%s', key '%s'",
            op,
            key[:9] + "...",
        )

        meta_k = self.__meta_key(op, key)
        body_k = self.__body_key(op, key)

        # Compare-and-delete: drop the claim only if the current metadata is
        # byte-for-byte our own pending claim. A completed record (DONE), a claim
        # re-acquired for a different payload hash or by another invocation, or an
        # already-expired claim is left untouched. This replaces the racy
        # GET-check-DELETE with one atomic server-side step.
        pending_meta, legacy_meta = self.__pending_meta(payload_hash)

        await self.client.run_script(
            IDEMPOTENCY_RELEASE,
            [meta_k, body_k],
            [
                default_json_codec.dumps(pending_meta),
                default_json_codec.dumps(legacy_meta),
            ],
        )
