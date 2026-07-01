"""Redis-backed :class:`~forze.application.contracts.idempotency.IdempotencyPort` adapter."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from datetime import timedelta
from typing import Any, Final, TypedDict, final

import attrs

from forze.application.contracts.idempotency import IdempotencyPort, IdempotencyRecord
from forze.base.exceptions import exc

from ._logger import logger
from .base import RedisBaseAdapter
from .codecs import default_json_codec

# ----------------------- #

_PENDING: Final[str] = "P"
_DONE: Final[str] = "D"
_IDEMPOTENCY_SCOPE: Final[str] = "idempotency"
_BODY_SUFFIX: Final[str] = "body"


# ....................... #


class _MetaPayload(TypedDict, total=False):
    """JSON metadata stored at the primary idempotency key (no raw result)."""

    st: str
    ph: str


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisIdempotencyAdapter(IdempotencyPort, RedisBaseAdapter):
    """Redis implementation of :class:`~forze.application.contracts.idempotency.IdempotencyPort`.

    Uses ``SET NX`` on a small JSON metadata key for :meth:`begin`, stores the
    serialized result as raw bytes on a sibling key on :meth:`commit`, and keeps
    metadata and result expiries aligned via a transactional pipeline.
    :meth:`fail` deletes a matching *pending* claim so a retry of a failed
    request can re-execute before the TTL expires.
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

    def __meta_key(self, op: str, key: str) -> str:
        return self.construct_key(_IDEMPOTENCY_SCOPE, op, key)

    def __body_key(self, op: str, key: str) -> str:
        return self.construct_key(_IDEMPOTENCY_SCOPE, op, key, _BODY_SUFFIX)

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
        idem_p: _MetaPayload = {"st": _PENDING, "ph": payload_hash}

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
            raise exc.precondition("Payload hash mismatch")

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
        meta_done: _MetaPayload = {
            "st": _DONE,
            "ph": payload_hash,
        }

        async with self.client.pipeline(transaction=True):
            await self.client.set(body_k, record.result, ex=ex)
            await self.client.set(
                meta_k,
                default_json_codec.dumps(meta_done),
                ex=ex,
                xx=True,
            )

        raw_check = await self.client.get(meta_k)

        if raw_check is None:
            raise exc.conflict("Idempotency commit failed (key missing or expired)")

        done_meta: dict[str, Any] = default_json_codec.loads(raw_check)

        if str(done_meta.get("st", "")) != _DONE:
            raise exc.conflict("Idempotency commit failed (key missing or expired)")

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
        raw = await self.client.get(meta_k)

        if raw is None:
            return  # claim already expired or never taken

        data: dict[str, Any] = default_json_codec.loads(raw)

        # Only release our own pending claim: a completed record (DONE) or a
        # claim for a different payload hash is left untouched.
        if (
            str(data.get("st", "")) != _PENDING
            or str(data.get("ph", "")) != payload_hash
        ):
            return

        await self.client.delete(meta_k, self.__body_key(op, key))
