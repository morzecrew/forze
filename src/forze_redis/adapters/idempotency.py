"""Redis-backed :class:`~forze.application.contracts.idempotency.IdempotencyPort` adapter."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

import base64
from datetime import timedelta
from typing import Final, TypedDict, final

import attrs

from forze.application.contracts.idempotency import IdempotencyPort, IdempotencySnapshot
from forze.base.errors import ConflictError

from ._logger import logger
from .base import RedisBaseAdapter
from .codecs import default_json_codec

# ----------------------- #

_PENDING: Final[str] = "P"
_DONE: Final[str] = "D"
_IDEMPOTENCY_SCOPE: Final[str] = "idempotency"

# ....................... #


class _Payload(TypedDict, total=False):
    """Internal JSON envelope stored in Redis for each idempotency record.

    All fields are optional so that a minimal pending record (only ``st`` and
    ``ph``) can be written on :meth:`RedisIdempotencyAdapter.begin`, then
    enriched with response data on :meth:`RedisIdempotencyAdapter.commit`.
    """

    st: str
    ph: str
    code: int
    ct: str
    body_b64: str


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisIdempotencyAdapter(IdempotencyPort, RedisBaseAdapter):
    """Redis implementation of :class:`~forze.application.contracts.idempotency.IdempotencyPort`.

    Stores a JSON :class:`_Payload` per ``(op, key)`` pair using ``SET NX``
    with a configurable TTL.  :meth:`begin` acquires the slot (pending state)
    and returns a cached snapshot when the operation was already completed.
    :meth:`commit` overwrites the slot with the final response snapshot.
    """

    ttl: timedelta = timedelta(seconds=30)
    """TTL for the idempotency keys."""

    # ....................... #

    def __key(self, op: str, key: str) -> str:
        return self.construct_key(_IDEMPOTENCY_SCOPE, op, key)

    # ....................... #

    def __decode_body(self, b64: str) -> bytes:  #! use codec instead
        return base64.b64decode(b64.encode("ascii"))

    # ....................... #

    def __encode_body(self, body: bytes) -> str:
        return base64.b64encode(body).decode("ascii")

    # ....................... #

    async def __acuire(self, key: str, p: _Payload) -> bool:
        return await self.client.set(
            key,
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
    ) -> IdempotencySnapshot | None:
        if not key:
            logger.debug("Idempotency key is not provided for op '%s', skipping", op)
            return None

        logger.debug("Beginning idempotency for op '%s', key '%s'", op, key[:9] + "...")

        k = self.__key(op, key)
        idem_p = _Payload(st=_PENDING, ph=payload_hash)

        if await self.__acuire(k, idem_p):
            logger.debug("Idempotency key is acquired")
            return None

        raw = await self.client.get(k)

        if raw is None:
            if await self.__acuire(k, idem_p):
                logger.debug("Idempotency key is acquired")
                return None

            raise ConflictError("Idempotency is in progress (not readable)")

        data: _Payload = default_json_codec.loads(raw)

        data_st = data.get("st", "")
        data_ph = data.get("ph", "")
        data_b64 = data.get("body_b64", None)

        if data_ph != payload_hash:
            raise ConflictError("Payload hash mismatch")

        if data_st == _PENDING:
            raise ConflictError("Idempotency is in progress (pending)")

        if data_st != _DONE:
            raise ConflictError("Idempotency is in progress (unknown state)")

        if data_b64 is None:
            raise ConflictError("Idempotency is in progress (done without body)")

        body = self.__decode_body(data_b64)

        return IdempotencySnapshot(
            code=int(data.get("code", 200)),
            content_type=data.get("ct", "application/json"),
            body=body,
        )

    # ....................... #

    async def commit(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
        snapshot: IdempotencySnapshot,
    ) -> None:
        if not key:
            logger.debug("Idempotency key is not provided for op '%s', skipping", op)
            return None

        logger.debug(
            "Committing idempotency for op '%s', key '%s'",
            op,
            key[:9] + "...",
        )

        k = self.__key(op, key)
        idem_p = _Payload(
            st=_DONE,
            ph=payload_hash,
            code=snapshot.code,
            ct=snapshot.content_type,
            body_b64=self.__encode_body(snapshot.body),
        )

        ok = await self.client.set(
            k,
            default_json_codec.dumps(idem_p),
            ex=int(self.ttl.total_seconds()),
            xx=True,
        )

        if not ok:
            raise ConflictError("Idempotency commit failed (key missing or expired)")
