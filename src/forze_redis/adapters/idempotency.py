"""Redis-backed :class:`~forze.application.contracts.idempotency.IdempotencyPort` adapter."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

import base64
from datetime import timedelta
from typing import Any, Final, TypedDict, final

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
_BODY_SUFFIX: Final[str] = "body"


# ....................... #


class _MetaPayload(TypedDict, total=False):
    """JSON metadata stored at the primary idempotency key (no raw body)."""

    st: str
    ph: str
    code: int
    ct: str


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisIdempotencyAdapter(IdempotencyPort, RedisBaseAdapter):
    """Redis implementation of :class:`~forze.application.contracts.idempotency.IdempotencyPort`.

    Uses ``SET NX`` on a small JSON metadata key for :meth:`begin`, stores the
    response body as raw bytes on a sibling key on :meth:`commit`, and keeps
    metadata and body expiries aligned via a transactional pipeline.
    """

    ttl: timedelta = timedelta(seconds=30)
    """TTL for the idempotency keys."""

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
    ) -> IdempotencySnapshot | None:
        if not key:
            logger.debug("Idempotency key is not provided for op '%s', skipping", op)
            return None

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

            raise ConflictError("Idempotency is in progress (not readable)")

        data: dict[str, Any] = default_json_codec.loads(raw)

        data_st = str(data.get("st", ""))
        data_ph = str(data.get("ph", ""))

        if data_ph != payload_hash:
            raise ConflictError("Payload hash mismatch")

        if data_st == _PENDING:
            raise ConflictError("Idempotency is in progress (pending)")

        if data_st != _DONE:
            raise ConflictError("Idempotency is in progress (unknown state)")

        body_raw = await self.client.get(self.__body_key(op, key))
        if body_raw is None:
            # Legacy layout: body embedded as base64 in JSON.
            legacy_b64 = data.get("body_b64")
            if isinstance(legacy_b64, str):
                body = base64.b64decode(legacy_b64.encode("ascii"))
            else:
                raise ConflictError("Idempotency is in progress (done without body)")

        else:
            body = (
                body_raw
                if isinstance(
                    body_raw, (bytes, bytearray)
                )  # pyright: ignore[reportUnnecessaryIsInstance]
                else str(body_raw).encode("utf-8")
            )

        return IdempotencySnapshot(
            code=int(data.get("code", 200)),
            content_type=str(data.get("ct", "application/json")),
            body=bytes(body),
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

        meta_k = self.__meta_key(op, key)
        body_k = self.__body_key(op, key)
        ex = int(self.ttl.total_seconds())
        meta_done: _MetaPayload = {
            "st": _DONE,
            "ph": payload_hash,
            "code": snapshot.code,
            "ct": snapshot.content_type,
        }

        async with self.client.pipeline(transaction=True):
            await self.client.set(body_k, snapshot.body, ex=ex)
            await self.client.set(
                meta_k,
                default_json_codec.dumps(meta_done),
                ex=ex,
                xx=True,
            )

        raw_check = await self.client.get(meta_k)
        if raw_check is None:
            raise ConflictError("Idempotency commit failed (key missing or expired)")

        done_meta: dict[str, Any] = default_json_codec.loads(raw_check)
        if str(done_meta.get("st", "")) != _DONE:
            raise ConflictError("Idempotency commit failed (key missing or expired)")
