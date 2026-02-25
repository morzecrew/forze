import base64
from typing import Final, Optional, TypedDict

import attrs

from forze.application.kernel.ports import IdempotencyPort, IdempotencySnapshot
from forze.base.errors import ConflictError
from forze.infra.utils import JsonCodec, KeyCodec

from ..platform import RedisClient

# ----------------------- #

_PENDING: Final[str] = "P"
_DONE: Final[str] = "D"

# ....................... #


class _Payload(TypedDict, total=False):
    st: str
    ph: str
    code: int
    ct: str
    body_b64: str


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisIdempotencyGateway(IdempotencyPort):
    client: RedisClient

    # Non initable fields #! TODO: replace with static definitions
    key_codec: KeyCodec = attrs.field(
        factory=lambda: KeyCodec(namespace="idem"),
        init=False,
    )
    json_codec: JsonCodec = attrs.field(factory=JsonCodec, init=False)

    # Defaults (overrideable)
    ttl_s: int = 30

    # ....................... #

    def _key(self, op: str, key: str) -> str:
        return self.key_codec.join(op, key)

    # ....................... #

    def _decode_body(self, b64: str) -> bytes:
        return base64.b64decode(b64.encode("ascii"))

    # ....................... #

    def _encode_body(self, body: bytes) -> str:
        return base64.b64encode(body).decode("ascii")

    # ....................... #

    async def _acuire(self, key: str, p: _Payload) -> bool:
        return await self.client.set(
            key,
            self.json_codec.dumps(p),
            ex=self.ttl_s,
            nx=True,
        )

    # ....................... #

    async def begin(
        self,
        op: str,
        key: Optional[str],
        payload_hash: str,
    ) -> IdempotencySnapshot | None:
        if not key:
            return

        k = self._key(op, key)
        idem_p = _Payload(st=_PENDING, ph=payload_hash)

        if await self._acuire(k, idem_p):
            return

        raw = await self.client.get(k)

        if raw is None:
            if await self._acuire(k, idem_p):
                return None

            raise ConflictError("Idempotency is in progress (not readable)")

        data: _Payload = self.json_codec.loads(raw)

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

        body = self._decode_body(data_b64)

        return IdempotencySnapshot(
            code=int(data.get("code", 200)),
            content_type=data.get("ct", "application/json"),
            body=body,
        )

    # ....................... #

    async def commit(
        self,
        op: str,
        key: Optional[str],
        payload_hash: str,
        snapshot: IdempotencySnapshot,
    ) -> None:
        if not key:
            return

        k = self._key(op, key)
        idem_p = _Payload(
            st=_DONE,
            ph=payload_hash,
            code=snapshot["code"],
            ct=snapshot["content_type"],
            body_b64=self._encode_body(snapshot["body"]),
        )

        ok = await self.client.set(
            k, self.json_codec.dumps(idem_p), ex=self.ttl_s, xx=True
        )

        if not ok:
            raise ConflictError("Idempotency commit failed (key missing or expired)")
