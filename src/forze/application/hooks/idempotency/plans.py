"""Wire engine-level idempotency into operation-registry plans."""

from typing import Any, Awaitable, Callable, final

import attrs
import msgspec
from pydantic import BaseModel

from forze.application._logger import logger
from forze.application.contracts.execution import (
    Middleware,
    MiddlewareFactory,
    MiddlewareStep,
)
from forze.application.contracts.crypto import KeyringDepKey
from forze.application.contracts.idempotency import IdempotencyRecord, IdempotencySpec
from forze.application.execution.context import ExecutionContext
from forze.application.integrations.idempotency import encrypting_idempotency_port
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, stable_payload_fingerprint
from forze.base.serialization import default_model_codec

# ----------------------- #


def _hash_args(args: Any) -> str:
    """Stable fingerprint of normalized operation arguments."""

    payload: Any

    if args is None:
        payload = {}

    elif isinstance(args, (BaseModel, msgspec.Struct)):
        payload = default_model_codec(type(args)).encode_mapping(args)

    else:
        payload = args

    return stable_payload_fingerprint(payload)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class IdempotencyWrap(MiddlewareFactory):
    """Wrap middleware that replays a stored result for a duplicate idempotency key.

    On a cache hit the handler (and its transaction) is skipped and the stored,
    typed result is returned; on a miss the handler runs and its result is stored.
    When the handler fails, the pending claim is released (best-effort) so a retry
    of the failed request re-executes instead of conflicting until the claim TTL.
    The wrap is a no-op when no idempotency key is bound on the invocation.
    """

    op: str
    """Operation name; namespaces the stored entry."""

    spec: IdempotencySpec
    """Idempotency store spec (TTL / routing)."""

    result_type: type[Any]
    """Operation result type; its default codec (de)serializes the stored result."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> Middleware[Any, Any]:
        if not (
            isinstance(self.result_type, type)  # pyright: ignore[reportUnnecessaryIsInstance]
            and issubclass(self.result_type, (BaseModel, msgspec.Struct))
        ):
            raise exc.configuration(
                f"IdempotencyWrap result_type must be a Pydantic or msgspec model, "
                f"got {self.result_type!r}",
            )

        port = ctx.idempotency(self.spec)

        if self.spec.encrypt_result:
            port = encrypting_idempotency_port(
                port,
                cipher=(
                    ctx.deps.provide(KeyringDepKey)
                    if ctx.deps.exists(KeyringDepKey)
                    else None
                ),
                tenant_provider=ctx.inv_ctx.get_tenant,
                spec_name=str(self.spec.name),
            )

        codec = default_model_codec(self.result_type)

        async def _wrap(
            next: Callable[[Any], Awaitable[Any]],
            args: Any,
        ) -> Any:
            key = ctx.inv_ctx.get_idempotency_key()

            if key is None:
                return await next(args)

            payload_hash = _hash_args(args)
            existing = await port.begin(self.op, key, payload_hash)

            if existing is not None:
                return codec.decode_json_bytes(existing.result)

            try:
                result = await next(args)

            except Exception:
                # Release the pending claim so a legitimate retry of the failed
                # request can re-execute. Best-effort: a fail() error must not
                # mask the handler error.
                try:
                    await port.fail(self.op, key, payload_hash)

                except Exception:
                    logger.exception(
                        "Idempotency fail() errored for op '%s'; the pending "
                        "claim may persist until its TTL",
                        self.op,
                    )

                raise

            await port.commit(
                self.op,
                key,
                payload_hash,
                IdempotencyRecord(result=codec.encode_json_bytes(result)),
            )

            return result

        return _wrap

    # ....................... #

    def provides_idempotency(self) -> bool:
        """Marker (``ProvidesIdempotency``): this wrap deduplicates the op's effects."""

        return True

    # ....................... #

    def to_step(
        self,
        *,
        step_id: StrKey = "idempotency",
        priority: int = 10,
    ) -> MiddlewareStep:
        """Build a :class:`MiddlewareStep`.

        The default low ``priority`` places idempotency as the outermost wrap, so a
        cache hit skips inner wraps and the transaction. ``before`` hooks
        (authn / authz) still run first, so a replayed result stays authorized.
        """

        return MiddlewareStep(id=step_id, factory=self, priority=priority)
