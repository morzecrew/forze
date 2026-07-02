"""Wire engine-level idempotency into operation-registry plans."""

from typing import Any, Awaitable, Callable, final

import attrs
from pydantic import BaseModel

from forze.application._logger import logger
from forze.application.contracts.execution import (
    Middleware,
    MiddlewareFactory,
    MiddlewareStep,
    OnSuccess,
    OnSuccessFactory,
)
from forze.application.contracts.crypto import KeyringDepKey
from forze.application.contracts.idempotency import (
    IdempotencyPort,
    IdempotencyRecord,
    IdempotencySpec,
)
from forze.application.execution.context import ExecutionContext
from forze.application.integrations.idempotency import encrypting_idempotency_port
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, stable_payload_fingerprint
from forze.base.serialization import default_model_codec

from ._state import (
    close_recording_scope,
    mark_recorded_in_tx,
    open_recording_scope,
    recorded_in_tx,
)

# ----------------------- #


def _hash_args(args: Any) -> str:
    """Stable fingerprint of normalized operation arguments."""

    payload: Any

    if args is None:
        payload = {}

    elif isinstance(args, BaseModel):
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

    def _require_model_result(self) -> None:
        if not (
            isinstance(self.result_type, type)  # pyright: ignore[reportUnnecessaryIsInstance]
            and issubclass(self.result_type, BaseModel)
        ):
            raise exc.configuration(
                f"IdempotencyWrap result_type must be a Pydantic model, "
                f"got {self.result_type!r}",
            )

    # ....................... #

    def _resolve_port(self, ctx: ExecutionContext) -> IdempotencyPort:
        """Resolve the idempotency port for *ctx*, sealing results when the spec opts in."""

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

        return port

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> Middleware[Any, Any]:
        self._require_model_result()

        port = self._resolve_port(ctx)
        codec = default_model_codec(self.result_type)

        async def _wrap(
            next: Callable[[Any], Awaitable[Any]],
            args: Any,
        ) -> Any:
            key = ctx.inv_ctx.get_idempotency_key()

            if key is None:
                return await next(args)

            # Bracket this invocation in its own recording scope so a nested idempotent
            # operation's in-transaction mark cannot leak into (or out of) this op's read.
            recording = open_recording_scope()

            try:
                payload_hash = _hash_args(args)
                existing = await port.begin(self.op, key, payload_hash)

                if existing is not None:
                    return codec.decode_json_bytes(existing.result)

                try:
                    result = await next(args)

                except Exception:
                    # Release the pending claim so a legitimate retry of the failed
                    # request can re-execute. Best-effort: a fail() error must not mask
                    # the handler error. For a transactional store the in-transaction
                    # record write (if any) already rolled back with the business tx.
                    try:
                        await port.fail(self.op, key, payload_hash)

                    except Exception:
                        logger.exception(
                            "Idempotency fail() errored for op '%s'; the pending "
                            "claim may persist until its TTL",
                            self.op,
                        )

                    raise

                if recorded_in_tx():
                    # A co-located store recorded the result inside the business
                    # transaction (via the paired on_success hook), so it committed
                    # atomically with the business writes — nothing to record here.
                    return result

                # Out-of-transaction store (or no in-tx hook ran): record the result
                # now. The business effect already committed inside ``next``, so a
                # failure to cache must not turn the successful operation into a failure
                # — log and return (the claim then stays pending until its TTL: the
                # documented at-least-once gap of an out-of-transaction store).
                try:
                    await port.commit(
                        self.op,
                        key,
                        payload_hash,
                        IdempotencyRecord(result=codec.encode_json_bytes(result)),
                    )

                except Exception:
                    logger.exception(
                        "Idempotency commit failed after a successful operation '%s'; "
                        "the result was not cached and a duplicate may re-execute",
                        self.op,
                    )

                return result

            finally:
                close_recording_scope(recording)

        return _wrap

    # ....................... #

    def provides_idempotency(self) -> bool:
        """Marker (``ProvidesIdempotency``): this wrap deduplicates the op's effects."""

        return True

    # ....................... #

    def commit_on_success(self) -> OnSuccessFactory:
        """Paired in-transaction record-write hook for a co-located (transactional) store.

        Runs inside the business transaction with the operation result: for a store whose
        :attr:`~forze.application.contracts.idempotency.IdempotencyPort.commits_in_transaction`
        is set, the result record and the business writes commit atomically, closing the
        crash window that an out-of-transaction ``commit`` leaves open. It marks the result
        recorded so the middleware skips its out-of-transaction commit. A no-op for a
        non-transactional store or when no idempotency key is bound.
        """

        self._require_model_result()

        def _factory(ctx: ExecutionContext) -> OnSuccess[Any, Any]:
            port = self._resolve_port(ctx)
            codec = default_model_codec(self.result_type)

            async def _hook(args: Any, result: Any) -> None:
                if not port.commits_in_transaction:
                    return

                key = ctx.inv_ctx.get_idempotency_key()

                if key is None:
                    return

                await port.commit(
                    self.op,
                    key,
                    _hash_args(args),
                    IdempotencyRecord(result=codec.encode_json_bytes(result)),
                )
                mark_recorded_in_tx()

            return _hook

        return _factory

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
