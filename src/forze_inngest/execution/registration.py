"""Register Forze durable functions with the Inngest SDK."""

from forze_inngest._compat import require_inngest

require_inngest()

# ....................... #

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from typing import Any, Generic, Self, TypeVar, final

import attrs
import inngest
from pydantic import BaseModel, ValidationError

from forze.application.contracts.crypto import KeyringDepKey
from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionEventTrigger,
    DurableFunctionSpec,
    DurableFunctionTrigger,
)
from forze.application.contracts.execution import Handler
from forze.application.execution.context import (
    ExecutionContext,
    ExecutionContextFactory,
)
from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze.application.execution.operations.run import handler_for_registry_operation
from forze.application.integrations.crypto import is_encrypted_payload
from forze.base.exceptions import CoreException, exc, exception_egress_policy

from ..adapters.context import (
    InngestDecodedContext,
    split_envelope,
)
from ..adapters.crypto import open_event_payload
from ..adapters.step import bind_inngest_step, reset_inngest_step
from ..kernel.client import InngestClientPort
from ._logger import logger

# ----------------------- #

In = TypeVar("In", bound=BaseModel)
Out = TypeVar("Out", bound=BaseModel)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class InngestFunctionConfig:
    """Optional Inngest function-level controls forwarded to ``@sdk.create_function``.

    All fields default to ``None`` (the SDK's own defaults), so an unset config changes nothing.
    Values are the Inngest SDK's native types (``inngest.Concurrency`` / ``RateLimit`` / …).
    """

    retries: int | None = None
    concurrency: list[inngest.Concurrency] | None = None
    rate_limit: inngest.RateLimit | None = None
    throttle: inngest.Throttle | None = None
    idempotency: str | None = None
    priority: inngest.Priority | None = None
    debounce: inngest.Debounce | None = None
    batch_events: inngest.Batch | None = None
    timeouts: inngest.Timeouts | None = None
    singleton: inngest.Singleton | None = None
    cancel: list[inngest.Cancel] | None = None


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class InngestFunctionBinding(Generic[In, Out]):
    """Binds a :class:`DurableFunctionSpec` to a handler or frozen registry."""

    spec: DurableFunctionSpec[In, Out]
    """Function specification (triggers and argument types)."""

    handler_factory: Callable[[ExecutionContext], Handler[In, Out]] | None = None
    """Custom handler factory when :attr:`~DurableFunctionSpec.operation` is unset."""

    registry: FrozenOperationRegistry | None = None
    """Frozen registry for :attr:`~DurableFunctionSpec.operation` (optional if passed to :func:`register_functions`)."""

    config: InngestFunctionConfig | None = None
    """Optional Inngest function-level config (retries / concurrency / rate-limit / …)."""

    def __attrs_post_init__(self) -> None:
        if self.spec.operation is not None and self.handler_factory is not None:
            raise exc.configuration(
                "InngestFunctionBinding cannot set both spec.operation and handler_factory",
            )

        if self.spec.operation is None and self.handler_factory is None:
            raise exc.configuration(
                "InngestFunctionBinding requires handler_factory when spec.operation is unset",
            )

    @classmethod
    def for_registry_operation(
        cls,
        spec: DurableFunctionSpec[In, Out],
        registry: FrozenOperationRegistry,
        *,
        config: InngestFunctionConfig | None = None,
    ) -> Self:
        """Bind *spec* with :attr:`~DurableFunctionSpec.operation` to *registry*."""

        return cls(spec=spec, registry=registry, config=config)


# ....................... #


def _map_trigger(
    trigger: DurableFunctionTrigger,
) -> inngest.TriggerEvent | inngest.TriggerCron:
    if isinstance(trigger, DurableFunctionEventTrigger):
        return inngest.TriggerEvent(event=str(trigger.event))

    if isinstance(trigger, DurableFunctionCronTrigger):  # pyright: ignore[reportUnnecessaryIsInstance]
        return inngest.TriggerCron(cron=trigger.expression)

    raise TypeError(f"unsupported durable function trigger: {type(trigger)!r}")


# ....................... #


@contextmanager
def _bind_invocation(
    ctx: ExecutionContext,
    envelope: InngestDecodedContext,
    *,
    bind_identity: bool,
) -> Iterator[None]:
    # The ``_forze`` envelope is plaintext, attacker-controllable event data — any producer able to
    # emit an event can set ``principal_id`` / ``tenant_id`` to whatever it likes. So the claimed
    # identity is bound only when the caller opted in (``bind_identity_from_event=True``, for a
    # deployment where every event producer is trusted); by default it is dropped, otherwise any
    # event would impersonate any principal in any tenant. Metadata (correlation/execution ids) is
    # tracing context, not an authority, so it always propagates.
    authn = envelope.authn if bind_identity else None
    tenant = envelope.tenant if bind_identity else None

    if envelope.metadata is not None:
        with ctx.inv_ctx.bind(
            metadata=envelope.metadata,
            authn=authn,
            tenant=tenant,
        ):
            yield

    else:
        with ctx.inv_ctx.bind_identity(
            authn=authn,
            tenant=tenant,
        ):
            yield


# ....................... #


def _as_non_retriable(e: CoreException) -> "inngest.NonRetriableError | None":
    """Map a per-policy *terminal* failure to ``NonRetriableError``, else ``None``.

    A non-retryable-per-policy failure (validation / domain / precondition / auth / a
    forged-tenant AEAD open / …) won't converge on retry, so Inngest should stop. Retryable kinds
    (infrastructure, throttled, concurrency) return ``None`` so they propagate unchanged and
    Inngest's own retry policy applies.
    """

    if not exception_egress_policy(e.kind).retryable:
        return inngest.NonRetriableError(str(e))

    return None


# ....................... #


def _resolve_handler_factory(
    binding: InngestFunctionBinding[Any, Any],
    *,
    registry: FrozenOperationRegistry | None,
) -> Callable[[ExecutionContext], Handler[Any, Any]]:
    spec = binding.spec

    if spec.operation is not None:
        reg = binding.registry or registry

        if reg is None:
            raise exc.configuration(
                "register_functions requires registry= when spec.operation is set "
                "and InngestFunctionBinding.registry is unset",
            )

        return handler_for_registry_operation(reg, spec.operation)

    if binding.handler_factory is None:
        raise exc.configuration(
            "InngestFunctionBinding.handler_factory is required when spec.operation is unset",
        )

    return binding.handler_factory


# ....................... #


def _register_one(
    sdk: inngest.Inngest,
    binding: InngestFunctionBinding[Any, Any],
    *,
    ctx_factory: ExecutionContextFactory,
    registry: FrozenOperationRegistry | None,
    bind_identity_from_event: bool,
) -> inngest.Function[Any]:
    spec = binding.spec
    handler_factory = _resolve_handler_factory(binding, registry=registry)
    triggers = [_map_trigger(t) for t in spec.triggers]

    trigger: (
        inngest.TriggerEvent
        | inngest.TriggerCron
        | list[inngest.TriggerEvent | inngest.TriggerCron]
    )

    trigger = triggers[0] if len(triggers) == 1 else triggers

    cfg = binding.config or InngestFunctionConfig()

    @sdk.create_function(
        fn_id=str(spec.name),
        trigger=trigger,
        retries=cfg.retries,
        concurrency=cfg.concurrency,
        rate_limit=cfg.rate_limit,
        throttle=cfg.throttle,
        idempotency=cfg.idempotency,
        priority=cfg.priority,
        debounce=cfg.debounce,
        batch_events=cfg.batch_events,
        timeouts=cfg.timeouts,
        singleton=cfg.singleton,
        cancel=cfg.cancel,
    )
    async def _handler(ctx: inngest.Context) -> Any:
        raw_data: dict[str, Any] = dict(ctx.event.data) if ctx.event else {}

        envelope, payload = split_envelope(raw_data)
        execution_ctx = ctx_factory()

        if is_encrypted_payload(payload):
            # End-to-end sealed payload: decrypt before validating the typed args, so the
            # handler never sees ciphertext. The key resolves from the self-describing
            # envelope; the tenant for the AAD comes from the (plaintext) ``_forze`` context.
            cipher = (
                execution_ctx.deps.provide(KeyringDepKey)
                if execution_ctx.deps.exists(KeyringDepKey)
                else None
            )
            try:
                payload = await open_event_payload(cipher, payload, tenant=envelope.tenant)
            except CoreException as e:
                # A forged tenant / tampered ciphertext fails the AEAD open deterministically —
                # retrying never converges. Map a terminal decrypt failure to NonRetriableError
                # so Inngest stops; a retryable kind (e.g. a transient key fetch) propagates.
                if (non_retriable := _as_non_retriable(e)) is not None:
                    raise non_retriable from e
                raise

        try:
            args = binding.spec.run.args_type.model_validate(payload)
        except ValidationError as e:
            # A malformed event is deterministic — retrying it forever never converges, so tell
            # Inngest to stop.
            raise inngest.NonRetriableError(f"Invalid event payload for {spec.name!r}: {e}") from e

        step_token = bind_inngest_step(ctx.step)

        logger.debug("Inngest function invoked", function=str(spec.name))

        try:
            with _bind_invocation(execution_ctx, envelope, bind_identity=bind_identity_from_event):
                handler = handler_factory(execution_ctx)
                return await handler(args)

        except CoreException as e:
            # Map a non-retryable-per-policy failure (validation/domain/precondition/auth/…) to
            # Inngest's NonRetriableError so it stops retrying; retryable kinds (infrastructure,
            # throttled, concurrency) propagate unchanged so Inngest's own retry policy applies.
            if (non_retriable := _as_non_retriable(e)) is not None:
                raise non_retriable from e
            raise

        finally:
            reset_inngest_step(step_token)

    return _handler


# ....................... #


def register_functions(
    client: InngestClientPort,
    bindings: Sequence[InngestFunctionBinding[Any, Any]],
    *,
    ctx_factory: ExecutionContextFactory,
    registry: FrozenOperationRegistry | None = None,
    bind_identity_from_event: bool = False,
) -> list[inngest.Function[Any]]:
    """Build Inngest SDK functions from Forze bindings.

    ``bind_identity_from_event`` (default ``False``) controls whether the ``principal_id`` /
    ``tenant_id`` carried in the event's plaintext ``_forze`` envelope are bound as the invocation
    identity. That envelope is **untrusted** — any producer able to emit an event sets it — so it
    stays off by default: enabling it lets an event impersonate any principal in any tenant, so
    only turn it on for a deployment where every event producer is trusted (mirrors the inbox
    consumer's ``bind_tenant_from_headers``). Tracing metadata (correlation/execution ids) is
    propagated regardless; end-to-end payload decryption still uses the envelope tenant for AAD,
    which is self-authenticating (a forged tenant fails the AEAD open).
    """

    sdk = client.native

    return [
        _register_one(
            sdk,
            binding,
            ctx_factory=ctx_factory,
            registry=registry,
            bind_identity_from_event=bind_identity_from_event,
        )
        for binding in bindings
    ]
