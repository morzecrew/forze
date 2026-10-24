"""Bind the boundary context and run an operation through the governed pipeline."""

from typing import Any
from uuid import UUID

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.primitives import StrKey, uuid7

from ..context import ExecutionContext, InvocationMetadata
from ..operations import FrozenOperationRegistry, run_operation

# ----------------------- #


def mint_invocation_metadata(
    *,
    correlation_id: UUID | None = None,
    causation_id: UUID | None = None,
) -> InvocationMetadata:
    """Metadata for a new invocation: a fresh ``execution_id``, and a fresh
    ``correlation_id`` unless the caller carries one."""

    return InvocationMetadata(
        execution_id=uuid7(),
        correlation_id=correlation_id or uuid7(),
        causation_id=causation_id,
    )


# ....................... #


async def invoke_at_ingress(
    registry: FrozenOperationRegistry,
    ctx: ExecutionContext,
    op: StrKey,
    args: Any,
    *,
    authn: AuthnIdentity | None,
    tenant: TenantIdentity | None,
    metadata: InvocationMetadata,
    idempotency_key: str | None = None,
    deadline: float | None = None,
) -> Any:
    """Bind metadata, identity, idempotency key and deadline on *ctx*, then run *op*.

    The ingress enforces nothing itself: authz, tenancy, read-only enforcement and audit
    apply inside :func:`~forze.application.execution.operations.run_operation`.

    :param registry: The frozen operation registry.
    :param ctx: The scope's execution context.
    :param op: The operation key; check the catalog first, since an unknown key is an
        ``internal`` error.
    :param args: The validated input DTO (see :func:`validate_operation_input`).
    :param authn: The identity to bind, verified by the caller.
    :param tenant: The tenant to bind, resolved by the caller.
    :param metadata: The invocation's metadata (see :func:`mint_invocation_metadata`).
    :param idempotency_key: The caller's idempotency key, if any. ``None`` is bound too,
        so the call never inherits a key bound around it.
    :param deadline: Seconds from now the operation must finish in; it only ever tightens
        the plan's deadline.
    """

    with (
        ctx.inv_ctx.bind(metadata=metadata, authn=authn, tenant=tenant),
        ctx.inv_ctx.bind_idempotency(idempotency_key),
        ctx.inv_ctx.bind_deadline(deadline),
    ):
        return await run_operation(registry, op, args, ctx)
