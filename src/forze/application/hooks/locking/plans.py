"""Serialize an operation per input, under a lock its transaction holds until it ends."""

from collections.abc import Callable
from datetime import datetime
from typing import Any, Final, cast, final
from uuid import UUID

import attrs

from forze.application.contracts.execution import Before, BeforeFactory, BeforeStep
from forze.application.contracts.transaction import TxLockPort
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, advisory_lock_key

# ----------------------- #

type OperationLockKey = Callable[[Any], object]
"""``args -> part`` or ``args -> (part, ...)``: what two invocations share when they must not
overlap. Each part is a ``str``, an ``int``, a ``UUID`` or a timezone-aware ``datetime``."""

_LOCK_DOMAIN: Final = "forze.operation"
"""First part of every operation lock key, so one never coincides with a ``SerializedBy`` key."""

_GROUP_DOMAIN: Final = "forze.operation.group"
"""First part of every group lock key, so group ``x`` never coincides with operation ``x``."""

_PART_TYPES: Final = frozenset({str, int, UUID, datetime})
"""Exact types a key part may have: each derives the same key in every process and release."""


def _lock_parts(value: object, *, op: str) -> tuple[object, ...]:
    """The parts of *op*'s lock key, from what its key function returned.

    Exact types only: a subclass (an enum member, ``bool``) may render differently in another
    process or release, and two invocations deriving different keys for one input would not
    contend at all. A datetime keys on its UTC instant, so one offset or another locks alike.

    :param value: A part, or a tuple of parts.
    :param op: The operation, named in a refusal.
    :returns: The parts, in order.
    :raises CoreException: ``configuration`` (``operation_lock_key_unsupported``) for a part of
        any other type; ``validation`` (``operation_lock_key_naive_datetime``) for a datetime
        without a timezone, whose instant is unknown.
    """

    parts = cast(tuple[object, ...], value) if isinstance(value, tuple) else (value,)

    for part in parts:
        kind = type(part)

        if kind not in _PART_TYPES:
            raise exc.configuration(
                f"Operation {op!r} cannot derive its lock key from {kind.__module__}."
                f"{kind.__qualname__}: its key function must return a str, an int, a UUID or "
                "a timezone-aware datetime (pass an enum's .value), or a tuple of them.",
                code="operation_lock_key_unsupported",
            )

        if isinstance(part, datetime) and part.utcoffset() is None:
            raise exc.validation(
                f"Operation {op!r} cannot derive its lock key from a naive datetime: its "
                "instant is unknown, so two invocations for one moment could lock different "
                "keys.",
                code="operation_lock_key_naive_datetime",
            )

    return parts


def _validate_group(_instance: object, _attribute: object, value: str | None) -> None:
    """attrs validator for :attr:`SerializeOperation.group`: unset, or a trimmed name.

    Surrounding whitespace is refused, not stripped: ``"x "`` and ``"x"`` would otherwise be
    one lock spelled two ways, or two locks that look like one.
    """

    if value is not None and (
        not isinstance(value, str)  # pyright: ignore[reportUnnecessaryIsInstance]
        or not value
        or value != value.strip()
    ):
        raise exc.configuration(
            "SerializeOperation group must be a non-empty name without surrounding "
            f"whitespace, got {value!r}.",
            code="operation_lock_group_invalid",
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SerializeOperation(BeforeFactory):
    """Serialize invocations of an operation that share a key, each until its transaction ends.

    Attach it to the operation's transaction scope
    (``.bind_tx().set_route(...).before(SerializeOperation(key=...).to_step())``). Before the
    handler runs, it takes the route's transaction-scoped lock for the operation, the bound
    tenant and :attr:`key`. A second invocation with the same key waits until the first commits
    or rolls back; other keys and other tenants never wait on it. On Postgres that is
    ``pg_advisory_xact_lock``, released by commit or rollback, so no lease expires mid-way.

    The lock is held until the **root** transaction ends. An operation running nested in
    another's transaction takes it in a savepoint: if the nested operation fails, Postgres
    releases the lock with the writes it guarded, and if it succeeds the lock stays until the
    root commits or rolls back. The in-memory store keeps it until the root ends either way.

    The registry binds it to its operation and route at freeze. An operation carrying it in its
    outer scope or without a transaction route is refused there, and a route whose backend has
    no such lock (Mongo, Firestore) is refused when the operation is resolved. What
    :attr:`key` returns is known only per call, so a value it cannot key on (``None``, a
    float, an enum member, a naive datetime) refuses that call, naming the operation.

    With :attr:`group`, every operation naming that group shares one lock: the group takes the
    operation's place in the key, the tenant and :attr:`key` stay. One route's backend holds
    the lock, so freeze refuses a group whose operations run on different transaction routes.
    """

    key: OperationLockKey
    """Derives the key from the operation's arguments (see :data:`OperationLockKey`)."""

    group: str | None = attrs.field(default=None, validator=_validate_group)
    """Shares the lock with every operation naming this group, instead of keying it by the
    operation alone."""

    # ....................... #

    def for_operation(self, op: StrKey, route: StrKey) -> BeforeFactory:
        """Marker (:class:`~forze.application.contracts.execution.HoldsTransactionLock`): this
        hook bound to *op* and its transaction *route*."""

        return _BoundSerializeOperation(key=self.key, op=str(op), route=route, group=self.group)

    # ....................... #

    def lock_group(self) -> str | None:
        """Marker (:class:`~forze.application.contracts.execution.SharesTransactionLock`): the
        group whose operations share this lock."""

        return self.group

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> Before[Any]:
        raise exc.configuration(
            "SerializeOperation runs only once a registry binds it to its operation: attach it "
            "to an operation's transaction scope and freeze the registry.",
        )

    # ....................... #

    def to_step(
        self,
        *,
        step_id: StrKey = "serialize_operation",
        priority: int = 0,
    ) -> BeforeStep:
        """Build a :class:`BeforeStep` for the operation's transaction scope."""

        return BeforeStep(id=step_id, factory=self, priority=priority)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _BoundSerializeOperation:
    """:class:`SerializeOperation` bound to its operation and transaction route."""

    key: OperationLockKey
    op: str
    route: StrKey
    group: str | None = None

    # ....................... #

    def lock_group(self) -> str | None:
        """Marker (:class:`~forze.application.contracts.execution.SharesTransactionLock`), kept
        once bound, so a plan validated again after binding still has its group checked."""

        return self.group

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> Before[Any]:
        # Resolved with the operation, so a route without a lock fails at wiring, not on a call.
        port: TxLockPort = ctx.tx_locks(self.route)
        key, op = self.key, self.op
        domain, name = (_LOCK_DOMAIN, op) if self.group is None else (_GROUP_DOMAIN, self.group)

        async def _before(args: Any) -> None:
            tenant = ctx.inv_ctx.get_tenant()
            parts = _lock_parts(key(args), op=op)
            tenant_id = tenant.tenant_id if tenant is not None else None

            await port.hold(advisory_lock_key(domain, name, tenant_id, *parts))

        return _before
