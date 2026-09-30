"""A document-backed :class:`~forze.application.contracts.audit.AuditPort` and its deps module."""

from typing import Any, final

import attrs

from forze.application.contracts.audit import AuditDepKey, AuditEntry, AuditPort
from forze.application.contracts.deps import Deps, DepsModule
from forze.application.contracts.document import DocumentCommandDepKey, DocumentCommandPort
from forze.application.execution import ExecutionContext
from forze.base.primitives import StrKey

from .record import AuditCreate, AuditDocumentSpec, audit_record_spec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentAuditPort(AuditPort):
    """Writes each entry as one row of the audit collection.

    Inside an operation's transaction on :attr:`tx_route` the row joins it; outside one it
    commits in a transaction of its own there.
    """

    ctx: ExecutionContext

    command: DocumentCommandPort[Any, Any, AuditCreate, Any]

    tx_route: StrKey
    """The route of the transaction a row written outside one commits in."""

    # ....................... #

    async def record(self, entry: AuditEntry) -> None:
        ref = entry.object_ref
        payload = AuditCreate(
            action=entry.action,
            outcome=entry.outcome,
            actor_id=entry.actor_id,
            subject_id=entry.subject_id,
            actor_ids=list(entry.actor_ids),
            object_type=ref.type if ref is not None else None,
            object_id=ref.id if ref is not None else None,
            metadata=dict(entry.metadata),
            at=entry.at,
        )

        # Inside the operation's transaction this nests on the same route and commits or rolls
        # back with it; on another route it refuses, since a row on another database cannot be
        # atomic with the operation's write. Outside one it is a transaction of its own.
        async with self.ctx.tx_ctx.scope(self.tx_route):
            await self.command.create(payload, return_new=False)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuditDepsModule(DepsModule):
    """Register a :class:`DocumentAuditPort` under
    :data:`~forze.application.contracts.audit.AuditDepKey`.

    The collection must be wired like any document spec (the application's document deps
    module routes :attr:`spec`), on a database the audited operations' transactions share:
    an admitted operation's row is written inside its transaction.
    """

    tx_route: StrKey
    """The transaction route a row written outside an operation's transaction commits on."""

    spec: AuditDocumentSpec = attrs.field(factory=audit_record_spec)
    """The audit collection."""

    # ....................... #

    def __call__(self) -> Deps:
        return Deps.plain({AuditDepKey: self._port})

    def _port(self, ctx: ExecutionContext) -> AuditPort:
        # The row is the framework's record of an operation, not the operation's effect, so it
        # is the one write a read-only (QUERY) operation may make: this collection's command
        # port is resolved past the read-only guard, and nothing else is.
        command = ctx.deps.resolve_configurable(
            ctx, DocumentCommandDepKey, self.spec, route=self.spec.name
        )

        return DocumentAuditPort(ctx=ctx, command=command, tx_route=self.tx_route)
