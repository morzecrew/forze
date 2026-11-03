"""Child collection reconciliation: make a parent's children match an incoming list.

Reads the parent's current children, matches them to the incoming create commands by a
business key, and writes the difference over the bulk document ports: a dropped child is
deleted, a new one created, and a kept one updated only on the fields that changed. An
unchanged child is not written, so ids, revisions and audit fields survive a save.

It is a standalone component, not an :class:`~forze_kits.aggregates.AggregateKit` arm, and it
neither touches nor re-reads the parent: both stay with the caller (RFC 0050).
"""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, KeyedUpdate
from forze.base.exceptions import exc
from forze.domain.constants import ID_FIELD, REV_FIELD
from forze.domain.models import BaseDTO
from forze_kits.domain.soft_deletion.constants import SOFT_DELETE_FIELD

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ChildCollectionChanges[R: BaseModel]:
    """What one :meth:`ChildCollectionSync.reconcile` wrote."""

    removed: Sequence[R]
    """Children deleted, as they were read before the delete."""

    created: Sequence[R]
    """Children created, as the store returned them, in incoming order."""

    updated: Sequence[tuple[R, R]]
    """``(as read, as written)`` for each child the store changed. A patch the store found
    already applied is left out, so the pairs list only changes this call made."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ChildCollectionSync[R: BaseModel, C: BaseDTO]:
    """Reconcile one parent's child collection against an incoming list.

    Children are matched by :attr:`key_fields`, read off both the stored child and the
    incoming create command. Writes go in a fixed order: deletes, then creates, then updates.
    A new child may therefore take a unique value a removed one held, such as a code or a
    position. A new child taking a value a kept child gives up in the same call needs that
    uniqueness to hold at commit (``UniqueTogether(holds="commit")``, a deferred constraint).

    A child spec whose domain carries ``is_deleted`` is soft-deleted, by writing the flag at the
    revision read; deleted children are not part of the current collection. Any other spec is
    hard-deleted with ``kill_many``.

    :raises CoreException: ``configuration`` at construction when the spec has no write side,
        a named field is not a stored field of the read model and the create command, a compare
        field is not on the update command, the soft-delete flag is not readable and writable,
        or a spec without it forbids hard deletes (``hard_delete_forbidden``).
    """

    child_spec: DocumentSpec[R, Any, C, Any]
    """The child aggregate."""

    parent_fk: str
    """The field on the child holding the parent's id."""

    key_fields: tuple[str, ...]
    """The fields identifying a child within its parent: its business key. A null is part of
    the key, so ``("revision_id", "subproduct_id")`` keys a child holding exactly one of them."""

    compare_fields: frozenset[str] = frozenset()
    """The fields whose change makes a kept child an update. Compared with ``==``; a child
    differing on none of them is not written."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        spec = self.child_spec
        name = str(spec.name)

        if spec.write is None:
            raise exc.configuration(f"Child collection {name!r} needs a writable child spec.")

        if not self.key_fields:
            raise exc.configuration(
                f"Child collection {name!r} names no key field. Without a business key a "
                "changed child cannot be told from one removed and another added.",
            )

        create = spec.write["create_cmd"].model_fields.keys()
        update = spec.write.get("update_cmd")
        writable = update.model_fields.keys() if update is not None else set[str]()
        stored = spec.read.model_fields.keys() - spec.resolved_lenient_read_fields
        named = {self.parent_fk, *self.key_fields, *self.compare_fields}

        for missing, where in (
            ({ID_FIELD, REV_FIELD, *named} - stored, f"stored fields of {spec.read.__name__}"),
            (named - create, "fields of its create command"),
            (self.compare_fields - writable, "fields its update command writes"),
        ):
            if missing:
                raise exc.configuration(
                    f"Child collection {name!r} reads {sorted(missing)}, which are not {where}.",
                )

        if not self._soft_delete:
            spec.require_hard_delete()

        elif SOFT_DELETE_FIELD not in writable or SOFT_DELETE_FIELD not in stored:
            raise exc.configuration(
                f"Child collection {name!r} soft-deletes its children, so its read model must "
                f"store {SOFT_DELETE_FIELD!r} and its update command write it.",
                code="child_soft_delete_unwired",
            )

    # ....................... #

    @property
    def _soft_delete(self) -> bool:
        write = self.child_spec.write

        return write is not None and SOFT_DELETE_FIELD in write["domain"].model_fields

    # ....................... #

    def _key(self, model: BaseModel) -> tuple[Any, ...]:
        return tuple(getattr(model, field) for field in self.key_fields)

    # ....................... #

    async def reconcile(
        self,
        ctx: "ExecutionContext",
        parent_id: UUID,
        incoming: Sequence[C],
    ) -> ChildCollectionChanges[R]:
        """Make *parent_id*'s children match *incoming*.

        Runs inside the caller's transaction, so a failure in any batch leaves none committed.
        It takes no lock: a concurrent writer surfaces as the port's own error.

        :param ctx: Execution context, inside a transaction.
        :param parent_id: The parent whose children are reconciled.
        :param incoming: One create command per wanted child, each naming *parent_id*. An
            empty list deletes every child.
        :returns: The children removed, created and updated.
        :raises CoreException: ``precondition`` (``child_sync_outside_transaction``) outside a
            transaction; ``validation`` (``child_parent_mismatch``) when a command names another
            parent, or (``child_key_duplicate``) when two share a key; ``conflict``
            (``child_key_ambiguous``) when two stored children share one. Nothing is written
            in these cases.
        """

        if ctx.tx_ctx.depth() == 0:
            raise exc.precondition(
                "Reconciling a child collection writes up to three batches, so it must run "
                "inside a transaction: outside one, a failed batch leaves the others committed.",
                code="child_sync_outside_transaction",
            )

        wanted: dict[tuple[Any, ...], C] = {}

        for cmd in incoming:
            if getattr(cmd, self.parent_fk) != parent_id:
                raise exc.validation(
                    f"A child names another parent in {self.parent_fk!r}.",
                    code="child_parent_mismatch",
                )

            if wanted.setdefault(self._key(cmd), cmd) is not cmd:
                raise exc.validation(
                    f"Two children share the key {self.key_fields}.",
                    code="child_key_duplicate",
                )

        live: dict[str, Any] = {self.parent_fk: parent_id}

        if self._soft_delete:
            live[SOFT_DELETE_FIELD] = False

        page = await ctx.document.query(self.child_spec).find_many(filters={"$values": live})
        current: dict[tuple[Any, ...], R] = {}

        for row in page.hits:
            if current.setdefault(self._key(row), row) is not row:
                raise exc.conflict(
                    f"Two stored children of one parent share the key {self.key_fields}.",
                    code="child_key_ambiguous",
                )

        return await self._write(ctx, current, wanted)

    # ....................... #

    async def _write(
        self,
        ctx: "ExecutionContext",
        current: dict[tuple[Any, ...], R],
        wanted: dict[tuple[Any, ...], C],
    ) -> ChildCollectionChanges[R]:
        write = self.child_spec.write  # not None: checked at construction
        update_cmd: Any = write.get("update_cmd") if write is not None else None
        port = ctx.document.command(self.child_spec)

        def keyed(row: R, patch: dict[str, Any]) -> KeyedUpdate[Any]:
            return KeyedUpdate(
                id=getattr(row, ID_FIELD), rev=getattr(row, REV_FIELD), dto=update_cmd(**patch)
            )

        removed = [row for key, row in current.items() if key not in wanted]
        new = [cmd for key, cmd in wanted.items() if key not in current]
        changed: list[tuple[R, dict[str, Any]]] = []

        for key, row in current.items():
            if (cmd := wanted.get(key)) is None:
                continue

            patch = {
                field: getattr(cmd, field)
                for field in sorted(self.compare_fields)
                if getattr(row, field) != getattr(cmd, field)
            }

            if patch:
                changed.append((row, patch))

        if removed and self._soft_delete:
            soft = [keyed(row, {SOFT_DELETE_FIELD: True}) for row in removed]
            await port.update_many(soft, return_new=False)

        elif removed:
            await port.kill_many([getattr(row, ID_FIELD) for row in removed])

        created = await port.create_many(new) if new else []
        updated: list[tuple[R, R]] = []

        if changed:
            written = await port.update_many(
                [keyed(row, patch) for row, patch in changed], return_diff=True
            )
            updated = [
                (before, after)
                for (before, _), (after, diff) in zip(changed, written, strict=True)
                if diff
            ]

        return ChildCollectionChanges(removed=removed, created=created, updated=updated)
