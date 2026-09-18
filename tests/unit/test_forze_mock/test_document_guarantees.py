"""The in-memory store enforces a declared storage guarantee, on every write path.

The vocabulary and the reconciliation are tested against the contract; what needs testing here
is the enforcement, and specifically that it is not attached to whichever method the first test
happened to call. The mock's store is a plain dict that nine methods write into — ``create``,
``ensure``, ``update``, ``update_many``, ``touch``, ``touch_many``, ``delete``, ``delete_many``,
``restore`` — and a guarantee checked at one of them is a guarantee eight paths ignore. Each
path that can produce a violation gets its own leg below.

``restore`` is the least obvious and the most important: a soft-deleted row sitting outside a
filtered guarantee comes back *inside* it, which is a conflict created by an un-delete.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentSpec,
    DocumentWriteTypes,
    KeyedUpdate,
)
from forze.application.contracts.guarantees import NonOverlapping, UniqueTogether
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _Fact(Document):
    root_id: str
    is_current: bool = True
    label: str = ""
    supersedes_id: str | None = None


class _FactRead(ReadDocument):
    root_id: str
    is_current: bool = True
    label: str = ""
    supersedes_id: str | None = None


class _FactCreate(CreateDocumentCmd):
    root_id: str
    is_current: bool = True
    label: str = ""
    supersedes_id: str | None = None


class _FactUpdate(BaseDTO):
    is_current: bool | None = None
    label: str | None = None


class _Erasable(Document):
    root_id: str
    is_deleted: bool = False


class _ErasableRead(ReadDocument):
    root_id: str
    is_deleted: bool = False


class _ErasableCreate(CreateDocumentCmd):
    root_id: str


class _ErasableUpdate(BaseDTO):
    root_id: str | None = None


def _erasable_spec(
    *guarantees: UniqueTogether | NonOverlapping,
) -> DocumentSpec[_ErasableRead, _Erasable, _ErasableCreate, _ErasableUpdate]:
    return DocumentSpec[_ErasableRead, _Erasable, _ErasableCreate, _ErasableUpdate](
        name="erasable",
        read=_ErasableRead,
        write=DocumentWriteTypes(
            domain=_Erasable,
            create_cmd=_ErasableCreate,
            update_cmd=_ErasableUpdate,
        ),
        guarantees=guarantees,
    )


def _spec(
    *guarantees: UniqueTogether | NonOverlapping,
) -> DocumentSpec[_FactRead, _Fact, _FactCreate, _FactUpdate]:
    return DocumentSpec[_FactRead, _Fact, _FactCreate, _FactUpdate](
        name="fact",
        read=_FactRead,
        write=DocumentWriteTypes(domain=_Fact, create_cmd=_FactCreate, update_cmd=_FactUpdate),
        guarantees=guarantees,
    )


ONE_CURRENT = UniqueTogether(fields=("root_id",), where={"$values": {"is_current": True}})
ONE_EVER = UniqueTogether(fields=("root_id",))


def _command(spec: DocumentSpec[Any, Any, Any, Any]) -> Any:
    """The write port for *spec*.

    Typed loosely on purpose: two unrelated aggregates are exercised here (one soft-deletable),
    and the guarantee under test is the same for both.
    """

    return context_from_modules(MockDepsModule()).doc.command(spec)


# ....................... #


class TestTheFilteredGuarantee:
    async def test_the_first_current_row_is_accepted(self) -> None:
        row = await _command(_spec(ONE_CURRENT)).create(_FactCreate(root_id="r1"))

        assert row.root_id == "r1"

    async def test_a_second_current_row_is_refused(self) -> None:
        command = _command(_spec(ONE_CURRENT))
        first = await command.create(_FactCreate(root_id="r1"))

        with pytest.raises(CoreException) as caught:
            await command.create(_FactCreate(root_id="r1"))

        details = caught.value.details or {}

        assert caught.value.kind.value == "conflict"
        assert str(first.id) in caught.value.summary
        assert details["guarantee"] == "unique_together"
        assert details["fields"] == ["root_id"]

    async def test_a_duplicate_outside_the_filter_is_accepted(self) -> None:
        # The whole reason `where` exists: history rows share the tuple and only one of them
        # is current. A guarantee that refused these would make the shape unstorable.
        command = _command(_spec(ONE_CURRENT))
        await command.create(_FactCreate(root_id="r1"))
        row = await command.create(_FactCreate(root_id="r1", is_current=False))

        assert row.is_current is False

    async def test_a_different_tuple_is_accepted(self) -> None:
        command = _command(_spec(ONE_CURRENT))
        await command.create(_FactCreate(root_id="r1"))
        row = await command.create(_FactCreate(root_id="r2"))

        assert row.root_id == "r2"

    async def test_no_guarantee_means_no_refusal(self) -> None:
        # The contrast that makes the legs above about the guarantee rather than about the mock.
        command = _command(_spec())
        await command.create(_FactCreate(root_id="r1"))
        row = await command.create(_FactCreate(root_id="r1"))

        assert row.root_id == "r1"


# ....................... #


class TestEveryWritePathIsCovered:
    """One leg per path that can carry a row into or out of the guarantee's selection."""

    async def test_update_that_moves_a_row_into_the_filter(self) -> None:
        command = _command(_spec(ONE_CURRENT))
        await command.create(_FactCreate(root_id="r1"))
        stale = await command.create(_FactCreate(root_id="r1", is_current=False))

        with pytest.raises(CoreException, match="at most one row per"):
            await command.update(stale.id, stale.rev, _FactUpdate(is_current=True))

    async def test_update_within_the_filter_that_changes_nothing_relevant(self) -> None:
        command = _command(_spec(ONE_CURRENT))
        row = await command.create(_FactCreate(root_id="r1"))
        updated = await command.update(row.id, row.rev, _FactUpdate(label="renamed"))

        assert updated.label == "renamed"

    async def test_ensure_is_covered(self) -> None:
        command = _command(_spec(ONE_CURRENT))
        await command.create(_FactCreate(root_id="r1"))

        with pytest.raises(CoreException, match="at most one row per"):
            await command.ensure(uuid4(), _FactCreate(root_id="r1"))

    async def test_upsert_is_covered(self) -> None:
        command = _command(_spec(ONE_CURRENT))
        await command.create(_FactCreate(root_id="r1"))

        with pytest.raises(CoreException, match="at most one row per"):
            await command.upsert(uuid4(), _FactCreate(root_id="r1"), _FactUpdate())

    async def test_create_many_is_covered(self) -> None:
        command = _command(_spec(ONE_CURRENT))

        with pytest.raises(CoreException, match="at most one row per"):
            await command.create_many([_FactCreate(root_id="r1"), _FactCreate(root_id="r1")])

    async def test_update_many_is_covered(self) -> None:
        command = _command(_spec(ONE_CURRENT))
        await command.create(_FactCreate(root_id="r1"))
        stale = await command.create(_FactCreate(root_id="r1", is_current=False))

        with pytest.raises(CoreException, match="at most one row per"):
            await command.update_many(
                [KeyedUpdate(id=stale.id, rev=stale.rev, dto=_FactUpdate(is_current=True))],
            )

    async def test_restore_brings_a_row_back_into_the_filter(self) -> None:
        # An un-delete is a write that can create a conflict, and it is the path a reader is
        # least likely to think of. The guarantee excludes deleted rows, which is what lets a
        # replacement be created at all; restoring the original then puts two live rows on one
        # tuple, and nothing but this path can catch it.
        live_only = UniqueTogether(fields=("root_id",), where={"$values": {"is_deleted": False}})
        command = _command(_erasable_spec(live_only))
        first = await command.create(_ErasableCreate(root_id="r1"))
        deleted = await command.delete(first.id, first.rev)

        replacement = await command.create(_ErasableCreate(root_id="r1"))

        assert replacement.root_id == "r1"

        with pytest.raises(CoreException, match="at most one row per"):
            await command.restore(first.id, deleted.rev)

    async def test_an_unfiltered_guarantee_keeps_a_deleted_row_s_tuple(self) -> None:
        # The consequence a consumer has to know about: with no `where`, a soft-deleted row is
        # still a row, so its tuple stays reserved and no replacement can be created. Whether
        # that is wanted is the consumer's call — which is why `where` exists.
        command = _command(_erasable_spec(UniqueTogether(fields=("root_id",))))
        first = await command.create(_ErasableCreate(root_id="r1"))
        await command.delete(first.id, first.rev)

        with pytest.raises(CoreException, match="at most one row per"):
            await command.create(_ErasableCreate(root_id="r1"))

    async def test_touch_leaves_a_satisfied_guarantee_satisfied(self) -> None:
        # The path that cannot violate anything, asserted so the check is not accidentally
        # refusing a row against itself — the commonest way an enforcement like this is wrong.
        command = _command(_spec(ONE_CURRENT))
        row = await command.create(_FactCreate(root_id="r1"))

        await command.touch(row.id, return_new=False)

        assert (await command.get(row.id)).root_id == "r1"

    async def test_updating_a_row_does_not_conflict_with_itself(self) -> None:
        command = _command(_spec(ONE_EVER))
        row = await command.create(_FactCreate(root_id="r1"))
        updated = await command.update(row.id, row.rev, _FactUpdate(label="same tuple"))

        assert updated.root_id == "r1"


# ....................... #


class TestSkipNull:
    """A real ``None``, not an empty string.

    The first version of this class used ``label=""`` and read as though it tested nulls; a
    sabotage that applied ``skip_null`` unconditionally passed all of it, because no value was
    ever null. Both directions are pinned below, against a field that can actually hold one.
    """

    async def test_two_nulls_collide_by_default(self) -> None:
        # The stricter reading, and the default: two rows with no supersedes_id are two rows
        # sharing a tuple.
        command = _command(_spec(UniqueTogether(fields=("supersedes_id",))))
        await command.create(_FactCreate(root_id="r1"))

        with pytest.raises(CoreException, match="at most one row per"):
            await command.create(_FactCreate(root_id="r2"))

    async def test_skip_null_exempts_two_nulls(self) -> None:
        command = _command(_spec(UniqueTogether(fields=("supersedes_id",), skip_null=True)))
        await command.create(_FactCreate(root_id="r1"))
        second = await command.create(_FactCreate(root_id="r2"))

        assert second.supersedes_id is None

    async def test_skip_null_still_refuses_two_equal_values(self) -> None:
        # The other half: exempting nulls must not exempt everything.
        command = _command(_spec(UniqueTogether(fields=("supersedes_id",), skip_null=True)))
        await command.create(_FactCreate(root_id="r1", supersedes_id="s1"))

        with pytest.raises(CoreException, match="at most one row per"):
            await command.create(_FactCreate(root_id="r2", supersedes_id="s1"))

    async def test_a_partly_null_tuple_is_exempt_under_skip_null(self) -> None:
        # One null in a two-field tuple is enough, which is the semantic a partial index gives.
        command = _command(
            _spec(UniqueTogether(fields=("root_id", "supersedes_id"), skip_null=True))
        )
        await command.create(_FactCreate(root_id="r1"))
        second = await command.create(_FactCreate(root_id="r1"))

        assert second.root_id == "r1"


# ....................... #


class TestAnUnenforcedMemberNeverReachesAWrite:
    async def test_declaring_non_overlapping_refuses_at_resolution(self) -> None:
        # Nothing maps it yet, so reconciliation must stop it before a write path meets an
        # arm it cannot serve. This is what keeps the unenforced arm unreachable rather than
        # silently permissive.
        spec = _spec(NonOverlapping(key=("root_id",), period=("label", "root_id")))

        with pytest.raises(CoreException) as caught:
            _command(spec)

        assert caught.value.code == "storage_guarantee_unsupported"
