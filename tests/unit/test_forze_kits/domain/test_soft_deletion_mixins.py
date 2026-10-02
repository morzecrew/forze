from uuid import UUID, uuid4

import pytest

from forze import build_runtime
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import JsonDict
from forze.domain.models import CreateDocumentCmd, ReadDocument
from forze_kits.domain.soft_deletion import (
    DocWithSoftDeletion,
    SoftDeletionMixin,
    UpdateCmdWithSoftDeletion,
)
from forze_kits.domain.soft_deletion.constants import SOFT_DELETE_FIELD
from forze_kits.domain.versioned import DocWithVersioning
from forze_mock import MockDepsModule


class SoftDoc(SoftDeletionMixin): ...

def test_soft_deletion_mixin_defaults_to_not_deleted() -> None:
    doc = SoftDoc()
    assert doc.is_deleted is False

def test_soft_deletion_validator_blocks_non_soft_delete_updates_for_deleted_doc() -> (
    None
):
    before = SoftDoc(is_deleted=True)
    after = SoftDoc(is_deleted=True)
    diff: JsonDict = {"other": 1}

    with pytest.raises(CoreException):
        SoftDoc._validate_soft_deletion(before, after, diff)  # type: ignore[misc]

def test_soft_deletion_validator_allows_soft_delete_only_update() -> None:
    before = SoftDoc(is_deleted=True)
    after = SoftDoc(is_deleted=True)
    diff: JsonDict = {SOFT_DELETE_FIELD: True}

    SoftDoc._validate_soft_deletion(before, after, diff)  # type: ignore[misc]

def test_soft_deletion_validator_allows_is_deleted_with_last_update_at() -> None:
    before = SoftDoc(is_deleted=True)
    after = SoftDoc(is_deleted=False)
    diff: JsonDict = {SOFT_DELETE_FIELD: False, "last_update_at": "2025-01-01T00:00:00Z"}

    SoftDoc._validate_soft_deletion(before, after, diff)  # type: ignore[misc]


# ....................... #
# Companion fields: what else a deleted row's restore may change alongside the flag.


class Part(DocWithSoftDeletion):
    soft_delete_companions = frozenset({"deleted_with"})

    title: str = ""
    deleted_with: UUID | None = None


class PartCreate(CreateDocumentCmd):
    title: str


class PartUpdate(UpdateCmdWithSoftDeletion):
    title: str | None = None
    deleted_with: UUID | None = None


class PartRead(ReadDocument):
    title: str = ""
    is_deleted: bool = False
    deleted_with: UUID | None = None


class TestCompanionFields:
    def test_a_companion_changes_with_the_flag(self) -> None:
        before = Part(is_deleted=True, deleted_with=uuid4())
        diff: JsonDict = {SOFT_DELETE_FIELD: False, "deleted_with": None}

        Part._validate_soft_deletion(before, before, diff)  # type: ignore[misc]

    def test_a_companion_alone_is_still_refused(self) -> None:
        # Companions ride along with a delete or a restore; they are not editable on their own.
        before = Part(is_deleted=True, deleted_with=uuid4())

        with pytest.raises(CoreException):
            Part._validate_soft_deletion(before, before, {"deleted_with": None})  # type: ignore[misc]

    def test_a_companion_alone_is_refused_on_a_live_row_too(self) -> None:
        # A live row's marker is no more editable than a deleted row's: only the write that
        # flips the flag sets or clears it.
        before = Part(title="bolt")

        with pytest.raises(CoreException, match="deleted_with"):
            Part._validate_soft_deletion(  # type: ignore[misc]
                before, before, {"deleted_with": str(uuid4())}
            )

    def test_a_delete_sets_a_companion_on_a_live_row(self) -> None:
        before = Part(title="bolt")

        Part._validate_soft_deletion(  # type: ignore[misc]
            before, before, {SOFT_DELETE_FIELD: True, "deleted_with": str(uuid4())}
        )

    def test_a_live_row_still_takes_ordinary_edits(self) -> None:
        before = Part(title="bolt")

        Part._validate_soft_deletion(before, before, {"title": "nut"})  # type: ignore[misc]

    def test_an_undeclared_field_is_still_refused(self) -> None:
        before = Part(is_deleted=True)

        with pytest.raises(CoreException):
            Part._validate_soft_deletion(  # type: ignore[misc]
                before, before, {SOFT_DELETE_FIELD: False, "title": "x"}
            )

    def test_a_model_declares_none_by_default(self) -> None:
        before = SoftDoc(is_deleted=True)

        with pytest.raises(CoreException):
            SoftDoc._validate_soft_deletion(  # type: ignore[misc]
                before, before, {SOFT_DELETE_FIELD: False, "deleted_with": None}
            )

    def test_a_subclass_inherits_the_declaration(self) -> None:
        class Gear(Part): ...

        before = Gear(is_deleted=True, deleted_with=uuid4())

        Gear._validate_soft_deletion(  # type: ignore[misc]
            before, before, {SOFT_DELETE_FIELD: False, "deleted_with": None}
        )

    def test_a_list_declaration_works_like_a_set(self) -> None:
        # Normalised at class creation, so the first restore does not trip over its type.
        class Listed(DocWithSoftDeletion):
            soft_delete_companions = ["deleted_with"]  # type: ignore[assignment]  # noqa: RUF012

            deleted_with: UUID | None = None

        before = Listed(is_deleted=True, deleted_with=uuid4())

        Listed._validate_soft_deletion(  # type: ignore[misc]
            before, before, {SOFT_DELETE_FIELD: False, "deleted_with": None}
        )
        assert Listed.soft_delete_companions == frozenset({"deleted_with"})

    def test_a_declaration_without_classvar_is_refused(self) -> None:
        # Annotated without ClassVar it would be a stored field, so each row's own data would
        # decide what its deleted self may change.
        with (
            pytest.raises(CoreException, match="ClassVar") as ei,
            pytest.warns(UserWarning, match="shadows an attribute"),
        ):

            class Stored(DocWithSoftDeletion):
                soft_delete_companions: frozenset[str] = frozenset({"deleted_with"})

                deleted_with: UUID | None = None

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    @pytest.mark.parametrize("declared", [None, 3, [1]], ids=["none", "int", "non-str-name"])
    def test_a_declaration_that_is_not_a_set_of_names_is_refused(self, declared: object) -> None:
        with pytest.raises(CoreException, match="field names") as ei:
            type(
                "Odd",
                (DocWithSoftDeletion,),
                {"soft_delete_companions": declared, "__module__": __name__},
            )

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_a_bare_string_is_refused(self) -> None:
        with pytest.raises(CoreException, match="field names") as ei:

            class Bare(DocWithSoftDeletion):
                soft_delete_companions = "deleted_with"  # type: ignore[assignment]

                deleted_with: UUID | None = None

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_a_companion_the_model_lacks_is_refused_at_class_creation(self) -> None:
        with pytest.raises(CoreException, match="deleted_by") as ei:

            class Broken(DocWithSoftDeletion):
                soft_delete_companions = frozenset({"deleted_by"})

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    async def test_a_restore_clears_its_companion_in_one_write(self) -> None:
        spec = DocumentSpec(
            name="parts",
            read=PartRead,
            write=DocumentWriteTypes(domain=Part, create_cmd=PartCreate, update_cmd=PartUpdate),
        )
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            cmd = runtime.get_context().document.command(spec)
            part = await cmd.create(PartCreate(title="bolt"))
            deleted = await cmd.update(
                part.id, part.rev, PartUpdate(is_deleted=True, deleted_with=uuid4())
            )

            restored = await cmd.update(
                part.id, deleted.rev, PartUpdate(is_deleted=False, deleted_with=None)
            )

            assert (restored.is_deleted, restored.deleted_with) == (False, None)

    async def test_an_update_cannot_forge_a_marker_on_a_live_row(self) -> None:
        spec = DocumentSpec(
            name="parts",
            read=PartRead,
            write=DocumentWriteTypes(domain=Part, create_cmd=PartCreate, update_cmd=PartUpdate),
        )
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            cmd = runtime.get_context().document.command(spec)
            part = await cmd.create(PartCreate(title="bolt"))

            with pytest.raises(CoreException) as ei:
                await cmd.update(part.id, part.rev, PartUpdate(deleted_with=uuid4()))

            assert ei.value.kind is ExceptionKind.DOMAIN
            assert (await cmd.get(part.id)).deleted_with is None


# ....................... #
# A versioned fact keeps what a version asserts; its companions still move with the flag.


class Meter(DocWithVersioning, SoftDeletionMixin):
    soft_delete_companions = frozenset({"deleted_with"})

    reading: int = 0
    deleted_with: UUID | None = None


def _meter(**fields: object) -> Meter:
    root = uuid4()
    return Meter(id=root, root_id=root, version=1, **fields)


class TestCompanionsOnAVersionedFact:
    def test_a_delete_sets_the_marker_in_one_write(self) -> None:
        deleted, diff = _meter().update({SOFT_DELETE_FIELD: True, "deleted_with": uuid4()})

        assert deleted.is_deleted and deleted.deleted_with is not None
        assert {SOFT_DELETE_FIELD, "deleted_with"} <= set(diff)

    def test_a_restore_clears_the_marker_in_one_write(self) -> None:
        before = _meter(is_deleted=True, deleted_with=uuid4())

        restored, _ = before.update({SOFT_DELETE_FIELD: False, "deleted_with": None})

        assert (restored.is_deleted, restored.deleted_with) == (False, None)

    def test_the_marker_alone_is_still_refused(self) -> None:
        with pytest.raises(CoreException):
            _meter().update({"deleted_with": uuid4()})

    def test_what_a_version_asserts_is_still_refused_with_the_flag(self) -> None:
        # The companion carve-out is for the declared fields, not for anything riding a delete.
        with pytest.raises(CoreException, match="correct"):
            _meter().update({SOFT_DELETE_FIELD: True, "reading": 7})
