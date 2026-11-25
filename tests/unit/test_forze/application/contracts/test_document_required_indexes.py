"""``DocumentSpec.required_indexes``: indexes a spec relies on that are not guarantees
(RFC 0080 row 34)."""

import pytest
from pydantic import computed_field

from forze.application.contracts.document import DocumentIndex, DocumentSpec
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, Document, ReadDocument

pytestmark = pytest.mark.unit

# ----------------------- #


class _Account(Document):
    username: str
    email: str | None = None
    note: str = ""


class _Create(BaseDTO):
    username: str


class _Read(ReadDocument):
    username: str


def _spec(
    *indexes: DocumentIndex, write: bool = True
) -> DocumentSpec[_Read, _Account, _Create, BaseDTO]:
    return DocumentSpec(
        name="accounts",
        read=_Read,
        write={"domain": _Account, "create_cmd": _Create} if write else None,
        write_omit_fields=frozenset({"note"}) if write else frozenset(),
        required_indexes=indexes,
    )


def test_a_required_index_over_stored_fields_is_kept_as_declared() -> None:
    index = DocumentIndex(fields=("username",), unique=True)

    assert _spec(index).required_indexes == (index,)
    assert _spec().required_indexes == ()


@pytest.mark.parametrize(
    "index",
    [
        DocumentIndex(fields=("usernme",), unique=True),
        # Omitted from every write, so no column holds it.
        DocumentIndex(fields=("note",)),
    ],
    ids=["misspelled", "not-stored"],
)
def test_a_required_index_over_a_field_the_row_lacks_is_refused(index: DocumentIndex) -> None:
    with pytest.raises(CoreException) as caught:
        _spec(index)

    assert caught.value.code == "document_index_invalid"


def test_a_read_only_spec_declares_no_required_index() -> None:
    with pytest.raises(CoreException) as caught:
        _spec(DocumentIndex(fields=("username",)), write=False)

    assert caught.value.code == "document_index_invalid"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"fields": ()},
        {"fields": ("username", "username")},
        {"fields": ("email",), "skip_null": True},
    ],
    ids=["no-field", "repeated", "skip-null-not-unique"],
)
def test_a_malformed_index_is_refused_as_it_is_built(kwargs: dict[str, object]) -> None:
    with pytest.raises(CoreException):
        DocumentIndex(**kwargs)  # type: ignore[arg-type]


def test_a_materialized_field_may_be_indexed() -> None:
    # Materialized means persisted: its column exists, so an index over it is one a store
    # can keep. A computed field that is not materialized has no column and stays refused.
    class _Named(Document):
        first: str

        @computed_field  # type: ignore[prop-decorator]
        @property
        def full(self) -> str:
            return self.first

    class _NamedRead(ReadDocument):
        first: str

        @computed_field  # type: ignore[prop-decorator]
        @property
        def full(self) -> str:
            return self.first

    index = DocumentIndex(fields=("full",))

    def spec(materialized: frozenset[str]) -> DocumentSpec[_NamedRead, _Named, BaseDTO, BaseDTO]:
        return DocumentSpec(
            name="named",
            read=_NamedRead,
            write={"domain": _Named, "create_cmd": BaseDTO},
            materialized=materialized,
            required_indexes=(index,),
        )

    assert spec(frozenset({"full"})).required_indexes == (index,)

    with pytest.raises(CoreException) as caught:
        spec(frozenset())

    assert caught.value.code == "document_index_invalid"
