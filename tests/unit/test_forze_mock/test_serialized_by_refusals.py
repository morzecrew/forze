"""A backend that cannot serialize writes per key refuses the declaration.

Which is every backend with a real connection today: the member is declared, the in-memory store
keeps it, and no integration adapter maps it yet. That is the honest state rather than a gap —
a spec claiming its writes are serialized while they are not is worse than one claiming nothing,
so the refusal is the feature until a mapping lands.
"""

from __future__ import annotations

import pytest

from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.guarantees import (
    FULL_STORAGE_GUARANTEES,
    GUARANTEE_UNSUPPORTED,
    SerializedBy,
    StorageGuaranteeCapabilities,
    validate_storage_guarantees,
)
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock.adapters.document import MockDocumentAdapter
from forze_mongo.adapters.document import MongoDocumentAdapter
from forze_postgres.adapters.document import PostgresDocumentAdapter

# ----------------------- #

BY_OWNER = SerializedBy(key=("employee_id",))


class _Read(ReadDocument):
    employee_id: str
    note: str = ""


class _Domain(Document):
    employee_id: str
    note: str = ""


class _Create(CreateDocumentCmd):
    employee_id: str
    note: str = ""


class _Update(BaseDTO):
    note: str | None = None


class _Port:
    def __init__(self, declared: StorageGuaranteeCapabilities) -> None:
        self.storage_guarantees = declared


# ....................... #


class TestWhoClaimsIt:
    def test_the_in_memory_store_does(self) -> None:
        assert MockDocumentAdapter.storage_guarantees.serialized_by is True
        assert FULL_STORAGE_GUARANTEES.unmet(BY_OWNER) == ()

    @pytest.mark.parametrize(
        "adapter", [PostgresDocumentAdapter, MongoDocumentAdapter], ids=["postgres", "mongo"]
    )
    def test_no_integration_adapter_does_yet(self, adapter: type) -> None:
        # When one maps it, this leg is the one that has to change — which is the point: the
        # capability and the mapping move together or the declaration becomes a comment.
        assert adapter.storage_guarantees.serialized_by is False

    @pytest.mark.parametrize(
        "adapter", [PostgresDocumentAdapter, MongoDocumentAdapter], ids=["postgres", "mongo"]
    )
    def test_the_refusal_names_the_axis(self, adapter: type) -> None:
        with pytest.raises(CoreException) as caught:
            validate_storage_guarantees(
                (BY_OWNER,),
                _Port(adapter.storage_guarantees),
                spec_name="shifts",
                backend="integration",
            )

        assert caught.value.code == GUARANTEE_UNSUPPORTED
        assert "serializing writes per key" in caught.value.summary

    def test_a_store_that_claims_it_is_not_refused(self) -> None:
        # The contrast: the refusal is about the missing capability, not about the member.
        validate_storage_guarantees(
            (BY_OWNER,),
            _Port(StorageGuaranteeCapabilities(serialized_by=True)),
            spec_name="shifts",
            backend="claims-it",
        )


# ....................... #


class TestTheDeclarationRefusesWhatCannotBeAKey:
    def test_no_field_is_refused(self) -> None:
        with pytest.raises(CoreException, match="names no field"):
            SerializedBy(key=())

    def test_a_repeated_field_is_refused(self) -> None:
        with pytest.raises(CoreException, match="more than once"):
            SerializedBy(key=("employee_id", "employee_id"))


# ....................... #


class TestAnEncryptedOwnerCannotBeSerializedBy:
    """Serializing by a sealed field would serialize nothing at all.

    Each write seals its value under its own nonce, so two rows for one owner hold different
    bytes — and a key derived from the stored value would put the two writers on different
    locks. The declaration would read as a rule and keep none of it, which is why it is refused
    where a sort key or an indexed content field naming a sealed column already is.
    """

    def test_a_sealed_key_field_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            DocumentSpec[_Read, _Domain, _Create, _Update](
                name="shifts",
                read=_Read,
                write=DocumentWriteTypes(domain=_Domain, create_cmd=_Create, update_cmd=_Update),
                guarantees=(BY_OWNER,),
                encryption=FieldEncryption(encrypted="employee_id"),
            )

        assert "serializes writes by" in caught.value.summary
        assert "employee_id" in caught.value.summary

    def test_encrypting_another_field_is_fine(self) -> None:
        # The contrast: the refusal is about the key, not about encryption.
        spec = DocumentSpec[_Read, _Domain, _Create, _Update](
            name="shifts",
            read=_Read,
            write=DocumentWriteTypes(domain=_Domain, create_cmd=_Create, update_cmd=_Update),
            guarantees=(BY_OWNER,),
            encryption=FieldEncryption(encrypted="note"),
        )

        assert spec.guarantees == (BY_OWNER,)
