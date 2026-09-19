"""A backend that cannot serialize writes per key refuses the declaration.

Which is every backend with a real connection today: the member is declared, the in-memory store
keeps it, and no integration adapter maps it yet. That is the honest state rather than a gap —
a spec claiming its writes are serialized while they are not is worse than one claiming nothing,
so the refusal is the feature until a mapping lands.
"""

from __future__ import annotations

import pytest

from forze.application.contracts.guarantees import (
    FULL_STORAGE_GUARANTEES,
    GUARANTEE_UNSUPPORTED,
    SerializedBy,
    StorageGuaranteeCapabilities,
    validate_storage_guarantees,
)
from forze.base.exceptions import CoreException
from forze_mock.adapters.document import MockDocumentAdapter
from forze_mongo.adapters.document import MongoDocumentAdapter
from forze_postgres.adapters.document import PostgresDocumentAdapter

# ----------------------- #

BY_OWNER = SerializedBy(key=("employee_id",))


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
