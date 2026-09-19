"""Mongo refuses an effective-dated aggregate, and says why.

The deliberate half of the design: Mongo has no mechanism for non-overlap, so the guarantee is
unsatisfiable there rather than skipped, and the kit refuses at wiring. A refusal is the whole
value — a temporal aggregate whose overlap rule is unenforced reads as a guarantee and is not
one, which is the defect the aggregate was written to replace.
"""

from __future__ import annotations

from datetime import date

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.guarantees import (
    GUARANTEE_UNSUPPORTED,
    NonOverlapping,
    capabilities_of,
)
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, ReadDocument
from forze_kits.domain.temporal import CreateCmdWithTemporalFields, DocWithTemporal
from forze_mongo.adapters.document import MongoDocumentAdapter

# ----------------------- #

NO_OVERLAP = NonOverlapping(key=("employee_id",), period=("valid_from", "valid_to"), bounds="[]")


class Contract(DocWithTemporal):
    employee_id: str
    hours: int = 0


class ContractCreate(CreateCmdWithTemporalFields):
    employee_id: str
    hours: int = 0


class ContractUpdate(BaseDTO):
    hours: int | None = None


class ContractRead(ReadDocument):
    employee_id: str
    hours: int = 0
    valid_from: date
    valid_to: date | None = None


SPEC = DocumentSpec[ContractRead, Contract, ContractCreate, ContractUpdate](
    name="contracts",
    read=ContractRead,
    write=DocumentWriteTypes(domain=Contract, create_cmd=ContractCreate, update_cmd=ContractUpdate),
    guarantees=(NO_OVERLAP,),
)


# ....................... #


class TestMongoDoesNotClaimIt:
    def test_the_adapter_declares_no_non_overlap(self) -> None:
        declared = MongoDocumentAdapter.storage_guarantees

        assert declared.non_overlapping is False
        assert declared.unique_together is True

    def test_the_unmet_axis_is_named(self) -> None:
        # The refusal has to say *which* part is missing, so a reader knows this is "wire a
        # different backend" rather than "write another index".
        unmet = MongoDocumentAdapter.storage_guarantees.unmet(NO_OVERLAP)

        assert unmet == ("non-overlap of periods per key",)

    def test_reconciliation_refuses_the_spec(self) -> None:
        from forze.application.contracts.guarantees import validate_storage_guarantees

        class _Port:
            storage_guarantees = MongoDocumentAdapter.storage_guarantees

        with pytest.raises(CoreException) as caught:
            validate_storage_guarantees(
                SPEC.guarantees, _Port(), spec_name=str(SPEC.name), backend="mongo"
            )

        assert caught.value.code == GUARANTEE_UNSUPPORTED
        assert "non_overlapping" in caught.value.summary
        assert capabilities_of(_Port()).non_overlapping is False
