"""A versioned aggregate cannot be served by Mongo, and the refusal says so at wiring.

This is the whole Mongo battery for correction lineage, and the absence of a behaviour battery is
the finding rather than a gap in it. A versioned aggregate declares
``UniqueTogether(("supersedes_id",), skip_null=True)`` — one successor per predecessor, exempting
the first versions that supersede nothing — and Mongo has no mechanism for the exemption. A sparse
index is the obvious candidate and does something else: it skips a document only when *every*
indexed field is missing, and it still indexes an explicit null, so the rows the exemption exists
to let through stay in the index and collide.

So reconciliation refuses when the port is built, which is the earliest and cheapest place to find
out. What is pinned here is that the refusal happens, that it names the axis, and that it is about
``skip_null`` specifically rather than about filtered uniqueness in general — because the day Mongo
grows a mechanism, this is the leg that should start failing.
"""

from __future__ import annotations

import pytest

from forze.application.contracts.guarantees import (
    GUARANTEE_UNSUPPORTED,
    UniqueTogether,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kits.aggregates.versioned import ONE_CURRENT_VERSION, ONE_SUCCESSOR
from forze_mongo.adapters.document import MongoDocumentAdapter

pytestmark = [pytest.mark.integration]

# ----------------------- #


class TestMongoCannotKeepTheLineageGuarantee:
    def test_the_successor_guarantee_is_unmet(self) -> None:
        unmet = MongoDocumentAdapter.storage_guarantees.unmet(ONE_SUCCESSOR)

        assert unmet == ("exempting rows whose tuple holds a null (`skip_null`)",)

    def test_the_current_version_guarantee_is_met(self) -> None:
        # The contrast, and the reason the refusal is specific: Mongo keeps a filtered
        # uniqueness perfectly well. Only the null exemption is missing.
        assert MongoDocumentAdapter.storage_guarantees.unmet(ONE_CURRENT_VERSION) == ()

    def test_the_same_tuple_without_the_exemption_is_fine(self) -> None:
        # And it is the exemption, not the field: uniqueness over `supersedes_id` counting nulls
        # as values is something Mongo can keep — it is just not what a chain needs, since every
        # first version would then collide on its null pointer.
        assert (
            MongoDocumentAdapter.storage_guarantees.unmet(
                UniqueTogether(fields=("supersedes_id",))
            )
            == ()
        )


# ....................... #


class TestTheRefusalReachesTheOperator:
    def test_it_carries_the_unsupported_code_and_names_the_axis(self) -> None:
        from forze.application.contracts.guarantees import validate_storage_guarantees

        with pytest.raises(CoreException) as caught:
            validate_storage_guarantees(
                (ONE_CURRENT_VERSION, ONE_SUCCESSOR),
                MongoDocumentAdapter,
                spec_name="readings",
                backend="forze_mongo",
            )

        assert caught.value.kind is ExceptionKind.PRECONDITION
        assert caught.value.code.endswith(GUARANTEE_UNSUPPORTED)
        assert "skip_null" in caught.value.summary
        assert "readings" in caught.value.summary
