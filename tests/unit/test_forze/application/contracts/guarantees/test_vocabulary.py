"""The vocabulary, and the reconciliation that refuses a guarantee no store can keep."""

import pytest

from forze.application.contracts.guarantees import (
    FULL_STORAGE_GUARANTEES,
    GUARANTEE_UNSUPPORTED,
    NonOverlapping,
    SerializedBy,
    StorageGuaranteeCapabilities,
    UniqueTogether,
    capabilities_of,
    guarantees_of,
    validate_storage_guarantees,
)
from forze.base.exceptions import CoreException

# ----------------------- #

CURRENT_ONLY = UniqueTogether(fields=("root_id",), where={"$values": {"is_current": True}})
ANY_ROW = UniqueTogether(fields=("root_id",))
NULLABLE = UniqueTogether(fields=("supersedes_id",), skip_null=True)
NO_OVERLAP = NonOverlapping(key=("employee_id",), period=("valid_from", "valid_to"), bounds="[]")


class _Bare:
    """A port that has never heard of guarantees."""


class _Full:
    storage_guarantees = FULL_STORAGE_GUARANTEES


class _Unfiltered:
    storage_guarantees = StorageGuaranteeCapabilities(unique_together=True)


class _Spec:
    name = "fact"
    guarantees = (CURRENT_ONLY,)


class _SpecWithout:
    name = "plain"


# ....................... #


class TestTheVocabularyRefusesNonsense:
    """Construction-time refusals, because a guarantee that says nothing is a mistake."""

    def test_uniqueness_over_no_field(self) -> None:
        with pytest.raises(CoreException, match="names no field"):
            UniqueTogether(fields=())

    def test_uniqueness_naming_a_field_twice(self) -> None:
        with pytest.raises(CoreException, match="more than once"):
            UniqueTogether(fields=("a", "b", "a"))

    def test_non_overlap_over_no_key(self) -> None:
        with pytest.raises(CoreException, match="names no key field"):
            NonOverlapping(key=(), period=("f", "t"))

    def test_non_overlap_with_one_field_for_both_endpoints(self) -> None:
        with pytest.raises(CoreException, match="for both endpoints"):
            NonOverlapping(key=("k",), period=("at", "at"))

    @pytest.mark.parametrize("period", [("only",), ("a", "b", "c")])
    def test_non_overlap_with_the_wrong_number_of_endpoints(
        self,
        period: tuple[str, ...],
    ) -> None:
        # The annotation says two, and a declaration read from configuration is a tuple at
        # runtime whatever the annotation says. Unpacked blindly it raises a bare `ValueError`,
        # which no configuration handler can classify and no operator can act on.
        with pytest.raises(CoreException, match="exactly a start and an end"):
            NonOverlapping(key=("k",), period=period)  # type: ignore[arg-type]

    def test_a_guarantee_is_frozen(self) -> None:
        with pytest.raises(AttributeError):
            CURRENT_ONLY.fields = ("other",)  # type: ignore[misc]


# ....................... #


class TestWhatAStoreCannotKeep:
    """`unmet` names the missing axis, not just that something is missing."""

    def test_the_superset_keeps_every_enforced_member(self) -> None:
        assert FULL_STORAGE_GUARANTEES.unmet(CURRENT_ONLY) == ()
        assert FULL_STORAGE_GUARANTEES.unmet(ANY_ROW) == ()
        assert FULL_STORAGE_GUARANTEES.unmet(NULLABLE) == ()

    def test_non_overlap_is_kept_too(self) -> None:
        # The in-memory store enforces it, so the superset keeps it. A member arrives in this
        # value only once a store maps it: one that reconciled and then failed at the first
        # write would be worse than one that refused.
        assert FULL_STORAGE_GUARANTEES.unmet(NO_OVERLAP) == ()

    def test_a_store_without_it_names_the_axis(self) -> None:
        # The refusal Mongo and every other unmapped store still gives.
        assert StorageGuaranteeCapabilities().unmet(NO_OVERLAP) == (
            "non-overlap of periods per key",
        )

    def test_a_store_with_nothing_names_every_axis(self) -> None:
        unmet = StorageGuaranteeCapabilities().unmet(CURRENT_ONLY)

        assert unmet == (
            "uniqueness over a field tuple",
            "uniqueness restricted to a subset of rows (`where`)",
        )

    def test_unfiltered_uniqueness_is_not_filtered_uniqueness(self) -> None:
        # The axis that matters most: a store with a plain unique index would otherwise pass
        # reconciliation for "one *current* row per fact" and fail at the first second row.
        assert _Unfiltered.storage_guarantees.unmet(ANY_ROW) == ()
        assert _Unfiltered.storage_guarantees.unmet(CURRENT_ONLY) == (
            "uniqueness restricted to a subset of rows (`where`)",
        )

    def test_skip_null_is_its_own_axis(self) -> None:
        assert _Unfiltered.storage_guarantees.unmet(NULLABLE) == (
            "exempting rows whose tuple holds a null (`skip_null`)",
        )


# ....................... #


class TestReadingBothHalves:
    def test_a_port_that_declares_nothing_enforces_nothing(self) -> None:
        assert capabilities_of(_Bare()) == StorageGuaranteeCapabilities()

    def test_a_port_that_declares_is_read(self) -> None:
        assert capabilities_of(_Full()) is FULL_STORAGE_GUARANTEES

    def test_a_spec_that_declares_nothing_requires_nothing(self) -> None:
        assert guarantees_of(_SpecWithout()) == ()

    def test_a_spec_that_declares_is_read(self) -> None:
        assert guarantees_of(_Spec()) == (CURRENT_ONLY,)


# ....................... #


class TestReconciliation:
    def test_a_store_that_can_keep_it_is_accepted(self) -> None:
        validate_storage_guarantees((CURRENT_ONLY,), _Full(), spec_name="fact", backend="mock")

    def test_no_guarantees_never_consults_the_port(self) -> None:
        # The inert case, which is every spec today: a port that would refuse everything is
        # not even asked.
        validate_storage_guarantees((), _Bare(), spec_name="plain", backend="nowhere")

    def test_a_store_that_cannot_is_refused_by_name(self) -> None:
        with pytest.raises(CoreException) as caught:
            validate_storage_guarantees((CURRENT_ONLY,), _Bare(), spec_name="fact", backend="void")

        assert caught.value.code == GUARANTEE_UNSUPPORTED
        assert "void" in caught.value.summary
        assert "fact" in caught.value.summary
        assert "uniqueness over a field tuple" in caught.value.summary

    def test_every_unmet_guarantee_is_reported_in_one_refusal(self) -> None:
        # One pass, whole gap: a deployment that learns one missing mechanism per restart
        # takes as many restarts as it has guarantees.
        with pytest.raises(CoreException) as caught:
            validate_storage_guarantees(
                (CURRENT_ONLY, NO_OVERLAP),
                _Bare(),
                spec_name="fact",
                backend="void",
            )

        assert "2 guarantee(s)" in caught.value.summary
        assert "unique_together" in caught.value.summary
        assert "non_overlapping" in caught.value.summary

    def test_the_refusal_carries_the_spec_and_backend_as_details(self) -> None:
        with pytest.raises(CoreException) as caught:
            validate_storage_guarantees(
                (NO_OVERLAP,), _Bare(), spec_name="shift", backend="void"
            )

        assert caught.value.details["spec"] == "shift"
        assert caught.value.details["backend"] == "void"


# ....................... #


class TestWhenAGuaranteeHolds:
    """``holds`` defaults to after every write; ``"commit"`` needs a store that says it can."""

    def test_every_member_holds_always_unless_told_otherwise(self) -> None:
        assert ANY_ROW.holds == "always"
        assert NO_OVERLAP.holds == "always"

    @pytest.mark.parametrize("holds", ["Commit", "deferred", "", None])
    def test_a_moment_outside_the_two_is_refused(self, holds: object) -> None:
        # From configuration, "Commit" would compare unequal to "commit" everywhere and quietly
        # behave as "always".
        for build in (
            lambda: UniqueTogether(fields=("a",), holds=holds),  # type: ignore[arg-type]
            lambda: NonOverlapping(key=("a",), period=("s", "e"), holds=holds),  # type: ignore[arg-type]
        ):
            with pytest.raises(CoreException) as caught:
                build()

            assert caught.value.kind.value == "configuration"

    def test_serialization_has_no_moment_to_hold_at(self) -> None:
        # It orders writes; it is not a state that can be inspected at a point in time.
        with pytest.raises(TypeError):
            SerializedBy(key=("a",), holds="commit")  # type: ignore[call-arg]

    def test_a_store_that_cannot_defer_names_it(self) -> None:
        store = StorageGuaranteeCapabilities(unique_together=True, non_overlapping=True)

        for guarantee in (
            UniqueTogether(fields=("a",), holds="commit"),
            NonOverlapping(key=("a",), period=("s", "e"), holds="commit"),
        ):
            assert store.unmet(guarantee) == ('holding only at commit (`holds="commit"`)',)

        assert store.unmet(ANY_ROW) == ()

    def test_the_superset_keeps_every_moment(self) -> None:
        for guarantee in (
            UniqueTogether(fields=("a",), where={"$values": {"x": 1}}, holds="commit"),
            UniqueTogether(fields=("a",), skip_null=True, holds="commit"),
            NonOverlapping(
                key=("a",), period=("s", "e"), where={"$values": {"x": 1}}, holds="commit"
            ),
        ):
            assert FULL_STORAGE_GUARANTEES.unmet(guarantee) == ()


class TestWiringAMomentAgainstEachBackend:
    """Reconciliation, not the vocabulary, refuses what one backend cannot keep — so a
    declaration the in-memory store honours is not forbidden everywhere."""

    PARTIAL_AT_COMMIT = (
        UniqueTogether(fields=("a",), where={"$values": {"x": 1}}, holds="commit"),
        UniqueTogether(fields=("a",), skip_null=True, holds="commit"),
    )

    @staticmethod
    def _validate(guarantee: object, port: object, backend: str) -> None:
        validate_storage_guarantees((guarantee,), port, spec_name="items", backend=backend)  # type: ignore[arg-type]

    @pytest.mark.parametrize("guarantee", PARTIAL_AT_COMMIT, ids=["where", "skip_null"])
    def test_a_partial_uniqueness_at_commit_is_refused_on_postgres(
        self, guarantee: UniqueTogether
    ) -> None:
        from forze_postgres.adapters.document import PostgresDocumentAdapter

        with pytest.raises(CoreException) as caught:
            self._validate(guarantee, PostgresDocumentAdapter, "postgres")

        assert caught.value.code == GUARANTEE_UNSUPPORTED
        assert "a partial uniqueness" in caught.value.summary
        assert "holding only at commit" not in caught.value.summary

    @pytest.mark.parametrize("guarantee", PARTIAL_AT_COMMIT, ids=["where", "skip_null"])
    def test_the_same_declaration_is_kept_by_the_mock(self, guarantee: UniqueTogether) -> None:
        from forze_mock.adapters.document import MockDocumentAdapter

        self._validate(guarantee, MockDocumentAdapter, "mock")

    def test_postgres_keeps_the_forms_it_has_a_constraint_for(self) -> None:
        from forze_postgres.adapters.document import PostgresDocumentAdapter

        for guarantee in (
            UniqueTogether(fields=("a", "b"), holds="commit"),
            NonOverlapping(
                key=("a",), period=("s", "e"), where={"$values": {"x": 1}}, holds="commit"
            ),
        ):
            self._validate(guarantee, PostgresDocumentAdapter, "postgres")

    def test_mongo_checks_every_write_and_cannot_defer(self) -> None:
        from forze_mongo.adapters.document import MongoDocumentAdapter

        with pytest.raises(CoreException) as caught:
            self._validate(
                UniqueTogether(fields=("a",), holds="commit"), MongoDocumentAdapter, "mongo"
            )

        assert "holding only at commit" in caught.value.summary
