"""The vocabulary, and the reconciliation that refuses a guarantee no store can keep."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any
from uuid import UUID, uuid4

import attrs
import pytest
from pydantic import (
    AfterValidator,
    PlainSerializer,
    field_serializer,
    field_validator,
    model_serializer,
    model_validator,
)

from forze.application.contracts.deps.proxy import PortProxy
from forze.application.contracts.guarantees import (
    FULL_STORAGE_GUARANTEES,
    GUARANTEE_UNSUPPORTED,
    NonOverlapping,
    Reference,
    SerializedBy,
    StorageGuaranteeCapabilities,
    UniqueTogether,
    capabilities_of,
    guarantees_of,
    implied_guarantees_of,
    validate_storage_guarantees,
)
from forze.application.contracts.document.specs import serialization_keys
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import advisory_lock_key
from forze.domain.models import Document

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

    def test_a_wrapped_port_is_read_for_its_backend(self) -> None:
        # Tracing, OTel spans, interceptors and a resilience policy all wrap a port the same
        # way; a protocol check of the wrapper never sees the declaration it forwards.
        assert capabilities_of(PortProxy(inner=PortProxy(inner=_Full()))) is FULL_STORAGE_GUARANTEES

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

    def test_an_implied_guarantee_names_the_opt_out_on_the_model(self) -> None:
        # Nothing in the spec to drop: the model put it there, and the model is where it goes.
        with pytest.raises(CoreException) as caught:
            validate_storage_guarantees(
                (NO_OVERLAP, ANY_ROW),
                _Bare(),
                spec_name="fact",
                backend="void",
                declared=(NO_OVERLAP,),
            )

        assert "unique_together needs uniqueness over a field tuple (implied" in (
            caught.value.summary
        )
        assert "`implied_guarantees = ()`" in caught.value.summary
        assert "non_overlapping needs non-overlap of periods per key\n" in caught.value.summary

    def test_a_declared_guarantee_does_not(self) -> None:
        with pytest.raises(CoreException) as caught:
            validate_storage_guarantees(
                (ANY_ROW,), _Bare(), spec_name="fact", backend="void", declared=(ANY_ROW,)
            )

        assert "implied" not in caught.value.summary


# ....................... #


class TestWhatAModelImplies:
    def test_a_model_without_the_attribute_implies_nothing(self) -> None:
        assert implied_guarantees_of(Document) == ()

    def test_a_tuple_of_guarantees_is_read(self) -> None:
        class Numbered:
            implied_guarantees = (ANY_ROW,)

        assert implied_guarantees_of(Numbered) == (ANY_ROW,)

    @pytest.mark.parametrize(
        "value",
        [ANY_ROW, [ANY_ROW], (ANY_ROW, "root_id")],
        ids=["bare guarantee", "list", "stray item"],
    )
    def test_anything_else_is_refused_naming_the_model(self, value: object) -> None:
        class Numbered:
            implied_guarantees = value

        with pytest.raises(CoreException) as caught:
            implied_guarantees_of(Numbered)

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.code == "implied_guarantees_invalid"
        assert "Numbered.implied_guarantees" in caught.value.summary


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
        UniqueTogether(
            fields=("a",), where={"$values": {"x": 1}}, skip_null=True, holds="commit"
        ),
    )

    @staticmethod
    def _validate(guarantee: object, port: object, backend: str) -> None:
        validate_storage_guarantees((guarantee,), port, spec_name="items", backend=backend)  # type: ignore[arg-type]

    @pytest.mark.parametrize("guarantee", PARTIAL_AT_COMMIT, ids=["where", "where-skip_null"])
    def test_a_partial_uniqueness_at_commit_is_refused_on_postgres(
        self, guarantee: UniqueTogether
    ) -> None:
        from forze_postgres.adapters.document import PostgresDocumentAdapter

        with pytest.raises(CoreException) as caught:
            self._validate(guarantee, PostgresDocumentAdapter, "postgres")

        assert caught.value.code == GUARANTEE_UNSUPPORTED
        assert "a partial uniqueness" in caught.value.summary
        assert "holding only at commit" not in caught.value.summary

    @pytest.mark.parametrize("guarantee", PARTIAL_AT_COMMIT, ids=["where", "where-skip_null"])
    def test_the_same_declaration_is_kept_by_the_mock(self, guarantee: UniqueTogether) -> None:
        from forze_mock.adapters.document import MockDocumentAdapter

        self._validate(guarantee, MockDocumentAdapter, "mock")

    def test_postgres_keeps_the_forms_it_has_a_constraint_for(self) -> None:
        from forze_postgres.adapters.document import PostgresDocumentAdapter

        for guarantee in (
            UniqueTogether(fields=("a", "b"), holds="commit"),
            # A plain unique constraint already treats a tuple holding a null as distinct, so
            # the exemption needs no predicate and the constraint can be deferred.
            UniqueTogether(fields=("a", "b"), skip_null=True, holds="commit"),
            NonOverlapping(
                key=("a",), period=("s", "e"), where={"$values": {"x": 1}}, holds="commit"
            ),
        ):
            self._validate(guarantee, PostgresDocumentAdapter, "postgres")

    def test_skip_null_at_commit_is_partial_unless_the_store_says_otherwise(self) -> None:
        # Where a plain uniqueness counts nulls as values, the exemption is a predicate, and a
        # store that cannot defer a partial uniqueness cannot defer this one either.
        store = StorageGuaranteeCapabilities(
            unique_together=True,
            unique_together_skip_null=True,
            checks_deferred_to_commit=True,
        )
        at_commit = UniqueTogether(fields=("a",), skip_null=True, holds="commit")

        assert store.unmet(at_commit) == (
            "a partial uniqueness (`where` or `skip_null`) that holds only at commit",
        )
        assert (
            attrs.evolve(store, unique_together_skip_null_at_commit=True).unmet(at_commit) == ()
        )

    def test_mongo_checks_every_write_and_cannot_defer(self) -> None:
        from forze_mongo.adapters.document import MongoDocumentAdapter

        with pytest.raises(CoreException) as caught:
            self._validate(
                UniqueTogether(fields=("a",), holds="commit"), MongoDocumentAdapter, "mongo"
            )

        assert "holding only at commit" in caught.value.summary


# ....................... #


class _Line(Document):
    order_id: UUID
    sku: str


BY_ORDER = SerializedBy(key=("order_id",))
BY_ORDER_SKU = SerializedBy(key=("sku", "order_id"))


class TestTheKeysAHoldTakes:
    def test_it_is_the_key_a_write_for_the_owner_derives(self) -> None:
        order = uuid4()

        keys = serialization_keys(
            [BY_ORDER], [{"order_id": order}], domain=_Line, scope="lines", tenant=None
        )

        assert keys == [advisory_lock_key("lines", None, "order_id", order)]

    def test_a_value_is_validated_against_the_field_type(self) -> None:
        # A string id would derive another key and lock an owner no write ever takes.
        order = uuid4()

        def keys(value: object) -> list[int]:
            return serialization_keys(
                [BY_ORDER], [{"order_id": value}], domain=_Line, scope="lines", tenant=None
            )

        assert keys(str(order)) == keys(order)

    def test_a_constraint_on_the_field_applies_as_it_does_to_the_domain(self) -> None:
        class Shouting(Document):
            owner: Annotated[str, AfterValidator(str.upper)]

        by_owner = SerializedBy(key=("owner",))
        held = serialization_keys(
            [by_owner], [{"owner": "abc"}], domain=Shouting, scope="s", tenant=None
        )

        assert held == [advisory_lock_key("s", None, "owner", Shouting(owner="abc").owner)]

    @pytest.mark.parametrize(
        "name",
        [
            "datetime",
            "datetime alias",
            "field validator",
            "every field",
            "model",
            "plain serializer",
            "field serializer",
            "model serializer",
        ],
    )
    def test_a_key_a_write_derives_otherwise_is_refused(self, name: str) -> None:
        # A write keys the value after the domain's own validators, a stored row in the form its
        # serializers store, and a stored instant as the database returns it in the session's
        # time zone: a hold could lock nobody's owner.
        type Stamp = datetime

        class AtInstant(Document):
            owner: datetime | None = None

        class AtAlias(Document):
            owner: Stamp

        class PlainSerialized(Document):
            owner: Annotated[UUID, PlainSerializer(str)]

        class FieldSerialized(Document):
            owner: str

            @field_serializer("owner")
            def _upper(self, value: str) -> str:
                return value.upper()

        class ModelSerialized(Document):
            owner: str

            @model_serializer(mode="wrap")
            def _same(self, handler: Any) -> Any:
                return handler(self)

        class Validated(Document):
            owner: str

            @field_validator("owner")
            @classmethod
            def _lower(cls, value: str) -> str:
                return value.lower()

        class EveryField(Document):
            owner: str

            @field_validator("*")
            @classmethod
            def _same(cls, value: object) -> object:
                return value

        class Modelled(Document):
            owner: str

            @model_validator(mode="after")
            def _same(self) -> "Modelled":
                return self

        domain, value = {
            "datetime": (AtInstant, datetime.now(UTC)),
            "datetime alias": (AtAlias, datetime.now(UTC)),
            "field validator": (Validated, "A"),
            "every field": (EveryField, "a"),
            "model": (Modelled, "a"),
            "plain serializer": (PlainSerialized, uuid4()),
            "field serializer": (FieldSerialized, "a"),
            "model serializer": (ModelSerialized, "a"),
        }[name]

        with pytest.raises(CoreException) as refused:
            serialization_keys(
                [SerializedBy(key=("owner",))],
                [{"owner": value}],
                domain=domain,
                scope="s",
                tenant=None,
            )

        assert refused.value.kind is ExceptionKind.CONFIGURATION
        assert "'owner'" in refused.value.summary or "has a model" in refused.value.summary

    def test_a_recursive_alias_without_an_instant_is_held(self) -> None:
        type Path = str | list[Path]

        class Nested(Document):
            owner: Path

        by_owner = SerializedBy(key=("owner",))

        assert serialization_keys(
            [by_owner], [{"owner": "a"}], domain=Nested, scope="s", tenant=None
        ) == [advisory_lock_key("s", None, "owner", "a")]

    def test_fields_match_in_any_order_and_values_follow_the_declaration(self) -> None:
        order = uuid4()

        keys = serialization_keys(
            [BY_ORDER, BY_ORDER_SKU],
            [{"order_id": order, "sku": "a"}, {"order_id": order}, {"order_id": order}],
            domain=_Line,
            scope="lines",
            tenant=None,
        )

        assert keys == sorted(
            {
                advisory_lock_key("lines", None, "sku", "order_id", "a", order),
                advisory_lock_key("lines", None, "order_id", order),
            }
        )

    @pytest.mark.parametrize(
        ("declarations", "owners"),
        [
            ((), [{"order_id": uuid4()}]),
            ((BY_ORDER,), []),
            ((BY_ORDER,), [{"sku": "a"}]),
            ((BY_ORDER,), [{"order_id": uuid4(), "sku": "a"}]),
        ],
        ids=["nothing declared", "no owner", "another field", "a wider key"],
    )
    def test_what_it_cannot_lock_is_refused(
        self, declarations: tuple[SerializedBy, ...], owners: list[dict[str, object]]
    ) -> None:
        with pytest.raises(CoreException) as refused:
            serialization_keys(declarations, owners, domain=_Line, scope="lines", tenant=None)

        assert refused.value.kind is ExceptionKind.CONFIGURATION


# ....................... #


class TestReference:
    def test_an_unknown_delete_action_is_refused(self) -> None:
        # Read from configuration, a misspelt action would otherwise be enforced as none.
        with pytest.raises(CoreException, match="on_delete"):
            Reference(field="stage_id", spec="stages", on_delete="delete")  # type: ignore[arg-type]

    @pytest.mark.parametrize(("field", "spec"), [("", "stages"), ("stage_id", "")])
    def test_a_reference_names_both_a_field_and_a_spec(self, field: str, spec: str) -> None:
        with pytest.raises(CoreException, match="both a field and a spec"):
            Reference(field=field, spec=spec, on_delete="cascade")

    def test_a_spec_object_for_its_name_is_refused(self) -> None:
        # Converted to text it would read as its repr and never match a row.
        with pytest.raises(CoreException, match="must be strings"):
            Reference(field="stage_id", spec=_Spec(), on_delete="cascade")  # type: ignore[arg-type]

    def test_a_str_enum_name_reads_as_its_value(self) -> None:
        class _Names(StrEnum):
            STAGES = "stages"

        reference = Reference(field="stage_id", spec=_Names.STAGES, on_delete="cascade")

        assert type(reference.spec) is str
        assert reference.spec == "stages"

    def test_the_mock_applies_it(self) -> None:
        reference = Reference(field="stage_id", spec="stages", on_delete="cascade")

        assert FULL_STORAGE_GUARANTEES.unmet(reference) == ()

    def test_a_store_that_cannot_apply_it_is_refused_at_wiring(self) -> None:
        reference = Reference(field="stage_id", spec="stages", on_delete="restrict")

        with pytest.raises(CoreException) as caught:
            validate_storage_guarantees(
                (reference,), _Unfiltered(), spec_name="lines", backend="forze_mongo"
            )

        assert caught.value.code == GUARANTEE_UNSUPPORTED
        assert "reference needs applying a reference's `on_delete`" in caught.value.summary

    def test_field_is_the_shorthand_for_one_field_holding_the_id(self) -> None:
        # Either spelling reads back, and compares, the same.
        short = Reference(field="stage_id", spec="stages", on_delete="cascade")
        long = Reference(fields=("stage_id",), spec="stages", on_delete="cascade")

        assert short == long
        assert hash(short) == hash(long)
        assert (short.fields, short.references) == (("stage_id",), ("id",))

    def test_a_composite_reference_pairs_its_fields_with_the_parent_s(self) -> None:
        class _Names(StrEnum):
            SOURCE = "source_id"

        reference = Reference(
            fields=["stage_id", _Names.SOURCE],  # type: ignore[arg-type]  # as configuration reads
            spec="stages",
            references=("id", "source_id"),
            on_delete="cascade",
        )

        assert reference.fields == ("stage_id", "source_id")
        assert type(reference.fields[1]) is str
        assert reference.references == ("id", "source_id")
        assert reference.field is None

    def test_one_field_may_reference_another_parent_field(self) -> None:
        reference = Reference(field="code", references=("code",), spec="s", on_delete="cascade")

        assert (reference.fields, reference.references) == (("code",), ("code",))
        # Not one field holding the id, however it was spelt.
        assert reference.field is None

    def test_field_reads_the_one_field_holding_the_id(self) -> None:
        assert Reference(fields=("stage_id",), spec="s", on_delete="cascade").field == "stage_id"

    def test_evolve_changes_the_stored_fields_and_action(self) -> None:
        # `field` is not stored, so evolve passes the fields back rather than both spellings.
        reference = Reference(field="stage_id", spec="stages", on_delete="set_null")

        assert attrs.evolve(reference, on_delete="cascade") == Reference(
            field="stage_id", spec="stages", on_delete="cascade"
        )

        composite = attrs.evolve(
            reference, fields=("stage_id", "source_id"), references=("id", "source_id")
        )

        assert composite.field is None
        assert (composite.fields, composite.references) == (
            ("stage_id", "source_id"),
            ("id", "source_id"),
        )

        with pytest.raises(CoreException, match="not both"):
            attrs.evolve(reference, field="origin_id")

    @pytest.mark.parametrize(
        ("names", "match"),
        [
            ({"field": "stage_id", "fields": ("source_id",)}, "not both"),
            ({"field": "stage_id", "fields": ("stage_id", "source_id")}, "not both"),
            ({"fields": ()}, "both a field and a spec"),
            ({"fields": ("stage_id", "source_id")}, "names no references"),
            ({"fields": ("stage_id",), "references": ()}, "names no references"),
            ({"fields": ("a", "b"), "references": ("id",)}, "pairs 2 field"),
            ({"fields": ("a",), "references": ("id", "b")}, "pairs 1 field"),
            ({"field": "a", "references": ("id", "b")}, "pairs 1 field"),
            ({"fields": ("a", "a"), "references": ("id", "b")}, "'a' more than once"),
            ({"fields": ("a", "b"), "references": ("id", "id")}, "'id' more than once"),
            ({"fields": "stage_id"}, "must be a tuple of field names"),
            ({"fields": ("a", 1), "references": ("id", "b")}, "must be a tuple of field names"),
            ({"fields": ("a", ""), "references": ("id", "b")}, "must be a tuple of field names"),
            ({"field": 1}, "must be strings"),
        ],
    )
    def test_a_malformed_pairing_is_refused(self, names: dict[str, Any], match: str) -> None:
        with pytest.raises(CoreException, match=match) as caught:
            Reference(spec="s", on_delete="cascade", **names)

        assert caught.value.kind is ExceptionKind.CONFIGURATION

    def test_set_null_may_clear_some_fields_of_the_reference(self) -> None:
        # ON DELETE SET NULL (stage_id): the stage goes, the source stays.
        reference = Reference(
            fields=("stage_id", "source_id"),
            references=("id", "source_id"),
            spec="stages",
            on_delete="set_null",
            set_null_fields=["stage_id"],  # type: ignore[arg-type]  # as configuration reads
        )

        assert reference.set_null_fields == ("stage_id",)
        assert Reference(field="s", spec="x", on_delete="set_null").set_null_fields is None

    @pytest.mark.parametrize(
        ("on_delete", "cleared", "match"),
        [
            ("cascade", ("stage_id",), "but on_delete is 'cascade'"),
            ("restrict", ("stage_id",), "but on_delete is 'restrict'"),
            ("set_null", (), "names no field"),
            ("set_null", ("origin_id",), r"names \['origin_id'\], which are not among"),
            ("set_null", ("stage_id", "stage_id"), "'stage_id' more than once"),
            ("set_null", "stage_id", "must be a tuple of field names"),
        ],
    )
    def test_malformed_set_null_fields_are_refused(
        self, on_delete: Any, cleared: Any, match: str
    ) -> None:
        with pytest.raises(CoreException, match=match) as caught:
            Reference(
                fields=("stage_id", "source_id"),
                references=("id", "source_id"),
                spec="stages",
                on_delete=on_delete,
                set_null_fields=cleared,
            )

        assert caught.value.kind is ExceptionKind.CONFIGURATION
