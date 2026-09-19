"""Unit tests for Mongo document index validation."""

from datetime import UTC, date, datetime
from decimal import Decimal
from enum import IntEnum, StrEnum
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from bson import Decimal128
from pydantic import BaseModel

from forze.application.contracts.guarantees import UniqueTogether
from forze.base.exceptions import CoreException
from forze_mongo.adapters.document import MongoDocumentAdapter
from forze_mongo.kernel.client import MongoClient
from forze_mongo.kernel.introspect import MongoIndexInfo, MongoIntrospector
from forze_mongo.kernel.validate_indexes import (
    MongoDocumentIndexSpec,
    _bson_key,  # pyright: ignore[reportPrivateUsage]
    _bson_types,  # pyright: ignore[reportPrivateUsage]
    _equalities,  # pyright: ignore[reportPrivateUsage]
    _guarantee_equalities,  # pyright: ignore[reportPrivateUsage]
    _mongosh,  # pyright: ignore[reportPrivateUsage]
    _require_guarantee_indexes,  # pyright: ignore[reportPrivateUsage]
    validate_mongo_document_indexes,
)


@pytest.mark.asyncio
async def test_validate_warns_on_secondary_unique() -> None:
    client = MagicMock(spec=MongoClient)
    client.list_indexes = AsyncMock(
        return_value=[
            {"name": "_id_", "key": {"_id": 1}},
            {"name": "email_1", "key": {"email": 1}, "unique": True},
        ],
    )
    intro = MongoIntrospector(client=client)

    mock_logger = MagicMock()
    with patch("forze_mongo.kernel.validate_indexes.logger", mock_logger):
        await validate_mongo_document_indexes(
            intro,
            [
                MongoDocumentIndexSpec(
                    name="projects",
                    write_relation=("app", "projects"),
                ),
            ],
        )

    mock_logger.warning.assert_called_once()
    assert "secondary unique" in str(mock_logger.warning.call_args)


# ....................... #


class TestWhatMongoDeclaresItCanKeep:
    """The declaration is checked against what the mechanisms actually do.

    A capability that reconciles and then does not enforce is the failure the whole convention
    exists to prevent, and it is invisible until a deployment writes the row that should have
    been refused — so the claim is pinned here rather than read off the docstring.
    """

    def test_skip_null_is_claimed(self) -> None:
        # Not through `sparse`, which reads like the mechanism and is not — it still indexes an
        # explicit null. Through a `partialFilterExpression` naming what the field *is*, which
        # is how a partial filter says "not null" given it admits no negation.
        assert MongoDocumentAdapter.storage_guarantees.unique_together_skip_null is True

    def test_a_skip_null_guarantee_is_met(self) -> None:
        assert (
            MongoDocumentAdapter.storage_guarantees.unmet(
                UniqueTogether(fields=("supersedes_id",), skip_null=True)
            )
            == ()
        )

    def test_the_filtered_form_is_claimed(self) -> None:
        # The contrast: the refusal above is about `skip_null`, not about Mongo being unable to
        # restrict a guarantee at all.
        assert (
            MongoDocumentAdapter.storage_guarantees.unmet(
                UniqueTogether(fields=("root_id",), where={"$values": {"is_current": True}})
            )
            == ()
        )


# ....................... #


class TestTheFilterIsComparedBySelectedDocuments:
    """A `partialFilterExpression` counts when it selects the documents the guarantee covers.

    Two weaker readings were tried first and both accept an index that does not keep the
    guarantee: matching the filter's *text* takes `status_code` for `status`, and matching its
    *field names* takes `{is_current: false}` for a guarantee about current documents. What has
    to agree is the document set, so the two filters are reduced to their equality constraints
    and compared — and anything that does not reduce that way is refused rather than guessed at,
    because whether it covers the guarantee's documents is an implication between predicates.
    """

    @staticmethod
    def _spec(where: dict[str, object]) -> MongoDocumentIndexSpec:
        return MongoDocumentIndexSpec(
            name="fact",
            write_relation=("db", "coll"),
            guarantees=(UniqueTogether(fields=("root_id",), where={"$values": where}),),
        )

    @staticmethod
    def _index(partial_filter: dict[str, object] | None) -> MongoIndexInfo:
        return MongoIndexInfo(
            name="ix",
            keys=(("root_id", 1),),
            unique=True,
            partial_filter=partial_filter,
        )

    def _validate(self, where: dict[str, object], partial_filter: dict[str, object]) -> None:
        self._validate_where({"$values": where}, partial_filter)

    def _validate_where(
        self,
        where: dict[str, object],
        partial_filter: dict[str, object],
    ) -> None:
        """:meth:`_validate` for a filter that is not a plain ``$values`` map."""

        _require_guarantee_indexes(
            MongoDocumentIndexSpec(
                name="fact",
                write_relation=("db", "coll"),
                guarantees=(UniqueTogether(fields=("root_id",), where=where),),
            ),
            [self._index(partial_filter)],
            database="db",
            collection="coll",
        )

    def test_a_longer_field_name_containing_the_declared_one_is_refused(self) -> None:
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"status": "current"}, {"status_code": "current"})

    def test_the_declared_field_itself_counts(self) -> None:
        self._validate({"status": "current"}, {"status": "current"})

    def test_a_wrong_value_on_the_right_field_is_refused(self) -> None:
        # The failure the field-name reading could not see: the index restricts exactly the
        # column the guarantee filters on, to the opposite value, so every document the
        # guarantee covers is outside the index and duplicates are insertable.
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"is_current": True}, {"is_current": False})

    def test_the_operator_spelling_of_the_same_equality_counts(self) -> None:
        # `{f: v}` and `{f: {$eq: v}}` are one constraint; refusing the second would fail a
        # correct migration over a spelling the server treats as identical.
        self._validate({"status": "current"}, {"$and": [{"status": {"$eq": "current"}}]})

    def test_an_extra_restriction_is_refused(self) -> None:
        # A document the guarantee covers but the index skips — one with no `tenant_id` — can be
        # duplicated, so an index narrower than the declaration does not keep it.
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate(
                {"status": "current"},
                {"$and": [{"status": "current"}, {"tenant_id": {"$exists": True}}]},
            )

    def test_a_broader_restriction_is_refused_too(self) -> None:
        # Sound for the guarantee and wrong for the aggregate: an index over current *and* draft
        # documents refuses a second draft, which the declaration deliberately allows. The same
        # reason a plain unique index does not satisfy a filtered guarantee.
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate(
                {"status": "current"},
                {"$or": [{"status": "current"}, {"status": "draft"}]},
            )

    def test_a_dotted_path_is_not_the_field_it_starts_with(self) -> None:
        # `meta.kind == "x"` selects different documents from `meta == "x"`.
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"meta": "x"}, {"meta.kind": "x"})

    def test_an_operator_name_is_not_read_as_a_field(self) -> None:
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"exists": True}, {"root_id": {"$exists": True}})

    def test_a_field_inside_a_matched_document_is_not_a_restricted_field(self) -> None:
        # `{metadata: {deleted: true}}` matches documents whose `metadata` equals that whole
        # document. It says nothing about a top-level `deleted`, and reading it as if it did
        # accepts an index that does not restrict the documents the guarantee covers.
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"deleted": True}, {"metadata": {"deleted": True}})

    def test_a_document_under_an_operator_is_not_descended_into_either(self) -> None:
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"deleted": True}, {"metadata": {"$eq": {"deleted": True}}})

    def test_an_index_over_other_fields_does_not_count(self) -> None:
        with pytest.raises(CoreException, match="no unique index"):
            _require_guarantee_indexes(
                self._spec({"status": "current"}),
                [
                    MongoIndexInfo(
                        name="ix",
                        keys=(("tenant_id", 1),),
                        unique=True,
                        partial_filter={"status": "current"},
                    )
                ],
                database="db",
                collection="coll",
            )

    def test_a_special_index_type_does_not_count(self) -> None:
        # A `text` or `hashed` index is a different structure over a different value; taking one
        # for a unique tuple index would accept a collection with no such constraint on it.
        with pytest.raises(CoreException, match="no unique index"):
            _require_guarantee_indexes(
                self._spec({"status": "current"}),
                [
                    MongoIndexInfo(
                        name="ix",
                        keys=(("root_id", "text"),),
                        unique=True,
                        partial_filter={"status": "current"},
                    )
                ],
                database="db",
                collection="coll",
            )

    @pytest.mark.parametrize(
        "expression",
        [
            "not-an-expression",
            {"$or": [{"a": 1}]},
            {"a": {"$gt": 1}},
            {"$and": "not-a-list"},
        ],
    )
    def test_what_does_not_reduce_to_equalities_says_so(self, expression: object) -> None:
        # `None`, not an empty map: "no constraints" and "constraints I cannot compare" are
        # different answers, and only the first would ever match a guarantee's own filter.
        assert _equalities(expression) is None

    def test_a_matched_document_is_a_value_like_any_other(self) -> None:
        assert _equalities({"metadata": {"deleted": True}}) is None

    @pytest.mark.parametrize(
        "where",
        [
            {"$or": [{"$values": {"a": 1}}, {"$values": {"b": 2}}]},
            {"$values": {"a": {"$gt": 1}}},
        ],
    )
    def test_a_guarantee_filter_that_is_not_plain_equalities_says_so(
        self,
        where: dict[str, object],
    ) -> None:
        # The same answer from the declaration's side: a guarantee whose own filter is a
        # disjunction or a range has no equality map to compare, so no index can be proven to
        # cover it and every one of them is refused.
        assert _guarantee_equalities(where) is None

    def test_a_guarantee_filter_of_plain_equalities_reduces(self) -> None:
        # Reduced to typed keys rather than raw values, so the comparison cannot be fooled by
        # Python equalities the server does not share.
        assert _guarantee_equalities({"$values": {"a": 1, "b": "x"}}) == {
            "a": ("eq", ("number", 1)),
            "b": ("eq", ("str", "x")),
        }

    def test_a_boolean_is_not_the_number_one(self) -> None:
        # Python compares `True == 1`; Mongo does not. An index filtered `{is_current: 1}`
        # indexes none of the documents a guarantee filtered `{is_current: true}` selects, so
        # reading the two as the same filter accepts an index that enforces nothing for them.
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"is_current": True}, {"is_current": 1})

    def test_numeric_widths_are_the_same_value(self) -> None:
        # The other direction, and the reason a number's key carries no width: Mongo compares an
        # int and a double by value, so refusing this would fail a correct index.
        self._validate({"count": 1}, {"count": 1.0})

    def test_two_numbers_one_double_cannot_tell_apart_stay_apart(self) -> None:
        # `float(9007199254740993)` is `9007199254740992.0`, so normalising through a double to
        # make the widths comparable would read these as one value — and the index then excludes
        # every document the guarantee selects. Python compares int to float exactly, which is
        # why the key carries the value rather than a cast of it.
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"seq": 9007199254740993}, {"seq": 9007199254740992.0})

    @pytest.mark.parametrize(
        "partial_filter",
        [
            {"$and": [{"active": False}, {"active": True}]},
            {"$and": [{"active": True}, {"active": {"$eq": False}}]},
        ],
    )
    def test_a_filter_naming_one_field_twice_is_refused(
        self,
        partial_filter: dict[str, object],
    ) -> None:
        # The filter selects no documents at all, so the index enforces nothing. Letting the
        # later constraint overwrite the earlier one reduces it to a plausible single equality —
        # which is how an index over nothing comes to satisfy a guarantee.
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"active": True}, partial_filter)

    @pytest.mark.parametrize(
        "partial_filter",
        [
            {"$and": [{"active": True}], "active": False},
            {"$and": [{"active": True}], "active": {"$eq": False}},
        ],
    )
    def test_a_conflict_between_a_branch_and_a_sibling_field_is_refused(
        self,
        partial_filter: dict[str, object],
    ) -> None:
        # `$and` alongside a plain field is a legal filter, so the contradiction can straddle
        # the two rather than sitting inside one conjunction.
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"active": True}, partial_filter)

    def test_the_same_constraint_twice_is_not_a_conflict(self) -> None:
        self._validate({"active": True}, {"$and": [{"active": True}, {"active": {"$eq": True}}]})

    def test_a_guarantee_filter_written_as_an_explicit_and_counts(self) -> None:
        # An explicit `$and` nests one conjunction per branch, and reading only the outer level
        # refuses a filter the author is entitled to write.
        self._validate_where(
            {"$and": [{"$values": {"a": 1}}, {"$values": {"b": 2}}]},
            {"a": 1, "b": 2},
        )

    def test_a_null_test_is_an_equality_against_null(self) -> None:
        # The parser spells it `$null` and Mongo spells it `{f: null}`; they select the same
        # documents, so refusing the index would fail a correct migration.
        self._validate_where(
            {"$values": {"supersedes_id": None}},
            {"supersedes_id": None},
        )

    def test_a_guarantee_whose_filter_cannot_be_compared_is_refused(self) -> None:
        with pytest.raises(CoreException, match="partialFilterExpression"):
            _require_guarantee_indexes(
                MongoDocumentIndexSpec(
                    name="fact",
                    write_relation=("db", "coll"),
                    guarantees=(
                        UniqueTogether(
                            fields=("root_id",),
                            where={"$values": {"status": {"$gt": "a"}}},
                        ),
                    ),
                ),
                [
                    MongoIndexInfo(
                        name="ix",
                        keys=(("root_id", 1),),
                        unique=True,
                        partial_filter={"status": {"$gt": "a"}},
                    )
                ],
                database="db",
                collection="coll",
            )


# ....................... #


class TestTheRefusalPrintsAMigrationThatWouldWork:
    """The message is the whole value of a startup refusal, so it has to be runnable.

    An operator reading "guarantee not satisfied" with a statement that does not satisfy it is
    worse off than one reading nothing: they run it, startup fails again, and the message is
    what they now distrust.
    """

    @staticmethod
    def _refusal(where: object, indexes: list[MongoIndexInfo] | None = None) -> str:
        spec = MongoDocumentIndexSpec(
            name="fact",
            write_relation=("db", "coll"),
            guarantees=(UniqueTogether(fields=("root_id",), where=where),),  # type: ignore[arg-type]
        )

        with pytest.raises(CoreException) as caught:
            _require_guarantee_indexes(spec, indexes or [], database="db", collection="coll")

        return caught.value.summary

    def test_a_boolean_is_printed_as_mongosh_spells_it(self) -> None:
        # `repr` writes `True`, which mongosh rejects. The printed statement has to be one an
        # operator can paste.
        message = self._refusal({"$values": {"is_current": True}})

        assert "partialFilterExpression: {is_current: true}" in message
        assert "True" not in message

    def test_a_null_is_printed_as_null(self) -> None:
        assert "{supersedes_id: null}" in self._refusal({"$values": {"supersedes_id": None}})

    def test_a_string_is_quoted(self) -> None:
        assert '{status: "current"}' in self._refusal({"$values": {"status": "current"}})

    def test_a_whole_number_keeps_its_integer_spelling(self) -> None:
        assert "{tier: 2}" in self._refusal({"$values": {"tier": 2}})

    @pytest.mark.parametrize(
        ("value", "expected"),
        [(0.5, "{ratio: 0.5}"), (2.0, "{ratio: 2}")],
    )
    def test_a_fractional_number_keeps_its_fraction(
        self,
        value: float,
        expected: str,
    ) -> None:
        # A whole double still prints as an integer, which mongosh stores as a double anyway;
        # a fractional one has to survive, or the printed statement filters on another value.
        assert expected in self._refusal({"$values": {"ratio": value}})

    @pytest.mark.parametrize(
        ("field", "expected"),
        [
            ("is_current", "{is_current: true}"),
            ("effective-date", '{"effective-date": true}'),
            ("a.b", '{"a.b": true}'),
            ("$weird", "{$weird: true}"),
        ],
    )
    def test_a_field_name_is_quoted_only_where_it_has_to_be(
        self,
        field: str,
        expected: str,
    ) -> None:
        # `effective-date` is a legal Mongo field and not a legal JavaScript identifier, so
        # printing it bare hands over a statement that does not parse. Bare elsewhere, because
        # that is how the statement is written by hand.
        assert expected in self._refusal({"$values": {field: True}})

    def test_an_index_key_name_is_quoted_the_same_way(self) -> None:
        # The filter's field names were quoted and the index key names beside them were not, so
        # the same statement still did not parse. One rule, both halves.
        spec = MongoDocumentIndexSpec(
            name="fact",
            write_relation=("db", "coll"),
            guarantees=(UniqueTogether(fields=("effective-date",), where={"$values": {"x": 1}}),),
        )

        with pytest.raises(CoreException) as caught:
            _require_guarantee_indexes(spec, [], database="db", collection="coll")

        assert 'createIndex({"effective-date": 1}' in " ".join(caught.value.summary.split())

    def test_two_not_a_numbers_are_one_value(self) -> None:
        # Mongo matches a `{x: NaN}` filter against the documents whose `x` is NaN, which IEEE
        # equality cannot express — left raw, two distinct NaN objects never compare equal, so
        # no index could ever satisfy such a guarantee and the same constraint written twice
        # would read as a contradiction.
        assert _bson_key(float("nan")) == _bson_key(float("nan"))

    def test_a_non_finite_filter_still_gets_its_migration(self) -> None:
        # The diagnostic path has to survive every value the declaration admits: `int(nan)`
        # raises, so rendering it there replaced an actionable refusal with a stack trace at
        # startup — the one moment the message is what an operator has.
        assert "{ratio: NaN}" in self._refusal({"$values": {"ratio": float("nan")}})

    def test_a_filter_naming_no_field_still_asks_for_a_partial_index(self) -> None:
        # The validation requires a partialFilterExpression whenever `where` is set, so a
        # message suggesting a plain unique index would send the operator to a migration that
        # leaves startup failing.
        message = self._refusal({"$and": []})

        assert "partialFilterExpression" in message

    def test_an_unfiltered_guarantee_still_asks_for_a_plain_index(self) -> None:
        # The contrast: the fix above must not make every refusal ask for a partial index.
        message = self._refusal(None)

        assert "partialFilterExpression" not in message
        assert "{unique: true}" in message


# ....................... #


class TestEveryValueShapeSurvivesTheRoundTrip:
    """A guarantee's filter can hold more than booleans, and the message has to print all of it.

    Each of these reaches the refusal an operator reads, so a shape that renders as a Python
    literal there is a statement that does not run — the failure the boolean case already
    showed, in the value types a schema reaches for next.
    """

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (["a", 1], '["a", 1]'),
            (datetime(2026, 9, 19, tzinfo=UTC), 'ISODate("2026-09-19T00:00:00+00:00")'),
            (date(2026, 9, 19), 'ISODate("2026-09-19T00:00:00+00:00")'),
            (datetime(2026, 9, 19), 'ISODate("2026-09-19T00:00:00+00:00")'),
            (
                UUID("00000000-0000-0000-0000-00000000002a"),
                '"00000000-0000-0000-0000-00000000002a"',
            ),
            (9007199254740993, 'Long("9007199254740993")'),
            (Decimal("9.99"), 'Decimal128("9.99")'),
            (float("nan"), "NaN"),
            (float("inf"), "Infinity"),
            (float("-inf"), "-Infinity"),
        ],
    )
    def test_it_renders_as_mongosh_spells_it(self, value: object, expected: str) -> None:
        # A bare literal is not always safe: mongosh reads a plain number as a double, so an
        # integer past 2**53 and a Decimal each have to be spelled as the type that holds them,
        # or the index the operator creates filters on a different value than the one printed.
        assert _mongosh(_bson_key(value)) == expected

    @pytest.mark.parametrize(
        "value",
        [["a", 1], datetime(2026, 9, 19, tzinfo=UTC)],
    )
    def test_a_value_is_not_its_own_rendering(self, value: object) -> None:
        # The comparison is over these keys, so a type reducing to its own rendering would make
        # an index filtered by the string satisfy a guarantee filtered by the value.
        assert _bson_key(value) == _bson_key(value)
        assert _bson_key(value) != _bson_key(str(value))

    @pytest.mark.parametrize(
        ("stored", "declared"),
        [
            (
                "00000000-0000-0000-0000-00000000002a",
                UUID("00000000-0000-0000-0000-00000000002a"),
            ),
            (Decimal128("9.99"), Decimal("9.99")),
            (datetime(2026, 9, 19, tzinfo=UTC), date(2026, 9, 19)),
            (datetime(2026, 9, 19, tzinfo=UTC), datetime(2026, 9, 19)),
            (
                datetime(2026, 9, 19, 12, 30, 0, 123000, tzinfo=UTC),
                datetime(2026, 9, 19, 12, 30, 0, 123567, tzinfo=UTC),
            ),
            (Decimal128("NaN"), float("nan")),
            # A *signaling* NaN is one Decimal128 round-trips, and converting one to a float
            # raises — so the reduction has to reach it without going through `math.isnan`.
            (Decimal128(Decimal("sNaN")), float("nan")),
            (Decimal("sNaN"), Decimal("NaN")),
        ],
    )
    def test_what_this_adapter_stores_reduces_to_what_a_spec_declares(
        self,
        stored: object,
        declared: object,
    ) -> None:
        # The two exceptions to the rule above, and they are storage facts rather than BSON
        # ones: this adapter writes a UUID as its canonical string and a Decimal as a
        # Decimal128, so an index filter read back from the server carries the stored spelling
        # while the guarantee carries the domain one. Telling them apart refuses a correct index.
        assert _bson_key(stored) == _bson_key(declared)


# ....................... #


class _Colour(StrEnum):
    RED = "red"


class _Tier(IntEnum):
    GOLD = 1


class TestNamingAFieldsStoredType:
    """The null exemption is expressed by naming what a field *is*, so the naming must be right.

    Every answer here is either a type this adapter demonstrably writes, or a refusal. There is
    no default: guessing would index the wrong documents, and an index over the wrong documents
    is a guarantee that silently is not kept.
    """

    @pytest.mark.parametrize(
        ("annotation", "expected"),
        [
            (str, ("string",)),
            (UUID, ("string",)),  # written as its canonical string
            (bool, ("bool",)),  # a bool is an int in Python and is not in BSON
            (int, ("int", "long")),  # which one depends on magnitude
            (float, ("double",)),
            (Decimal, ("decimal",)),  # written as a Decimal128
            (datetime, ("date",)),
            (date, ("date",)),
            (bytes, ("binData",)),
            (UUID | None, ("string",)),  # the optional wrapper is stripped
            (str | int, ("string", "int", "long")),
            (list[str], ("array",)),
            (_Colour, ("string",)),
            (_Tier, ("int", "long")),
        ],
    )
    def test_it_names_what_the_adapter_writes(
        self,
        annotation: object,
        expected: tuple[str, ...],
    ) -> None:
        assert _bson_types(annotation) == expected

    @pytest.mark.parametrize(
        "annotation",
        [
            None,  # an unannotated field
            object,  # nothing this can name
            dict[str, int],  # a stored document, whose fields are not the guarantee's
            UUID | object,  # one unresolvable arm poisons the union
        ],
    )
    def test_it_refuses_what_it_cannot_name(self, annotation: object) -> None:
        assert _bson_types(annotation) is None


# ....................... #


class TestTheExemptionRefusesWhatItCannotCheck:
    @staticmethod
    def _spec(read_model: object) -> MongoDocumentIndexSpec:
        return MongoDocumentIndexSpec(
            name="fact",
            write_relation=("db", "coll"),
            guarantees=(UniqueTogether(fields=("pointer",), skip_null=True),),
            read_model=read_model,  # type: ignore[arg-type]
        )

    @staticmethod
    def _index() -> MongoIndexInfo:
        return MongoIndexInfo(
            name="ix",
            keys=(("pointer", 1),),
            unique=True,
            partial_filter={"pointer": {"$type": "string"}},
        )

    def test_a_matching_index_is_accepted(self) -> None:
        class Model(BaseModel):
            pointer: UUID | None = None

        _require_guarantee_indexes(
            self._spec(Model), [self._index()], database="db", collection="coll"
        )

    def test_no_read_model_is_a_refusal(self) -> None:
        # Without it the exemption cannot be stated at all, so the index cannot be checked —
        # and an unverifiable guarantee is refused rather than assumed kept.
        with pytest.raises(CoreException):
            _require_guarantee_indexes(
                self._spec(None), [self._index()], database="db", collection="coll"
            )

    def test_a_filtered_exemption_is_not_accepted_on_its_filter_alone(self) -> None:
        """The refusal above passes for a weaker reason than it looks.

        With only `skip_null` there is nothing to compare either way, so an unverifiable
        exemption and a missing index are indistinguishable. Add a `where` and they part: the
        filter's equalities *can* be checked, and an index matching only those keeps half the
        guarantee while exempting nothing. That is the index this has to refuse.
        """

        spec = MongoDocumentIndexSpec(
            name="fact",
            write_relation=("db", "coll"),
            guarantees=(
                UniqueTogether(
                    fields=("pointer",),
                    where={"$values": {"live": True}},
                    skip_null=True,
                ),
            ),
            read_model=None,
        )
        index = MongoIndexInfo(
            name="ix",
            keys=(("pointer", 1),),
            unique=True,
            partial_filter={"live": True},
        )

        with pytest.raises(CoreException):
            _require_guarantee_indexes(spec, [index], database="db", collection="coll")

    def test_a_field_absent_from_the_read_model_is_a_refusal(self) -> None:
        class Model(BaseModel):
            other: str = ""

        with pytest.raises(CoreException):
            _require_guarantee_indexes(
                self._spec(Model), [self._index()], database="db", collection="coll"
            )

    def test_an_unnameable_type_is_a_refusal(self) -> None:
        class Model(BaseModel):
            model_config = {"arbitrary_types_allowed": True}
            pointer: object = None

        with pytest.raises(CoreException):
            _require_guarantee_indexes(
                self._spec(Model), [self._index()], database="db", collection="coll"
            )

    @pytest.mark.parametrize(
        "partial_filter",
        [
            {"pointer": {"$type": 2}},  # an alias that is not a name
            {"pointer": {"$type": ["string", 7]}},
            {"$and": [{"pointer": {"$type": "string"}}, {"pointer": {"$type": "int"}}]},
            # The contradiction straddling a branch and a sibling field, which is where the
            # two constraints meet through a different path than two branches do.
            {"$and": [{"pointer": {"$type": "int"}}], "pointer": {"$type": "string"}},
        ],
    )
    def test_an_index_whose_type_clause_cannot_be_read_is_refused(
        self,
        partial_filter: dict[str, object],
    ) -> None:
        class Model(BaseModel):
            pointer: UUID | None = None

        index = MongoIndexInfo(
            name="ix", keys=(("pointer", 1),), unique=True, partial_filter=partial_filter
        )

        with pytest.raises(CoreException):
            _require_guarantee_indexes(
                self._spec(Model), [index], database="db", collection="coll"
            )
