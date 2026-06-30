"""Broad coverage of in-memory filter, compare, quantifier, and aggregate helpers."""

from __future__ import annotations

import statistics
from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.querying import (
    ELEM_SCALAR_FIELD,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryField,
    QueryOr,
)
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze.application.contracts.querying import (
    GroupField,
    GroupTrunc,
)
from forze.base.exceptions import CoreException
from forze_mock.adapters import MockDocumentAdapter, MockState
from forze_mock.query import _match_expr  # type: ignore[reportPrivateUsage]
from forze_mock.query._types import _MISSING  # type: ignore[reportPrivateUsage]
from forze_mock.query.matching import (  # type: ignore[reportPrivateUsage]
    _aggregate_docs,
    _coerce_datetime_for_bucket,
    _coerce_set,
    _group_key_part,
    _is_descendant_path,
    _match_text,
    _memb_contains,
    _normalize_array_value,
    _path_get,
    _path_text,
    _percentile_cont,
    _project,
    _require_numeric,
    _sort_docs,
    _value_is_empty,
)

pytestmark = pytest.mark.unit

# ----------------------- #
# _match_field: comparison / set / text / path operators


def test_compare_value_operators() -> None:
    doc = {"n": 5}
    assert _match_expr(doc, QueryField("n", "$gt", 4)) is True
    assert _match_expr(doc, QueryField("n", "$gte", 5)) is True
    assert _match_expr(doc, QueryField("n", "$lt", 6)) is True
    assert _match_expr(doc, QueryField("n", "$lte", 5)) is True
    assert _match_expr({}, QueryField("n", "$gt", 4)) is False
    assert _match_expr({}, QueryField("n", "$lt", 4)) is False


def test_in_nin_null_empty() -> None:
    assert _match_expr({"t": "a"}, QueryField("t", "$in", ["a", "b"])) is True
    assert _match_expr({"t": "z"}, QueryField("t", "$nin", ["a"])) is True
    assert _match_expr({}, QueryField("t", "$nin", ["a"])) is True  # missing → nin true
    assert _match_expr({"x": None}, QueryField("x", "$null", True)) is True
    assert _match_expr({"x": []}, QueryField("x", "$empty", True)) is True


def test_set_relation_operators() -> None:
    assert _match_expr({"s": [1, 2, 3]}, QueryField("s", "$superset", [1, 2])) is True
    assert _match_expr({"s": [1]}, QueryField("s", "$subset", [1, 2])) is True
    assert _match_expr({"s": [1, 2]}, QueryField("s", "$disjoint", [3])) is True
    assert _match_expr({"s": [1, 2]}, QueryField("s", "$overlaps", [2, 9])) is True
    assert _match_expr({}, QueryField("s", "$superset", [1])) is False
    assert _match_expr({}, QueryField("s", "$subset", [1])) is False
    assert _match_expr({}, QueryField("s", "$overlaps", [1])) is False


def test_text_operators_like_ilike_regex() -> None:
    assert _match_expr({"t": "Road"}, QueryField("t", "$like", "Ro%")) is True
    assert _match_expr({"t": "Road"}, QueryField("t", "$ilike", "ro%")) is True
    assert _match_expr({"t": "abc"}, QueryField("t", "$regex", "b")) is True


def test_descendant_and_ancestor_paths() -> None:
    assert (
        _match_expr({"p": "a.b.c"}, QueryField("p", "$descendant_of", "a.b")) is True
    )
    assert _match_expr({"p": "a.b"}, QueryField("p", "$ancestor_of", "a.b.c")) is True
    assert _match_expr({}, QueryField("p", "$descendant_of", "a")) is False
    assert _match_expr({}, QueryField("p", "$ancestor_of", "a")) is False


def test_nested_path_access() -> None:
    assert _match_expr({"a": {"b": {"c": 7}}}, QueryField("a.b.c", "$eq", 7)) is True


# ----------------------- #
# _match_compare: field-to-field comparisons


def test_compare_field_to_field() -> None:
    assert _match_expr({"a": 2, "b": 1}, QueryCompare("a", "$gt", "b")) is True
    assert _match_expr({"a": 2, "b": 2}, QueryCompare("a", "$gte", "b")) is True
    assert _match_expr({"a": 1, "b": 2}, QueryCompare("a", "$lt", "b")) is True
    assert _match_expr({"a": 1, "b": 1}, QueryCompare("a", "$lte", "b")) is True
    assert _match_expr({"a": 1, "b": 1}, QueryCompare("a", "$eq", "b")) is True
    assert _match_expr({"a": 1, "b": 2}, QueryCompare("a", "$neq", "b")) is True


def test_compare_missing_operands() -> None:
    assert _match_expr({"a": 1}, QueryCompare("a", "$eq", "missing")) is False
    assert _match_expr({"a": 1}, QueryCompare("a", "$gt", "missing")) is False
    assert _match_expr({"a": 1}, QueryCompare("a", "$lt", "missing")) is False
    assert _match_expr({"a": 1}, QueryCompare("a", "$gte", "missing")) is False
    assert _match_expr({"a": 1}, QueryCompare("a", "$lte", "missing")) is False
    # neq: a missing operand yields True (cannot be proven equal)
    assert _match_expr({"a": 1}, QueryCompare("a", "$neq", "missing")) is True
    assert _match_expr({"b": 1}, QueryCompare("missing", "$neq", "b")) is True


def test_compare_type_mismatch_false() -> None:
    assert _match_expr({"a": "x", "b": 1}, QueryCompare("a", "$gt", "b")) is False


# ----------------------- #
# _match_elem: array quantifiers + vacuous truth


def _scalar_elem(quantifier: str, op: str, value: Any) -> QueryElem:
    return QueryElem("arr", quantifier, QueryField(ELEM_SCALAR_FIELD, op, value))


def test_quantifier_any_all_none_on_scalars() -> None:
    doc = {"arr": [1, 2, 3]}
    assert _match_expr(doc, _scalar_elem("$any", "$gt", 2)) is True
    assert _match_expr(doc, _scalar_elem("$all", "$gt", 0)) is True
    assert _match_expr(doc, _scalar_elem("$none", "$gt", 9)) is True
    assert _match_expr(doc, _scalar_elem("$all", "$gt", 2)) is False


def test_quantifier_vacuous_truth_on_empty_array() -> None:
    doc = {"arr": []}
    # $all / $none are vacuously true on empty/missing arrays; $any is false.
    assert _match_expr(doc, _scalar_elem("$all", "$gt", 0)) is True
    assert _match_expr(doc, _scalar_elem("$none", "$gt", 0)) is True
    assert _match_expr(doc, _scalar_elem("$any", "$gt", 0)) is False
    assert _match_expr({}, _scalar_elem("$all", "$gt", 0)) is True


def test_quantifier_over_object_elements() -> None:
    doc = {"items": [{"q": 1}, {"q": 5}]}
    node = QueryElem("items", "$any", QueryField("q", "$gte", 5))
    assert _match_expr(doc, node) is True
    node_all = QueryElem("items", "$all", QueryField("q", "$gte", 5))
    assert _match_expr(doc, node_all) is False


def test_quantifier_inner_and_or() -> None:
    doc = {"arr": [2]}
    inner_and = QueryAnd(
        [
            QueryField(ELEM_SCALAR_FIELD, "$gt", 1),
            QueryField(ELEM_SCALAR_FIELD, "$lt", 3),
        ]
    )
    assert _match_expr(doc, QueryElem("arr", "$any", inner_and)) is True
    inner_or = QueryOr(
        [
            QueryField(ELEM_SCALAR_FIELD, "$eq", 99),
            QueryField(ELEM_SCALAR_FIELD, "$eq", 2),
        ]
    )
    assert _match_expr(doc, QueryElem("arr", "$any", inner_or)) is True


def test_quantifier_field_inner_on_non_dict_element_false() -> None:
    # An object-field inner over a scalar element yields no match.
    doc = {"arr": [1, 2]}
    node = QueryElem("arr", "$any", QueryField("q", "$eq", 1))
    assert _match_expr(doc, node) is False


# ----------------------- #
# Aggregation via the document adapter (drives _aggregate_docs end to end)


class _Fields(BaseModel):
    grp: str
    kind: str | None = None
    amount: int = 0


class _Create(CreateDocumentCmd, _Fields):
    pass


class _Doc(Document, _Fields):
    pass


class _Read(ReadDocument, _Fields):
    pass


def _mock() -> MockDocumentAdapter[Any, Any, Any, Any]:
    spec = DocumentSpec(
        name="aggcov",
        read=_Read,
        write=DocumentWriteTypes(domain=_Doc, create_cmd=_Create),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="aggcov",
        read_model=_Read,
        domain_model=_Doc,
    )


async def _rows(doc: Any, aggregates: dict[str, Any]) -> list[dict[str, Any]]:
    page = await doc.aggregate_page(aggregates=aggregates, pagination={"limit": 100})
    return page.hits


@pytest.mark.asyncio
async def test_aggregate_core_functions() -> None:
    doc = _mock()
    for a in (10, 20, 30, 40):
        await doc.create(_Create(grp="g", amount=a))

    rows = await _rows(
        doc,
        {
            "$groups": {"g": "grp"},
            "$computed": {
                "s": {"$sum": "amount"},
                "a": {"$avg": "amount"},
                "med": {"$median": "amount"},
                "lo": {"$min": "amount"},
                "hi": {"$max": "amount"},
                "cnt": {"$count": None},
            },
        },
    )
    assert len(rows) == 1
    r = rows[0]
    assert r["s"] == 100
    assert r["a"] == pytest.approx(25.0)
    assert r["med"] == pytest.approx(25.0)  # even count → mean of middle two
    assert r["lo"] == 10
    assert r["hi"] == 40
    assert r["cnt"] == 4


@pytest.mark.asyncio
async def test_aggregate_median_odd_count() -> None:
    doc = _mock()
    for a in (1, 5, 9):
        await doc.create(_Create(grp="g", amount=a))
    rows = await _rows(
        doc,
        {"$groups": {"g": "grp"}, "$computed": {"med": {"$median": "amount"}}},
    )
    assert rows[0]["med"] == 5


@pytest.mark.asyncio
async def test_aggregate_stats_and_count_distinct() -> None:
    doc = _mock()
    for a in (10, 20, 30, 40):
        await doc.create(_Create(grp="g", kind="x" if a < 30 else "y", amount=a))

    rows = await _rows(
        doc,
        {
            "$groups": {"g": "grp"},
            "$computed": {
                "sp": {"$stddev_pop": "amount"},
                "ss": {"$stddev_samp": "amount"},
                "vp": {"$var_pop": "amount"},
                "vs": {"$var_samp": "amount"},
                "p": {"$percentile": {"field": "amount", "p": 0.5}},
                "cd": {"$count_distinct": "kind"},
            },
        },
    )
    r = rows[0]
    data = [10, 20, 30, 40]
    assert r["sp"] == pytest.approx(statistics.pstdev(data))
    assert r["ss"] == pytest.approx(statistics.stdev(data))
    assert r["vp"] == pytest.approx(statistics.pvariance(data))
    assert r["vs"] == pytest.approx(statistics.variance(data))
    assert r["p"] == pytest.approx(25.0)
    assert r["cd"] == 2


@pytest.mark.asyncio
async def test_aggregate_multi_key_grouping_and_having() -> None:
    doc = _mock()
    await doc.create(_Create(grp="a", kind="x", amount=1))
    await doc.create(_Create(grp="a", kind="x", amount=2))
    await doc.create(_Create(grp="a", kind="y", amount=5))
    await doc.create(_Create(grp="b", kind="x", amount=9))

    rows = await _rows(
        doc,
        {
            "$groups": {"g": "grp", "k": "kind"},
            "$computed": {"total": {"$sum": "amount"}, "cnt": {"$count": None}},
            "$having": {"$values": {"total": {"$gte": 3}}},
        },
    )
    got = {(r["g"], r["k"], r["total"]) for r in rows}
    # (a,x)=3, (a,y)=5, (b,x)=9 survive; nothing dropped below 3 here.
    assert got == {("a", "x", 3), ("a", "y", 5), ("b", "x", 9)}


# ----------------------- #
# Pure helper coverage (small leaf functions)


def test_path_get_non_dict_intermediate() -> None:
    # Walking into a non-dict intermediate yields _MISSING.
    assert _path_get({"a": 5}, "a.b") is _MISSING
    assert _path_get({"a": {"b": 1}}, "a.b") == 1


def test_path_text_scalar_and_missing() -> None:
    assert _path_text({"n": 7}, "n") == "7"  # non-str/non-seq → str()
    assert _path_text({}, "n") == ""  # missing → ""
    assert _path_text({"t": ["x", "y"]}, "t") == "x y"


def test_value_is_empty_variants() -> None:
    assert _value_is_empty(None) is True
    assert _value_is_empty("") is True
    assert _value_is_empty([1]) is False
    assert _value_is_empty(5) is False  # non-collection, non-None


def test_coerce_set_scalar_wraps() -> None:
    assert _coerce_set(3) == {3}
    assert _coerce_set([1, 2]) == {1, 2}


def test_memb_contains_scalar_and_array() -> None:
    assert _memb_contains("a", ["a", "b"]) is True
    assert _memb_contains(["a", "c"], ["c"]) is True
    assert _memb_contains("z", ["a"]) is False


def test_match_text_unknown_op_false() -> None:
    assert _match_text("abc", "$nope", "a") is False
    assert _match_text(_MISSING, "$like", "a") is False


def test_is_descendant_path_non_string() -> None:
    assert _is_descendant_path(1, "a") is False
    assert _is_descendant_path("a.b", "a") is True


def test_normalize_array_value() -> None:
    assert _normalize_array_value([1, 2]) == [1, 2]
    assert _normalize_array_value(None) is None
    assert _normalize_array_value(_MISSING) is None
    assert _normalize_array_value("x") is None  # non-list scalar


def test_match_field_type_error_branches() -> None:
    # Comparing incompatible types returns False (TypeError swallowed).
    assert _match_expr({"n": "x"}, QueryField("n", "$gt", 1)) is False
    assert _match_expr({"n": "x"}, QueryField("n", "$gte", 1)) is False
    assert _match_expr({"n": "x"}, QueryField("n", "$lt", 1)) is False
    assert _match_expr({"n": "x"}, QueryField("n", "$lte", 1)) is False


def test_memb_contains_array_item_match() -> None:
    # field value is a list; one element equals a candidate (inner-loop hit).
    assert _match_expr({"t": ["a", "b"]}, QueryField("t", "$in", ["b"])) is True


def test_quantifier_nested_scalar_array_of_arrays() -> None:
    # Scalar element is itself a sub-array to quantify (ELEM_SCALAR nested QueryElem).
    doc = {"matrix": [[1, 2], [9]]}
    nested = QueryElem(
        ELEM_SCALAR_FIELD, "$any", QueryField(ELEM_SCALAR_FIELD, "$gt", 5)
    )
    node = QueryElem("matrix", "$any", nested)
    assert _match_expr(doc, node) is True


def test_quantifier_nested_object_subarray_and_non_dict() -> None:
    # A nested quantifier over a document element with its own sub-array.
    doc = {"rows": [{"vals": [1, 2]}, {"vals": [8]}]}
    inner = QueryElem("vals", "$any", QueryField(ELEM_SCALAR_FIELD, "$gt", 5))
    node = QueryElem("rows", "$any", inner)
    assert _match_expr(doc, node) is True
    # Non-dict element under a nested object quantifier → no match.
    doc2 = {"rows": [5]}
    assert _match_expr(doc2, node) is False


def test_match_expr_or_and_short_circuit() -> None:
    assert (
        _match_expr(
            {"a": 1}, QueryOr([QueryField("a", "$eq", 1), QueryField("a", "$eq", 9)])
        )
        is True
    )
    assert (
        _match_expr(
            {"a": 1}, QueryAnd([QueryField("a", "$eq", 1), QueryField("a", "$gt", 0)])
        )
        is True
    )


def test_match_compare_all_type_error_branches() -> None:
    assert _match_expr({"a": "x", "b": 1}, QueryCompare("a", "$gte", "b")) is False
    assert _match_expr({"a": "x", "b": 1}, QueryCompare("a", "$lt", "b")) is False
    assert _match_expr({"a": "x", "b": 1}, QueryCompare("a", "$lte", "b")) is False


def test_match_expr_unknown_expression_raises() -> None:
    with pytest.raises(CoreException, match="Unknown query expression"):
        _match_expr({}, object())  # type: ignore[arg-type]


def test_project_passthrough_and_missing_skip() -> None:
    doc = {"a": 1, "b": {"c": 2}}
    assert _project(doc, None) == doc  # no return_fields → full copy
    out = _project(doc, ["a", "b.c", "missing"])
    # A dotted path reshapes into the nested output; a missing field is skipped.
    assert out == {"a": 1, "b": {"c": 2}}


def test_sort_docs_empty_keys_passthrough() -> None:
    docs = [{"n": 2}, {"n": 1}]
    assert _sort_docs(docs, None) is docs  # no keys → unchanged
    ordered = _sort_docs(docs, {"n": "asc"})
    assert [d["n"] for d in ordered] == [1, 2]


def test_sort_docs_tie_returns_zero() -> None:
    # Equal sort values exercise the per-key comparator's tie (return 0) path.
    docs = [{"n": 1, "id": "a"}, {"n": 1, "id": "b"}]
    ordered = _sort_docs(docs, {"n": "asc"})
    assert [d["id"] for d in ordered] == ["a", "b"]


def test_aggregate_median_single_value() -> None:
    # A single value drives the odd-length median branch with one item.
    rows = _aggregate_docs(
        [{"g": "x", "amount": 7}],
        {"$groups": {"g": "g"}, "$computed": {"med": {"$median": "amount"}}},
    )
    assert rows[0]["med"] == 7


def test_aggregate_empty_metric_values_are_none() -> None:
    # No numeric values → sum/avg/median/min/max/stddev/percentile all None.
    rows = _aggregate_docs(
        [{"g": "x"}],
        {
            "$groups": {"g": "g"},
            "$computed": {
                "s": {"$sum": "amount"},
                "a": {"$avg": "amount"},
                "med": {"$median": "amount"},
                "lo": {"$min": "amount"},
                "hi": {"$max": "amount"},
                "sp": {"$stddev_pop": "amount"},
                "vp": {"$var_pop": "amount"},
                "p": {"$percentile": {"field": "amount", "p": 0.5}},
            },
        },
    )
    r = rows[0]
    for alias in ("s", "a", "med", "lo", "hi", "sp", "vp", "p"):
        assert r[alias] is None


def test_require_numeric_rejects_non_numeric() -> None:
    assert _require_numeric(3, function="$sum", field="x") == 3
    with pytest.raises(CoreException, match="numeric"):
        _require_numeric("nope", function="$sum", field="x")
    with pytest.raises(CoreException, match="numeric"):
        _require_numeric(True, function="$sum", field="x")  # bool excluded


def test_percentile_cont_edges() -> None:
    assert _percentile_cont([], 0.5) is None
    assert _percentile_cont([7], 0.5) == 7.0  # single value
    assert _percentile_cont([10, 20], 0.5) == pytest.approx(15.0)


def test_coerce_datetime_for_bucket_variants() -> None:
    from datetime import datetime, timezone

    naive = datetime(2020, 1, 1)
    aware = _coerce_datetime_for_bucket(naive)
    assert aware.tzinfo is timezone.utc
    assert _coerce_datetime_for_bucket("2020-01-01T00:00:00Z").year == 2020
    assert _coerce_datetime_for_bucket(0).year == 1970  # epoch
    with pytest.raises(CoreException, match="Invalid timestamp"):
        _coerce_datetime_for_bucket(object())


def test_group_key_part_ref_trunc_and_unsupported() -> None:
    from datetime import datetime, timezone

    from forze.application.contracts.querying.internal.time_bucket import (
        parse_aggregate_timezone,
    )

    assert _group_key_part({"g": "x"}, GroupField(field="g")) == "x"
    assert _group_key_part({}, GroupField(field="g")) is None  # missing → None

    trunc = GroupTrunc(
        field="ts", unit="day", timezone=parse_aggregate_timezone("UTC")
    )
    ts = datetime(2020, 5, 17, 13, 0, tzinfo=timezone.utc)
    floored = _group_key_part({"ts": ts}, trunc)
    assert isinstance(floored, str) and floored.startswith("2020-05-17")
    assert _group_key_part({}, trunc) is None  # missing timestamp → None

    with pytest.raises(CoreException, match="Unsupported group expression"):
        _group_key_part({}, object())


def test_aggregate_docs_no_groups_empty_input() -> None:
    # No groups and no docs still produces a single global row.
    rows = _aggregate_docs([], {"$computed": {"cnt": {"$count": None}}})
    assert rows == [{"cnt": 0}]


@pytest.mark.asyncio
async def test_aggregate_computed_filter_subsets_rows() -> None:
    # ``filter`` on a computed field restricts which rows feed the aggregate.
    doc = _mock()
    await doc.create(_Create(grp="g", kind="x", amount=10))
    await doc.create(_Create(grp="g", kind="y", amount=100))
    rows = await _rows(
        doc,
        {
            "$groups": {"g": "grp"},
            "$computed": {
                "x_total": {"$sum": {"field": "amount", "filter": {"$values": {"kind": {"$eq": "x"}}}}},
            },
        },
    )
    assert rows[0]["x_total"] == 10


@pytest.mark.asyncio
async def test_aggregate_trunc_grouping() -> None:
    # GroupTrunc bucket grouping through the document adapter.
    from datetime import datetime, timezone

    class _TFields(BaseModel):
        ts: datetime
        amount: int = 0

    class _TCreate(CreateDocumentCmd, _TFields):
        pass

    class _TDoc(Document, _TFields):
        pass

    class _TRead(ReadDocument, _TFields):
        pass

    spec = DocumentSpec(
        name="tsagg",
        read=_TRead,
        write=DocumentWriteTypes(domain=_TDoc, create_cmd=_TCreate),
    )
    doc = MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="tsagg",
        read_model=_TRead,
        domain_model=_TDoc,
    )
    await doc.create(
        _TCreate(ts=datetime(2020, 1, 1, 9, tzinfo=timezone.utc), amount=1)
    )
    await doc.create(
        _TCreate(ts=datetime(2020, 1, 1, 18, tzinfo=timezone.utc), amount=2)
    )
    await doc.create(
        _TCreate(ts=datetime(2020, 1, 2, 9, tzinfo=timezone.utc), amount=4)
    )
    rows = await _rows(
        doc,
        {
            "$groups": {"day": {"$trunc": {"field": "ts", "unit": "day"}}},
            "$computed": {"total": {"$sum": "amount"}},
        },
    )
    totals = {r["total"] for r in rows}
    assert totals == {3, 4}  # Jan-1 buckets 1+2; Jan-2 = 4
