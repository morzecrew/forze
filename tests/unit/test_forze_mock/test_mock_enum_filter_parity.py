"""The mock matches a filter on a plain ``Enum`` field by the member's value, as a store does.

The Postgres twin is ``tests/integration/test_forze_postgres/test_pg_enum_filter_parity.py``.
"""

from __future__ import annotations

from enum import Enum
from uuid import UUID

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.querying import evaluate_filter
from forze_mock.adapters import MockDocumentAdapter, MockState
from tests.support.enum_filter_parity import (
    ROWS,
    Status,
    StatusCreate,
    StatusDoc,
    StatusRead,
    assert_enum_cases,
)

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_enum_filters_match_on_mock() -> None:
    doc = MockDocumentAdapter(
        spec=DocumentSpec(
            name="enum_rows",
            read=StatusRead,
            write=DocumentWriteTypes(domain=StatusDoc, create_cmd=StatusCreate),
        ),
        state=MockState(),
        namespace="enum_rows",
        read_model=StatusRead,
        domain_model=StatusDoc,
    )
    # Created through the port, each row keeps the member, not the value a filter names.
    ids = {key: (await doc.create(StatusCreate.model_validate(row))).id for key, row in ROWS.items()}

    await assert_enum_cases(doc, ids, backend="mock")


class _Ref(Enum):
    A = UUID("00000000-0000-0000-0000-00000000000a")


class _Unhashable(Enum):
    A = ["x"]


def test_a_uuid_valued_member_matches_its_string_form() -> None:
    assert evaluate_filter({"ref": _Ref.A}, {"$values": {"ref": str(_Ref.A.value)}})


def test_a_member_with_an_unhashable_value_still_compares_as_a_set_member() -> None:
    row = {"tags": [_Unhashable.A]}

    assert not evaluate_filter(row, {"$values": {"tags": {"$overlaps": ["x"]}}})
    assert evaluate_filter(row, {"$values": {"tags": {"$disjoint": ["x"]}}})


@pytest.mark.parametrize("held", [_Ref.A, _Ref.A.value], ids=["member", "uuid"])
def test_set_operators_match_a_uuid_by_its_string_form_as_in_does(held: object) -> None:
    row = {"refs": [held]}
    text = [str(_Ref.A.value)]

    assert evaluate_filter(row, {"$values": {"refs": {"$in": text}}})

    for op in ("$overlaps", "$superset", "$subset"):
        assert evaluate_filter(row, {"$values": {"refs": {op: text}}}), op

    assert not evaluate_filter(row, {"$values": {"refs": {"$disjoint": text}}})


class _Maybe(Enum):
    NONE = None
    SOME = "some"


def test_a_member_whose_value_is_none_reads_as_null() -> None:
    """Postgres stores the member as ``NULL``; a missing field still reads as null too."""

    assert evaluate_filter({"x": _Maybe.NONE}, {"$values": {"x": {"$null": True}}})
    assert not evaluate_filter({"x": _Maybe.NONE}, {"$values": {"x": {"$null": False}}})
    assert evaluate_filter({"x": _Maybe.SOME}, {"$values": {"x": {"$null": False}}})
    assert evaluate_filter({}, {"$values": {"x": {"$null": True}}})
    assert not evaluate_filter({}, {"$values": {"x": {"$null": False}}})


def test_field_comparison_reads_members_inside_an_array_as_values() -> None:
    row = {"a": [Status.OPEN], "b": ["open"], "c": {"k": Status.OPEN}, "d": {"k": "open"}}

    assert evaluate_filter(row, {"$fields": {"a": {"$eq": "b"}}})
    assert evaluate_filter(row, {"$fields": {"c": {"$eq": "d"}}})
    assert not evaluate_filter(row, {"$fields": {"a": {"$neq": "b"}}})
