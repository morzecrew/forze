"""Backend-agnostic query-DSL parity corpus + a shared runner.

One fixed seed dataset and a list of filter cases, each with the **expected matching
rows** (hand-authored — the oracle). :func:`run_parity_cases` seeds a document port,
runs every case, and asserts the matched rows equal the oracle — for cases the
backend's :class:`QueryCapabilities` support; for the rest it asserts the backend
rejects them with the clean ``query_feature_unsupported`` error rather than returning
wrong rows or a 500.

The in-memory mock (full capabilities) is the canonical reference: a unit test runs
this corpus against it, pinning the expected rows. Each real backend then runs the
*same* corpus (over testcontainers) and must reproduce the mock — which is how
semantic divergences (e.g. a backend mis-evaluating ``$all`` with an ordering
predicate) surface as a failing parity case rather than silently.
"""

from __future__ import annotations

from typing import Any, cast

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.querying import (
    UNSUPPORTED_QUERY_FEATURE_CODE,
    QueryCapabilities,
    QueryFilterExpressionParser,
    validate_query_capabilities,
)
from forze.base.exceptions import CoreException
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument

# ----------------------- #
# Models exercised by the corpus (scalars, scalar array, object array, nullable)


class CorpusItem(BaseModel):
    sku: str
    qty: int
    tags: list[str] = []  # sub-array, for nested-quantifier cases


class _CorpusFields(BaseModel):
    name: str
    nick: str = ""
    age: int = 0
    tags: list[str] = []
    nums: list[int] = []
    score: int | None = None
    items: list[CorpusItem] = []


class CorpusCreate(CreateDocumentCmd, _CorpusFields):
    pass


class CorpusDoc(Document, _CorpusFields):
    pass


class CorpusRead(ReadDocument, _CorpusFields):
    pass


# ....................... #
# Seed dataset (stable string keys → row). Expected sets reference the keys.

SEED: dict[str, CorpusCreate] = {
    "alice": CorpusCreate(
        name="alice", nick="alice", age=30, tags=["x", "y"], nums=[2, 5], score=10,
        items=[
            CorpusItem(sku="a", qty=5, tags=["hot"]),
            CorpusItem(sku="b", qty=1, tags=["cold"]),
        ],
    ),
    "bob": CorpusCreate(
        name="bob", nick="robert", age=25, tags=["y", "z"], nums=[1, 3], score=None,
        items=[CorpusItem(sku="a", qty=2, tags=["cold"])],
    ),
    "carol": CorpusCreate(
        name="carol", nick="carol", age=40, tags=[], nums=[], score=5, items=[],
    ),
    "dave": CorpusCreate(
        name="dave", nick="dave", age=30, tags=["x"], nums=[9], score=10,
        items=[CorpusItem(sku="c", qty=9, tags=["hot", "new"])],
    ),
}


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class QueryCase:
    """One filter and the row keys it must match (the oracle)."""

    name: str
    filters: dict[str, Any]
    expected: frozenset[str]


CASES: tuple[QueryCase, ...] = (
    QueryCase(name="eq", filters={"$values": {"name": {"$eq": "alice"}}},
              expected=frozenset({"alice"})),
    QueryCase(name="ord_gt", filters={"$values": {"age": {"$gt": 28}}},
              expected=frozenset({"alice", "carol", "dave"})),
    QueryCase(name="membership_in", filters={"$values": {"name": {"$in": ["alice", "bob"]}}},
              expected=frozenset({"alice", "bob"})),
    QueryCase(name="null_true", filters={"$values": {"score": {"$null": True}}},
              expected=frozenset({"bob"})),
    # Text op — rejected by search/MVP backends.
    QueryCase(name="text_regex", filters={"$values": {"name": {"$regex": "^a"}}},
              expected=frozenset({"alice"})),
    # Set ops on the scalar array — rejected by search/MVP backends.
    QueryCase(name="set_superset", filters={"$values": {"tags": {"$superset": ["x", "y"]}}},
              expected=frozenset({"alice"})),
    QueryCase(name="set_overlaps", filters={"$values": {"tags": {"$overlaps": ["z"]}}},
              expected=frozenset({"bob"})),
    # Element quantifiers — rejected by search/MVP backends.
    QueryCase(name="quant_any_scalar", filters={"$values": {"tags": {"$any": "x"}}},
              expected=frozenset({"alice", "dave"})),
    QueryCase(name="quant_all_scalar_vacuous",
              filters={"$values": {"tags": {"$all": "x"}}},
              # all-elements-equal-x: dave [x]; carol [] vacuously true.
              expected=frozenset({"carol", "dave"})),
    # Ordering predicate inside a quantifier — the suspect path: a backend that
    # renders $all via a min/max equality shortcut gets this wrong.
    QueryCase(name="quant_all_scalar_ordering",
              filters={"$values": {"nums": {"$all": {"$gte": 2}}}},
              # nums: alice[2,5] all>=2; bob[1,3] no; carol[] vacuous; dave[9] yes.
              expected=frozenset({"alice", "carol", "dave"})),
    QueryCase(name="quant_any_scalar_ordering",
              filters={"$values": {"nums": {"$any": {"$gt": 4}}}},
              # nums: alice[2,5] 5>4; dave[9] yes; bob[1,3] no; carol[] no.
              expected=frozenset({"alice", "dave"})),
    # RANGE inside a quantifier (newly allowed — the multi-op element constraint).
    QueryCase(name="quant_any_scalar_range",
              filters={"$values": {"nums": {"$any": {"$gt": 1, "$lt": 3}}}},
              # element strictly in (1,3) i.e. ==2: only alice[2,5].
              expected=frozenset({"alice"})),
    QueryCase(name="quant_all_scalar_range",
              filters={"$values": {"nums": {"$all": {"$gte": 1, "$lte": 5}}}},
              # all in [1,5]: alice[2,5], bob[1,3], carol[] vacuous; dave[9] no.
              expected=frozenset({"alice", "bob", "carol"})),
    QueryCase(name="quant_any_object_range",
              filters={"$values": {"items": {"$any": {"$values": {"qty": {"$gt": 1, "$lt": 6}}}}}},
              # any item qty in (1,6): alice(5), bob(2); carol none; dave(9) no.
              expected=frozenset({"alice", "bob"})),
    QueryCase(name="quant_all_object_range",
              filters={"$values": {"items": {"$all": {"$values": {"qty": {"$gt": 1, "$lt": 9}}}}}},
              # all items qty in (1,9): bob[2]; carol[] vacuous; alice has 1 (no); dave 9 (no).
              expected=frozenset({"bob", "carol"})),
    # Membership inside a quantifier (Slice B — $in/$nin in element predicates).
    QueryCase(name="quant_any_scalar_in",
              filters={"$values": {"tags": {"$any": {"$in": ["z", "w"]}}}},
              # any tag in {z,w}: bob[y,z]; others none.
              expected=frozenset({"bob"})),
    QueryCase(name="quant_any_scalar_nin",
              filters={"$values": {"tags": {"$any": {"$nin": ["x"]}}}},
              # any tag not in {x}: alice[x,y]->y, bob[y,z]; dave[x] only x (no); carol[] none.
              expected=frozenset({"alice", "bob"})),
    QueryCase(name="quant_any_object_in",
              filters={"$values": {"items": {"$any": {"$values": {"sku": {"$in": ["a"]}}}}}},
              # any item sku in {a}: alice[a,b], bob[a]; carol none; dave[c] no.
              expected=frozenset({"alice", "bob"})),
    # NESTED quantifiers (Slice C): any item that has a 'hot' tag (sub-array).
    QueryCase(name="nested_any_any",
              filters={"$values": {"items": {"$any": {"$values": {"tags": {"$any": "hot"}}}}}},
              # alice item a [hot]; dave item c [hot,new]; bob [cold] no; carol none.
              expected=frozenset({"alice", "dave"})),
    QueryCase(name="nested_any_all",
              filters={"$values": {"items": {"$any": {"$values": {"tags": {"$all": "hot"}}}}}},
              # any item whose tags are ALL 'hot': alice item a [hot]; dave [hot,new] no.
              expected=frozenset({"alice"})),
    QueryCase(name="nested_any_none",
              filters={"$values": {"items": {"$any": {"$values": {"tags": {"$none": "cold"}}}}}},
              # any item with NO 'cold' tag: alice item a [hot]; dave item c; bob[cold] no.
              expected=frozenset({"alice", "dave"})),
    # Nested quantifiers under an OUTER $all/$none — the cases that need the aggregation
    # ($expr) rendering on Mongo (query-form negation can't De-Morgan a nested $elemMatch).
    QueryCase(name="nested_all_any",
              filters={"$values": {"items": {"$all": {"$values": {"tags": {"$any": "hot"}}}}}},
              # every item has a 'hot' tag: alice item b [cold] no; bob [cold] no;
              # carol [] vacuous; dave item c [hot,new] yes.
              expected=frozenset({"carol", "dave"})),
    QueryCase(name="nested_all_all",
              filters={"$values": {"items": {"$all": {"$values": {"tags": {"$all": "hot"}}}}}},
              # every item's tags are ALL 'hot': alice item b [cold] no; bob no;
              # carol [] vacuous; dave [hot,new] no. → only carol.
              expected=frozenset({"carol"})),
    QueryCase(name="nested_all_none",
              filters={"$values": {"items": {"$all": {"$values": {"tags": {"$none": "cold"}}}}}},
              # every item has NO 'cold' tag: alice item b [cold] no; bob [cold] no;
              # carol [] vacuous; dave [hot,new] yes.
              expected=frozenset({"carol", "dave"})),
    QueryCase(name="nested_none_any",
              filters={"$values": {"items": {"$none": {"$values": {"tags": {"$any": "hot"}}}}}},
              # no item has a 'hot' tag: alice (item a hot) no; bob [cold] yes;
              # carol [] vacuous; dave (hot) no.
              expected=frozenset({"bob", "carol"})),
    QueryCase(name="nested_none_all",
              filters={"$values": {"items": {"$none": {"$values": {"tags": {"$all": "hot"}}}}}},
              # no item whose tags are ALL 'hot': alice (item a all-hot) no; bob yes;
              # carol [] vacuous; dave [hot,new] not all-hot → yes.
              expected=frozenset({"bob", "carol", "dave"})),
    QueryCase(name="quant_any_object",
              filters={"$values": {"items": {"$any": {"$values": {"qty": {"$gte": 5}}}}}},
              expected=frozenset({"alice", "dave"})),
    # Field-to-field compare — rejected by search/MVP backends.
    QueryCase(name="field_compare", filters={"$fields": {"name": {"$eq": "nick"}}},
              expected=frozenset({"alice", "carol", "dave"})),
    # Negation — rejected by Firestore MVP.
    QueryCase(name="negation", filters={"$not": {"$values": {"name": {"$eq": "alice"}}}},
              expected=frozenset({"bob", "carol", "dave"})),
    # Combinator nesting (all-supported ops).
    QueryCase(
        name="and_quant",
        filters={"$and": [
            {"$values": {"age": {"$gte": 30}}},
            {"$values": {"tags": {"$any": "x"}}},
        ]},
        expected=frozenset({"alice", "dave"}),
    ),
)


# ....................... #


def case_supported_by(filters: dict[str, Any], caps: QueryCapabilities) -> bool:
    """Whether *caps* advertises every feature *filters* uses (via the real validator).

    Using :func:`validate_query_capabilities` itself as the skip oracle keeps the corpus
    free of hand-maintained feature tags — a case is "supported" exactly when the
    backend would not reject it.
    """

    try:
        validate_query_capabilities(
            QueryFilterExpressionParser.parse(cast(Any, filters)), caps, backend="probe"
        )
        return True

    except CoreException as error:
        if error.code == UNSUPPORTED_QUERY_FEATURE_CODE:
            return False

        raise


# ....................... #


@attrs.define(slots=True)
class CombinedDocPort:
    """Adapt a separate command + query port pair into one create/find_many port.

    The mock adapter is a single object; real backends split command and query ports,
    so integration tests wrap them with this to feed :func:`run_parity_cases`.
    """

    command: Any
    query: Any

    async def create(self, cmd: Any) -> Any:
        return await self.command.create(cmd)

    async def find_many(self, *, filters: Any, pagination: Any) -> Any:
        return await self.query.find_many(filters=filters, pagination=pagination)


async def run_parity_cases(
    doc: Any,
    caps: QueryCapabilities,
    *,
    backend: str,
) -> None:
    """Seed the corpus into *doc* and assert every case against the oracle.

    Supported cases must match the expected row keys; unsupported cases must be
    rejected with ``query_feature_unsupported``. Works for the mock and every real
    document backend — the only per-backend input is its *caps*.
    """

    key_to_id: dict[str, Any] = {}

    for key, cmd in SEED.items():
        created = await doc.create(cmd)
        key_to_id[key] = created.id

    id_to_key = {value: key for key, value in key_to_id.items()}

    for case in CASES:
        label = f"{backend}/{case.name}"

        if case_supported_by(case.filters, caps):
            page = await doc.find_many(filters=case.filters, pagination={"limit": 1000})
            got = frozenset(id_to_key[hit.id] for hit in page.hits)

            assert got == case.expected, f"{label}: matched {sorted(got)} != {sorted(case.expected)}"

        else:
            with pytest.raises(CoreException) as ei:
                await doc.find_many(filters=case.filters, pagination={"limit": 1000})

            assert ei.value.code == UNSUPPORTED_QUERY_FEATURE_CODE, f"{label}: wrong error"
