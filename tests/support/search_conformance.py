"""Shared search-plane conformance battery: the portable contract, on every backend.

The search plane has four independent query implementations — the in-memory oracle, Postgres
FTS, Mongo text, and Meilisearch — and unlike the other planes, most of what they do is
*meant* to differ. Stemming, tokenization, scoring and tie-breaking are the engine's
business, and a battery that tried to unify them would be wrong.

So this battery deliberately asserts only the **structural** contract: the part a caller
writes portable code against, where a divergence is a bug rather than a characteristic.

- Membership is decided by the corpus, not by ranking: every document contains the probe
  term, so any check about *which* documents come back is engine-independent.
- Ordering is only asserted under an **explicit sort**. Relevance order is never compared.
- Counts, page windows, filters, projections and empty results are compared exactly.

What each check pins:

1. A query matching nothing is an empty page with a zero count, not an error.
2. A page's ``count`` agrees with the hits it returns when the limit exceeds the total.
3. ``limit``/``offset`` windows tile the ordered result set — no overlap, no gap.
4. An offset past the end is an empty page whose count still reports the true total.
5. An exact-predicate filter narrows the matching set to exactly the matching documents.
6. An explicit sort determines page order, overriding relevance.
7. Projection returns exactly the requested fields.
8. ``search`` and ``search_page`` agree on hits; only the total distinguishes them.

Not asserted, on purpose: blank-query semantics. ``search("")`` means "everything, filters
only" on some engines and "nothing" on others, and which one is right is a genuine product
question rather than a defect — see :func:`check_a_blank_query_is_declared_not_guessed`,
which pins each backend's *declared* answer instead of forcing agreement.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.querying import UNSUPPORTED_QUERY_FEATURE_CODE
from forze.application.contracts.search import (
    SearchCommandPort,
    SearchManagementPort,
    SearchQueryPort,
)
from forze.base.exceptions import CoreException

# ----------------------- #

PROBE_TERM = "python"
"""A term every corpus document contains, so relevance never decides membership."""

CORPUS: tuple[tuple[str, str, str], ...] = (
    ("alpha guide", "python basics", "books"),
    ("beta guide", "python advanced", "books"),
    ("gamma notes", "python tips", "notes"),
    ("delta notes", "python tricks", "notes"),
)
"""``(title, content, category)`` rows every leg seeds identically.

Titles are distinct and sort unambiguously (alpha/beta/delta/gamma), so an explicit sort
has exactly one correct answer on every engine. ``category`` splits the corpus 2/2 for the
filter check and is never searched, so it stays an exact predicate rather than a text match.
"""

TITLES_ASC = ("alpha guide", "beta guide", "delta notes", "gamma notes")
"""The corpus titles in ascending order — the expected order under an explicit sort."""

NOTES_TITLES = ("delta notes", "gamma notes")
"""Titles of the two ``category="notes"`` rows, sorted."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchHarness:
    """One backend's seam for the battery.

    Only the port is needed: seeding is the fixture's job, because the four backends load a
    corpus in four incompatible ways — Postgres and Mongo search their system of record
    (rows inserted by the test), the oracle reads the mock document store, and Meilisearch
    owns a derived index written through a command port. That difference is architectural,
    not something a shared seam could paper over.
    """

    query: SearchQueryPort[Any]
    """A port over a spec whose searchable fields are ``title`` and ``content``."""

    backend: str
    """Label used in assertion messages, so a failure names the leg that disagreed."""

    blank_query_matches_all: bool
    """What ``search("")`` means on this backend — declared, not discovered.

    An empty query is "match everything, filters only" on an engine that treats the term as
    absent, and "match nothing" on one that compiles it to an empty predicate. Both are
    defensible, so the battery records the answer per backend rather than forcing one; what
    it does enforce is that the answer is *one of the two* and is stable.
    """


Check = Callable[[SearchHarness], Any]
"""One battery check. Async, but typed loosely so the tuple stays homogeneous."""


# ....................... #


def _titles(page: Any) -> list[str]:
    return [hit.title for hit in page.hits]


async def _all_titles(h: SearchHarness) -> list[str]:
    page = await h.query.search(PROBE_TERM, None, {"limit": 50}, {"title": "asc"})

    return _titles(page)


# ....................... #


async def check_a_zero_match_query_is_an_empty_page(h: SearchHarness) -> None:
    """No match is an empty page with a zero count — never an error, never a null."""

    page = await h.query.search_page("zzzznotacorpustermzzzz")

    assert page.hits == [] or list(page.hits) == [], h.backend
    assert page.count == 0, h.backend


async def check_page_count_matches_the_hits_it_returns(h: SearchHarness) -> None:
    """With a limit above the total, the count is the number of hits — not an estimate.

    Backends that only report an estimated total declare it via
    ``SearchCapabilities.exact_total_count``; the assertion relaxes to a lower bound for
    those rather than pretending the estimate is exact.
    """

    page = await h.query.search_page(PROBE_TERM, None, {"limit": 50})

    assert len(page.hits) == len(CORPUS), h.backend

    if h.query.search_capabilities.exact_total_count:
        assert page.count == len(CORPUS), h.backend

    else:
        assert page.count >= len(CORPUS), h.backend


async def check_limit_offset_windows_partition_the_result_set(h: SearchHarness) -> None:
    """Successive windows tile the ordered set: no document repeated, none skipped.

    Asserted under an explicit sort, so the tiling is checked against one well-defined
    order rather than against whatever each engine's relevance ranking happens to be.
    """

    sorts = {"title": "asc"}
    first = await h.query.search(PROBE_TERM, None, {"limit": 2, "offset": 0}, sorts)
    second = await h.query.search(PROBE_TERM, None, {"limit": 2, "offset": 2}, sorts)

    assert _titles(first) == list(TITLES_ASC[:2]), h.backend
    assert _titles(second) == list(TITLES_ASC[2:]), h.backend


async def check_an_offset_past_the_end_is_an_empty_page(h: SearchHarness) -> None:
    """A window beyond the last hit is empty, and the total still tells the truth.

    The total is what a caller needs to render "page 40 of 2" instead of a blank screen, so
    an empty window must not zero it.
    """

    page = await h.query.search_page(PROBE_TERM, None, {"limit": 5, "offset": 100})

    assert list(page.hits) == [], h.backend

    if h.query.search_capabilities.exact_total_count:
        assert page.count == len(CORPUS), h.backend


async def check_an_exact_filter_narrows_the_matching_set(h: SearchHarness) -> None:
    """A filter on a non-searched field is an exact predicate, not a relevance signal."""

    page = await h.query.search_page(PROBE_TERM, {"$values": {"category": {"$eq": "notes"}}}, {"limit": 50})

    assert sorted(_titles(page)) == list(NOTES_TITLES), h.backend

    if h.query.search_capabilities.exact_total_count:
        assert page.count == len(NOTES_TITLES), h.backend


async def check_an_explicit_sort_orders_the_page(h: SearchHarness) -> None:
    """An explicit sort decides order — relevance does not get a vote."""

    ascending = await h.query.search(PROBE_TERM, None, {"limit": 50}, {"title": "asc"})
    descending = await h.query.search(PROBE_TERM, None, {"limit": 50}, {"title": "desc"})

    assert _titles(ascending) == list(TITLES_ASC), h.backend
    assert _titles(descending) == list(reversed(TITLES_ASC)), h.backend


async def check_projection_returns_only_the_requested_fields(h: SearchHarness) -> None:
    """A projected row carries the requested keys and nothing else."""

    page = await h.query.project_search(["title"], PROBE_TERM, None, {"limit": 2})

    assert len(page.hits) == 2, h.backend

    for row in page.hits:
        assert set(row) == {"title"}, f"{h.backend}: {row}"


async def check_countless_and_page_agree_on_hits(h: SearchHarness) -> None:
    """Asking for a total must not change which documents come back.

    ``search`` skips the count query for speed; if that skip also changed the hits, the
    cheap call would not be a drop-in for the expensive one.
    """

    sorts = {"title": "asc"}
    countless = await h.query.search(PROBE_TERM, None, {"limit": 3}, sorts)
    counted = await h.query.search_page(PROBE_TERM, None, {"limit": 3}, sorts)

    assert _titles(countless) == _titles(counted), h.backend


async def check_a_blank_query_is_declared_not_guessed(h: SearchHarness) -> None:
    """``search("")`` follows the backend's declared reading, and does not error.

    The two readings — "everything, filters only" and "nothing" — are both defensible, so
    this check does not unify them. It pins that the backend gives one of them, stably, and
    that the harness's declaration is the truthful one: a caller relying on empty-query
    behaviour has to know it per backend, and a silent flip would break them either way.
    """

    try:
        page = await h.query.search_page("", None, {"limit": 50})

    except CoreException as e:  # pragma: no cover — only if a backend refuses outright
        pytest.fail(f"{h.backend}: a blank query raised instead of resolving: {e}")

    if h.blank_query_matches_all:
        assert len(page.hits) == len(CORPUS), h.backend

    else:
        assert list(page.hits) == [], h.backend


# ....................... #

async def check_windows_partition_under_a_non_unique_sort(h: SearchHarness) -> None:
    """Windows tile even when the sort key ties — the classic source of lost rows.

    Sorting by a field that splits the corpus 2/2 gives the engine no total order of its
    own, so an unstable tie-break shows up as a document appearing in both pages while
    another appears in neither. Order within a tie is not asserted (no engine promises
    one); coverage is.
    """

    sorts = {"category": "asc"}
    first = await h.query.search(PROBE_TERM, None, {"limit": 2, "offset": 0}, sorts)
    second = await h.query.search(PROBE_TERM, None, {"limit": 2, "offset": 2}, sorts)

    seen = _titles(first) + _titles(second)

    assert sorted(seen) == sorted(TITLES_ASC), f"{h.backend}: {seen}"


async def check_phrase_combine_is_honored(h: SearchHarness) -> None:
    """A term list is OR by default and AND under ``phrase_combine="all"``.

    An adapter that ignored the knob would answer the conjunction with the disjunction's
    hits — a silently wider result set, which is the failure mode a caller cannot see. The
    unsatisfiable conjunction is the discriminator: it must come back empty.
    """

    disjunction = await h.query.search_page(
        [PROBE_TERM, "zzzznope"], None, {"limit": 50}, options={"phrase_combine": "any"}
    )
    unsatisfiable = await h.query.search_page(
        [PROBE_TERM, "zzzznope"], None, {"limit": 50}, options={"phrase_combine": "all"}
    )
    conjunction = await h.query.search_page(
        [PROBE_TERM, "notes"], None, {"limit": 50}, options={"phrase_combine": "all"}
    )

    assert len(disjunction.hits) == len(CORPUS), h.backend
    assert list(unsatisfiable.hits) == [], h.backend
    assert sorted(_titles(conjunction)) == list(NOTES_TITLES), h.backend


async def check_the_stream_gate_matches_the_declaration(h: SearchHarness) -> None:
    """A backend serves ``search_stream`` if and only if it declares ``supports_stream``.

    Templated on the inference capability gate, but the shape it found is different and
    worth stating: the search oracle does **not** advertise a superset. It declares the same
    narrow surface Postgres and Mongo do, so there is no untold-mock divergence to mirror
    away — which is why this check asserts self-consistency per backend instead.

    That is the property with teeth here anyway. The capability's own contract says an
    offset-only backend must *refuse* the stream rather than emulate it via deep offset,
    "which would silently truncate" — a backend quietly serving a stream it declared it
    could not is how a bounded-memory export comes back short with no error at all. Both
    directions are checked, because a backend that refuses everything satisfies half of
    this just as well as a correct one does.
    """

    declared = h.query.search_capabilities.supports_stream

    if declared:
        # The stream yields CHUNKS, not hits — the bounded-memory unit is the page.
        streamed = [
            hit.title
            async for chunk in h.query.search_stream(PROBE_TERM, None, None)
            for hit in chunk
        ]

        assert sorted(streamed) == sorted(await _all_titles(h)), (
            f"{h.backend}: declares supports_stream but the stream and the page disagree"
        )

        return

    with pytest.raises(CoreException) as refused:
        async for _chunk in h.query.search_stream(PROBE_TERM, None, None):
            pass

    assert refused.value.code == UNSUPPORTED_QUERY_FEATURE_CODE, (
        f"{h.backend}: refused the undeclared stream, but not as a capability gate"
    )


SEARCH_BATTERY: tuple[Check, ...] = (
    check_the_stream_gate_matches_the_declaration,
    check_a_zero_match_query_is_an_empty_page,
    check_page_count_matches_the_hits_it_returns,
    check_limit_offset_windows_partition_the_result_set,
    check_an_offset_past_the_end_is_an_empty_page,
    check_an_exact_filter_narrows_the_matching_set,
    check_an_explicit_sort_orders_the_page,
    check_projection_returns_only_the_requested_fields,
    check_countless_and_page_agree_on_hits,
    check_a_blank_query_is_declared_not_guessed,
    check_windows_partition_under_a_non_unique_sort,
    check_phrase_combine_is_honored,
)


# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchWriteHarness:
    """One backend's seam for the *write*-plane battery.

    Only two backends have one: the oracle and Meilisearch. Postgres and Mongo search their
    system of record, so there is no index to provision or wipe and they implement neither
    port — they are absent from this battery rather than skipped in it.
    """

    command: SearchCommandPort[Any]
    """Data-plane writer (``upsert`` / ``delete``)."""

    management: SearchManagementPort
    """Control-plane provisioning (``ensure_index`` / ``delete_all``)."""

    query: SearchQueryPort[Any]
    """A reader over the same surface, so a write's *effect* is observable.

    Without it every write check could only assert "did not raise", which is exactly how a
    wipe that quietly does nothing passes an idempotency test.
    """

    backend: str

    new_row: Callable[[str], Any]
    """Build one document with the given ``title`` and a fresh id — the model differs per
    backend spec, so constructing it belongs to the fixture."""


WriteCheck = Callable[[SearchWriteHarness], Any]
"""One write-plane battery check."""


# ....................... #


async def _indexed_titles(h: SearchWriteHarness) -> list[str]:
    page = await h.query.search_page("", None, {"limit": 50})

    return sorted(hit.title for hit in page.hits)


async def check_delete_all_empties_the_index(h: SearchWriteHarness) -> None:
    """The wipe actually wipes — the control the idempotency checks depend on.

    Without this, ``delete_all`` could be a total no-op and every "wiping twice is safe"
    assertion below would still pass.
    """

    await h.management.ensure_index()
    await h.command.upsert([h.new_row("to be wiped")])

    assert await _indexed_titles(h) == ["to be wiped"], h.backend

    await h.management.delete_all()

    assert await _indexed_titles(h) == [], h.backend


async def check_delete_all_on_an_unprovisioned_index_is_a_no_op(h: SearchWriteHarness) -> None:
    """Wiping a surface nothing has created yet succeeds — it already holds no documents.

    This is the documented wipe-then-rebuild path on a fresh deployment. Meilisearch failed
    it (``index_not_found``) while the oracle succeeded, so the workflow the docs recommend
    was verified only against the backend that could not have caught the problem.
    """

    await h.management.delete_all()


async def check_delete_all_is_idempotent(h: SearchWriteHarness) -> None:
    """A second wipe is not an error — a retried teardown must not fail the run."""

    await h.management.ensure_index()
    await h.command.upsert([h.new_row("doomed")])
    await h.management.delete_all()
    await h.management.delete_all()

    assert await _indexed_titles(h) == [], h.backend


async def check_ensure_index_is_idempotent(h: SearchWriteHarness) -> None:
    """Provisioning twice reconciles settings; it does not fail on the second startup."""

    await h.management.ensure_index()
    await h.management.ensure_index()


async def check_deleting_an_absent_id_is_a_no_op(h: SearchWriteHarness) -> None:
    """Removing something that is not there is success — the postcondition holds."""

    await h.management.ensure_index()
    await h.management.delete_all()
    await h.command.upsert([h.new_row("survivor")])

    await h.command.delete([str(uuid4())])

    assert await _indexed_titles(h) == ["survivor"], h.backend


async def check_empty_batches_are_no_ops(h: SearchWriteHarness) -> None:
    """An empty write is a no-op, not an error and not a wipe."""

    await h.management.ensure_index()
    await h.management.delete_all()
    await h.command.upsert([h.new_row("kept")])

    await h.command.upsert([])
    await h.command.upsert_many([])
    await h.command.delete([])

    assert await _indexed_titles(h) == ["kept"], h.backend


async def check_upserting_the_same_id_twice_replaces(h: SearchWriteHarness) -> None:
    """The same primary key upserted twice is one document, holding the later value."""

    await h.management.ensure_index()
    await h.management.delete_all()

    first = h.new_row("first version")
    second = first.model_copy(update={"title": "second version"})

    await h.command.upsert([first])
    await h.command.upsert([second])

    assert await _indexed_titles(h) == ["second version"], h.backend


# ....................... #

SEARCH_WRITE_BATTERY: tuple[WriteCheck, ...] = (
    check_delete_all_empties_the_index,
    check_delete_all_on_an_unprovisioned_index_is_a_no_op,
    check_delete_all_is_idempotent,
    check_ensure_index_is_idempotent,
    check_deleting_an_absent_id_is_a_no_op,
    check_empty_batches_are_no_ops,
    check_upserting_the_same_id_twice_replaces,
)


# ....................... #


def corpus_rows(id_factory: Callable[[], Any]) -> list[dict[str, Any]]:
    """The corpus as plain records, ready for whichever writer a backend needs."""

    return [
        {"id": id_factory(), "title": title, "content": content, "category": category}
        for title, content, category in CORPUS
    ]


def searchable_fields() -> Sequence[str]:
    """The spec's searchable fields — ``category`` is excluded so it stays a predicate."""

    return ("title", "content")
