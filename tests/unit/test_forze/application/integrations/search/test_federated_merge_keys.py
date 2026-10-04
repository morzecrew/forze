"""The federated RRF merge keys hits by id where it can and still merges as it did by record.

Keying a hit by its serialized record cost a ``model_dump`` and a sorted ``json.dumps`` per hit,
about half of a full-path federated search's CPU. A hit whose typed id is unique in its member is
now keyed by ``(member, type, id)``; any other keeps its record key. The merge and the highlights
must come out as the record-keyed versions they replace, which this suite keeps as oracles.
"""

from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import textwrap
from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import FederatedSearchReadModel
from forze.application.integrations.search import (
    build_federated_highlight_index,
    federated_highlights_for_hits,
)
from forze.application.integrations.search.snapshot import SearchResultSnapshot

# ----------------------- #


class _Hit(BaseModel):
    id: UUID
    label: str


class _NoId(BaseModel):
    label: str


class _MaybeId(BaseModel):
    id: UUID | None = None
    label: str


class _AnyId(BaseModel):
    id: Any
    label: str


class _Page:
    def __init__(self, hits: list[BaseModel], highlights: list[Any]) -> None:
        self.hits = hits
        self.highlights = highlights


def _record_key(member: str, hit: BaseModel) -> str:
    return f"{member}\0{json.dumps(hit.model_dump(mode='json'), sort_keys=True)}"


def _oracle(
    leg_rows: list[tuple[str, list[BaseModel], float]], k: int
) -> list[tuple[str, BaseModel, float]]:
    """The record-keyed merge as it stood before ``(member, id)`` keys."""

    scores: dict[str, float] = {}
    models: dict[str, tuple[str, BaseModel]] = {}

    for member, hits, weight in leg_rows:
        if weight <= 0.0:
            continue

        for rank, hit in enumerate(hits, start=1):
            key = _record_key(member, hit)
            scores[key] = scores.get(key, 0.0) + float(weight) / (float(k) + float(rank))
            models.setdefault(key, (member, hit))

    ordered = sorted(scores, key=lambda rk: (-scores[rk], models[rk][0], rk))

    return [(models[rk][0], models[rk][1], scores[rk]) for rk in ordered]


def _oracle_highlights(
    leg_rows: list[tuple[str, list[BaseModel], float]],
    leg_highlights: dict[str, list[dict[str, list[str]]]],
    merged: list[tuple[str, BaseModel, float]],
) -> list[Any]:
    """Highlights keyed by record, the first one per record kept, as the merge keeps hits."""

    index: dict[str, Any] = {}

    for member, hits, _w in leg_rows:
        for hit, hl in zip(hits, leg_highlights[member], strict=True):
            index.setdefault(_record_key(member, hit), hl)

    return [index.get(_record_key(member, hit), {}) for member, hit, _s in merged]


def _merge(
    leg_rows: list[tuple[str, list[BaseModel], float]], k: int
) -> list[tuple[str, BaseModel, float]]:
    merged = SearchResultSnapshot.weighted_rrf_merge_rows(leg_rows=leg_rows, k=k)

    return [(item.member, item.hit, score) for item, score in merged]


def _uuid(rng: random.Random) -> UUID:
    return UUID(int=rng.getrandbits(128))


# Ids an untyped ``id`` may hold. A UUID and its string share a JSON form, so a record carrying
# one equals a record carrying the other; the rest are distinct as records.
_UUID_ID = UUID(int=7)
_ANY_IDS: tuple[Any, ...] = (1, 1.0, True, 2, "1", "x", False, 0, _UUID_ID, str(_UUID_ID))


def _random_legs(rng: random.Random) -> list[tuple[str, list[BaseModel], float]]:
    """Legs with repeated ids (same or differing content), mixed id types and weights."""

    legs: list[tuple[str, list[BaseModel], float]] = []
    pool = [_uuid(rng) for _ in range(rng.randint(1, 30))]
    labels = ("a", "b", "c", "same")

    for m in range(rng.randint(1, 6)):
        kind = rng.choice((_Hit, _NoId, _MaybeId, _AnyId))
        # Usually one record per id, as a table returns it; sometimes a join repeats an id
        # with differing content.
        stable = {rid: rng.choice(labels) for rid in (*pool, *_ANY_IDS)}
        joins = rng.random() < 0.4
        hits: list[BaseModel] = []

        for _ in range(rng.randint(0, 40)):
            if kind is _NoId:
                hits.append(_NoId(label=rng.choice(labels)))
                continue
            if kind is _MaybeId and rng.random() < 0.3:
                hits.append(_MaybeId(id=None, label=rng.choice(labels)))
                continue

            rid = rng.choice(_ANY_IDS) if kind is _AnyId else rng.choice(pool)
            label = rng.choice(labels) if joins else stable[rid]
            hits.append(kind(id=rid, label=label))

        # A record repeated within a leg: the same content, so both keyings merge it.
        if hits and rng.random() < 0.5:
            hits.insert(rng.randint(0, len(hits)), hits[rng.randrange(len(hits))])

        weight = rng.choice((1.0, 0.5, 2.0, 0.0, -1.0))
        legs.append((f"m{m}", hits, weight))

    return legs


# ....................... #


class TestTheMergeMatchesTheRecordKeyedMerge:
    @pytest.mark.parametrize("seed", range(400))
    def test_on_random_legs(self, seed: int) -> None:
        rng = random.Random(seed)
        legs = _random_legs(rng)
        k = rng.choice((1, 10, 60))

        merged = _merge(legs, k)

        assert merged == _oracle(legs, k)

        # Each leg's hit gets a distinct highlight, so a wrong pairing shows.
        leg_hls = {m: [{"t": [f"{m}-{i}"]} for i in range(len(h))] for m, h, _w in legs}
        index = build_federated_highlight_index([(m, _Page(h, leg_hls[m])) for m, h, _w in legs])
        found = federated_highlights_for_hits(
            [FederatedSearchReadModel(hit=hit, member=m) for m, hit, _s in merged], index
        )

        assert (found or []) == (_oracle_highlights(legs, leg_hls, merged) if index else [])

    def test_a_joined_id_keeps_its_records_and_their_highlights(self) -> None:
        class _Row(BaseModel):
            id: str
            title: str

        leg: list[BaseModel] = [
            _Row(id="1", title="first"),
            _Row(id="1", title="second"),
            _Row(id="2", title="other"),
        ]
        hls = [{"t": ["hl-first"]}, {"t": ["hl-second"]}, {"t": ["hl-other"]}]
        merged = _merge([("a", leg, 1.0)], 60)
        found = federated_highlights_for_hits(
            [FederatedSearchReadModel(hit=hit, member=m) for m, hit, _s in merged],
            build_federated_highlight_index([("a", _Page(leg, hls))]),
        )

        assert [hit for _m, hit, _s in merged] == leg
        assert found == hls

    def test_equal_ids_of_different_types_stay_apart(self) -> None:
        hits: list[BaseModel] = [
            _AnyId(id=1, label="x"),
            _AnyId(id=1.0, label="x"),
            _AnyId(id=True, label="x"),
        ]

        assert len(_merge([("m", hits, 1.0)], 60)) == 3

    def test_a_tie_inside_one_member_breaks_by_record_as_before(self) -> None:
        # With k=10, a record repeated at ranks 6 and 38 scores exactly what rank 2 scores.
        early = _Hit(id=UUID(int=2), label="z")
        repeated = _Hit(id=UUID(int=1), label="a")
        hits: list[BaseModel] = [_Hit(id=UUID(int=100 + r), label="f") for r in range(1, 41)]
        hits[1] = early
        hits[5] = repeated
        hits[37] = repeated
        legs: list[tuple[str, list[BaseModel], float]] = [("m", hits, 1.0)]

        merged = _merge(legs, 10)
        tied = [hit for _m, hit, score in merged if score == pytest.approx(1 / 12)]

        assert tied == [repeated, early]
        assert merged == _oracle(legs, 10)

    def test_a_record_repeated_in_one_leg_is_merged_once(self) -> None:
        hit = _Hit(id=UUID(int=1), label="x")
        legs: list[tuple[str, list[BaseModel], float]] = [("m", [hit, hit], 1.0)]

        merged = _merge(legs, 60)

        assert len(merged) == 1
        assert merged[0][2] == pytest.approx(1 / 61 + 1 / 62)

    def test_the_same_record_in_two_members_stays_two_hits(self) -> None:
        hit = _Hit(id=UUID(int=1), label="x")

        merged = _merge([("a", [hit], 1.0), ("b", [hit], 1.0)], 60)

        assert [(member, score) for member, _h, score in merged] == [
            ("a", pytest.approx(1 / 61)),
            ("b", pytest.approx(1 / 61)),
        ]


class _NoDump(_Hit):
    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        if kwargs.get("include") != {"id"}:
            raise AssertionError("a hit with a unique id was serialized whole to key it")

        return super().model_dump(*args, **kwargs)


class TestAnIdHitIsKeyedWithoutSerializingTheRecord:
    def test_the_merge_never_dumps_a_hit_that_has_an_id(self) -> None:
        hits: list[BaseModel] = [_NoDump(id=UUID(int=i), label="x") for i in range(3)]

        assert len(_merge([("m", hits, 1.0)], 60)) == 3

    def test_the_highlight_index_never_dumps_one_either(self) -> None:
        hit = _NoDump(id=UUID(int=1), label="x")

        index = build_federated_highlight_index([("m", _Page([hit], [{"label": ["x"]}]))])

        assert federated_highlights_for_hits(
            [FederatedSearchReadModel(hit=hit, member="m")], index
        ) == [{"label": ["x"]}]


class _ListId(BaseModel):
    id: list[int]
    label: str


class TestAnUnhashableIdFallsBackToTheRecord:
    def test_the_merge_keys_it_by_record(self) -> None:
        hits: list[BaseModel] = [_ListId(id=[1], label="x"), _ListId(id=[2], label="y")]

        assert [hit for _m, hit, _s in _merge([("m", hits, 1.0)], 60)] == hits

    def test_a_tuple_holding_a_list_merges_like_any_other_id(self) -> None:
        hits: list[BaseModel] = [_AnyId(id=(1, []), label="x"), _AnyId(id=(2, []), label="y")]
        legs: list[tuple[str, list[BaseModel], float]] = [("m", hits, 1.0)]

        assert _merge(legs, 60) == _oracle(legs, 60)


class TestAnIdEqualAsJsonIsOneId:
    def test_a_uuid_and_its_string_are_the_record_they_render_as(self) -> None:
        same: list[BaseModel] = [
            _AnyId(id=_UUID_ID, label="x"),
            _AnyId(id=str(_UUID_ID), label="x"),
        ]
        apart: list[BaseModel] = [
            _AnyId(id=_UUID_ID, label="x"),
            _AnyId(id=str(_UUID_ID), label="y"),
        ]

        for hits in (same, apart):
            legs: list[tuple[str, list[BaseModel], float]] = [("m", hits, 1.0)]
            assert _merge(legs, 60) == _oracle(legs, 60)

        assert len(_merge([("m", same, 1.0)], 60)) == 1
        assert len(_merge([("m", apart, 1.0)], 60)) == 2

    def test_a_mapping_id_is_one_id_whatever_its_key_order(self) -> None:
        hits: list[BaseModel] = [
            _AnyId(id={"a": 1, "b": 2}, label="x"),
            _AnyId(id={"b": 2, "a": 1}, label="x"),
        ]
        legs: list[tuple[str, list[BaseModel], float]] = [("m", hits, 1.0)]

        assert _merge(legs, 60) == _oracle(legs, 60)
        assert len(_merge(legs, 60)) == 1


class TestHighlightsFollowTheirHit:
    def test_each_merged_hit_gets_its_own_legs_highlight(self) -> None:
        a = _Hit(id=UUID(int=1), label="x")
        b = _NoId(label="y")

        index = build_federated_highlight_index(
            [
                ("m1", _Page([a], [{"label": ["<em>x</em>"]}])),
                ("m2", _Page([b], [{"label": ["y"]}])),
            ]
        )
        merged = [
            FederatedSearchReadModel(hit=b, member="m2"),
            FederatedSearchReadModel(hit=a, member="m1"),
        ]

        assert federated_highlights_for_hits(merged, index) == [
            {"label": ["y"]},
            {"label": ["<em>x</em>"]},
        ]


class TestTheOrderIsTheSameInEveryProcess:
    def test_across_hash_seeds(self) -> None:
        script = textwrap.dedent(
            """
            import json, random
            from uuid import UUID
            from pydantic import BaseModel
            from forze.application.integrations.search.snapshot import SearchResultSnapshot

            class H(BaseModel):
                id: UUID
                label: str

            rng = random.Random(7)
            pool = [UUID(int=rng.getrandbits(128)) for _ in range(50)]
            legs = [
                (f"m{m}", [H(id=rng.choice(pool), label="same") for _ in range(40)], 1.0)
                for m in range(5)
            ]
            merged = SearchResultSnapshot.weighted_rrf_merge_rows(leg_rows=legs, k=60)
            print(json.dumps([[i.member, str(i.hit.id), s] for i, s in merged]))
            """
        )
        outputs = set()

        for seed in ("0", "1", "42", "1234"):
            env = {**os.environ, "PYTHONHASHSEED": seed}
            out = subprocess.run(
                [sys.executable, "-c", script], env=env, capture_output=True, text=True, check=True
            )
            outputs.add(out.stdout)

        assert len(outputs) == 1
