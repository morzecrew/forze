"""Merge rules for fallback-marked registrations (mock-as-fallback composition).

The relaxation these cover is narrow on purpose: a *fallback* registration yields to a
real one instead of colliding, and nothing else changes. Every same-provenance overlap —
two real modules, or two fallback environments — still fails loud, in both merge orders.
"""

from itertools import permutations
from typing import Any

import pytest

from forze.application.contracts.deps import DepKey, Deps, ProviderStore
from forze.base.exceptions import CoreException

_A = DepKey[str]("a")
_B = DepKey[str]("b")
_R = DepKey[str]("r")

# ....................... #


def _real_plain(value: str = "real") -> Deps:
    return Deps.plain({_A: value})


def _fallback_plain(value: str = "mock") -> Deps:
    return Deps.plain({_A: value}, fallback=True)


def _merged_both_ways(left: Deps, right: Deps) -> tuple[ProviderStore, ProviderStore]:
    """Merge a pair in both orders — provenance, not position, must decide."""

    return Deps.merge(left, right).store, Deps.merge(right, left).store


# ....................... #


class TestFallbackBeatsNothing:
    def test_marks_are_inert_without_overlap(self) -> None:
        # The standalone fallback environment (the mock alone) resolves exactly as before.
        store = Deps.merge(
            Deps.plain({_A: "mock"}, fallback=True),
            Deps.plain({_B: "mock-b"}, fallback=True),
        ).store

        assert store.get_provider(_A) == "mock"
        assert store.get_provider(_B) == "mock-b"

    def test_pure_fallback_store_is_not_hybrid(self) -> None:
        report = Deps.plain({_A: "mock"}, fallback=True).fallback_report()

        assert not report.hybrid
        assert report.served_plain == {_A}

    def test_pure_real_store_is_not_hybrid_and_reports_nothing(self) -> None:
        report = Deps.merge(_real_plain(), Deps.routed({_R: {"main": 1}})).fallback_report()

        assert not report.hybrid
        assert report.empty


class TestPlainOverPlain:
    def test_real_plain_beats_fallback_plain_in_both_orders(self) -> None:
        for store in _merged_both_ways(_real_plain(), _fallback_plain()):
            assert store.get_provider(_A) == "real"
            # Nothing is fallback-served any more, but the displacement is on the record.
            assert store.fallback_plain == frozenset()
            assert store.fallback_report().shadowed_names() == ("a (plain)",)
            assert store.fallback_report().hybrid

    def test_two_real_plain_still_conflict(self) -> None:
        with pytest.raises(CoreException, match="Conflicting plain dependencies: a"):
            Deps.merge(_real_plain("one"), _real_plain("two"))

    def test_two_fallback_plain_still_conflict(self) -> None:
        # Two fallback environments in one context is a wiring bug, not a resolution question.
        with pytest.raises(CoreException, match="Conflicting plain dependencies: a"):
            Deps.merge(_fallback_plain("one"), _fallback_plain("two"))

    def test_real_plain_beside_a_second_real_and_a_fallback_still_conflicts(self) -> None:
        # A fallback in the mix never disarms the guard between the two real registrations.
        with pytest.raises(CoreException, match="Conflicting plain dependencies: a"):
            Deps.merge(_real_plain("one"), _fallback_plain(), _real_plain("two"))


class TestRoutedOverPlain:
    def test_real_routes_coexist_with_a_fallback_catch_all(self) -> None:
        real = Deps.routed({_A: {"main": "real-main"}})

        for store in _merged_both_ways(real, _fallback_plain()):
            assert store.get_provider(_A, route="main") == "real-main"
            # The whole point of the hybrid: everything the real module does not cover
            # keeps resolving through the fallback.
            assert store.get_provider(_A, route="other") == "mock"
            assert store.get_provider(_A) == "mock"
            assert store.fallback_report().served_names() == ("a",)

    def test_real_plain_displaces_fallback_routes(self) -> None:
        # A real plain catch-all has to *win*, so the fallback routes cannot stay: routed
        # lookup runs first and they would answer ahead of it.
        fallback_routed = Deps.routed({_A: {"main": "mock-main"}}, fallback=True)

        for store in _merged_both_ways(_real_plain(), fallback_routed):
            assert store.get_provider(_A, route="main") == "real"
            assert store.routed_deps == {}
            assert store.fallback_report().shadowed_names() == ("a (route 'main')",)

    def test_real_plain_and_real_routed_still_conflict(self) -> None:
        with pytest.raises(CoreException, match="registered both as plain and routed: a"):
            Deps.merge(_real_plain(), Deps.routed({_A: {"main": "also-real"}}))

    def test_fallback_plain_and_fallback_routed_still_conflict(self) -> None:
        with pytest.raises(CoreException, match="registered both as plain and routed: a"):
            Deps.merge(
                _fallback_plain(),
                Deps.routed({_A: {"main": "mock-main"}}, fallback=True),
            )

    def test_one_store_may_register_a_key_plain_and_routed(self) -> None:
        # Unchanged: within a single store this is a deliberate catch-all, not a collision.
        store = ProviderStore(plain_deps={_A: "plain"}, routed_deps={_A: {"main": "routed"}})
        merged = ProviderStore.merge(store, ProviderStore(plain_deps={_B: 1}))

        assert merged.get_provider(_A, route="main") == "routed"
        assert merged.get_provider(_A, route="other") == "plain"


class TestRoutedOverRouted:
    def test_real_route_wins_and_other_fallback_routes_survive(self) -> None:
        real = Deps.routed({_R: {"main": "real-main"}})
        fallback = Deps.routed({_R: {"main": "mock-main", "spare": "mock-spare"}}, fallback=True)

        for store in _merged_both_ways(real, fallback):
            assert store.get_provider(_R, route="main") == "real-main"
            assert store.get_provider(_R, route="spare") == "mock-spare"
            assert store.fallback_routes == {_R: frozenset({"spare"})}
            assert store.fallback_report().shadowed_names() == ("r (route 'main')",)

    def test_two_real_routes_still_conflict(self) -> None:
        with pytest.raises(CoreException, match="Conflicting routed dependencies for 'r': main"):
            Deps.merge(Deps.routed({_R: {"main": 1}}), Deps.routed({_R: {"main": 2}}))

    def test_two_fallback_routes_still_conflict(self) -> None:
        with pytest.raises(CoreException, match="Conflicting routed dependencies for 'r': main"):
            Deps.merge(
                Deps.routed({_R: {"main": 1}}, fallback=True),
                Deps.routed({_R: {"main": 2}}, fallback=True),
            )

    def test_disjoint_real_routes_still_union(self) -> None:
        store = Deps.merge(Deps.routed({_R: {"x": 1}}), Deps.routed({_R: {"y": 2}})).store

        assert store.get_provider(_R, route="x") == 1
        assert store.get_provider(_R, route="y") == 2

    def test_routed_group_can_be_marked_fallback(self) -> None:
        fallback = Deps.routed_group({_R: "mock"}, routes={"main", "spare"}, fallback=True)
        store = Deps.merge(Deps.routed({_R: {"main": "real"}}), fallback).store

        assert store.get_provider(_R, route="main") == "real"
        assert store.get_provider(_R, route="spare") == "mock"


class TestMergeOrderIndependence:
    def test_every_permutation_resolves_identically(self) -> None:
        parts = (
            Deps.plain({_A: "mock-a", _B: "mock-b"}, fallback=True),
            Deps.routed({_A: {"main": "real-a"}}),
            Deps.plain({_B: "real-b"}),
        )

        results: set[tuple[Any, ...]] = set()

        for order in permutations(parts):
            store = Deps.merge(*order).store
            results.add(
                (
                    store.get_provider(_A, route="main"),
                    store.get_provider(_A, route="typo"),
                    store.get_provider(_B),
                    store.fallback_report().served_names(),
                    store.fallback_report().shadowed_names(),
                )
            )

        assert results == {(("real-a"), "mock-a", "real-b", ("a",), ("b (plain)",))}

    def test_chained_merge_matches_flat_merge(self) -> None:
        fallback = Deps.plain({_A: "mock", _B: "mock-b"}, fallback=True)
        real_routed = Deps.routed({_A: {"main": "real-a"}})
        real_plain = Deps.plain({_B: "real-b"})

        chained = Deps.merge(Deps.merge(fallback, real_routed), real_plain).store
        flat = Deps.merge(fallback, real_routed, real_plain).store

        assert chained.plain_deps == flat.plain_deps
        assert chained.routed_deps == flat.routed_deps
        assert chained.fallback_plain == flat.fallback_plain
        assert chained.fallback_report().shadowed_names() == flat.fallback_report().shadowed_names()

    def test_marks_survive_a_merge_so_later_merges_reapply_them(self) -> None:
        # A fallback that survives one merge is still a fallback for the next one.
        once = Deps.merge(Deps.plain({_A: "mock"}, fallback=True), Deps.plain({_B: "mock-b"}))
        twice = Deps.merge(once, Deps.plain({_A: "real"})).store

        assert twice.get_provider(_A) == "real"

    def test_a_conflict_between_reals_survives_a_merge_boundary(self) -> None:
        once = Deps.merge(Deps.plain({_A: "mock"}, fallback=True), Deps.plain({_A: "real"}))

        with pytest.raises(CoreException, match="Conflicting plain dependencies: a"):
            Deps.merge(once, Deps.plain({_A: "other-real"}))


class TestCatchAllIsTheHazardSet:
    """``catch_all`` separates "this plane is mocked" from "this plane is real, with a mock
    behind it" — only the second can absorb a mistyped route, so only it is a hazard."""

    def test_a_fallback_plain_under_real_routes_is_a_catch_all(self) -> None:
        store = Deps.merge(Deps.routed({_A: {"main": "real"}}), _fallback_plain()).store

        assert store.fallback_report().catch_all_names() == ("a",)

    def test_a_fallback_plain_nobody_routed_is_not(self) -> None:
        store = Deps.merge(_fallback_plain(), Deps.plain({_B: "real-b"})).store

        assert store.fallback_report().catch_all_names() == ()
        assert store.fallback_report().served_names() == ("a",)

    def test_fallback_routes_over_a_fallback_plain_do_not_count(self) -> None:
        # Both sides fallback: an unwired plane, not a half-real one. (One store may
        # author this pair; merging two of them is still a conflict.)
        store = ProviderStore(
            plain_deps={_A: "mock"},
            routed_deps={_A: {"main": "mock-main"}},
            fallback_plain=frozenset({_A}),
            fallback_routes={_A: frozenset({"main"})},
        )

        assert store.fallback_report().catch_all_names() == ()


class TestTypoRouteHazard:
    def test_a_mistyped_route_reaches_the_fallback_and_the_report_says_so(self) -> None:
        # The residual hazard the RFC accepts: in a context that includes a fallback
        # environment, a route the real module never registered resolves through the
        # fallback instead of failing. The report is the mitigation — it names the key.
        store = Deps.merge(
            Deps.routed({_A: {"main": "real"}}),
            Deps.plain({_A: "mock"}, fallback=True),
        ).store

        assert store.get_provider(_A, route="tyop") == "mock"
        assert store.fallback_report().hybrid
        assert "a" in store.fallback_report().served_names()


class TestFallbackMarkValidation:
    def test_marking_an_unregistered_plain_key_raises(self) -> None:
        with pytest.raises(CoreException, match="unregistered plain dependencies: a"):
            ProviderStore(plain_deps={_B: 1}, fallback_plain=frozenset({_A}))

    def test_marking_an_unregistered_route_raises(self) -> None:
        with pytest.raises(CoreException, match="unregistered routes for 'r': ghost"):
            ProviderStore(routed_deps={_R: {"main": 1}}, fallback_routes={_R: frozenset({"ghost"})})


class TestFallbackMarksSurviveCopies:
    def test_without_drops_the_key_and_its_marks(self) -> None:
        store = Deps.plain({_A: "mock", _B: "mock-b"}, fallback=True).store.without(_A)

        assert store.fallback_plain == frozenset({_B})

    def test_without_route_keeps_the_remaining_marks(self) -> None:
        store = Deps.routed(
            {_R: {"main": "mock", "spare": "mock2"}},
            fallback=True,
        ).store.without_route(_R, "main")

        assert store.fallback_routes == {_R: frozenset({"spare"})}

    def test_without_route_drops_the_key_entirely_when_it_empties(self) -> None:
        store = Deps.routed({_R: {"main": "mock"}}, fallback=True).store.without_route(_R, "main")

        assert store.routed_deps == {}
        assert store.fallback_routes == {}
