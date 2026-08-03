"""Merge rules for fallback-marked registrations (mock-as-fallback composition).

The relaxation these cover is narrow on purpose: a *fallback* registration yields to a
real one instead of colliding, and nothing else changes. Every same-provenance overlap —
two real modules, or two fallback environments — still fails loud, in both merge orders.
"""

from itertools import combinations_with_replacement, permutations
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

    def test_a_real_registration_does_not_disarm_the_guard_between_two_fallbacks(self) -> None:
        # Two fallback environments colliding across plain/routed is an error on its own;
        # a real registration outranking both must not swallow it, or the second mock is
        # discovered as wrong behaviour instead of a wiring error. Judged pairwise, so the
        # winner is irrelevant.
        mock_plain = _fallback_plain()
        mock_routed = Deps.routed({_A: {"main": "mock-main"}}, fallback=True)

        for extra in (_real_plain(), Deps.routed({_A: {"main": "real-main"}})):
            for order in permutations((mock_plain, mock_routed, extra)):
                with pytest.raises(CoreException, match="registered both as plain and routed: a"):
                    Deps.merge(*order)

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

    def test_without_route_forgets_the_shadow_record_for_that_route(self) -> None:
        # A shadow record describes a slot; once the slot is gone the record is history
        # about nothing, and it would keep reporting the store as hybrid.
        merged = Deps.merge(
            Deps.routed({_R: {"main": "real", "spare": "real-spare"}}),
            Deps.routed({_R: {"main": "mock"}}, fallback=True),
        ).store

        assert merged.fallback_report().shadowed_names() == ("r (route 'main')",)

        pruned = merged.without_route(_R, "main")

        assert pruned.fallback_report().shadowed == ()
        assert not pruned.fallback_report().hybrid

    def test_without_forgets_the_shadow_records_for_that_key(self) -> None:
        merged = Deps.merge(_real_plain(), _fallback_plain()).store

        assert merged.fallback_report().shadowed_names() == ("a (plain)",)
        assert merged.without(_A).fallback_report().shadowed == ()


def _shape(kind: str, tag: str) -> Deps:
    """One store's registrations for ``_A``, tagged so the merge output identifies it."""

    marked = kind.endswith("fallback")

    if kind.startswith("plain"):
        return Deps.plain({_A: f"plain-{tag}"}, fallback=marked)

    if kind.startswith("route"):
        return Deps.routed({_A: {"main": f"route-{tag}"}}, fallback=marked)

    # A single store registering both: a catch-all *and* a specific route. Resolution
    # prefers the route and falls back to the plain entry, so this is a real shape a
    # module can author — and the one a cross-store rule most easily mishandles.
    return Deps(
        store=ProviderStore(
            plain_deps={_A: f"plain-{tag}"},
            routed_deps={_A: {"main": f"route-{tag}"}},
            fallback_plain=frozenset({_A}) if marked else frozenset(),
            fallback_routes={_A: frozenset({"main"})} if marked else {},
        )
    )


_SHAPES = (
    "plain-real",
    "plain-fallback",
    "route-real",
    "route-fallback",
    "both-real",
    "both-fallback",
)


def _tiers(blob: Deps) -> set[str]:
    """Which provenances this blob registers ``_A`` under (plain or route alike)."""

    store = blob.store
    tiers: set[str] = set()

    if _A in store.plain_deps:
        tiers.add("fallback" if _A in store.fallback_plain else "real")

    if "main" in (store.routed_deps.get(_A) or {}):
        tiers.add("fallback" if "main" in (store.fallback_routes.get(_A) or ()) else "real")

    return tiers


def _real_values(blob: Deps) -> tuple[str | None, str | None]:
    """The non-fallback plain value and route value this blob contributes, if any."""

    store = blob.store
    plain = store.plain_deps.get(_A) if _A not in store.fallback_plain else None
    routes = store.routed_deps.get(_A) or {}
    marked = store.fallback_routes.get(_A) or frozenset()

    return plain, (routes.get("main") if "main" not in marked else None)


class TestNoRealRegistrationIsEverSilentlyDropped:
    """Exhaustive over every small composition, because this is the property both merge
    bugs violated: a fallback exists to *yield*, so whatever survives a successful merge
    must still answer with the real registration's own provider — never a different one
    that happens to sit behind it.
    """

    @pytest.mark.parametrize("size", [2, 3])
    def test_every_composition_keeps_its_real_registrations(self, size: int) -> None:
        for kinds in combinations_with_replacement(_SHAPES, size):
            parts = [_shape(kind, str(i)) for i, kind in enumerate(kinds)]

            try:
                store = Deps.merge(*parts).store

            except CoreException:
                continue  # a conflict is a legitimate outcome; the property is about success

            expected_plain = [value for blob in parts if (value := _real_values(blob)[0])]
            expected_route = [value for blob in parts if (value := _real_values(blob)[1])]

            assert len(expected_plain) <= 1, f"{kinds}: two real plain entries should conflict"
            assert len(expected_route) <= 1, f"{kinds}: two real routes should conflict"

            if expected_plain:
                assert store.plain_deps.get(_A) == expected_plain[0], kinds

            if expected_route:
                # The route must survive *and* still answer with its own provider — being
                # dropped in favour of a real catch-all reads as "resolved" but is not.
                assert (store.routed_deps.get(_A) or {}).get("main") == expected_route[0], kinds
                assert store.get_provider(_A, route="main") == expected_route[0], kinds

    @pytest.mark.parametrize("size", [2, 3])
    def test_a_composition_conflicts_exactly_when_two_stores_share_a_tier(
        self, size: int
    ) -> None:
        # The other half of the bug class: a merge must raise when two *different* stores
        # claim the same slot at the same provenance — and must not raise otherwise. Stated
        # as a biconditional so neither a suppressed guard nor a spurious one can pass.
        for kinds in combinations_with_replacement(_SHAPES, size):
            parts = [_shape(kind, str(i)) for i, kind in enumerate(kinds)]

            same_tier_overlap = any(
                sum(1 for blob in parts if _tiers(blob) & {tier}) > 1 for tier in ("real", "fallback")
            )
            raised = _outcome(tuple(parts))[0] == "error"

            assert raised == same_tier_overlap, f"{kinds}: raised={raised}"

    @pytest.mark.parametrize("size", [2, 3])
    def test_every_composition_is_order_independent(self, size: int) -> None:
        for kinds in combinations_with_replacement(_SHAPES, size):
            parts = [_shape(kind, str(i)) for i, kind in enumerate(kinds)]
            outcomes = {_outcome(order) for order in permutations(parts)}

            assert len(outcomes) == 1, f"{kinds}: merge order changed the result — {outcomes}"


def _outcome(parts: tuple[Deps, ...]) -> tuple[Any, ...]:
    """What a merge of *parts* resolves to, or the error it raises — for comparison."""

    try:
        store = Deps.merge(*parts).store

    except CoreException as error:
        return ("error", str(error))

    return (
        "ok",
        store.plain_deps.get(_A),
        tuple(sorted((str(route), dep) for route, dep in (store.routed_deps.get(_A) or {}).items())),
        store.fallback_report().shadowed_names(),
        store.fallback_report().served_names(),
    )


class TestRegistrationIsASnapshot:
    """A registration blob is frozen, so what the caller does to the mapping afterwards
    must not reach it — least of all split the registrations from their provenance, which
    would turn an intended fallback into a same-tier conflict."""

    def test_plain_registrations_do_not_track_the_caller_mapping(self) -> None:
        deps = {_A: "mock-a"}
        blob = Deps.plain(deps, fallback=True)
        deps[_B] = "mock-b"

        assert set(blob.plain_deps) == {_A}
        # ...so a real registration of the late key composes instead of colliding.
        assert Deps.merge(blob, Deps.plain({_B: "real-b"})).store.get_provider(_B) == "real-b"

    def test_routed_registrations_do_not_track_the_caller_route_map(self) -> None:
        routes = {"main": "mock-main"}
        blob = Deps.routed({_R: routes}, fallback=True)
        routes["spare"] = "mock-spare"

        assert set(blob.routed_deps[_R]) == {"main"}
        assert (
            Deps.merge(blob, Deps.routed({_R: {"spare": "real-spare"}})).store.get_provider(
                _R, route="spare"
            )
            == "real-spare"
        )

    def test_routed_group_registrations_do_not_track_the_caller_route_set(self) -> None:
        routes = {"main"}
        blob = Deps.routed_group({_R: "mock"}, routes=routes, fallback=True)
        routes.add("spare")

        assert set(blob.routed_deps[_R]) == {"main"}

    def test_a_marked_registration_cannot_be_emptied_from_under_its_marks(self) -> None:
        # The mirror hazard: removing a key would leave a mark naming nothing registered.
        deps = {_A: "mock-a", _B: "mock-b"}
        blob = Deps.plain(deps, fallback=True)
        deps.pop(_A)

        assert set(blob.plain_deps) == set(blob.store.fallback_plain) == {_A, _B}


class TestReportIsASnapshot:
    def test_served_routes_cannot_be_edited_by_a_reader(self) -> None:
        # The report is frozen; a reader holding it must not be able to change what it
        # later says about the wiring.
        report = Deps.routed({_R: {"main": "mock"}}, fallback=True).fallback_report()

        with pytest.raises((TypeError, AttributeError)):
            report.served_routes.clear()  # type: ignore[attr-defined]

        with pytest.raises(TypeError):
            report.served_routes[_A] = frozenset({"x"})  # type: ignore[index]

        assert report.served_names() == ("r[main]",)
