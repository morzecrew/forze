"""Unit tests for :class:`ProviderStore`."""

import pickle

import pytest

from forze.application.contracts.deps import DepKey, ProviderStore, ShadowedFallback
from forze.application.execution.deps.resolution import frame_for
from forze.base.exceptions import CoreException, ExceptionKind

_A = DepKey[str]("a")
_B = DepKey[str]("b")
_R = DepKey[str]("r")


class TestDepKeyHash:
    def test_cached_hash_follows_equality(self) -> None:
        key = DepKey[str]("a")
        hash(key)  # fill the cache

        assert key == _A and hash(key) == hash(_A)
        assert key != _B and hash(key) != hash(_B)

    def test_unpickled_key_hashes_like_the_original(self) -> None:
        # The cache is dropped on unpickle: a string hash differs across processes.
        hash(_A)
        copy = pickle.loads(pickle.dumps(_A))

        assert copy == _A and hash(copy) == hash(_A)
        assert {_A: 1}[copy] == 1


class TestProviderStoreConstruction:
    def test_routed_map_must_not_be_empty(self) -> None:
        with pytest.raises(CoreException, match="no routes"):
            ProviderStore(routed_deps={_R: {}})

    def test_shadow_records_are_deduplicated_and_sorted(self) -> None:
        late, early = ShadowedFallback(key=_R, route="x"), ShadowedFallback(key=_A)

        assert ProviderStore(shadowed_fallbacks=(late, early, late)).shadowed_fallbacks == (
            early,
            late,
        )


class TestProviderStoreGetProvider:
    def test_plain_not_found_raises(self) -> None:
        with pytest.raises(CoreException, match="Plain dependency"):
            ProviderStore().get_provider(_A)

    def test_plain_not_found_is_configuration_with_registered_hint(self) -> None:
        # A missing dependency is a server-side wiring (configuration) mistake, not an
        # internal crash, and the error names what *is* registered to make it actionable.
        store = ProviderStore(plain_deps={DepKey[int]("already_here"): 1})

        with pytest.raises(CoreException) as ei:
            store.get_provider(_A)

        assert ei.value.kind is ExceptionKind.CONFIGURATION
        assert "already_here" in str(ei.value)
        assert "DepsModule" in str(ei.value)

    def test_routed_not_found_is_configuration_with_route_hint(self) -> None:
        store = ProviderStore(routed_deps={_R: {"known": "v"}})

        with pytest.raises(CoreException) as ei:
            store.get_provider(_R, route="missing", fallback_to_plain=False)

        assert ei.value.kind is ExceptionKind.CONFIGURATION
        assert "known" in str(ei.value)

    def test_routed_fallback_to_plain(self) -> None:
        store = ProviderStore(plain_deps={_A: "plain"}, routed_deps={_R: {"z": "z"}})

        assert store.get_provider(_A, route="missing", fallback_to_plain=True) == "plain"


class TestProviderStoreMerge:
    def test_merge_plain_conflict_raises(self) -> None:
        a = ProviderStore(plain_deps={_A: 1})
        b = ProviderStore(plain_deps={_A: 2})

        with pytest.raises(CoreException, match="Conflicting plain"):
            ProviderStore.merge(a, b)

    def test_merge_combines_routes(self) -> None:
        a = ProviderStore(routed_deps={_R: {"x": 1}})
        b = ProviderStore(routed_deps={_R: {"y": 2}})
        merged = ProviderStore.merge(a, b)

        assert merged.get_provider(_R, route="x") == 1
        assert merged.get_provider(_R, route="y") == 2

    def test_merge_of_one_store_is_that_store(self) -> None:
        store = ProviderStore(plain_deps={_A: 1}, routed_deps={_R: {"x": 2}})

        assert ProviderStore.merge(store) is store

    def test_disjoint_merge_keeps_order_marks_and_shadows(self) -> None:
        # Stores sharing no key merge as a union; it keeps what resolving overlaps would.
        hybrid = ProviderStore.merge(
            ProviderStore(plain_deps={_A: "real"}),
            ProviderStore(plain_deps={_A: "mock"}, fallback_plain={_A}),
        )
        other = ProviderStore(
            plain_deps={_B: "mock-b"},
            routed_deps={_R: {"x": 1, "y": 2}},
            fallback_plain={_B},
            fallback_routes={_R: {"y"}},
        )
        merged = ProviderStore.merge(other, hybrid)

        assert list(merged.plain_deps.items()) == [(_B, "mock-b"), (_A, "real")]
        assert list(merged.routed_deps[_R].items()) == [("x", 1), ("y", 2)]
        assert merged.fallback_plain == {_B}
        assert merged.fallback_routes == {_R: frozenset({"y"})}
        assert merged.shadowed_fallbacks == (ShadowedFallback(key=_A),)
        assert merged.fallback_report().hybrid


class TestProviderStoreInventory:
    def test_registered_frames(self) -> None:
        store = ProviderStore(plain_deps={_A: 1}).merge(
            ProviderStore(routed_deps={_R: {"u": 2, "v": 3}}),
        )
        frames = store.registered_frames()

        assert frame_for(_A, None) in frames
        assert frame_for(_R, "u") in frames
        assert frame_for(_R, "v") in frames
