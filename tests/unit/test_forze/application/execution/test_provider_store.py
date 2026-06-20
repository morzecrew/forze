"""Unit tests for :class:`ProviderStore`."""

import pytest

from forze.application.contracts.deps import DepKey
from forze.application.execution.deps.resolution import frame_for
from forze.application.contracts.deps import ProviderStore
from forze.base.exceptions import CoreException

_A = DepKey[str]("a")
_R = DepKey[str]("r")


class TestProviderStoreConstruction:
    def test_routed_map_must_not_be_empty(self) -> None:
        with pytest.raises(CoreException, match="no routes"):
            ProviderStore(routed_deps={_R: {}})


class TestProviderStoreGetProvider:
    def test_plain_not_found_raises(self) -> None:
        with pytest.raises(CoreException, match="Plain dependency"):
            ProviderStore().get_provider(_A)

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


class TestProviderStoreInventory:
    def test_registered_frames(self) -> None:
        store = ProviderStore(plain_deps={_A: 1}).merge(
            ProviderStore(routed_deps={_R: {"u": 2, "v": 3}}),
        )
        frames = store.registered_frames()

        assert frame_for(_A, None) in frames
        assert frame_for(_R, "u") in frames
        assert frame_for(_R, "v") in frames
