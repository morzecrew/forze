"""Unit tests for :class:`ProviderStore`."""

import pytest

from forze.application.contracts.deps import DepKey, ProviderStore
from forze.application.execution.deps.resolution import frame_for
from forze.base.exceptions import CoreException, ExceptionKind

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


class TestProviderStoreInventory:
    def test_registered_frames(self) -> None:
        store = ProviderStore(plain_deps={_A: 1}).merge(
            ProviderStore(routed_deps={_R: {"u": 2, "v": 3}}),
        )
        frames = store.registered_frames()

        assert frame_for(_A, None) in frames
        assert frame_for(_R, "u") in frames
        assert frame_for(_R, "v") in frames
