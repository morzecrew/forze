"""Dependency keys for mock integration."""

from forze.application.contracts.deps import DepKey

from forze_mock.state import MockState
from forze_mock.tenancy import MockRoutedStateRegistry

# ----------------------- #

MockStateDepKey: DepKey[MockState] = DepKey("mock_state")
"""Dependency key for shared :class:`~forze_mock.state.MockState`."""

MockRoutedStateDepKey: DepKey[MockRoutedStateRegistry] = DepKey("mock_routed_state")
"""Dependency key for :class:`~forze_mock.tenancy.MockRoutedStateRegistry`."""
