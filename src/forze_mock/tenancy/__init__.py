"""Multi-tenancy helpers for ``forze_mock``."""

from .mixin import MockTenancyMixin
from .namespace import resolve_mock_namespace, resolve_mock_namespace_sync
from .partition import partition_namespace
from .routed import (
    MockRoutedStateDepKey,
    MockRoutedStateRegistry,
    mock_routed_state_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "MockTenancyMixin",
    "partition_namespace",
    "resolve_mock_namespace",
    "resolve_mock_namespace_sync",
    "MockRoutedStateRegistry",
    "MockRoutedStateDepKey",
    "mock_routed_state_lifecycle_step",
]
