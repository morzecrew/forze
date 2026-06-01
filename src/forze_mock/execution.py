"""Backward-compatible re-exports for mock execution wiring."""

from forze_mock.execution import (
    MockDepsModule,
    MockRouteConfig,
    MockRoutedStateDepKey,
    MockStateDepKey,
    mock_txmanager,
)

__all__ = [
    "MockStateDepKey",
    "MockRoutedStateDepKey",
    "MockRouteConfig",
    "MockDepsModule",
    "mock_txmanager",
]
