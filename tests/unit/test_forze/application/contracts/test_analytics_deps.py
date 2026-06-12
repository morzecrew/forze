"""Tests for :mod:`forze.application.contracts.analytics.deps`."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from forze.application.contracts.analytics.deps import AnalyticsDeps
from forze.base.exceptions import CoreException


class TestAnalyticsDeps:
    def test_command_is_alias_for_ingest(self) -> None:
        port = object()
        ctx = MagicMock()
        ctx.inv_ctx.is_read_only.return_value = False
        ctx.deps.resolve_configurable.return_value = port
        deps = AnalyticsDeps()
        deps.lock(ctx)
        spec = MagicMock()

        assert deps.ingest(spec) is port
        assert deps.command(spec) is port
        assert ctx.deps.resolve_configurable.call_count == 2
        first, second = ctx.deps.resolve_configurable.call_args_list
        assert first == second

    def test_ingest_guarded_in_read_only_operation(self) -> None:
        ctx = MagicMock()
        ctx.inv_ctx.is_read_only.return_value = True
        deps = AnalyticsDeps()
        deps.lock(ctx)

        with pytest.raises(CoreException, match="read-only"):
            deps.ingest(MagicMock())

    def test_command_guarded_in_read_only_operation(self) -> None:
        ctx = MagicMock()
        ctx.inv_ctx.is_read_only.return_value = True
        deps = AnalyticsDeps()
        deps.lock(ctx)

        with pytest.raises(CoreException, match="read-only"):
            deps.command(MagicMock())
