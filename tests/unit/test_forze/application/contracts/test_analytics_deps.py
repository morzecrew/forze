"""Tests for :mod:`forze.application.contracts.analytics.deps`."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from forze.application.contracts.analytics.deps import AnalyticsDeps
from forze.base.exceptions import CoreException


class TestAnalyticsDeps:
    def test_command_is_alias_for_ingest(self) -> None:
        port = MagicMock(marker="real")
        ctx = MagicMock()
        ctx.inv_ctx.is_read_only.return_value = False
        ctx.deps.resolve_configurable.return_value = port
        deps = AnalyticsDeps()
        deps.lock(ctx)
        spec = MagicMock()

        assert deps.ingest(spec).marker == "real"
        assert deps.command(spec).marker == "real"
        first, second = ctx.deps.resolve_configurable.call_args_list[:2]
        assert first == second

    def test_ingest_guarded_in_read_only_operation(self) -> None:
        ctx = MagicMock()
        ctx.inv_ctx.is_read_only.return_value = True
        deps = AnalyticsDeps()
        deps.lock(ctx)

        port = deps.ingest(MagicMock())  # held, not used: allowed

        with pytest.raises(CoreException, match="read-only"):
            _ = port.append

    def test_command_guarded_in_read_only_operation(self) -> None:
        ctx = MagicMock()
        ctx.inv_ctx.is_read_only.return_value = True
        deps = AnalyticsDeps()
        deps.lock(ctx)

        port = deps.command(MagicMock())

        with pytest.raises(CoreException, match="read-only"):
            _ = port.append
