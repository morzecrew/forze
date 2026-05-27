"""Tests for runtime trace debug logging."""

from __future__ import annotations

import os
from unittest.mock import patch

from forze.application.execution import Deps, runtime_tracer_from_flag
from forze.application.execution.tracing.emit import init_runtime_tracing, record
from forze.application.execution.tracing.log import log_runtime_trace


class TestLogRuntimeTrace:
    def test_noop_when_env_unset(self) -> None:
        deps = Deps(runtime_tracer=runtime_tracer_from_flag(True))
        init_runtime_tracing(deps)
        record(deps=deps, domain="document", op="query")

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("FORZE_RUNTIME_TRACE_LOG", None)
            log_runtime_trace(deps)

    def test_logs_when_env_truthy(self) -> None:
        deps = Deps(runtime_tracer=runtime_tracer_from_flag(True))
        init_runtime_tracing(deps)
        record(deps=deps, domain="document", op="query")

        with patch.dict(os.environ, {"FORZE_RUNTIME_TRACE_LOG": "true"}):
            with patch("forze.application.execution.tracing.log.logger") as log_mock:
                log_runtime_trace(deps)

        log_mock.debug.assert_called_once()
        assert log_mock.debug.call_args[0][1] == 1

    def test_skips_when_trace_empty(self) -> None:
        deps = Deps(runtime_tracer=runtime_tracer_from_flag(True))

        with patch.dict(os.environ, {"FORZE_RUNTIME_TRACE_LOG": "1"}):
            with patch("forze.application.execution.tracing.log.logger") as log_mock:
                log_runtime_trace(deps)

        log_mock.debug.assert_not_called()
