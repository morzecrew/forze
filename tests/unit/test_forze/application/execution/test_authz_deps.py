"""Tests for ``ctx.authz`` on :class:`~forze.application.execution.context.execution.ExecutionContext`."""

from __future__ import annotations

import pytest

from forze.application.contracts.authz import AuthzSpec
from forze.application.execution import Deps, ExecutionContext

pytestmark = pytest.mark.unit


def test_execution_context_has_authz_deps() -> None:
    ctx = ExecutionContext(deps=Deps())

    assert ctx.authz is not None
