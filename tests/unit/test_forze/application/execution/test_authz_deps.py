"""Tests for ``ctx.authz`` on :class:`~forze.application.execution.context.execution.ExecutionContext`."""

from __future__ import annotations

import pytest

from forze.application.contracts.authz import AuthzSpec
from forze.application.execution import Deps, ExecutionContext
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps

pytestmark = pytest.mark.unit


def test_execution_context_has_authz_deps() -> None:
    ctx = context_from_deps(Deps())

    assert ctx.authz is not None
