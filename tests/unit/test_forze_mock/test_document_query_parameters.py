"""Query-parameter channel: contract + capability gate (fail-closed until a backend executes it)."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException
from forze.domain.models import ReadDocument
from forze_mock.execution import MockDepsModule
from tests.support.execution_context import context_from_deps

# ----------------------- #


class _Read(ReadDocument):
    region: str = "eu"


class _Window(BaseModel):
    window: str = "2026-01-01"


def test_with_parameters_unsupported_fails_closed() -> None:
    spec = DocumentSpec(name="sales", read=_Read, query_params=_Window)
    ctx = context_from_deps(MockDepsModule()())
    with pytest.raises(CoreException, match="query_parameters_unsupported"):
        ctx.document.query(spec).with_parameters(_Window())


def test_with_parameters_undeclared_rejected() -> None:
    spec = DocumentSpec(name="plain", read=_Read)  # no query_params contract
    ctx = context_from_deps(MockDepsModule()())
    with pytest.raises(CoreException, match="query_parameters_undeclared"):
        ctx.document.query(spec).with_parameters(_Window())


def test_with_parameters_type_mismatch_rejected() -> None:
    spec = DocumentSpec(name="sales", read=_Read, query_params=_Window)
    ctx = context_from_deps(MockDepsModule()())

    class _Other(BaseModel):
        x: int = 1

    with pytest.raises(CoreException, match="query_parameters_type_mismatch"):
        ctx.document.query(spec).with_parameters(_Other())
