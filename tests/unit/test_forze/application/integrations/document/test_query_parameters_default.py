"""Default ``with_parameters`` on the integration ``DocumentAdapter``.

A backend that can't apply query parameters inherits the base behaviour: validate the contract,
then fail closed so the parameters are never silently ignored. Backends that do support the channel
(Postgres, mock) override this.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.integrations.document import DocumentAdapter, DocumentCache
from forze.base.exceptions import CoreException
from forze.domain.models import Document
from tests.unit._gateway_codec_helpers import codec_for

# ----------------------- #


class _Row(Document):
    title: str = "x"


class _Params(BaseModel):
    window: str = "2026-01-01"


class _FakeReadGw:
    model_type = _Row
    tenant_aware = False


def _adapter() -> DocumentAdapter:
    spec = DocumentSpec(name="report", read=_Row, query_params=_Params)
    cache = DocumentCache(
        read_model_type=_Row,
        read_codec=codec_for(_Row),
        document_name=spec.name,
        cache=None,
        after_commit=None,
    )
    return DocumentAdapter(spec=spec, read_gw=_FakeReadGw(), document_cache=cache)


# ....................... #


def test_with_parameters_fails_closed_on_unsupporting_backend() -> None:
    with pytest.raises(CoreException, match="query_parameters_unsupported"):
        _adapter().with_parameters(_Params())


def test_with_parameters_validates_contract_first() -> None:
    class _Other(BaseModel):
        x: int = 1

    # The contract check runs before the unsupported-backend refusal.
    with pytest.raises(CoreException, match="query_parameters_type_mismatch"):
        _adapter().with_parameters(_Other())
