"""The wire names ``forze_load`` restates, pinned to the server's own constants.

``forze_load`` is a client and never imports ``forze_fastapi`` (import-linter), so a rename on
the server side must fail here rather than turn every operation reference into a mismatch.
"""

from __future__ import annotations

import pytest

from forze.application.contracts.tenancy.tenant_hint import TENANT_ID_HEADER
from forze.application.execution.operations import OperationKind
from forze_fastapi.routes._attach import FORZE_KIND_EXTENSION
from forze_load.catalog import COMMAND, KIND_EXTENSION, QUERY
from forze_load.plan import TENANT_HEADER

pytestmark = pytest.mark.unit

# ----------------------- #


def test_the_kind_key_is_the_one_forze_fastapi_writes() -> None:
    assert KIND_EXTENSION == FORZE_KIND_EXTENSION


def test_the_kind_values_are_the_catalog_s() -> None:
    assert {QUERY, COMMAND} == {kind.value for kind in OperationKind}
    assert OperationKind.QUERY.value == QUERY


def test_the_tenant_header_is_the_one_forze_reads() -> None:
    assert TENANT_HEADER == TENANT_ID_HEADER
