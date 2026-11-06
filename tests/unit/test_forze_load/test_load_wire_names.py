"""The wire names ``forze_load`` restates, pinned to the server's own constants.

``forze_load`` is a client and never imports ``forze_fastapi`` (import-linter), so a rename on
the server side must fail here rather than turn every operation reference into a mismatch.
"""

from __future__ import annotations

import pytest

from forze.application.contracts.tenancy.tenant_hint import TENANT_ID_HEADER
from forze.application.execution import observability as execution
from forze.application.execution.operations import OperationKind
from forze.application.integrations.document import observability as document
from forze.application.integrations.realtime import REALTIME_DRAINING_CODE
from forze_fastapi import exceptions as fastapi_exceptions
from forze_fastapi.realtime import sse, ws
from forze_fastapi.routes._attach import FORZE_KIND_EXTENSION
from forze_fastapi.routes._snapshots import _AGE_FAMILY  # pyright: ignore[reportPrivateUsage]
from forze_load import realtime
from forze_load.catalog import COMMAND, KIND_EXTENSION, QUERY
from forze_load.plan import TENANT_HEADER
from forze_load.soak import CAPACITY, PROCESS_PREFIXES, SNAPSHOT_AGE_FAMILY

pytestmark = pytest.mark.unit

# ----------------------- #


def test_the_kind_key_is_the_one_forze_fastapi_writes() -> None:
    assert KIND_EXTENSION == FORZE_KIND_EXTENSION


def test_the_kind_values_are_the_catalog_s() -> None:
    assert {QUERY, COMMAND} == {kind.value for kind in OperationKind}
    assert OperationKind.QUERY.value == QUERY


def test_the_tenant_header_is_the_one_forze_reads() -> None:
    assert TENANT_HEADER == TENANT_ID_HEADER


def _rendered(instrument: str) -> str:
    # The OTel Prometheus exporter's name for an instrument whose unit is "1".
    return instrument.replace(".", "_")


def test_each_capacity_pair_is_one_forze_exports() -> None:
    assert CAPACITY == {
        _rendered(document.DOCUMENT_L1_SIZE_GAUGE): _rendered(document.DOCUMENT_L1_CAPACITY_GAUGE),
        _rendered(execution.TENANT_POOL_SIZE_GAUGE): _rendered(
            execution.TENANT_POOL_CAPACITY_GAUGE
        ),
        _rendered(execution.CONNECTION_POOL_IN_USE_GAUGE): _rendered(
            execution.CONNECTION_POOL_MAX_GAUGE
        ),
        _rendered(execution.CONNECTION_POOL_IDLE_GAUGE): _rendered(
            execution.CONNECTION_POOL_MAX_GAUGE
        ),
        _rendered(execution.BULKHEAD_IN_USE_GAUGE): _rendered(execution.BULKHEAD_LIMIT_GAUGE),
    }


def test_the_process_gauges_are_instrument_process_s() -> None:
    assert PROCESS_PREFIXES == (
        _rendered(execution.PROCESS_CPU_TIME_COUNTER),
        _rendered(execution.PROCESS_MEMORY_USAGE_COUNTER),
    )


def test_the_unfitted_age_gauge_is_the_union_route_s() -> None:
    assert SNAPSHOT_AGE_FAMILY == _AGE_FAMILY


def test_the_realtime_names_are_the_routes_own() -> None:
    assert realtime.LAST_EVENT_ID_HEADER == sse.LAST_EVENT_ID_HEADER
    assert realtime.ERROR_CODE_HEADER == fastapi_exceptions.ERROR_CODE_HEADER
    assert realtime.DRAINING_CODE == REALTIME_DRAINING_CODE
    assert realtime.WS_RECONNECT_CLOSE == ws.WS_RECONNECT_CLOSE
    assert realtime.WS_POLICY_CLOSE == ws.WS_POLICY_CLOSE
