"""Header parsing is defensive: an untrusted, malformed header never raises.

Raising in the bridge would fail the message and reclaim-loop it forever, so a bad
``forze_tenant_id`` drops the tenant and a bad ``forze_hlc`` falls back to a wall-clock
stamp. Socket.IO may JSON-decode a header into a number, so a non-str value (which makes
``UUID`` / ``HlcTimestamp.parse`` raise ``AttributeError`` / a validation error) must be
handled too.
"""

from __future__ import annotations

from uuid import uuid4

from forze.application.contracts.envelope import HEADER_HLC, HEADER_TENANT_ID
from forze.base.primitives import HlcTimestamp
from forze_socketio.gateway import _hlc_from_headers, _tenant_from_headers

# ----------------------- #


def test_tenant_header_round_trips() -> None:
    tenant = uuid4()
    assert _tenant_from_headers({HEADER_TENANT_ID: str(tenant)}) == tenant


def test_tenant_header_drops_a_malformed_string() -> None:
    assert _tenant_from_headers({HEADER_TENANT_ID: "not-a-uuid"}) is None


def test_tenant_header_drops_a_numeric_value_without_raising() -> None:
    # a JSON-decoded number makes UUID raise AttributeError, not ValueError
    assert _tenant_from_headers({HEADER_TENANT_ID: 123456}) is None


def test_hlc_header_round_trips() -> None:
    hlc = HlcTimestamp(physical_ms=123, logical=4)
    assert _hlc_from_headers({HEADER_HLC: hlc.encode()}) == hlc


def test_hlc_header_falls_back_on_a_malformed_string() -> None:
    # parse() raises exc.validation (a CoreException), which must fall back, not propagate
    assert isinstance(_hlc_from_headers({HEADER_HLC: "abc"}), HlcTimestamp)


def test_hlc_header_falls_back_on_a_numeric_value_without_raising() -> None:
    assert isinstance(_hlc_from_headers({HEADER_HLC: 999}), HlcTimestamp)
