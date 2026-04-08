"""Unit tests for :mod:`forze_temporal.interceptors.codecs`."""

from uuid import UUID

import pytest

pytest.importorskip("temporalio")

from temporalio.api.common.v1 import Payload

from forze.application.execution import CallContext, PrincipalContext
from forze.base.primitives import uuid7
from forze_temporal.interceptors.codecs import (
    TemporalContextBinder,
    TemporalContextCodec,
)

_EXEC_HEADER = "Forze-Execution-ID"
_CORR_HEADER = "Forze-Correlation-ID"
_TENANT_HEADER = "Forze-Tenant-ID"
_ACTOR_HEADER = "Forze-Actor-ID"


class TestTemporalContextCodecEncode:
    """Encoding of call and principal data into Temporal headers."""

    def test_encode_includes_execution_and_correlation_not_causation_header(
        self,
    ) -> None:
        """Causation is not sent as a separate header; the chain uses execution id on decode."""

        codec = TemporalContextCodec()
        eid = uuid7()
        cid = uuid7()
        caus = uuid7()
        call = CallContext(execution_id=eid, correlation_id=cid, causation_id=caus)

        headers = codec.encode(call=call)

        assert headers[_EXEC_HEADER].data == str(eid).encode("utf-8")
        assert headers[_CORR_HEADER].data == str(cid).encode("utf-8")

    def test_encode_principal_tenant_and_actor(self) -> None:
        codec = TemporalContextCodec()
        tid = uuid7()
        aid = uuid7()
        principal = PrincipalContext(tenant_id=tid, actor_id=aid)

        headers = codec.encode(principal=principal)

        assert headers[_TENANT_HEADER].data == str(tid).encode("utf-8")
        assert headers[_ACTOR_HEADER].data == str(aid).encode("utf-8")

    def test_encode_empty_when_no_contexts(self) -> None:
        assert TemporalContextCodec().encode() == {}


class TestTemporalContextCodecDecode:
    """Decoding headers to :class:`TemporalDecodedContext` and binding to execution types."""

    def test_bind_preserves_correlation_and_sets_causation_from_parent_execution_header(
        self,
    ) -> None:
        codec = TemporalContextCodec()
        binder = TemporalContextBinder()
        parent_eid = uuid7()
        cid = uuid7()
        headers: dict[str, Payload] = {
            _EXEC_HEADER: Payload(data=str(parent_eid).encode("utf-8")),
            _CORR_HEADER: Payload(data=str(cid).encode("utf-8")),
        }

        decoded = codec.decode(headers)
        call, _principal = binder.bind(decoded)

        assert call.correlation_id == cid
        assert call.causation_id == parent_eid

    def test_bind_generates_correlation_when_missing(self) -> None:
        codec = TemporalContextCodec()
        binder = TemporalContextBinder()

        decoded = codec.decode({})
        call, _principal = binder.bind(decoded)

        assert isinstance(call.correlation_id, UUID)
        assert call.correlation_id == call.execution_id

    def test_bind_restores_tenant_and_actor(self) -> None:
        codec = TemporalContextCodec()
        binder = TemporalContextBinder()
        tid = uuid7()
        aid = uuid7()
        headers = {
            _TENANT_HEADER: Payload(data=str(tid).encode("utf-8")),
            _ACTOR_HEADER: Payload(data=str(aid).encode("utf-8")),
        }

        decoded = codec.decode(headers)
        _call, principal = binder.bind(decoded)

        assert principal.tenant_id == tid
        assert principal.actor_id == aid

    def test_bind_does_not_restore_local_execution_id_from_encoded_headers(
        self,
    ) -> None:
        """Wire ``Forze-Execution-ID`` is the parent span; local execution id is always fresh."""

        codec = TemporalContextCodec()
        eid = uuid7()
        cid = uuid7()
        call_in = CallContext(execution_id=eid, correlation_id=cid, causation_id=None)
        headers = codec.encode(call=call_in)

        assert _EXEC_HEADER in headers

        decoded = codec.decode(headers)
        call_out, _ = TemporalContextBinder().bind(decoded)

        assert call_out.execution_id != eid
        assert call_out.correlation_id == cid
        assert call_out.causation_id == eid
