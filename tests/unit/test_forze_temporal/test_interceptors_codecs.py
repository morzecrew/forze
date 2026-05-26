"""Unit tests for :mod:`forze_temporal.interceptors.codecs`."""

from uuid import UUID

import pytest

pytest.importorskip("temporalio")

from temporalio.api.common.v1 import Payload

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import InvocationMetadata
from forze.base.primitives import uuid7
from forze_temporal.interceptors.codecs import TemporalContextBinder, TemporalContextCodec

_EXEC_HEADER = "Forze-Execution-ID"
_CORR_HEADER = "Forze-Correlation-ID"
_TENANT_HEADER = "Forze-Tenant-ID"
_PRINCIPAL_HEADER = "Forze-Principal-ID"


class TestTemporalContextCodecEncode:
    def test_encode_includes_execution_and_correlation_not_causation_header(
        self,
    ) -> None:
        codec = TemporalContextCodec()
        eid = uuid7()
        cid = uuid7()
        caus = uuid7()
        metadata = InvocationMetadata(
            execution_id=eid,
            correlation_id=cid,
            causation_id=caus,
        )

        headers = codec.encode(metadata=metadata)

        assert headers[_EXEC_HEADER].data == str(eid).encode("utf-8")
        assert headers[_CORR_HEADER].data == str(cid).encode("utf-8")

    def test_encode_principal_and_tenant(self) -> None:
        codec = TemporalContextCodec()
        pid = uuid7()
        tid = uuid7()
        identity = AuthnIdentity(principal_id=pid)
        tenancy = TenantIdentity(tenant_id=tid)

        headers = codec.encode(authn=identity, tenant=tenancy)

        assert headers[_PRINCIPAL_HEADER].data == str(pid).encode("utf-8")
        assert headers[_TENANT_HEADER].data == str(tid).encode("utf-8")

    def test_encode_empty_when_no_contexts(self) -> None:
        assert TemporalContextCodec().encode() == {}


class TestTemporalContextCodecDecode:
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
        metadata, identity, tenancy = binder.bind(decoded)

        assert metadata.correlation_id == cid
        assert metadata.causation_id == parent_eid
        assert identity is None
        assert tenancy is None

    def test_bind_generates_correlation_when_missing(self) -> None:
        codec = TemporalContextCodec()
        binder = TemporalContextBinder()

        decoded = codec.decode({})
        metadata, identity, tenancy = binder.bind(decoded)

        assert isinstance(metadata.correlation_id, UUID)
        assert metadata.correlation_id == metadata.execution_id
        assert identity is None
        assert tenancy is None

    def test_bind_restores_tenant_without_principal(self) -> None:
        codec = TemporalContextCodec()
        binder = TemporalContextBinder()
        tid = uuid7()
        headers = {
            _TENANT_HEADER: Payload(data=str(tid).encode("utf-8")),
        }

        decoded = codec.decode(headers)
        _metadata, identity, tenancy = binder.bind(decoded)

        assert identity is None
        assert tenancy is not None and tenancy.tenant_id == tid

    def test_bind_restores_principal(self) -> None:
        codec = TemporalContextCodec()
        binder = TemporalContextBinder()
        pid = uuid7()
        headers = {
            _PRINCIPAL_HEADER: Payload(data=str(pid).encode("utf-8")),
        }

        decoded = codec.decode(headers)
        _metadata, identity, tenancy = binder.bind(decoded)

        assert identity is not None and identity.principal_id == pid
        assert not hasattr(identity, "tenant_id")
        assert tenancy is None

    def test_bind_does_not_restore_local_execution_id_from_encoded_headers(
        self,
    ) -> None:
        codec = TemporalContextCodec()
        eid = uuid7()
        cid = uuid7()
        metadata_in = InvocationMetadata(
            execution_id=eid,
            correlation_id=cid,
            causation_id=None,
        )
        headers = codec.encode(metadata=metadata_in)

        assert _EXEC_HEADER in headers

        decoded = codec.decode(headers)
        metadata_out, _, _ = TemporalContextBinder().bind(decoded)

        assert metadata_out.execution_id != eid
        assert metadata_out.correlation_id == cid
        assert metadata_out.causation_id == eid
