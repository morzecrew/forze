"""Tests for forze.application.contracts.tenant.ports."""

from uuid import UUID, uuid4

from forze.application.contracts.tenant.ports import TenantContextPort


class _StubTenantContext:
    def __init__(self) -> None:
        self._tenant_id: UUID | None = None

    def get(self) -> UUID:
        if self._tenant_id is None:
            raise RuntimeError("No tenant bound")
        return self._tenant_id

    def set(self, tenant_id: UUID) -> None:
        self._tenant_id = tenant_id


class TestTenantContextPort:
    def test_is_runtime_checkable(self) -> None:
        stub = _StubTenantContext()
        assert isinstance(stub, TenantContextPort)

    def test_set_and_get(self) -> None:
        stub = _StubTenantContext()
        tid = uuid4()
        stub.set(tid)
        assert stub.get() == tid

    def test_get_without_set_raises(self) -> None:
        stub = _StubTenantContext()
        try:
            stub.get()
            assert False, "Should have raised"
        except RuntimeError:
            pass

    def test_override(self) -> None:
        stub = _StubTenantContext()
        t1 = uuid4()
        t2 = uuid4()
        stub.set(t1)
        stub.set(t2)
        assert stub.get() == t2

    def test_non_conforming_not_instance(self) -> None:
        class Bad:
            pass

        assert not isinstance(Bad(), TenantContextPort)
