"""Tests for forze.application.contracts.idempotency.ports and types."""

from forze.application.contracts.idempotency import IdempotencyPort, IdempotencyRecord


class _StubIdempotency:
    """Concrete implementation for testing IdempotencyPort."""

    def __init__(self) -> None:
        self._store: dict[str, IdempotencyRecord] = {}

    async def begin(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
    ) -> IdempotencyRecord | None:
        k = f"{op}:{key}:{payload_hash}"
        return self._store.get(k)

    async def commit(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
        record: IdempotencyRecord,
    ) -> None:
        k = f"{op}:{key}:{payload_hash}"
        self._store[k] = record

    async def fail(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
    ) -> None:
        k = f"{op}:{key}:{payload_hash}"
        self._store.pop(k, None)


class TestIdempotencyPort:
    def test_is_runtime_checkable(self) -> None:
        stub = _StubIdempotency()
        assert isinstance(stub, IdempotencyPort)

    async def test_begin_returns_none_for_new(self) -> None:
        stub = _StubIdempotency()
        result = await stub.begin("create", "k1", "hash1")
        assert result is None

    async def test_commit_and_begin_returns_record(self) -> None:
        stub = _StubIdempotency()
        record = IdempotencyRecord(result=b'{"ok":1}')
        await stub.commit("create", "k1", "hash1", record)
        result = await stub.begin("create", "k1", "hash1")
        assert result is not None
        assert result.result == b'{"ok":1}'

    async def test_different_keys_independent(self) -> None:
        stub = _StubIdempotency()
        record = IdempotencyRecord(result=b"ok")
        await stub.commit("create", "k1", "h1", record)
        assert await stub.begin("create", "k2", "h1") is None
        assert await stub.begin("create", "k1", "h2") is None

    async def test_fail_releases_claim(self) -> None:
        stub = _StubIdempotency()
        record = IdempotencyRecord(result=b"ok")
        await stub.commit("create", "k1", "h1", record)
        await stub.fail("create", "k1", "h1")
        assert await stub.begin("create", "k1", "h1") is None

    def test_non_conforming_not_instance(self) -> None:
        class Bad:
            pass

        assert not isinstance(Bad(), IdempotencyPort)


class TestIdempotencyRecord:
    def test_create_record(self) -> None:
        record = IdempotencyRecord(result=b"{}")
        assert record.result == b"{}"
