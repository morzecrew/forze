"""Tests for forze.application.contracts.idempotency.ports and types."""

from typing import Optional

from forze.application.contracts.idempotency import IdempotencyPort, IdempotencySnapshot


class _StubIdempotency:
    """Concrete implementation for testing IdempotencyPort."""

    def __init__(self) -> None:
        self._store: dict[str, IdempotencySnapshot] = {}

    async def begin(
        self,
        op: str,
        key: Optional[str],
        payload_hash: str,
    ) -> Optional[IdempotencySnapshot]:
        k = f"{op}:{key}:{payload_hash}"
        return self._store.get(k)

    async def commit(
        self,
        op: str,
        key: Optional[str],
        payload_hash: str,
        snapshot: IdempotencySnapshot,
    ) -> None:
        k = f"{op}:{key}:{payload_hash}"
        self._store[k] = snapshot


class TestIdempotencyPort:
    def test_is_runtime_checkable(self) -> None:
        stub = _StubIdempotency()
        assert isinstance(stub, IdempotencyPort)

    async def test_begin_returns_none_for_new(self) -> None:
        stub = _StubIdempotency()
        result = await stub.begin("create", "k1", "hash1")
        assert result is None

    async def test_commit_and_begin_returns_snapshot(self) -> None:
        stub = _StubIdempotency()
        snap = IdempotencySnapshot(
            code=200, content_type="application/json", body=b'{"ok":1}'
        )
        await stub.commit("create", "k1", "hash1", snap)
        result = await stub.begin("create", "k1", "hash1")
        assert result is not None
        assert result.code == 200
        assert result.body == b'{"ok":1}'

    async def test_different_keys_independent(self) -> None:
        stub = _StubIdempotency()
        snap = IdempotencySnapshot(code=201, content_type="text/plain", body=b"ok")
        await stub.commit("create", "k1", "h1", snap)
        assert await stub.begin("create", "k2", "h1") is None
        assert await stub.begin("create", "k1", "h2") is None

    def test_non_conforming_not_instance(self) -> None:
        class Bad:
            pass

        assert not isinstance(Bad(), IdempotencyPort)


class TestIdempotencySnapshot:
    def test_create_snapshot(self) -> None:
        snap = IdempotencySnapshot(
            code=200, content_type="application/json", body=b"{}"
        )
        assert snap.code == 200
        assert snap.content_type == "application/json"
        assert snap.body == b"{}"
