from typing import Optional, Protocol, TypedDict, runtime_checkable

# ----------------------- #


class IdempotencySnapshot(TypedDict):
    code: int
    content_type: str
    body: bytes


# ....................... #
#! TODO: use contextvar or so for idempotency snapshot and async context manager
#! to wrap the scope


@runtime_checkable
class IdempotencyPort(Protocol):
    async def begin(
        self,
        op: str,
        key: Optional[str],
        payload_hash: str,
    ) -> Optional[IdempotencySnapshot]: ...

    async def commit(
        self,
        op: str,
        key: Optional[str],
        payload_hash: str,
        snapshot: IdempotencySnapshot,
    ) -> None: ...
