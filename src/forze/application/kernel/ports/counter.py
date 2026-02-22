from typing import Optional, Protocol, runtime_checkable

# ----------------------- #


@runtime_checkable
class CounterPort(Protocol):
    async def incr(self, by: int = 1, *, suffix: Optional[str] = None) -> int: ...
    async def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: Optional[str] = None,
    ) -> list[int]: ...
    async def decr(self, by: int = 1, *, suffix: Optional[str] = None) -> int: ...
    async def reset(self, value: int = 1, *, suffix: Optional[str] = None) -> int: ...
