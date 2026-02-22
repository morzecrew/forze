from typing import AsyncContextManager, Literal, Protocol, runtime_checkable

# ----------------------- #


@runtime_checkable
class AppRuntimePort(Protocol):
    def transaction(self) -> AsyncContextManager[None]: ...
    async def health(self) -> tuple[Literal["ok", "err"], dict[str, str]]: ...
