"""Runtime port used by kernel usecases."""

from typing import AsyncContextManager, Literal, Protocol, runtime_checkable

# ----------------------- #


@runtime_checkable
class AppRuntimePort(Protocol):
    """Abstract application runtime contract.

    Implementations typically wrap a unit-of-work or transaction boundary and
    expose health information used by higher layers.
    """

    def transaction(self) -> AsyncContextManager[None]:
        """Return an async context manager that wraps a logical transaction.

        The context manager MUST join or start a transaction such that all
        operations inside the block either commit or roll back together.
        """
        ...

    async def health(self) -> tuple[Literal["ok", "err"], dict[str, str]]:
        """Return overall health status and subsystem diagnostics.

        :returns: A tuple of a status flag (``"ok"`` or ``"err"``) and a mapping
            of subsystem names to short human-readable messages.
        """
        ...
