from typing import Any, Optional, Protocol, Sequence

from forze.base.primitives import JsonDict

# ----------------------- #
#! TODO: support status retrieval and success / failure handling or/and tracing


class WorkflowPort(Protocol):
    async def start(
        self,
        name: str,
        id: str,  # ? UUID?
        args: Sequence[Any],
        queue: Optional[str] = None,
    ) -> None: ...

    async def signal(
        self,
        id: str,  # ? UUID?
        signal: str,
        data: Sequence[JsonDict],  # ? support for pydantic models ?
    ) -> None: ...
