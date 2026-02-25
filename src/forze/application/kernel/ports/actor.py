from typing import Protocol, runtime_checkable
from uuid import UUID

# ----------------------- #
#! TODO: model for actor context


@runtime_checkable
class ActorContextPort(Protocol):
    def get(self) -> UUID: ...
    def set(self, actor_id: UUID) -> None: ...
