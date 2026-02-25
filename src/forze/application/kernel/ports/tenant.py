from typing import Protocol, runtime_checkable
from uuid import UUID

# ----------------------- #


@runtime_checkable
class TenantContextPort(Protocol):
    def get(self) -> UUID: ...
    def set(self, tenant_id: UUID) -> None: ...
