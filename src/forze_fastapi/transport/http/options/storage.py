from datetime import timedelta
from typing import TypedDict

# ----------------------- #


class StorageConfigSpec(TypedDict, total=False):
    enable_idempotency: bool
    idempotency_ttl: timedelta
