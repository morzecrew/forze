from datetime import timedelta
from typing import TypedDict

# ----------------------- #


class DocumentConfigSpec(TypedDict, total=False):
    enable_etag: bool
    etag_auto_304: bool
    enable_idempotency: bool
    idempotency_ttl: timedelta
