"""GCS-backed object storage adapter (storage query and command ports)."""

from forze_gcs._compat import require_gcs

require_gcs()

# ....................... #

from collections.abc import Awaitable, Callable
from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.resolution import NamedResourceSpec
from forze.application.integrations.storage import (
    ObjectStorageAdapter,
    guess_content_type_with_magic,
)

from ..kernel.relation import resolve_gcs_bucket

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GCSStorageAdapter(ObjectStorageAdapter):
    """Storage adapter that persists files in a GCS bucket.

    Implements the storage query and command ports via
    :class:`~forze.application.integrations.storage.ObjectStorageAdapter`.
    """

    resolve_bucket: Callable[[NamedResourceSpec, UUID | None], Awaitable[str]] = (
        attrs.field(default=resolve_gcs_bucket)
    )

    # ....................... #

    @staticmethod
    def _guess_content_type(filename: str, data: bytes) -> str:
        return guess_content_type_with_magic(filename, data)
