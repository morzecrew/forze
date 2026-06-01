"""GCS-backed implementation of :class:`~forze.application.contracts.storage.StoragePort`."""

from forze_gcs._compat import require_gcs

require_gcs()

# ....................... #

from collections.abc import Awaitable, Callable
from typing import final
from uuid import UUID

import attrs
import magic

from forze.application.contracts.resolution import NamedResourceSpec
from forze.application.integrations.storage import ObjectStorageAdapter

from ..kernel.relation import resolve_gcs_bucket

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GCSStorageAdapter(ObjectStorageAdapter):
    """Storage adapter that persists files in a GCS bucket.

    Implements :class:`~forze.application.contracts.storage.StoragePort` via
    :class:`~forze.application.integrations.storage.ObjectStorageAdapter`.
    """

    resolve_bucket: Callable[[NamedResourceSpec, UUID | None], Awaitable[str]] = (
        attrs.field(default=resolve_gcs_bucket)
    )

    # ....................... #

    @staticmethod
    def _guess_content_type(filename: str, data: bytes) -> str:
        try:
            ct_magic = magic.from_buffer(data, mime=True)

            if ct_magic:
                return ct_magic

        except Exception:  # nosec B110
            pass

        return ObjectStorageAdapter._guess_content_type(filename, data)
