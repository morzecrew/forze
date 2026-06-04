"""S3-backed implementation of :class:`~forze.application.contracts.storage.StoragePort`."""

from forze_s3._compat import require_s3

require_s3()

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

from ..kernel.relation import resolve_s3_bucket

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class S3StorageAdapter(ObjectStorageAdapter):
    """Storage adapter that persists files in an S3-compatible bucket.

    Implements :class:`~forze.application.contracts.storage.StoragePort` via
    :class:`~forze.application.integrations.storage.ObjectStorageAdapter`.
    """

    resolve_bucket: Callable[[NamedResourceSpec, UUID | None], Awaitable[str]] = (
        attrs.field(default=resolve_s3_bucket)
    )

    # ....................... #

    @staticmethod
    def _guess_content_type(filename: str, data: bytes) -> str:
        return guess_content_type_with_magic(filename, data)
