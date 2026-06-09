"""Kit specification bundling document, storage, search, and outbox routes."""

from typing import Any, final

import attrs

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.storage import StorageSpec

from .models import (
    StoredFileCreateCmd,
    StoredFileDocument,
    StoredFileRead,
    StoredFileUpdateCmd,
)

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StoredFileKitSpec:
    """Closed-schema specification for the stored-file kit.

    Register infrastructure configs (Postgres/Mongo document, S3/GCS storage,
    search index, outbox) under the same :attr:`name` route.
    """

    name: str
    """Shared logical route name for document, storage, search, and outbox."""

    storage: StorageSpec | None = attrs.field(default=None)
    """Object storage spec; use :attr:`resolved_storage` at runtime."""

    search: SearchSpec[StoredFileRead] | None = attrs.field(default=None)
    """Optional search spec; when set, search handlers and sync stages are enabled."""

    outbox: OutboxSpec[Any] | None = attrs.field(default=None)
    """Optional outbox spec for integration events."""

    # ....................... #

    @property
    def resolved_storage(self) -> StorageSpec:
        """Object storage spec, defaulting to ``StorageSpec(name=name)``."""

        if self.storage is not None:
            return self.storage

        return StorageSpec(name=self.name)

    # ....................... #

    @property
    def document(self) -> DocumentSpec[
        StoredFileRead,
        StoredFileDocument,
        StoredFileCreateCmd,
        StoredFileUpdateCmd,
    ]:
        """Document specification for the stored-file table."""

        return DocumentSpec(
            name=self.name,
            read=StoredFileRead,
            write={
                "domain": StoredFileDocument,
                "create_cmd": StoredFileCreateCmd,
                "update_cmd": StoredFileUpdateCmd,
            },
        )

    # ....................... #

    @property
    def search_spec(self) -> SearchSpec[StoredFileRead] | None:
        """Resolved search spec when :attr:`search` is configured."""

        return self.search

    # ....................... #

    @classmethod
    def default_search(cls, name: str) -> SearchSpec[StoredFileRead]:
        """Build a default search spec for :class:`StoredFileRead`."""

        return SearchSpec(
            name=name,
            model_type=StoredFileRead,
            fields=["filename", "description"],
            default_sort={"filename": "asc"},
        )
