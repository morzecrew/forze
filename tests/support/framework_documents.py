"""Every document spec forze ships, catalogued as the framework's (RFC 0080 §5.7).

``forze schema`` prints the table of each of these, so a test that covers "every framework
spec" takes them from here: the identity plane's nineteen, audit, progress jobs, the realtime
mailbox and cursors, and stored files.
"""

from typing import Any

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.inventory import (
    FrozenSpecRegistry,
    SpecPlane,
    SpecRegistry,
    SpecSource,
)
from forze_identity import spec_contributions as identity_contributions
from forze_kits.domain.stored_file import StoredFileKitSpec
from forze_kits.integrations.audit import audit_record_spec, audit_spec_contributions
from forze_kits.integrations.progress import job_record_spec, progress_spec_contributions
from forze_kits.integrations.realtime import (
    realtime_cursor_spec,
    realtime_document_contributions,
    realtime_mailbox_spec,
)
from forze_postgres.execution.deps.configs import PostgresDocumentConfig

# ----------------------- #

STORED_FILES = StoredFileKitSpec(name="stored_files")
"""The stored-file kit, named for its table."""


def framework_contributions() -> SpecRegistry:
    """Every framework document spec, catalogued through the helper that ships it."""

    return identity_contributions().merge(
        audit_spec_contributions(audit_record_spec()),
        progress_spec_contributions(spec=job_record_spec()),
        realtime_document_contributions(
            mailbox=realtime_mailbox_spec(), cursor=realtime_cursor_spec()
        ),
        STORED_FILES.document_contributions(),
    )


def framework_registry() -> FrozenSpecRegistry:
    """:func:`framework_contributions`, frozen."""

    return framework_contributions().freeze()


def framework_document_specs() -> list[DocumentSpec[Any, Any, Any, Any]]:
    """Each framework document spec, in registry order."""

    registry = framework_registry()

    return [
        entry.spec  # pyright: ignore[reportUnknownMemberType]
        for entry in registry.of_plane(SpecPlane.DOCUMENT)
        if entry.source is SpecSource.FRAMEWORK and isinstance(entry.spec, DocumentSpec)
    ]


def document_config(
    schema: str,
    table: str,
    *,
    tenant_aware: bool = False,
) -> PostgresDocumentConfig:
    """A read-write route over ``schema.table``, bookkeeping kept by the application."""

    return PostgresDocumentConfig(
        read=(schema, table),
        write=(schema, table),
        bookkeeping_strategy="application",
        tenant_aware=tenant_aware,
    )
