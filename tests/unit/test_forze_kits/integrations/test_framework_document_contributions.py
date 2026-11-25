"""The kits catalogue the document specs forze ships as the framework's (RFC 0080 row 28).

Forze ships these specs' models, so the application cannot change their tables, and
``forze schema --specs`` prints them. ``KIT`` would not do: a kit registers the application's
own spec as ``KIT``, and that table stays the application's.
"""

import pytest

from forze.application.contracts.inventory import SpecPlane, SpecRegistry, SpecSource
from forze.base.exceptions import CoreException
from forze_kits.domain.stored_file import StoredFileKitSpec
from forze_kits.integrations.audit import audit_record_spec, audit_spec_contributions
from forze_kits.integrations.realtime import (
    realtime_cursor_spec,
    realtime_document_contributions,
    realtime_mailbox_spec,
)

pytestmark = pytest.mark.unit

# ----------------------- #


def _documents(registry: SpecRegistry) -> list[tuple[str, SpecSource]]:
    return [(entry.name, entry.source) for entry in registry.freeze().of_plane(SpecPlane.DOCUMENT)]


def test_the_audit_collection_is_the_framework_s() -> None:
    assert _documents(audit_spec_contributions(audit_record_spec("trail"))) == [
        ("trail", SpecSource.FRAMEWORK)
    ]


def test_the_realtime_collections_are_the_framework_s() -> None:
    registry = realtime_document_contributions(
        mailbox=realtime_mailbox_spec("chat"), cursor=realtime_cursor_spec("chat")
    )

    assert sorted(_documents(registry)) == [
        ("chat-cursors", SpecSource.FRAMEWORK),
        ("chat-mailbox", SpecSource.FRAMEWORK),
    ]
    assert _documents(realtime_document_contributions(cursor=realtime_cursor_spec())) == [
        ("realtime-cursors", SpecSource.FRAMEWORK)
    ]


def test_a_realtime_contribution_of_nothing_is_refused() -> None:
    with pytest.raises(CoreException) as caught:
        realtime_document_contributions()

    assert caught.value.code == "realtime_document_contributions_empty"


def test_the_stored_file_collection_is_the_framework_s() -> None:
    kit = StoredFileKitSpec(name="files", search=StoredFileKitSpec.default_search("files"))
    registry = kit.document_contributions().freeze()

    # The search spec is the application's to register.
    assert [(entry.plane, entry.name, entry.source) for entry in registry.entries] == [
        (SpecPlane.DOCUMENT, "files", SpecSource.FRAMEWORK)
    ]
