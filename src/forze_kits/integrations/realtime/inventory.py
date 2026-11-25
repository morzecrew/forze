"""What the realtime offline collections contribute to an application's spec inventory.

The mailbox and cursor collections come out of :func:`~.mailbox.realtime_mailbox_spec` and
:func:`~.mailbox.realtime_cursor_spec`, whose models are private, so forze owns their shape:
the application binds them to tables and cannot change their columns. They are catalogued as
:attr:`~forze.application.contracts.inventory.SpecSource.FRAMEWORK`, which also lets
``forze schema`` print their tables (RFC 0080 §5.7).
"""

from forze.application.contracts.inventory import SpecRegistry, SpecSource
from forze.base.exceptions import exc

from .mailbox import CursorDocumentSpec, MailboxDocumentSpec

# ----------------------- #


def realtime_document_contributions(
    *,
    mailbox: MailboxDocumentSpec | None = None,
    cursor: CursorDocumentSpec | None = None,
) -> SpecRegistry:
    """Catalogue the offline collections this deployment binds — merge it at assembly::

        registry = SpecRegistry().register(*my_specs).merge(
            transport.spec_contributions(),
            realtime_document_contributions(
                mailbox=realtime_mailbox_spec(), cursor=realtime_cursor_spec()
            ),
        )

    Pass only the specs the deployment wires: a catalogued route that no dependency binds
    refuses startup. :meth:`RealtimeTransport.spec_contributions
    <forze_kits.integrations.realtime.RealtimeTransport.spec_contributions>` covers the
    transport's stream, outbox and inbox, not these.

    :raises CoreException: ``configuration``, code ``realtime_document_contributions_empty``,
        when neither spec is given.
    """

    if mailbox is None and cursor is None:
        raise exc.configuration(
            "realtime_document_contributions() was given neither the mailbox nor the cursor "
            "collection, so it would contribute nothing: pass the specs this deployment wires "
            "(mailbox=realtime_mailbox_spec(), and/or cursor=realtime_cursor_spec()).",
            code="realtime_document_contributions_empty",
        )

    return SpecRegistry().register(
        *(spec for spec in (mailbox, cursor) if spec is not None),
        source=SpecSource.FRAMEWORK,
    )
