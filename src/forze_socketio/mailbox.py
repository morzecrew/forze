"""Offline store-and-forward mailbox — re-exported from the transport-neutral kernel.

The mailbox + cursor seams (and their in-memory implementations) are shared by every
realtime transport, so they live in :mod:`forze.application.integrations.realtime`;
this module keeps the established ``forze_socketio`` import paths working.
"""

from forze.application.integrations.realtime import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
    MailboxCursors,
    MailboxEntry,
    RealtimeMailbox,
)

# ----------------------- #

__all__ = [
    "MailboxEntry",
    "RealtimeMailbox",
    "MailboxCursors",
    "InMemoryRealtimeMailbox",
    "InMemoryMailboxCursors",
]
