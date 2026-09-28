"""The audit port."""

from collections.abc import Awaitable
from typing import Protocol, runtime_checkable

from .value_objects import AuditEntry

# ----------------------- #


@runtime_checkable
class AuditPort(Protocol):  # pragma: no cover
    """Writes audit rows.

    A write **joins the transaction that is open** when it is made, so a row recorded inside
    an operation's transaction commits or rolls back with the operation's own write; outside
    one it commits on its own. The audit hooks rely on both halves: an admitted operation's
    row is written inside its transaction, a failed or denied one's after it has closed.
    """

    def record(self, entry: AuditEntry) -> Awaitable[None]:
        """Write *entry*."""
        ...
