from typing import Protocol, runtime_checkable

import attrs

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class CommitStreamGroupCapabilities:
    """Offset-log features a :class:`CommitStreamGroupQueryPort` backend supports.

    Reported via the opt-in :class:`CommitStreamGroupAware` protocol and
    consulted by fail-closed checks (a backend that does not report capabilities
    is treated as supporting neither). Only flags backed by a real kernel check
    live here — no decorative capabilities.
    """

    supports_replay: bool
    """Can :meth:`CommitStreamGroupAdminPort.reset_offsets` seek backward / to a
    timestamp? A backend that only advances the cursor reports ``False``, and a
    replay call fails closed (``stream.replay_unsupported``)."""

    supports_transactions: bool
    """Native exactly-once (transactional produce-consume)? ``False`` means the
    stack relies on inbox dedup for exactly-once *effect*; a spec requesting
    transport-level exactly-once fails closed (``stream.transactions_unsupported``)."""


# ....................... #


@runtime_checkable
class CommitStreamGroupAware(Protocol):
    """Opt-in extension for offset-log ports that report their capabilities.

    Kept separate from :class:`CommitStreamGroupQueryPort` /
    :class:`CommitStreamGroupAdminPort` so a backend opts in only when it can
    report — the kernel checks a call/spec against :meth:`capabilities` at the
    admin-call / resolve boundary (fail-closed). Mirrors
    :class:`~forze.application.contracts.dlock.FencingAware` and
    :class:`~forze.application.contracts.transaction.IsolationAware`.
    """

    def capabilities(self) -> CommitStreamGroupCapabilities:
        """Report the offset-log features this backend supports."""
        ...
