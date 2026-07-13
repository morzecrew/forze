"""In-memory HLC high-water-mark adapter."""

from __future__ import annotations

from typing import final

import attrs

from forze.application.contracts.hlc import HlcCheckpointPort
from forze.base.primitives import HlcTimestamp
from forze_mock.adapters._journal import record_undo
from forze_mock.state import MockState

# ----------------------- #

_DEFAULT_NODE_KEY = "default"


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockHlcCheckpointAdapter(HlcCheckpointPort):
    """In-memory HLC high-water-mark store backed by :class:`MockState`.

    Mirrors the Postgres store: :meth:`advance` is a monotonic max keyed by ``node_key``
    and participates in the mock transaction (reverts on rollback via the undo journal, so
    a rolled-back outbox flush does not advance the mark), and :meth:`load` returns the max
    across all node keys so a restart resumes above the whole deployment's emissions.

    The mark lives in :class:`MockState`, not the clock, so it survives a clock instance
    being discarded — a test can therefore simulate a restart by building a fresh clock and
    resuming it from :meth:`load`.
    """

    state: MockState
    node_key: str = _DEFAULT_NODE_KEY
    """Which node's row this adapter writes; :meth:`load` reads across all of them."""

    # ....................... #

    async def load(self) -> HlcTimestamp | None:
        with self.state.lock:
            if not self.state.hlc_checkpoint:
                return None

            return HlcTimestamp.unpack(max(self.state.hlc_checkpoint.values()))

    # ....................... #

    async def advance(self, mark: HlcTimestamp) -> None:
        packed = mark.pack()

        with self.state.lock:
            current = self.state.hlc_checkpoint.get(self.node_key)

            if current is not None and current >= packed:
                return  # monotonic max: an equal or older mark never lowers it

            prior = current

            def _revert() -> None:
                # A business rollback reverts the mark too (atomic with the flushed rows);
                # a no-op outside a transaction, where the write is already durable. Runs
                # later (journal replay), so take the lock like every other mutation path
                # (reentrant, so safe if already held).
                with self.state.lock:
                    if prior is None:
                        self.state.hlc_checkpoint.pop(self.node_key, None)

                    else:
                        self.state.hlc_checkpoint[self.node_key] = prior

            record_undo(_revert)
            self.state.hlc_checkpoint[self.node_key] = packed
