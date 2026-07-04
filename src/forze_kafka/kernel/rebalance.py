"""Rebalance listener that keeps offset-log consumption loss/skip-free.

``aiokafka`` reassigns partitions across live group members automatically. Between
a ``read`` and its follow-up ``commit`` a rebalance can (a) strand a stale
partition→consumer routing entry — a later commit on the now-revoked partition then
raises ``CommitFailedError`` / ``IllegalStateError`` and kills the run — and (b)
leave a consumer's in-memory read position ahead of its committed offset, so the
next fetch would skip uncommitted records. This listener invalidates the routing
for revoked partitions and seeks freshly-assigned partitions back to committed, so
a routine rebalance neither crashes consumption nor double/skips.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import suppress
from typing import final

from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
from aiokafka.structs import TopicPartition

from ._logger import logger

# ----------------------- #


@final
class KafkaCommitRebalanceListener(ConsumerRebalanceListener):  # type: ignore[misc]  # aiokafka ships no types, so the base resolves to Any
    """Invalidate stale routing on revoke and seek to committed on assign.

    The routing map lives in the consumer adapter, so *on_revoke* is injected by
    the adapter (this module stays ignorant of the map's shape). :attr:`consumer`
    is bound by the client once the ``aiokafka`` consumer this listener is
    subscribed on exists — the callbacks only fire during a poll, long after the
    bind, so there is no window where an assignment seek runs unbound.
    """

    def __init__(
        self,
        *,
        on_revoke: Callable[[Sequence[TopicPartition]], None],
    ) -> None:
        self._on_revoke = on_revoke
        self.consumer: AIOKafkaConsumer | None = None

    # ....................... #

    async def on_partitions_revoked(self, revoked: Sequence[TopicPartition]) -> None:
        # Forget the revoked partitions' routing so a commit for one is skipped
        # (redelivered + inbox-deduped) rather than raising on a member that no
        # longer owns the partition.
        self._on_revoke(list(revoked))

    # ....................... #

    async def on_partitions_assigned(self, assigned: Sequence[TopicPartition]) -> None:
        consumer = self.consumer

        if consumer is None or not assigned:
            return

        # Re-fetch newly-assigned partitions from the committed offset, never a
        # stale in-memory position — best-effort so a transient seek failure does
        # not abort the group join (the fetch position defaults to committed anyway).
        with suppress(Exception):
            await consumer.seek_to_committed(*assigned)

        logger.trace(
            "Kafka rebalance: sought %s assigned partition(s) to committed",
            len(assigned),
        )
