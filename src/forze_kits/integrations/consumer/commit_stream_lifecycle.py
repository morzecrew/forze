"""Lifecycle helpers for the background offset-log (commit-stream) consumer.

The offset-log twin of :mod:`forze_kits.integrations.consumer.lifecycle` (the
ack/queue model): a supervised step that runs a
:class:`~forze_kits.integrations.consumer.CommitStreamGroupConsumer` forever with
crash-restart + jittered backoff. Restarts are **loss-free**: a crash mid-batch may
leave the backend's pooled reader positioned past uncommitted records, so before it
re-runs the loop the supervisor rewinds the group to its committed offset (a no-op
on backends whose read position is already the committed cursor).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import timedelta
from typing import Any, Final, final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.stream import StreamMessage, StreamSpec
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, current_entropy_source
from forze_kits.integrations._logger import logger
from forze_kits.lifecycle import BackgroundLoopControl

from .commit_stream_runner import CommitStreamGroupConsumer

# ----------------------- #

_STOP_GRACE_SECONDS: Final[float] = 5.0
"""Fallback budget when a hook stops a loop directly (the runtime supplies its own)."""


@final
@attrs.define(slots=True, kw_only=True)
class _CommitStreamConsumerBackgroundStartup(LifecycleHook):
    """Start a background task that runs a :class:`CommitStreamGroupConsumer` forever."""

    consumer: CommitStreamGroupConsumer[Any]
    """The configured consumer; carries all consume options (validated on build)."""

    restart_backoff: timedelta

    # ....................... #

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(
            lambda self: BackgroundLoopControl(
                name=f"commit_stream_consumer:{self.consumer.group}"
            ),
            takes_self=True,
        ),
        init=False,
    )
    """Stop signal and bounded teardown, shared with every other background loop."""

    # ....................... #

    @property
    def task(self) -> asyncio.Task[None] | None:
        """The running loop, if any."""

        return self.control.task

    # ....................... #

    @property
    def loop_name(self) -> str:
        """Satisfies ``DrainableLoop``."""

        return self.control.loop_name

    # ....................... #

    async def stop(self, *, deadline: float) -> bool:
        """Stop the consumer at its next batch boundary. Idempotent.

        This is the loop that pays most for a blunt cancel: a run killed mid-batch never
        commits its offsets, so every message it had just processed is redelivered to whoever
        starts next. Stopping *between* batches means the offsets are already committed.
        """

        return await self.control.stop(deadline=deadline)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.restart_backoff.total_seconds() <= 0:
            raise exc.configuration("Restart backoff must be positive")

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        async def _loop() -> None:
            while True:
                try:
                    # timeout=None: consume forever. Poison / per-message failures
                    # are absorbed inside the runner's decision ladder (which already
                    # rewinds to committed on a pause); only a mid-batch crash
                    # (broker connection loss, a transient KMS fault on decrypt, ...)
                    # escapes to here.
                    result = await self.consumer.run(ctx, timeout=None, stop=self.control.event)

                except asyncio.CancelledError:
                    raise

                except Exception:
                    logger.exception(
                        "Commit-stream consumer for %s crashed; restarting after backoff",
                        self.consumer.topics,
                    )
                    # Loss-free restart: a crash may have advanced the pooled
                    # reader past uncommitted records; rewind to committed so the
                    # restart re-fetches them instead of skipping. Best-effort —
                    # a failed rewind must not stop supervision.
                    with suppress(Exception):
                        await self.consumer.reset_to_committed(ctx)

                    # Jittered backoff, then restart so a flapping broker cannot
                    # hot-loop the consumer. Jittered: a shared downstream outage
                    # crashes every replica at once; without jitter they all restart
                    # (and re-crash) in lockstep.
                    if await self.control.sleep_or_stop(
                        # Desynchronization jitter, not security randomness.
                        self.restart_backoff.total_seconds()
                        * current_entropy_source().as_random().uniform(1.0, 1.5)
                    ):
                        return

                    continue

                if self.control.stopping:
                    # The run ended at a batch boundary because we asked it to — offsets
                    # committed, nothing to alert about.
                    return

                # run() returned rather than raising: with timeout=None the only other exit
                # is a consumer-wide pause-and-alert poison (failed > 0). The runner
                # already alerted and left the record uncommitted; restarting would
                # re-fetch the same poison from the committed offset and pause again
                # in a backoff loop, so honor the documented operator-intervention
                # contract and stop supervising. The operator clears the poison (or
                # adds a dead-letter route) and restarts the process.
                logger.error(
                    "Commit-stream consumer for %s paused on poison (failed=%d); "
                    "supervision stopped pending operator intervention",
                    self.consumer.topics,
                    result.failed,
                )
                return

        if self.control.running:
            # The runtime invokes startup once per scope; a direct double call must
            # not leak (and orphan) the previous consumer task.
            logger.warning(
                "Commit-stream consumer for %s already running; ignoring duplicate startup",
                self.consumer.topics,
            )
            return

        self.control.arm()
        self.control.task = asyncio.create_task(_loop(), name=self.control.loop_name)
        ctx.drainables.register(self)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _CommitStreamConsumerBackgroundShutdown(LifecycleHook):
    """Stop the background commit-stream consumer.

    Normally a no-op — the runtime stops every registered loop before teardown begins. This is
    the fallback for a hand-driven lifecycle; ``stop`` is idempotent.
    """

    startup: _CommitStreamConsumerBackgroundStartup
    """Startup hook."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.stop(deadline=clock.time() + _STOP_GRACE_SECONDS)


# ....................... #


def commit_stream_consumer_background_lifecycle_step(
    *,
    topics: list[str],
    group: str,
    consumer: str,
    stream_spec: StreamSpec[Any],
    handler: Callable[[StreamMessage[Any]], Awaitable[None]],
    inbox_spec: InboxSpec,
    tx_route: StrKey,
    message_id: Callable[[StreamMessage[Any]], str] | None = None,
    bind_tenant_from_headers: bool = False,
    max_attempts: int = 1,
    retry_policy: StrKey | None = None,
    dlq_stream: str | None = None,
    batch_limit: int | None = None,
    restart_backoff: timedelta = timedelta(seconds=5),
    step_id: StrKey | None = None,
) -> LifecycleStep:
    """Build a lifecycle step that runs a :class:`~forze_kits.integrations.consumer.CommitStreamGroupConsumer` in the background.

    The offset-log (commit sub-model) counterpart of
    :func:`~forze_kits.integrations.consumer.queue_consumer_background_lifecycle_step`.
    Startup spawns a consume-forever task (``timeout=None``); shutdown cancels and
    awaits it. Per-message retry/dead-letter is handled inside the runner's decision
    ladder. A **consumer-wide pause-and-alert poison** (decrypt/decode poison, or
    handler poison with no dead-letter route) makes the run *return* rather than raise;
    the supervisor then **stops** — a restart would only re-fetch the same uncommitted
    record from the committed offset and pause again — logging an alert and leaving the
    consumer paused until an operator intervenes. A **crash** of the consume itself
    (broker connection loss) is instead logged and the consume **restarts after a
    jittered *restart_backoff***, first rewinding the group to its committed offset so
    no uncommitted record is skipped on restart; unprocessed offsets are redelivered
    and deduped by the inbox.

    One step consumes one consumer's topics within one group. For more consumers —
    or more members across processes — register multiple steps with distinct
    *step_id* values (the default ``commit_stream_consumer:<group>`` keeps per-group
    steps unique). There is deliberately no in-process concurrency knob: one
    sequential consumer per step keeps ordering and failure semantics simple; scale
    out with more steps/processes (Kafka rebalances partitions across members).

    Opt-in for long-running processes; all runner parameters (*max_attempts*,
    *retry_policy*, *dlq_stream*, *batch_limit*, *bind_tenant_from_headers*, ...)
    pass through with the same defaults and caveats.
    """

    runner = CommitStreamGroupConsumer(
        topics=topics,
        group=group,
        consumer=consumer,
        stream_spec=stream_spec,
        handler=handler,
        inbox_spec=inbox_spec,
        tx_route=tx_route,
        message_id=message_id,
        bind_tenant_from_headers=bind_tenant_from_headers,
        max_attempts=max_attempts,
        retry_policy=retry_policy,
        dlq_stream=dlq_stream,
        batch_limit=batch_limit,
    )
    startup = _CommitStreamConsumerBackgroundStartup(
        consumer=runner,
        restart_backoff=restart_backoff,
    )
    shutdown = _CommitStreamConsumerBackgroundShutdown(startup=startup)

    return LifecycleStep(
        id=step_id if step_id is not None else f"commit_stream_consumer:{group}",
        startup=startup,
        shutdown=shutdown,
    )
