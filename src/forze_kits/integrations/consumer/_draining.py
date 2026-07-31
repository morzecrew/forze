"""The drain-gate refusal classification, shared by every consumer runner.

A rolling deploy flips the drain gate before a consumer loop's stop signal arrives, so
a handler's dispatch can be refused with ``THROTTLED``/``code="draining"`` mid-batch.
That is a shutdown artifact, never poison — but the consequence lives in each runner's
own ladder (the queue runner requeues without counting, the offset-log runner stops
without committing past), and when the classification was inline per runner, only the
runner it was written in had it: the commit-stream twin dead-lettered a healthy message
on graceful shutdown.

The predicate now lives in the execution plane
(:mod:`forze.application.execution.context.drain`), right next to the gate whose code
it classifies — one definition for the kits runners *and* transport gateways (the
Socket.IO bridge), so the ladders cannot drift on *what counts as draining* again.
This module re-exports it for the runners' existing imports.
"""

from forze.application.execution import is_draining_refusal

# ----------------------- #

__all__ = ["is_draining_refusal"]
