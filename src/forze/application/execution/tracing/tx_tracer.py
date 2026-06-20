"""Optional recorder for transaction scope enter/exit events."""

from __future__ import annotations

from typing import Protocol, final, runtime_checkable

import attrs

from .runtime_tracer import RuntimeTracer

# ----------------------- #


@runtime_checkable
class TxTracer(Protocol):
    """Records transaction scope boundaries for development diagnostics."""

    @property
    def enabled(self) -> bool:
        """Whether event recording is active."""
        ...

    def on_scope_enter(self, *, route: str, depth: int) -> None:
        """Record root transaction scope entry."""
        ...

    def on_scope_exit(self, *, route: str, depth: int) -> None:
        """Record root transaction scope exit."""
        ...


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class NoopTxTracer:
    """Transaction tracer that records nothing."""

    @property
    def enabled(self) -> bool:
        return False

    def on_scope_enter(self, *, route: str, depth: int) -> None:
        del route, depth
        return

    def on_scope_exit(self, *, route: str, depth: int) -> None:
        del route, depth
        return


# ....................... #

NOOP_TX_TRACER = NoopTxTracer()
"""Shared noop transaction tracer instance."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class RuntimeBackedTxTracer:
    """Delegates transaction events to a :class:`RuntimeTracer`."""

    runtime: RuntimeTracer
    """Underlying runtime event recorder."""

    # ....................... #

    @property
    def enabled(self) -> bool:
        return self.runtime.enabled

    # ....................... #

    def on_scope_enter(self, *, route: str, depth: int) -> None:
        self.runtime.record(
            domain="tx",
            op="enter",
            route=route,
            tx_route=route,
            tx_depth=depth,
        )

    # ....................... #

    def on_scope_exit(self, *, route: str, depth: int) -> None:
        self.runtime.record(
            domain="tx",
            op="exit",
            route=route,
            tx_route=route,
            tx_depth=depth,
        )


# ....................... #


def tx_tracer_from_runtime(runtime: RuntimeTracer) -> TxTracer:
    """Return a noop or runtime-backed transaction tracer."""

    if not runtime.enabled:
        return NOOP_TX_TRACER

    return RuntimeBackedTxTracer(runtime=runtime)
