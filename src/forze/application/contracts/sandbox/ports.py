"""The sandbox port (command-plane).

One :class:`~forze.application.contracts.sandbox.specs.SandboxSpec` names one governed
execution route; the wiring config binds it to a backend — a bare subprocess, a container
runtime, a remote sandbox service — so moving from "runs on this host" to "runs in a
container" is a wiring change with no handler edits.

The port is **command-plane**: spawning a process is an effect on the world, so it cannot
be acquired inside a read-only (``QUERY``) operation, whatever the child then does.
"""

from collections.abc import AsyncGenerator, Awaitable
from typing import Protocol, runtime_checkable

from .capabilities import SandboxCapabilities
from .specs import SandboxSpec
from .value_objects import SandboxEvent, SandboxRequest, SandboxResult

# ----------------------- #


@runtime_checkable
class BaseSandboxPort(Protocol):
    """Shared ``spec`` binding for sandbox adapters."""

    spec: SandboxSpec
    """``SandboxSpec`` for this port instance."""


# ....................... #


class SandboxPort(BaseSandboxPort, Protocol):
    """One governed out-of-process execution route, bound to one spec."""

    def run(self, request: SandboxRequest) -> Awaitable[SandboxResult]:
        """Run *request* to completion and return what happened.

        A child that exited non-zero, ran out of memory, or was killed on its deadline is a
        :class:`SandboxResult` — the caller owns that policy, because for generated code a
        non-zero exit is often the answer rather than an error. Only the framework's own
        failures raise: a workspace it could not create, staging that could not read an
        input, an output it could not store.
        """
        ...

    def run_stream(self, request: SandboxRequest) -> AsyncGenerator[SandboxEvent]:
        """Run *request*, yielding output as it arrives and the result last.

        Exactly one ``result`` event, and it is the last: a consumer that keeps the newest
        one it saw ends with the run's answer whatever came before it.

        **A caller who stops iterating stops the run.** Abandoning the generator — a
        ``break``, an exception, a cancelled task — closes it, and closing it ends the work
        and cleans up after it, exactly as :meth:`run` does on every exit path. The
        guarantee belongs to the contract rather than to one adapter, because a caller
        writing the ``break`` cannot see which one it is talking to. Wrap the iteration in
        :func:`contextlib.aclosing` to make that happen at a point you chose rather than
        whenever the generator is collected.

        Backends that cannot serve it refuse up front (``sandbox_feature_unsupported``)
        rather than buffering the whole run and pretending.
        """
        ...

    @property
    def sandbox_capabilities(self) -> SandboxCapabilities:
        """What this adapter actually confines and enforces."""
        ...
