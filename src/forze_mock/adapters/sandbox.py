"""In-memory :class:`~forze.application.contracts.sandbox.SandboxPort` for tests / simulation.

The mock runs nothing. Each route is answered by a **pure function** of the request, which
is what makes the plane simulable at all: real out-of-process work is wall-clock work off
the loop, the exact thing the DST loop refuses, so under simulation the seam is where the
work stops and a scripted answer begins.

That scripting is the point rather than a limitation. A real child rarely OOMs on cue and
almost never gets killed at the interesting moment; a handler returns ``killed_oom``,
``killed_timeout``, ``spawn_failed`` or a truncated capture whenever the simulation asks,
so the paths that only run on a bad day are the ones a test can drive.

What the mock does **not** do is pretend to enforce. It declares the full capability
surface by default — including isolation tiers no in-memory object can provide — because a
route standing in for a container backend must let the gates behave as they will in
production. Register the real backend's capabilities (``registry.on(..., capabilities=...)``)
to make a route refuse exactly where its deployed counterpart would.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable
from typing import final

import attrs

from forze.application.contracts.sandbox import (
    FULL_SANDBOX_CAPABILITIES,
    SandboxCapabilities,
    SandboxEvent,
    SandboxPort,
    SandboxRequest,
    SandboxResult,
    SandboxSpec,
    validate_stream_supported,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

# ----------------------- #

MOCK_SANDBOX_BACKEND = "mock"
"""Backend label used in boundary errors."""

MockSandboxRun = Callable[[SandboxRequest], SandboxResult]
"""Answer for one sandbox route: a pure function of the request.

Pure is the contract — no ambient time, randomness or I/O — so a simulation replay produces
the same run twice. A handler that wants to vary by call counts its own calls in a closure,
which is deterministic and visible, rather than reading a clock."""


@final
@attrs.define(slots=True)
class MockSandboxRegistry:
    """Programmable in-memory sandbox answers, keyed by route (spec) name."""

    _runs: dict[str, MockSandboxRun] = attrs.field(factory=dict[str, MockSandboxRun])
    _capabilities: dict[str, SandboxCapabilities] = attrs.field(
        factory=dict[str, SandboxCapabilities],
    )
    calls: dict[str, list[SandboxRequest]] = attrs.field(
        factory=dict[str, list[SandboxRequest]],
    )
    """Every request each route received, in order — what a test asserts against when the
    interesting part is *what was asked*, not what came back."""

    # ....................... #

    def on(
        self,
        route: StrKey | str,
        run: MockSandboxRun,
        *,
        capabilities: SandboxCapabilities | None = None,
    ) -> MockSandboxRegistry:
        """Register *run* for sandbox *route*. Returns self (chainable).

        Pass *capabilities* to stand in for a specific backend: the mock otherwise
        advertises the full surface, so every gate passes against the oracle and can only
        fail in production — the divergence a mock exists to catch. Declaring the real
        adapter's surface makes a gated request fail here exactly where it would there.
        """

        self._runs[str(route)] = run

        if capabilities is not None:
            self._capabilities[str(route)] = capabilities

        else:
            # Re-registering without capabilities restores the full-surface default; a
            # stale declaration from an earlier registration must not linger.
            self._capabilities.pop(str(route), None)

        return self

    # ....................... #

    def run_for(self, route: str) -> MockSandboxRun | None:
        return self._runs.get(route)

    def capabilities_for(self, route: str) -> SandboxCapabilities | None:
        return self._capabilities.get(route)

    def requests_for(self, route: StrKey | str) -> tuple[SandboxRequest, ...]:
        """Requests seen by *route*, oldest first."""

        return tuple(self.calls.get(str(route), ()))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockSandbox(SandboxPort):
    """In-memory ``SandboxPort`` bound to one spec + an answer registry."""

    spec: SandboxSpec
    registry: MockSandboxRegistry

    # ....................... #

    @property
    def sandbox_capabilities(self) -> SandboxCapabilities:
        return self.registry.capabilities_for(str(self.spec.name)) or FULL_SANDBOX_CAPABILITIES

    # ....................... #

    async def run(self, request: SandboxRequest) -> SandboxResult:
        """Answer *request* from the registered function for this route."""

        route = str(self.spec.name)
        answer = self.registry.run_for(route)

        if answer is None:
            raise exc.internal(
                f"Mock sandbox route {route!r} has no registered run. Program it with "
                "MockSandboxRegistry.on(route, run) — the mock executes nothing, so an "
                "unprogrammed route has no honest answer to give.",
                code="mock.sandbox.unprogrammed",
                details={"route": route},
            )

        self.registry.calls.setdefault(route, []).append(request)

        return answer(request)

    # ....................... #

    async def run_stream(self, request: SandboxRequest) -> AsyncGenerator[SandboxEvent]:
        """Replay a registered answer as a stream: output first, then the result.

        Refused when the route stands in for a backend that declares no streaming, so a
        caller relying on it fails against the oracle rather than in production.
        """

        validate_stream_supported(self.sandbox_capabilities, backend=MOCK_SANDBOX_BACKEND)

        result = await self.run(request)

        if result.stdout.text:
            yield SandboxEvent(kind="stdout", text=result.stdout.text)

        if result.stderr.text:
            yield SandboxEvent(kind="stderr", text=result.stderr.text)

        yield SandboxEvent(kind="result", result=result)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMockSandbox:
    """Build a :class:`MockSandbox` for a given spec (one factory, every route)."""

    registry: MockSandboxRegistry

    # ....................... #

    def __call__(self, ctx: object, spec: SandboxSpec) -> MockSandbox:
        _ = ctx

        return MockSandbox(spec=spec, registry=self.registry)
