"""Shared ``SandboxPort`` conformance battery — the seam, never what runs inside it.

Two implementations answer this port and most of what they do is *meant* to differ. The
mock executes nothing and says so through its capabilities; the container adapter really
starts a process, kills it, and destroys the workspace afterwards. So this battery asserts
only the promises that must hold whichever one a handler resolved, and the RFC's own
doctrine is the scope line: the framework guarantees the seam, not the program's behaviour.

**What is here.** A non-zero exit is a result rather than an exception, and a caller's
policy rather than the plane's. A streamed run ends with exactly one result event, so a
consumer keeping the newest one it saw ends with the run's answer. A request naming a
ceiling its route does not impose is refused rather than run uncapped. And untrusted
provenance resolves exactly when the route's isolation can hold it — the plane's headline
gate, stated as the rule both engines obey rather than as the answer either one gives.

**What is deliberately not here.** Isolation, kills, and reaping: only the real adapter can
be wrong about those, and a mock that pretended to enforce them would prove the pretence.
Staged inputs and collected outputs are in the same category for a less obvious reason —
the mock runs nothing and stages nothing, and its per-route answer is documented as a pure
function of the request, so a leg asserting that declared outputs travel would be asserting
it against a handler this file wrote. That is a test of the battery, not of the plane.

Used by:

- ``tests/unit/test_forze_mock/test_mock_sandbox_conformance.py`` (the oracle)
- ``tests/integration/test_forze_sandbox_container/test_container_sandbox_conformance.py``
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol

import attrs
import pytest

from forze.application.contracts.sandbox import (
    UNDERISOLATED_CODE,
    UNSUPPORTED_SANDBOX_FEATURE_CODE,
    ResourceRequest,
    SandboxCapabilities,
    SandboxPort,
    SandboxRequest,
    contains_untrusted,
)
from forze.base.exceptions import CoreException

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SandboxScript:
    """What one run should produce, in terms both engines can honour.

    The container leg turns this into a program; the mock leg turns it into the answer its
    route returns. Neither is asked for anything an in-memory object cannot do honestly.
    """

    stdout: str = ""
    """Text the run writes to standard output, ending in a newline if it should have one."""

    stderr: str = ""
    """Text the run writes to standard error."""

    exit_code: int = 0
    """Status the run exits with."""


@attrs.define(slots=True, kw_only=True, frozen=True)
class SandboxHarness:
    """One backend's seam for the sandbox battery."""

    backend: str
    """Engine name, for failure messages that say which leg broke."""

    capabilities: SandboxCapabilities
    """What :attr:`invoke`'s route declares. Read by the checks that are about the rule an
    engine follows rather than about the answer it happens to give."""

    invoke: Callable[[SandboxScript], tuple[SandboxPort, SandboxRequest]]
    """A port on a permissive route, and the request that makes it produce *script*."""

    resolve: Callable[[str], SandboxPort]
    """Resolve a port for a spec of the given provenance, raising if the route may not
    serve it. The refusal is the plane's headline gate and belongs to the resolve, not to
    the call."""

    uncapped: Callable[[], SandboxPort]
    """A port on a route that imposes no memory ceiling, for the refusal a request asking
    for one must meet."""


class Check(Protocol):
    """One battery check, run against every engine's harness."""

    __name__: str

    def __call__(self, harness: SandboxHarness) -> Awaitable[None]: ...


# ....................... #


async def check_a_zero_exit_carries_the_run_s_own_output(harness: SandboxHarness) -> None:
    """A successful run comes back as a result carrying what it wrote."""

    port, request = harness.invoke(SandboxScript(stdout="ready\n", stderr="warned\n"))
    result = await port.run(request)

    assert result.outcome == "exited", harness.backend
    assert result.exit_code == 0
    assert result.succeeded
    assert result.stdout.text == "ready\n"
    assert result.stderr.text == "warned\n"
    assert not result.stdout.truncated


async def check_a_non_zero_exit_is_a_result_not_an_exception(harness: SandboxHarness) -> None:
    """Exit 3 is an answer. Whether it is a failure is the caller's policy.

    The one promise most likely to be implemented as an exception by an adapter author
    reaching for the obvious shape — and the one that would make every generated script
    reporting through its status code look like an infrastructure fault.
    """

    port, request = harness.invoke(SandboxScript(stdout="partial\n", exit_code=3))
    result = await port.run(request)

    assert result.outcome == "exited", harness.backend
    assert result.exit_code == 3
    assert not result.succeeded
    assert result.stdout.text == "partial\n"


async def check_a_stream_ends_with_exactly_one_result(harness: SandboxHarness) -> None:
    """A streamed run yields one result event, and it is last.

    Both halves matter. *Exactly one* is what lets a consumer keep the newest result it saw
    without deciding between two; *last* is what makes that the run's answer rather than an
    early guess at it.
    """

    if not harness.capabilities.supports_stream:  # pragma: no cover - both engines stream
        pytest.skip(f"{harness.backend} declares no streaming")

    port, request = harness.invoke(SandboxScript(stdout="chunk\n", exit_code=1))
    events = [event async for event in port.run_stream(request)]

    assert [event.kind for event in events].count("result") == 1, harness.backend
    assert events[-1].kind == "result"
    assert events[-1].result is not None
    assert events[-1].result.exit_code == 1
    assert "chunk" in "".join(event.text for event in events if event.kind == "stdout")


async def check_a_ceiling_the_route_does_not_impose_is_refused(harness: SandboxHarness) -> None:
    """Asking for a limit the backend cannot apply fails, rather than running uncapped.

    Fail-closed because the alternative is silent: a caller that asked for a memory ceiling
    and did not get one believes the child is bounded, and finds out otherwise on the run
    that would have hit it.
    """

    port = harness.uncapped()

    with pytest.raises(CoreException) as raised:
        await port.run(
            SandboxRequest(
                command=("true",),
                resources=ResourceRequest(memory_bytes=64 * 1024 * 1024),
            )
        )

    assert raised.value.code == UNSUPPORTED_SANDBOX_FEATURE_CODE, harness.backend


async def check_untrusted_resolves_exactly_when_the_tier_can_hold_it(
    harness: SandboxHarness,
) -> None:
    """The plane's headline gate, as the rule rather than as either engine's answer.

    Each leg proves its own branch — a route standing in for a bare child refuses, a
    container route serves — and what the battery pins is that both decide it the same way,
    from the declared isolation rather than from the adapter's name.
    """

    expected = contains_untrusted(harness.capabilities.isolation)

    try:
        port = harness.resolve("untrusted")

    except CoreException as error:
        assert not expected, f"{harness.backend} refused untrusted code it can contain"
        assert error.code == UNDERISOLATED_CODE

        return

    assert expected, f"{harness.backend} served untrusted code it cannot contain"
    assert contains_untrusted(port.sandbox_capabilities.isolation)


async def check_a_trusted_spec_always_resolves(harness: SandboxHarness) -> None:
    """Nothing refuses trusted provenance. The gate is about what the code is, not the tier.

    Paired with the check above so a backend cannot pass that one by refusing everything.
    """

    port = harness.resolve("trusted")

    assert port.sandbox_capabilities.isolation == harness.capabilities.isolation, harness.backend


# ....................... #

SANDBOX_BATTERY: tuple[Check, ...] = (
    check_a_zero_exit_carries_the_run_s_own_output,
    check_a_non_zero_exit_is_a_result_not_an_exception,
    check_a_stream_ends_with_exactly_one_result,
    check_a_ceiling_the_route_does_not_impose_is_refused,
    check_untrusted_resolves_exactly_when_the_tier_can_hold_it,
    check_a_trusted_spec_always_resolves,
)
