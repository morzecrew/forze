"""What a sandbox adapter can actually do, declared per backend and enforced fail-closed.

The plane's honesty lives here. Every adapter publishes an isolation tier and a set of
enforcement flags, and the gates read them rather than the adapter's name — so "is this
thing a sandbox?" is a value the wiring can compare, not a claim in a docstring.

The rule that keeps it honest: **a declared capability without a test that exercises it is
a lie.** No adapter claims :attr:`SandboxCapabilities.enforces_memory` without a test that
drives a real child into its memory ceiling on that backend, and none claims a tier it
cannot demonstrate.
"""

from typing import Final, Literal

import attrs

from forze.base.exceptions import exc

# ----------------------- #

UNSUPPORTED_SANDBOX_FEATURE_CODE = "sandbox_feature_unsupported"
"""Error code raised when a request needs a feature the backend lacks."""

UNDERISOLATED_CODE = "sandbox_untrusted_underisolated"
"""Error code for untrusted provenance on an adapter that cannot contain it."""

Isolation = Literal["none", "process", "container", "vm"]
"""Increasing containment.

``none`` — a bare child: same kernel, filesystem, network and user as the worker. Not a
sandbox; the word appears in this plane's name to make an author think about containment,
not because this tier provides any.

``process`` — resource and fault isolation: rlimits, a fresh workspace, optionally a
dropped uid. It bounds *accidents*, not adversaries: the filesystem, the network, ``/proc``
and ptrace are all still shared, so it is explicitly **not** a security boundary.

``container`` — namespaced filesystem, network and pids. The first tier that confines a
program written by someone you do not trust.

``vm`` — microVM or gVisor-class supervisor, for actively hostile code."""

_ISOLATION_ORDER: Final[dict[Isolation, int]] = {
    "none": 0,
    "process": 1,
    "container": 2,
    "vm": 3,
}
"""Rank for comparisons. A dict rather than the ``Literal``'s order because the gate has to
compare tiers, and comparing strings alphabetically would rank ``container`` below ``none``
and pass every untrusted route."""

MINIMUM_UNTRUSTED_ISOLATION: Final[Isolation] = "container"
"""The floor for untrusted code. Process isolation is not a security boundary, so nothing
below this may run a program nobody reviewed."""


def isolation_rank(isolation: Isolation) -> int:
    """Rank of an isolation tier, for comparisons."""

    return _ISOLATION_ORDER[isolation]


def contains_untrusted(isolation: Isolation) -> bool:
    """Whether *isolation* is enough to run code of untrusted provenance."""

    return isolation_rank(isolation) >= isolation_rank(MINIMUM_UNTRUSTED_ISOLATION)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SandboxCapabilities:
    """What a sandbox adapter can serve, declared per backend.

    Defaults describe the weakest honest surface — a bare child that can be killed and
    nothing more. Richer adapters widen it via :func:`attrs.evolve`, and each widening is a
    claim they owe a test.
    """

    isolation: Isolation = "none"
    """Containment tier — what this adapter actually confines."""

    network: Literal["none", "egress"] = "none"
    """Whether the child can reach the network. ``egress`` is a data-exfiltration surface
    for generated code, so a route enabling it must acknowledge that explicitly."""

    enforces_memory: bool = False
    """Whether a memory ceiling is imposed on the child (and an over-run surfaces as
    ``killed_oom`` rather than as a dead worker)."""

    enforces_cpu: bool = False
    """Whether a CPU-time ceiling is imposed on the child."""

    enforces_open_files: bool = False
    """Whether a file-descriptor ceiling is imposed on the child."""

    hard_kill: bool = True
    """Whether the adapter can actually kill a running child. Out-of-process work is the
    reason this plane can promise a red button at all, so an adapter that cannot is
    declaring something a caller must know before relying on a timeout."""

    reaps_descendants: bool = False
    """Whether killing the sandbox reaps everything it started.

    A bare subprocess kills the process it spawned; a child that double-forked can orphan
    its own children. Only the container and vm tiers guarantee the whole tree goes."""

    supports_stream: bool = False
    """Whether ``run_stream`` is honored rather than refused."""


DEFAULT_SANDBOX_CAPABILITIES: Final = SandboxCapabilities()
"""The narrowest surface: a killable bare child, no enforcement, no streaming."""

FULL_SANDBOX_CAPABILITIES: Final = SandboxCapabilities(
    isolation="vm",
    network="egress",
    enforces_memory=True,
    enforces_cpu=True,
    enforces_open_files=True,
    hard_kill=True,
    reaps_descendants=True,
    supports_stream=True,
)
"""The superset a mock may claim while standing in for any backend."""


# ....................... #


def validate_stream_supported(capabilities: SandboxCapabilities, *, backend: str) -> None:
    """Refuse ``run_stream`` on a backend that cannot serve it."""

    if not capabilities.supports_stream:
        raise exc.precondition(
            f"Sandbox backend {backend!r} does not support streamed execution; use run().",
            code=UNSUPPORTED_SANDBOX_FEATURE_CODE,
            details={"backend": backend, "feature": "run_stream"},
        )


def validate_provenance(
    *,
    provenance: str,
    capabilities: SandboxCapabilities,
    backend: str,
    route: str,
) -> None:
    """Refuse untrusted code on an adapter that cannot contain it.

    The plane's single most valuable check. Everything else here is ergonomics; this is the
    line that stops generated code running in something that shares the host's filesystem
    because a container was inconvenient on the afternoon it was wired.
    """

    if provenance != "untrusted" or contains_untrusted(capabilities.isolation):
        return

    raise exc.configuration(
        f"Sandbox route {route!r} runs untrusted code on backend {backend!r}, whose isolation "
        f"is {capabilities.isolation!r}. Untrusted provenance requires "
        f"{MINIMUM_UNTRUSTED_ISOLATION!r} or stronger: a bare subprocess shares this host's "
        "filesystem, network and /proc with the worker, and process-tier limits bound "
        "accidents rather than adversaries. Wire a containerized adapter for this route, or "
        "declare the provenance trusted if the code really is yours.",
        code=UNDERISOLATED_CODE,
        details={"route": route, "backend": backend, "isolation": capabilities.isolation},
    )
