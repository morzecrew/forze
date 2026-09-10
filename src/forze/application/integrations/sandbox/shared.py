"""What every sandbox adapter has to do the same way, whatever it runs the code in.

Four of these decide something a caller can be lied to about — how long a run really gets,
how much of its output is kept, which environment reaches it, and whether a resolved secret
survives into the captured text — so they live in one place rather than once per tier. A
second adapter that narrowed the budget slightly differently, or masked a secret in one
capture and not the other, would be a divergence nobody sees until the two are compared.

The tier-specific half stays in the adapter: how the ceilings are applied, how files cross
into the workspace, and what ends the run are all things the backends genuinely do
differently, and pretending otherwise here would be worse than the duplication.
"""

from datetime import timedelta
from typing import TYPE_CHECKING, Any

import attrs

from forze.application.contracts.sandbox import (
    CapturedStream,
    SandboxRequest,
)
from forze.application.contracts.secrets import SecretRef, SecretsDepKey
from forze.application.contracts.storage import StorageSpec
from forze.base.exceptions import exc
from forze.base.scrubbing import SECRET_PLACEHOLDER

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #

STORAGE_UNWIRED_CODE = "sandbox_storage_unwired"
"""Error code for a route asked to stage or collect files with no storage wired."""


# ....................... #


def budget_seconds(ceiling: timedelta, request: SandboxRequest, remaining: float | None) -> float:
    """Seconds this run gets: the route's ceiling, narrowed by whoever asks for less.

    Never widened. The request narrows its own budget, the invocation deadline narrows it
    again, and the route's ceiling is the roof over both.
    """

    seconds = ceiling.total_seconds()

    if request.timeout is not None:
        seconds = min(seconds, request.timeout.total_seconds())

    if request.resources is not None and request.resources.wall_clock is not None:
        seconds = min(seconds, request.resources.wall_clock.total_seconds())

    if remaining is not None:
        seconds = min(seconds, remaining)

    return max(seconds, 0.0)


# ....................... #


def output_cap(cap: int, request: SandboxRequest) -> int:
    """The route's per-stream capture cap, narrowed by the request if it asked for less."""

    if request.resources is not None and request.resources.max_output_bytes is not None:
        return min(cap, request.resources.max_output_bytes)

    return cap


# ....................... #


async def resolve_environment(
    ctx: "ExecutionContext",
    passthrough: tuple[str, ...],
    request: SandboxRequest,
    *,
    host_environment: dict[str, str],
) -> tuple[dict[str, str], tuple[str, ...]]:
    """Exactly what the request named, plus the route's declared passthrough.

    Returns the environment and the resolved secret values in it, which the caller masks out
    of the capture. A child can print what it was given — deliberately, or in a traceback
    that dumps its whole environment — and a ``SandboxResult`` is journaled verbatim by a
    durable step, so a secret that reaches the capture reaches storage.

    *host_environment* is passed in rather than read here because the tiers disagree about
    what "the host" means: a bare child inherits this process's variables and a container
    inherits its image's, so a route wiring a passthrough list is naming different things in
    each. Sorted longest-first on the way out, so masking a secret that contains another
    does not leave a readable fragment of the one still to be replaced.
    """

    env = {name: host_environment[name] for name in passthrough if name in host_environment}
    secrets: Any = None
    resolved: list[str] = []

    for name, value in request.env.items():
        if isinstance(value, SecretRef):
            if secrets is None:
                secrets = ctx.deps.provide(SecretsDepKey)

            env[name] = await secrets.resolve_str(value)
            resolved.append(env[name])

        else:
            env[name] = value

    return env, tuple(sorted({value for value in resolved if value}, key=len, reverse=True))


# ....................... #


def mask_secrets(
    captured: tuple[CapturedStream, CapturedStream], secrets: tuple[str, ...]
) -> tuple[CapturedStream, CapturedStream]:
    """Replace every resolved secret value in the captures with the scrubber's placeholder.

    Longest first, so a secret that contains another is not half-replaced into a fragment of
    the one still readable. The byte count and the truncation flag are untouched: they
    describe what the child wrote, and rewriting them to match the masked text would make
    the capture lie about the run instead of about the secret.
    """

    if not secrets:
        return captured

    def scrub(stream: CapturedStream) -> CapturedStream:
        text = stream.text

        for secret in secrets:
            text = text.replace(secret, SECRET_PLACEHOLDER)

        if text == stream.text:
            return stream

        return attrs.evolve(stream, text=text)

    return scrub(captured[0]), scrub(captured[1])


# ....................... #


def refuse_unwired_storage(
    request: SandboxRequest, storage: StorageSpec | None, *, route: str
) -> None:
    """Refuse, before anything runs, a request whose files have nowhere to come from."""

    if (request.input_files or request.output_globs) and storage is None:
        raise exc.configuration(
            f"Sandbox route {route!r} stages or collects files but is wired with no storage "
            "spec. Running the command anyway would start it without its inputs, or discard "
            "the outputs it was asked to produce.",
            code=STORAGE_UNWIRED_CODE,
            details={"route": route},
        )


def require_storage(storage: StorageSpec | None, *, route: str) -> StorageSpec:
    """The route's storage spec, or the refusal that :func:`refuse_unwired_storage` made."""

    if storage is None:  # pragma: no cover - guarded before any staging runs
        raise exc.configuration(
            f"Sandbox route {route!r} has no storage spec wired.",
            code=STORAGE_UNWIRED_CODE,
        )

    return storage
