"""Specification for one governed out-of-process execution route."""

from typing import Literal, final

import attrs

from ..base import BaseSpec

# ----------------------- #

Provenance = Literal["trusted", "untrusted"]
"""Who wrote the code this route runs.

``trusted`` — a program the application ships or a human reviewed. ``untrusted`` —
generated, user-supplied, or otherwise unreviewed per-execution. It is a *threat
declaration*, not a capability: what it buys is the wiring refusal that stops untrusted
code reaching an adapter that cannot contain it."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SandboxSpec(BaseSpec):
    """One logical out-of-process execution route.

    The spec carries only the *portable* declaration — what the code is. Everything
    deployment-shaped (image, resource ceilings, network policy, mounts, which storage
    bucket stages the files) lives on the route's wiring config, where the backend enforces
    it and where a boot-time gate can read it.

    :attr:`provenance` has no default on purpose. A plane whose whole value is a
    fail-closed threat gate cannot let the threat declaration be forgotten into the safe
    look-alike, and there is no honest default: guessing ``trusted`` would run generated
    code in a bare subprocess, and guessing ``untrusted`` would fail the boot of every
    application that only ever runs its own binaries.
    """

    provenance: Provenance
    """Who wrote the code — the threat declaration the wiring gate reads."""

    description: str | None = None
    """Optional human-readable description for documentation."""
