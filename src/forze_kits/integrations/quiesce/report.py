"""What a quiesce sweep saw, plane by plane."""

from __future__ import annotations

from typing import Literal, final

import attrs

from forze.base.exceptions import exc

# ----------------------- #

PlaneState = Literal["settled", "residual", "not_wired", "unobserved", "error"]
"""How one plane ended the sweep.

``settled`` — observed, and nothing was left moving. ``residual`` — observed, but still
holding work when the budget ran out. ``not_wired`` — the plane does not exist on this runtime
(no spec, no port), so there is genuinely nothing to settle. ``unobserved`` — the plane
**exists** (a spec is catalogued, or the caller named it) but the sweep has no way to read it:
its admin port is not wired, no probe exists for its kind, or the runtime carries no inventory
at all. An unobserved plane is not an empty one, and it does not attest — the difference
between ``not_wired`` and ``unobserved`` is the difference between "nothing there" and "cannot
look". ``error`` — the probe itself failed, so the plane's state is *unknown* (which is not
the same as empty, and does not attest).
"""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class QuiescePlane:
    """One plane's outcome."""

    name: str
    """What was watched — ``operations``, ``outbox:events``, ``durable``, ``stream:orders``."""

    state: PlaneState
    """How it ended."""

    detail: str = ""
    """Human-readable residual: what was still moving, and (where known) for how long."""

    # ....................... #

    @property
    def settled(self) -> bool:
        """Whether this plane came to rest (or had nothing to come to rest about)."""

        return self.state in ("settled", "not_wired")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class QuiesceReport:
    """The outcome of a quiesce sweep.

    :attr:`settled` and :attr:`attested` answer two different questions, and the difference
    is the whole point of the type. *Settled* is an observation: nothing was moving when the
    sweep finished. *Attested* is a promise: nothing was moving **and nothing could arrive**,
    because the runtime was holding the door shut. Only the second is safe to build on — an
    export written from a merely-settled runtime can be stale before it is finished.
    """

    planes: tuple[QuiescePlane, ...]
    """Every plane the sweep touched, in the order it watched them."""

    admission_held: bool
    """Whether the runtime was refusing new invocations for the length of the sweep.

    ``False`` when quiesce only *looked* (``close_gate=False``): the planes may all have been
    at rest, but nothing stopped a handler from committing the moment the sweep looked away,
    so the reading cannot be built on. See :attr:`attested`."""

    # ....................... #

    @property
    def settled(self) -> bool:
        """Whether every plane was at rest when the sweep finished.

        Planes this runtime does not wire (``not_wired``) do not count against it — there is
        nothing there to settle. A ``residual`` or an ``error`` does: an unreadable plane is
        not an empty one.
        """

        return all(plane.settled for plane in self.planes)

    # ....................... #

    @property
    def attested(self) -> bool:
        """Whether the runtime came to a standstill *and* was held there.

        This is the question a caller with consequences asks — *may I now treat this
        runtime's state as final?* It needs both halves: every plane at rest
        (:attr:`settled`) **and** admission closed (:attr:`admission_held`), or the answer
        was already going stale as it was computed.

        It speaks only for this process and only for the planes quiesce can see. A Temporal
        workflow lives in the Temporal cluster, and a sibling replica that is still serving
        writes is invisible from here — a fleet-wide claim is a deployment procedure, not
        something a report can make.
        """

        return self.settled and self.admission_held

    # ....................... #

    @property
    def unsettled(self) -> tuple[QuiescePlane, ...]:
        """The planes that did not come to rest."""

        return tuple(plane for plane in self.planes if not plane.settled)

    # ....................... #

    def raise_if_unattested(self) -> None:
        """Raise unless the runtime came to a standstill and was held there.

        For callers that must not proceed on a half-quiesced runtime — an export that would
        otherwise write an artifact missing whatever the outbox never emitted, or one that
        was overtaken by a write while it was being written.
        """

        if self.attested:
            return

        reasons = [f"  - {plane.name}: [{plane.state}] {plane.detail}" for plane in self.unsettled]

        if not self.admission_held:
            reasons.append(
                "  - operations: admission was never closed (close_gate=False), so the "
                "runtime could accept new work at any point during the sweep"
            )

        raise exc.precondition("Runtime is not quiesced:\n" + "\n".join(reasons))
