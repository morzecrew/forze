"""What a quiesce sweep saw, plane by plane."""

from __future__ import annotations

from typing import Literal, final

import attrs

from forze.base.exceptions import exc

# ----------------------- #

PlaneState = Literal["settled", "residual", "not_wired", "error"]
"""How one plane ended the sweep.

``settled`` — observed, and nothing was left moving. ``residual`` — observed, but still
holding work when the budget ran out. ``not_wired`` — no port is registered for it, so there
is nothing to settle. ``error`` — the probe itself failed, so the plane's state is *unknown*
(which is not the same as empty, and does not attest).
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

    :attr:`attested` is the question RFC-grade callers actually ask — *may I now treat this
    runtime's state as final?* It is deliberately conservative: an unreadable plane counts
    against it, because "I could not look" is not "there was nothing there".
    """

    planes: tuple[QuiescePlane, ...]
    """Every plane the sweep touched, in the order it watched them."""

    # ....................... #

    @property
    def attested(self) -> bool:
        """Whether every plane came to a standstill.

        Planes this runtime does not wire (``not_wired``) do not count against it — there is
        nothing there to settle. A ``residual`` or an ``error`` does.

        This does **not** cover work the framework cannot see at all: a Temporal-backed
        workflow lives in the Temporal cluster and is invisible from here. Attestation
        speaks for the planes quiesce watched, and no others.
        """

        return all(plane.settled for plane in self.planes)

    # ....................... #

    @property
    def unsettled(self) -> tuple[QuiescePlane, ...]:
        """The planes that did not come to rest."""

        return tuple(plane for plane in self.planes if not plane.settled)

    # ....................... #

    def raise_if_unsettled(self) -> None:
        """Raise a single precondition error naming every plane that is still moving.

        For callers that must not proceed on a half-quiesced runtime — an export that would
        otherwise write an artifact missing whatever the outbox never emitted.
        """

        if self.attested:
            return

        lines = "\n".join(
            f"  - {plane.name}: [{plane.state}] {plane.detail}" for plane in self.unsettled
        )

        raise exc.precondition(
            f"Runtime did not quiesce: {len(self.unsettled)} plane(s) still moving:\n{lines}"
        )
