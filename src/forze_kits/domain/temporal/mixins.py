"""The two dates an effective-dated row carries, and what they may not say."""

from datetime import date
from typing import ClassVar, Self

from pydantic import Field, model_validator

from forze.base.exceptions import exc
from forze.base.primitives import Bounds, Period
from forze.domain.models import CoreModel

# ----------------------- #


class TemporalMixin(CoreModel):
    """When this row's assertion is in force.

    Two scalar columns rather than one range value: the filter language compares scalars and
    every adapter maps a date, while what the pair *means* — which endpoint is in force, what an
    open end covers, when two of them overlap — is
    :class:`~forze.base.primitives.Period`'s, which the aggregate's declared convention names.

    :attr:`valid_from` is frozen because moving it moves the row's period, and the rule that two
    periods under one key may not overlap is kept by the store against the row as written. An
    aggregate that needs to restate when something came into force is asserting a different
    fact, not editing this one.
    """

    temporal_bounds: ClassVar[Bounds] = "[]"
    """Which endpoints this aggregate puts in force.

    Here as well as on the policy because only the model can be asked about a row on *every*
    write path — a patch that moves an end date carries no start date, so nothing between the
    boundary and the store sees the period the write actually produces. The kit refuses to
    build when the two disagree, so the duplication cannot drift."""

    valid_from: date = Field(frozen=True)
    """The day the assertion comes into force."""

    valid_to: date | None = None
    """The day it stops, or ``None`` while it is still in force."""

    # ....................... #

    @model_validator(mode="after")
    def _validate_validity(self) -> Self:
        """Refuse a period that is in force on no day.

        Two shapes of the same defect. An end *before* its start is empty under every
        convention; an end *equal* to its start is empty under every convention but ``"[]"``,
        where it is an ordinary one-day period. Either way the row would sit in the relation
        answering no effective-on query and colliding with nothing, which is not a state a
        caller means to create — "end this as of the day it started" means delete it.

        On the model rather than on the generated operations because the update patch carries
        one endpoint and the stored row carries the other, so only here is the resulting period
        visible; and because a repair script or a hand-written handler reaches the row through
        the port without passing an operation at all.
        """

        # Checked before `Period` is built, because building one over an inverted pair raises
        # `validation` from the value object — a different kind, carrying none of the detail a
        # caller of this aggregate needs, for a case this validator's own docstring claims.
        if self.valid_to is not None and self.valid_to < self.valid_from:
            raise exc.domain(
                f"Validity ends before it starts: valid_to {self.valid_to.isoformat()} "
                f"precedes valid_from {self.valid_from.isoformat()}. A period like that is in "
                "force on no day, so nothing reads it and nothing conflicts with it.",
                details={
                    "valid_from": self.valid_from.isoformat(),
                    "valid_to": self.valid_to.isoformat(),
                },
            )

        if not Period(self.valid_from, self.valid_to, self.temporal_bounds).is_empty:
            return self

        # How far apart the endpoints must be to cover a single day: neither excluded endpoint
        # is in force, so each one costs a day.
        shortest = (0 if self.temporal_bounds[0] == "[" else 1) + (
            0 if self.temporal_bounds[1] == "]" else 1
        )

        raise exc.domain(
            f"Validity from {self.valid_from.isoformat()} to "
            f"{self.valid_to.isoformat() if self.valid_to else '∞'} is in force on no day under "
            f"bounds {self.temporal_bounds!r}. Nothing would read this row and nothing would "
            f"conflict with it. Under {self.temporal_bounds!r} a period covering a single day "
            f"spans {shortest} day(s) between its endpoints.",
            details={"valid_from": self.valid_from.isoformat(), "bounds": self.temporal_bounds},
        )


# ....................... #


class CreateCmdWithTemporal(CoreModel):
    """The create-command half: the same two dates, supplied by the caller.

    Unlike the versioned kit's lineage fields, these are the author's to set — when a contract
    starts and ends is the fact being recorded, not bookkeeping the kit fills in.
    """

    valid_from: date
    """The day the assertion comes into force."""

    valid_to: date | None = None
    """The day it stops, or ``None`` while it is still in force."""
