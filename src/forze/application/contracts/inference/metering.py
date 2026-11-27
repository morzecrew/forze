"""What the inference calls inside a scope spent, for a caller that stores it.

Usage is telemetry first: an adapter puts a call's token counts on its span, and ``predict``
returns the prediction alone. A caller that keeps the cost with what it produced — a
proposal, a draft — opens a meter scope around the calls instead of receiving an envelope
from each one:

.. code-block:: python

    async with ctx.inference.metered() as meter:
        mapping = await ctx.inference.model(MAPPER).predict(source)

    draft = Draft(mapping=mapping, model=meter.model, tokens=meter.tokens)

The meter is fed where an adapter records usage, once for each response it reads. A retried
or hedged call and a call that raised after its response arrived spent tokens too, and a
reader handed only the returned prediction would never see them. An attempt abandoned in
flight, as a hedge's loser or one a timeout cancels, reports nothing, though the provider may
bill it.
"""

from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from contextvars import ContextVar
from types import MappingProxyType
from typing import Final, final

import attrs

from forze.base.primitives import monotonic

# ----------------------- #


def _sum(left: int | None, right: int | None) -> int | None:
    """Two counts added, where ``None`` is a count nothing reported."""

    if left is None:
        return right

    if right is None:
        return left

    return left + right


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class InferenceUsage:
    """Token counts one model reported. ``None`` is a count it did not report, never zero."""

    input_tokens: int | None = None
    output_tokens: int | None = None

    # ....................... #

    @property
    def tokens(self) -> int | None:
        """Input and output together, or ``None`` when neither was reported."""

        return _sum(self.input_tokens, self.output_tokens)

    # ....................... #

    def __add__(self, other: "InferenceUsage") -> "InferenceUsage":
        return InferenceUsage(
            input_tokens=_sum(self.input_tokens, other.input_tokens),
            output_tokens=_sum(self.output_tokens, other.output_tokens),
        )


# ....................... #


@final
@attrs.define(slots=True, eq=False)
class InferenceMeter:
    """What the inference calls inside one :meth:`InferenceDeps.metered` scope spent.

    It counts every request an adapter reports inside the scope, including those of tasks
    the scope started, since a task inherits the scope it was created in. A scope inside
    another reports to both. Its numbers stop moving when it exits: a task that outlives
    the scope still reports to any scope around it, but not to this one.

    It sums what the calls reported. A route that reports no usage, such as a scoring
    dialect or an in-process model, adds nothing, so a meter over only those reads ``None``.
    """

    _parent: "InferenceMeter | None" = attrs.field(alias="parent")
    _started: float = attrs.field(factory=monotonic, init=False)
    _finished: float | None = attrs.field(default=None, init=False)
    _usage: dict[str, InferenceUsage] = attrs.field(factory=dict, init=False)

    # ....................... #

    @property
    def by_model(self) -> Mapping[str, InferenceUsage]:
        """Usage per model, under the name the provider reported, in order of first use."""

        return MappingProxyType(dict(self._usage))

    @property
    def model(self) -> str | None:
        """The model the calls used, or ``None`` when none reported or several did."""

        return next(iter(self._usage)) if len(self._usage) == 1 else None

    @property
    def total(self) -> InferenceUsage:
        """Usage summed across every model."""

        total = InferenceUsage()

        for usage in self._usage.values():
            total += usage

        return total

    @property
    def input_tokens(self) -> int | None:
        return self.total.input_tokens

    @property
    def output_tokens(self) -> int | None:
        return self.total.output_tokens

    @property
    def tokens(self) -> int | None:
        return self.total.tokens

    @property
    def latency_ms(self) -> int:
        """Wall-clock time inside the scope so far, or until it exited.

        Not the calls' durations summed: calls run together, or a hedge racing its primary,
        overlap, and their sum would exceed the time anyone waited.
        """

        end = monotonic() if self._finished is None else self._finished

        return round((end - self._started) * 1000)

    # ....................... #

    def _record(self, model: str, usage: InferenceUsage) -> None:
        if self._finished is not None:
            return

        known = self._usage.get(model)
        self._usage[model] = usage if known is None else known + usage

    def _finish(self) -> None:
        self._finished = monotonic()


# ....................... #

_CURRENT_METER: Final[ContextVar[InferenceMeter | None]] = ContextVar(
    "forze_inference_meter",
    default=None,
)
"""The innermost open meter scope of this task, or ``None`` outside any."""


# ....................... #


@asynccontextmanager
async def inference_meter_scope() -> AsyncGenerator[InferenceMeter]:
    """Open a meter scope; :meth:`InferenceDeps.metered` is the way a handler reaches it."""

    meter = InferenceMeter(parent=_CURRENT_METER.get())
    token = _CURRENT_METER.set(meter)

    try:
        yield meter

    finally:
        # First: a reset from another context raises, and the meter must close regardless.
        meter._finish()  # pyright: ignore[reportPrivateUsage]
        _CURRENT_METER.reset(token)


# ....................... #


def record_inference_usage(
    model: str,
    *,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
) -> None:
    """Report one request's usage to every meter scope open around the caller.

    For an adapter, once per request it sent, where it records the same numbers on the
    span. Outside any scope it does nothing.
    """

    usage = InferenceUsage(input_tokens=input_tokens, output_tokens=output_tokens)
    meter = _CURRENT_METER.get()

    while meter is not None:
        meter._record(model, usage)  # pyright: ignore[reportPrivateUsage]
        meter = meter._parent  # pyright: ignore[reportPrivateUsage]
