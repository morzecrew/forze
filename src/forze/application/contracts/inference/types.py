"""Per-call options accepted by the inference port."""

from datetime import timedelta
from typing import TypedDict

# ----------------------- #


class InferenceRunOptions(TypedDict, total=False):
    """Optional per-call knobs for ``predict`` / ``predict_many`` / ``predict_stream``.

    Deliberately free of model-targeting fields: which model (and which version) a route
    invokes is a wiring fact declared in the adapter's config, never a per-call choice —
    the same governance stance the procedure port takes for its registered SQL.
    """

    timeout: timedelta
    """Per-call budget, tighten-only: the effective deadline is the earlier of this and the
    ambient invocation deadline — a per-call timeout can shorten the budget, never extend it."""

    max_batch_size: int
    """Advisory chunk cap for adapters that sub-batch ``predict_stream`` wire calls. Ignored
    by ``predict_many`` (all-or-nothing: an oversized batch is refused, never split)."""
