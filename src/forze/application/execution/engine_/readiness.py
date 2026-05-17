from typing import Sequence

import attrs

from forze.base.primitives import StrKey

# ----------------------- #


@attrs.define(slots=True)
class CapabilityReadiness:
    """Tracks which capability keys are satisfied for one chain invocation."""

    _ready: set[StrKey] = attrs.field(factory=set, repr=False)
    _skipped: set[StrKey] = attrs.field(factory=set, repr=False)

    # ....................... #

    def is_ready(self, keys: set[StrKey] | Sequence[StrKey]) -> bool:
        if not keys:
            return True

        return all(key in self._ready and key not in self._skipped for key in keys)

    # ....................... #

    def mark_success(self, keys: set[StrKey] | Sequence[StrKey]) -> None:
        for key in keys:
            self._skipped.discard(key)
            self._ready.add(key)

    # ....................... #

    def mark_skipped(self, keys: set[StrKey] | Sequence[StrKey]) -> None:
        for key in keys:
            self._ready.discard(key)
            self._skipped.add(key)
