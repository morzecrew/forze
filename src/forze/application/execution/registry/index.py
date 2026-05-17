from typing import Any

import attrs

from forze.base.primitives import StrKey

# ----------------------- #


@attrs.define(slots=True)
class UsecaseIndex:  #! bad naming tbh
    _by_op: dict[StrKey, Any] = attrs.field(factory=dict)

    # ....................... #
