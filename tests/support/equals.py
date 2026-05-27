"""Common dirty-equals matchers for integration assertions."""

from __future__ import annotations

from typing import Any

from dirty_equals import (
    IsDatetime as _IsDatetime,
)
from dirty_equals import (
    IsList as _IsList,
)
from dirty_equals import (
    IsPartialDict as _IsPartialDict,
)
from dirty_equals import (
    IsStr as _IsStr,
)
from dirty_equals import (
    IsUUID as _IsUUID,
)

IsPartialDict = _IsPartialDict
IsUUID = _IsUUID
IsDatetime = _IsDatetime
IsList = _IsList
IsStr = _IsStr


def document_partial(**fields: Any) -> _IsPartialDict:
    """Partial dict matcher for document rows (id/rev/timestamps often ignored)."""

    return _IsPartialDict(fields)
