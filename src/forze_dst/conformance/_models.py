"""Tiny aggregates the isolation-conformance battery drives anomalies against.

Two minimal documents carry nothing but what the anomalies need: a ``Cell`` (one integer
``value``) for the read anomalies and lost update, and an ``OnCall`` roster (a boolean) for
write skew, whose invariant is "at least one person on call". The battery is the point, not
the domain — these stay deliberately featureless so an app author can run the suite without
wiring any of their own models.
"""

from __future__ import annotations

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

# ----------------------- #
# Cell — a single integer value (read anomalies, lost update).


class Cell(Document):
    value: int = 0


class CellCreate(CreateDocumentCmd):
    value: int = 0


class CellRead(ReadDocument):
    value: int


class CellUpdate(BaseDTO):
    value: int | None = None


CELL = DocumentSpec(
    name="conformance_cell",
    read=CellRead,
    write=DocumentWriteTypes(domain=Cell, create_cmd=CellCreate, update_cmd=CellUpdate),
)


# ....................... #
# OnCall — an on-call roster; the cross-item invariant is "at least one on call" (write skew).


class OnCall(Document):
    on_call: bool = True


class OnCallCreate(CreateDocumentCmd):
    on_call: bool = True


class OnCallRead(ReadDocument):
    on_call: bool


class OnCallUpdate(BaseDTO):
    on_call: bool | None = None


ONCALL = DocumentSpec(
    name="conformance_oncall",
    read=OnCallRead,
    write=DocumentWriteTypes(domain=OnCall, create_cmd=OnCallCreate, update_cmd=OnCallUpdate),
)
