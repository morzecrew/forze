"""The record a correction leaves behind: what replaced what, by whom, and why."""

from uuid import UUID

from forze.domain.models import CoreModel, CreateDocumentCmd, Document

# ----------------------- #


class CorrectionMixin(CoreModel):
    """One aggregate's correction lineage, as a queryable record.

    A document rather than an event, because "show me every correction to this fact with its
    reason" is the question a ledger is audited on, and an event is not queryable. It covers one
    aggregate's lineage and nothing else — cross-aggregate who-did-what is a different subject.

    There is no ``at`` field: a correction record is written in the correcting transaction, so
    ``created_at`` already *is* when the correction happened, and a second timestamp would be one
    more thing that can disagree with it.
    """

    root_id: UUID
    """The fact that was corrected."""

    from_id: UUID
    """The version that was current before."""

    to_id: UUID
    """The version that is current now."""

    actor_id: UUID | None = None
    """Who corrected it, read from the invocation's identity.

    ``None`` when the correcting call carried no authenticated identity — a migration or an
    internal job. Recorded as absent rather than refused, because a correction with no actor is
    still a correction and losing the record would be worse than losing the name."""

    reason: str
    """Why, in the author's own vocabulary."""


# ....................... #


class CorrectionDoc(Document, CorrectionMixin): ...


class CreateCorrectionCmd(CreateDocumentCmd, CorrectionMixin): ...
