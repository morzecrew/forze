"""The lineage fields a corrected fact carries, and the writes they refuse."""

from datetime import datetime
from typing import Self
from uuid import UUID

from pydantic import Field

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.domain.models import CoreModel
from forze.domain.validation import update_validator

from .constants import (
    ALLOWED_ORDINARY_DIFF_KEYS,
    ALLOWED_SUPERSEDE_DIFF_KEYS,
    IS_CURRENT_FIELD,
)

# ----------------------- #


class VersionedMixin(CoreModel):
    """Correction lineage: which fact this row asserts, which version of it, and what it replaced.

    ``id`` is this version; :attr:`root_id` is the fact. A first insert sets them equal, so a
    reference to the fact and a reference to its first assertion are distinguishable from the
    outset rather than after the first correction.

    A superseded row stays readable and stays addressable — that is the whole point of correcting
    rather than overwriting — so the only update it accepts is the one that retired it. A current
    row is no more editable: what a version asserts is replaced by a successor, never rewritten.
    """

    root_id: UUID = Field(frozen=True)
    """The fact this row asserts something about, stable across every correction of it."""

    version: int = Field(frozen=True, ge=1)
    """Which assertion this is, counting from 1."""

    supersedes_id: UUID | None = Field(default=None, frozen=True)
    """The version this one replaced; ``None`` on the first."""

    is_current: bool = True
    """Whether this is the fact's current assertion (a stored flag, never a derived anti-join)."""

    superseded_at: datetime | None = None
    """When this row stopped being current; ``None`` while it is."""

    # ....................... #

    @update_validator
    def _validate_versioning(before: Self, _: Self, diff: JsonDict) -> None:
        """Refuse any write that edits what a version asserts, on a current row or an old one.

        A version is an assertion someone made about the fact, and the aggregate's whole offer is
        that such an assertion is replaced by a successor rather than rewritten: an update that
        changes one in place leaves no earlier value, no successor and nothing recording that
        anyone changed it. Two writes are not assertions and pass — the one that retires a
        predecessor, and soft deletion, which hides a row without contradicting it.

        The guard sits on the model rather than on the generated operation because a repair
        script, a bulk update or a hand-written handler reaches the row without passing one.
        """

        keys = set(diff.keys())
        superseding = IS_CURRENT_FIELD in keys and keys <= ALLOWED_SUPERSEDE_DIFF_KEYS

        if not before.is_current and not superseding:
            raise exc.domain(
                "Cannot update a superseded version of a fact — correct the current version "
                "instead, which records the change and leaves this one readable.",
            )

        if not keys <= ALLOWED_ORDINARY_DIFF_KEYS:
            raise exc.domain(
                "Cannot overwrite what a version of a fact asserts — correct it instead, which "
                "writes the new value as the next version and leaves this one readable.",
            )


# ....................... #


class SupersedeCmdMixin(CoreModel):
    """The update-command half: the two fields retiring a version is allowed to set.

    Deliberately not :class:`VersionedMixin`. A patch carrying ``root_id`` and ``version`` would
    let a caller re-point a row at another fact or renumber it, and both are frozen on the domain
    model precisely because nothing may. What a correction writes to the predecessor is only that
    it is no longer current, and when it stopped being so.
    """

    is_current: bool | None = None
    """Set to ``False`` when retiring this version; ``None`` leaves it alone."""

    superseded_at: datetime | None = None
    """When the version was retired; ``None`` leaves it alone."""


# ....................... #


class CreateCmdWithVersioning(CoreModel):
    """The create-command half: lineage fields the kit fills, never the caller.

    Present with defaults so the create command can carry them to the domain model, and
    overwritten on every insert by the kit's own handler — a caller that set :attr:`root_id`
    would be declaring this row to be a version of some other fact, which is what ``correct``
    is for and what a create must never do.
    """

    root_id: UUID | None = None
    """Filled with the new row's own id on a first insert, or the fact's id on a correction."""

    version: int = 1
    """Filled by the kit; 1 on a first insert."""

    supersedes_id: UUID | None = None
    """Filled by the kit; ``None`` on a first insert."""

    is_current: bool = True
    """Filled by the kit; a newly written version is always the current one."""

    superseded_at: datetime | None = None
    """Filled by the kit; ``None`` while the version is current."""
