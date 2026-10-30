from typing import ClassVar

from pydantic import PositiveInt

from forze.application.contracts.guarantees import StorageGuarantees, UniqueTogether
from forze.domain.models import BaseDTO, CoreModel

from .constants import NUMBER_ID_FIELD

# ----------------------- #


class NumberIdMixin(CoreModel):
    """Mixin adding a required positive integer number ID for human-readable identification.

    Use :class:`NumberCreateCmdMixin` or :class:`NumberUpdateCmdMixin` for
    command DTOs.
    """

    number_id: PositiveInt
    """Unique number identifier of the document."""

    implied_guarantees: ClassVar[StorageGuarantees] = (UniqueTogether(fields=(NUMBER_ID_FIELD,)),)
    """At most one row per number: a document spec over this model declares it by itself.

    The counter hands each number out once: per tenant when its own route is tenant-aware, as
    the stores keep a tenant-aware document route's guarantee. Give the two routes the same
    tenancy; a per-tenant counter feeding a shared relation repeats numbers, which this refuses.
    A uniqueness the spec declares over ``number_id`` replaces this one. Override with ``()``
    where the store keeps no uniqueness, such as Firestore."""


# ....................... #


class NumberIdCreateCmdMixin(BaseDTO):
    """Create command mixin with required number ID."""

    number_id: PositiveInt
    """Unique number identifier of the document."""


# ....................... #


class NumberIdUpdateCmdMixin(BaseDTO):
    """Update command mixin with optional number ID.

    When provided, updates the document's numeric identifier.
    """

    number_id: PositiveInt | None = None
    """Unique number identifier of the document."""
