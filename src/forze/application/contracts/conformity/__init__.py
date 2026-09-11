"""Shared model↔storage conformity validation for specifications.

Rules common to every spec that exposes a storage-conformity knob (``DocumentSpec``,
``SearchSpec``), kept in one place so they cannot drift:

- *lenient read fields* — a read-model field with no backing column, dropped from the
  read projection and hydrated from its model default on read.
- *materialized fields* — a ``@computed_field`` persisted as a real column so a derived
  value can be filtered and sorted at the database.
- *derived read fields* — a read-model field the backend produces from another relation
  (a view's joined column), which no write of this application's produces.
"""

from .derived_read import DerivedReadField, validate_derived_read_fields
from .lenient_read import (
    IDENTITY_READ_FIELDS,
    ReadConformity,
    derive_lenient_read_fields,
    validate_lenient_read_fields,
)
from .materialized import validate_materialized_computed

# ----------------------- #

__all__ = [
    "IDENTITY_READ_FIELDS",
    "DerivedReadField",
    "ReadConformity",
    "derive_lenient_read_fields",
    "validate_lenient_read_fields",
    "validate_derived_read_fields",
    "validate_materialized_computed",
]
