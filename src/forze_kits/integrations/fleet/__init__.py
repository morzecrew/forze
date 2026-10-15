"""Fleet membership: keep this replica's entry current from startup to shutdown."""

from .lifecycle import fleet_membership_lifecycle_step

# ----------------------- #

__all__ = ["fleet_membership_lifecycle_step"]
