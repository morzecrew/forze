"""Connection settings for one Firestore client.

The thinnest of the family, and it stays that way because the client's own surface is:
:func:`~forze_firestore.firestore_lifecycle_step` takes a project and a database name, and
credentials come from Application Default Credentials. There is no config object to build.
"""

from pydantic import BaseModel

from forze.base.settings import require

# ----------------------- #


class FirestoreSettings(BaseModel):
    """Project and database for one Firestore client."""

    project_id: str | None = None
    """Required when read — see :meth:`require_project_id`."""

    database: str = "(default)"
    """Firestore's own name for the unnamed database. A project with several databases
    names the one this process talks to; the parentheses are part of the value."""

    # ....................... #

    def require_project_id(self) -> str:
        """The project id, refused by name when unset.

        :raises CoreException: ``configuration`` when :attr:`project_id` is unset or blank.
        """

        return require(self.project_id, service="Firestore", setting="project_id")


# ....................... #

__all__ = ["FirestoreSettings"]
