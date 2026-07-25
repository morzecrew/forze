"""Rotation target — the backend-specific steps of a credential rotation.

A contract (not a kit-local protocol) so integration packages can implement it
without importing ``forze_kits``. The rotator workflow stays generic: it mints,
stages, and promotes through the secrets store; the target owns everything that is
backend-shaped (composing a DSN around an idle role, ``ALTER ROLE``, proving a real
connection).
"""

from collections.abc import Awaitable
from typing import Protocol, final
from uuid import UUID

import attrs

from .value_objects import SecretRef
from .versioning import SecretVersion

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PendingCredential:
    """A staged-but-not-promoted credential, identified by ref and version only.

    JSON-trivial by construction — this is what durable rotation steps journal.
    Steps re-read the staged value by ref through the secrets store; secret text
    never rides a step result.
    """

    ref: SecretRef
    """Where the pending value is staged (the ``<path>.pending`` convention)."""

    version: SecretVersion
    """The staged version at that ref."""


# ....................... #


class RotationTargetPort(Protocol):
    """The backend-specific compose/set/test steps of a rotation.

    Implemented by integration packages (``forze_postgres`` first); consumed only by
    the rotator kit. Implementations resolve the staged value through their injected
    :class:`~forze.application.contracts.secrets.SecretsPort` at call time.
    """

    def compose(
        self,
        tenant_id: UUID | None,
        *,
        current: str,
        minted: str,
    ) -> Awaitable[str]:
        """Compose the pending secret value from a freshly minted credential.

        Backend-shaped by nature: a Postgres dual-user target reads the active role
        out of *current* and returns a DSN naming the idle role with *minted* as its
        password; an API-key target may return *minted* unchanged.

        :param tenant_id: Tenant under rotation, or ``None`` for a global secret.
        :param current: The current (pre-rotation) secret text.
        :param minted: Freshly minted CSPRNG credential material.
        :returns: The full pending secret text to stage. Never log or journal it.
        """

        ...  # pragma: no cover

    def apply(self, tenant_id: UUID | None, pending: PendingCredential) -> Awaitable[None]:
        """Make the pending credential valid at the backend (``ALTER ROLE`` /
        alternate user / API-key mint). Idempotent — a durable-run retry re-applies
        safely."""

        ...  # pragma: no cover

    def verify(self, tenant_id: UUID | None, pending: PendingCredential) -> Awaitable[None]:
        """Prove the pending credential works — a real connection or authenticated
        call, NOT a syntactic check. Raises on failure; the rotation halts before
        promote."""

        ...  # pragma: no cover
