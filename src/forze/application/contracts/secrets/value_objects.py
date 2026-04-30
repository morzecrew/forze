"""Secrets contract value objects."""

from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True)
class SecretRef:
    """Logical reference to a secret value in a backend (Vault path, env name, etc.).

    Backends interpret :attr:`path` according to their own rules.
    """

    path: str
    """Opaque logical path or identifier."""
