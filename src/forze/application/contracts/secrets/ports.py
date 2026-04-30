"""Ports for async secret resolution."""

from typing import Awaitable, Protocol

from .value_objects import SecretRef

# ----------------------- #


class SecretsPort(Protocol):
    """Port for resolving secrets as UTF-8 strings."""

    def resolve_str(self, ref: SecretRef) -> Awaitable[str]:
        """Return the secret value as a string.

        :param ref: Secret reference.
        :returns: Decoded secret text (e.g. DSN, JSON blob, token).
        :raises SecretNotFoundError: When the secret cannot be found.
        """

        ...  # pragma: no cover

    def exists(self, ref: SecretRef) -> Awaitable[bool]:
        """Return ``True`` if the secret is present without fetching the payload when possible."""

        ...  # pragma: no cover
