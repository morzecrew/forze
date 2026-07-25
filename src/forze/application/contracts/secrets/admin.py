"""Control-plane write surface for secret rotators."""

from collections.abc import Awaitable
from typing import Protocol

from .value_objects import SecretRef
from .versioning import SecretVersion

# ----------------------- #


class SecretsAdminPort(Protocol):
    """Rotator-facing write surface.

    Deliberately minimal: no delete, no metadata editing, no policy management —
    those stay in the store's own tooling. Backends that cannot honor writes
    (directory, env — platform-managed stores) refuse via
    :class:`~forze.application.contracts.secrets.SecretsCapabilities` instead of
    fighting their platform.
    """

    def put(self, ref: SecretRef, value: str) -> Awaitable[SecretVersion]:
        """Write *value* as the new current version at *ref*.

        :param ref: Secret reference.
        :param value: The new secret text.
        :returns: The version the write produced.
        """

        ...  # pragma: no cover
