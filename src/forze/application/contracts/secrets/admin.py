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

    def put(
        self,
        ref: SecretRef,
        value: str,
        *,
        expected_version: SecretVersion | None = None,
    ) -> Awaitable[SecretVersion]:
        """Write *value* as the new current version at *ref*.

        With *expected_version* set, the write is **compare-and-set**: it succeeds
        only while *ref*'s current version equals it, and raises a
        ``CONCURRENCY``-kind error (code ``secret_version_conflict``) otherwise —
        never overwriting a newer value. This is the rotator's promote fence: a
        distributed lock is advisory (its loss surfaces late), so the conditional
        write is what actually prevents a stale owner from clobbering a newer
        rotation. Write-capable backends must honor it (Vault KV v2 maps it to
        native ``cas``; in-memory stores compare under their own lock).

        :param ref: Secret reference.
        :param value: The new secret text.
        :param expected_version: Fence — the version the caller last observed;
            ``None`` writes unconditionally.
        :returns: The version the write produced.
        """

        ...  # pragma: no cover
