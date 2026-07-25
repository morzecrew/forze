"""Versioned secret reads — the change-detection primitive of the secrets lifecycle plane.

A :class:`SecretVersion` is an opaque equality-only token: at one ref, the same token
implies the same value, and nothing more. Stores with native versioning (Vault KV v2)
surface their own tokens; stores without one (files, env, mappings) derive a
content-hash pseudo-version via :func:`content_secret_version` — which is what makes
poll-based change detection work over *every* backend, not just Vault.
"""

import hashlib
from collections.abc import Awaitable
from typing import Protocol, final

import attrs

from .value_objects import SecretRef

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True)
class SecretVersion:
    """Opaque change-detection token for one secret ref."""

    token: str
    """Opaque token. Contract: at one ref, same token ⇒ same value.

    NO ordering, NO arithmetic — Vault yields integers, files yield content hashes,
    cloud managers yield staging labels or version ids. Consumers only ever compare
    for equality.
    """


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SecretValue:
    """A secret payload together with the version it was read at."""

    text: str = attrs.field(repr=False)
    """Decoded secret text. Excluded from ``repr`` so the value never leaks into logs."""

    version: SecretVersion
    """The version this text was read at."""


# ....................... #


class VersionedSecretsPort(Protocol):
    """Port for reading secrets together with their change-detection version."""

    def resolve_versioned(self, ref: SecretRef) -> Awaitable[SecretValue]:
        """Return the secret value and its current version in one read.

        Hash-based backends read once and hash the payload, so text and version can
        never be torn against each other.

        :param ref: Secret reference.
        :returns: The current value with its version.
        :raises SecretNotFoundError: When the secret cannot be found.
        """

        ...  # pragma: no cover

    def current_version(self, ref: SecretRef) -> Awaitable[SecretVersion]:
        """Return the current version without the payload where the backend allows it.

        Vault serves this from KV metadata; hash-based backends necessarily read the
        value to hash it. This is the poll watcher's hot path — it never fetches
        values it does not need.

        :param ref: Secret reference.
        :returns: The current version token.
        :raises SecretNotFoundError: When the secret cannot be found.
        """

        ...  # pragma: no cover


# ....................... #


def content_secret_version(text: str) -> SecretVersion:
    """Derive a content-hash pseudo-version for stores with no native version concept.

    Deterministic across processes: SHA-256 of the UTF-8 payload. Backends using this
    declare ``native_versions=False`` in their
    :class:`~forze.application.contracts.secrets.SecretsCapabilities`.
    """

    return SecretVersion(hashlib.sha256(text.encode("utf-8")).hexdigest())
