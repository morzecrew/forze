"""Per-backend secrets capabilities + fail-closed validators.

The secrets plane presents one surface, but backends diverge on which *lifecycle*
features they can serve: a process-env store has no versions to watch; a
kubelet-managed file mount must never be written through the app (writes go through
the platform); only stores with a lease engine can issue dynamic credentials.

:class:`SecretsCapabilities` makes that surface **declarative**, mirroring
:class:`~forze.application.contracts.search.capabilities.SearchCapabilities`: each
adapter publishes what it can serve via a ``secrets_capabilities`` property, and a
request that strays is rejected up front with a clean
:func:`~forze.base.exceptions.exc.precondition` (code ``secrets_feature_unsupported``)
naming the feature and backend — never a silent no-op. The in-memory mock is the
canonical superset (:data:`FULL_SECRETS_CAPABILITIES`).

Note ``change_feed=False`` does **not** mean "no watching": the poll watcher works
over ``versioned_reads`` alone; the flag advertises a *backend-native* change source.
"""

from typing import Final, final

import attrs

from forze.base.exceptions import exc

# ----------------------- #

UNSUPPORTED_SECRETS_FEATURE_CODE: Final[str] = "secrets_feature_unsupported"
"""Error code for a secrets lifecycle feature requested of a backend that lacks it."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SecretsCapabilities:
    """What a secrets backend can serve, declared per adapter.

    Defaults describe today's plain resolve-only adapter: no versions, no writes, no
    native change source, no leases. Richer adapters widen the surface; consumers
    fail closed through the ``validate_*`` helpers before first use instead of
    degrading silently.
    """

    versioned_reads: bool = False
    """``resolve_versioned`` / ``current_version`` honored."""

    native_versions: bool = False
    """Versions are store-assigned tokens (Vault KV v2), not content-derived hashes."""

    writes: bool = False
    """:class:`~forze.application.contracts.secrets.SecretsAdminPort` honored."""

    change_feed: bool = False
    """A backend-native change source exists (file mount, native watch). The poll
    watcher needs only ``versioned_reads``, so ``False`` still supports watching."""

    dynamic_credentials: bool = False
    """:class:`~forze.application.contracts.secrets.DynamicSecretsPort` honored."""

    def __attrs_post_init__(self) -> None:
        # A store-assigned version token is meaningless if versioned reads are refused.
        if self.native_versions and not self.versioned_reads:
            raise exc.configuration(
                "SecretsCapabilities.native_versions=True requires versioned_reads=True.",
            )


# ....................... #

FULL_SECRETS_CAPABILITIES: Final[SecretsCapabilities] = SecretsCapabilities(
    versioned_reads=True,
    native_versions=True,
    writes=True,
    change_feed=True,
    dynamic_credentials=True,
)
"""The canonical full lifecycle surface — the in-memory mock serves all of it."""

DEFAULT_SECRETS_CAPABILITIES: Final[SecretsCapabilities] = SecretsCapabilities()
"""The plain resolve-only surface (all off) — what an adapter serves unless it
declares more."""


# ....................... #


def secrets_capabilities_of(backend: object) -> SecretsCapabilities:
    """Read a backend's declared capabilities, defaulting to the resolve-only surface.

    Adapters declare via a ``secrets_capabilities`` property; anything without one is
    treated as today's plain :class:`~forze.application.contracts.secrets.SecretsPort`.
    """

    caps = getattr(backend, "secrets_capabilities", None)

    if isinstance(caps, SecretsCapabilities):
        return caps

    return DEFAULT_SECRETS_CAPABILITIES


# ....................... #


def _secrets_cap_fail(backend: str, feature: str) -> None:
    raise exc.precondition(
        f"Secrets feature {feature} is not supported by the {backend!r} backend.",
        code=UNSUPPORTED_SECRETS_FEATURE_CODE,
    )


def validate_versioned_reads_supported(caps: SecretsCapabilities, *, backend: str) -> None:
    """Raise cleanly if versioned reads are asked of a *backend* that cannot serve them.

    Call it when wiring a poll watcher, before the first tick — a watcher over an
    unversioned backend would silently never detect a change.
    """

    if not caps.versioned_reads:
        _secrets_cap_fail(backend, "versioned reads")


def validate_secret_writes_supported(caps: SecretsCapabilities, *, backend: str) -> None:
    """Raise cleanly if a control-plane write is asked of a *backend* that refuses it.

    Directory and env stores refuse by design: kubelet-managed files and process env
    are rotated through their platform, not through the app.
    """

    if not caps.writes:
        _secrets_cap_fail(backend, "control-plane writes")


def validate_dynamic_credentials_supported(caps: SecretsCapabilities, *, backend: str) -> None:
    """Raise cleanly if lease issuance is asked of a *backend* without a lease engine."""

    if not caps.dynamic_credentials:
        _secrets_cap_fail(backend, "dynamic credentials")
