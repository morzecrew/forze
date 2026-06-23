from __future__ import annotations

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ClientIdentity:
    """The device/session a request arrives from — a dimension distinct from *who*.

    Kept separate from :class:`~forze.application.contracts.authn.value_objects.identity.AuthnIdentity`
    (which stays principal-only) exactly as effective tenant lives on its own
    :class:`~forze.application.contracts.tenancy.value_objects.TenantIdentity`. Both
    fields are optional, so the whole value object degrades gracefully when nothing
    is known about the client instance.

    Its primary use is a **stable per-device key** for realtime delivery cursors
    (offline store-and-forward): a client may supply a long-lived ``device_id``, or
    the framework falls back to the authenticated ``session_id`` (the ``sid`` the
    identity layer already mints and carries in the verified token). It is reusable
    beyond realtime — per-device rate limits, session audit, "log out other devices".
    """

    device_id: str | None = None
    """Client-supplied, stable across re-logins (e.g. a persisted install id).

    Survives logout/login, so a cursor keyed on it does too. Supplied by the client
    (e.g. a Socket.IO connect-handshake field); ``None`` when the client supplies none."""

    session_id: str | None = None
    """The authenticated session identifier (the token ``sid``), server-authoritative.

    Already minted by the identity layer, so it costs the client nothing — but it
    resets when the session does (logout / refresh rotation)."""

    # ....................... #

    @property
    def key(self) -> str | None:
        """The stable client key: the ``device_id`` if present, else the ``session_id``.

        ``None`` when neither is known (the caller then falls back to a per-connection
        identifier). Always namespace it under the principal before use as a key, so a
        spoofed value can only ever address that principal's own state.
        """

        return self.device_id or self.session_id
