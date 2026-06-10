"""VK ID authorization-code exchange (server-side only)."""

from typing import final

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze_identity.oidc import verify_id_token_nonce

from .._exchange import oidc_code_exchange
from .config import VkIdOidcConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class VkTokenResponse:
    """Token endpoint response — only ``id_token`` belongs in :class:`OidcTokenVerifier`."""

    id_token: str
    """JWT to pass to the bootstrap ``TokenVerifierPort``."""

    access_token: str | None = attrs.field(default=None)
    """Opaque VK API token — keep server-side; do not use as Forze bearer."""

    refresh_token: str | None = attrs.field(default=None)
    """Opaque refresh token — token endpoint only; never on API boundary."""

    expires_in: int | None = attrs.field(default=None)
    """Access token lifetime in seconds when provided."""

    token_type: str | None = attrs.field(default=None)
    """Token type label from VK (typically ``Bearer``)."""


# ....................... #


def _parse_token_response(payload: JsonDict) -> VkTokenResponse:
    id_token = payload.get("id_token")
    if not isinstance(id_token, str) or not id_token:
        raise exc.authentication(
            "VK token response missing id_token",
            code="vk_token_exchange_failed",
        )

    access = payload.get("access_token")
    refresh = payload.get("refresh_token")
    expires_raw = payload.get("expires_in")
    token_type = payload.get("token_type")

    expires_in: int | None = None
    if isinstance(expires_raw, int):
        expires_in = expires_raw
    elif isinstance(expires_raw, str) and expires_raw.isdigit():
        expires_in = int(expires_raw)

    return VkTokenResponse(
        id_token=id_token,
        access_token=access if isinstance(access, str) else None,
        refresh_token=refresh if isinstance(refresh, str) else None,
        expires_in=expires_in,
        token_type=token_type if isinstance(token_type, str) else None,
    )


# ....................... #


async def exchange_authorization_code(
    config: VkIdOidcConfig,
    *,
    code: str,
    code_verifier: str,
    redirect_uri: str | None = None,
    device_id: str | None = None,
    expected_nonce: str | None = None,
    timeout: float = 10.0,
) -> VkTokenResponse:
    """Exchange an authorization code for tokens (PKCE).

    Validates only the returned ``id_token`` with :class:`ConfigurableVkIdOidcVerifier`.
    Store ``access_token`` / ``refresh_token`` server-side if needed for VK API calls.

    When ``expected_nonce`` is set (the session nonce you sent on the authorize URL),
    the returned ``id_token`` is bound to it via
    :func:`forze_identity.oidc.verify_id_token_nonce` — a missing or mismatched
    ``nonce`` claim raises an authentication error. ``None`` skips the binding.
    """

    data: dict[str, str] = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": config.client_id,
        "redirect_uri": redirect_uri or config.redirect_uri,
        "code_verifier": code_verifier,
    }

    secret = config.client_secret_value()
    if secret is not None:
        data["client_secret"] = secret

    if device_id is not None:
        data["device_id"] = device_id

    payload = await oidc_code_exchange(
        token_endpoint=config.token_endpoint,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        provider="VK",
        error_code="vk_token_exchange_failed",
        timeout=timeout,
    )

    response = _parse_token_response(payload)

    if expected_nonce is not None:
        verify_id_token_nonce(response.id_token, expected_nonce)

    return response
