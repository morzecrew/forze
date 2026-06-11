"""Telegram Login authorization-code exchange (server-side only)."""

from typing import final

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze_identity.oidc import verify_id_token_nonce

from .._exchange import oidc_code_exchange
from .config import TelegramLoginOidcConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TelegramTokenResponse:
    """Token endpoint response — verify ``id_token`` only via OIDC verifier."""

    id_token: str
    """JWT for bootstrap authentication."""

    access_token: str | None = attrs.field(default=None)
    """Opaque Telegram token — not for :class:`OidcTokenVerifier`."""

    token_type: str | None = attrs.field(default=None)
    """Token type label from Telegram (typically ``Bearer``)."""

    expires_in: int | None = attrs.field(default=None)
    """Access token lifetime in seconds when provided."""


# ....................... #


def _parse_token_response(payload: JsonDict) -> TelegramTokenResponse:
    id_token = payload.get("id_token")
    if not isinstance(id_token, str) or not id_token:
        raise exc.authentication(
            "Telegram token response missing id_token",
            code="telegram_token_exchange_failed",
        )

    access = payload.get("access_token")
    token_type = payload.get("token_type")
    expires_raw = payload.get("expires_in")

    expires_in: int | None = None

    if isinstance(expires_raw, int):
        expires_in = expires_raw

    elif isinstance(expires_raw, str) and expires_raw.isdigit():
        expires_in = int(expires_raw)

    return TelegramTokenResponse(
        id_token=id_token,
        access_token=access if isinstance(access, str) else None,
        token_type=token_type if isinstance(token_type, str) else None,
        expires_in=expires_in,
    )


# ....................... #


async def exchange_authorization_code(
    config: TelegramLoginOidcConfig,
    *,
    code: str,
    code_verifier: str,
    redirect_uri: str | None = None,
    expected_nonce: str | None = None,
    timeout: float = 10.0,
) -> TelegramTokenResponse:
    """Exchange an authorization code for tokens (PKCE + Basic client auth).

    When ``expected_nonce`` is set (the session nonce you sent on the authorize URL),
    the returned ``id_token`` is bound to it via
    :func:`forze_identity.oidc.verify_id_token_nonce` — a missing or mismatched
    ``nonce`` claim raises an authentication error. ``None`` skips the binding.
    """

    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri or config.redirect_uri,
        "client_id": config.client_id,
        "code_verifier": code_verifier,
    }

    payload = await oidc_code_exchange(
        token_endpoint=config.token_endpoint,
        data=data,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {config.basic_auth_header()}",
        },
        provider="Telegram",
        error_code="telegram_token_exchange_failed",
        timeout=timeout,
    )

    response = _parse_token_response(payload)

    if expected_nonce is not None:
        verify_id_token_nonce(response.id_token, expected_nonce)

    return response
