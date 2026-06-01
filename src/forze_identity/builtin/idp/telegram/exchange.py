"""Telegram Login authorization-code exchange (server-side only)."""

from typing import cast, final

import attrs
import httpx

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

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
    timeout: float = 10.0,
) -> TelegramTokenResponse:
    """Exchange an authorization code for tokens (PKCE + Basic client auth)."""

    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri or config.redirect_uri,
        "client_id": config.client_id,
        "code_verifier": code_verifier,
    }

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                config.token_endpoint,
                data=data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Authorization": f"Basic {config.basic_auth_header()}",
                },
            )

    except httpx.HTTPError as e:
        raise exc.infrastructure(
            "Telegram token exchange request failed",
            code="telegram_token_exchange_failed",
        ) from e

    if response.status_code >= 400:
        raise exc.authentication(
            "Telegram token exchange rejected the authorization code",
            code="telegram_token_exchange_failed",
        )

    try:
        payload = response.json()

    except ValueError as e:
        raise exc.infrastructure(
            "Telegram token response is not valid JSON",
            code="telegram_token_exchange_failed",
        ) from e

    if not isinstance(payload, dict):
        raise exc.infrastructure(
            "Telegram token response must be a JSON object",
            code="telegram_token_exchange_failed",
        )

    payload = cast(JsonDict, payload)

    return _parse_token_response(payload)
