"""Telegram Login Widget verification via the documented HMAC-SHA256 scheme.

The `Login Widget <https://core.telegram.org/widgets/login>`_ is Telegram's
browser sign-in button. Unlike the Telegram *Login OIDC* flow (code + PKCE +
``id_token`` verified through JWKS, see :mod:`~forze_identity.builtin.idp.telegram`),
the widget hands the browser an authenticated **data object** — ``id``,
``first_name``, ``auth_date``, ``hash``, … — and trust rests entirely on the
``hash``: an HMAC-SHA256 over the sorted *data-check-string*, keyed by the SHA-256
of the bot token. This module verifies that hash (constant-time) and the
``auth_date`` freshness, then emits the canonical :class:`VerifiedAssertion`.

Pure standard-library crypto (``hmac`` / ``hashlib``) — no JWT is involved despite
the module living beside the OIDC preset.
"""

import hashlib
import hmac
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, Final, final

import attrs
from pydantic import SecretStr

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    TokenVerifierPort,
    VerifiedAssertion,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, utcnow

# ----------------------- #

TELEGRAM_LOGIN_WIDGET_ISSUER: Final[str] = "https://telegram.org"
"""Issuer recorded on widget assertions (the principal-resolver discriminator)."""

_HASH_FIELD: Final[str] = "hash"
_AUTH_DATE_FIELD: Final[str] = "auth_date"
_ID_FIELD: Final[str] = "id"


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TelegramWidgetVerifier(TokenVerifierPort):
    """Verify Telegram Login Widget callback data (HMAC-SHA256, keyed by the bot token).

    The widget returns a field map whose ``hash`` authenticates the rest. Verification
    (per Telegram's spec): build the *data-check-string* — every received field except
    ``hash`` as ``key=value``, sorted by key, joined by ``\\n`` — then compare, in
    constant time, ``HMAC_SHA256(SHA256(bot_token), data_check_string)`` against the
    presented ``hash``. A stale ``auth_date`` (older than :attr:`max_age`) is rejected to
    bound replay.

    Two entry points:

    - :meth:`verify` — the direct API for a callback handler that already parsed the
      widget fields into a mapping.
    - :meth:`verify_token` — the :class:`TokenVerifierPort` adapter; it parses
      ``credentials.token`` as a URL-encoded query string (exactly what Telegram appends
      to the widget redirect) so the widget can plug into the standard token route.

    Pair the emitted assertion with a principal resolver just like any other IdP verifier;
    :attr:`issuer` is the discriminator and the Telegram user id is the subject.
    """

    bot_token: str | SecretStr = attrs.field(repr=False)
    """The bot token from @BotFather (``123456:ABC-DEF...``). Never sent anywhere — only
    its SHA-256 keys the local HMAC."""

    max_age: timedelta | None = attrs.field(default=timedelta(days=1))
    """Maximum accepted age of ``auth_date`` before the payload is treated as stale
    (replay guard). ``None`` disables the freshness check (not recommended)."""

    issuer: str = attrs.field(default=TELEGRAM_LOGIN_WIDGET_ISSUER)
    """Issuer recorded on the emitted assertion (principal-resolver discriminator)."""

    bot_username: str | None = attrs.field(default=None)
    """Optional bot @username, recorded as the assertion audience when set."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self._bot_token_value().strip():
            raise exc.configuration("Telegram bot_token must be non-empty")

        if self.max_age is not None and self.max_age.total_seconds() <= 0:
            raise exc.configuration("max_age must be positive")

    # ....................... #

    def _bot_token_value(self) -> str:
        if isinstance(self.bot_token, SecretStr):
            return self.bot_token.get_secret_value()

        return self.bot_token

    # ....................... #

    def verify(self, data: Mapping[str, Any]) -> VerifiedAssertion:
        """Verify a widget field mapping and emit a :class:`VerifiedAssertion`.

        :raises CoreException: ``authentication`` when the ``hash`` is missing or does
            not match, when ``auth_date`` is missing/unparseable/stale, or when the
            widget carried no user ``id``.
        """

        fields = {key: str(value) for key, value in data.items()}

        presented_hash = fields.pop(_HASH_FIELD, None)

        if not presented_hash:
            raise exc.authentication(
                "Telegram widget payload has no hash",
                code="telegram_widget_invalid",
            )

        if not self._hash_matches(fields, presented_hash):
            raise exc.authentication(
                "Telegram widget hash does not match",
                code="telegram_widget_invalid",
            )

        self._require_fresh_auth_date(fields)

        subject = fields.get(_ID_FIELD)

        if not subject:
            raise exc.authentication(
                "Telegram widget payload carried no user id",
                code="telegram_widget_invalid",
            )

        claims: JsonDict = dict(fields)

        return VerifiedAssertion(
            issuer=self.issuer,
            subject=subject,
            audience=self.bot_username,
            claims=claims,
        )

    # ....................... #

    async def verify_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> VerifiedAssertion:
        """:class:`TokenVerifierPort` adapter — the token is the widget query string.

        ``credentials.token`` is parsed as ``application/x-www-form-urlencoded``
        (``id=...&auth_date=...&hash=...``), the form Telegram appends to the widget
        redirect, then verified by :meth:`verify`.
        """

        from urllib.parse import parse_qsl

        # ``strict_parsing`` surfaces a malformed query as a clear authentication error
        # rather than silently dropping fields (which would change the data-check-string).
        try:
            pairs = parse_qsl(
                credentials.token, keep_blank_values=True, strict_parsing=True
            )

        except ValueError as error:
            raise exc.authentication(
                "Telegram widget token is not a valid query string",
                code="telegram_widget_invalid",
            ) from error

        return self.verify(dict(pairs))

    # ....................... #

    def _hash_matches(self, fields: Mapping[str, str], presented_hash: str) -> bool:
        data_check_string = "\n".join(
            f"{key}={fields[key]}" for key in sorted(fields)
        )
        secret_key = hashlib.sha256(self._bot_token_value().encode()).digest()
        computed = hmac.new(
            secret_key, data_check_string.encode(), hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(computed, presented_hash)

    # ....................... #

    def _require_fresh_auth_date(self, fields: Mapping[str, str]) -> None:
        if self.max_age is None:
            return

        raw = fields.get(_AUTH_DATE_FIELD)

        try:
            auth_date = int(raw) if raw is not None else None

        except ValueError:
            auth_date = None

        if auth_date is None:
            raise exc.authentication(
                "Telegram widget payload has no valid auth_date",
                code="telegram_widget_invalid",
            )

        age_seconds = utcnow().timestamp() - auth_date

        if age_seconds > self.max_age.total_seconds():
            raise exc.authentication(
                "Telegram widget payload is stale (auth_date too old)",
                code="telegram_widget_expired",
            )
