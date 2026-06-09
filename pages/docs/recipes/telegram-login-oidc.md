# Telegram Login (OIDC)

Telegram Login Widget / OAuth with OpenID Connect: exchange the authorization code server-side, verify the **`id_token` JWT**, then issue Forze JWTs. Legacy hash-widget validation is **not** supported in this preset.

See [External bootstrap → Forze JWT](external-bootstrap-forze-jwt.md).

## Install

```bash
uv add 'forze[oidc]'
```

Register a **Client ID** and **Client Secret** with [@BotFather](https://t.me/BotFather) (OIDC client, not only the bot token).

## Preset and wiring

```python
from forze_identity.builtin.idp.telegram import (
    TelegramLoginOidcConfig,
    telegram_login_identity_deps,
)

deps = telegram_login_identity_deps(
    TelegramLoginOidcConfig(
        client_id="<bot-client-id>",
        client_secret="<bot-client-secret>",
        redirect_uri="https://your.app/auth/telegram/callback",
    ),
    authn_route="bootstrap",
)
```

## Authorization + token exchange

```python
from forze_identity.oauth import generate_pkce
from forze_identity.builtin.idp.telegram import exchange_authorization_code

pkce = generate_pkce()
# Build authorize URL per https://core.telegram.org/widgets/login#openid-connect

tokens = await exchange_authorization_code(
    config,
    code=authorization_code,
    code_verifier=pkce.code_verifier,
)

identity = await authn.authenticate_with_token(
    AccessTokenCredentials(token=tokens.id_token),
    spec=bootstrap_spec,
)
```

The exchange helper sends `POST https://oauth.telegram.org/token` with `Authorization: Basic base64(client_id:client_secret)` per Telegram docs.

## Defaults

| Field | Value |
|-------|-------|
| Issuer | `https://oauth.telegram.org` |
| JWKS | `https://oauth.telegram.org/.well-known/jwks.json` |
| Audience | Bot **Client ID** (string) |

## Learn more

- [External bootstrap → Forze JWT](external-bootstrap-forze-jwt.md)
- [Telegram Login — OpenID Connect](https://core.telegram.org/widgets/login#openid-connect)
