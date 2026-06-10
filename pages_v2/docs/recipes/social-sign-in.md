---
title: Social sign-in
icon: lucide/users-round
summary: Google, VK ID, and Telegram via shipped OIDC presets — all feed the bootstrap pattern
---

Google, VK ID, and Telegram Login are all shipped **presets**. Each produces a
`bootstrap` auth route that verifies that provider's `id_token`; from there it's
the same [bootstrap → Forze JWT](external-bootstrap-forze-jwt.md) flow. They
differ only in their config and in how you obtain the `id_token`.

Install the OIDC extra: `uv add 'forze[oidc]'`.

## The three presets

| | Google | VK ID | Telegram |
|---|--------|-------|----------|
| Package | `forze_identity.builtin.idp.google` | `…idp.vk` | `…idp.telegram` |
| Deps fn | `google_identity_deps` | `vk_identity_deps` | `telegram_login_identity_deps` |
| Config | `GoogleOidcConfig(client_id)` | `VkIdOidcConfig(client_id, redirect_uri, client_secret?)` | `TelegramLoginOidcConfig(client_id, client_secret, redirect_uri)` |
| `id_token` from | client SDK (direct) | PKCE **code exchange** | PKCE **code exchange** |
| Credentials | OAuth client id | VK app id (+ optional secret) | client id **+ secret** from **@BotFather** |

## Wire and exchange

Each preset wires the bootstrap route; for the code-exchange providers you swap
the auth code for an `id_token` first, then run the
[login handler](external-bootstrap-forze-jwt.md#the-login-handler) unchanged.

=== "Google"

    Google's client SDK hands you the `id_token` directly — no exchange:

    ```python
    from forze_identity.builtin.idp.google import GoogleOidcConfig, google_identity_deps

    bootstrap = google_identity_deps(
        GoogleOidcConfig(client_id="<google-oauth-client-id>"),
        authn_route="bootstrap",
    )
    # /login receives id_token from the client → bootstrap flow as-is.
    ```

=== "VK ID"

    VK uses an authorization-code + PKCE exchange; pass through the `device_id`
    VK returns on the callback:

    ```python
    from forze_identity.oauth import generate_pkce
    from forze_identity.builtin.idp.vk import (
        VkIdOidcConfig, vk_identity_deps, exchange_authorization_code,
    )

    config = VkIdOidcConfig(client_id="<vk-app-id>", redirect_uri="https://app/cb")
    bootstrap = vk_identity_deps(config, authn_route="bootstrap")

    # authorize step: build the URL with generate_pkce().code_challenge (S256),
    # keep the code_verifier in the session.
    tokens = await exchange_authorization_code(
        config, code=auth_code, code_verifier=code_verifier, device_id=device_id,
    )
    id_token = tokens.id_token  # opaque access/refresh stay server-side
    ```

=== "Telegram"

    Telegram is the same code-exchange shape, with the client id **and secret**
    from BotFather and no `device_id`:

    ```python
    from forze_identity.oauth import generate_pkce
    from forze_identity.builtin.idp.telegram import (
        TelegramLoginOidcConfig, telegram_login_identity_deps, exchange_authorization_code,
    )

    config = TelegramLoginOidcConfig(
        client_id="<bot-client-id>",
        client_secret="<bot-client-secret>",
        redirect_uri="https://app/cb",
    )
    bootstrap = telegram_login_identity_deps(config, authn_route="bootstrap")

    tokens = await exchange_authorization_code(
        config, code=auth_code, code_verifier=code_verifier,
    )
    id_token = tokens.id_token
    ```

Once you have the `id_token`, the [login handler](external-bootstrap-forze-jwt.md#the-login-handler)
verifies it and mints first-party tokens — identical across all three providers.

## Notes

- `generate_pkce()` returns a `code_verifier` (keep it in the session between the
  authorize redirect and the callback) and a `code_challenge` (put it in the
  authorize URL).
- Only the `id_token` is a JWT the verifier accepts — the providers' opaque
  `access_token` / `refresh_token` are not bearer credentials for your API.
- Issuer, JWKS, and audience defaults are baked into each preset; override them on
  the config only for non-standard deployments.
