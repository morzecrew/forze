# VK ID (OIDC)

Server-side OAuth 2.1 + PKCE with VK ID, bootstrap on the returned **`id_token` JWT** only, then Forze JWTs for your API. Opaque `access_token` / `refresh_token` from VK stay on the server if you need VK API calls later.

See [External bootstrap → Forze JWT](external-bootstrap-forze-jwt.md).

## Install

```bash
uv add 'forze[oidc]'
```

## Preset and wiring

```python
from forze_identity.builtin.idp.vk import VkIdOidcConfig, vk_identity_deps

deps = vk_identity_deps(
    VkIdOidcConfig(
        client_id="<vk-app-id>",
        redirect_uri="https://your.app/auth/vk/callback",
        client_secret=None,  # optional when using PKCE-only
    ),
    authn_route="bootstrap",
)
```

## Authorization + token exchange

```python
from forze_identity.oauth import generate_pkce
from forze_identity.builtin.idp.vk import exchange_authorization_code

pkce = generate_pkce()
# Redirect user to VK authorize URL with pkce.code_challenge (S256) ...

tokens = await exchange_authorization_code(
    config,
    code=authorization_code,
    code_verifier=pkce.code_verifier,
    device_id=device_id_from_callback,  # required by VK when returned in callback
)

# Only tokens.id_token goes to Forze bootstrap authn:
identity = await authn.authenticate_with_token(
    AccessTokenCredentials(token=tokens.id_token),
    spec=bootstrap_spec,
)
```

Do **not** pass `tokens.access_token` to `OidcTokenVerifier` — it is opaque, not a JWT.

## Defaults

| Field | Value |
|-------|-------|
| Issuer | `https://id.vk.ru` |
| JWKS | `https://id.vk.ru/.well-known/jwks.json` |
| Token endpoint | `https://id.vk.ru/oauth2/auth` |

## Learn more

- [External bootstrap → Forze JWT](external-bootstrap-forze-jwt.md)
- [VK ID API reference](https://id.vk.com/about/business/go/docs/ru/vkid/latest/vk-id/connection/api-description)
