# Google Sign-In (OIDC)

Bootstrap authentication with Google-issued `id_token` JWTs, then issue first-party Forze JWTs for API traffic. See [External bootstrap → Forze JWT](external-bootstrap-forze-jwt.md) for the two-route pattern.

## Install

```bash
uv add 'forze[oidc]'
```

## Preset

```python
from forze_identity.builtin.idp.google import GoogleOidcConfig, google_identity_deps

deps = google_identity_deps(
    GoogleOidcConfig(client_id="<google-oauth-client-id>"),
    authn_route="bootstrap",
)
```

Defaults:

| Field | Value |
|-------|-------|
| Issuer | `https://accounts.google.com` |
| JWKS | `https://www.googleapis.com/oauth2/v3/certs` |
| Audience | Your OAuth **client id** |

## Front channel

Obtain an `id_token` from Google Identity Services (web), Google Sign-In for Android/iOS, or your own OAuth authorization flow. Send it to your **login** endpoint (HTTPS only); do not use it as the long-lived API bearer.

```python
identity = await authn.authenticate_with_token(
    AccessTokenCredentials(token=id_token_from_client),
    spec=bootstrap_spec,
)
issued = await lifecycle.issue_tokens(identity)
```

v1 of the preset ships **verifier + deps wiring** only — no in-framework authorization-code exchange. Add your own OAuth redirect handler if you need server-side code exchange.

## Learn more

- [External bootstrap → Forze JWT](external-bootstrap-forze-jwt.md)
- [External IdPs over OIDC](external-idp-oidc.md)
