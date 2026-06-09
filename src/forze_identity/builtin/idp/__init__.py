"""Pre-built OIDC IdP presets (Google, VK ID, Telegram Login).

Requires ``forze[oidc]`` (``pyjwt[crypto]`` and ``httpx`` for code exchange).

Generic OIDC verification and :class:`~forze_identity.oidc.OidcIdpPreset` live in
:mod:`forze_identity.oidc`; PKCE in :mod:`forze_identity.oauth`. This package supplies
vendor issuer/JWKS defaults, bootstrap deps wiring, and optional authorization-code
exchange helpers. Pair bootstrap routes with first-party Forze JWT issuance — see
``pages/docs/recipes/external-bootstrap-forze-jwt.md``.
"""

from ._compat import require_oidc

require_oidc()
