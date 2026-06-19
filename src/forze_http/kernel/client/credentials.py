"""Helpers for :class:`HttpRoutingCredentials`."""

from forze_http.kernel.client.routing_credentials import HttpRoutingCredentials

# ----------------------- #


def credential_auth_headers(creds: HttpRoutingCredentials) -> dict[str, str]:
    """Build default headers from routing credentials."""

    headers = dict(creds.headers or {})

    # HTTP header names are case-insensitive: an explicit ``authorization``
    # header (any casing) must suppress the default bearer, else two conflicting
    # Authorization headers are sent.
    has_authorization = any(k.lower() == "authorization" for k in headers)

    if creds.bearer_token is not None and not has_authorization:
        headers["Authorization"] = f"Bearer {creds.bearer_token.get_secret_value()}"

    return headers
