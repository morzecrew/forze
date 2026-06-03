"""Helpers for :class:`HttpRoutingCredentials`."""

from forze_http.kernel.client.routing_credentials import HttpRoutingCredentials

# ----------------------- #


def credential_auth_headers(creds: HttpRoutingCredentials) -> dict[str, str]:
    """Build default headers from routing credentials."""

    headers = dict(creds.headers or {})

    if creds.bearer_token is not None and "Authorization" not in headers:
        headers["Authorization"] = f"Bearer {creds.bearer_token.get_secret_value()}"

    return headers
