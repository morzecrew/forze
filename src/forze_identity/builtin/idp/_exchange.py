"""Shared server-side OIDC authorization-code exchange over httpx.

Each IdP preset builds its provider-specific form ``data`` / ``headers`` and
parses the returned payload into its own token-response type; this helper
centralizes the transport and the failure mapping shared across presets.
"""

from typing import cast

import httpx

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #


async def oidc_code_exchange(
    *,
    token_endpoint: str,
    data: dict[str, str],
    headers: dict[str, str],
    provider: str,
    error_code: str,
    timeout: float,
) -> JsonDict:
    """POST an authorization-code exchange and return the validated JSON payload.

    :param token_endpoint: Provider token endpoint URL.
    :param data: Form-encoded request body.
    :param headers: Request headers (content type and optional client auth).
    :param provider: Human-readable provider label used in error messages.
    :param error_code: Forze error code attached to every failure.
    :param timeout: Request timeout in seconds.
    :returns: The decoded JSON object payload.
    :raises CoreException: On transport failure, HTTP >= 400, or non-object JSON.
    """

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(token_endpoint, data=data, headers=headers)

    except httpx.HTTPError as e:
        raise exc.infrastructure(
            f"{provider} token exchange request failed",
            code=error_code,
        ) from e

    if response.status_code >= 400:
        raise exc.authentication(
            f"{provider} token exchange rejected the authorization code",
            code=error_code,
        )

    try:
        payload = response.json()

    except ValueError as e:
        raise exc.infrastructure(
            f"{provider} token response is not valid JSON",
            code=error_code,
        ) from e

    if not isinstance(payload, dict):
        raise exc.infrastructure(
            f"{provider} token response must be a JSON object",
            code=error_code,
        )

    return cast(JsonDict, payload)
