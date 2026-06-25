"""VK ID ``id_token`` verification via server-side introspection (``public_info``)."""

from typing import cast, final

import attrs
import httpx

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    TokenVerifierPort,
    VerifiedAssertion,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from .config import VK_ID_OIDC_ISSUER, VK_ID_PUBLIC_INFO_ENDPOINT

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class VkPublicInfoTokenVerifier(TokenVerifierPort):
    """Verify a VK ``id_token`` by introspecting it at VK's ``public_info`` endpoint.

    VK ID publishes **no JWKS document** (and no OIDC discovery document), so local
    JWT signature verification is impossible. VK's documented validation path is
    server-side introspection: ``POST {public_info_url}`` with form parameters
    ``client_id`` and ``id_token``. On success VK returns masked public user data
    (``{"user": {"user_id": ..., ...}}``); on failure it returns an OAuth-style
    error object (``invalid_id_token``, ``invalid_client``, ...) — note VK replies
    with HTTP 200 even for vendor errors, so failure is detected from the payload.

    .. warning:: Security model

        This is server-side introspection — trust comes from the TLS connection to
        ``id.vk.ru``, **not** from a local signature check. The ``id_token`` is sent
        to VK only (its own issuer), never to a third party.

    The emitted :class:`VerifiedAssertion` uses the configured ``issuer`` and the
    VK ``user_id`` (stringified) as ``subject`` — the same identity derivation that
    feeds :class:`~forze_identity.authn.execution.deps.ConfigurableDeterministicUuidResolver`.
    """

    client_id: str
    """VK application id; VK validates the token against this app."""

    public_info_url: str = attrs.field(default=VK_ID_PUBLIC_INFO_ENDPOINT)
    """VK ID ``public_info`` introspection endpoint."""

    issuer: str = attrs.field(default=VK_ID_OIDC_ISSUER)
    """Issuer recorded on the emitted assertion (principal-resolver discriminator)."""

    timeout: float = attrs.field(default=10.0)
    """Request timeout in seconds for the introspection call."""

    transport: httpx.AsyncBaseTransport | None = attrs.field(default=None, eq=False)
    """Optional httpx transport override (inject :class:`httpx.MockTransport` in tests)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.client_id.strip():
            raise exc.configuration("VK client_id must be non-empty")

        if self.timeout <= 0:
            raise exc.configuration("Timeout must be positive")

    # ....................... #

    async def verify_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> VerifiedAssertion:
        data = {
            "client_id": self.client_id,
            "id_token": credentials.token,
        }

        try:
            async with httpx.AsyncClient(
                timeout=self.timeout,
                transport=self.transport,
            ) as client:
                response = await client.post(
                    self.public_info_url,
                    data=data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )

        except httpx.HTTPError as e:
            raise exc.infrastructure(
                "VK id_token introspection request failed",
                code="vk_public_info_unavailable",
            ) from e

        try:
            payload = response.json()

        except ValueError as e:
            raise exc.infrastructure(
                "VK id_token introspection returned a non-JSON response",
                code="vk_public_info_invalid_response",
            ) from e

        if not isinstance(payload, dict):
            raise exc.infrastructure(
                "VK id_token introspection response must be a JSON object",
                code="vk_public_info_invalid_response",
            )

        payload = cast(JsonDict, payload)

        error = payload.get("error")

        if error is not None or response.status_code >= 400:
            # All vendor errors (invalid_id_token, invalid_client, invalid_request,
            # access_denied, slow_down) map to authentication: the verifier cannot
            # reliably distinguish a misconfigured client_id from a token minted
            # for a different app, and a remote response must not be trusted to
            # reclassify our own configuration. The vendor error code (never the
            # token) is preserved in details for operators.
            raise exc.authentication(
                "VK rejected the id_token",
                code="vk_id_token_rejected",
                details={"vendor_error": error} if isinstance(error, str) else None,
            )

        subject = self._extract_user_id(payload)

        if subject is None:
            raise exc.authentication(
                "VK id_token introspection response carried no user id",
                code="vk_id_token_rejected",
            )

        return VerifiedAssertion(
            issuer=self.issuer,
            subject=subject,
            audience=self.client_id,
            claims=self._safe_claims(payload),
        )

    # ....................... #

    @staticmethod
    def _safe_claims(payload: JsonDict) -> JsonDict:
        """Only the masked ``user`` object VK returns — never the top-level envelope.

        VK wraps the user data in a ``user`` object alongside protocol/envelope fields
        (``error``, ``state``, ...) and replies HTTP 200 even on failure. Only that
        object is identity data — the same data the subject is derived from. Copying the
        whole payload would surface attacker-influenceable envelope fields to downstream
        claim/tenant mappers, so the claims keep just the user object (empty when VK
        returns the id at the top level only).
        """

        user = payload.get("user")
        return {"user": dict(cast(JsonDict, user))} if isinstance(user, dict) else {}

    # ....................... #

    @staticmethod
    def _extract_user_id(payload: JsonDict) -> str | None:
        """Extract the VK user id as a non-empty string, or ``None``.

        The documented success shape is ``{"user": {"user_id": ...}}``; a top-level
        ``user_id`` is accepted defensively. Only non-empty string / integer scalars
        qualify — anything else is treated as a failed verification.
        """

        user = payload.get("user")

        if isinstance(user, dict):
            container = cast(JsonDict, user)

        else:
            container = payload

        raw = container.get("user_id")

        if isinstance(raw, bool):
            return None

        if isinstance(raw, int):
            return str(raw)

        if isinstance(raw, str) and raw.strip():
            return raw

        return None
