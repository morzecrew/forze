from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    AuthnIdentity,
    AuthnPort,
    PasswordCredentials,
    TokenCredentials,
)
from forze.application.contracts.document import DocumentQueryPort
from forze.base.errors import AuthenticationError, CoreError
from forze.base.validators import NoneValidator

from ..domain.constants import ACCESS_TOKEN_KIND, ACCESS_TOKEN_SCHEME
from ..domain.models.account import ReadApiKeyAccount, ReadPasswordAccount
from ..services import AccessTokenService, ApiKeyService, PasswordService
from ._utils import find_api_key_account_by_key_hash, find_password_account_by_login

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnAdapter(AuthnPort):
    """Authentication adapter."""

    access_svc: AccessTokenService | None = attrs.field(default=None)
    """Access token service."""

    password_svc: PasswordService | None = attrs.field(default=None)
    """Password service."""

    api_key_svc: ApiKeyService | None = attrs.field(default=None)
    """Optional API key service."""

    # ....................... #

    pa_qry: DocumentQueryPort[ReadPasswordAccount] | None = attrs.field(default=None)
    """Query port for password accounts."""

    ak_qry: DocumentQueryPort[ReadApiKeyAccount] | None = attrs.field(default=None)
    """Query port for API key accounts."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not NoneValidator.all_or_none(self.password_svc, self.pa_qry):
            raise CoreError("All or none password dependencies must be provided")

        if not NoneValidator.all_or_none(self.api_key_svc, self.ak_qry):
            raise CoreError("All or none API key dependencies must be provided")

        if self.pa_qry is not None:
            pa_spec = self.pa_qry.spec

            # Questionable, but..
            if pa_spec.cache is not None:
                raise CoreError(
                    "Password account caching is forbidden by security reasons"
                )

            if pa_spec.history_enabled:
                raise CoreError(
                    "Password account history is forbidden by security reasons"
                )

        if self.ak_qry is not None:
            ak_spec = self.ak_qry.spec

            if ak_spec.cache is not None:
                raise CoreError(
                    "API key account caching is forbidden by security reasons"
                )

            if ak_spec.history_enabled:
                raise CoreError(
                    "API key account history is forbidden by security reasons"
                )

    # ....................... #

    def _require_password_svc(self) -> PasswordService:
        if self.password_svc is None:
            raise CoreError("Password service is required")

        return self.password_svc

    def _require_api_key_svc(self) -> ApiKeyService:
        if self.api_key_svc is None:
            raise CoreError("API key service is required")

        return self.api_key_svc

    def _require_password_account_qry(self) -> DocumentQueryPort[ReadPasswordAccount]:
        if self.pa_qry is None:
            raise CoreError("Password account query port is required")

        return self.pa_qry

    def _require_api_key_account_qry(self) -> DocumentQueryPort[ReadApiKeyAccount]:
        if self.ak_qry is None:
            raise CoreError("API key account query port is required")

        return self.ak_qry

    def _require_access_svc(self) -> AccessTokenService:
        if self.access_svc is None:
            raise CoreError("Access token service is required")

        return self.access_svc

    # ....................... #

    async def authenticate_with_password(
        self,
        credentials: PasswordCredentials,
    ) -> AuthnIdentity:
        svc = self._require_password_svc()
        qry = self._require_password_account_qry()

        account = await find_password_account_by_login(qry, credentials.login)

        if account is None or not account.is_active:
            raise AuthenticationError("Password account not found")

        password_ok = svc.verify_password(
            password=credentials.password,
            password_hash=account.password_hash,
        )

        if not password_ok:
            raise AuthenticationError("Invalid password")

        return AuthnIdentity(principal_id=account.principal_id)

    # ....................... #

    async def authenticate_with_api_key(
        self,
        credentials: ApiKeyCredentials,
    ) -> AuthnIdentity:
        svc = self._require_api_key_svc()
        qry = self._require_api_key_account_qry()

        key_hash = svc.calculate_key_digest(credentials.key)
        account = await find_api_key_account_by_key_hash(qry, key_hash)

        if account is None or not account.is_active:
            raise AuthenticationError("API key account not found")

        key_ok = svc.verify_key(
            key=credentials.key,
            expected_digest=account.key_hash,
        )

        if not key_ok:
            raise AuthenticationError("Invalid API key")

        return AuthnIdentity(principal_id=account.principal_id)

    # ....................... #

    async def authenticate_with_token(
        self,
        credentials: TokenCredentials,
    ) -> AuthnIdentity:
        svc = self._require_access_svc()

        if (
            credentials.scheme != ACCESS_TOKEN_SCHEME
            or credentials.kind != ACCESS_TOKEN_KIND
        ):
            raise AuthenticationError("Invalid token")

        # will raise error by itself if token is invalid
        claims = svc.verify_token(credentials.token)

        principal_id = UUID(claims["sub"])

        return AuthnIdentity(principal_id=principal_id)
