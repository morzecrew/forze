from typing import final

import attrs

from forze.application.contracts.authn import (
    PasswordCredentials,
    PasswordVerifierPort,
    VerifiedAssertion,
)
from forze.application.contracts.document import DocumentQueryPort
from forze.base.errors import AuthenticationError, CoreError

from ..adapters._utils import find_password_account_by_login
from ..domain.constants import ISSUER_FORZE_PASSWORD
from ..domain.models.account import ReadPasswordAccount
from ..services import PasswordService

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Argon2PasswordVerifier(PasswordVerifierPort):
    """Verify password credentials against a document-backed account using Argon2.

    Emits a :class:`VerifiedAssertion` whose ``subject`` is the resolved account's
    ``principal_id`` rendered as a string. Pairing this verifier with the
    :class:`~forze_authn.resolvers.jwt_native_uuid.JwtNativeUuidResolver` reproduces the
    pre-refactor first-party login behaviour.
    """

    password_svc: PasswordService
    """Argon2 hasher service."""

    pa_qry: DocumentQueryPort[ReadPasswordAccount]
    """Query port for password accounts."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        spec = self.pa_qry.spec

        if spec.cache is not None:
            raise CoreError(
                "Password account caching is forbidden by security reasons"
            )

        if spec.history_enabled:
            raise CoreError(
                "Password account history is forbidden by security reasons"
            )

    # ....................... #

    async def verify_password(
        self,
        credentials: PasswordCredentials,
    ) -> VerifiedAssertion:
        account = await find_password_account_by_login(self.pa_qry, credentials.login)

        if account is None or not account.is_active:
            raise AuthenticationError("Password account not found")

        ok = self.password_svc.verify_password(
            password=credentials.password,
            password_hash=account.password_hash,
        )

        if not ok:
            raise AuthenticationError("Invalid password")

        return VerifiedAssertion(
            issuer=ISSUER_FORZE_PASSWORD,
            subject=str(account.principal_id),
        )
