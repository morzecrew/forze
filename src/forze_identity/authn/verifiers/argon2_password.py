from typing import Any, final

import attrs

from forze.application.contracts.authn import (
    PasswordCredentials,
    PasswordVerifierPort,
    VerifiedAssertion,
)
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.base.exceptions import exc
from forze_identity._secure_spec import forbid_cache_and_history

from .._logger import logger
from ..adapters._utils import find_password_account_by_login
from ..domain.constants import ISSUER_FORZE_PASSWORD
from ..domain.models.account import (
    PasswordAccount,
    ReadPasswordAccount,
    UpdatePasswordAccountCmd,
)
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

    When ``pa_cmd`` is wired, a successful verification against a hash produced with
    outdated Argon2 parameters transparently re-hashes the password with the current
    :class:`~forze_identity.authn.services.password.PasswordConfig` and persists it
    (rev-conditioned update). The rehash is best-effort: any failure — including a
    concurrent-update conflict — is logged and never fails the login; the next login
    retries. When ``pa_cmd`` is ``None`` (the default), behaviour is unchanged and
    stored hashes are never rewritten.
    """

    password_svc: PasswordService
    """Argon2 hasher service."""

    pa_qry: DocumentQueryPort[ReadPasswordAccount]
    """Query port for password accounts."""

    pa_cmd: (
        DocumentCommandPort[
            ReadPasswordAccount,
            PasswordAccount,
            Any,
            UpdatePasswordAccountCmd,
        ]
        | None
    ) = None
    """Optional command port enabling rehash-on-login; ``None`` disables persistence."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        specs = [self.pa_qry.spec]

        if self.pa_cmd is not None:
            specs.append(self.pa_cmd.spec)

        forbid_cache_and_history(*specs, label="Password account")

    # ....................... #

    async def verify_password(
        self,
        credentials: PasswordCredentials,
    ) -> VerifiedAssertion:
        account = await find_password_account_by_login(self.pa_qry, credentials.login)

        if account is not None and account.is_active:
            password_hash = account.password_hash
        else:
            password_hash = await self.password_svc.timing_dummy_hash()

        ok = await self.password_svc.verify_password(
            password_hash=password_hash,
            password=credentials.password,
        )

        if not ok or account is None or not account.is_active:
            raise exc.authentication(
                "Invalid login or password",
                code="invalid_credentials",
            )

        if self.pa_cmd is not None:
            await self._maybe_rehash(account, credentials.password)

        return VerifiedAssertion(
            issuer=ISSUER_FORZE_PASSWORD,
            subject=str(account.principal_id),
        )

    # ....................... #

    async def _maybe_rehash(
        self,
        account: ReadPasswordAccount,
        password: str,
    ) -> None:
        """Upgrade an outdated stored hash to current Argon2 parameters.

        Fire-safe: the password is already verified, so a lost rehash (e.g. a
        rev conflict from a concurrent update) is harmless — log and move on.
        """

        try:
            if not self.password_svc.password_needs_rehash(account.password_hash):
                return

            new_hash = await self.password_svc.hash_password(password)
            upd_cmd = UpdatePasswordAccountCmd(password_hash=new_hash)

            await self.pa_cmd.update(  # type: ignore[union-attr]
                account.id,
                account.rev,
                upd_cmd,
                return_new=False,
            )

        except Exception as e:  # noqa: BLE001 — rehash is best-effort; login must not fail
            logger.warning(
                "Password rehash-on-login failed for account %s: %s",
                account.id,
                e,
            )
