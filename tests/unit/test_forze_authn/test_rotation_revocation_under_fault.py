"""First-party access-token key rotation and session revocation under fault.

The happy-path tests cover a rotation *overlap* (old + new key both verify) and a *fresh* session
read rejecting a revoked token. This file pins the fault dimension those miss:

* **Revocation under replica lag** — a session read served by a lagging read replica returns the
  pre-revocation snapshot, so a revoked token is still admitted until replication catches up. A
  synchronous (read-your-writes) store rejects it at once; the lag is the window the synchronous mock
  cannot exhibit. Revocation is only as fresh as the session read.
* **Key-rotation propagation lag** — while a rotation propagates across a fleet, a replica still on
  the old key set REJECTS a new-key token (unknown ``kid``, fail closed) rather than admitting it
  unverified, and the overlap keeps in-flight old-key tokens valid. Rotation is safe under lag (no
  false accept), with a bounded new-key rejection window until the fleet reloads the key.
"""

from __future__ import annotations

from datetime import datetime
from typing import cast
from uuid import UUID, uuid4

import attrs
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from forze.application.contracts.authn import AccessTokenCredentials
from forze.application.contracts.document import DocumentQueryPort
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze_identity.authn import ForzeJwtTokenVerifier
from forze_identity.authn.domain.models.session import ReadSession
from forze_identity.authn.services import (
    AccessTokenConfig,
    AccessTokenService,
    Hs256Signer,
    LocalAsymmetricSigner,
)

# ----------------------- #

_SECRET = b"s" * 32
_CFG = AccessTokenConfig(issuer="forze", audience="forze")


@attrs.define
class _SessionRow:
    """Only the fields `ForzeJwtTokenVerifier` reads off a resolved session."""

    principal_id: UUID
    tenant_id: UUID | None = None
    revoked_at: datetime | None = None
    rotated_at: datetime | None = None


@attrs.frozen
class _SessionSpec:
    """A spec that passes the verifier's `forbid_cache_and_history` guard (no cache, no history)."""

    cache: object | None = None
    history_enabled: bool = False


@attrs.define
class _ReplicatedSessionQuery:
    """A session read served by an async read replica.

    ``find`` returns the last *replicated* snapshot, so a revocation written to the primary
    (:meth:`revoke`) is invisible until :meth:`replicate` — modeling replica lag, which a
    synchronous read-your-writes store never exhibits.
    """

    primary: _SessionRow
    spec: _SessionSpec = attrs.field(factory=_SessionSpec)
    replica: _SessionRow = attrs.field(
        init=False,
        default=attrs.Factory(lambda self: self._snapshot(), takes_self=True),
    )

    def _snapshot(self) -> _SessionRow:
        return _SessionRow(
            principal_id=self.primary.principal_id,
            tenant_id=self.primary.tenant_id,
            revoked_at=self.primary.revoked_at,
            rotated_at=self.primary.rotated_at,
        )

    def revoke(self) -> None:
        self.primary.revoked_at = utcnow()  # the write lands on the primary only

    def replicate(self) -> None:
        self.replica = self._snapshot()  # the read replica catches up

    async def find(self, *, filters: object = None) -> _SessionRow:
        _ = filters
        return self.replica


def _rs256_signer(kid: str) -> LocalAsymmetricSigner:
    return LocalAsymmetricSigner(
        private_key=rsa.generate_private_key(public_exponent=65537, key_size=2048),
        algorithm="RS256",
        kid=kid,
    )


# ....................... #


class TestRevocationUnderReplicaLag:
    async def test_lagging_replica_admits_a_revoked_token_until_it_catches_up(
        self,
    ) -> None:
        principal_id = uuid4()
        svc = AccessTokenService(signer=Hs256Signer(secret=_SECRET))
        token = await svc.issue_token(principal_id=principal_id, session_id=uuid4())

        store = _ReplicatedSessionQuery(primary=_SessionRow(principal_id=principal_id))
        verifier = ForzeJwtTokenVerifier(
            access_svc=svc,
            session_qry=cast("DocumentQueryPort[ReadSession]", store),
        )
        credentials = AccessTokenCredentials(token=token)

        # Baseline: an active session, replica in sync → admitted.
        assert (await verifier.verify_token(credentials)).subject == str(principal_id)

        # Revoke on the primary; the replica has NOT replicated yet.
        store.revoke()

        # Replica lag: the verifier reads the stale (active) snapshot, so the revoked token is STILL
        # admitted — the security gap a synchronous store cannot show.
        assert (await verifier.verify_token(credentials)).subject == str(principal_id)

        # Replication catches up → the revocation is enforced.
        store.replicate()
        with pytest.raises(CoreException) as excinfo:
            await verifier.verify_token(credentials)
        assert excinfo.value.code == "session_revoked"


class TestKeyRotationPropagationLag:
    async def test_replica_on_the_old_key_rejects_new_key_tokens_fail_closed(
        self,
    ) -> None:
        principal_id = uuid4()
        old = _rs256_signer("k1")
        new = _rs256_signer("k2")

        old_token = await AccessTokenService(signer=old, config=_CFG).issue_token(
            principal_id=principal_id
        )
        # The issuer has rotated to `new`, keeping `old` for the overlap.
        rotated = AccessTokenService(
            signer=new, config=_CFG, additional_verifiers=(old,)
        )
        new_token = await rotated.issue_token(principal_id=principal_id)

        # Overlap: the rotated issuer still verifies in-flight OLD-key tokens.
        assert (await rotated.verify_token(old_token))["sub"] == str(principal_id)

        # A replica still on the pre-rotation key set (knows only `old`) has not picked up `new`.
        lagging = AccessTokenService(signer=old, config=_CFG)

        # Propagation lag: the lagging replica REJECTS the new-key token (unknown kid) — fail closed,
        # never a silent accept.
        with pytest.raises(CoreException) as excinfo:
            await lagging.verify_token(new_token)
        assert excinfo.value.code == "invalid_access_token"

        # Once the replica reloads the rotated key set, the new-key token verifies.
        caught_up = AccessTokenService(
            signer=new, config=_CFG, additional_verifiers=(old,)
        )
        assert (await caught_up.verify_token(new_token))["sub"] == str(principal_id)
