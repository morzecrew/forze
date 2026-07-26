"""Postgres counterparty-rotated credential store configuration."""

from datetime import timedelta
from typing import final

import attrs

from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec
from forze.application.contracts.secrets import CredentialExchangerPort
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresRotatingCredentialsConfig(TenantAwareIntegrationConfig):
    """Configuration for the Postgres rotating-credential store.

    See :class:`~forze_postgres.adapters.rotating_credentials.PostgresRotatingCredentialStore`.
    """

    relation: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Schema-qualified table holding one document per ``(tenant_id, ref)``."""

    exchanger: CredentialExchangerPort
    """The application's call to the counterparty's token endpoint.

    Required, and deliberately not defaulted: the exchange is a request to someone else's
    provider, so there is nothing sensible to guess. A store without one could hold a
    credential it can never rotate."""

    exchange_timeout: timedelta = timedelta(seconds=30)
    """Bound on the exchange, and the source of the row-locked transaction's own bounds."""

    encrypt: bool = True
    """Seal the stored credential at rest under the wired keyring. **On by default.**

    Unlike every other store's ``encrypt`` flag, this one defaults to ``True``, because
    every row here *is* a replayable long-lived credential: a leaked logical backup or a
    read-only replica of a plaintext table hands out working third-party access for every
    tenant. Only the ``payload`` column is sealed, so ``expires_at`` stays readable for
    operators hunting expiring grants.

    The factory fails closed at resolve if this is on and no keyring is wired."""

    acknowledge_plaintext: bool = False
    """Required to be ``True`` when :attr:`encrypt` is off — an explicit statement that
    storing replayable credentials in the clear is the intent.

    Two fields rather than one because the name has to carry the consequence: ``encrypt``
    reads like a performance toggle, and this does not. Mirrors the archive exporter, which
    refuses to write credential-adjacent data as plaintext until an operator says so."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.encrypt and not self.acknowledge_plaintext:
            raise exc.configuration(
                "Rotating-credential storage would keep replayable credentials in the "
                "clear: pass acknowledge_plaintext=True to state that a plaintext table "
                "is the intent, or leave encrypt=True and wire a keyring.",
            )
