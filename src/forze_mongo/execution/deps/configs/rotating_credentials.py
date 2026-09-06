"""Mongo counterparty-rotated credential store configuration."""

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
class MongoRotatingCredentialsConfig(TenantAwareIntegrationConfig):
    """Configuration for the Mongo rotating-credential store.

    See :class:`~forze_mongo.adapters.rotating_credentials.MongoRotatingCredentialStore`.
    """

    collection: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """``(database, collection)`` holding one document per ``(tenant, ref)``."""

    exchanger: CredentialExchangerPort
    """The application's call to the counterparty's token endpoint.

    Required, and deliberately not defaulted: the exchange is a request to someone else's
    provider, so there is nothing sensible to guess. A store without one could hold a
    credential it can never rotate."""

    exchange_timeout: timedelta = timedelta(seconds=30)
    """Bound on the exchange, and the source of the credential lease's own duration."""

    encrypt: bool = True
    """Seal the stored credential at rest under the wired keyring. **On by default.**

    Unlike every other store's ``encrypt`` flag, this one defaults to ``True``, because
    every document here *is* a replayable long-lived credential: a leaked backup or a
    read-only secondary of a plaintext collection hands out working third-party access for
    every tenant. Only ``payload`` is sealed, so ``expires_at`` stays readable for operators
    hunting expiring grants.

    The factory fails closed at resolve if this is on and no keyring is wired."""

    acknowledge_plaintext: bool = False
    """Required to be ``True`` when :attr:`encrypt` is off — an explicit statement that
    storing replayable credentials in the clear is the intent.

    Two fields rather than one because the name has to carry the consequence: ``encrypt``
    reads like a performance toggle, and this does not."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.encrypt and not self.acknowledge_plaintext:
            raise exc.configuration(
                "Rotating-credential storage would keep replayable credentials in the "
                "clear: pass acknowledge_plaintext=True to state that a plaintext "
                "collection is the intent, or leave encrypt=True and wire a keyring.",
            )
