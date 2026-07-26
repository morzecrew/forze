"""Postgres counterparty-rotated credential store configuration."""

from datetime import timedelta
from typing import final

import attrs

from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec
from forze.application.contracts.secrets import CredentialExchangerPort
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

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
