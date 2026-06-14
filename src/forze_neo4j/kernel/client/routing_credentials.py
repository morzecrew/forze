"""Per-tenant Neo4j connection credentials (a JSON secret resolved per tenant)."""

from pydantic import BaseModel, SecretStr

# ----------------------- #


class Neo4jRoutingCredentials(BaseModel):
    """Per-tenant Neo4j connection credentials resolved from a ``SecretsPort``.

    :class:`~forze_neo4j.RoutedNeo4jClient` uses these to open a **dedicated** driver per
    tenant (the ``dedicated`` isolation tier — a separate instance / credentials per tenant).
    """

    uri: SecretStr
    """Bolt / Neo4j connection URI (e.g. ``neo4j+s://host:7687``)."""

    username: str | None = None
    """Auth username (``None`` for no auth or URI-embedded auth)."""

    password: SecretStr | None = None
    """Auth password (paired with :attr:`username`)."""
