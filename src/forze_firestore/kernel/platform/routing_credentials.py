"""Structured secrets for tenant-routed Firestore clients."""

from pydantic import BaseModel, Field

# ----------------------- #


class FirestoreRoutingCredentials(BaseModel):
    """JSON shape stored in secrets for :class:`~forze_firestore.kernel.platform.RoutedFirestoreClient`.

    Use with :func:`~forze.application.contracts.secrets.resolve_structured`.

    Per-tenant service account overrides are not supported in v1; use Application
    Default Credentials and isolate tenants by project/database or ``tenant_aware`` documents.
    """

    project_id: str = Field(..., min_length=1)
    database: str = "(default)"
