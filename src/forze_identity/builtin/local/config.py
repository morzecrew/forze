"""Frozen configuration models for local identity backends."""

from typing import Mapping, Self, final
from uuid import UUID

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class LocalApiKeyEntry:
    """Principal (and optional tenant) bound to one raw API key string."""

    principal_id: UUID
    """Canonical Forze principal id."""

    tenant_id: UUID | None = attrs.field(default=None)
    """Optional tenant resolved together with this key."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class LocalIdentityConfig:
    """Static API-key and principal→tenant mappings loaded from env or a JSON file."""

    api_keys: Mapping[str, LocalApiKeyEntry] = attrs.field(
        factory=dict[str, LocalApiKeyEntry]
    )
    """Lookup by raw API key string (constant-time compare at verify time)."""

    principal_tenants: Mapping[UUID, UUID] = attrs.field(factory=dict[UUID, UUID])
    """Principal id to tenant id (merged with inline tenant ids from api_keys at load)."""

    default_tenant_id: UUID | None = attrs.field(default=None)
    """Fallback tenant when a principal has no explicit mapping."""

    # ....................... #

    @classmethod
    def from_mapping(cls, data: JsonDict) -> Self:
        """Build config from a plain mapping (tests and programmatic demos)."""

        raw_keys = data.get("api_keys") or {}  # type: ignore

        if not isinstance(raw_keys, Mapping):
            raise exc.configuration("api_keys must be a mapping")

        api_keys: dict[str, LocalApiKeyEntry] = {}
        principal_tenants: dict[UUID, UUID] = {}

        explicit_pt = data.get("principal_tenants") or {}  # type: ignore

        if not isinstance(explicit_pt, Mapping):
            raise exc.configuration("principal_tenants must be a mapping")

        for pid_raw, tid_raw in explicit_pt.items():  # type: ignore
            try:
                principal_tenants[UUID(str(pid_raw))] = UUID(str(tid_raw))  # type: ignore

            except ValueError:
                raise exc.configuration("principal_tenants must be a mapping of UUIDs")

        for key, entry_raw in raw_keys.items():  # type: ignore
            if not isinstance(key, str) or not key:
                raise exc.configuration("api_keys keys must be non-empty strings")

            if key in api_keys:
                raise exc.configuration(f"duplicate api_keys entry: {key!r}")

            if not isinstance(entry_raw, Mapping):
                raise exc.configuration(f"api_keys[{key!r}] must be a mapping")

            try:
                principal_id = UUID(str(entry_raw["principal_id"]))  # type: ignore

            except ValueError:

                raise exc.configuration("principal_id must be a valid UUID")

            tenant_raw = entry_raw.get("tenant_id")  # type: ignore

            try:
                tenant_id = UUID(str(tenant_raw)) if tenant_raw is not None else None  # type: ignore

            except ValueError:
                raise exc.configuration("tenant_id must be a valid UUID")

            api_keys[key] = LocalApiKeyEntry(
                principal_id=principal_id,
                tenant_id=tenant_id,
            )

            if tenant_id is not None:
                principal_tenants[principal_id] = tenant_id

        default_raw = data.get("default_tenant_id")
        default_tenant_id = UUID(str(default_raw)) if default_raw is not None else None

        return cls(
            api_keys=api_keys,
            principal_tenants=principal_tenants,
            default_tenant_id=default_tenant_id,
        )
