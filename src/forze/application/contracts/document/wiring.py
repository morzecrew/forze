"""Shared document wiring helpers for integration deps modules."""

from typing import Callable, Protocol

from forze.application.contracts.resolution import RelationSpec

# ----------------------- #


class ReadWriteDocumentConfig(Protocol):
    """Minimal read-write document config shape for read-only derivation."""

    @property
    def read(self) -> RelationSpec: ...

    @property
    def tenant_aware(self) -> bool: ...

    @property
    def batch_size(self) -> int: ...


# ....................... #


def derive_read_only_document_config[ReadOnlyConfigT](
    config: ReadWriteDocumentConfig,
    *,
    factory: Callable[..., ReadOnlyConfigT],
) -> ReadOnlyConfigT:
    """Build a read-only document config from a read-write config."""

    return factory(
        read=config.read,
        tenant_aware=config.tenant_aware,
        batch_size=config.batch_size,
    )
