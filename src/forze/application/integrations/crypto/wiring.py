"""Document field-encryption wiring resolution and fail-closed validation.

The document analog of
:func:`~forze.application.integrations.storage.validate_storage_encryption_wiring`.
Where storage knows its ``encrypt`` flag statically (per route config), document
encryption is declared on the *spec* (``encrypted_fields`` / ``searchable_fields``)
and so can only be resolved at factory call time. :func:`resolve_document_codecs`
runs there, performing two fail-closed checks before wrapping the codec bundle:

1. **Infra presence** — a spec that marks fields for encryption MUST have a keyring
   wired (and a deterministic cipher when it declares searchable fields). Without
   it the values would silently persist as plaintext, so this raises rather than
   degrading.
2. **Coverage floor** — when the deployment declares ``required_encryption``, a
   spec whose derived tier is weaker is refused (e.g. it forgot to mark any
   field). Documents can only ever provide per-``field`` coverage.
"""

from collections.abc import Callable
from typing import Any

from forze.application.contracts.crypto import (
    DeterministicFieldCipherPort,
    EncryptionTier,
    FieldCipherPort,
    validate_required_encryption,
)
from forze.application.contracts.document import DocumentCodecs
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import exc

from .codec import encrypting_document_codecs

# ----------------------- #


def resolve_document_codecs(
    codecs: DocumentCodecs[Any, Any, Any, Any],
    *,
    spec_name: str,
    encrypted_fields: frozenset[str],
    searchable_fields: frozenset[str],
    keyring: FieldCipherPort | None,
    deterministic: DeterministicFieldCipherPort | None,
    tenant_provider: Callable[[], TenantIdentity | None],
    integration: str,
    code: str,
    required_encryption: EncryptionTier | None = None,
    bind_record_id: bool = False,
) -> DocumentCodecs[Any, Any, Any, Any]:
    """Validate encryption wiring for a document spec and wrap its codecs.

    *keyring* / *deterministic* are the resolved ciphers or ``None`` when the
    dependency is not registered (the caller passes ``None`` instead of letting
    the lookup raise, so this can report a precise, actionable error). When the
    spec declares no encrypted or searchable fields the bundle is returned
    unchanged — subject only to the ``required_encryption`` floor.
    """

    declares = bool(encrypted_fields or searchable_fields)
    derived: EncryptionTier = "field" if declares else "none"

    # Coverage floor first: catches a spec that declares nothing under a deployment
    # that requires encryption (the "forgot to mark fields" case).
    validate_required_encryption(
        integration=f"{integration} document {spec_name!r}",
        derived=derived,
        required=required_encryption,
        code=code,
        max_supported="field",
    )

    if not declares:
        return codecs

    # Infra presence: fail closed rather than persist plaintext for fields the spec
    # explicitly marked sensitive.
    if keyring is None:
        raise exc.configuration(
            f"{integration} document {spec_name!r} declares encrypted fields "
            f"{sorted(encrypted_fields | searchable_fields)} but no keyring is "
            "wired. Add a CryptoDepsModule (registers the keyring) or remove the "
            "encrypted/searchable field declarations.",
            code=code,
            details={
                "document": spec_name,
                "encrypted_fields": sorted(encrypted_fields),
                "searchable_fields": sorted(searchable_fields),
            },
        )

    if searchable_fields and deterministic is None:
        raise exc.configuration(
            f"{integration} document {spec_name!r} declares searchable "
            f"(deterministic) fields {sorted(searchable_fields)} but no "
            "deterministic cipher is wired. Set CryptoDepsModule(deterministic_root="
            "...) or remove the searchable field declarations.",
            code=code,
            details={
                "document": spec_name,
                "searchable_fields": sorted(searchable_fields),
            },
        )

    return encrypting_document_codecs(
        codecs,
        fields=encrypted_fields,
        cipher=keyring,
        tenant_provider=tenant_provider,
        label=spec_name,
        searchable_fields=searchable_fields,
        deterministic=deterministic,
        record_id_field="id" if bind_record_id else None,
    )
