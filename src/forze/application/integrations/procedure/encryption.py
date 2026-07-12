"""Field-encryption resolution for the procedures port.

Encryption applies to **params** first; result encryption is deferred. This wraps a
:class:`~forze.application.contracts.procedure.ProcedureSpec`'s params codec with an
:class:`EncryptingModelCodec`, so declared fields are sealed before they are bound into the SQL.
"""

from collections.abc import Callable
from typing import Any

import attrs

from forze.application.contracts.crypto import (
    DeterministicFieldCipherPort,
    FieldCipherPort,
)
from forze.application.contracts.procedure import ProcedureSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import EncryptingModelCodec
from forze.base.exceptions import exc

# ----------------------- #

_WIRING_CODE = "core.procedures.encryption_wiring"

# ....................... #


def resolve_procedure_codecs_spec(
    spec: ProcedureSpec[Any, Any],
    *,
    keyring: FieldCipherPort | None,
    deterministic: DeterministicFieldCipherPort | None,
    tenant_provider: Callable[[], TenantIdentity | None],
) -> ProcedureSpec[Any, Any]:
    """Return *spec* with its params codec wrapped for field encryption, or unchanged.

    Fail-closed: declaring encrypted/searchable params without the matching cipher wired raises
    rather than silently binding plaintext.
    """

    encryption = spec.encryption
    if encryption is None or encryption.is_empty:
        return spec

    # ``binds_record_id`` is rejected at spec construction — procedure params have no stable id.

    if keyring is None:
        raise exc.configuration(
            f"ProcedureSpec {spec.name!r} declares encrypted/searchable params but no keyring "
            "is wired. Register a CryptoDepsModule or clear the encrypted fields.",
            code=_WIRING_CODE,
        )

    if encryption.searchable and deterministic is None:
        raise exc.configuration(
            f"ProcedureSpec {spec.name!r} declares searchable params but no deterministic "
            "cipher is wired (CryptoDepsModule(deterministic_root=...)).",
            code=_WIRING_CODE,
        )

    params_codec = EncryptingModelCodec(
        inner=spec.resolved_params_codec,
        cipher=keyring,
        fields=encryption.encrypted,
        searchable_fields=encryption.searchable,
        deterministic=deterministic,
        tenant_provider=tenant_provider,
        reject_plaintext=encryption.reject_plaintext,
    )

    return attrs.evolve(spec, params_codec=params_codec)
