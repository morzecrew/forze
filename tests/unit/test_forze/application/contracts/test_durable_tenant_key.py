"""The one encoding every durable store keys a caller-chosen key under.

# covers: scope_tenant_key
# covers: unscope_tenant_key
"""

from __future__ import annotations

from itertools import product
from uuid import UUID

import pytest

from forze.application.contracts.durable.function import scope_tenant_key, unscope_tenant_key

# ----------------------- #

_A = UUID("00000000-0000-0000-0000-00000000000a")
_B = UUID("00000000-0000-0000-0000-00000000000b")

_KEYS = ["", ":", ":k", "k:v", f"{_A}:k", f"{_B}:k", str(_A)]
"""Keys shaped like each other's stored forms, the separator alone, and a bare tenant id."""

_TENANTS = [None, _A, _B]


@pytest.mark.parametrize(("key", "tenant_id"), list(product(_KEYS, _TENANTS)))
def test_a_scoped_key_reads_back_as_written(key: str, tenant_id: UUID | None) -> None:
    assert unscope_tenant_key(scope_tenant_key(key, tenant_id), tenant_id) == key


def test_no_two_callers_share_a_stored_key() -> None:
    stored = [scope_tenant_key(key, tenant) for key, tenant in product(_KEYS, _TENANTS)]

    # Untagged, tenant A and tenant B never meet, whatever each spells its key like.
    assert len(set(stored)) == len(stored)
