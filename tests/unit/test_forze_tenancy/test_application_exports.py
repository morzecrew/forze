"""``forze_identity.tenancy.application`` exports its resource names, as authn's does."""

from __future__ import annotations

import pytest

import forze_identity.authn.application as authn_application
import forze_identity.tenancy.application as tenancy_application
from forze_identity.authn.application.constants import AuthnResourceName
from forze_identity.tenancy.domain.constants import TenancyResourceName

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("module", "enum"),
    [(authn_application, AuthnResourceName), (tenancy_application, TenancyResourceName)],
    ids=["authn", "tenancy"],
)
def test_the_resource_names_are_exported(module: object, enum: type) -> None:
    assert enum.__name__ in module.__all__  # type: ignore[attr-defined]
    assert getattr(module, enum.__name__) is enum
