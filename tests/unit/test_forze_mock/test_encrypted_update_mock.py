"""The mock updates a field-encrypted document as the real stores must."""

from __future__ import annotations

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import CryptoDepsModule
from forze_mock import MockDepsModule, MockKeyManagement
from tests.support.encrypted_update import (
    ENCRYPTION,
    KEY,
    EncCreate,
    EncDoc,
    EncRead,
    EncUpdate,
    assert_encrypted_updates,
)
from tests.support.execution_context import context_from_modules

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_a_sealed_field_of_any_type_updates() -> None:
    ctx = context_from_modules(
        MockDepsModule(), CryptoDepsModule(kms=MockKeyManagement(), directory=KEY)
    )
    spec = DocumentSpec(
        name="enc",
        read=EncRead,
        write=DocumentWriteTypes(domain=EncDoc, create_cmd=EncCreate, update_cmd=EncUpdate),
        encryption=ENCRYPTION,
    )

    await assert_encrypted_updates(ctx.document.command(spec), ctx.document.query(spec))
