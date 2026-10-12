"""Tests for :mod:`forze_kits.aggregates.authn.handlers.deactivate_principal`."""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kits.aggregates.authn.handlers.deactivate_principal import (
    DeactivatePrincipalHandler,
    DeactivatePrincipalRequestDTO,
)


class TestDeactivatePrincipalHandler:
    @pytest.mark.asyncio
    async def test_delegates_to_port(self) -> None:
        principal_id = uuid4()
        port = AsyncMock()
        port.deactivate = AsyncMock(return_value=None)
        handler = DeactivatePrincipalHandler(
            resolver=lambda: AuthnIdentity(principal_id=uuid4()), deactivation=port
        )

        await handler(DeactivatePrincipalRequestDTO(principal_id=principal_id))

        port.deactivate.assert_awaited_once_with(principal_id)

    @pytest.mark.asyncio
    async def test_no_identity_is_401_before_the_port(self) -> None:
        # Registered in an app's own registry without guards, it still refuses anyone anonymous.
        port = AsyncMock()
        handler = DeactivatePrincipalHandler(resolver=lambda: None, deactivation=port)

        with pytest.raises(CoreException) as caught:
            await handler(DeactivatePrincipalRequestDTO(principal_id=uuid4()))

        assert caught.value.kind is ExceptionKind.AUTHENTICATION
        port.deactivate.assert_not_awaited()
