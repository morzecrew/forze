"""Tests for :mod:`forze_kits.aggregates.authn.handlers.deactivate_principal`."""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

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
        handler = DeactivatePrincipalHandler(deactivation=port)

        await handler(DeactivatePrincipalRequestDTO(principal_id=principal_id))

        port.deactivate.assert_awaited_once_with(principal_id)
