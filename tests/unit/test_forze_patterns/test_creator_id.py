"""Tests for creator_id pattern mixins and mapping step."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest
from unittest.mock import MagicMock

from pydantic import BaseModel

from forze.application.contracts.authn import AuthnIdentity
from forze_patterns.creator_id.constants import CREATOR_ID_FIELD
from forze_patterns.creator_id.mapping import (
    CreatorIdMappingStep,
    CreatorIdMappingStepFactory,
)
from forze_patterns.creator_id.mixins import (
    CreatorIdCreateCmdMixin,
    CreatorIdMixin,
    CreatorIdUpdateCmdMixin,
)


class _Doc(CreatorIdMixin):
    title: str


class _Create(CreatorIdCreateCmdMixin):
    title: str


class _Update(CreatorIdUpdateCmdMixin):
    title: str | None = None


class _Source(BaseModel):
    value: str = "x"


def test_creator_id_mixin_defaults() -> None:
    doc = _Doc(title="x")
    assert doc.creator_id is None


def test_create_and_update_cmd_mixins() -> None:
    cid = uuid4()
    create = _Create(title="a", creator_id=cid)
    update = _Update(title="b", creator_id=cid)
    assert create.creator_id == cid
    assert update.creator_id == cid


@pytest.mark.asyncio
async def test_creator_id_mapping_step_with_identity() -> None:
    pid = uuid4()
    step = CreatorIdMappingStep(
        resolver=lambda: AuthnIdentity(principal_id=pid),
    )
    source = (_Source(), {})
    out = await step(source)
    assert out == {CREATOR_ID_FIELD: pid}


@pytest.mark.asyncio
async def test_creator_id_mapping_step_without_identity() -> None:
    step = CreatorIdMappingStep(resolver=lambda: None)
    source = (_Source(), {})
    out = await step(source)
    assert out == {CREATOR_ID_FIELD: None}


def test_creator_id_mapping_step_factory() -> None:
    ctx = SimpleNamespace(inv=SimpleNamespace(get_authn=lambda: None))
    factory = CreatorIdMappingStepFactory()
    step = factory(ctx)  # type: ignore[arg-type]
    assert isinstance(step, CreatorIdMappingStep)
