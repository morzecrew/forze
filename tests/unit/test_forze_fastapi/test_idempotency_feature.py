"""Unit tests for :class:`~forze_fastapi.endpoints.http.features.idempotency.feature.IdempotencyFeature`."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException, Response
from pydantic import BaseModel

from forze.application.contracts.idempotency import IdempotencySpec
from forze_fastapi.endpoints.http.contracts.context import HttpEndpointContext
from forze_fastapi.endpoints.http.features.idempotency.constants import (
    IDEMPOTENCY_KEY_HEADER,
)
from forze_fastapi.endpoints.http.features.idempotency.feature import IdempotencyFeature


class _Body(BaseModel):
    name: str


def _make_ctx(
    *,
    headers: dict[str, str],
    idem_port: MagicMock,
    body: _Body,
    status_code: int = 200,
) -> HttpEndpointContext:
    raw_request = MagicMock()
    raw_request.headers.get = lambda k, d=None: headers.get(k, d)

    exec_ctx = MagicMock()
    exec_ctx.dep = MagicMock(
        return_value=lambda _ctx, _spec: idem_port,
    )

    spec = MagicMock()
    spec.http = {"status_code": status_code}
    spec.response = None

    return HttpEndpointContext(
        raw_request=raw_request,
        raw_kwargs={},
        exec_ctx=exec_ctx,
        facade=MagicMock(),
        dto=MagicMock(),
        input=body,
        spec=spec,
        operation_id="op.test",
    )


@pytest.mark.asyncio
async def test_idempotency_missing_header_raises_400() -> None:
    feature = IdempotencyFeature(spec=IdempotencySpec(name="r"))
    idem = MagicMock()
    ctx = _make_ctx(headers={}, idem_port=idem, body=_Body(name="a"))

    async def handler(_c: HttpEndpointContext) -> dict[str, bool]:
        return {"ok": True}

    wrapped = feature.wrap(handler)

    with pytest.raises(HTTPException) as exc:
        await wrapped(ctx)

    assert exc.value.status_code == 400
    assert IDEMPOTENCY_KEY_HEADER in str(exc.value.detail)
    idem.begin.assert_not_called()


@pytest.mark.asyncio
async def test_idempotency_replay_returns_raw_response() -> None:
    feature = IdempotencyFeature(spec=IdempotencySpec(name="r"))
    idem = MagicMock()
    idem.begin = AsyncMock(
        return_value={
            "body": b'{"cached":true}',
            "code": 201,
            "content_type": "application/json",
        },
    )
    ctx = _make_ctx(
        headers={IDEMPOTENCY_KEY_HEADER: "k1"},
        idem_port=idem,
        body=_Body(name="a"),
    )

    async def handler(_c: HttpEndpointContext) -> dict[str, bool]:
        return {"ok": False}

    wrapped = feature.wrap(handler)

    out = await wrapped(ctx)

    assert isinstance(out, Response)
    assert out.status_code == 201
    assert out.body == b'{"cached":true}'
    assert out.media_type == "application/json"
    idem.commit.assert_not_called()


@pytest.mark.asyncio
async def test_idempotency_executes_and_commits_snapshot() -> None:
    feature = IdempotencyFeature(spec=IdempotencySpec(name="r"))
    idem = MagicMock()
    idem.begin = AsyncMock(return_value=None)
    idem.commit = AsyncMock()
    ctx = _make_ctx(
        headers={IDEMPOTENCY_KEY_HEADER: "k2"},
        idem_port=idem,
        body=_Body(name="b"),
        status_code=202,
    )

    async def handler(c: HttpEndpointContext) -> dict[str, bool]:
        return {"ran": True}

    wrapped = feature.wrap(handler)
    result = await wrapped(ctx)

    assert result == {"ran": True}
    idem.commit.assert_awaited_once()
    _op, _key, _hash, snap = idem.commit.await_args.args
    assert _op == "op.test"
    assert _key == "k2"
    assert snap["code"] == 202
    assert snap["content_type"] == "application/json"


@pytest.mark.asyncio
async def test_idempotency_handler_response_bypasses_commit() -> None:
    feature = IdempotencyFeature(spec=IdempotencySpec(name="r"))
    idem = MagicMock()
    idem.begin = AsyncMock(return_value=None)
    idem.commit = AsyncMock()
    ctx = _make_ctx(
        headers={IDEMPOTENCY_KEY_HEADER: "k3"},
        idem_port=idem,
        body=_Body(name="c"),
    )

    async def handler(c: HttpEndpointContext) -> Response:
        return Response(content=b"raw", media_type="text/plain")

    wrapped = feature.wrap(handler)
    out = await wrapped(ctx)

    assert isinstance(out, Response)
    idem.commit.assert_not_called()


@pytest.mark.asyncio
async def test_idempotency_commit_failure_still_returns_handler_result() -> None:
    feature = IdempotencyFeature(spec=IdempotencySpec(name="r"))
    idem = MagicMock()
    idem.begin = AsyncMock(return_value=None)
    idem.commit = AsyncMock(side_effect=RuntimeError("store down"))
    ctx = _make_ctx(
        headers={IDEMPOTENCY_KEY_HEADER: "k4"},
        idem_port=idem,
        body=_Body(name="d"),
    )

    async def handler(_c: HttpEndpointContext) -> dict[str, bool]:
        return {"still": True}

    wrapped = feature.wrap(handler)
    result = await wrapped(ctx)

    assert result == {"still": True}
