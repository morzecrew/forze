"""Shared harness for starting a Forze-backed Inngest app in integration tests."""

import os
import socket
import threading
import time
from collections.abc import Mapping
from typing import Any, Sequence

import attrs
import httpx
import uvicorn
from fastapi import FastAPI

from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.context import ExecutionContextFactory
from forze_inngest import InngestFunctionBinding
from forze_inngest.execution.deps import InngestDepsModule
from forze_inngest.execution.deps.configs import InngestEventConfig
from forze_inngest.fastapi import serve
from forze_inngest.kernel.client import InngestClient, InngestClientPort
from tests.support.execution_context import context_from_deps

from .inngest_dev_server import InngestDevTarget

# ----------------------- #


def free_tcp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("0.0.0.0", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


# ....................... #


@attrs.define(kw_only=True)
class InngestAppHarness:
    """Running FastAPI app registered with a dev server."""

    client: InngestClientPort
    deps: Deps
    ctx_factory: ExecutionContextFactory
    port: int
    outcomes: list[Any] = attrs.field(factory=list)
    server: uvicorn.Server = attrs.field(repr=False)
    thread: threading.Thread = attrs.field(repr=False)

    @property
    def serve_origin(self) -> str:
        return f"http://host.docker.internal:{self.port}"

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=10)


def sync_with_dev_server(
    target: InngestDevTarget,
    *,
    port: int,
    timeout_sec: float = 15.0,
) -> httpx.Response:
    """Register the app with the dev server (``PUT /api/inngest``)."""

    _ = target
    with httpx.Client(timeout=timeout_sec) as http:
        return http.put(f"http://127.0.0.1:{port}/api/inngest")


def _wait_for_app_ready(port: int, *, timeout_sec: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_sec

    with httpx.Client(timeout=1.0) as http:
        while time.monotonic() < deadline:
            try:
                http.get(f"http://127.0.0.1:{port}/openapi.json")
                return

            except httpx.HTTPError:
                time.sleep(0.1)

    raise TimeoutError(f"uvicorn app on port {port} did not become ready")


def start_forze_inngest_app(
    target: InngestDevTarget,
    *,
    bindings: Sequence[InngestFunctionBinding[Any, Any]],
    events: Mapping[str, InngestEventConfig] | None = None,
    app_id: str = "forze-it",
    extra_plain_deps: dict[Any, Any] | None = None,
) -> InngestAppHarness:
    """Start uvicorn + Forze Inngest serve and sync to the dev server."""

    port = free_tcp_port()
    os.environ["INNGEST_SERVE_ORIGIN"] = f"http://host.docker.internal:{port}"

    from forze_inngest import InngestConfig

    client = InngestClient(
        app_id=app_id,
        config=InngestConfig(is_production=False),
    )
    module = InngestDepsModule(
        client=client,
        events=events,
        function_bindings=list(bindings),
    )
    deps = Deps.plain(extra_plain_deps or {}).merge(module())

    def ctx_factory() -> ExecutionContext:
        return context_from_deps(deps)

    app = FastAPI()
    serve(app, client, bindings, ctx_factory=ctx_factory)

    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=port,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    try:
        _wait_for_app_ready(port)
        response = sync_with_dev_server(target, port=port)

        if response.status_code != 200:
            raise RuntimeError(
                f"Inngest sync failed ({response.status_code}): {response.text}",
            )

    except Exception:
        server.should_exit = True
        thread.join(timeout=5)
        raise

    return InngestAppHarness(
        client=client,
        deps=deps,
        ctx_factory=ctx_factory,
        port=port,
        server=server,
        thread=thread,
    )


async def wait_for_outcome(
    outcomes: list[Any],
    *,
    timeout_sec: float = 30.0,
    poll_interval_sec: float = 0.25,
) -> Any:
    """Poll until ``outcomes`` is non-empty or timeout."""

    import asyncio

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_sec

    while loop.time() < deadline:
        if outcomes:
            return outcomes[-1]

        await asyncio.sleep(poll_interval_sec)

    raise TimeoutError(
        f"timed out waiting for Inngest function outcome (got {outcomes!r})"
    )
