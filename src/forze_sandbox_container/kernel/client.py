"""Exactly the Docker Engine API calls one governed run needs, and nothing else.

Written against the Engine API rather than a client library because the surface is nine
endpoints and the library would be a dependency carrying a synchronous transport, a
connection model and a release cadence for the sake of them. Podman publishes the same API
on its own socket, so the config names an endpoint rather than a vendor.

Two shapes are worth knowing before reading anything below:

**The multiplexed stream.** With no TTY, ``/logs`` frames every chunk with eight bytes —
one for the stream it came from, three unused, four for the payload length, big-endian.
A reader that assumes a frame arrives whole loses output the moment a chunk is split across
two reads, so :meth:`ContainerEngine.follow` reassembles from a running buffer.

**Attach as the way in for stdin.** The daemon answers ``/attach`` with a protocol upgrade,
which no ordinary HTTP client can then read from. It does not need to: the contract's stdin
is a fixed string of bytes rather than an interactive session, so the bytes go up as the
request body and the connection closing is what the child sees as end of input.
"""

import json
from collections.abc import AsyncIterator
from typing import IO, Any, Final, Literal, final

import httpx

from forze.base.exceptions import exc

# ----------------------- #

DEFAULT_DOCKER_HOST: Final = "unix:///var/run/docker.sock"
"""Where a Docker or Podman daemon listens unless a route says otherwise."""

DAEMON_ERROR_CODE: Final = "sandbox_container_daemon_error"
"""Error code for a daemon that answered something this adapter cannot act on."""

CONTAINER_NAME_PREFIX: Final = "forze-sandbox-"
"""Prefix every container this adapter creates carries, so a host can be read."""

_FRAME_HEADER: Final = 8
"""Bytes of stream framing before each payload on a non-TTY log stream."""

_FRAME_KIND: Final[dict[int, Literal["stdout", "stderr"]]] = {1: "stdout", 2: "stderr"}
"""Frame's first byte to the stream it came from. Zero is stdin, which never comes back."""

_DOWNLOAD_CHUNK: Final = 256 * 1024
"""Bytes per read while draining an archive to disk."""


# ....................... #


@final
class ContainerNotCreated(Exception):
    """The daemon refused to create the container, so the run never started.

    Carried as an exception rather than a status because every caller does the same thing
    with it — turn it into a ``spawn_failed`` result — and the alternative is an optional
    return value that every other call site has to remember to check.
    """

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


# ....................... #


def endpoint(docker_host: str) -> tuple[httpx.AsyncBaseTransport, str]:
    """Transport and base URL for *docker_host*.

    ``unix://`` is the ordinary case and the only one that needs a transport of its own; the
    host part of the URL is then a formality the daemon never reads, so it is a fixed
    placeholder rather than something a route could get subtly wrong.
    """

    if docker_host.startswith("unix://"):
        return httpx.AsyncHTTPTransport(uds=docker_host.removeprefix("unix://")), "http://daemon"

    if docker_host.startswith("tcp://"):
        return httpx.AsyncHTTPTransport(), "http://" + docker_host.removeprefix("tcp://").rstrip(
            "/"
        )

    if docker_host.startswith(("http://", "https://")):
        return httpx.AsyncHTTPTransport(), docker_host.rstrip("/")

    raise exc.configuration(
        f"Sandbox container route names a daemon at {docker_host!r}, which is not a "
        "unix://, tcp://, http:// or https:// endpoint.",
        code="sandbox_container_endpoint_invalid",
        details={"docker_host": docker_host},
    )


# ....................... #


@final
class ContainerEngine:
    """One connection to a container daemon, for the length of one run.

    Deliberately per-run rather than pooled behind a lifecycle step: a run makes about seven
    calls over a unix socket and then spends seconds or minutes inside a container, so the
    connection setup is noise beside the work. A route doing many very short runs is what
    would change that, and it would change it by adding a pool here rather than by anything
    a caller writes.
    """

    def __init__(self, docker_host: str, *, timeout: float) -> None:
        transport, base_url = endpoint(docker_host)
        self._docker_host = docker_host
        self._client = httpx.AsyncClient(
            transport=transport,
            base_url=base_url,
            # No total timeout: the run's own budget is the deadline, and a second one here
            # would cut a legitimately long container off at a number nobody chose.
            timeout=httpx.Timeout(None, connect=timeout),
        )

    # ....................... #

    async def aclose(self) -> None:
        await self._client.aclose()

    # ....................... #

    async def create(self, body: dict[str, Any], *, name: str) -> str:
        """Create a container under *name*, or say why the run never started.

        Named rather than left to the daemon's word pairs so that an operator looking at a
        busy host can tell which containers are this plane's, and so a leak check can count
        them without counting everything else running there.
        """

        response = await self._call("POST", "/containers/create", params={"name": name}, json=body)

        if response.status_code in (400, 404, 409):
            raise ContainerNotCreated(_message(response))

        self._expect(response, "create a container", (201,))

        return str(response.json()["Id"])

    async def put_archive(self, container: str, path: str, data: bytes) -> None:
        """Extract *data*, a tar stream, into the container's filesystem at *path*."""

        response = await self._call(
            "PUT", f"/containers/{container}/archive", params={"path": path}, content=data
        )

        if response.status_code in (400, 403):
            raise ContainerNotCreated(f"staging the workspace: {_message(response)}")

        self._expect(response, "stage the workspace", (200,))

    async def attach_stdin(self, container: str, data: bytes) -> None:
        """Hand the container its standard input, then close it.

        The response is a protocol upgrade this client never reads: the bytes have already
        gone up as the request body by the time the daemon answers, and closing the
        connection is what the child observes as end of input.
        """

        async with self._client.stream(
            "POST",
            f"/containers/{container}/attach",
            params={"stream": "1", "stdin": "1", "stdout": "0", "stderr": "0"},
            content=data,
            headers={"Connection": "Upgrade", "Upgrade": "tcp"},
        ) as response:
            if response.status_code not in (101, 200):
                # A container that ignored its stdin and exited first is not an error: the
                # bytes had nowhere to go and the run already happened.
                await response.aread()
                self._expect(response, "attach to the container", (101, 200, 400, 404, 409))

    async def start(self, container: str) -> None:
        """Start a created container."""

        response = await self._call("POST", f"/containers/{container}/start")
        self._expect(response, "start the container", (204, 304))

    async def follow(
        self, container: str
    ) -> AsyncIterator[tuple[Literal["stdout", "stderr"], bytes]]:
        """Yield the container's output as it arrives, demultiplexed, until it exits."""

        async with self._client.stream(
            "GET",
            f"/containers/{container}/logs",
            params={"follow": "1", "stdout": "1", "stderr": "1"},
        ) as response:
            if response.status_code != 200:
                await response.aread()
                self._expect(response, "follow the container's output", (200,))

            buffer = b""

            async for chunk in response.aiter_bytes():
                buffer += chunk

                while len(buffer) >= _FRAME_HEADER:
                    size = int.from_bytes(buffer[4:_FRAME_HEADER], "big")

                    if len(buffer) < _FRAME_HEADER + size:
                        break

                    kind = _FRAME_KIND.get(buffer[0])
                    payload = buffer[_FRAME_HEADER : _FRAME_HEADER + size]
                    buffer = buffer[_FRAME_HEADER + size :]

                    if kind is not None:
                        yield kind, payload

    async def wait(self, container: str) -> int:
        """Block until the container exits and return its status."""

        response = await self._call("POST", f"/containers/{container}/wait")
        self._expect(response, "wait for the container", (200,))

        return int(response.json()["StatusCode"])

    async def inspect(self, container: str) -> dict[str, Any]:
        """The container's state, which is where the daemon says what ended it."""

        response = await self._call("GET", f"/containers/{container}/json")
        self._expect(response, "inspect the container", (200,))
        state = response.json().get("State")

        return state if isinstance(state, dict) else {}

    async def signal(self, container: str, name: str) -> None:
        """Send one signal, tolerating a container that has already gone.

        A 404 or a 409 here is the race this method exists inside: the run being killed on
        its deadline is exactly the run most likely to have exited a moment earlier, and
        raising would replace a real outcome with an error about tidying up after it.
        """

        response = await self._call(
            "POST", f"/containers/{container}/kill", params={"signal": name}
        )
        self._expect(response, "signal the container", (204, 404, 409))

    async def remove(self, container: str) -> None:
        """Remove the container and everything it wrote, force-killing it if it still runs."""

        response = await self._call(
            "DELETE", f"/containers/{container}", params={"force": "1", "v": "1"}
        )
        self._expect(response, "remove the container", (204, 404, 409))

    async def download(self, container: str, path: str, sink: IO[bytes], cap: int) -> bool:
        """Drain the tar of *path* into *sink*, stopping past *cap* bytes.

        Returns whether the whole archive arrived. The workspace holds everything the child
        wrote, declared or not, so this is the one call whose size the request does not
        bound — the cap is what stops an undeclared file the caller never asked for from
        spending the worker's disk on the way to being discarded.
        """

        written = 0

        async with self._client.stream(
            "GET", f"/containers/{container}/archive", params={"path": path}
        ) as response:
            if response.status_code == 404:
                # The workspace is gone, which is what a container that never started looks
                # like from here. Nothing was collected and nothing was lost.
                await response.aread()

                return True

            if response.status_code != 200:
                await response.aread()
                self._expect(response, "collect the workspace", (200,))

            async for chunk in response.aiter_bytes(_DOWNLOAD_CHUNK):
                written += len(chunk)

                if written > cap:
                    return False

                sink.write(chunk)

        return True

    # ....................... #

    async def _call(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        try:
            return await self._client.request(method, url, **kwargs)

        except httpx.HTTPError as error:
            raise exc.infrastructure(
                f"Could not reach the container daemon at {self._docker_host!r}: {error}. "
                "The sandbox route cannot run anything without it.",
                code=DAEMON_ERROR_CODE,
                details={"docker_host": self._docker_host, "method": method, "url": url},
            ) from error

    def _expect(self, response: httpx.Response, doing: str, allowed: tuple[int, ...]) -> None:
        if response.status_code in allowed:
            return

        raise exc.infrastructure(
            f"The container daemon at {self._docker_host!r} refused to {doing}: "
            f"{response.status_code} {_message(response)}",
            code=DAEMON_ERROR_CODE,
            details={"docker_host": self._docker_host, "status": response.status_code},
        )


# ----------------------- #


def _message(response: httpx.Response) -> str:
    """The daemon's own explanation, or its body when it did not give one."""

    try:
        payload = response.json()

    except (json.JSONDecodeError, ValueError):
        return response.text.strip()[:500]

    if isinstance(payload, dict) and isinstance(payload.get("message"), str):
        return str(payload["message"])

    return response.text.strip()[:500]
