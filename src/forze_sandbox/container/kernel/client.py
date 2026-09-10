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

**Attach is the one call that leaves the HTTP client.** The daemon answers ``/attach`` with
a protocol upgrade, and an ordinary client has already sent the request body by then — which
the daemon discards, measurably and silently, leaving the child with an empty standard
input and the run with a wrong answer nobody is told about. So that one call is made over a
connection this module owns, writing the bytes after the upgrade and half-closing to end
them, which is what the ``docker`` client does.
"""

import asyncio
import contextlib
import json
import ssl
from collections.abc import AsyncIterator, Mapping
from typing import IO, Any, Final, Literal, cast, final

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

CLEARTEXT_CODE: Final = "sandbox_container_endpoint_cleartext"
"""Error code for an unencrypted daemon connection leaving this machine."""

_LOOPBACK: Final = frozenset({"localhost", "127.0.0.1", "::1", ""})
"""Hosts that are this machine, where cleartext never leaves it."""

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

    Cleartext to a daemon that is not on this machine is refused. Every command and every
    staged input crosses that connection, and the environment those inputs run under carries
    whatever secrets the request resolved — so an unencrypted hop to another host is the
    exfiltration path this plane spends the rest of its effort closing. Loopback is the
    local socket in another spelling and stays; anything further wants ``https://``.
    """

    if docker_host.startswith("unix://"):
        return httpx.AsyncHTTPTransport(uds=docker_host.removeprefix("unix://")), "http://daemon"

    for scheme in ("tcp://", "http://"):
        if docker_host.startswith(scheme):
            authority = docker_host.removeprefix(scheme).rstrip("/")
            _refuse_cleartext_to_another_host(docker_host, authority)

            return httpx.AsyncHTTPTransport(), "http://" + authority

    if docker_host.startswith("https://"):
        return httpx.AsyncHTTPTransport(), docker_host.rstrip("/")

    raise exc.configuration(
        f"Sandbox container route names a daemon at {docker_host!r}, which is not a "
        "unix://, tcp://, http:// or https:// endpoint.",
        code="sandbox_container_endpoint_invalid",
        details={"docker_host": docker_host},
    )


def split_authority(authority: str, default_port: int) -> tuple[str, int]:
    """``host`` and ``port`` from an authority, bracketed IPv6 included."""

    if authority.startswith("["):
        host, _, rest = authority.partition("]")

        return host.removeprefix("["), int(rest.removeprefix(":") or default_port)

    host, _, port = authority.partition(":")

    return host, int(port or default_port)


def _refuse_cleartext_to_another_host(docker_host: str, authority: str) -> None:
    """Refuse an unencrypted connection to a daemon that is not on this machine."""

    host, _ = split_authority(authority, 2375)

    if host in _LOOPBACK:
        return

    raise exc.configuration(
        f"Sandbox container route names a daemon at {docker_host!r}: an unencrypted "
        "connection to another host. Every command and every staged input crosses it, and "
        "the environment those inputs run under carries whatever secrets the request "
        "resolved. Name the daemon over https://, or over a unix socket if it is local.",
        code=CLEARTEXT_CODE,
        details={"docker_host": docker_host, "host": host},
    )


# ....................... #


async def _upgraded(reader: asyncio.StreamReader) -> None:
    """Read the daemon's answer to an upgrade request, and refuse anything but one.

    The status line and the headers have to come off the connection before the stream is
    the container's: leaving them there would put the daemon's own reply into the child's
    standard input.
    """

    status = (await reader.readline()).decode(errors="replace").strip()
    code = status.split(" ")[1] if len(status.split(" ")) > 1 else ""

    while True:
        line = await reader.readline()

        if line in (b"\r\n", b"\n", b""):
            break

    if code not in ("101", "200"):
        raise exc.infrastructure(
            f"The container daemon would not hand over the connection for standard input: "
            f"{status!r}.",
            code=DAEMON_ERROR_CODE,
            details={"status": status},
        )


def demultiplex(buffer: bytes) -> tuple[list[tuple[Literal["stdout", "stderr"], bytes]], bytes]:
    """Whole frames in *buffer*, and the bytes of the one still arriving.

    Split out of the read loop because it is the client's most delicate logic and the only
    part of it a daemon quirk decides: a frame is split across two reads whenever the writes
    line up that way, which is often enough to matter and rare enough that no test reliably
    produces one. As a function it can simply be handed the split.

    A frame this reader did not ask for — stream 0 is stdin — is consumed and dropped rather
    than yielded, because skipping the payload is what keeps the next header aligned.
    """

    frames: list[tuple[Literal["stdout", "stderr"], bytes]] = []

    while len(buffer) >= _FRAME_HEADER:
        size = int.from_bytes(buffer[4:_FRAME_HEADER], "big")

        if len(buffer) < _FRAME_HEADER + size:
            break

        kind = _FRAME_KIND.get(buffer[0])
        payload = buffer[_FRAME_HEADER : _FRAME_HEADER + size]
        buffer = buffer[_FRAME_HEADER + size :]

        if kind is not None:
            frames.append((kind, payload))

    return frames, buffer


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
        self._timeout = timeout
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
        self._expect(response, "stage the workspace", (200,))

    async def attach_stdin(self, container: str, data: bytes) -> None:
        """Hand the container its standard input over the hijacked connection, then end it.

        The one call that cannot go through the HTTP client, and the reason is measured
        rather than assumed: an ordinary client sends the request body **before** the
        protocol upgrade completes, and the daemon discards what arrives that early. The
        loss is silent — the child reads an empty stdin and the run succeeds with the wrong
        answer — which is the worst shape a failure can take on this plane. Over a raw
        connection the bytes go up *after* the upgrade, which is what the ``docker`` client
        itself does.

        Half-closed rather than closed: the daemon reads that as end of input and closes the
        container's stdin, which is what the child is waiting for.
        """

        reader, writer = await asyncio.wait_for(self._hijack(), timeout=self._timeout)

        try:
            writer.write(
                (
                    f"POST /containers/{container}/attach"
                    "?stream=1&stdin=1&stdout=0&stderr=0 HTTP/1.1\r\n"
                    "Host: daemon\r\n"
                    "Connection: Upgrade\r\n"
                    "Upgrade: tcp\r\n"
                    "Content-Length: 0\r\n\r\n"
                ).encode()
            )
            await writer.drain()
            await asyncio.wait_for(_upgraded(reader), timeout=self._timeout)
            writer.write(data)
            await writer.drain()
            writer.write_eof()

        finally:
            writer.close()

            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _hijack(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """A connection this client owns outright, for the one call that needs one.

        Every scheme :func:`endpoint` accepts is dialled here too. They disagreed once —
        ``https://`` was good enough for the other eight calls and not for this one — so a
        route configured that way booted clean and failed the first request that carried
        standard input.
        """

        if self._docker_host.startswith("unix://"):
            return await asyncio.open_unix_connection(self._docker_host.removeprefix("unix://"))

        for scheme, default, tls in (
            ("tcp://", 2375, False),
            ("http://", 2375, False),
            ("https://", 443, True),
        ):
            if self._docker_host.startswith(scheme):
                host, port = split_authority(
                    self._docker_host.removeprefix(scheme).rstrip("/"), default
                )

                if not tls:
                    return await asyncio.open_connection(host, port)

                # The hostname travels with the dial or the certificate is verified against
                # nothing, which is the whole of what TLS was for here.
                return await asyncio.open_connection(
                    host, port, ssl=ssl.create_default_context(), server_hostname=host
                )

        raise exc.configuration(
            f"Sandbox container route names its daemon at {self._docker_host!r}, and a "
            "request carrying stdin needs a connection this adapter can take over. Drop the "
            "request's stdin, or name the daemon over a unix socket, tcp or https.",
            code="sandbox_container_stdin_unavailable",
            details={"docker_host": self._docker_host},
        )

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
                complete, buffer = demultiplex(buffer + chunk)

                for frame in complete:
                    yield frame

    async def wait(self, container: str) -> int:
        """Block until the container exits and return its status."""

        response = await self._call("POST", f"/containers/{container}/wait")
        self._expect(response, "wait for the container", (200,))

        return int(response.json()["StatusCode"])

    async def inspect(self, container: str) -> Mapping[str, object]:
        """The container's state, which is where the daemon says what ended it."""

        response = await self._call("GET", f"/containers/{container}/json")
        self._expect(response, "inspect the container", (200,))
        payload = _object(response.json())

        return _object(payload.get("State")) or {} if payload is not None else {}

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


def _object(value: object) -> Mapping[str, object] | None:
    """*value* as a JSON object, or nothing when it is not one.

    The one place this module narrows what the daemon sent. ``httpx`` types ``json()`` as
    ``Any``, which would otherwise spread untyped through every reader of a reply; here it
    becomes a mapping of strings to values each caller has to narrow for itself. The cast is
    what a JSON object *is* — decoded keys are strings — rather than a claim about the
    values, which stay ``object``.
    """

    return cast("Mapping[str, object]", value) if isinstance(value, dict) else None


def _message(response: httpx.Response) -> str:
    """The daemon's own explanation, or its body when it did not give one."""

    try:
        payload = _object(response.json())

    except (json.JSONDecodeError, ValueError):
        return response.text.strip()[:500]

    if payload is not None:
        said = payload.get("message")

        if isinstance(said, str):
            return said

    return response.text.strip()[:500]
