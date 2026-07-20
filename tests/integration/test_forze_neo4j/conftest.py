"""Fixtures for forze_neo4j integration tests."""

import hashlib
import json
import os
import shutil
import tempfile
import urllib.request
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
import pytest_asyncio

pytest.importorskip("neo4j")
pytest.importorskip("testcontainers.neo4j")

from testcontainers.neo4j import Neo4jContainer

from forze_neo4j.kernel.client import Neo4jClient

_NEO4J_IMAGE = "neo4j:5.26"
_GDS_VERSIONS_URL = "https://graphdatascience.ninja/versions.json"


def _ensure_docker() -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for Neo4j integration tests")


def _resolve_gds_jar() -> Path | None:
    """Locate (or download once) a GDS jar compatible with the test image.

    GDS is an optional heavy plugin the test container can't auto-download (no in-container
    egress), so we cache it host-side. Returns ``None`` when it can't be obtained (offline /
    no cache) — GDS-dependent tests then skip rather than fail.
    """

    env = os.environ.get("FORZE_GDS_JAR")
    if env and Path(env).is_file():
        return Path(env)

    cache = Path(tempfile.gettempdir()) / "forze-gds-cache"
    cache.mkdir(exist_ok=True)
    cached = sorted(cache.glob("gds-*.jar"))
    if cached:
        return cached[0]

    try:
        with urllib.request.urlopen(_GDS_VERSIONS_URL, timeout=30) as resp:
            versions = json.load(resp)
        entry = next(
            e for e in versions if str(e.get("neo4j", "")).startswith("5.26")
        )
        url = entry.get("downloadUrl") or entry.get("jar")
        target = cache / "gds-5.26.jar"
        # Download to a temp path and verify integrity before promoting atomically, so a
        # truncated / HTML-error-page download never lands at the cached path and gets reused by
        # every GDS test. Prefer the source-provided checksum; fall back to structural validation.
        tmp = cache / "gds-5.26.jar.part"
        urllib.request.urlretrieve(url, tmp)
        expected = entry.get("sha256") or entry.get("checksum")
        payload = tmp.read_bytes()
        checksum_ok = (
            hashlib.sha256(payload).hexdigest().lower() == str(expected).lower()
            if expected
            else True
        )
        if not checksum_ok or len(payload) < 1_000_000 or payload[:4] != b"PK\x03\x04":
            tmp.unlink(missing_ok=True)
            return None
        os.replace(tmp, target)
        return target
    except Exception:
        return None


@pytest.fixture(scope="session")
def neo4j_container() -> Iterator[Neo4jContainer]:
    """Start a Neo4j container for the test session."""

    _ensure_docker()

    with Neo4jContainer(image=_NEO4J_IMAGE) as container:
        yield container


@pytest_asyncio.fixture
async def neo4j_client(neo4j_container: Neo4jContainer) -> AsyncIterator[Neo4jClient]:
    """Provide an initialized client; wipe the database before each test."""

    client = Neo4jClient()
    await client.initialize(
        neo4j_container.get_connection_url(),
        auth=(neo4j_container.username, neo4j_container.password),
    )

    await _reset_database(client)

    yield client

    await client.close()


@pytest.fixture(scope="session")
def gds_neo4j_container() -> Iterator[Neo4jContainer]:
    """Neo4j with the GDS plugin mounted (skips if the jar can't be obtained)."""

    _ensure_docker()

    jar = _resolve_gds_jar()
    if jar is None:
        pytest.skip("Neo4j GDS plugin unavailable (offline / no cached jar)")

    plugins = Path(tempfile.mkdtemp(prefix="forze-gds-plugins-"))
    shutil.copy(jar, plugins / "gds.jar")
    # The neo4j container user (uid 7474) must be able to read the mount, but mkdtemp is 0700.
    os.chmod(plugins, 0o755)
    os.chmod(plugins / "gds.jar", 0o644)

    container = (
        Neo4jContainer(image=_NEO4J_IMAGE)
        .with_volume_mapping(str(plugins), "/plugins")
        .with_env("NEO4J_dbms_security_procedures_unrestricted", "gds.*")
        .with_env("NEO4J_dbms_security_procedures_allowlist", "gds.*")
    )

    try:
        with container as running:
            yield running
    finally:
        # Remove the copied gds.jar and its temp parent dir once the session ends.
        shutil.rmtree(plugins, ignore_errors=True)


@pytest_asyncio.fixture
async def gds_neo4j_client(
    gds_neo4j_container: Neo4jContainer,
) -> AsyncIterator[Neo4jClient]:
    """An initialized client against the GDS-enabled container, reset before each test."""

    client = Neo4jClient()
    await client.initialize(
        gds_neo4j_container.get_connection_url(),
        auth=(gds_neo4j_container.username, gds_neo4j_container.password),
    )

    await _reset_database(client)

    yield client

    await client.close()


async def _reset_database(client: Neo4jClient) -> None:
    """Wipe data *and* schema so provisioning tests don't leak constraints/indexes."""

    await client.run("MATCH (n) DETACH DELETE n")

    for row in await client.run("SHOW CONSTRAINTS YIELD name RETURN name"):
        await client.run(f"DROP CONSTRAINT `{row['name']}` IF EXISTS")

    # Drop constraints first (removes their backing indexes); then any standalone index,
    # skipping the built-in token-lookup indexes which cannot be dropped by name.
    for row in await client.run("SHOW INDEXES YIELD name, type RETURN name, type"):
        if row["type"] != "LOOKUP":
            await client.run(f"DROP INDEX `{row['name']}` IF EXISTS")
