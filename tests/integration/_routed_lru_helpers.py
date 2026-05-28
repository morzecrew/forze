"""Helpers for routed-client LRU eviction integration tests (distinct dedup fingerprints)."""

from __future__ import annotations

from uuid import UUID


def postgres_dsns_for_lru_eviction(
    base: str,
    t1: UUID,
    t2: UUID,
    t3: UUID,
) -> dict[UUID, str]:
    """Return three DSNs to the same server with distinct LRU dedup fingerprints."""

    def _with_options(tid: UUID) -> str:
        sep = "&" if "?" in base else "?"
        return f"{base}{sep}options=-c%20application_name%3Dforze_{tid.hex[:12]}"

    return {t1: base, t2: _with_options(t2), t3: _with_options(t3)}


def s3_payloads_for_lru_eviction(
    endpoint: str,
    t1: UUID,
    t2: UUID,
    t3: UUID,
    *,
    base_payload: dict[str, str],
) -> dict[UUID, dict[str, str]]:
    """Distinct endpoint strings (same MinIO) for per-tenant S3 fingerprints."""

    ep = endpoint.rstrip("/")

    if "127.0.0.1" in ep:
        host_alt = ep.replace("127.0.0.1", "localhost")
    else:
        host_alt = ep.replace("localhost", "127.0.0.1")

    return {
        t1: {**base_payload, "endpoint": ep},
        t2: {**base_payload, "endpoint": host_alt},
        t3: {**base_payload, "endpoint": f"{ep}/"},
    }


def rabbitmq_dsns_for_lru_eviction(
    base: str,
    t1: UUID,
    t2: UUID,
    t3: UUID,
) -> dict[UUID, str]:
    """Distinct AMQP URLs (same broker) for per-tenant RabbitMQ fingerprints."""

    from urllib.parse import urlparse, urlunparse

    parsed = urlparse(base)
    host = parsed.hostname or "localhost"

    def _host_dsn(hostname: str) -> str:
        if not parsed.netloc:
            return base

        userinfo = ""

        if parsed.username:
            userinfo = parsed.username

            if parsed.password:
                userinfo = f"{userinfo}:{parsed.password}"

            userinfo = f"{userinfo}@"

        port = f":{parsed.port}" if parsed.port else ""
        netloc = f"{userinfo}{hostname}{port}"

        return urlunparse(parsed._replace(netloc=netloc))

    alt = "127.0.0.1" if host != "127.0.0.1" else "localhost"
    third = "[::1]" if host == "localhost" else "localhost"

    return {t1: base, t2: _host_dsn(alt), t3: _host_dsn(third)}


def sqs_payloads_for_lru_eviction(
    base: dict[str, str],
    t1: UUID,
    t2: UUID,
    t3: UUID,
) -> dict[UUID, dict[str, str]]:
    """Distinct endpoint strings for per-tenant SQS fingerprints."""

    ep = base["endpoint"].rstrip("/")

    return {
        t1: base,
        t2: {**base, "endpoint": f"{ep}?dedup={t2.hex[:8]}"},
        t3: {**base, "endpoint": f"{ep}?dedup={t3.hex[:8]}"},
    }


def temporal_hosts_for_lru_eviction(
    host_target: str,
    t1: UUID,
    t2: UUID,
    t3: UUID,
) -> dict[UUID, str]:
    """Distinct host strings (same Temporal frontend) for per-tenant fingerprints."""

    if "localhost" in host_target:
        return {
            t1: host_target,
            t2: host_target.replace("localhost", "127.0.0.1"),
            t3: host_target.replace("localhost", "127.0.0.1", 1) + f"?t={t3.hex[:8]}",
        }

    return {
        t1: host_target,
        t2: f"{host_target}?dedup={t2.hex[:8]}",
        t3: f"{host_target}?dedup={t3.hex[:8]}",
    }
