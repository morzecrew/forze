---
title: Installation
summary: Install the Forze core package and integrations
external_links:
    pypi: https://pypi.org/project/forze/
---

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) or another PEP 517-compatible package manager

## Install core package

/// tab | uv

    :::bash
    uv add forze
///

/// tab | pip

    :::bash
    pip install forze
///

## Install with integrations

Install only what you need. Extras map directly to integration packages:

| Extra | Installs |
|-------|----------|
| `fastapi` | FastAPI integration (`forze_fastapi`) |
| `postgres` | Postgres integration (`forze_postgres`) |
| `redis` | Redis/Valkey integration (`forze_redis`) |
| `s3` | S3-compatible integration (`forze_s3`) |
| `mongo` | MongoDB integration (`forze_mongo`) |
| `temporal` | Temporal integration (`forze_temporal`) |
| `socketio` | Socket.IO integration (`forze_socketio`) |
| `sqs` | SQS integration (`forze_sqs`) |
| `rabbitmq` | RabbitMQ integration (`forze_rabbitmq`) |
| `authn` | First-party `forze_authn` (Argon2, PyJWT, email-validator) |
| `authz` | Empty extra (symmetry / future-proofing); `forze_authz` ships in the main wheel with `forze` |
| `oidc` | Generic OIDC verifier (`forze_oidc`, PyJWT with crypto) for external IdPs |

The PyPI wheel includes `forze_authn`, `forze_authz`, `forze_oidc`, and `forze_tenancy` without selecting those extras; extras only add **transitive** dependencies (for example `authn` pulls Argon2 and validators for password flows).

`forze_mock` is bundled with the core package and requires no extra. It provides in-memory adapters for development and testing without external infrastructure.

Common setup:

/// tab | uv

    :::bash
    uv add 'forze[fastapi,postgres,redis,s3,mongo,temporal,socketio]'
///

/// tab | pip

    :::bash
    pip install 'forze[fastapi,postgres,redis,s3,mongo,temporal,socketio]'
///

## Quick sanity check

    :::python
    from forze.application.execution import ExecutionRuntime

    runtime = ExecutionRuntime()
    print(type(runtime).__name__)  # ExecutionRuntime

## Development Installation

To install the project for local development:

    :::bash
    git clone https://github.com/morzecrew/forze
    cd forze
    uv sync --all-groups --all-extras

This installs test, quality, docs, and integration extras.

Then use [just](https://github.com/casey/just) to run necessary commands:

    :::bash
    # Run unit and integration tests
    just test

    # Run performance tests
    just perf

    # Run all quality checks
    just quality

    # Run all quality checks in strict mode (fail on any issue)
    just quality -s

    # Serve documentation
    just pages serve

    # Build diagrams
    just pages diagrams
