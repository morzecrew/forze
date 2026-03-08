# Contracts and Adapters

Forze follows **hexagonal architecture** (ports and adapters). **Contracts** (also called ports) are interfaces defined by the application. **Adapters** are implementations provided by infrastructure. The application depends on contracts, not adapters.

<div class="d2-diagram">
  <img class="d2-light" src="../../assets/diagrams/light/contracts-adapters.svg" alt="Contracts and adapters">
  <img class="d2-dark" src="../../assets/diagrams/dark/contracts-adapters.svg" alt="Contracts and adapters">
</div>

## How It Works

1. The application defines **contracts** — protocol interfaces that describe required capabilities
2. Infrastructure provides **adapters** — concrete implementations (Postgres, Redis, S3, etc.)
3. The **dependency plan** wires adapters to contracts at startup
4. Operations resolve contracts from the execution context; they never import adapters

Switching from Postgres to Mongo means changing the plan, not the operations.

## Available Contracts

| Contract | Purpose |
|----------|---------|
| **Document storage** | Read, write, search for document aggregates (split into read/write/search ports) |
| **Transaction manager** | Begin, commit, rollback; scoped ports participate in the active transaction |
| **Document cache** | Optional caching for document read models |
| **Blob storage** | Store and retrieve files (S3-style) |
| **Counters** | Distributed increment (e.g. sequence numbers) |
| **Idempotency** | Track and deduplicate requests |
| **Streams** | Publish and consume events |
| **Workflows** | Orchestrate long-running processes (e.g. Temporal) |
| **Tenant context** | Ambient tenant identity for multi-tenant routing |
| **Actor context** | Ambient actor identity for audit and creator injection |

## Contract-oriented code example

In application code, resolve only contracts:

    :::python
    doc_port = ctx.doc(project_spec)
    search_port = ctx.search(project_search_spec)
    storage_port = ctx.storage("app-assets")

In infrastructure composition, wire adapters once:

    :::python
    deps = Deps.merge(
        PostgresDepsModule(
            client=pg_client,
            rev_bump_strategy="database",
            history_write_strategy="database",
        )(),
        RedisDepsModule(client=redis_client)(),
        S3DepsModule(client=s3_client)(),
    )

## Testing

Tests stub contracts with in-memory or fake implementations. Business logic is exercised without real databases or external services. Integration tests wire real adapters when needed.
