# Aggregate Specification

For document-like aggregates, a **specification** binds together what adapters need to configure themselves. You define the spec once; switching adapters means swapping the implementation, not rewriting the spec.

## Spec Elements

| Element | Purpose |
|---------|---------|
| **Namespace** | Cache key prefix; used to isolate keys per aggregate type |
| **Storage relations** | Read/write/history relation names (tables, views) |
| **Model types** | Read model, domain model, create command, update command |
| **Features** | Search config, soft delete, caching |

## Document Spec Structure

A document spec typically includes:

- **Namespace** — string used as a prefix for cache keys
- **Sources** — relation names for read, write, and optional history
- **Models** — concrete classes for read, domain, create command, update command
- **Cache** — optional TTL and enable flag

## Why It Matters

- **Single source of truth** — the spec describes the aggregate for all adapters
- **Adapter-agnostic** — storage, cache, and search adapters read the spec and configure themselves
- **Consistent configuration** — no duplicated table names or model references across modules
