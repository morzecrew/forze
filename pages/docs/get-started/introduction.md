---
title: Introduction
icon: lucide/lightbulb
summary: Why Forze exists, who it helps, and when to use it
---

## What is Forze?

Forze is a Python toolkit for building backend services with clear boundaries between business logic and infrastructure. It implements Domain-Driven Design (DDD) and Hexagonal Architecture patterns, letting you swap databases, caches, or queues without touching your business code.

## Who should use Forze?

Forze fits backend developers who:

- Build services where business rules are central, not secondary
- Want infrastructure decisions to stay reversible
- Need consistent patterns across teams
- Value testability without complex mocking

## When Forze helps

| Situation | Why Forze fits |
|-----------|----------------|
| Rapid prototyping | In-memory mock adapters run with no Docker and no wiring, so you build the real operation flow in minutes and add infrastructure later |
| Domain-rich applications | Aggregates, events, and sagas keep complex business logic organized |
| Evolving infrastructure | Swap Postgres for Mongo, Redis for Memcached, without rewriting handlers |
| Growing teams | Shared contracts and layers reduce guesswork about where code belongs |
| Test-first workflows | Mock adapters run in-memory; domain logic tests without containers |

## From prototype to production

Forze isn't only for "large" projects. Because every capability is a pluggable port, it scales *down* as readily as up. Start a spike against in-memory mock adapters — no database, no Docker, no config — and you already have the real operation flow. When the idea sticks, point the same handlers at Postgres, Redis, or S3 by changing wiring, not business code. You don't pay for infrastructure you haven't added yet, so composability is what makes the fast start possible *and* keeps it from becoming throwaway.

## When you might not need Forze

Forze adds a thin layer of structure. For a throwaway single-file script, or a one-endpoint passthrough with no logic beyond validation, plain FastAPI or Flask is the shorter path. Reach for Forze the moment you have operations worth keeping clean — which, with mock adapters, can be on day one.

## Core ideas in one minute

1. **Layers** — Domain (business rules), Application (operations), Infrastructure (adapters), Interface (routes). Dependencies point inward: domain never imports infrastructure.

2. **Ports and adapters** — Your handlers ask for capabilities ("document storage"), not implementations ("Postgres"). The runtime injects the wired adapter.

3. **Specifications** — A logical name (`"users"`) ties a model to its operations and adapters. Change the adapter, not the spec.

4. **Execution context** — The seam where ports resolve to adapters. Every handler receives it; none learn which database they use.

These ideas come from established patterns. If you've seen Hexagonal Architecture or Clean Architecture elsewhere, Forze applies them to Python backends.

## Prerequisites

Before diving in, you should be comfortable with:

- **Python 3.13+** and async/await
- **Pydantic** models and validation
- **FastAPI** basics (or another async framework)

Familiarity with DDD terminology (aggregates, domain events, bounded contexts) helps but isn't required. The [Core Concepts](../core-concepts/overview.md) section explains each term as you encounter it.

## Next steps

<div class="grid cards" markdown>

-   :lucide-download: **[Installation](installation.md)**

    ---

    Set up your environment and install Forze.

-   :lucide-zap: **[Quickstart](quickstart.md)**

    ---

    Build a working service in ten minutes.

</div>
