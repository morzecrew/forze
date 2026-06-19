---
title: Overview
icon: lucide/compass
summary: The mental model behind Forze — what it separates, and the vocabulary you'll meet
---

This page is the mental model the rest of Learn builds on — what Forze keeps
separate, why, and the vocabulary you'll meet throughout.

## Background: DDD and Hexagonal Architecture

Forze builds on two established architectural patterns. You don't need deep expertise in either — but knowing the basics helps you understand why Forze works the way it does.

**Domain-Driven Design (DDD)** puts business concepts at the center of software development. Instead of modeling your app around database tables or API endpoints, you model it around the domain — the real-world problem you're solving. Key ideas:

- **Aggregates** — clusters of related objects treated as a unit for data changes
- **Domain events** — facts about things that happened in the business domain
- **Bounded contexts** — clear boundaries around parts of the system with their own models

**Hexagonal Architecture** (also called Ports and Adapters) separates core logic from external systems. The core defines "ports" — abstract interfaces for capabilities it needs — and "adapters" implement those ports for specific technologies. This means:

- Business logic never imports database drivers, HTTP clients, or queue libraries
- You can swap implementations (Postgres → Mongo, Redis → Memcached) without changing handlers
- Testing uses in-memory adapters instead of spinning up containers

Forze combines these patterns into a practical toolkit for Python backends. The rest of this page shows how.

---

Forze splits a backend into two halves and keeps them honest:

- **What your app does** — the business rules and the operations that run them.
- **How it's done** — the databases, caches, queues, and APIs that carry it out.

The two halves meet at a single seam: the **execution context**. Your
operations ask it for a capability ("give me document storage for `users`");
it hands back whatever adapter was wired in. The operation never learns
whether that's Postgres, Mongo, or an in-memory fake.

!!! abstract "The big idea"

    Business logic depends on **capabilities**, never on **implementations**.
    Swap the implementation — Postgres for Mongo, a real queue for a fake —
    and the business logic doesn't change, because it only ever talked to the
    seam.

![Forze separates what your app does from how it's done, joined at the execution context](../_diagrams/light/concept-overview.svg#only-light){ data-src="../_diagrams/light/concept-overview.svg#only-light" }
![Forze separates what your app does from how it's done, joined at the execution context](../_diagrams/dark/concept-overview.svg#only-dark){ data-src="../_diagrams/dark/concept-overview.svg#only-dark" }

## The vocabulary

Six nouns carry most of Forze. You'll meet each one in depth over the next
pages — this is the map:

| Term | What it is | Lives in |
|------|------------|----------|
| **Aggregate** | A business entity with identity, versioning, and rules — you subclass `Document` (and `AggregateRoot` for domain events) | Domain |
| **Specification** | The logical name (`"users"`) binding a model to its operations and adapters — `Spec` in code | Application |
| **Operation** | One business action you run — create a user, list orders | Application |
| **Operation registry** | The frozen catalog mapping each name to its operation and hooks | Application |
| **Port** | A contract describing a capability the app needs — storage, cache, search | Application |
| **Adapter** | A concrete implementation of a port for one backend | Infrastructure |

The **execution context** is the runtime object that resolves a port to its
adapter on demand — the seam in the diagram above.

## How a request flows

The nouns click together when you trace one request end to end:

1. A request arrives at the interface (an HTTP route, say) and names an
   **operation** — `users.create`.
2. The route resolves that operation from the frozen **registry** and runs it.
3. The operation asks the **execution context** for the capabilities it needs —
   document storage for the `users` **specification**.
4. The context resolves the matching **port** to the **adapter** that was wired
   in — Postgres in production, an in-memory fake in tests.
5. It applies **domain** rules (validating the new `User` **aggregate**) and
   writes through the adapter.
6. A **read model** goes back out as the response.

Every arrow in that chain crosses a boundary the architecture enforces — and
none of them require the handler to import a database driver.

## Your path through Core Concepts

Read these in order; each one builds on the last.

<div class="grid cards" markdown>

-   :lucide-layers: **[Architecture](architecture.md)**

    ---

    The four layers and the one rule that keeps them honest: dependencies
    point inward.

-   :lucide-box: **[Domain](domain-layer.md)**

    ---

    Aggregates, commands, and read models — the business entities you model
    first.

-   :lucide-workflow: **[Application](application-layer.md)**

    ---

    Specifications, operations, and the registry — how an action is defined,
    handled, and composed.

-   :lucide-plug: **[Contracts & adapters](contracts.md)**

    ---

    Ports describe capabilities, adapters implement them — the seam, up close.

-   :lucide-cog: **[Runtime](runtime.md)**

    ---

    The execution context, lifecycle, and transactions that run it all.

</div>
