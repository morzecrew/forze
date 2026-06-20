---
title: Introduction
icon: lucide/lightbulb
summary: Why Forze exists, who it helps, and when to use it
---

You know the failure mode: a database call buried three layers deep in a
business rule, a unit test that won't run without a live Redis, a "quick
prototype" you throw away because the infrastructure grew into the logic. The
part that's actually yours — the business rules — ends up welded to the
machinery that serves them, and every infrastructure choice you made early
hardens into one you can't walk back.

## What is Forze?

Forze is a Python toolkit that keeps those two apart. Your code asks for a
**capability** — *store this document*, *publish this event* — never a vendor,
and the runtime supplies the adapter behind it. Start against in-memory adapters
with no database and no Docker; when the idea sticks, point the same handlers at
Postgres, Redis, or S3 by changing wiring, not code. Infrastructure decisions
stay reversible, and the code that matters never learns which datastore it's
talking to.

Underneath are two well-worn ideas: [Domain-Driven Design](https://martinfowler.com/bliki/DomainDrivenDesign.html "Martin Fowler — Domain-Driven Design")
— model the business, not the database — and [Hexagonal Architecture](https://alistair.cockburn.us/hexagonal-architecture/ "Alistair Cockburn — the original ports-and-adapters write-up"),
where the domain reaches the outside world only through contracts. You write
aggregates, operations, and the contracts they depend on; Forze resolves those
contracts to real adapters at runtime — Postgres, Mongo, Redis, S3, Temporal,
and [more integrations](../integrations/index.md), batteries included.

You don't need to be fluent in either pattern. If you've met Clean or Hexagonal
Architecture before, this is those ideas made concrete for Python backends; if
you haven't, [Core Concepts](../core-concepts/overview.md) introduces each term
as you meet it.

## What it changes for you

| Instead of | With Forze |
|---|---|
| Mocking infrastructure to test a rule | Run the real operation flow on in-memory adapters — no Docker |
| Rewriting handlers to swap a datastore | Change one wiring module; the handlers don't move |
| Prototypes that become throwaway | The spike *is* the architecture — promote it by adding adapters |
| "Where does this code go?" debates | Layers and contracts answer it for you |

## Is Forze for you?

It fits backend developers building services where the business rules are the
point — where you want infrastructure choices to stay reversible, consistent
patterns across a team, and tests that don't fight a container.

It's overkill for a throwaway script or a single-endpoint passthrough with no
logic beyond validation; plain FastAPI is the shorter path there. Reach for
Forze the moment you have operations worth keeping clean — which, with in-memory
adapters, can be day one.

## Core ideas in one minute

1. **Layers** — Domain (business rules), Application (operations),
   Infrastructure (adapters), Interface (routes). Dependencies point inward; the
   domain imports no infrastructure.

2. **Ports and adapters** — your handlers ask for a capability ("document
   storage"), not an implementation ("Postgres"). The runtime injects the wired
   adapter.

3. **Specifications** — a logical name (`"users"`) ties a model to its operations
   and adapters. Change the adapter, keep the spec.

4. **Execution context** — the seam where contracts resolve to adapters. Every
   handler receives it; none learn which datastore they use.

## Prerequisites

You'll be at home here if you know **Python 3.13+** and async/await, **Pydantic**
models, and the basics of an async web framework like **FastAPI**. DDD
vocabulary — aggregates, domain events, bounded contexts — helps but isn't
required.

## Next steps

<div class="grid cards fz-cards" markdown>

-   :lucide-download: **[Installation](installation.md)**

    ---

    Set up your environment and install Forze.

-   :lucide-zap: **[Quickstart](quickstart.md)**

    ---

    Build a working service in about ten minutes — no Docker.

</div>
