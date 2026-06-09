---
title: Home
summary: Domain-Driven Design and Hexagonal Architecture for backend services
hide:
  - navigation
  - toc
---

# Forze <!-- markdownlint-disable-line -->

**Build backend services with clear boundaries.** Domain-first models,
application-level orchestration, and infrastructure you can swap without
touching business logic.

[Get started](get-started/installation.md){ .md-button .md-button--primary }
[Quickstart in 10 minutes](get-started/quickstart.md){ .md-button }

---

## Start here

<div class="grid cards" markdown>

-   :lucide-download: **Install**

    ---

    Add the core package and only the integrations you need, via optional
    extras.

    [:octicons-arrow-right-24: Installation](get-started/installation.md)

-   :lucide-zap: **Quickstart**

    ---

    Stand up a working CRUD service with in-memory adapters — no Docker, no
    database.

    [:octicons-arrow-right-24: Quickstart](get-started/quickstart.md)

-   :lucide-compass: **Core concepts**

    ---

    Understand the layers, contracts, and runtime that keep Forze projects
    maintainable as they grow.

    [:octicons-arrow-right-24: Core concepts](core-concepts/overview.md)

-   :lucide-chef-hat: **Recipes**

    ---

    Task-oriented guides for wiring real backends: Postgres, Redis, FastAPI,
    and more.

    [:octicons-arrow-right-24: Recipes](recipes/postgres.md)

</div>

## Why Forze

<div class="grid cards" markdown>

-   :lucide-layers: **Layered by design**

    ---

    Dependencies flow inward. Domain logic never imports a database driver or
    web framework — so it never breaks when one changes.

-   :lucide-plug: **Ports and adapters**

    ---

    The application depends on contracts, not implementations. Swap Postgres
    for Mongo by changing a dependency module, not your handlers.

-   :lucide-boxes: **Batteries, not lock-in**

    ---

    Optional integrations for Postgres, Redis, S3, Temporal, RabbitMQ,
    FastAPI, Socket.IO and more — each behind a stable contract.

-   :lucide-shield-check: **Boundaries you can trust**

    ---

    Architecture rules are enforced by import contracts, not convention. The
    structure holds up under real teams and real deadlines.

</div>

## A taste of Forze

Define a domain model, declare a spec, and let the runtime resolve the
infrastructure behind a contract:

```python
from forze.application.contracts.document import DocumentSpec
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument


# Domain model: business behaviour and invariants.
# Document gives you id, rev, and timestamps for free.
class User(Document):
    name: str
    email: str | None = None


class CreateUserCmd(CreateDocumentCmd):
    name: str
    email: str | None = None


class ReadUser(ReadDocument):
    name: str
    email: str | None = None


# Spec: the logical name adapters and routes share. Handlers reference
# "users" — never knowing if it lands in Postgres, Mongo, or memory.
user_spec = DocumentSpec(
    name="users",
    read=ReadUser,
    write={"domain": User, "create_cmd": CreateUserCmd},
)
```

[See the full walkthrough :octicons-arrow-right-24:](get-started/quickstart.md){ .md-button }
</invoke>
