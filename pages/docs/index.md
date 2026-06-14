---
title: Home
summary: Domain-Driven Design and Hexagonal Architecture for backend services
hide:
  - navigation
  - toc
  - footer
---

<div class="forze-hero" markdown>

<canvas class="forze-hero__canvas" id="forze-hero-canvas" aria-hidden="true"></canvas>

<div class="forze-hero__text" markdown>

# Forze <!-- markdownlint-disable-line -->

Backend services with **clear boundaries** — swap any infrastructure without
touching business logic.

[Get started](get-started/introduction.md){ .md-button .md-button--primary }
[Quickstart in 10 minutes](get-started/quickstart.md){ .md-button }

</div>

</div>

<div class="forze-overview" markdown>

![Forze separates what your app does from how it's done, joined at the execution context](_diagrams/light/concept-overview.svg#only-light){ loading=lazy }
![Forze separates what your app does from how it's done, joined at the execution context](_diagrams/dark/concept-overview.svg#only-dark){ loading=lazy }

<div class="grid cards" markdown>

-   :lucide-layers: **Layered by design**

    Domain, application, infrastructure, interface — each layer has one job, and dependencies only point inward.

-   :lucide-plug: **Ports and adapters**

    Your handlers ask for capabilities, not implementations. Swap Postgres for Mongo without touching business logic.

-   :lucide-flask-conical: **Testable by default**

    Mock adapters run in-memory. Test your domain logic without Docker or external services.

-   :lucide-boxes: **Composable infrastructure**

    Mix and match: Postgres for documents, Redis for cache, RabbitMQ for events — wired together at startup.

</div>

</div>
