<p align="center">
  <a href="https://morzecrew.github.io/forze/">
    <img src="https://raw.githubusercontent.com/morzecrew/forze/main/.github/assets/forze-banner.png" alt="Forze">
  </a>
</p>

<p align="center">
  <em>Domain-Driven Design and Hexagonal Architecture for backend services</em>
</p>

<div align="center">

[![PyPI](https://img.shields.io/pypi/v/forze?label=PyPI)](https://pypi.org/project/forze/)
[![Python](https://img.shields.io/pypi/pyversions/forze)](https://pypi.org/project/forze/)
[![License](https://img.shields.io/pypi/l/forze?label=License)](https://github.com/morzecrew/forze/blob/main/LICENSE)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/12151/badge)](https://www.bestpractices.dev/projects/12151)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/morzecrew/forze/badge)](https://scorecard.dev/viewer/?uri=github.com/morzecrew/forze)
[![codecov](https://codecov.io/github/morzecrew/forze/graph/badge.svg?token=WIKAC2IUS9)](https://codecov.io/github/morzecrew/forze)
[![CodeFactor](https://www.codefactor.io/repository/github/morzecrew/forze/badge)](https://www.codefactor.io/repository/github/morzecrew/forze)
[![Socket Badge](https://badge.socket.dev/pypi/package/forze)](https://socket.dev/pypi/package/forze)

</div>

**Forze** is a Python toolkit for building backend services with Domain-Driven Design
and Hexagonal Architecture.

Your domain and application code imports nothing from a web framework, a database driver,
or a transport: it talks to ports, and exactly one place in the service names an adapter.
Forze supplies those ports, the runtime that resolves them, and the wiring that keeps the
rule true as the service grows.

## Quick start

```bash
uv add forze
```

```python
import asyncio
from uuid import UUID

import structlog

from forze import (
    CreateDocumentCmd,
    Document,
    DocumentSpec,
    DocumentWriteTypes,
    ExecutionContext,
    ReadDocument,
    build_runtime,
    configure_logging,
)
from forze_mock import MockDepsModule

log = structlog.get_logger("hexagon")


# Domain — plain models. Nothing here knows about HTTP, SQL, or a broker.
class Order(Document):
    item: str


class CreateOrder(CreateDocumentCmd):
    item: str


class ReadOrder(ReadDocument):  # adds id, rev, created_at, last_update_at
    item: str


# The port — one spec names the aggregate and the types that cross its boundary.
ORDERS = DocumentSpec(
    name="orders",
    read=ReadOrder,
    write=DocumentWriteTypes(domain=Order, create_cmd=CreateOrder),
)


# Application — speaks to the port, never learns which storage answers it.
async def place_order(ctx: ExecutionContext, item: str) -> ReadOrder:
    return await ctx.document.command(ORDERS).create(CreateOrder(item=item))


async def read_order(ctx: ExecutionContext, order_id: UUID) -> ReadOrder:
    return await ctx.document.query(ORDERS).get(order_id)


async def main() -> None:
    # Wiring — the only place an adapter is named. A real backend replaces this module
    # (Postgres takes a client, its relation config and a lifecycle module — see
    # examples/recipes/crud_fastapi), and nothing above this line changes.
    runtime = build_runtime(MockDepsModule())
    async with runtime.scope():
        ctx = runtime.get_context()
        placed = await place_order(ctx, item="widget")
        order = await read_order(ctx, placed.id)
        log.info("stored and read back", id=str(order.id), item=order.item, rev=order.rev)


if __name__ == "__main__":
    # Configure logging only when run as a script, so imports and tests stay unaffected.
    configure_logging(level="info", logger_names=["hexagon", "forze"])
    asyncio.run(main())
```

That file is [`examples/hexagon/app.py`](https://github.com/morzecrew/forze/blob/main/examples/hexagon/app.py),
copied verbatim and run by CI on every commit — `uv run python -m examples.hexagon.app`.
The in-memory adapter it wires needs no extras. A real backend replaces that one module — for
Postgres, a client, the relation config for the aggregate and a lifecycle module, as in
[recipes/crud_fastapi](https://github.com/morzecrew/forze/blob/main/examples/recipes/crud_fastapi/app.py)
— and the domain, the spec and the two application functions above it stay exactly as they are.

## What Forze does not do

- **No ORM.** Nothing here models tables, relations, or migrations. The Postgres integration
  is a driver-level client behind the same port Mongo, Firestore and the in-memory mock
  implement; your DDL stays yours.
- **No dependency-injection container.** Deps are values you register in modules and resolve
  by key. Nothing scans your code and nothing autowires by type, so the wiring is something you
  can read — and `check_wiring` dry-runs every registered operation before you serve traffic.
- **No web framework, and none in the core.** The core installs eleven libraries — none of
  them a server, a driver, or a client. `import forze` loads three modules; the runtime is
  pulled in only when you touch a name that needs it.
- **No code generation and no scaffolding.** There is no `forze new` and no `forze generate`.
  The optional CLI has two commands — `dst` (deterministic simulation) and `mock` (serve an
  app on in-memory backends) — and neither writes code into your project.
- **No opinion about your directory layout.** The docs suggest one; nothing enforces it. What
  the library enforces is the dependency direction, not your folder names.

### When not to use it

- **A service that is CRUD over one table.** There is no domain to isolate, and the ports will
  cost you more than they return.
- **A team that has not agreed on its domain vocabulary.** Forze gives that agreement a place
  to live; it cannot substitute for having one.
- **You want batteries and one blessed way to do things.** That is a full-stack framework, and
  Forze is deliberately not one — it assumes you assemble the service yourself.

## Skills for AI agents using Forze

Forze ships [Agent Skills](https://agentskills.io/) for **applications that use Forze as a
dependency**, so an assistant working in your service repo wires ports, specs, and handlers the
way the contracts actually expect rather than inventing a plausible shape.

```bash
npx skills add morzecrew/forze                # all skills
npx skills add morzecrew/forze@forze-wiring   # just one
```

A few of them:

| Skill | Covers |
| --- | --- |
| `forze-wiring` | Runtime, `DepsRegistry`, lifecycle, governed aggregates, pipeline stages |
| `forze-framework-usage` | `ExecutionContext`, ports, transactions, identity context, the query DSL |
| `forze-domain-aggregates` | Document aggregates, mixins, validators, logical specs, composition DTOs |
| `forze-deps-consumption` | Plain vs routed deps, `route=spec.name`, built-in `*DepsModule`, merge debugging |
| `forze-custom-deps` | Custom `DepKey` and `DepsModule` for private integrations |

All 21, with descriptions, are in
[skills/README.md](https://github.com/morzecrew/forze/blob/main/skills/README.md).

## Writing an adapter

Every port is a `Protocol` under `forze.application.contracts.<plane>`. An adapter is a class
that satisfies one — some planes ship a base that does the boilerplate, such as
`DocumentAdapter` — plus a `DepsModule` that binds it to the key handlers resolve. That is the
whole mechanism the shipped integrations use; being in this repo buys them nothing extra. The
smallest one to read end to end is
[`forze_vault`](https://github.com/morzecrew/forze/tree/main/src/forze_vault); the pattern is
documented in the [wiring guide](https://morzecrew.github.io/forze/latest/writing-operation/wiring/)
and the `forze-custom-deps` skill above.

You do not have to take the contract on faith where it is hardest to satisfy: `forze_dst`
ships backend-agnostic batteries for transactional isolation (the classic anomalies, with the
verdict each level owes) and for outbox → inbox delivery across a crash. Bring your backend and
run them.

How far the seam is proven, plainly: document, storage and messaging ports each have several
independent backends, which is what makes those seams credible. HTTP does not — FastAPI is the
only web-framework adapter shipped, so on Litestar, Django-Ninja, Robyn or bare ASGI you are
writing the first alternative implementation, and you should expect it to surface one or two
places where framework-shaped assumptions leaked into the edge.

## Examples

Every example under [`examples/`](https://github.com/morzecrew/forze/tree/main/examples) is a
module you can run and is executed by a test, so none of them can quietly rot. Most need no
Docker.

| Example | Shows |
| --- | --- |
| `hexagon/` | The slice above: domain, port, wiring, no transport |
| `quickstart/` | A CRUD HTTP API over the same document shape |
| `recipes/order_fulfillment/` | Saga → aggregate event → outbox → relay → inbox → downstream, in-process |
| `recipes/analytics_duckdb/` | A named, typed analytics query over a local data lake |
| `recipes/mcp_server/` | An aggregate served over MCP, every operation a tool |

Twenty-odd more recipes — caching, idempotency, realtime, secrets rotation, durable
workflows — are listed in
[examples/README.md](https://github.com/morzecrew/forze/blob/main/examples/README.md).

## Documentation

Full documentation: [morzecrew.github.io/forze](https://morzecrew.github.io/forze/).

## Stability

Forze is 0.x, and pre-1.0 here means what SemVer says it means: a minor release may change
public contracts. What you get in exchange is a written record — every breaking change lands
in [CHANGELOG.md](https://github.com/morzecrew/forze/blob/main/CHANGELOG.md) naming the
contract that moved and the migration it needs, including SQL where a schema is involved.

There is no deprecation window yet: a contract that moves, moves in that release with its note,
rather than shipping beside the old one for a cycle first. Read the changelog before upgrading
a minor.

Python 3.13 and 3.14 are supported.

## Contributing

Contributions, issues, and feature requests are welcome.
See [CONTRIBUTING.md](https://github.com/morzecrew/forze/blob/main/CONTRIBUTING.md) for details.

## Security

Please report security vulnerabilities privately as described in
[SECURITY.md](https://github.com/morzecrew/forze/blob/main/SECURITY.md).

## License

Forze is licensed under the MIT License — see
[LICENSE](https://github.com/morzecrew/forze/blob/main/LICENSE) for details.
