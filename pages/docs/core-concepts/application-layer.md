# Application Layer

The application layer **orchestrates** domain logic and coordinates infrastructure. It defines *what* happens, not *how* persistence or transport work. Operations receive an execution context and resolve dependencies from it.

## Operations (Use Cases)

An **operation** (usecase) is a single, well-defined business action. It takes arguments and returns a result. Operations support composition via guards, effects, and middlewares.

<div class="d2-diagram">
  <img class="d2-light" src="../../assets/diagrams/light/operation-composition.svg" alt="Operation composition">
  <img class="d2-dark" src="../../assets/diagrams/dark/operation-composition.svg" alt="Operation composition">
</div>

| Hook | When | Purpose |
|------|------|---------|
| **Guards** | Before execution | Validate or enrich arguments |
| **Middlewares** | Around execution | Retries, metrics, cross-cutting concerns |
| **Effects** | After execution | Logging, indexing, event publishing |

Composition is **immutable** — adding a guard or effect returns a new operation instance. **Transactional operations** add explicit transaction boundaries and support **side guards/effects** that run outside the transaction (for example, sending a notification only after commit).

## Execution Runtime

The **execution runtime** is the runnable scope where operations run. It combines three elements:

<div class="d2-diagram">
  <img class="d2-light" src="../../assets/diagrams/light/execution-runtime.svg" alt="Execution runtime">
  <img class="d2-dark" src="../../assets/diagrams/dark/execution-runtime.svg" alt="Execution runtime">
</div>

| Element | Purpose |
|---------|---------|
| **Deps plan** | Describes how to build the dependency container |
| **Lifecycle plan** | Startup and shutdown hooks |
| **Execution context** | Deps and transactions passed to operations |

The runtime follows a clear lifecycle:

<div class="d2-diagram">
  <img class="d2-light" src="../../assets/diagrams/light/runtime-lifecycle.svg" alt="Runtime lifecycle">
  <img class="d2-dark" src="../../assets/diagrams/dark/runtime-lifecycle.svg" alt="Runtime lifecycle">
</div>

| Phase | Actions |
|-------|---------|
| **Setup** | Enter scope → create context → run startup hooks |
| **Run** | Execute operations |
| **Teardown** | Run shutdown hooks → reset context → exit scope |

Dependencies and lifecycle are configured once, declaratively. Operations receive a context and resolve what they need. No global state, no hidden coupling.

## Dependency Plan

The **dependency plan** describes how to build the dependency container. Modules produce dependencies; plans compose them (for example, base + database + cache). The runtime builds the container before any operation runs.

<div class="d2-diagram">
  <img class="d2-light" src="../../assets/diagrams/light/dependency-plan.svg" alt="Dependency plan">
  <img class="d2-dark" src="../../assets/diagrams/dark/dependency-plan.svg" alt="Dependency plan">
</div>

Dependencies are **not** limited to contracts. The container can hold:

- Raw clients (database connections, HTTP clients)
- Contract implementations (adapters)
- Custom services
- Parameterized factories

**Dependency routers** select the right implementation when resolution depends on a parameter (for example, aggregate type) or when multiple adapters exist for the same contract.

## Operation Registry and Plan

The **operation registry** maps operation names to factories. The **operation plan** describes how each operation is composed (guards, effects, middlewares). Resolution applies both: the registry provides the base operation, the plan wraps it.

<div class="d2-diagram">
  <img class="d2-light" src="../../assets/diagrams/light/operation-registry.svg" alt="Operation registry">
  <img class="d2-dark" src="../../assets/diagrams/dark/operation-registry.svg" alt="Operation registry">
</div>

| Concept | Purpose |
|---------|---------|
| **Registry** | Maps operation keys (e.g. `"get"`, `"create"`) to factories |
| **Factory** | Builds the base operation from execution context |
| **Plan** | Wraps the base operation with guards, effects, transaction middleware |

Plans are keyed by operation name and mergeable. A base plan (wildcard `*`) might add logging to all operations; a specific plan might add authorization to `"create"` only. Per-operation plans extend the base plan. **Priorities** control the order of hooks when merged. Transactional operations support in-tx and after-commit buckets.

## Why It Matters

- **Operations are registered once** — composition is declared in plans
- **Add auditing, idempotency, or custom behavior** — extend the plan without touching the core operation
- **Testability** — stub the context with in-memory implementations
