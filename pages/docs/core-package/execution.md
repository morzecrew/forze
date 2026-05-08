# Execution

This page moved to [Execution](../reference/execution.md).

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/deps-resolution.svg" alt="Dependency resolution from DepsPlan through modules, Deps, keys, and ports">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/deps-resolution.svg" alt="Dependency resolution from DepsPlan through modules, Deps, keys, and ports">
</div>

## Troubleshooting

| Symptom | Likely cause | Fix | See also |
|---------|--------------|-----|----------|
| A lifecycle startup step did not run before a request or usecase. | The runtime scope was not entered, or `startup()` was not called from the application lifespan. | Use `async with runtime.scope()` for tests and workers, or call `runtime.startup()`/`runtime.shutdown()` from the framework lifespan. | [Execution](../reference/execution.md#executionruntime) |
| Resolving `ctx.dep(...)`, `ctx.doc_query(...)`, or another helper raises a missing dependency error. | The `DepsPlan` does not include the module that registers that key/route, or the route does not match the spec name. | Add the correct integration deps module and verify the spec name, dependency key, and route are registered together. | [Specs and wiring](../concepts/specs-and-wiring.md) |
| `Deps.merge()` raises a key collision while building the plan. | Multiple modules provide the same dependency key and route. | Register only one provider per key/route, or separate providers with routed names before merging modules. | [Execution](../reference/execution.md#dependencies) |
