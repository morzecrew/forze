# Specs and wiring

This page moved to [Specs and wiring](../concepts/specs-and-wiring.md).

## Troubleshooting

| Symptom | Likely cause | Fix | See also |
|---------|--------------|-----|----------|
| A spec resolves locally but fails after adding an integration module. | The logical spec name was confused with an infrastructure name such as a SQL table, collection, bucket, or Redis namespace. | Keep `spec.name` as the logical route and map it separately to infrastructure names inside the integration config. | [Specs and wiring](../concepts/specs-and-wiring.md) |
| Building the dependency plan raises a duplicate key or route error. | Two modules registered the same dependency key and route, such as two document query adapters for the same `DocumentSpec.name`. | Remove one registration, split routes by unique spec names, or merge only complementary modules. | [Execution](../reference/execution.md#dependencies) |
| Write endpoints or command usecases are skipped or fail for a document. | A read-only spec/config was used where read-write behavior is expected, or the document was registered under `ro_documents` instead of `rw_documents`. | Put read/write documents in the read-write map with a write config; reserve read-only specs/configs for query-only projections. | [PostgreSQL integration](../integrations/postgres.md) |
