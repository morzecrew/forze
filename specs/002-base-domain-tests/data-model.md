## Data & Behavior Model: Base & Domain Unit Tests

### Base utilities

- **String normalization**: Functions that normalize and clean user-provided text while preserving meaningful Unicode semantics.
- **Datetime/UUID helpers**: Functions that produce and parse time-aware values (e.g., `utcnow`, UUIDv7 helpers) used across the domain.
- **Serialization helpers**: Pydantic-based dump/validate utilities and dict diff/patch helpers that convert between in-memory models and JSON-like structures.
- **Error wrapping**: Core error types and helpers that normalize low-level exceptions into domain-aware errors.

### Domain models and mixins

- **Document model**: Core domain document abstraction with identifiers, revisioning, timestamps, and update semantics based on JSON-like diffs.
- **Update validators**: Decorator-driven validators that enforce business rules when documents change.
- **Mixins**: Name, number, and soft-deletion mixins that add common fields and behaviors to documents and related DTOs.

