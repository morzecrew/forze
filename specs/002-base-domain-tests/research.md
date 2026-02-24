## Research & Decisions: Base & Domain Unit Tests

### Decision: Testing framework and layout

- **Choice**: Use existing pytest-based test runner and extend the `tests/unit` tree with `base` and `domain` subpackages.
- **Rationale**: Aligns with current project conventions and keeps new tests fast and focused.
- **Alternatives considered**: Adding integration-level tests hitting higher layers was rejected for this feature because the goal is to validate core behaviors in isolation.

### Decision: Scope of behaviors to cover

- **Choice**: Focus on observable contracts of `forze.base` (string normalization, datetime/UUID helpers, serialization helpers, error wrapping) and `forze.domain` (document updates, validators, mixins), including key edge cases.
- **Rationale**: These modules underpin multiple application layers; covering their contracts yields maximal safety per test added.
- **Alternatives considered**: Expanding into full end-to-end flows was rejected to keep the scope contained and tests fast.

