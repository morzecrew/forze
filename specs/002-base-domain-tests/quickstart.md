## Quickstart: Base & Domain Unit Tests

### Running the tests

- From the repository root, run:

  ```bash
  pytest tests/unit
  ```

- To focus on this feature’s coverage (once added), target:

  ```bash
  pytest tests/unit/base tests/unit/domain
  ```

### What this validates

- Core helpers in `forze.base` behave as documented, including edge cases.
- Domain models and mixins in `forze.domain` enforce expected update and validation rules.

