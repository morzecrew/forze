# Feature Specification: Postgres Query Builder Refactor

**Feature Branch**: `001-postgres-query-builder-refactor`  
**Created**: 2025-02-23  
**Status**: Draft  
**Input**: User description: "Refactor postgres query builder to make it easier to maintain, use and extend. Make sure to remove redundant aliases for operators, keep all features if possible (combining operators, building OR chains etc)"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Single canonical operator names (Priority: P1)

A developer maintaining or extending the query builder needs to work with one clear name per operator instead of multiple aliases (e.g. "eq", "==", "="). This reduces cognitive load, avoids duplicate code paths, and makes the public contract obvious.

**Why this priority**: Removing redundant aliases is the main structural change and unblocks simpler maintenance and extension.

**Independent Test**: Can be fully tested by verifying that the builder accepts exactly one canonical name per operator and rejects or does not document alternate spellings for the same operator.

**Acceptance Scenarios**:

1. **Given** the refactored builder, **When** a caller uses the single canonical name for an operator, **Then** the builder produces the same filter behavior as before for that operator.
2. **Given** the refactored builder, **When** a caller uses a previously supported alias (e.g. "==" or "ge"), **Then** either it is no longer accepted or it is documented as deprecated with a single supported canonical form.

---

### User Story 2 - Preserved filter capabilities (Priority: P1)

A developer building queries must still be able to combine multiple operators on the same field (AND), build OR chains (disjunctions of conditions), and use all current operator types (comparison, membership, array, ltree, etc.) without regression.

**Why this priority**: Feature parity is non-negotiable; the refactor must not remove capabilities.

**Independent Test**: Can be fully tested by running existing or new tests that cover combined operators, OR chains, and each operator family; all must pass with unchanged or clarified semantics.

**Acceptance Scenarios**:

1. **Given** a filter that combines several operators on one field (e.g. range + null check), **When** the builder runs, **Then** the resulting predicate correctly ANDs those conditions.
2. **Given** a filter that expresses an OR of multiple sub-conditions, **When** the builder runs, **Then** the resulting predicate correctly ORs those conditions.
3. **Given** each currently supported operator type (equality, comparison, in/not_in, null, array contains/contained_by/overlaps/empty, ltree ancestor/descendant/match/level), **When** the builder runs with valid inputs, **Then** behavior matches current (or explicitly documented) semantics.

---

### User Story 3 - Easier to maintain and extend (Priority: P2)

A developer maintaining the codebase can locate operator definitions and add a new operator or new behavior without duplicating logic or hunting through many alias branches. The structure of the builder makes the mapping from operator to SQL predictable and localized.

**Why this priority**: Maintainability and extendability are the main goals of the refactor after simplifying the operator surface.

**Independent Test**: Can be tested by adding a hypothetical new operator (or documenting the steps to do so) and verifying that the change touches a minimal, clear set of places (e.g. one definition, one behavior branch, one validation rule).

**Acceptance Scenarios**:

1. **Given** the refactored codebase, **When** a maintainer looks for where operators are defined and compiled, **Then** they find a small, identifiable set of modules or functions rather than scattered alias handling.
2. **Given** a decision to add a new operator, **When** following project conventions, **Then** the steps do not require adding multiple string aliases or parallel code paths for the same semantic.

---

### User Story 4 - Clearer usage for API consumers (Priority: P2)

A developer or system that builds filter payloads (e.g. from API or config) can rely on a single, documented operator vocabulary. Documentation and examples use only canonical names, reducing ambiguity and support burden.

**Why this priority**: Improves usability and consistency for anyone constructing queries.

**Independent Test**: Can be tested by checking that public documentation and examples use only the canonical operator set and that the builder’s contract (e.g. error messages or schema) references the same names.

**Acceptance Scenarios**:

1. **Given** the public API or schema for filters, **When** a consumer reads it, **Then** they see exactly one name per operator (no alias list for the same op).
2. **Given** invalid or unsupported operator input, **When** the builder rejects it, **Then** the error indicates the expected canonical form where applicable.

---

### Edge Cases

- What happens when existing callers or persisted configs still use old aliases? Migration path or explicit deprecation/removal must be defined (e.g. single release support for aliases with warnings, or breaking change with release notes).
- How does the builder behave when a filter mixes valid canonical operators with invalid keys? Clear validation errors and no partial application.
- How are ltree and array-specific operators (e.g. ancestor_of, contains) represented in the canonical set? One canonical name per semantic, with type-specific validation unchanged.
- How are public names that differ from internal enums (e.g. `in` vs internal `in_`, `or` vs internal `or_`) mapped and documented so there is still exactly one canonical public name per operator?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST expose exactly one canonical name per filter operator in the public contract (input and documentation).
- **FR-002**: The system MUST preserve the ability to combine multiple operators on the same field (AND semantics) in a single filter expression.
- **FR-003**: The system MUST preserve the ability to build OR chains (disjunction of conditions) with the same semantics as today.
- **FR-004**: The system MUST preserve support for all current operator families: equality, inequality, comparison (gt, gte, lt, lte), membership (in, not_in), null check (is_null), array (contains, contained_by, overlaps, empty), and ltree (ancestor_of, descendant_of, match, level).
- **FR-005**: The system MUST produce the same query results for equivalent inputs after refactor (behavioral parity), except where behavior is explicitly changed (e.g. alias removal).
- **FR-006**: The system MUST validate filter input and MUST report errors that reference the canonical operator vocabulary where relevant.
- **FR-007**: The system MUST be structured so that adding a new operator requires changes in a minimal, well-defined set of places (no redundant alias branches).

### Key Entities

- **Filter expression**: A structured input (e.g. key–value map) keyed by field names, with values that are either scalars, lists, or nested structures representing operators and their arguments. No implementation structure implied.
- **Operator**: A named predicate type (e.g. equality, “in list”, “is null”) with a single canonical name and zero or more deprecated aliases during transition, then a single name only.
- **Compiled predicate**: The result of building a filter (e.g. a WHERE clause and parameters). Semantics must remain equivalent for the same logical condition.

## Assumptions

- The refactor is limited to the postgres query builder used for filters (and related sort/build logic if it shares operator concepts). Other parts of the stack are out of scope unless they consume the same operator names.
- “Remove redundant aliases” means converging on one **public** name per operator; canonical names are snake_case where practical, with the explicit choice that membership uses `in` and disjunction uses `or` as the public vocabulary.
- Existing tests or acceptance criteria that assert on current behavior will be updated as needed to use canonical names and to lock in preserved behavior (combined operators, OR chains, operator set).
- A single release may support deprecated aliases with warnings before removal, or the project may choose a hard cut; the spec does not mandate which.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Maintainers can add a new operator by changing no more than a small, fixed number of clearly identified places (e.g. one definition, one behavior branch, one validation rule) without adding alias lists.
- **SC-002**: All existing filter behaviors (combined operators, OR chains, and each operator type) are covered by tests and pass after refactor, with no regressions for canonical-name usage.
- **SC-003**: Public documentation and API contract list exactly one name per operator; no duplicate aliases for the same semantic.
- **SC-004**: Time to onboard a new developer to “how to add an operator” decreases or stays the same, as measured by clarity of code layout and documentation (e.g. single place to look for operator definitions and compilation).
