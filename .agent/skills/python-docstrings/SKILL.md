---
name: python-docstrings
description: Write docstrings for Python code using Sphinx/reST roles. Use when the user asks to write or update docstrings, or when writing or editing Python code that should be documented.
---

# Python Docstring Writer (Sphinx/reST)

Write **consistent, high-signal docstrings** using **Sphinx/reST roles** for cross-linking. Optimize for: fast scanning in IDE/tooltips, Sphinx-friendly references, minimal redundancy with type hints, and explaining **why/behavior** rather than restating types.

**Conventions:** Type aliases/constants → immediate string literal. Classes/functions → PEP 257 docstring. Attributes/TypedDict keys → trailing docstring. Cross-references → reST roles (e.g. ``:class:`Foo``, ``:meth:`Bar.baz``), not plain text. For ``@overload`` and :class:`typing.Protocol`, follow the dedicated sections below.

---

## 1. Type aliases / constants

Docstring immediately after the assignment. One line when possible. Use double backticks for literals (e.g. ``"dict"``, ``None``). Prefer meaning and effects over restating the type.

```python
RowFactory = Literal["tuple", "dict"]
"""Row format for fetch methods: ``"tuple"`` for sequences, ``"dict"`` for column-keyed dicts."""

IsolationLevel = Literal["repeatable read", "serializable"]
"""Supported transaction isolation levels."""
```

---

## 2. Classes

First line: short noun phrase. Then: lifecycle, concurrency/transaction semantics, invariants. Don’t list every attribute. Use roles for references. Add exactly one blank line between the docstring and the class definition below the docstring.

```python
@attrs.define(slots=True)
class PostgresClient:
    """Async Postgres client with connection pooling and context-bound transactions.

    Must be :meth:`initialize`d with a DSN before use. Uses context variables to
    share a single connection per logical request, so nested :meth:`transaction`
    blocks reuse the same connection and use savepoints.
    """
```
---

## 2.1. :class:`typing.Protocol`

For :class:`typing.Protocol` interfaces:

- Document the **contract and semantics** (what implementers must guarantee), not implementation details.
- Prefer documenting **when** methods are called, expected side effects, idempotency, ordering, and concurrency guarantees.
- Do **not** document ``:raises`` for protocols unless an exception is a required part of the contract (usually unknown for interfaces).
- Use reST roles for referenced types and callables.
- Don't forget to add ellipsis below the docstring for protocol methods, otherwise the method will be treated as broken.
- To document protocol overloads refer to 3.1. ``@overload``

```python
from typing import Protocol, AsyncContextManager

class AppRuntimePort(Protocol):
    """Application runtime contract for transactional execution.

    Implementations provide a transaction boundary for usecases. Nested
    transactions may be supported via savepoints; callers should not assume a
    specific strategy unless explicitly documented by the implementation.
    """

    def transaction(self) -> AsyncContextManager[None]:
        """Return an async context manager that scopes a transaction.

        The returned context manager starts a transaction on entry and commits or
        rolls back on exit according to implementation policy.
        """
        ...
```

---

## 3. Methods and functions

Brief summary + behavioral details. Use **reST field lists** for params/returns/raises when needed. Explain what it does, what it returns, and edge cases. Document errors only when meaningful (e.g. ``:raises SomeError: When ...``). Add exactly one blank line between the docstring and the function definition below the docstring.

```python
async def fetch_one(self, query: str, *args: Any) -> Mapping[str, Any] | None:
    """Execute a query and return a single row.

    Returns ``None`` when no rows match.

    :param query: SQL query text.
    :param args: Query parameters.
    :returns: A row mapping or ``None``.
    """
```

---

## 3.1. ``@overload``

For overloaded callables, docstrings should primarily reflect the **semantic
differences between overload variants**, not just duplicate a shared description.

Many IDEs display the docstring of the **selected overload signature**. If an
overload stub lacks a docstring, callers may see no documentation at all.
Therefore, each ``@overload`` should have a docstring.

**Rules:**

- Each ``@overload`` stub must have a docstring.
- Prefer documenting the **behavior specific to that signature** (e.g.
  return shape, mutation vs new instance, sentinel handling, narrowing).
- Avoid repeating the entire shared description unless necessary.
- If overload semantics cannot be meaningfully distinguished, duplicate the
  shared docstring verbatim as a fallback.
- The implementation may contain a general docstring, but overload docstrings
  are the primary source of truth for per-signature guarantees.
  - Don't forget to add ellipsis below the docstring for overloads, otherwise the method will be treated as broken.

```python
from typing import overload, Literal, Self

@overload
def register(self, op: str, *, inplace: Literal[True]) -> None:
    """Register an operation factory and mutate the registry.

    Does not return a value. Raises :exc:`CoreError` if the operation
    is already registered.
    """
    ...

@overload
def register(self, op: str, *, inplace: Literal[False] = False) -> Self:
    """Register an operation factory and return a new registry.

    Does not mutate the original instance. Raises :exc:`CoreError`
    if the operation is already registered.
    """
    ...

def register(self, op: str, *, inplace: bool = False) -> Self | None:
    """Register an operation factory.

    Dispatches to the appropriate overload behavior based on ``inplace``.
    """
```

Fallback (no meaningful semantic difference):

```python
@overload
def normalize(value: int) -> int:
    """Normalize a numeric value without changing its meaning."""
    ...

@overload
def normalize(value: str) -> str:
    """Normalize a numeric value without changing its meaning."""
    ...

def normalize(value: int | str) -> int | str:
    """Normalize a value without changing its meaning."""
    ...
```

---

## 4. Attributes / fields

Trailing docstring when public or needing clarification. Omit or keep very short for private fields (only if subtle or critical).

```python
min_size: int = 2
"""Minimum number of connections in the pool."""

_ctx_depth: ContextVar[int] = ...
"""Transaction nesting depth used to manage savepoints."""
```

---

## 5. TypedDict keys

Class docstring: what the dict represents and where it’s used. Key docstrings: behavior, defaults, interpretation. If a key is optional (e.g. `total=False`), note what happens when absent.

```python
class TransactionOptions(TypedDict, total=False):
    """Options for :meth:`PostgresClient.transaction`."""

    read_only: bool
    """If true, transaction is read-only."""

    isolation: IsolationLevel
    """Transaction isolation level (e.g. ``"repeatable read"``, ``"serializable"``)."""
```

---

## reST roles (use for cross-references)

| Role | Use for |
| ------ | -------- |
| ``:class:`MyClass`` | Classes |
| ``:meth:`MyClass.method`` | Methods |
| ``:func:`my_function`` | Functions |
| ``:attr:`MyClass.attr`` | Attributes/properties |
| ``:mod:`package.module`` | Modules |
| ``:data:`CONSTANT`` | Module-level data/constants |
| ``:exc:`SomeError`` | Exceptions |

Same-class: ``:meth:`initialize`` is fine. Cross-module: prefer fully-qualified, e.g. ``:class:`pkg.mod.Foo``.

---

## Formatting (hard requirements)

- **Sentence-cased**, end with a period. One blank line between code and docstring.
- **Present tense** (“Returns …”, “Acquires …”).
- Double backticks for literal values, SQL fragments, flags, env vars.
- Blank line between summary and body. ~88 chars line length when reasonable.
- **Do not repeat type hints** in the docstring. Add semantics, invariants, side effects, concurrency, performance caveats.

---

## Anti-pattern

**Bad** (repeats type, no roles):

```python
timeout: int
"""Timeout as an integer."""
"""Options for PostgresClient.transaction."""
```

**Good**:

```python
timeout: int
"""Timeout in seconds for acquiring a connection from the pool."""
"""Options for :meth:`PostgresClient.transaction`."""
```

---

## Checklist before writing

1. **User-facing?** → Document behavior and edge cases.
2. **Type hint already clear?** → Docstring adds semantics, not types.
3. **Correctness-sensitive?** (transactions, concurrency, caching, idempotency) → Must document.
4. **Can cross-link?** → Use ``:meth:`...`` / ``:class:`...``.
5. ``@overload``? → Prefer a single docstring on the implementation; stubs omit or duplicate.
6. :class:`typing.Protocol`? → Document contract/semantics; avoid ``:raises`` unless mandated.

---

## Minimal templates

**Type alias / constant:**
`Thing = ...` → `"""What it represents and how callers should interpret it."""`

**Class:**
One-line summary, then key behaviors, lifecycle, invariants, and ``:meth:`X.foo`` / ``:class:`Y`` references.

**Function / method:**
One-line summary, then details; ``:param name: Meaning.`` ``:returns: Meaning.`` ``:raises SomeError: When.``

**Field:**
`field: Type = default` → `"""Meaning, units, constraints, or why it exists."""`

---

Compatible with PEP 257, Sphinx/reST, and IDE tooltips. Keep roles even in Markdown-only contexts; they stay readable and Sphinx benefits.
