---
name: python-docstrings
description: Write docstrings for Python code using Sphinx/reST roles. Use when the user asks to write or update docstrings, or when writing or editing Python code that should be documented.
---

# Python Docstring Writer (Sphinx/reST)

Write **consistent, high-signal docstrings** using **Sphinx/reST roles** for cross-linking. Optimize for: fast scanning in IDE/tooltips, Sphinx-friendly references, minimal redundancy with type hints, and explaining **why/behavior** rather than restating types.

**Conventions:** Type aliases/constants → immediate string literal. Classes/functions → PEP 257 docstring. Attributes/TypedDict keys → trailing docstring. Cross-references → reST roles (e.g. ``:class:`Foo``, ``:meth:`Bar.baz``), not plain text.

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
