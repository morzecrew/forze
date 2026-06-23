---
name: python-google-docstrings
description: Write consistent Python docstrings in Google style with typed sections. Use when writing or updating docstrings, documenting Python code, or when the user mentions docstrings, Google style, Napoleon, or API documentation.
---

# Python Docstring Writer (Google Style)

Write **consistent, high-signal docstrings** in **Google style** with typed sections. Optimize for: fast scanning in IDE/tooltips, Napoleon/Sphinx compatibility, minimal redundancy with type hints, and explaining **why/behavior** rather than restating types.

Target format (this is the canonical shape):

```python
def make_duplicate_error(kind: str, keys: set[str]) -> CoreException:
    """Construct a configuration exception for duplicate registry entries.

    Args:
        kind (str): Category of duplicate entries (e.g. ``'handler factories'``,
            ``'operation plans'``).
        keys (set[str]): Operation keys that are duplicated.

    Returns:
        CoreException: A configuration exception describing the conflict.
    """
```

## Core Conventions

- **Section keyword:** Use `Args:` for arguments (the canonical Google keyword; Napoleon also accepts `Parameters:` as a synonym, but prefer `Args:` and use it consistently).
- **Typed entries:** Each parameter, attribute, or key is written as `name (type): description`. The `(type)` is encouraged even when annotations exist, because it renders inline in the docstring and aids tooltips.
- **Summary first:** One imperative-or-descriptive line, ending with a period, on the line right after `"""`.
- **Blank line** between the summary and any section block.
- **Indentation:** Section bodies indent one level (4 spaces) under the keyword; continuation lines indent one further level (8 spaces).
- **Cross-references:** Use double backticks for literals/values (e.g. ``None``, ``'tuple'``) and Sphinx roles where useful (e.g. ``:class:`Foo```), but plain readable names are acceptable in Google style.

## Supported Sections

| Section | Use for |
|---------|--------|
| `Args:` | Function/method arguments (alias: `Parameters:`) |
| `Returns:` | Return value and its meaning |
| `Yields:` | Values produced by a generator |
| `Raises:` | Exceptions raised and the conditions |
| `Attributes:` | Public attributes documented on a class |
| `Examples:` | Usage examples (doctest-friendly) |
| `Note:` | Important caveats |
| `Warning:` | Dangerous or surprising behavior |

Only include sections that add information. Omit empty or trivially-obvious ones.

---

## 1. Type aliases / constants

Docstring immediately after the assignment. One line when possible. Use double backticks for literals. Prefer meaning and effects over restating the type.

```python
RowFactory = Literal["tuple", "dict"]
"""Row format for fetch methods: ``"tuple"`` for sequences, ``"dict"`` for column-keyed dicts."""

IsolationLevel = Literal["repeatable read", "serializable"]
"""Supported transaction isolation levels."""
```

---

## 2. Classes

First line: short noun phrase. Then lifecycle, concurrency/transaction semantics, and invariants. Document public attributes in an `Attributes:` section. Add exactly one blank line between the docstring and the class body.

```python
@attrs.define(slots=True)
class PostgresClient:
    """Async Postgres client with connection pooling and context-bound transactions.

    Must be initialized with a DSN via :meth:`initialize` before use. Uses context
    variables to share a single connection per logical request, so nested
    :meth:`transaction` blocks reuse the same connection via savepoints.

    Attributes:
        min_size (int): Minimum number of connections kept in the pool.
        max_size (int): Maximum number of connections the pool may open.
    """
```

Prefer the class-level `Attributes:` section for public fields. Reserve trailing attribute docstrings (section 6) for private/subtle fields or when the attribute needs more room than a one-liner.

---

## 2.1. typing.Protocol

For `typing.Protocol` interfaces:

- Document the **contract and semantics** (what implementers must guarantee), not implementation details.
- Prefer documenting **when** methods are called, expected side effects, idempotency, ordering, and concurrency guarantees.
- Do **not** document `Raises:` unless an exception is a required part of the contract.
- Add an ellipsis below the docstring for protocol methods, otherwise the method is treated as a broken stub.

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

        The context manager starts a transaction on entry and commits or rolls
        back on exit according to implementation policy.

        Returns:
            AsyncContextManager[None]: Scope that brackets a single transaction.
        """
        ...
```

---

## 3. Methods and functions

Brief summary + behavioral details. Use typed sections for parameters, returns, and meaningful errors. Explain what it does, what it returns, and edge cases. Document errors only when meaningful. Add exactly one blank line between the docstring and the body.

```python
async def fetch_one(self, query: str, *args: Any) -> Mapping[str, Any] | None:
    """Execute a query and return a single row.

    Returns ``None`` when no rows match.

    Args:
        query (str): SQL query text.
        *args (Any): Positional query parameters bound to placeholders.

    Returns:
        Mapping[str, Any] | None: The first row as a mapping, or ``None`` if empty.

    Raises:
        QueryError: If the query is malformed or the connection is closed.
    """
```

Generators use `Yields:` instead of `Returns:`:

```python
async def stream_rows(self, query: str) -> AsyncIterator[Mapping[str, Any]]:
    """Stream query results one row at a time.

    Args:
        query (str): SQL query text.

    Yields:
        Mapping[str, Any]: Each matching row, in result order.
    """
```

---

## 3.1. @overload

For overloaded callables, docstrings should reflect the **semantic differences between overload variants**, not just duplicate a shared description.

Many IDEs display the docstring of the **selected overload signature**. If an overload stub lacks a docstring, callers may see no documentation at all. Therefore, each `@overload` should have a docstring.

**Rules:**

- Each `@overload` stub must have a docstring.
- Prefer documenting the **behavior specific to that signature** (return shape, mutation vs new instance, sentinel handling, narrowing).
- Avoid repeating the entire shared description unless necessary.
- If overload semantics cannot be meaningfully distinguished, duplicate the shared docstring verbatim as a fallback.
- The implementation may carry a general docstring, but overload docstrings are the primary source of truth for per-signature guarantees.
- Add an ellipsis below each overload docstring, otherwise the stub is treated as broken.

```python
from typing import overload, Literal, Self

@overload
def register(self, op: str, *, inplace: Literal[True]) -> None:
    """Register an operation factory and mutate the registry in place.

    Args:
        op (str): Unique operation key.
        inplace (Literal[True]): Mutate this registry; no value is returned.

    Raises:
        CoreError: If ``op`` is already registered.
    """
    ...

@overload
def register(self, op: str, *, inplace: Literal[False] = False) -> Self:
    """Register an operation factory and return a new registry.

    Args:
        op (str): Unique operation key.
        inplace (Literal[False]): Leave this registry unchanged.

    Returns:
        Self: A new registry that includes ``op``.

    Raises:
        CoreError: If ``op`` is already registered.
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
    """Normalize a value without changing its meaning."""
    ...

@overload
def normalize(value: str) -> str:
    """Normalize a value without changing its meaning."""
    ...

def normalize(value: int | str) -> int | str:
    """Normalize a value without changing its meaning."""
    ...
```

---

## 4. Attributes / fields

Prefer the class-level `Attributes:` section (section 2). Use a trailing docstring when a field is private, subtle, or needs more explanation than a one-line entry.

```python
min_size: int = 2
"""Minimum number of connections kept in the pool."""

_ctx_depth: ContextVar[int] = ...
"""Transaction nesting depth used to manage savepoints."""
```

---

## 5. TypedDict keys

Class docstring: what the dict represents and where it is used. Document keys in an `Attributes:` section (Napoleon renders `TypedDict` keys as attributes). If a key is optional (e.g. `total=False`), note what happens when absent.

```python
class TransactionOptions(TypedDict, total=False):
    """Options for :meth:`PostgresClient.transaction`.

    Attributes:
        read_only (bool): If true, the transaction is read-only. Defaults to
            ``False`` when absent.
        isolation (IsolationLevel): Transaction isolation level (e.g.
            ``"repeatable read"``, ``"serializable"``). Uses the server default
            when absent.
    """

    read_only: bool
    isolation: IsolationLevel
```

---

## Description length (be concise)

The description explains intent and non-obvious behavior — it is not a place for prose. This budget applies to the **summary and body**, not to `Args:`/`Returns:`/`Raises:` entries (size those to the actual API surface).

- **Default to the one-line summary.** For simple or self-explanatory APIs, the summary is the whole docstring; do not add a body.
- **Add a body only when it earns its place** — non-obvious behavior, side effects, invariants, edge cases, or "why". Keep it to ~1–3 sentences.
- **Scale with complexity, not by default.** Reserve longer explanations for genuinely complex or tricky code (concurrency, transactions, subtle contracts). Even then, prefer the shortest correct explanation.

---

## Formatting (hard requirements)

- **Sentence-cased** summary, ending with a period. One blank line between summary and the first section.
- **Present tense** ("Returns …", "Acquires …").
- Each typed entry follows `name (type): description`; wrap continuation lines with an extra indent level.
- Double backticks for literal values, SQL fragments, flags, and env vars.
- ~88 chars line length when reasonable.
- **Do not merely repeat type hints** in prose. The `(type)` marker is fine; the description should add semantics, invariants, side effects, concurrency, or performance caveats.
- Keep section order: `Args:` → `Returns:`/`Yields:` → `Raises:` → others.

---

## Anti-patterns

**Parameter** — Bad (repeats type, no meaning): `count (int): An integer.`
Good: `count (int): Number of retries before giving up; ``0`` disables retrying.`

**Returns** — Bad (no info): `Returns: The result.`
Good: `Returns: Mapping[str, Any] | None: The first row, or ``None`` if empty.`

**Section keyword drift** — Bad: mixing `Args:` and `Parameters:` within one project.
Good: standardize on `Args:` and use it everywhere.

**Empty sections** — Bad: a `Raises:` block listing exceptions the function never raises.
Good: omit sections that add nothing.

---

## Checklist before writing

1. **User-facing?** → Document behavior and edge cases.
2. **Type hint already clear?** → Description adds semantics, not just the type.
3. **Correctness-sensitive?** (transactions, concurrency, caching, idempotency) → Must document.
4. **Generator?** → Use `Yields:` instead of `Returns:`.
5. **Raises meaningful errors?** → Add a `Raises:` section with conditions.
6. `@overload`? → Each overload stub must have a docstring; document semantic differences per signature.
7. `typing.Protocol`? → Document contract/semantics; avoid `Raises:` unless mandated.

---

## Minimal templates

**Type alias / constant:**
`Thing = ...` → `"""What it represents and how callers should interpret it."""`

**Class:**
One-line summary, then key behaviors, lifecycle, invariants, and an `Attributes:` section for public fields.

**Function / method:**

```python
"""One-line summary.

Args:
    name (Type): Meaning and constraints.

Returns:
    Type: Meaning of the value.

Raises:
    SomeError: When the failure condition occurs.
"""
```

**Field:**
`field: Type = default` → `"""Meaning, units, constraints, or why it exists."""`

---

Compatible with PEP 257, the Google Python Style Guide, and Sphinx Napoleon. Keep typed `(type)` markers even in annotated code; they render in tooltips and keep docstrings self-contained.
