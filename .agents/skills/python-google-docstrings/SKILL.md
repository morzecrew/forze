---
name: python-google-docstrings
description: Write Google-style Python docstrings — Args/Returns/Raises/Attributes sections that render under Sphinx Napoleon and read well in IDE tooltips. Use whenever writing, editing, or reviewing Python docstrings or API documentation, documenting functions, classes, modules, or constants, or when the user mentions docstrings, Google style, Napoleon, or asks to "document this code".
---

# Python Docstrings — Google Style

A docstring earns its place by saying what type hints cannot: meaning, constraints,
side effects, and failure conditions. Google style expresses those as indented
sections (`Args:`, `Returns:`, `Raises:`) that Sphinx Napoleon compiles into the same
field lists reST uses — so write for two readers at once: a human scanning a tooltip
and Sphinx rendering API docs.

## Use this skill when

- Writing or editing Python docstrings in a project that uses Google style (`Args:` sections)
- Documenting new Python functions, classes, methods, modules, or constants
- Reviewing or fixing docstrings for Sphinx Napoleon rendering
- Standardizing drifting or mixed docstring conventions toward one consistent style

## Do not use this skill when

- The project writes reST field lists (`:param x:`) — use python-rest-docstrings instead
- The project uses NumPy style (section names underlined with dashes)
- Writing READMEs, guides, or comments — docstrings state API contracts, not narratives

## Canonical shape

```python
async def fetch_one(self, query: str, *args: Any) -> Mapping[str, Any] | None:
    """Executes a query and returns the first matching row.

    Args:
        query (str): SQL query text with numbered placeholders.
        *args (Any): Positional parameters bound to the placeholders.

    Returns:
        Mapping[str, Any] | None: The first row as a mapping, or ``None``
            when no rows match.

    Raises:
        QueryError: If the query is malformed or the connection is closed.
    """
```

- Section order: `Args:` → `Returns:`/`Yields:` → `Raises:` → `Attributes:`/`Examples:`/`Note:`.
- Entries are `name (type): description`; continuation lines indent one extra level.
- List varargs with their stars: `*args`, `**kwargs`.
- Google requires the `(type)` marker only when a parameter lacks an annotation.
  Keep it anyway: Napoleon renders it inline, and the docstring stays self-contained
  in tooltips and diffs that hide the signature.

## Summary line

- One line directly after `"""`, ending with `.`, `?`, or `!`; blank line before
  anything else. State what the caller gets — never "This function ...".
- Google accepts descriptive mood ("Fetches rows.") or imperative ("Fetch rows.")
  but requires consistency within a file. Default to descriptive — it matches the
  style guide's own examples; if the file already uses imperative, follow the file.

## Sections

| Section | Use for |
|---|---|
| `Args:` | Parameters, including `*args` / `**kwargs` |
| `Returns:` | Meaning of the return value; skip for functions returning `None` |
| `Yields:` | Generator output — replaces `Returns:` |
| `Raises:` | Exceptions relevant to the interface, with their conditions |
| `Attributes:` | Public class attributes, excluding properties |
| `Examples:` | Doctest-friendly usage |
| `Note:` / `Warning:` | Caveats / dangerous or surprising behavior |

Napoleon treats `Args`, `Arguments`, and `Parameters` as aliases — standardize on
`Args:` so grep and review stay trivial. Include only sections that add information;
an empty or restating section is noise.

## Cross-references and literals

Plain names are acceptable in Google style, but because Napoleon converts docstrings
to reST before Sphinx parses them, Sphinx roles work inside any description and
produce real links: `:class:`, `:meth:`, `:func:`, `:attr:`, `:exc:`, `:data:`.
Prefix a target with `~` to render only its last component:

```text
:meth:`initialize`          -> link "initialize" (resolved within the class)
:class:`pkg.mod.Foo`        -> link "pkg.mod.Foo" (cross-module: fully qualify)
:meth:`~queue.Queue.get`    -> link rendered as just "get"
```

Use double backticks for literal values — ``None``, ``'tuple'``, SQL fragments,
flags, environment variables — so they render as code, not prose.

## Raises: discipline

- Document only exceptions relevant to the caller's interface, each with its
  trigger condition — a bare exception name tells the caller nothing actionable.
- Never document exceptions raised because the caller violated the documented
  contract: per the style guide, that would paradoxically make behavior under
  violation of the API part of the API.
- On protocols and other interfaces, add `Raises:` only when raising is a required
  part of the contract, not a detail of one implementation.

## Generators

Use `Yields:` in place of `Returns:`; describe one yielded item and any ordering
guarantee.

```python
async def stream_rows(self, query: str) -> AsyncIterator[Mapping[str, Any]]:
    """Streams query results one row at a time.

    Yields:
        Mapping[str, Any]: Each matching row, in result order.
    """
```

## Classes, attributes, properties

- Class summary is a noun phrase; the body covers lifecycle, invariants, and
  concurrency — what a caller cannot recover from the signature.
- Document public attributes (excluding properties) in `Attributes:`, in the same
  `name (type): description` shape as `Args:`.
- Document a property on its getter, worded like an attribute:
  `"""The Bigtable path."""`, never `"""Returns the Bigtable path."""`.
- Use a trailing docstring under the assignment for private or subtle fields that
  need more room than a one-line entry.

```python
class PostgresClient:
    """Async Postgres client with pooling and context-bound transactions.

    Must be initialized with a DSN via :meth:`initialize` before use. Nested
    :meth:`transaction` blocks reuse one connection via savepoints.

    Attributes:
        min_size (int): Minimum number of pooled connections.
        max_size (int): Maximum number of connections the pool may open.
    """
```

## Type aliases, constants, TypedDict

- Constants and aliases take a trailing one-line docstring right after the
  assignment, explaining meaning and effect — the type is already on the line above.
- TypedDict: the class docstring says what the dict configures; document keys in
  `Attributes:`. For `total=False` keys, always state what absence means.

```python
RowFactory = Literal["tuple", "dict"]
"""Row format for fetch methods: ``"tuple"`` for sequences, ``"dict"`` for dicts."""

class TransactionOptions(TypedDict, total=False):
    """Options for :meth:`PostgresClient.transaction`.

    Attributes:
        read_only (bool): Run the transaction read-only. Defaults to ``False``
            when absent.
        isolation (IsolationLevel): Isolation level; server default when absent.
    """
```

## Stubs: @overload and Protocol

- Give every `@overload` stub its own docstring — IDEs show the docstring of the
  selected overload, so an undocumented stub shows the caller nothing.
- Document what differs per signature: return shape, mutation vs new instance,
  sentinel handling. If nothing meaningfully differs, duplicate the shared summary
  verbatim. Keep a general docstring on the implementation.
- Protocol methods document the contract — when they are called, idempotency,
  ordering, side effects — never one implementation's details.
- End each stub body with `...` after the docstring. A docstring alone is a valid
  body; the `...` marks the stub as intentional, not unfinished.

```python
@overload
def register(self, op: str, *, inplace: Literal[True]) -> None:
    """Registers an operation in place; returns nothing."""
    ...

@overload
def register(self, op: str, *, inplace: Literal[False] = False) -> Self:
    """Registers an operation on a new registry, leaving this one unchanged."""
    ...

def register(self, op: str, *, inplace: bool = False) -> Self | None:
    """Registers an operation factory.

    Raises:
        CoreError: If ``op`` is already registered.
    """
```

## Length and formatting

- Default docstring is the summary line alone. Add a body only when it earns its
  place — non-obvious behavior, side effects, invariants, "why" — in 1–3 sentences.
  Size `Args:`/`Returns:`/`Raises:` to the real API surface.
- Never restate the type in prose. The `(type)` marker carries the type; the
  description adds semantics: units, constraints, defaults on absence.
- Section bodies indent 4 spaces under the keyword; continuations 8.
- Wrap near 88 columns; the summary must stay on one physical line.

## Anti-patterns

| Wrong | Right |
|---|---|
| `count (int): An integer.` | `count (int): Retries before giving up; 0 disables retrying.` |
| `Returns: The result.` | `Returns: bool: True if the row was inserted.` |
| Property: `"""Returns the path."""` | `"""The Bigtable path."""` — properties read as attributes |
| `Raises: ValueError:` for arguments the contract already forbids | Omit — contract violations are not interface behavior |
| Mixing `Args:` and `Parameters:` across a project | `Args:` everywhere |

## Related skills

- python-rest-docstrings — the same rules expressed as reST field lists, for projects not using Napoleon
- self-documenting-code — better names and structure shrink what docstrings must explain
- altitude-docs — deciding what belongs in docstrings vs higher-level documentation
