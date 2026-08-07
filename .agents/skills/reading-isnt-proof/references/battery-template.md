# Battery file shape

The structure a shared conformance battery converged on across six contracts
(idempotency, search, inference, storage, counters, graph management). Language
here is Python/pytest; the ordering is the point and ports to any stack.

One file per contract, living with the shared test support code — **not** next to
any one backend's tests, because "which file does this belong in?" is exactly the
question that produced the gap.

## Order of the file

1. **Module docstring** — the contract's promises, numbered.
2. **Constants** — the axes the checks turn on, each explained.
3. **Harness** — a frozen value type holding one implementation's seams.
4. **`Check` alias** — the shape of a battery entry.
5. **`check_*` free functions** — one per numbered promise, in docstring order.
6. **`*_BATTERY` tuple** — the registry, in the same order.
7. **Consumers** — per-backend test modules that parametrise over the tuple.

## 1. Module docstring: number the claims

Open by saying why the battery exists — usually *"each implementation was verified
against a different subset, so the plane had no statement that they agree."* Then
call out the promise that matters most and why (which failure it prevents), and
number what each check pins.

```python
"""Shared ``IdempotencyPort`` conformance battery: every promise, on every store.

The port's docstrings make nine testable promises, and all three stores honour
them — but each store was verified against a *different subset*, so the plane had
no statement that they agree. The gaps were not symmetric: the payload-hash and
in-progress refusals were pinned on Postgres and Redis but not the oracle, ``fail``
was pinned on the oracle and Postgres but not Redis.

The unowned-claim promise is worth the most: ``fail`` releases a pending claim so a
legitimate retry can re-execute, and if it released a claim belonging to a
*different* payload it would hand a duplicate request permission to run. That is
the failure this port exists to prevent, so it is asserted on all three rather
than on the store that happened to have a test.

What each check pins:

1. A fresh claim returns ``None`` — nothing stored yet, the caller should execute.
2. A completed operation replays its record instead of re-executing.
3. The same key with a different payload is refused.
...
9. ``commit`` without a matching pending claim is refused rather than writing.

Checks 3 and 4 are the control for 7: they are what make "the claim is still
there" after an unowned ``fail`` observable at all.
"""
```

That last paragraph is the **positive control**, stated explicitly so a later
reader cannot delete a check as redundant without noticing what it holds up.

## 2. Constants: name the axis

```python
OP = "battery_op"
"""Operation name every check uses; isolation comes from the per-check key."""

HASH_A = "hash-aaaa"
HASH_B = "hash-bbbb"
"""Two payload hashes for the same key — the axis checks 3 and 7 turn on."""
```

## 3. Harness: document why each seam cannot be generic

Every field is a place the implementations genuinely differ. If a field's reason
can't be written down, it should have been a constant.

```python
@attrs.define(slots=True, kw_only=True, frozen=True)
class IdempotencyHarness:
    """One store's seam for the battery."""

    store: IdempotencyPort
    """The store under test."""

    backend: str
    """Label used in assertion messages, so a failure names the store that disagreed."""

    key: Callable[[], str]
    """Mint a key unused by any other check.

    A factory rather than a fixed key because Postgres and Redis keep state across
    the checks in one session, so a shared key would make the battery
    order-dependent — and an order-dependent conformance suite is one that passes
    for the wrong reason.
    """


Check = Callable[[IdempotencyHarness], Any]
"""One battery check."""
```

Common seams: the subject under test, a `backend` label, a fresh-identifier
factory, a "settle"/flush hook for eventually-consistent backends, and capability
flags for checks a backend legitimately cannot support.

## 4. Checks: one claim each, discriminating assertion

Name the check after the claim, not the method. Its docstring says what breaks in
production if the claim fails — that's what stops it from being deleted as
"redundant".

```python
async def check_a_payload_hash_mismatch_is_refused(h: IdempotencyHarness) -> None:
    """One key, two payloads: the second is refused rather than served the first's result.

    Reusing an idempotency key for different arguments is a client bug, and
    answering it with the earlier result would silently return someone the wrong
    outcome.
    """

    key = h.key()
    await h.store.begin(OP, key, HASH_A)
    await h.store.commit(OP, key, HASH_A, _record())

    with pytest.raises(CoreException) as ei:
        await h.store.begin(OP, key, HASH_B)

    assert ei.value.kind == ExceptionKind.CONFLICT, h.backend
```

Note the two things that make it a conformance check rather than a unit test:
`ei.value.kind ==` (the discriminating detail — not merely `pytest.raises`) and
`, h.backend` (the failure names the implementation that disagreed).

The dangerous-direction check is worth writing out even when it looks paranoid:

```python
async def check_fail_ignores_a_claim_it_does_not_own(h: IdempotencyHarness) -> None:
    """A release for a *different* payload leaves the live claim in place.

    The dangerous direction: if ``fail`` dropped any claim under the key, a retry
    carrying different arguments could clear the in-flight operation's claim and
    let a duplicate run alongside it. The surviving refusal below is what proves
    the claim is still held.
    """

    key = h.key()

    assert await h.store.begin(OP, key, HASH_A) is None, h.backend  # positive control

    await h.store.fail(OP, key, HASH_B)

    with pytest.raises(CoreException) as ei:
        await h.store.begin(OP, key, HASH_A)

    assert ei.value.kind == ExceptionKind.CONFLICT, h.backend
```

## 5. Registry

```python
IDEMPOTENCY_BATTERY: tuple[Check, ...] = (
    check_a_fresh_claim_returns_none,
    check_a_completed_operation_replays_its_record,
    check_a_payload_hash_mismatch_is_refused,
    ...
)
```

A tuple, in docstring order. Adding an implementation must not require touching
this file; adding a promise must not require touching any backend's file.

## 6. Consumer: one per implementation

```python
@pytest.mark.parametrize("check", IDEMPOTENCY_BATTERY, ids=lambda c: c.__name__)
async def test_redis_idempotency_conformance(check: Check, redis_store) -> None:
    await check(IdempotencyHarness(store=redis_store, backend="redis", key=_key_factory()))
```

`ids=lambda c: c.__name__` makes the failure line read
`test_redis_idempotency_conformance[check_a_payload_hash_mismatch_is_refused]` —
the claim and the backend, straight from the test ID.

## Ceilings, not fakes

When an implementation genuinely cannot honour a promise (an in-memory oracle
where uniqueness is intrinsic and `drop_schema` cannot relax it), record that as a
stated ceiling — an honest no-op, documented — rather than faking the behavior or
silently skipping the check. A skip with no reason attached is a hole that looks
like coverage.
