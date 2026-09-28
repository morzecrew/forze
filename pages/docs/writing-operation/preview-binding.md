---
title: Preview binding
icon: lucide/badge-check
summary: A confirmation refuses when what it confirms is no longer what the caller was shown
---

Idempotency answers **"did I already send this?"** Preview binding answers **"is this still what I
saw?"** — the question every "confirm this" flow has: a quote, a filing, a consent form, an
invoice. Between preview and submit the facts can move — another user edits a row, a scheduled
job lands — and the caller then confirms something they never saw.

A preview returns its projection with a fingerprint. The confirming command carries the
fingerprint back, the projection is computed again inside the command's transaction, and a
mismatch **refuses** — never merges, never overwrites.

## Declaring a flow

One declaration, read by both sides, so the two can never fingerprint different things:

```python
from pydantic import BaseModel

from forze.application.execution import ExecutionContext
from forze.domain.models import BaseDTO
from forze_kits.domain.preview import PreviewBinding, ReviewedCommand


class QuoteArgs(BaseDTO):
    quote_id: str


class ConfirmQuote(QuoteArgs, ReviewedCommand):  # the preview's arguments, plus `fingerprint`
    pass


class QuoteShown(BaseModel):
    lines: list[str]
    total: int
    rendered_at: str


async def show_quote(ctx: ExecutionContext, args: QuoteArgs) -> QuoteShown: ...


QUOTE = PreviewBinding(name="quote", projector=show_quote, exclude={"rendered_at"})
```

- **The preview** returns `await QUOTE.reviewed(ctx, args)` — a `Reviewed[QuoteShown]` carrying
  `data` and `fingerprint`.
- **The confirmation** is bound with `QUOTE.bind(registry.bind("quote.confirm").bind_tx().set_route("pg").finish())`,
  and its command is a `ReviewedCommand`.
- **`exclude`** names fields the fingerprint ignores: a rendering hint, a `generated_at` stamp.
  Exclude rendering details, never the data being confirmed — a fingerprint over nothing
  confirms anything. A name the projection does not declare is refused.

## What the confirmation does

Inside its transaction, before the handler runs, the binding computes the projection again and
compares fingerprints. On a mismatch the operation fails with `precondition`, code
`preview_changed`, naming the flow. It says the preview is stale and **not what changed** — the
caller may no longer be allowed to see the new value. The remedy is to show the preview again
and confirm what it shows now.

The confirmation runs at **snapshot isolation** (`isolation=` raises it to serializable; read
committed is refused). Below snapshot, a write landing between the check and the handler's own
reads would reach the handler, which would then act on a state nobody was shown. Snapshot can
refuse a concurrent write with a `concurrency` error, which is retryable.
An operation that already declares serializable passes `isolation=IsolationLevel.SERIALIZABLE`
to `bind` — two different levels on one operation are refused when the registry freezes.

!!! note "Detection, not a lock"

    Nothing is held between preview and confirmation. Two callers can both preview, and the
    second one's confirmation refuses — correct, and not what "binding" might suggest. Keeping
    concurrent writers apart is [`SerializedBy`](../reference/contracts/document.md#storage-guarantees)'s job.

The fingerprint is `sha256-c1:<hex>` over a canonical form of its own — sets sorted, dates in
ISO 8601, UUIDs and decimals as strings — identical in every process and unmoved by dependency
upgrades. A future form takes a new prefix, so a fingerprint from an old one refuses rather than
matching. It is not signed: a tampered fingerprint only makes its own confirmation fail.
