---
title: Proposals
icon: lucide/file-check
summary: Machine-authored changes that stay inert until a human decides them — a batch of items, per-item verdicts, amendments, and an accept effect that lands with the decision
---

A model, or a rule, can draft a change faster than a person can write it: a mapping for a
field a source started sending, a metric definition, a fix for drift. What it must not do is
apply the change. The draft has to wait for someone to accept it, and what is accepted has
to land exactly as decided. Every application that wants this builds the same three things:
a status table, a review endpoint, and an apply step, and the part that goes wrong is the
gap between deciding and applying.

The proposal kit is that lifecycle, once. Nothing in it accepts on its own.

## The shape

A **proposal** is a batch of **items**. Each item proposes one change, carries the evidence
the proposer saw, and gets its own verdict: a reviewer accepts some items, possibly changing
them first, and rejects the rest in one decision. A proposal of one item is a single
generated artifact, such as a draft to keep or discard.

```text
request ──▶ pending ──run──▶ open ──decide──▶ decided
                       │
                       └──────▶ failed  (the proposer raised; request again)
```

An item the proposer is unsure of is a **question**, and is accepted only with the
reviewer's answer. A confidence may order a review queue; it never decides an item.

## Wiring it

The change and the evidence are your models. `proposal_spec` builds the collection around
them, and `ProposalKit` gives you its operations:

```python
from forze.application.execution import ExecutionContext
from forze_kits.integrations.proposal import (
    ProposalDraft,
    ProposalKit,
    ProposalRequest,
    proposal_spec,
)
from pydantic import BaseModel


class Mapping(BaseModel):
    field: str


class Seen(BaseModel):
    filled: float


MAPPINGS = proposal_spec("mappings", change=Mapping, evidence=Seen)


async def propose(ctx: ExecutionContext, request: ProposalRequest) -> ProposalDraft:
    return ProposalDraft(
        items=[
            MAPPINGS.item(key="amount", proposed=Mapping(field="amount"), evidence=Seen(filled=0.97)),
            MAPPINGS.item(key="cust", question="Is `cust` the customer id?", evidence=Seen(filled=0.4)),
        ]
    )


kit = ProposalKit(spec=MAPPINGS, proposer=propose)
operations = kit.registry(tx_route="main")
```

Bind `MAPPINGS.document` in your document deps module like any document spec, and merge
`kit.spec_contributions()` into your spec inventory. The proposer is a plain function here; a
model-backed one calls an inference port inside it (see [Inference](inference.md)) and returns
the same draft, with `model`, `tokens` and `latency_ms` filled in.

## The operations

Three operations, keyed under the collection's name (`mappings.request`, `mappings.run`,
`mappings.decide`):

| Operation | Takes | Does |
| --- | --- | --- |
| `request` | `RequestProposalDTO(scope, base=None)` | Creates a `pending` proposal for the scope, or returns the one already pending there. Without a `base`, the kit's `base_of` supplies it. |
| `run` | `ProposalIdDTO(id)` | Calls the proposer and stores its items as `open`, or `decided` when it returns none. An exception, or two items with one key, fails the proposal with the error. A proposal no longer `pending` is returned as it is. |
| `decide` | `DecideProposalDTO(id, verdicts, note)` | Records each item's verdict and runs `on_accept`, in one transaction on `tx_route`. An item left out is rejected. |

`run` is the slow one: call it from your own worker or durable function, so a model call never
holds a request open. A retried worker is a no-op on a proposal it already ran.

A verdict is `VerdictDTO(key, accept, change=None)`. Accepting with a `change` records the
amendment as what was decided; accepting a question needs one. The item keeps both what was
proposed and what was decided.

## What accepting authorises

`on_accept(ctx, proposal, accepted_items)` runs inside the decide transaction, after the
verdicts are stored: the effect and the decision land together, or neither does. A decision
that accepts nothing runs no effect. Keep the effect to the cheap part, such as saving a new
revision of what the items change.

For anything long-running, write its queue entry from the effect, so it commits with the
decision: an event on a [transactional outbox](../recipes/transactional-outbox.md), or an enqueue on a durable
run store that joins the transaction. Queued after the commit instead, a crash between the
two leaves an accepted change that nothing applies. Key the work by the proposal id, so a
redelivery applies it once.

`base_of(ctx, scope)` returns the current version of what a scope's proposals change. A
proposal records the version it was made against, and deciding it after that moved is
refused: the reviewer would be accepting changes to something that no longer exists in that
form. Request a new proposal instead.

That check reads before it writes, so it is a fast refusal, not the guarantee: two open
proposals on one base can pass it at once. Make the effect's write conditional on
`proposal.base`, as a compare-and-set on the target's version or a unique constraint on its
next version, so the second decision fails in the database rather than applying to a target
the first already moved.

## Refusals

| Code | Kind | When |
| --- | --- | --- |
| `proposal_principal_required` | `authentication` | `decide` with no authenticated principal. |
| `proposal_pending` | `conflict` | `decide` before the proposer finished. |
| `proposal_decided` | `conflict` | `decide` on a proposal already decided. |
| `revision_mismatch` | `precondition` | The second of two concurrent `decide` calls on one proposal: it read the proposal before the first stored its verdicts. |
| `proposal_stale` | `conflict` | `decide` after `base_of` moved past the proposal's base. |
| `proposal_verdict_unknown` | `validation` | A verdict names an item the proposal does not have. |
| `proposal_answer_required` | `validation` | Accepting a question, or an item that proposes nothing, without a `change`. |

## What stays out

- **No automatic acceptance.** Nothing in the kit accepts without a principal, and no
  threshold accepts at all. An application that wants automation composes a deterministic
  proposer with its own handler, visibly.
- **The guarantee is the kit's operations, not the table.** Proposals are documents, so
  any code holding the document port can still change one. Enforcement lives in `decide`,
  and in your choice to route decisions through it.
- **No review UI, notification or assignment.** Those are yours.

The collection is catalogued as the kit's, not forze's, because its items are your models:
`forze schema --specs` does not print its table, and you migrate it as your own. To replace
a generated operation, pass `handlers=`; to add one, `extra_ops=`. The design and its
decisions are in RFC 0039.
