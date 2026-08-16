# Agent Skills

Forze ships an AI agent skill for **applications that use Forze as a dependency**. Install it so assistants follow correct wiring, specs, handlers, and integration patterns in your service repo.

It follows the [Agent Skills](https://agentskills.io/) format. Maintainers: see [AUTHORING.md](AUTHORING.md).

## Installation

```bash
npx skills add morzecrew/forze
```

That installs `forze-skills` — one skill, one description in your agent's context, and 43 reference files it reads only when a task needs them.

There is no per-skill install. Forze's material is a graph of mutually-dependent procedures rather than a set of independent rules, so installing a fragment of it produced dangling navigation and advice with its prerequisites missing.

## Usage

The skill loads when the agent detects a relevant task (handlers, wiring, Temporal, auth, and so on). Its `SKILL.md` is a routing index: a mental model, a task-keyed table naming **every** reference a task needs, and an index of all 43.

Most tasks need three to five references. The index says so explicitly, because reading one and stopping is the failure mode of this shape.

| I want to… | The skill routes to |
|---|---|
| Bootstrap a new service | architecture → spec naming → deps resolution → runtime lifecycle |
| Add a governed aggregate | models → document spec → AggregateKit → backend config → mock tests |
| Write a custom handler | execution context → handlers → query DSL |
| Expose it over HTTP | FastAPI setup → generated routes → identity |
| Encrypt a field | field encryption → KMS backends → backend config |
| Simulate under faults | DST simulation → invariants → mock tests |

## Documentation

References link to the published docs at [morzecrew.github.io/forze](https://morzecrew.github.io/forze/latest/), which are **versioned**: links use the `latest` alias, and `SKILL.md` notes how to swap `latest` for an older pinned `forze` minor. Framework contribution is documented in the Forze repository (`AGENTS.md`, `CONTRIBUTING.md`), not in this skill.
