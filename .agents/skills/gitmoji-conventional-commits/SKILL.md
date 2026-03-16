---
name: gitmoji-conventional-commits
description: Format all git commit messages using Conventional Commits with a gitmoji prefix. Automatically apply whenever generating or suggesting a commit message.
---

# Git Commit Formatter (Deterministic Gitmoji + Conventional Commits)

You MUST output a commit message that follows this format:

`<gitmoji> <type>[optional scope][!]: <description>`

Examples:

```text
✨ feat(api): add OAuth login support
🔥 refactor(cache): remove deprecated cache layer
🚑 fix(auth): patch token validation vulnerability
```

## Automatic Application

This skill MUST be applied whenever the assistant:

- generates a git commit message
- suggests a commit message
- prepares a release plan that includes a commit message
- responds to prompts like: "commit this", "write a commit", "generate a commit message", "prepare commit", or similar

Apply it even if the user does not explicitly mention Conventional Commits or gitmoji.

## Deterministic Rule

The assistant MUST:

1. Choose the **gitmoji first**
2. Use the **type mapped to that gitmoji**
3. NEVER invent new gitmoji
4. NEVER invent new commit types

## Gitmoji → Conventional Commit Type Mapping

### Features

| Gitmoji | Meaning | Type |
|--------|--------|------|
| ✨ | New feature | feat |
| 🚸 | UX improvements | feat |
| 📊 | Analytics / tracking | feat |
| 💬 | Text / literals | feat |
| 🌱 | Seed data | feat |
| 🗃 | Database changes | feat |
| 🧵 | Multithreading / concurrency | feat |
| 🦺 | Validation | feat |
| 🦖 | Backwards compatibility | feat |
| 🛂 | Authorization / permissions | feat |
| 🧭 | Feature flags | feat |
| 🩺 | Healthchecks | feat |
| 🥚 | Easter egg | feat |

### Bug Fixes

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 🐛 | Bug fix | fix |
| 🚑 | Critical hotfix | fix |
| 🩹 | Small fix | fix |
| 🚨 | Fix linter / compiler warnings | fix |
| 🎯 | Catch errors | fix |

### Refactoring / Code Changes

| Gitmoji | Meaning | Type |
|--------|--------|------|
| ♻️ | Refactor code | refactor |
| 🔥 | Remove code/files | refactor |
| 💩 | Bad code needing improvement | refactor |
| 🚚 | Move/rename files | refactor |
| 🗑 | Deprecate code | refactor |
| ⚰️ | Remove dead code | refactor |
| 🏗 | Architectural changes | refactor |

### Style / Formatting

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 🎨 | Code formatting / structure | style |

### Performance

| Gitmoji | Meaning | Type |
|--------|--------|------|
| ⚡️ | Performance improvements | perf |

### Documentation

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 📝 | Documentation | docs |
| 💡 | Code comments | docs |
| ✏️ | Fix typos | docs |

### Testing

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 🧪 | Tests | test |
| 🤡 | Mocks | test |
| 📸 | Snapshots | test |

### Dependencies / Build

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 📦 | Packages / compiled files | build |
| ⬆️ | Upgrade dependencies | build |
| ⬇️ | Downgrade dependencies | build |
| 📌 | Pin dependencies | build |
| ➕ | Add dependency | build |
| ➖ | Remove dependency | build |
| 🧱 | Infrastructure | build |

### CI

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 👷 | CI configuration | ci |
| 💚 | Fix CI build | ci |

### Maintenance / Chore

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 🔧 | Maintenance | chore |
| 🔨 | Dev scripts | chore |
| 🙈 | .gitignore | chore |
| 🧪 | Experiments | chore |
| 🕵️ | Data exploration | chore |
| 🧑‍💻 | Developer experience | chore |
| 🔖 | Release / version tags | chore |
| 🚀 | Deployment | chore |
| 🚧 | Work in progress | chore |
| 🍻 | Code written drunkenly | chore |
| 🔀 | Merge branches | chore |

### Security

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 🔒 | Security changes | security |

### Reverts

| Gitmoji | Meaning | Type |
|--------|--------|------|
| ⏪ | Revert commit | revert |


### Breaking Changes

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 💥 | Introduce breaking changes | feat |

## Choosing the Gitmoji

Choose the gitmoji that **best represents the main change**.

If multiple changes exist, priority order: fix > feat > perf > refactor > build > docs > test > chore

## Scope

Use scope when it improves clarity. Common scopes: auth, api, core, cli, ui, deps, build, ci, db.

Examples:

```text
✨ feat(auth): add OAuth login
⬆️ build(deps): upgrade fastapi
🔥 refactor(cache): remove legacy cache
```

If scope is not obvious, omit it.

## Description Rules

The description MUST:

- be imperative
- be a single line
- be concise
- be ≤ 72 characters when possible
- not end with a period
- not start with list markers

Correct:

`✨ feat(api): add OAuth login support`

Incorrect:

`✨ feat(api): Added OAuth login support.`

Incorrect:

`- ✨ feat(api): add OAuth login support`

## Breaking Changes

If commit introduces a breaking change, use this format `<gitmoji> <type>[optional scope][!]: <description>` with a footer `BREAKING CHANGE: <details>`

Example:

```text
💥 feat(api)!: redesign authentication API

BREAKING CHANGE: authentication endpoints changed to OAuth2
```

## Commit Body (optional)

Use when context is helpful:

- there are 2+ meaningful changes to summarize, OR
- the subject needs context, OR
- you want to preserve "bullet-style change notes"

Body format:

- A blank line after the subject
- Then a short paragraph (optional)
- Then bullet list using "-" only (no "*", no "+")
- Keep bullets concise and action-oriented

Example:

```text
✨ feat(auth): add OAuth login

- add Google provider
- add GitHub provider
- store refresh tokens securely
```

## Footer (optional)

Supported footers:

- `BREAKING CHANGE: ...`
- `Closes #123`
- `Refs #123`

## Output Requirements

- Output ONLY the commit message
- No explanations
- No alternatives unless requested
