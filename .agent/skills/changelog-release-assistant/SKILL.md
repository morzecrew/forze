---
name: changelog-release-assistant
description: Maintain CHANGELOG.md in Keep a Changelog format and prepare tag-based releases (GHA publishes to OCI via PyOCI and creates GitHub Release from CHANGELOG).
---

# Changelog & Tag-Release Assistant

This skill helps keep `CHANGELOG.md` accurate and release-ready following **Keep a Changelog** format.
Releases are **tag-driven**: pushing a tag `vX.Y.Z` triggers GitHub Actions to publish the OCI artifact (via PyOCI) and create a GitHub Release using changelog content.

## Use this skill when

- You want to add entries to `## [Unreleased]`
- You want to categorize changes into `Added`, `Changed`, `Fixed`.
- You want to prepare a new release section `## [X.Y.Z] - YYYY-MM-DD`
- You want a release checklist (manual commands only)

## Do not use this skill when

- There is no `CHANGELOG.md` or it does not follow **Keep a Changelog** format
- The user asks the agent to tag, push, or publish without review/confirmation

## Repository Conventions (MUST FOLLOW)

### Scope of Changelog Entries

The changelog must include **only user-relevant product changes**.

For this repository, that means:

- Changes to code inside `src/`
- Changes affecting public APIs
- New domain primitives, contracts, behaviors
- Behavioral changes in existing modules
- Packaging changes that affect installation or runtime behavior

The changelog must NOT include:

- Test changes (`tests/`)
- CI/CD workflow updates
- GitHub Actions changes
- Agent skills or internal tooling
- Documentation-only changes
- Formatting or lint-only changes
- Refactors with no observable behavior impact
- Internal development tooling adjustments

If a change does not affect how users import, use, or install the library,
it should generally be excluded from the changelog.

### Notability Rule

Only include changes that are meaningful to library consumers.

Do not include:

- trivial renames
- internal restructuring
- minor refactors without API impact
- small code cleanups

### Changelog format

`CHANGELOG.md` must remain in this structure:

- `## [Unreleased]`
  - `### Added`
  - `### Changed`
  - `### Fixed`
- Version sections:
  - `## [X.Y.Z] - YYYY-MM-DD`
  - same categories inside as needed
- Reference links at the bottom exist (e.g. `[unreleased]: ...`, `[0.1.1]: ...`)

**Important:** The user updates the reference links manually after releasing.  
Do not add/modify bottom links unless explicitly asked.

### Release trigger

- Release is triggered by pushing a Git tag: `vX.Y.Z`
- GitHub Actions:
  - reads the changelog section for the version
  - publishes the OCI artifact via PyOCI (ghcr.io)
  - creates GitHub Release

## Categorization rules

- **Added**: new public APIs, new primitives, new modules, new features.
- **Changed**: behavior changes, refactors affecting usage, CI/release process changes (only if notable for users).
- **Fixed**: bug fixes, packaging/metadata fixes, build/installation fixes.
- Avoid duplicates between categories.
- Keep entries short, neutral, and user-focused.

## Workflow A — Update `Unreleased`

When the user says "add to changelog" or provides a list of changes:

1. Extract notable changes (from user summary, commits, PR descriptions, diffs, etc.).
2. Place each change under the best category in `## [Unreleased]`.
3. Preserve existing formatting and spacing.
4. Output:
   - a patch/diff OR
   - the updated `## [Unreleased]` block

## Workflow B — Prepare a release `X.Y.Z`

When the user says "prepare release 0.1.2" (or similar):

1. Use the target version `X.Y.Z`.
2. Use today's date in `YYYY-MM-DD` (user timezone).
3. Create a new section directly under `Unreleased` (or above the previous version entries):

   `## [X.Y.Z] - YYYY-MM-DD`

4. Move content from `## [Unreleased]` into that new version section:
   - Keep the same category headings.
   - If a category in Unreleased is empty (only `- ...`), do not copy it.
5. Reset `## [Unreleased]` categories back to placeholder state (`- ...`).
6. Do **not** edit reference links at the bottom.
7. Output the proposed changelog edits (diff or full updated blocks).
8. Provide a manual release plan (commands shown only; do not execute).

## Output format (always)

When updating the changelog, always produce:

1. Proposed `CHANGELOG.md` changes (diff or updated sections)
2. Any assumptions or TBDs (if applicable)
3. A short note that the tag-driven workflow will publish after the user creates/pushes the release tag
