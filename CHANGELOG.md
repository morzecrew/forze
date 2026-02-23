# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Infra layer (`forze.infra`) with Postgres, Redis, S3, and Temporal providers (gateways, platform clients, shared errors and codecs).

### Changed

- **Postgres filter builder** (`forze.infra.providers.postgres.builder`): filter input now accepts only canonical operator names (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `is_null`, `or`, array and ltree ops). Aliases such as `==`, `ge`, `not in`, `in_`, `or_` are no longer accepted and raise `ValidationError`. Use `in` and `or` (not `in_`/`or_`) for membership and disjunction. Contract: `specs/001-postgres-query-builder-refactor/contracts/filter-input.md`.

### Fixed

- ...

## [0.1.1] - 2026-02-23

### Added

- Initial DDD/Hex contracts: ports, results, errors.

### Fixed

- Packaging metadata for PyOCI classifiers.

[unreleased]: https://github.com/morzecrew/forze/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/morzecrew/forze/releases/tag/v0.1.1
