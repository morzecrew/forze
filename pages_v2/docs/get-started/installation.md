---
title: Installation
icon: lucide/download
---

## Requirements

Forze requires:

- Python 3.13 or newer
- [uv](https://docs.astral.sh/uv/){:target="_blank"} or another PEP 517-compatible package manager

## Install core

Install the framework core:

=== "uv"

    ```bash
    uv add forze
    ```

=== "pip"

    ```bash
    pip install forze
    ```

## Install integrations

Most functionality is provided through optional integrations packed as sibling packages alongside the core. To install required dependencies, use the `[extras]` syntax.

=== "postgres"

    ```bash
    uv add 'forze[postgres]'
    ```

=== "redis"

    ```bash
    uv add 'forze[redis]'
    ```

=== "fastapi"

    ```bash
    uv add 'forze[fastapi]'
    ```

=== "multiple"

    ```bash
    uv add 'forze[fastapi,postgres,redis]'
    ```

## Development setup

Clone the repository and install the dependencies:

```bash
git clone https://github.com/morzecrew/forze
cd forze

uv sync --all-groups --all-extras
```

For comfortable development it's recommended to use [just](https://github.com/casey/just){:target="_blank"} to run commands.

```bash
# Run unit and integration tests
just test

# Run performance tests
just perf

# Run quality checks (-s flag for strict mode)
just quality
```
