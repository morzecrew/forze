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

Verify the installation:

```bash
uv run python -c "from forze._version import __version__; print(__version__)"
```

## Install with integrations

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
