# Installation Guide

`forze` is distributed via **GitHub Container Registry (GHCR)** and proxied through **PyOCI**.
Because the package is hosted in a private registry, authentication is required during installation.
This guide covers local setup and CI environments.

## How Authentication Works

Package managers such as `uv` and `poetry` use two environment variables to authenticate against the PyOCI index.
For `uv`, these are:

- **`UV_INDEX_PYOCI_USERNAME`** — any non-empty string (typically `"github"`)
- **`UV_INDEX_PYOCI_PASSWORD`** — a personal access token (PAT) with permission to read organization packages

## Local Development

If you are using the GitHub CLI:

```bash
export UV_INDEX_PYOCI_USERNAME="github"
export UV_INDEX_PYOCI_PASSWORD=$(gh auth token)
```

Then install:

```bash
uv add forze --index pyoci=https://pyoci.com/ghcr.io/morzecrew/

# with extras
uv add 'forze[fastapi,postgres]' --index pyoci=https://pyoci.com/ghcr.io/morzecrew/
```

### Recommended: Use direnv

To avoid exporting credentials manually each time, use [direnv](https://direnv.net).
Create a `.envrc` file in your project root:

```bash
export UV_INDEX_PYOCI_USERNAME="github"
export UV_INDEX_PYOCI_PASSWORD=$(gh auth token)
```

Then run:

```bash
direnv allow
```

Credentials stay local to the project environment.

## CI Environments

In CI (e.g. GitHub Actions), set the same environment variables used locally.
Example for `uv`:

```yaml
permissions:
  packages: read   # Required for private registry access

env:
  UV_INDEX_PYOCI_USERNAME: github
  UV_INDEX_PYOCI_PASSWORD: ${{ secrets.GITHUB_TOKEN }}
```

Ensure the workflow has `packages: read` permission.

## Security Notes

- Never commit tokens to source control.
- Avoid hardcoding credentials in configuration files.
- Prefer short-lived tokens with minimal permissions.