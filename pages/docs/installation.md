# Installation

Forze is published on **PyPI** and can be installed using standard Python package managers.

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) (development only)
- [just](https://github.com/casey/just) (development only)

## Install

/// tab | uv

    :::bash
    uv add forze
///

/// tab | pip

    :::bash
    pip install forze
///

## Optional Integrations

Forze provides optional integrations for common infrastructure components. Install them via extras:

/// tab | uv

    :::bash
    uv add 'forze[fastapi,postgres,redis,s3,mongo,temporal]'
///

/// tab | pip

    :::bash
    pip install 'forze[fastapi,postgres,redis,s3,mongo,temporal]'
///

## Development Installation

To install the project for local development:

    :::bash
    git clone https://github.com/morzecrew/forze
    cd forze
    uv sync --all-groups --all-extras

This installs all development dependencies defined in the project configuration.
