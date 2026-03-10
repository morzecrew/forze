FROM mcr.microsoft.com/devcontainers/python:1-3.13-bookworm

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update  \
    && apt-get install -y --no-install-recommends \
    graphviz \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    curl -LsSf https://just.systems/install.sh | sh -s -- --to /usr/local/bin && \
    curl -fsSL https://d2lang.com/install.sh | sh -s --

ENV PATH="/home/vscode/.local/bin:${PATH}"

USER vscode
WORKDIR /workspaces/forze