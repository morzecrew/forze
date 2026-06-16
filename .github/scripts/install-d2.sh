#!/usr/bin/env bash
# Install the d2 diagram compiler used by `just build-diagrams`.
# Override the pinned version with the D2_VERSION environment variable.
set -euxo pipefail

D2_VERSION="${D2_VERSION:-v0.7.1}"

curl -LsSf -o /tmp/d2.tar.gz \
	"https://github.com/terrastruct/d2/releases/download/${D2_VERSION}/d2-${D2_VERSION}-linux-amd64.tar.gz"

mkdir -p /tmp/d2-extract
tar -xzf /tmp/d2.tar.gz -C /tmp/d2-extract
sudo mv "$(find /tmp/d2-extract -type f -name d2 | head -n 1)" /usr/local/bin/d2
sudo chmod +x /usr/local/bin/d2

d2 --version
