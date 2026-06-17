#!/usr/bin/env bash
# Install the d2 diagram compiler used by `just build-diagrams`.
# Override the pinned version with the D2_VERSION environment variable.
set -euxo pipefail

D2_VERSION="${D2_VERSION:-v0.7.1}"

# Pin the artifact's SHA256 (d2 ships no upstream checksums file, so this is the
# hash of the v0.7.1 linux-amd64 tarball, verified once and pinned here). An
# overridden D2_VERSION must supply its own D2_SHA256.
if [ "$D2_VERSION" = "v0.7.1" ]; then
	D2_SHA256="${D2_SHA256:-eb172adf59f38d1e5a70ab177591356754ffaf9bebb84e0ca8b767dfb421dad7}"
else
	D2_SHA256="${D2_SHA256:?overriding D2_VERSION requires D2_SHA256 for the tarball}"
fi

curl -LsSf -o /tmp/d2.tar.gz \
	"https://github.com/terrastruct/d2/releases/download/${D2_VERSION}/d2-${D2_VERSION}-linux-amd64.tar.gz"
echo "${D2_SHA256}  /tmp/d2.tar.gz" | sha256sum --check --strict

mkdir -p /tmp/d2-extract
tar -xzf /tmp/d2.tar.gz -C /tmp/d2-extract
sudo mv "$(find /tmp/d2-extract -type f -name d2 | head -n 1)" /usr/local/bin/d2
sudo chmod +x /usr/local/bin/d2

d2 --version
