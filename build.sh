#!/bin/sh
# Builds dist/mqtt-<version>.tar.gz: cross-compiles the bridge for the robot (linux/arm64) and packs package/.
# Needs Go and GNU tar; works in Linux, WSL and Git Bash.
set -eu
cd "$(dirname "$0")"
GO=${GO:-go}
VERSION=$(sed -n 's/.*"version": *"\([^"]*\)".*/\1/p' package/manifest.json)
BUILD=$(mktemp -d)
trap 'rm -rf "$BUILD"' EXIT
(cd src && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 "$GO" build -trimpath -ldflags "-s -w -X main.version=$VERSION" -o "$BUILD/mqtt-bridge" .)
mkdir -p dist
OUT="dist/mqtt-$VERSION.tar.gz"
# Modes are set by tar: a Windows drive does not keep them.
tar --owner=0 --group=0 --mode=0644 -cf "$BUILD/pkg.tar" -C package .
tar --owner=0 --group=0 --mode=0755 -rf "$BUILD/pkg.tar" -C "$BUILD" mqtt-bridge
gzip -9 -c "$BUILD/pkg.tar" > "$OUT"
sha256sum "$OUT"
