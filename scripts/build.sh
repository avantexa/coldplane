#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# ColdPlane CLI — cross-platform build script
#
# Usage:
#   ./scripts/build.sh              # build all platforms
#   ./scripts/build.sh linux        # linux amd64 + arm64
#   ./scripts/build.sh macos        # macOS arm64 + amd64
#
# Environment variables:
#   VERSION   — version string baked into the binary (default: git tag or "dev")
#   DIST_DIR  — output directory (default: dist)
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

export GOTOOLCHAIN=auto

DIST_DIR="${DIST_DIR:-dist}"
DOCKER_IMAGE="coldplane-builder"
DOCKER_GOMOD_VOL="coldplane-gomod"
DOCKER_CACHE_VOL="coldplane-gocache"

# Resolve version: explicit env var > git tag > "dev"
if [ -z "${VERSION:-}" ]; then
    VERSION="$(git describe --tags --abbrev=0 2>/dev/null || echo "dev")"
fi

COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")"
BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

LDFLAGS="-s -w"
LDFLAGS="$LDFLAGS -X github.com/avantexa/coldplane/cmd.Version=$VERSION"
LDFLAGS="$LDFLAGS -X github.com/avantexa/coldplane/cmd.Commit=$COMMIT"
LDFLAGS="$LDFLAGS -X github.com/avantexa/coldplane/cmd.BuildDate=$BUILD_DATE"

BINARY="coldplane"

mkdir -p "$DIST_DIR"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log()  { printf "\033[1;36m==>\033[0m %s\n" "$*"; }
ok()   { printf "\033[1;32m  ✓\033[0m %s\n" "$*"; }
fail() { printf "\033[1;31m  ✗\033[0m %s\n" "$*"; }

# build_native GOOS GOARCH OUTPUT
build_native() {
    local goos=$1 goarch=$2 output=$3
    log "Building $goos/$goarch (native)"
    CGO_ENABLED=1 GOOS="$goos" GOARCH="$goarch" \
        go build -a -ldflags "$LDFLAGS" -o "$output" . && \
        ok "$output ($(du -h "$output" | cut -f1 | xargs))" || \
        { fail "$goos/$goarch build failed"; return 1; }
}

# ensure_docker_image — build the Docker image if not present
ensure_docker_image() {
    if ! docker image inspect "$DOCKER_IMAGE" >/dev/null 2>&1; then
        log "Building Docker image: $DOCKER_IMAGE"
        docker build -t "$DOCKER_IMAGE" -f Dockerfile.build .
    fi
}

# build_docker GOOS GOARCH CC CXX OUTPUT
build_docker() {
    local goos=$1 goarch=$2 cc=$3 cxx=$4 output=$5
    local outname
    outname="$(basename "$output")"

    log "Building $goos/$goarch (Docker, CC=$cc)"
    docker run --rm \
        -v "$ROOT_DIR:/build" \
        -v "$DOCKER_GOMOD_VOL:/go/pkg/mod" \
        -v "$DOCKER_CACHE_VOL:/root/.cache/go-build" \
        -w /build \
        -e CGO_ENABLED=1 \
        -e GOOS="$goos" \
        -e GOARCH="$goarch" \
        -e CC="$cc" \
        -e CXX="$cxx" \
        "$DOCKER_IMAGE" \
        go build -a -ldflags "$LDFLAGS" -o "$DIST_DIR/$outname" . && \
        ok "$output ($(du -h "$output" | cut -f1 | xargs))" || \
        { fail "$goos/$goarch build failed"; return 1; }
}

# ---------------------------------------------------------------------------
# Platform builders
# ---------------------------------------------------------------------------

build_macos() {
    log "--- macOS ---"
    build_native darwin arm64 "$DIST_DIR/$BINARY-darwin-arm64"
    build_native darwin amd64 "$DIST_DIR/$BINARY-darwin-amd64"
}

build_linux() {
    log "--- Linux ---"
    ensure_docker_image
    build_docker linux amd64 x86_64-linux-gnu-gcc x86_64-linux-gnu-g++ "$DIST_DIR/$BINARY-linux-amd64"
    build_docker linux arm64 aarch64-linux-gnu-gcc aarch64-linux-gnu-g++ "$DIST_DIR/$BINARY-linux-arm64"
}

build_windows() {
    log "--- Windows ---"
    ensure_docker_image
    # Use the -posix threading variant — DuckDB's static lib is built with pthreads.
    build_docker windows amd64 x86_64-w64-mingw32-gcc-posix x86_64-w64-mingw32-g++-posix "$DIST_DIR/$BINARY-windows-amd64.exe"
}

# ---------------------------------------------------------------------------
# Checksums + summary
# ---------------------------------------------------------------------------

generate_checksums() {
    log "Generating checksums"
    (cd "$DIST_DIR" && shasum -a 256 "$BINARY"-* > checksums.txt 2>/dev/null || true)
    ok "$DIST_DIR/checksums.txt"
}

summary() {
    echo ""
    log "Build complete — $DIST_DIR/"
    ls -lh "$DIST_DIR"/"$BINARY"-* 2>/dev/null || true
    echo ""
    if [ -f "$DIST_DIR/checksums.txt" ]; then
        cat "$DIST_DIR/checksums.txt"
    fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

TARGET="${1:-all}"

log "Version: $VERSION"

case "$TARGET" in
    macos)   build_macos ;;
    linux)   build_linux ;;
    windows) build_windows ;;
    all)
        build_macos
        build_linux
        build_windows
        ;;
    *)
        echo "Usage: $0 [all|macos|linux|windows]"
        exit 1
        ;;
esac

generate_checksums
summary
