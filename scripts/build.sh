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
#   DIST_DIR  — output directory inside the repo (default: dist)
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

export GOTOOLCHAIN=auto

DIST_DIR="${DIST_DIR:-dist}"
LINUX_IMAGE="coldplane-builder-linux"
WINDOWS_IMAGE="coldplane-builder-windows"
# Newest glibc the Linux binaries may need (Ubuntu 22.04 has 2.35, RHEL 9 2.34).
MAX_GLIBC="2.34"
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

# The Docker builds write through the repo mount, so the output must be in it.
case "$DIST_DIR" in
    /*|..*) echo "DIST_DIR must be a relative path inside the repo, got $DIST_DIR" >&2; exit 1 ;;
esac
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

# ensure_docker_image IMAGE DOCKERFILE — (re)build the image; layer caching
# makes this quick when the Dockerfile is unchanged, and an edit always lands.
ensure_docker_image() {
    log "Docker image: $1 ($2)"
    docker build -q -t "$1" -f "$2" . >/dev/null
}

# build_docker IMAGE GOOS GOARCH CC CXX OUTPUT
build_docker() {
    local image=$1 goos=$2 goarch=$3 cc=$4 cxx=$5 output=$6
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
        "$image" \
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
    ensure_docker_image "$LINUX_IMAGE" Dockerfile.build
    build_docker "$LINUX_IMAGE" linux amd64 x86_64-linux-gnu-gcc x86_64-linux-gnu-g++ "$DIST_DIR/$BINARY-linux-amd64"
    build_docker "$LINUX_IMAGE" linux arm64 aarch64-linux-gnu-gcc aarch64-linux-gnu-g++ "$DIST_DIR/$BINARY-linux-arm64"
    check_glibc "$DIST_DIR/$BINARY-linux-amd64"
    check_glibc "$DIST_DIR/$BINARY-linux-arm64"
}

# check_glibc OUTPUT — fail when a Linux binary needs a glibc newer than MAX_GLIBC.
check_glibc() {
    local newest
    newest="$(docker run --rm -v "$ROOT_DIR:/build" -w /build "$LINUX_IMAGE" \
        sh -c "objdump -T '$1' | grep -o 'GLIBC_[0-9.]*' | sed 's/GLIBC_//' | sort -uV | tail -1")"
    if [ "$(printf '%s\n%s\n' "$MAX_GLIBC" "$newest" | sort -V | tail -1)" != "$MAX_GLIBC" ]; then
        fail "$1 needs glibc $newest, more than $MAX_GLIBC"
        return 1
    fi
    ok "$1 needs glibc $newest (max $MAX_GLIBC)"
}

build_windows() {
    log "--- Windows ---"
    ensure_docker_image "$WINDOWS_IMAGE" Dockerfile.build-windows
    # Use the -posix threading variant — DuckDB's static lib is built with pthreads.
    build_docker "$WINDOWS_IMAGE" windows amd64 x86_64-w64-mingw32-gcc-posix x86_64-w64-mingw32-g++-posix "$DIST_DIR/$BINARY-windows-amd64.exe"
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
