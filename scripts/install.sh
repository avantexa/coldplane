#!/usr/bin/env sh
set -e

# ---------------------------------------------------------------------------
# coldplane CLI installer
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/avantexa/coldplane/main/scripts/install.sh | sh
#
# Detects OS and architecture, downloads the latest binary from GitHub
# Releases, checks it against the release's checksums.txt, and installs it
# to /usr/local/bin.
#
# Environment variables:
#   COLDPLANE_INSTALL_DIR   — install directory (default: /usr/local/bin);
#                             a directory you own needs no sudo, e.g. ~/.local/bin
#   COLDPLANE_RELEASES_URL  — where to download from (default: the latest release)
# ---------------------------------------------------------------------------

RELEASES_URL="${COLDPLANE_RELEASES_URL:-https://github.com/avantexa/coldplane/releases/latest/download}"
INSTALL_DIR="${COLDPLANE_INSTALL_DIR:-/usr/local/bin}"
BINARY="coldplane"

main() {
    os="$(detect_os)"
    arch="$(detect_arch)"

    if [ -z "$os" ] || [ -z "$arch" ]; then
        echo "Error: Unsupported platform: $(uname -s)/$(uname -m)" >&2
        exit 1
    fi

    filename="${BINARY}-${os}-${arch}"
    url="${RELEASES_URL}/${filename}"

    echo "Installing coldplane..."
    echo "  Platform: ${os}/${arch}"
    echo "  URL:      ${url}"
    echo ""

    tmpdir="$(mktemp -d)"
    trap 'rm -rf "$tmpdir"' EXIT

    fetch "$url" "$tmpdir/$BINARY"
    fetch "${RELEASES_URL}/checksums.txt" "$tmpdir/checksums.txt"
    verify_checksum "$tmpdir/$BINARY" "$tmpdir/checksums.txt" "$filename"

    chmod +x "$tmpdir/$BINARY"
    install_binary "$tmpdir/$BINARY"

    echo ""
    echo "coldplane installed to $INSTALL_DIR/$BINARY"
    "$INSTALL_DIR/$BINARY" version
    echo ""
    echo "Run 'hash -r' or open a new terminal, then get started:"
    echo "  coldplane analyze --help"
    echo "  coldplane backup create --help"
}

# fetch URL FILE downloads URL to FILE with curl or wget.
fetch() {
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$1" -o "$2" || { echo "Error: download failed: $1" >&2; exit 1; }
    elif command -v wget >/dev/null 2>&1; then
        wget -qO "$2" "$1" || { echo "Error: download failed: $1" >&2; exit 1; }
    else
        echo "Error: curl or wget is required" >&2
        exit 1
    fi
}

# verify_checksum FILE CHECKSUMS NAME refuses FILE unless its SHA-256 is the
# one CHECKSUMS lists for NAME.
verify_checksum() {
    expected="$(awk -v n="$3" '$2 == n { print $1 }' "$2")"
    if [ -z "$expected" ]; then
        echo "Error: checksums.txt has no entry for $3" >&2
        exit 1
    fi
    if command -v sha256sum >/dev/null 2>&1; then
        actual="$(sha256sum "$1" | awk '{ print $1 }')"
    else
        actual="$(shasum -a 256 "$1" | awk '{ print $1 }')"
    fi
    if [ "$actual" != "$expected" ]; then
        echo "Error: checksum mismatch for $3" >&2
        echo "  expected: $expected" >&2
        echo "  actual:   $actual" >&2
        exit 1
    fi
    echo "  Checksum: ok"
}

# install_binary FILE moves FILE into INSTALL_DIR, with sudo only when the
# directory is not writable.
install_binary() {
    if mkdir -p "$INSTALL_DIR" 2>/dev/null && [ -w "$INSTALL_DIR" ]; then
        mv "$1" "$INSTALL_DIR/$BINARY"
    else
        echo "  Installing to $INSTALL_DIR (requires sudo)..."
        sudo mkdir -p "$INSTALL_DIR"
        sudo mv "$1" "$INSTALL_DIR/$BINARY"
    fi
}

detect_os() {
    case "$(uname -s)" in
        Linux*)  echo "linux" ;;
        Darwin*) echo "darwin" ;;
        *)       echo "" ;;
    esac
}

detect_arch() {
    # A shell under Rosetta reports x86_64 on Apple Silicon; install the native binary.
    if [ "$(uname -s)" = "Darwin" ] && [ "$(sysctl -n sysctl.proc_translated 2>/dev/null)" = "1" ]; then
        echo "arm64"
        return
    fi
    case "$(uname -m)" in
        x86_64|amd64)  echo "amd64" ;;
        arm64|aarch64) echo "arm64" ;;
        *)             echo "" ;;
    esac
}

main
