#!/usr/bin/env sh
set -e

# ---------------------------------------------------------------------------
# coldplane CLI installer
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/avantexa/coldplane/main/scripts/install.sh | sh
#
# Detects OS and architecture, downloads the latest binary from GitHub
# Releases, and installs it to /usr/local/bin.
# ---------------------------------------------------------------------------

RELEASES_URL="https://github.com/avantexa/coldplane/releases/latest/download"
INSTALL_DIR="/usr/local/bin"
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

    # Download
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$url" -o "$tmpdir/$BINARY"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO "$tmpdir/$BINARY" "$url"
    else
        echo "Error: curl or wget is required" >&2
        exit 1
    fi

    chmod +x "$tmpdir/$BINARY"

    # Install
    if [ -w "$INSTALL_DIR" ]; then
        mv "$tmpdir/$BINARY" "$INSTALL_DIR/$BINARY"
    else
        echo "  Installing to $INSTALL_DIR (requires sudo)..."
        sudo mv "$tmpdir/$BINARY" "$INSTALL_DIR/$BINARY"
    fi

    echo ""
    echo "coldplane installed to $INSTALL_DIR/$BINARY"
    "$INSTALL_DIR/$BINARY" version
    echo ""
    echo "Run 'hash -r' or open a new terminal, then get started:"
    echo "  coldplane analyze --help"
    echo "  coldplane backup create --help"
}

detect_os() {
    case "$(uname -s)" in
        Linux*)  echo "linux" ;;
        Darwin*) echo "darwin" ;;
        *)       echo "" ;;
    esac
}

detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64)  echo "amd64" ;;
        arm64|aarch64) echo "arm64" ;;
        *)             echo "" ;;
    esac
}

main
