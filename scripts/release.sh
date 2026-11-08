#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Publish a coldplane release to GitHub.
#
# Usage:
#   ./scripts/release.sh v0.1.0 notes.md            # build, then publish
#   SKIP_BUILD=1 ./scripts/release.sh v0.1.0 notes.md   # publish dist/ as built
#
# Releases only a commit that CI already published: HEAD must be clean,
# equal to origin/main and to GitHub's main. The tag is created on GitHub by
# the release (no git push to GitHub) and pushed to origin.
#
# Needs gh, logged in with release rights on avantexa/coldplane.
# ---------------------------------------------------------------------------

REPO="avantexa/coldplane"
VERSION="${1:?usage: $0 vX.Y.Z NOTES.md}"
NOTES="${2:?usage: $0 vX.Y.Z NOTES.md}"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

die() { printf "\033[1;31merror:\033[0m %s\n" "$*" >&2; exit 1; }
log() { printf "\033[1;36m==>\033[0m %s\n" "$*"; }

[[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "version must look like v1.2.3, got $VERSION"
[ -f "$NOTES" ] || die "no notes file: $NOTES"
command -v gh >/dev/null || die "gh is not installed (brew install gh)"
gh auth status >/dev/null 2>&1 || die "gh is not logged in (gh auth login)"

[ -z "$(git status --porcelain)" ] || die "working tree is not clean"
SHA="$(git rev-parse HEAD)"
git fetch -q origin main
[ "$SHA" = "$(git rev-parse origin/main)" ] || die "HEAD is not origin/main; push first"
[ "$SHA" = "$(gh api "repos/$REPO/commits/main" --jq .sha)" ] || die "GitHub main is not HEAD yet; wait for CI to publish it"
if git rev-parse -q --verify "refs/tags/$VERSION" >/dev/null || gh release view "$VERSION" --repo "$REPO" >/dev/null 2>&1; then
    die "$VERSION already exists"
fi

if [ -z "${SKIP_BUILD:-}" ]; then
    rm -rf dist
    VERSION="$VERSION" ./scripts/build.sh all
fi

ASSETS=(dist/coldplane-darwin-arm64 dist/coldplane-darwin-amd64 dist/coldplane-linux-amd64 dist/coldplane-linux-arm64 dist/coldplane-windows-amd64.exe dist/checksums.txt)
for f in "${ASSETS[@]}"; do [ -s "$f" ] || die "missing $f"; done
(cd dist && shasum -a 256 -c checksums.txt >/dev/null) || die "dist/checksums.txt does not match the binaries"

# The binary for this machine must report the version and commit being released.
native="dist/coldplane-$(go env GOOS)-$(go env GOARCH)"
"$native" version | grep -q "^coldplane $VERSION\$" || die "$native is not $VERSION"
"$native" version | grep -q "commit:  $(git rev-parse --short HEAD)\$" || die "$native was not built from HEAD"

log "Releasing $VERSION at $SHA"
gh release create "$VERSION" --repo "$REPO" --target "$SHA" --title "coldplane $VERSION" --notes-file "$NOTES" "${ASSETS[@]}"
# Mirror the tag GitHub just created onto origin.
git tag -a "$VERSION" -m "coldplane $VERSION" "$SHA"
git push origin "$VERSION"
log "Done: https://github.com/$REPO/releases/tag/$VERSION"
