#!/bin/sh
# Install paceq from a GitHub release (issue #43).
#
# Downloads the right archive for the platform, verifies its sha256 against
# the release's checksums.txt, and puts the binary in $PREFIX/bin. Verification
# is not optional: a script that skips it teaches a bad habit we cannot undo
# when signatures arrive with M8-04.
#
# Environment:
#   PACEQ_VERSION  the tag to install ("v0.1.0"); default: latest release
#   PREFIX         install root for bin/paceq;   default: ~/.local
set -eu

REPO="a-holm/paceq"
VERSION="${PACEQ_VERSION:-latest}"
PREFIX="${PREFIX:-$HOME/.local}"

die() {
	printf 'install.sh: %s\n' "$*" >&2
	exit 1
}

case "$(uname -s)" in
Linux) os=linux ;;
Darwin) os=darwin ;;
*)
	die "unsupported operating system '$(uname -s)': paceq ships Linux and macOS binaries"
	;;
esac

case "$(uname -m)" in
x86_64 | amd64) arch=amd64 ;;
aarch64 | arm64) arch=arm64 ;;
*)
	die "unsupported architecture '$(uname -m)': paceq ships amd64 and arm64 binaries"
	;;
esac

command -v curl >/dev/null 2>&1 ||
	die "curl is required to download paceq"

# sha256sum exists on Linux, shasum ships with macOS. Verification needs one
# of them; there is no third option that skips the check.
if command -v sha256sum >/dev/null 2>&1; then
	sum() {
		sha256sum -c -
	}
elif command -v shasum >/dev/null 2>&1; then
	sum() {
		shasum -a 256 -c -
	}
else
	die "neither sha256sum nor shasum found: refusing to install without verification"
fi

if [ "$VERSION" = latest ]; then
	VERSION="$(
		curl -sSfL "https://api.github.com/repos/$REPO/releases/latest" |
			sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p'
	)"
	[ -n "$VERSION" ] || die "could not resolve the latest release of $REPO"
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

base="https://github.com/$REPO/releases/download/$VERSION"
tarball="paceq_${VERSION#v}_${os}_${arch}.tar.gz"

curl -sSfL "$base/$tarball" -o "$tmp/$tarball"
curl -sSfL "$base/checksums.txt" -o "$tmp/checksums.txt"

# Check the exact line for this tarball, so both a mismatched hash and a
# missing entry fail hard.
expected=$(awk -v t="$tarball" '$2 == t { print $1 }' "$tmp/checksums.txt")
[ -n "$expected" ] || die "checksums.txt carries no entry for $tarball"
printf '%s  %s\n' "$expected" "$tmp/$tarball" | sum >/dev/null ||
	die "checksum verification failed for $tarball"

tar xzf "$tmp/$tarball" -C "$tmp"

mkdir -p "$PREFIX/bin"
install -m 0755 "$tmp/paceq" "$PREFIX/bin/paceq"
printf 'installed paceq %s to %s/bin/paceq\n' "$VERSION" "$PREFIX"
"$PREFIX/bin/paceq" version
