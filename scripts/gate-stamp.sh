#!/bin/sh
# A gate stamp is a claim that one make target exited zero against one exact
# working tree. The claim is worth exactly as much as its key, so the key covers
# every input the gate reads that no tracked file records: the content, path and
# executable bit of every tracked and untracked-but-not-ignored file, the Go
# toolchain and the go env settings that change what it produces, the make
# variables the environment can override, and whether the optional tools the
# gate branches on are installed. A target that printed SKIP because a tool was
# missing was never proven, so the tool set has to be in the key too.
#
# The stamp file lives under the per-worktree $GIT_DIR. It cannot be staged,
# cannot be committed, and one worktree cannot vouch for another.
#
# Nothing here ever decides that a target passed. Only scripts/gate-run.sh
# records, and only after make exited zero. Every uncertainty here answers "not
# proven", so the gate runs.
#
# Issue #176.
set -eu

self=${0##*/}

# Bump this when the key stops meaning what it meant. Every stamp written under
# an older version stops matching, which is the point.
KEY_VERSION='paceq-gate-stamp-key v2'

# The name is unknown to git, so `git rev-parse --git-path` resolves it under
# the per-worktree $GIT_DIR rather than the shared $GIT_COMMON_DIR.
STAMP_NAME=paceq-gate-stamps

# How long a stamp stays honoured. Not every input is a file: the vulnerability
# database govulncheck queries changes without this tree changing, and no key
# can see that. The window bounds how long such a change can hide behind a
# stamp.
TTL=${PACEQ_GATE_STAMP_TTL:-43200}

# The go env settings that change what the toolchain produces or refuses.
# GOVERSION carries the toolchain identity that `go version` prints.
GO_ENV_KEYS='GOVERSION GOOS GOARCH GOAMD64 GOARM64 CGO_ENABLED GOFLAGS GOEXPERIMENT GOTOOLCHAIN GOPROXY GOPRIVATE'

# The optional tools the Makefile branches on. Each one decides whether a target
# runs or prints SKIP, so its presence belongs in the key.
OPTIONAL_TOOLS="docker shellcheck ${PROMTOOL:-promtool}"

usage() {
	cat >&2 <<USAGE
usage: $self <command> [args]

  key                            print the content key for this tree and toolchain
  file                           print the path of the stamp file
  has <key> <target>             exit zero and print when <target> was proven for <key>
  record <key> <target> <secs>   record <target> as proven for <key>
  list                           print the live stamps
USAGE
	exit 2
}

stamp_file() {
	git rev-parse --git-path "$STAMP_NAME"
}

# tool_identity prints what the gate would find for one optional tool: where it
# is and what it calls itself, or that it is absent.
tool_identity() {
	if tool_path=$(command -v "$1" 2>/dev/null); then
		printf 'tool\t%s\t%s\t%s\n' "$1" "$tool_path" "$("$tool_path" --version 2>&1 | head -n 1)"
	else
		printf 'tool\t%s\tabsent\n' "$1"
	fi
}

# tree_fingerprint writes one record per path in the working tree: what kind of
# entry it is and, for anything with content, its blob hash. Paths are in the
# stream as well as hashes, so a rename that keeps the bytes is still a
# different tree.
#
# --no-filters is deliberate. Without it `git hash-object` runs the .gitattributes
# clean filters, and `* text=auto eol=lf` would hash a CRLF file to the same
# value as its LF twin. The gate's tools read the bytes on disk, so the key does
# too.
tree_fingerprint() {
	work=$1
	paths=$work/paths
	kinds=$work/kinds
	hashable=$work/hashable

	git ls-files -z --cached --others --exclude-standard >"$paths"

	: >"$kinds"
	: >"$hashable"
	if [ -s "$paths" ]; then
		# A tracked path deleted from the working tree is still listed by
		# --cached, and `git hash-object` cannot read it. It is recorded as
		# gone instead, which changes the key exactly like an edit.
		xargs -0 sh -c '
			for p do
				if [ -L "$p" ]; then
					printf "link\t%s\t%s\n" "$p" "$(readlink "$p")"
				elif [ ! -e "$p" ]; then
					printf "gone\t%s\n" "$p"
				elif [ -x "$p" ]; then
					printf "exec\t%s\n" "$p"
				else
					printf "file\t%s\n" "$p"
				fi
			done
		' sh <"$paths" >"$kinds"

		xargs -0 sh -c '
			for p do
				if [ -e "$p" ] && [ ! -L "$p" ]; then
					printf "%s\0" "$p"
				fi
			done
		' sh <"$paths" >"$hashable"
	fi

	cat "$kinds"
	if [ -s "$hashable" ]; then
		xargs -0 git hash-object --no-filters <"$hashable"
	fi
}

key() {
	work=$(mktemp -d)
	trap 'rm -rf "$work"' EXIT INT TERM

	{
		printf '%s\n' "$KEY_VERSION"
		# One go process for the whole toolchain identity. A key that
		# survived a toolchain upgrade would be a lie.
		"${GO:-go}" env $GO_ENV_KEYS | sed 's/^/goenv\t/'
		# The make variables the environment can override. The pins that
		# are not overridable live in the tracked Makefile and are covered
		# by its file hash.
		printf 'vars\t%s\n' "${PACEQ_GATE_VARS:-}"
		for tool in $OPTIONAL_TOOLS; do
			tool_identity "$tool"
		done
		tree_fingerprint "$work"
	} | sha256sum | cut -d' ' -f1
}

# has answers whether one target is proven for one key. It prints when the proof
# was taken, so the caller can say so. Anything short of an exact match on both
# fields, inside the window, is not proven.
has() {
	want_key=$1
	want_target=$2

	if [ -z "$want_key" ]; then
		return 1
	fi
	if [ "${PACEQ_GATE_STAMP:-1}" = 0 ]; then
		return 1
	fi
	file=$(stamp_file)
	if [ ! -f "$file" ] || [ ! -r "$file" ]; then
		return 1
	fi
	awk -v key="$want_key" -v target="$want_target" -v now="$(date +%s)" -v ttl="$TTL" '
		NF == 5 && $3 ~ /^[0-9]+$/ && now - $3 < ttl && $1 == key && $2 == target { proven = $4 }
		END { if (proven == "") { exit 1 } print proven }
	' "$file"
}

record() {
	rec_key=$1
	rec_target=$2
	rec_seconds=$3

	if [ -z "$rec_key" ]; then
		printf '%s: refusing to record %s without a key\n' "$self" "$rec_target" >&2
		return 1
	fi
	file=$(stamp_file)
	dir=${file%/*}
	if [ ! -d "$dir" ]; then
		printf '%s: no git directory at %s\n' "$self" "$dir" >&2
		return 1
	fi
	if [ -e "$file" ] && [ ! -f "$file" ]; then
		printf '%s: %s is not a regular file\n' "$self" "$file" >&2
		return 1
	fi

	now=$(date +%s)
	tmp=$file.$$
	{
		if [ -f "$file" ] && [ -r "$file" ]; then
			# Drop what has aged out and the previous line for this
			# target, so the file stays one line per proven target.
			awk -v now="$now" -v ttl="$TTL" -v key="$rec_key" -v target="$rec_target" '
				NF == 5 && $3 ~ /^[0-9]+$/ && now - $3 < ttl && !($1 == key && $2 == target)
			' "$file"
		fi
		printf '%s %s %s %s %s\n' \
			"$rec_key" "$rec_target" "$now" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$rec_seconds"
	} >"$tmp"
	mv "$tmp" "$file"
}

list() {
	file=$(stamp_file)
	if [ -f "$file" ] && [ -r "$file" ]; then
		cat "$file"
	fi
}

if [ $# -eq 0 ]; then
	usage
fi
action=$1
shift

case $action in
key)
	if [ $# -ne 0 ]; then usage; fi
	key
	;;
file)
	if [ $# -ne 0 ]; then usage; fi
	stamp_file
	;;
has)
	if [ $# -ne 2 ]; then usage; fi
	has "$1" "$2"
	;;
record)
	if [ $# -ne 3 ]; then usage; fi
	record "$1" "$2" "$3"
	;;
list)
	if [ $# -ne 0 ]; then usage; fi
	list
	;;
*)
	usage
	;;
esac
