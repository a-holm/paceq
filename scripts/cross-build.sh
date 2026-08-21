#!/bin/sh
# Cross build one target and prove the artifact is cgo free, statically linked
# and within the size budget. Used by `make cross` and by the ci workflow.
set -eu

if [ $# -ne 2 ]; then
	echo "usage: $0 GOOS GOARCH" >&2
	exit 2
fi

goos=$1
goarch=$2
out="bin/pulseq-$goos-$goarch"

# 30 MB, the binary budget in docs/plans/00-SYNTESE.md section 4.9.
size_budget=31457280

fail() {
	if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
		printf '::error::%s\n' "$*" >&2
	else
		printf '%s\n' "$*" >&2
	fi
	exit 1
}

# CGO_ENABLED=1 here on purpose: with cgo off the toolchain hides cgo files
# instead of reporting them, so the probe would pass on a package that only
# builds through cgo.
leaks=$(CGO_ENABLED=1 GOOS="$goos" GOARCH="$goarch" go list -deps \
	-f '{{if and .CgoFiles (not .Standard)}}{{.ImportPath}}{{end}}' ./cmd/pulseq)
if [ -n "$leaks" ]; then
	fail "cgo leak for $goos/$goarch: these packages need cgo: $(printf '%s' "$leaks" | tr '\n' ' ')"
fi

CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" go build -trimpath -o "$out" ./cmd/pulseq

cgo_setting=$(go version -m "$out" | awk '$1 == "build" && $2 ~ /^CGO_ENABLED=/ { print $2 }')
if [ "$cgo_setting" != "CGO_ENABLED=0" ]; then
	fail "cgo leak in $out: build setting is '${cgo_setting:-missing}', want CGO_ENABLED=0"
fi

if ! command -v file >/dev/null 2>&1; then
	fail "file(1) is required to check the linkage of $out"
fi
description=$(file -b "$out")

if [ "$goos" = linux ]; then
	case $description in
	*"statically linked"* | *"static-pie linked"*) ;;
	*) fail "$out is not statically linked: $description" ;;
	esac

	case $goarch in
	amd64) want="x86-64" ;;
	arm64) want="ARM aarch64" ;;
	*) want="" ;;
	esac
	case $description in
	*"$want"*) ;;
	*) fail "$out is not $goarch: $description" ;;
	esac
fi

size=$(wc -c <"$out")
if [ "$size" -ge "$size_budget" ]; then
	fail "$out is $size bytes, budget is $size_budget"
fi

printf '%s: cgo free, %s bytes, %s\n' "$out" "$size" "$description"
