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
out="bin/paceq-$goos-$goarch"

# 30 MB, the binary budget in docs/PLAN.md.
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
	-f '{{if and .CgoFiles (not .Standard)}}{{.ImportPath}}{{end}}' ./cmd/paceq)
if [ -n "$leaks" ]; then
	fail "cgo leak for $goos/$goarch: these packages need cgo: $(printf '%s' "$leaks" | tr '\n' ' ')"
fi

# The same probe over the whole module. The product binary reaches only what it
# imports, so a driver or a platform specific package that needs cgo stays
# invisible until something links it.
module_leaks=$(CGO_ENABLED=1 GOOS="$goos" GOARCH="$goarch" go list -deps \
	-f '{{if and .CgoFiles (not .Standard)}}{{.ImportPath}}{{end}}' ./...)
if [ -n "$module_leaks" ]; then
	fail "cgo leak for $goos/$goarch outside cmd/paceq: these packages need cgo: $(printf '%s' "$module_leaks" | tr '\n' ' ')"
fi

# Compile every package, not just the ones the binary imports. internal/store
# and its build tagged filesystem check reach a cross build no other way, so
# without this a package that does not build for darwin or windows lands
# unnoticed.
CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" go build ./...

CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" go build -trimpath -o "$out" ./cmd/paceq

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
