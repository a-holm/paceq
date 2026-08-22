#!/bin/sh
# Every commit that touches a gold standard fixture must carry a
# FIXTURE-CHANGE: line in its message, with a reason. A red fixture test is a
# reason to investigate, never a reason to edit an expectation in passing.
# Plan 04 section 8 point 2; issue #52.
#
# Usage: check-fixture-change.sh BASE HEAD
#   BASE and HEAD are any two revisions. Every commit reachable from HEAD but
#   not from BASE is checked.
set -u

if [ "$#" -ne 2 ]; then
	echo "usage: $0 BASE HEAD" >&2
	exit 2
fi

BASE=$1
HEAD=$2

GOLDEN_DIR=internal/cronx/testdata/golden

if ! git rev-parse --verify --quiet "$BASE" >/dev/null; then
	echo "check-fixture-change: base revision $BASE does not resolve" >&2
	exit 2
fi
if ! git rev-parse --verify --quiet "$HEAD" >/dev/null; then
	echo "check-fixture-change: head revision $HEAD does not resolve" >&2
	exit 2
fi

status=0
for commit in $(git rev-list "$BASE..$HEAD"); do
	changed=$(git diff-tree --no-commit-id --name-only -r "$commit" -- "$GOLDEN_DIR")
	if [ -z "$changed" ]; then
		continue
	fi
	if git log -1 --format=%B "$commit" | grep -q "FIXTURE-CHANGE:"; then
		continue
	fi
	echo "commit $(git rev-parse --short "$commit") touches $GOLDEN_DIR without a FIXTURE-CHANGE: line in its message:"
	git log -1 --format='    %s' "$commit"
	status=1
done

if [ "$status" -ne 0 ]; then
	echo "Regenerate fixtures with tools/gen-cron-fixtures/gen.py if the change is" >&2
	echo "intended, and put a FIXTURE-CHANGE: line with the reason in each commit" >&2
	echo "that edits files under $GOLDEN_DIR." >&2
fi
exit "$status"
