#!/bin/sh
# Run the gate targets in order, skipping only the ones already proven for this
# exact tree, and stamping each one that exits zero. This is the only writer of
# gate stamps, so a stamp can never claim a run that did not happen.
#
# Every target is announced: the ones that run, the ones that are skipped and
# the evidence each skip leans on. A gate that stops guarding quietly is worse
# than no gate, so there is no silent path through here.
#
# Issue #176.
set -eu

if [ $# -eq 0 ]; then
	echo "usage: $0 TARGET..." >&2
	exit 2
fi

here=${0%/*}
if [ "$here" = "$0" ]; then
	here=.
fi
stamp=$here/gate-stamp.sh

# A key that cannot be computed proves nothing, so the run goes on without one:
# every target runs and nothing is stamped.
compute_key() {
	if computed=$("$stamp" key); then
		printf '%s' "$computed"
	else
		echo "gate: the content key could not be computed, every target runs and nothing is stamped" >&2
	fi
}

key=$(compute_key)

if [ "${PACEQ_GATE_STAMP:-1}" = 0 ]; then
	echo "gate: PACEQ_GATE_STAMP=0, every target runs"
fi

total=0
skipped=0
ran=0
spent=0

for target do
	total=$((total + 1))
	if proven=$("$stamp" has "$key" "$target"); then
		printf 'gate: %s skipped, this exact tree passed it at %s\n' "$target" "$proven"
		skipped=$((skipped + 1))
		continue
	fi
	printf 'gate: %s\n' "$target"
	started=$(date +%s)
	if ! make --no-print-directory "$target"; then
		printf 'gate: %s failed, no stamp written\n' "$target" >&2
		exit 1
	fi
	seconds=$(($(date +%s) - started))
	# A stamp that cannot be written is bookkeeping, not a verdict on the code.
	# The target passed, so the gate passes; it simply stays unproven and runs
	# again next time.
	if [ -n "$key" ]; then
		if ! "$stamp" record "$key" "$target" "$seconds"; then
			printf 'gate: %s passed but could not be stamped, it runs again next time\n' "$target" >&2
		fi
	fi
	ran=$((ran + 1))
	spent=$((spent + seconds))

	# The stamp above is for the tree this target was handed. A target that
	# writes into the tree hands the next one something else, and keying that
	# one to what this one read would be the too-generous stamp the whole
	# mechanism exists to avoid. Rekeying costs a fraction of a second and only
	# happens after a target has actually run, so a fully proven tree still
	# computes the key once.
	after=$(compute_key)
	if [ "$after" != "$key" ]; then
		printf 'gate: %s changed the tree, the targets after it are keyed to what it left\n' "$target"
	fi
	key=$after
done

escape=
if [ "$skipped" -gt 0 ]; then
	escape='; PACEQ_GATE_STAMP=0 runs everything'
fi
printf 'gate-summary: %d of %d targets skipped as already proven for this tree, %d run in %ds%s\n' \
	"$skipped" "$total" "$ran" "$spent" "$escape"
