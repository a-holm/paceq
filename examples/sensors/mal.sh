#!/bin/sh
set -eu

cursor="${PACEQ_CURSOR:-}"
# A skip never moves the cursor: the same question gets asked again.
printf '{"cursor":"%s","triggers":[],"skip_reason":"nothing new"}\n' "$cursor"