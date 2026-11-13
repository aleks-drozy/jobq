#!/usr/bin/env bash
# The whole local run in one command: build the lab, run the fault matrix
# against it, take the lab down again whatever happened.
#
#   sudo chaos/local.sh [jobqchaos flags, e.g. -runs 10 -faults leader-kill]
#
# Expects linux builds of jobqd and jobqchaos in $JOBQ_CHAOS_DIR/bin
# (default /var/tmp/jobq-chaos/bin):
#
#   GOOS=linux go build -o /var/tmp/jobq-chaos/bin/ ./cmd/jobqd ./cmd/jobqchaos
#
# Results land in $JOBQ_CHAOS_DIR/results, node logs beside it. Exits with
# jobqchaos's status: 0 only if the contract held and every fault was seen.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
export JOBQ_CHAOS_DIR=${JOBQ_CHAOS_DIR:-/var/tmp/jobq-chaos}
lab=$JOBQ_CHAOS_DIR

[ -x "$lab/bin/jobqd" ] && [ -x "$lab/bin/jobqchaos" ] ||
  { echo "local.sh: need linux builds of jobqd and jobqchaos in $lab/bin" >&2; exit 2; }

"$here/netns.sh" down # whatever an interrupted run left behind
rm -rf "$lab"/node1 "$lab"/node2 "$lab"/node3 "$lab"/node?.log "$lab/results"
trap '"$here/netns.sh" down' EXIT

"$here/netns.sh" up
"$lab/bin/jobqchaos" -nodes "$("$here/netns.sh" nodes)" -hook "$here/netns.sh" -out "$lab/results" "$@"
