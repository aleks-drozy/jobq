#!/usr/bin/env bash
# The whole local run in one command: install the binaries, build the lab,
# run the fault matrix against it, take the lab down again whatever
# happened.
#
#   GOOS=linux go build -o chaos/bin/ ./cmd/jobqd ./cmd/jobqchaos
#   sudo chaos/local.sh [jobqchaos flags, e.g. -runs 10 -faults leader-kill]
#
# The binaries are taken from chaos/bin (or $JOBQ_CHAOS_BUILD) and installed
# into the lab directory, $JOBQ_CHAOS_DIR (default /var/lib/jobq-chaos),
# which is root's alone: see netns.sh for why it has to be. Results land in
# its results/ subdirectory, node logs beside it, and are printed as well;
# pass -out DIR to have them written somewhere you can read without sudo.
# Exits with jobqchaos's status: 0 only if the contract held and every
# fault was seen.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
export JOBQ_CHAOS_DIR=${JOBQ_CHAOS_DIR:-/var/lib/jobq-chaos}
lab=$JOBQ_CHAOS_DIR
build=${JOBQ_CHAOS_BUILD:-$here/bin}

[ "$(id -u)" = 0 ] || { echo "local.sh: must run as root (it creates network namespaces)" >&2; exit 2; }
[ -f "$build/jobqd" ] && [ -f "$build/jobqchaos" ] ||
  { echo "local.sh: need linux builds of jobqd and jobqchaos in $build" >&2; exit 2; }

# Whatever an interrupted run left behind. This also refuses a lab
# directory that is not root's alone, before anything is put in it.
"$here/netns.sh" down
install -d -m 0700 -o root -g root -- "$lab"
install -d -m 0755 -o root -g root -- "$lab/bin"
install -m 0755 -o root -g root -- "$build/jobqd" "$build/jobqchaos" "$lab/bin/"
rm -rf -- "$lab"/node1 "$lab"/node2 "$lab"/node3 "$lab"/node?.log "$lab/results"
trap '"$here/netns.sh" down' EXIT

"$here/netns.sh" up
"$lab/bin/jobqchaos" -nodes "$("$here/netns.sh" nodes)" -hook "$here/netns.sh" -out "$lab/results" "$@"
