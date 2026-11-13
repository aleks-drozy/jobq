#!/usr/bin/env bash
# A three-node jobq cluster on one Linux machine, each node in its own
# network namespace, and the faults jobqchaos asks for.
#
#   netns.sh up            build the network and start the three nodes
#   netns.sh down          stop the nodes and remove the network (keeps logs)
#   netns.sh nodes         print the -nodes value for jobqchaos
#   netns.sh status        what each node says about itself
#
# and, as the hook jobqchaos calls (netns.sh VERB NODE-ID):
#
#   kill ID                SIGKILL the node's jobqd and wait until it is gone
#   start ID               start it again on the same data directory
#   isolate ID             cut the node off from the other two, not from clients
#   heal ID                undo isolate
#   slow ID                netem on the node's interface: delay and loss
#   fast ID                undo slow
#
# Layout: a bridge on the host at NET.254, and node N in namespace jobqN at
# NET.N, clients on :8080 and peers on :9090. jobqchaos runs on the host
# side of the bridge, so it reaches every node whatever the nodes can or
# cannot say to each other.
#
# Needs root, iproute2 (ip, tc) and the sch_netem module. Nothing else: a
# partition is a blackhole route inside the node's namespace, not an
# iptables rule, so that there is nothing to install and nothing outside
# the namespaces is touched. Everything this script creates is named jobq*
# and `down` removes all of it.
set -euo pipefail

LAB=${JOBQ_CHAOS_DIR:-/var/tmp/jobq-chaos}
JOBQD=${JOBQD:-$LAB/bin/jobqd}
NET=${JOBQ_CHAOS_NET:-10.77.0}
DELAY=${JOBQ_CHAOS_DELAY:-100ms}
LOSS=${JOBQ_CHAOS_LOSS:-5%}
# Extra jobqd flags for every node, e.g. JOBQD_FLAGS="-tick 50ms".
EXTRA=${JOBQD_FLAGS:-}
BRIDGE=jobqbr0
IDS="1 2 3"

die() { echo "netns.sh: $*" >&2; exit 1; }

[ "$(id -u)" = 0 ] || die "must run as root (it creates network namespaces)"

valid_id() {
  case " $IDS " in *" $1 "*) ;; *) die "no such node: '${1:-}' (have $IDS)" ;; esac
}

node_map() { # node_map PORT -> 1=http://NET.1:PORT,2=...
  local out="" i
  for i in $IDS; do out="$out${out:+,}$i=http://$NET.$i:$1"; done
  echo "$out"
}

running() { # running ID: is the node's jobqd alive (and not a zombie)?
  local pid
  pid=$(cat "$LAB/node$1.pid" 2>/dev/null) || return 1
  [ -n "$pid" ] && [ -r "/proc/$pid/stat" ] || return 1
  # Field 3 of /proc/PID/stat is the state; Z is dead but not yet reaped.
  [ "$(sed 's/.*) //' "/proc/$pid/stat" | cut -d' ' -f1)" != Z ]
}

start() {
  valid_id "$1"
  running "$1" && die "node $1 is already running"
  [ -x "$JOBQD" ] || die "no jobqd at $JOBQD (build one for linux and set JOBQD)"
  mkdir -p "$LAB/node$1"
  # -break-lock: this script is the supervisor. It starts a node only
  # after checking, just above, that the last one is gone.
  # shellcheck disable=SC2086
  ip netns exec "jobq$1" setsid "$JOBQD" \
    -id "$1" -dir "$LAB/node$1" \
    -addr "$NET.$1:8080" -peer-addr "$NET.$1:9090" \
    -peers "$(node_map 9090)" -advertise "$(node_map 8080)" \
    -sync always -break-lock $EXTRA \
    >>"$LAB/node$1.log" 2>&1 </dev/null &
  echo $! >"$LAB/node$1.pid"
  # Return once it is listening, so the caller's clock for "back up"
  # does not include a process that has not bound its port yet.
  local i
  for i in $(seq 1 500); do
    if (exec 3<>"/dev/tcp/$NET.$1/8080") 2>/dev/null; then return 0; fi
    running "$1" || die "node $1 exited at once; see $LAB/node$1.log"
    sleep 0.01
  done
  die "node $1 did not start listening within 5s; see $LAB/node$1.log"
}

kill_node() {
  valid_id "$1"
  local pid i
  pid=$(cat "$LAB/node$1.pid" 2>/dev/null) || return 0
  running "$1" || return 0
  kill -9 "$pid"
  for i in $(seq 1 500); do
    running "$1" || return 0
    sleep 0.01
  done
  die "node $1 (pid $pid) survived SIGKILL for 5s"
}

others() { local i; for i in $IDS; do [ "$i" = "$1" ] || echo "$i"; done; }

isolate() {
  valid_id "$1"
  local j
  # Packets to the other nodes are dropped as they leave this one. That
  # cuts both directions of every connection between them, since a
  # connection needs replies, and leaves the route to clients alone.
  for j in $(others "$1"); do ip -n "jobq$1" route replace blackhole "$NET.$j/32"; done
}

heal() {
  valid_id "$1"
  local j
  for j in $(others "$1"); do ip -n "jobq$1" route del blackhole "$NET.$j/32" 2>/dev/null || true; done
}

slow() {
  valid_id "$1"
  ip netns exec "jobq$1" tc qdisc replace dev eth0 root netem delay "$DELAY" loss "$LOSS"
}

fast() {
  valid_id "$1"
  ip netns exec "jobq$1" tc qdisc del dev eth0 root 2>/dev/null || true
}

up() {
  ip link show "$BRIDGE" >/dev/null 2>&1 && die "$BRIDGE already exists: run 'netns.sh down' first"
  mkdir -p "$LAB"
  ip link add "$BRIDGE" type bridge
  ip addr add "$NET.254/24" dev "$BRIDGE"
  ip link set "$BRIDGE" up
  local i
  for i in $IDS; do
    ip netns add "jobq$i"
    ip link add "jobqv$i" type veth peer name eth0 netns "jobq$i"
    ip link set "jobqv$i" master "$BRIDGE"
    ip link set "jobqv$i" up
    ip -n "jobq$i" addr add "$NET.$i/24" dev eth0
    ip -n "jobq$i" link set eth0 up
    ip -n "jobq$i" link set lo up
  done
  # Fail here, not in the middle of a run, if the kernel cannot do netem.
  slow 1 || die "tc netem is not available (is the sch_netem module there?)"
  fast 1
  for i in $IDS; do start "$i"; done
}

down() {
  local i
  for i in $IDS; do
    kill_node "$i" 2>/dev/null || true
    ip netns del "jobq$i" 2>/dev/null || true
    ip link del "jobqv$i" 2>/dev/null || true
    rm -f "$LAB/node$i.pid"
  done
  ip link del "$BRIDGE" 2>/dev/null || true
}

status() {
  local i
  for i in $IDS; do
    if running "$i"; then
      printf 'node %s: ' "$i"
      curl -s --max-time 2 "http://$NET.$i:8080/v1/cluster" || printf 'running, not answering'
      echo
    else
      echo "node $i: not running"
    fi
  done
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  nodes) node_map 8080 ;;
  status) status ;;
  start) start "${2:-}" ;;
  kill) kill_node "${2:-}" ;;
  isolate) isolate "${2:-}" ;;
  heal) heal "${2:-}" ;;
  slow) slow "${2:-}" ;;
  fast) fast "${2:-}" ;;
  *) die "usage: netns.sh up|down|nodes|status, or kill|start|isolate|heal|slow|fast NODE-ID" ;;
esac
