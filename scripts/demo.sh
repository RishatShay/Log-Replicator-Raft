#!/usr/bin/env bash
# Walks through the cluster demo: start, write, fail over, lose quorum, recover.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NODES=(node1 node2 node3 node4 node5)
PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"
LOKI_URL="${LOKI_URL:-http://localhost:3100}"
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"

usage() {
  cat <<'TEXT'
usage: ./scripts/demo.sh <command>

  up             build and start the cluster with Prometheus, Loki and Grafana
  status         print the Raft status of every node
  workload       write and read a few keys
  compare        check that every node holds the same committed log
  failover       stop the leader and write again once a new one is elected
  crash          stop a follower, write, restart it and compare the logs
  quorum-loss    stop three nodes and show that writes are rejected
  restore        start every node again
  metrics        query the main Prometheus series
  logs           query the last Raft log lines from Loki
  down           stop the stack and delete the volumes

A full walkthrough: up, workload, compare, failover, crash, quorum-loss, restore, down
TEXT
}

section() { printf '\n== %s ==\n' "$1"; }

require() {
  for tool in "$@"; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 1; }
  done
}

# raftctl runs the CLI inside a node container, so it can use the cluster network.
raftctl() {
  local node="$1"
  shift
  docker compose exec -T "$node" raftctl -addr "${node}:9001" "$@"
}

running_nodes() {
  docker compose ps --services --status running | grep -E '^node[1-5]$' || true
}

find_leader() {
  local node role
  for node in $(running_nodes); do
    role="$(raftctl "$node" status 2>/dev/null | jq -r '.role' || true)"
    if [[ "$role" == "leader" ]]; then
      echo "$node"
      return 0
    fi
  done
  return 1
}

first_running_node() {
  running_nodes | head -n 1
}

# Cluster addresses of the nodes that are up, ready to be passed to "raftctl compare".
running_addresses() {
  local node
  for node in $(running_nodes); do
    printf '%s:9001 ' "$node"
  done
}

wait_for_leader() {
  local attempt leader
  for attempt in $(seq 1 30); do
    if leader="$(find_leader)"; then
      echo "$leader"
      return 0
    fi
    sleep 1
  done
  echo "no leader was elected" >&2
  return 1
}

cmd_up() {
  require docker jq
  section "Starting the stack"
  docker compose up --build -d

  section "Waiting for a leader"
  echo "leader: $(wait_for_leader)"
  echo "Grafana: ${GRAFANA_URL} (admin/admin), Prometheus: ${PROMETHEUS_URL}"
}

cmd_status() {
  local node
  for node in $(running_nodes); do
    section "$node"
    raftctl "$node" status | jq '{id, role, term, leader_id, commit_index, last_applied, last_log_index}'
  done
}

cmd_workload() {
  local target key
  target="$(first_running_node)"
  section "Writing through ${target}"
  for key in course topic author; do
    raftctl "$target" write "$key" "value-$(date +%H%M%S)"
  done

  section "Reading back"
  raftctl "$target" read course
}

cmd_compare() {
  local target
  target="$(first_running_node)"
  section "Comparing the logs of the running nodes"
  # shellcheck disable=SC2046 # the address list has to be split into arguments
  raftctl "$target" compare $(running_addresses)
}

cmd_failover() {
  local old_leader new_leader
  old_leader="$(wait_for_leader)"
  section "Stopping the leader ${old_leader}"
  docker compose stop "$old_leader"

  section "Waiting for a new leader"
  new_leader="$(wait_for_leader)"
  echo "new leader: ${new_leader}"

  section "Writing after the failover"
  raftctl "$new_leader" write after_failover ok
  raftctl "$new_leader" read after_failover
}

cmd_crash() {
  local leader follower node
  leader="$(wait_for_leader)"
  for node in $(running_nodes); do
    if [[ "$node" != "$leader" ]]; then
      follower="$node"
      break
    fi
  done

  section "Stopping the follower ${follower}"
  docker compose stop "$follower"

  section "Writing while it is down"
  raftctl "$leader" write during_crash yes

  section "Restarting ${follower}"
  docker compose start "$follower"
  sleep 5
  cmd_compare
}

cmd_quorum_loss() {
  local leader targets node
  leader="$(wait_for_leader)"
  targets=("$leader")
  for node in $(running_nodes); do
    if [[ ${#targets[@]} -ge 3 ]]; then
      break
    fi
    if [[ "$node" != "$leader" ]]; then
      targets+=("$node")
    fi
  done

  section "Stopping three of five nodes: ${targets[*]}"
  docker compose stop "${targets[@]}"
  sleep 10

  section "A write without quorum"
  raftctl "$(first_running_node)" write no_quorum please || echo "the write was rejected, as expected"
  cmd_status
}

cmd_restore() {
  section "Starting every node again"
  docker compose start "${NODES[@]}"
  echo "leader: $(wait_for_leader)"
  # Give the restarted nodes a few heartbeats to catch up before comparing.
  sleep 5
  cmd_compare
}

prom_query() {
  echo "-- $1"
  curl -fsS -G "${PROMETHEUS_URL}/api/v1/query" --data-urlencode "query=$1" |
    jq -r '.data.result[] | "   \(.metric | del(.__name__) | to_entries | map("\(.key)=\(.value)") | join(" ")) => \(.value[1])"'
}

cmd_metrics() {
  require curl jq
  section "Prometheus"
  prom_query 'up{job="raft"}'
  prom_query 'raft_is_leader'
  prom_query 'raft_commit_index'
  prom_query 'raft_replication_lag_entries'
  prom_query 'sum by (method, result) (increase(raft_rpc_total[5m]))'
}

cmd_logs() {
  require curl jq
  section "Loki"
  curl -fsS -G "${LOKI_URL}/loki/api/v1/query_range" \
    --data-urlencode 'query={node_id=~"node[1-5]"}' \
    --data-urlencode 'limit=20' |
    jq -r '.data.result[] | .stream.node_id as $node | .values[] | "   \($node) \(.[1])"'
}

cmd_down() {
  section "Stopping the stack"
  docker compose down -v
}

case "${1:-help}" in
  up) cmd_up ;;
  status) cmd_status ;;
  workload) cmd_workload ;;
  compare) cmd_compare ;;
  failover) cmd_failover ;;
  crash) cmd_crash ;;
  quorum-loss) cmd_quorum_loss ;;
  restore) cmd_restore ;;
  metrics) cmd_metrics ;;
  logs) cmd_logs ;;
  down) cmd_down ;;
  help | -h | --help) usage ;;
  *)
    echo "unknown command: $1" >&2
    usage >&2
    exit 2
    ;;
esac
