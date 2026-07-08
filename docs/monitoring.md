# Monitoring

Each node exposes Prometheus metrics on `RAFT_HTTP_ADDR` (`/metrics`, port 8001 inside the
container) and writes JSON logs to stdout. Prometheus scrapes the nodes every five seconds,
Promtail ships the container logs to Loki, and Grafana provisions one dashboard, one contact
point and four alert rules on startup.

## Metrics

Every series carries a `node_id` label, so all of them can be compared side by side.

| Metric | Type | Meaning |
| --- | --- | --- |
| `raft_is_leader` | gauge | 1 on the node that believes it is the leader. `sum(raft_is_leader)` should be exactly 1. |
| `raft_current_term` | gauge | Current term. Rising fast means elections keep failing. |
| `raft_commit_index` | gauge | Highest committed index. |
| `raft_last_applied` | gauge | Highest index applied to the state machine. |
| `raft_last_log_index` | gauge | Highest index stored locally, including uncommitted entries. |
| `raft_snapshot_index` | gauge | Index the latest snapshot covers. |
| `raft_elections_total` | counter | Elections this node started. |
| `raft_rpc_total{method,result}` | counter | Served consensus RPCs. Results include `success`, `granted`, `rejected`, `stale_term`, `log_mismatch`, `behind_snapshot`, `ignored`, `error`. |
| `raft_replication_lag_entries{peer_id}` | gauge | Entries the leader still has to ship to a peer. |
| `raft_commit_latency_seconds` | histogram | Append to majority commit, measured on the leader. |
| `raft_apply_latency_seconds` | histogram | Commit to applied in the state machine. |

The standard Go runtime and process collectors are registered as well.

Useful queries:

```promql
sum(raft_is_leader)                                    # 1 in a healthy cluster
max(raft_commit_index) - min(raft_commit_index)         # how far the slowest node is behind
sum by (result) (rate(raft_rpc_total{method="AppendEntries"}[1m]))
histogram_quantile(0.95, sum by (le) (rate(raft_commit_latency_seconds_bucket[5m])))
```

## Dashboard

Grafana loads "Raft Overview" from `deployments/grafana/dashboards`. Panels:

1. **Leader**: exactly one line at 1, the step during a failover is visible here.
2. **Commit and apply progress**: commit and applied index per node.
3. **Replication lag**: per follower, from the leader point of view.
4. **Write latency (p95)**: commit and apply latency.
5. **Consensus RPCs**: rate by method and result, rejections show recovery in progress.
6. **Elections started**: a rising line means the cluster cannot keep a leader.
7. **Snapshot position**: when compaction happened on each node.

## Alerts

`deployments/grafana/provisioning/alerting/alerts.yml` provisions four rules:

| Alert | Condition | For |
| --- | --- | --- |
| Raft node is down | `up{job="raft"} == 0` | 2m |
| Raft cluster has no leader | `sum(raft_is_leader) != 1` | 2m |
| Raft quorum is at risk | two nodes unreachable | 1m |
| Raft quorum is lost | three or more nodes unreachable | 30s |

All of them route to the `email-alerts` contact point. Email delivery is off by default: set
`ALERT_EMAIL` and the `SMTP_*` variables in `.env` (see `.env.example`) to turn it on. With
Gmail the password has to be an app password. Alerts still fire and are visible in Grafana
without SMTP, they simply are not delivered.

## Logs

Nodes log JSON through `log/slog`:

```json
{"time":"2026-08-17T17:25:47Z","level":"INFO","msg":"entry committed","node_id":"node5","index":2,"term":3,"op":"set","key":"course"}
```

Promtail parses `level`, `msg` and `node_id` and turns `level` and `node_id` into Loki
labels. Useful queries in Grafana Explore:

```logql
{node_id="node1"}                      # everything from one node
{node_id=~"node[1-5]"} |= "leader"     # elections and leadership changes
{node_id=~"node[1-5]", level="ERROR"}  # errors across the cluster
{node_id=~"node[1-5]"} |= "snapshot"   # compaction and snapshot shipping
```

The interesting messages to follow during a demo are `election started`, `vote granted`,
`became leader`, `stepping down`, `entry committed`, `snapshot created` and
`snapshot installed`.
