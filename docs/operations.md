# Running and validating the cluster

## Docker Compose

```bash
docker compose up --build -d
docker compose ps
docker compose logs -f node1
docker compose down -v        # also deletes the data volumes
```

Ports on the host: gRPC 9001 to 9005, HTTP metrics 8001 to 8005, Grafana 3000,
Prometheus 9090, Loki 3100.

Copy `.env.example` to `.env` if you want to change the Grafana credentials or enable email
alerts. The stack works without it.

## Without Docker

Five terminals, one per node. Each node needs its own data directory, its own ports and the
same peer list. The entry for the node itself is ignored, so the list can be copied as is.

```bash
export RAFT_PEERS="node1=localhost:9001,node2=localhost:9002,node3=localhost:9003"

NODE_ID=node1 RAFT_GRPC_ADDR=:9001 RAFT_HTTP_ADDR=:8001 RAFT_DATA_DIR=data/node1 go run ./cmd/raftnode
NODE_ID=node2 RAFT_GRPC_ADDR=:9002 RAFT_HTTP_ADDR=:8002 RAFT_DATA_DIR=data/node2 go run ./cmd/raftnode
NODE_ID=node3 RAFT_GRPC_ADDR=:9003 RAFT_HTTP_ADDR=:8003 RAFT_DATA_DIR=data/node3 go run ./cmd/raftnode
```

A single node without `RAFT_PEERS` also works: it becomes its own majority and commits
immediately, which is handy while developing.

## The demo script

`./scripts/demo.sh` needs `docker`, `jq` and `curl`.

| Command | What it does |
| --- | --- |
| `up` | Builds and starts everything, waits for a leader. |
| `status` | Role, term, commit and applied index of every running node. |
| `workload` | Writes three keys and reads one back. |
| `compare` | Verifies that the running nodes hold the same committed log. |
| `failover` | Stops the leader, waits for a new one, writes again. |
| `crash` | Stops a follower, writes while it is down, restarts it and compares logs. |
| `quorum-loss` | Stops three of five nodes and shows the write being rejected. |
| `restore` | Starts everything again and compares logs. |
| `metrics` | Queries the main Prometheus series. |
| `logs` | Prints the last node log lines from Loki. |
| `down` | Stops the stack and deletes the volumes. |

A full walkthrough: `up`, `workload`, `compare`, `failover`, `crash`, `quorum-loss`,
`restore`, `down`.

## Validation checklist

### Replication: one write reaches every node

```bash
docker compose exec node2 raftctl -addr node2:9001 write course sna
./scripts/demo.sh compare
```

`compare` prints each log and ends with a line like `5 nodes agree on entries 1..7`. It
compares `(index, term, command)` triples, so a divergence is reported with the index where
it starts.

### Leader failure: a new leader takes over

```bash
./scripts/demo.sh failover
```

Expect a new leader within roughly one election timeout (500 to 900 ms) plus the time
Docker needs to stop the container, and the write after the failover to succeed with a
higher term.

### Crash and restart: the restarted node catches up

```bash
./scripts/demo.sh crash
```

The restarted node reloads `current_term`, `voted_for` and its log from SQLite, the leader
notices the gap through the rejected `AppendEntries` and backfills it. If the log was
already compacted, the leader sends a snapshot instead, which you can see in the node log:

```bash
docker compose logs node2 | grep snapshot
```

### Quorum loss: writes stop, nothing is corrupted

```bash
./scripts/demo.sh quorum-loss
```

The remaining nodes keep starting elections and never win, so writes fail with
`no leader has been elected yet`. Grafana raises "Raft quorum is lost" after 30 seconds.
`./scripts/demo.sh restore` brings the cluster back and the logs converge again.

### Replication lag and apply delay

```bash
./scripts/demo.sh metrics
curl -s localhost:8001/metrics | grep -E 'raft_(commit|apply)_latency|replication_lag'
```

- `raft_replication_lag_entries{peer_id}` is how many entries the leader still owes a peer.
- `raft_commit_latency_seconds` measures append to majority commit on the leader.
- `raft_apply_latency_seconds` measures commit to applied in the state machine.

## Troubleshooting

**A write returns "no leader has been elected yet".** Fewer than three nodes are running,
or an election is in progress. Check `./scripts/demo.sh status`.

**A write returns "not committed by a majority".** The entry is on the leader but at least
three nodes have to store it. Usually two or more nodes are down.

**`compare` reports a small range like `entries 1..1`.** A node just restarted and has not
received the current commit index yet. Wait one heartbeat and run it again.

**Grafana shows no data.** Check <http://localhost:9090/targets>. Prometheus scrapes
`node1:8001` to `node5:8001` inside the compose network, so it needs the nodes to be up.

**Port already allocated on startup.** Another project is using 9001, 9090 or 3000 on the
host. Stop it or change the published ports in `docker-compose.yml`.
