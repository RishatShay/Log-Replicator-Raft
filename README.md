# Raft key/value cluster

[![CI](https://github.com/RishatShay/sna-final-project/actions/workflows/ci.yml/badge.svg)](https://github.com/RishatShay/sna-final-project/actions/workflows/ci.yml)

This is an implementation of Raft, the consensus algorithm that lets a cluster of nodes
agree on one leader and keep a replicated log in sync, even when nodes crash or the network
misbehaves. There is no single point of failure: if the leader dies, the remaining nodes
elect a new one and the cluster keeps serving writes. This is the same idea real systems
rely on, etcd (and through it, Kubernetes), Consul and CockroachDB all use Raft under the
hood for exactly this reason.

Here it backs a five node replicated key/value store, written in Go from scratch: leader
election, log replication, snapshots and a durable state machine on SQLite. Nodes talk to
each other over gRPC, and the same gRPC service is the client API.

This is not a toy that only handles the happy path. A follower that falls behind catches
up in one of two ways: the leader either replays the missing log entries, or, if those
entries were already compacted away, ships a full snapshot instead. Both paths are covered
by the integration tests, along with leader failover and crash recovery.

The repository also ships the monitoring you need to actually watch consensus happen:
Prometheus metrics on every node, JSON logs collected by Promtail into Loki, and a Grafana
dashboard with alerts for "no leader" and "quorum lost".

Russian version of this file: [docs/README.ru.md](docs/README.ru.md).

## What is implemented

- Leader election with randomized timeouts and persisted `currentTerm` / `votedFor`.
- Log replication with the `AppendEntries` consistency check, conflict truncation and
  `nextIndex` back off for followers that fell behind.
- Writes are answered only after a majority of nodes stored the entry on disk.
- Log compaction: applied entries are replaced by a snapshot, and a follower that is too
  far behind is repaired with `InstallSnapshot`.
- Reads are served by the leader after a heartbeat round confirms it still has a majority,
  so a partitioned leader cannot answer with stale data.
- Any node accepts client calls: a follower forwards them to the leader it knows about.
- `raftctl compare` checks that every node holds the same committed log.
- Prometheus metrics, JSON logs, Grafana dashboard and alert rules.

## Quick start

Requires Docker with Compose v2. Nothing else, the build happens in the image.

```bash
docker compose up --build -d     # five nodes plus Prometheus, Loki, Grafana
./scripts/demo.sh status         # who is the leader
```

Write and read a key. Both work through any node, followers forward to the leader:

```bash
docker compose exec node3 raftctl -addr node3:9001 write course sna
docker compose exec node1 raftctl -addr node1:9001 read course
```

Check that the log is identical everywhere:

```bash
docker compose exec node1 raftctl -addr node1:9001 compare \
  node1:9001 node2:9001 node3:9001 node4:9001 node5:9001
```

From the host, the nodes are on `localhost:9001` to `localhost:9005`, so `go run ./cmd/raftctl`
works too:

```bash
go run ./cmd/raftctl -addr localhost:9002 write topic consensus
go run ./cmd/raftctl -addr localhost:9002 status
go run ./cmd/raftctl compare
```

| Service | URL |
| --- | --- |
| Grafana | <http://localhost:3000> (admin / admin) |
| Prometheus | <http://localhost:9090> |
| Loki | <http://localhost:3100> |
| Node metrics and health | <http://localhost:8001/metrics>, `/healthz`, ports 8001 to 8005 |

`./scripts/demo.sh` also drives the interesting failure scenarios: `failover` stops the
leader, `crash` restarts a follower behind the cluster, `quorum-loss` takes three nodes
down and shows writes being rejected, `restore` brings everything back. See
[docs/operations.md](docs/operations.md).

## Repository layout

```text
api/proto/raft/v1/raft.proto   gRPC contract, the source of truth for the wire format
cmd/raftnode                   the node binary
cmd/raftctl                    the CLI: write, read, delete, status, log, compare
internal/raft                  consensus: election, replication, snapshots, client API
internal/storage                SQLite store: log, metadata, state machine, snapshots
internal/config                 environment configuration
internal/metrics                Prometheus metrics
internal/raftpb                 generated protobuf and gRPC code
deployments/                    Prometheus, Loki, Promtail and Grafana configuration
scripts/demo.sh                 start, workload, failover, crash, quorum loss, restore
docs/                           documentation
```

## Configuration

Every node reads its configuration from the environment. The defaults start a working
single node cluster from the repository root.

| Variable | Default | Meaning |
| --- | --- | --- |
| `NODE_ID` | `node1` | Stable node id, used in logs, metrics and votes. |
| `RAFT_GRPC_ADDR` | `:9001` | gRPC listen address. |
| `RAFT_HTTP_ADDR` | `:8001` | Address serving `/metrics` and `/healthz`. |
| `RAFT_DATA_DIR` | `data/node1` | Directory holding `raft.db`. |
| `RAFT_PEERS` | empty | `id=host:port` list, comma separated. May include this node. |
| `RAFT_ELECTION_MIN_MS` | `500` | Lower bound of the election timeout. |
| `RAFT_ELECTION_MAX_MS` | `900` | Upper bound of the election timeout. |
| `RAFT_HEARTBEAT_MS` | `100` | Leader heartbeat interval. |
| `RAFT_SNAPSHOT_THRESHOLD` | `10000` | Applied entries between two snapshots. |
| `RAFT_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. |

Invalid values are rejected at startup instead of being silently replaced, for example an
election timeout shorter than the heartbeat interval.

## Documentation

- [docs/architecture.md](docs/architecture.md): how a node is put together, what is
  persisted, how election and replication actually run, and which Raft parts are simplified.
- [docs/api.md](docs/api.md): the gRPC services, the state machine commands and the error
  codes clients should expect.
- [docs/operations.md](docs/operations.md): running the cluster, the demo scenarios and the
  validation checklist (replication, crash recovery, replication lag).
- [docs/monitoring.md](docs/monitoring.md): every metric, the dashboard panels, the alert
  rules and useful Loki queries.
- [docs/README.ru.md](docs/README.ru.md): this file in Russian.

## Development

```bash
make test     # unit and integration tests, a three node cluster runs in process
make race     # the same with the race detector
make vet
make build    # bin/raftnode and bin/raftctl
```

The generated gRPC code is committed, so a normal build does not need `protoc`. After
editing the proto file:

```bash
make proto-tools   # once, installs protoc-gen-go and protoc-gen-go-grpc
make proto
```

CI runs formatting, vet, tests and the race detector, and verifies that the committed
generated code still matches `raft.proto`.

The SQLite driver is [modernc.org/sqlite](https://modernc.org/sqlite), a pure Go
implementation, so the binaries are static and the image needs no C toolchain.

## Known limitations

This is a teaching implementation, deliberately kept small. It is not a drop in
replacement for etcd or Consul.

- Cluster membership is fixed at startup, there is no joint consensus reconfiguration.
- A fresh leader does not commit a no-op entry, so entries from previous terms are only
  committed once a new entry of the current term is committed.
- The state machine has two commands, `set` and `delete`, with string keys and values.
- No TLS and no authentication, everything is plaintext gRPC on a trusted network.
- Client writes are not deduplicated: a retried request may be applied twice.

## License

MIT, see [LICENSE](LICENSE).

Originally a university team project. This repository is the reworked version: the two
implementations the team wrote were merged into one, the generated gRPC code is real
instead of hand written, and the consensus code, tests, monitoring and documentation were
rewritten.
