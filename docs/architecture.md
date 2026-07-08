# Architecture

Every node is the same binary. It serves two gRPC services on one port: `RaftService` for
consensus traffic between nodes and `ClientService` for clients. A second, plain HTTP port
serves `/metrics` and `/healthz`.

```text
             clients (raftctl)
                    |
                    v
 +--------------------------------------+
 |  ClientService (write/read/delete)   |
 |  forwards to the leader if follower  |
 +--------------------------------------+
 |  raft.Node                           |
 |    role, term, commitIndex, ...      |
 |    election loop                     |
 |    one replication loop per peer     |
 +--------------------------------------+
 |  storage.Store (SQLite, WAL)         |
 |    metadata, log, kv, snapshot       |
 +--------------------------------------+
                    ^
                    | RaftService: RequestVote, AppendEntries, InstallSnapshot
                    v
                other nodes
```

## Packages

| Package | Responsibility |
| --- | --- |
| `internal/raft` | Consensus and the client API. Split into `node.go` (lifecycle and shared state), `election.go`, `replication.go` (leader side), `follower.go` (the two incoming consensus RPCs), `client.go` (client API), `command.go` (state machine commands). |
| `internal/storage` | Everything durable: persistent metadata, the log, the key/value state machine and the snapshot. |
| `internal/config` | Environment configuration with validation. |
| `internal/metrics` | Prometheus registry and the Raft metrics. |
| `internal/raftpb` | Code generated from `api/proto/raft/v1/raft.proto`. |

## State and concurrency

`raft.Node` keeps the Raft state behind one mutex: role, `currentTerm`, `votedFor`, the
known leader, `commitIndex`, `lastApplied`, and `nextIndex` / `matchIndex` per peer. Store
access happens under the same mutex, which keeps the invariant "state and disk agree" easy
to reason about. Methods that expect the mutex to be held end in `Locked`.

Three kinds of goroutines touch that state:

1. **The election loop** checks every 25 ms whether the election deadline passed and starts
   an election if it did.
2. **One replication loop per peer** sends `AppendEntries` on every heartbeat tick, or
   immediately when a client write signals it through a buffered channel. One loop per peer
   means a dead or slow follower cannot delay the others. Concurrent rounds towards the same
   peer are serialised with a per peer mutex.
3. **gRPC handlers** for incoming RPCs.

RPC calls always happen with the mutex released. Every call carries a timeout derived from
the node context, so shutting down a node cancels the calls in flight.

## What is persisted

SQLite runs in WAL mode with `synchronous=FULL`: an entry is on disk before the node
answers, which is what Raft requires. The schema is four small tables:

| Table | Content |
| --- | --- |
| `metadata` | `current_term`, `voted_for`, `last_applied`, `snapshot_index`, `snapshot_term` |
| `log_entries` | `idx`, `term`, `command` (JSON) |
| `kv` | the state machine: `key`, `value`, `log_index` |
| `snapshot` | one row: the serialized state machine plus the position it covers |

`last_applied` is updated in the same transaction as the state machine change, so applying
an entry is atomic: after a crash the node never applies an entry twice or forgets one.

`commitIndex` is not persisted. On startup a node sets it to `last_applied`, which is safe
because an applied entry is by definition committed, and the leader tells the node about
everything above it in the first heartbeat.

## Election

A node that has not heard from a leader within its randomized timeout
(`RAFT_ELECTION_MIN_MS` to `RAFT_ELECTION_MAX_MS`) becomes a candidate: it bumps the term,
votes for itself, persists both, then asks every peer for a vote in parallel and counts the
replies as they arrive. It stops early as soon as a majority agreed.

A vote is granted only if the candidate term is not stale, the node has not voted for
someone else in this term, and the candidate log is at least as up to date
(`lastLogTerm`, then `lastLogIndex`). The vote is written to disk before the reply is sent,
so a node that restarts mid election cannot vote twice.

Any message carrying a newer term makes the node step down to follower.

## Replication and commit

The leader keeps `nextIndex` and `matchIndex` per follower. Each round sends the entries
starting at `nextIndex` together with the position right before it. If the follower does not
have that position, it rejects and reports its own last index. The leader uses that hint to
jump back instead of stepping one entry at a time, then retries.

The follower drops a conflicting entry and everything after it before appending, which is
what makes the logs converge.

An entry is committed once a majority stores it, and only entries of the current term are
committed directly. Committed entries are applied to the state machine in index order.
Because the state machine is the same SQLite database, applying is durable, not just
in memory.

The write path is therefore: append locally, wake up the replication loops, wait for
`commitIndex` to reach the entry, answer. A write that cannot reach a majority within five
seconds fails with `DEADLINE_EXCEEDED` and stays in the log, which is honest: the entry may
still be committed later by a future leader.

## Snapshots and log compaction

After `RAFT_SNAPSHOT_THRESHOLD` applied entries, the node serializes the whole state machine
to JSON, stores it as the snapshot at the current applied position and deletes the log
entries it covers, all in one transaction.

If a follower needs entries that no longer exist, the leader ships the snapshot with
`InstallSnapshot`. The follower replaces its state machine, drops the compacted entries and
moves `commitIndex` and `lastApplied` to the snapshot position.

## Client requests

Followers do not answer client calls themselves, they forward them to the leader they know
about and pass the reply back. Forwarded requests are marked with a gRPC header and are
never forwarded a second time, so two nodes cannot bounce a request between each other
during an election.

Reads go through the leader and are answered only after one replication round reaches a
majority. That is the cheap version of a read barrier: it costs one round trip but keeps a
partitioned leader from serving values the cluster has already replaced.

`InspectLog` is not forwarded, it deliberately reports the local log and state machine so
that `raftctl compare` can look for differences between nodes.

## Simplifications

The implementation follows the Raft paper for elections, log matching and commitment, and
skips the parts that are not needed for a demonstration cluster:

- No membership changes, the peer list is fixed at startup.
- No no-op entry after an election, so entries from earlier terms wait for the first commit
  of the current term.
- No client request ids, so a retried write can be applied twice.
- No leadership transfer, no pre-vote, no batching or pipelining beyond a 128 entry batch.
