# API

The contract lives in [api/proto/raft/v1/raft.proto](../api/proto/raft/v1/raft.proto). Both
services listen on the same port (9001 inside the container).

## ClientService

The public API. Any node accepts these calls, a follower forwards them to the leader.

| RPC | Request | Answer |
| --- | --- | --- |
| `Write` | `key`, `value` | `leader_id`, `index`, `term` of the committed entry |
| `Delete` | `key` | same as `Write` |
| `Read` | `key` | `leader_id`, `value` |
| `Status` | empty | role, term, known leader, commit and applied index, per peer progress |
| `InspectLog` | empty | the local log entries plus the state machine of this node |

`Write` and `Delete` return only after a majority of nodes stored the entry. `Read` is
served by the leader after a heartbeat round confirms a majority.

### Error codes

Errors are plain gRPC status codes, so `raftctl` and any generated client can react to them
without parsing strings.

| Code | When |
| --- | --- |
| `INVALID_ARGUMENT` | The key is empty. |
| `NOT_FOUND` | `Read` on a key the state machine does not have. |
| `UNAVAILABLE` | No leader elected yet, this node is not the leader anymore, or the leader could not reach a majority. |
| `DEADLINE_EXCEEDED` | The entry was appended on the leader but not committed within five seconds. |
| `INTERNAL` | Storage failure. |

`UNAVAILABLE` is the retryable one: the message names the node that should be tried next
when the leader is known.

## RaftService

Consensus traffic between nodes. Clients have no reason to call it.

| RPC | Purpose |
| --- | --- |
| `RequestVote` | Election. Carries the candidate term and last log position. |
| `AppendEntries` | Heartbeat and log replication. Carries the previous log position for the consistency check and the leader commit index. |
| `InstallSnapshot` | Repairs a follower whose missing entries were already compacted. |

`AppendEntriesResponse.match_index` is both the acknowledged position on success and a hint
on failure: it tells the leader the last index the follower holds, so the retry can skip
straight to a plausible position.

## State machine commands

A log entry stores its command as JSON, which keeps `raftctl log` readable:

```json
{"op":"set","key":"course","value":"sna"}
{"op":"delete","key":"course"}
```

Unknown operations are rejected when the entry is applied rather than silently stored, so a
malformed command cannot desynchronise the nodes.

## CLI

```text
raftctl [-addr host:port] [-timeout 5s] <command>

write <key> <value>    append a set command and wait for a majority
read <key>             read a key from the leader
delete <key>           append a delete command and wait for a majority
status                 print the Raft state of one node
log                    print the log and state machine of one node
compare [addr ...]     check that all nodes hold the same committed log
```

`compare` compares the entries every node can be asked about: above the highest snapshot
position and up to the lowest commit index of the nodes it queried. Uncommitted tails are
allowed to differ, that is normal Raft behaviour, so comparing them would produce false
alarms.
