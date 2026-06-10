package raft

import (
	"context"

	"github.com/RishatShay/sna-final-project/internal/raftpb"
	"github.com/RishatShay/sna-final-project/internal/storage"
)

// AppendEntries implements the RaftService heartbeat and replication RPC.
func (n *Node) AppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.GetTerm() < n.currentTerm {
		n.metrics.RPCHandled("AppendEntries", "stale_term")
		return n.rejectAppendLocked()
	}
	if req.GetTerm() > n.currentTerm || n.role != RoleFollower {
		n.stepDownLocked(req.GetTerm(), req.GetLeaderId())
	}
	n.leaderID = req.GetLeaderId()
	n.resetElectionDeadlineLocked()

	snapshotIndex, _, err := n.store.SnapshotIndexTerm()
	if err != nil {
		n.metrics.RPCHandled("AppendEntries", "error")
		return nil, internalError(err)
	}
	// Our snapshot is ahead of the position the leader assumes: tell it where we are.
	if req.GetPrevLogIndex() < snapshotIndex {
		n.metrics.RPCHandled("AppendEntries", "behind_snapshot")
		return &raftpb.AppendEntriesResponse{Term: n.currentTerm, MatchIndex: snapshotIndex}, nil
	}

	prevTerm, known, err := n.store.Term(req.GetPrevLogIndex())
	if err != nil {
		n.metrics.RPCHandled("AppendEntries", "error")
		return nil, internalError(err)
	}
	if !known || prevTerm != req.GetPrevLogTerm() {
		n.metrics.RPCHandled("AppendEntries", "log_mismatch")
		return n.rejectAppendLocked()
	}

	matchIndex, err := n.storeLeaderEntriesLocked(req.GetEntries(), req.GetPrevLogIndex(), snapshotIndex)
	if err != nil {
		n.metrics.RPCHandled("AppendEntries", "error")
		return nil, internalError(err)
	}
	if err := n.followLeaderCommitLocked(req.GetLeaderCommit()); err != nil {
		n.metrics.RPCHandled("AppendEntries", "error")
		return nil, internalError(err)
	}

	n.publishStateLocked()
	n.metrics.RPCHandled("AppendEntries", "success")
	return &raftpb.AppendEntriesResponse{Term: n.currentTerm, Success: true, MatchIndex: matchIndex}, nil
}

// InstallSnapshot implements the RaftService snapshot RPC. A follower that fell
// too far behind is repaired with the full state machine instead of a log tail.
func (n *Node) InstallSnapshot(_ context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.GetTerm() < n.currentTerm {
		n.metrics.RPCHandled("InstallSnapshot", "stale_term")
		return &raftpb.InstallSnapshotResponse{Term: n.currentTerm}, nil
	}
	if req.GetTerm() > n.currentTerm || n.role != RoleFollower {
		n.stepDownLocked(req.GetTerm(), req.GetLeaderId())
	}
	n.leaderID = req.GetLeaderId()
	n.resetElectionDeadlineLocked()

	snapshotIndex, _, err := n.store.SnapshotIndexTerm()
	if err != nil {
		n.metrics.RPCHandled("InstallSnapshot", "error")
		return nil, internalError(err)
	}
	if req.GetLastIncludedIndex() <= snapshotIndex {
		n.metrics.RPCHandled("InstallSnapshot", "ignored")
		return &raftpb.InstallSnapshotResponse{Term: n.currentTerm}, nil
	}

	err = n.store.InstallSnapshot(storage.Snapshot{
		LastIncludedIndex: req.GetLastIncludedIndex(),
		LastIncludedTerm:  req.GetLastIncludedTerm(),
		Data:              req.GetData(),
	})
	if err != nil {
		n.metrics.RPCHandled("InstallSnapshot", "error")
		return nil, internalError(err)
	}

	n.commitIndex = max(n.commitIndex, req.GetLastIncludedIndex())
	n.lastApplied = max(n.lastApplied, req.GetLastIncludedIndex())
	n.publishStateLocked()
	n.metrics.RPCHandled("InstallSnapshot", "success")
	n.log.Info("snapshot installed",
		"leader_id", req.GetLeaderId(),
		"last_included_index", req.GetLastIncludedIndex())
	return &raftpb.InstallSnapshotResponse{Term: n.currentTerm}, nil
}

// storeLeaderEntriesLocked appends the entries the follower is missing and
// returns the highest index it holds afterwards. An entry that conflicts with the
// leader is dropped together with everything after it.
func (n *Node) storeLeaderEntriesLocked(entries []*raftpb.LogEntry, prevLogIndex, snapshotIndex uint64) (uint64, error) {
	matchIndex := prevLogIndex
	var missing []storage.Entry

	for _, entry := range entries {
		if entry.GetIndex() <= snapshotIndex {
			continue
		}
		matchIndex = max(matchIndex, entry.GetIndex())
		if len(missing) > 0 {
			// Everything from the first conflict on has to be rewritten anyway.
			missing = append(missing, toStorageEntry(entry))
			continue
		}

		localTerm, exists, err := n.store.Term(entry.GetIndex())
		if err != nil {
			return 0, err
		}
		if exists && localTerm == entry.GetTerm() {
			continue
		}
		if exists {
			if err := n.store.DeleteFrom(entry.GetIndex()); err != nil {
				return 0, err
			}
		}
		missing = append(missing, toStorageEntry(entry))
	}

	if err := n.store.AppendEntries(missing); err != nil {
		return 0, err
	}
	return matchIndex, nil
}

// followLeaderCommitLocked adopts the leader commit index, capped by what this
// node actually stores, and applies the newly committed entries.
func (n *Node) followLeaderCommitLocked(leaderCommit uint64) error {
	if leaderCommit <= n.commitIndex {
		return nil
	}
	lastLogIndex, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		return err
	}
	n.commitIndex = min(leaderCommit, lastLogIndex)
	n.applyCommittedLocked()
	return nil
}

// rejectAppendLocked answers with the local log position so the leader can pick a
// better nextIndex for the retry.
func (n *Node) rejectAppendLocked() (*raftpb.AppendEntriesResponse, error) {
	lastLogIndex, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		return nil, internalError(err)
	}
	return &raftpb.AppendEntriesResponse{Term: n.currentTerm, MatchIndex: lastLogIndex}, nil
}

func toStorageEntry(entry *raftpb.LogEntry) storage.Entry {
	return storage.Entry{
		Index:   entry.GetIndex(),
		Term:    entry.GetTerm(),
		Command: entry.GetCommand(),
	}
}
