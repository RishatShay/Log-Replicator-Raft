package raft

import (
	"context"
	"time"

	"github.com/RishatShay/sna-final-project/internal/storage"
	"github.com/RishatShay/sna-final-project/internal/wire"
)

func (n *Node) RequestVote(_ context.Context, req *wire.RequestVoteRequest) (*wire.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.GetTerm() < n.currentTerm {
		n.metrics.IncRPC("RequestVote", "stale_term")
		return &wire.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}, nil
	}
	if req.GetTerm() > n.currentTerm {
		n.stepDownLocked(req.GetTerm(), "")
	}

	lastLogIndex, lastLogTerm, err := n.store.LastIndexAndTerm()
	if err != nil {
		n.metrics.IncRPC("RequestVote", "error")
		return nil, wrapInternal(err)
	}
	upToDate := req.GetLastLogTerm() > lastLogTerm ||
		(req.GetLastLogTerm() == lastLogTerm && req.GetLastLogIndex() >= lastLogIndex)
	canVote := n.votedFor == "" || n.votedFor == req.GetCandidateId()

	if canVote && upToDate {
		n.votedFor = req.GetCandidateId()
		if err := n.store.SaveTermVote(n.currentTerm, n.votedFor); err != nil {
			n.metrics.IncRPC("RequestVote", "error")
			return nil, wrapInternal(err)
		}
		n.resetElectionDeadlineLocked()
		n.refreshMetricsLocked()
		n.metrics.IncRPC("RequestVote", "granted")
		return &wire.RequestVoteResponse{Term: n.currentTerm, VoteGranted: true}, nil
	}

	n.metrics.IncRPC("RequestVote", "rejected")
	return &wire.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}, nil
}

func (n *Node) AppendEntries(_ context.Context, req *wire.AppendEntriesRequest) (*wire.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.GetTerm() < n.currentTerm {
		n.metrics.IncRPC("AppendEntries", "stale_term")
		lastIndex, _, _ := n.store.LastIndexAndTerm()
		return &wire.AppendEntriesResponse{Term: n.currentTerm, Success: false, MatchIndex: lastIndex}, nil
	}
	if req.GetTerm() > n.currentTerm || n.role != RoleFollower {
		n.stepDownLocked(req.GetTerm(), req.GetLeaderId())
	}
	n.leaderID = req.GetLeaderId()
	n.resetElectionDeadlineLocked()

	snapIndex, _, err := n.store.SnapshotIndexTerm()
	if err != nil {
		n.metrics.IncRPC("AppendEntries", "error")
		return nil, wrapInternal(err)
	}
	if req.GetPrevLogIndex() < snapIndex {
		n.metrics.IncRPC("AppendEntries", "behind_snapshot")
		return &wire.AppendEntriesResponse{Term: n.currentTerm, Success: false, MatchIndex: snapIndex}, nil
	}
	prevTerm, ok, err := n.store.Term(req.GetPrevLogIndex())
	if err != nil {
		n.metrics.IncRPC("AppendEntries", "error")
		return nil, wrapInternal(err)
	}
	if !ok || prevTerm != req.GetPrevLogTerm() {
		n.metrics.IncRPC("AppendEntries", "log_mismatch")
		lastIndex, _, _ := n.store.LastIndexAndTerm()
		return &wire.AppendEntriesResponse{Term: n.currentTerm, Success: false, MatchIndex: lastIndex}, nil
	}

	incoming := make([]storage.Entry, 0, len(req.GetEntries()))
	matchIndex := req.GetPrevLogIndex()
	for _, entry := range req.GetEntries() {
		if entry.GetIndex() <= snapIndex {
			continue
		}
		if entry.GetIndex() > matchIndex {
			matchIndex = entry.GetIndex()
		}
		incoming = append(incoming, storage.Entry{
			Index:   entry.GetIndex(),
			Term:    entry.GetTerm(),
			Command: entry.GetCommand(),
		})
	}
	toAppend := incoming[:0]
	for i, entry := range incoming {
		localTerm, exists, err := n.store.Term(entry.Index)
		if err != nil {
			n.metrics.IncRPC("AppendEntries", "error")
			return nil, wrapInternal(err)
		}
		if !exists {
			toAppend = incoming[i:]
			break
		}
		if localTerm != entry.Term {
			if err := n.store.DeleteFrom(entry.Index); err != nil {
				n.metrics.IncRPC("AppendEntries", "error")
				return nil, wrapInternal(err)
			}
			toAppend = incoming[i:]
			break
		}
	}
	if len(toAppend) > 0 {
		if err := n.store.AppendEntries(toAppend); err != nil {
			n.metrics.IncRPC("AppendEntries", "error")
			return nil, wrapInternal(err)
		}
	}

	lastIndex, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		n.metrics.IncRPC("AppendEntries", "error")
		return nil, wrapInternal(err)
	}
	if req.GetLeaderCommit() > n.commitIndex {
		n.commitIndex = minUint64(req.GetLeaderCommit(), lastIndex)
		n.applyCommittedLocked()
	}
	n.refreshMetricsLocked()
	n.metrics.IncRPC("AppendEntries", "success")
	return &wire.AppendEntriesResponse{Term: n.currentTerm, Success: true, MatchIndex: matchIndex}, nil
}

func (n *Node) InstallSnapshot(_ context.Context, req *wire.InstallSnapshotRequest) (*wire.InstallSnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.GetTerm() < n.currentTerm {
		n.metrics.IncRPC("InstallSnapshot", "stale_term")
		return &wire.InstallSnapshotResponse{Term: n.currentTerm}, nil
	}
	if req.GetTerm() > n.currentTerm || n.role != RoleFollower {
		n.stepDownLocked(req.GetTerm(), req.GetLeaderId())
	}
	n.leaderID = req.GetLeaderId()
	n.resetElectionDeadlineLocked()

	currentSnapIndex, _, err := n.store.SnapshotIndexTerm()
	if err != nil {
		n.metrics.IncRPC("InstallSnapshot", "error")
		return nil, wrapInternal(err)
	}
	if req.GetLastIncludedIndex() <= currentSnapIndex {
		n.metrics.IncRPC("InstallSnapshot", "ignored")
		return &wire.InstallSnapshotResponse{Term: n.currentTerm}, nil
	}

	if err := n.store.InstallSnapshot(storage.Snapshot{
		LastIncludedIndex: req.GetLastIncludedIndex(),
		LastIncludedTerm:  req.GetLastIncludedTerm(),
		Data:              req.GetData(),
	}); err != nil {
		n.metrics.IncRPC("InstallSnapshot", "error")
		return nil, wrapInternal(err)
	}
	n.commitIndex = maxUint64(n.commitIndex, req.GetLastIncludedIndex())
	n.lastApplied = maxUint64(n.lastApplied, req.GetLastIncludedIndex())
	n.refreshMetricsLocked()
	n.metrics.IncRPC("InstallSnapshot", "success")
	return &wire.InstallSnapshotResponse{Term: n.currentTerm}, nil
}

func (n *Node) Write(ctx context.Context, req *wire.ClientWriteRequest) (*wire.ClientWriteResponse, error) {
	if req.GetKey() == "" {
		return &wire.ClientWriteResponse{Success: false, Error: "key is required"}, nil
	}

	n.mu.Lock()
	if n.role != RoleLeader {
		leaderID := n.leaderID
		client := n.apiClients[leaderID]
		n.mu.Unlock()
		if client != nil {
			return client.Write(ctx, req)
		}
		return newNotLeaderResponse(leaderID), nil
	}
	commandBytes, err := encodeSetCommand(req.GetKey(), req.GetValue())
	if err != nil {
		n.mu.Unlock()
		return nil, wrapInternal(err)
	}
	lastIndex, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		n.mu.Unlock()
		return nil, wrapInternal(err)
	}
	entry := storage.Entry{Index: lastIndex + 1, Term: n.currentTerm, Command: commandBytes}
	if err := n.store.AppendEntries([]storage.Entry{entry}); err != nil {
		n.mu.Unlock()
		return nil, wrapInternal(err)
	}
	n.matchIndex[n.id] = entry.Index
	if n.majorityLocked() == 1 {
		n.advanceCommitLocked()
	}
	n.refreshMetricsLocked()
	term := n.currentTerm
	n.mu.Unlock()

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		successes := n.replicateAllOnce(waitCtx)
		n.mu.Lock()
		if n.role != RoleLeader || n.currentTerm != term {
			leaderID := n.leaderID
			n.mu.Unlock()
			return newNotLeaderResponse(leaderID), nil
		}
		if n.commitIndex >= entry.Index {
			n.mu.Unlock()
			n.logger.Info("client write committed", map[string]any{
				"key":   req.GetKey(),
				"index": entry.Index,
				"term":  term,
			})
			return &wire.ClientWriteResponse{Success: true, LeaderId: n.id, Index: entry.Index, Term: term}, nil
		}
		majority := n.majorityLocked()
		n.mu.Unlock()
		if successes < majority {
			select {
			case <-waitCtx.Done():
				return &wire.ClientWriteResponse{Success: false, LeaderId: n.id, Error: waitCtx.Err().Error(), Index: entry.Index, Term: term}, nil
			case <-ticker.C:
			}
			continue
		}
		select {
		case <-waitCtx.Done():
			return &wire.ClientWriteResponse{Success: false, LeaderId: n.id, Error: waitCtx.Err().Error(), Index: entry.Index, Term: term}, nil
		case <-ticker.C:
		}
	}
}

func (n *Node) Read(ctx context.Context, req *wire.ClientReadRequest) (*wire.ClientReadResponse, error) {
	if req.GetKey() == "" {
		return &wire.ClientReadResponse{Success: false, Error: "key is required"}, nil
	}
	n.mu.Lock()
	if n.role != RoleLeader {
		leaderID := n.leaderID
		client := n.apiClients[leaderID]
		n.mu.Unlock()
		if client != nil {
			return client.Read(ctx, req)
		}
		return &wire.ClientReadResponse{Success: false, LeaderId: leaderID, Error: "not leader"}, nil
	}
	n.mu.Unlock()

	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	successes := n.replicateAllOnce(readCtx)
	cancel()

	n.mu.Lock()
	if n.role != RoleLeader {
		leaderID := n.leaderID
		n.mu.Unlock()
		return &wire.ClientReadResponse{Success: false, LeaderId: leaderID, Error: "not leader"}, nil
	}
	majority := n.majorityLocked()
	n.mu.Unlock()
	if successes < majority {
		return &wire.ClientReadResponse{Success: false, LeaderId: n.id, Error: "could not confirm leader quorum"}, nil
	}

	value, ok, err := n.store.Get(req.GetKey())
	if err != nil {
		return nil, wrapInternal(err)
	}
	if !ok {
		return &wire.ClientReadResponse{Success: false, LeaderId: n.id, Error: "key not found"}, nil
	}
	n.logger.Info("client read served", map[string]any{"key": req.GetKey()})
	return &wire.ClientReadResponse{Success: true, LeaderId: n.id, Value: value}, nil
}

func (n *Node) Status(context.Context, *wire.StatusRequest) (*wire.StatusResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	lastIndex, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		return nil, wrapInternal(err)
	}
	peers := make([]*wire.PeerStatus, 0, len(n.peers))
	for _, peer := range n.peers {
		peers = append(peers, &wire.PeerStatus{
			Id:         peer.ID,
			Address:    peer.Address,
			MatchIndex: n.matchIndex[peer.ID],
			NextIndex:  n.nextIndex[peer.ID],
		})
	}
	leaderID := n.leaderID
	if n.role == RoleLeader {
		leaderID = n.id
	}
	return &wire.StatusResponse{
		Id:           n.id,
		Role:         string(n.role),
		Term:         n.currentTerm,
		LeaderId:     leaderID,
		CommitIndex:  n.commitIndex,
		LastApplied:  n.lastApplied,
		LastLogIndex: lastIndex,
		Peers:        peers,
	}, nil
}

func (n *Node) MustLeaderID() (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == RoleLeader {
		return n.id, true
	}
	return n.leaderID, false
}
