package raft

import (
	"context"
	"time"

	"github.com/RishatShay/sna-final-project/internal/raftpb"
	"github.com/RishatShay/sna-final-project/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (n *Node) RequestVote(_ context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.GetTerm() < n.currentTerm {
		n.metrics.RPCHandled("RequestVote", "stale_term")
		return &raftpb.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}, nil
	}
	if req.GetTerm() > n.currentTerm {
		n.stepDownLocked(req.GetTerm(), "")
	}

	lastLogIndex, lastLogTerm, err := n.store.LastIndexAndTerm()
	if err != nil {
		n.metrics.RPCHandled("RequestVote", "error")
		return nil, wrapInternal(err)
	}
	upToDate := req.GetLastLogTerm() > lastLogTerm ||
		(req.GetLastLogTerm() == lastLogTerm && req.GetLastLogIndex() >= lastLogIndex)
	canVote := n.votedFor == "" || n.votedFor == req.GetCandidateId()

	if canVote && upToDate {
		n.votedFor = req.GetCandidateId()
		if err := n.store.SaveTermVote(n.currentTerm, n.votedFor); err != nil {
			n.metrics.RPCHandled("RequestVote", "error")
			return nil, wrapInternal(err)
		}
		n.resetElectionDeadlineLocked()
		n.refreshMetricsLocked()
		n.metrics.RPCHandled("RequestVote", "granted")
		return &raftpb.RequestVoteResponse{Term: n.currentTerm, VoteGranted: true}, nil
	}

	n.metrics.RPCHandled("RequestVote", "rejected")
	return &raftpb.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}, nil
}

func (n *Node) AppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.GetTerm() < n.currentTerm {
		n.metrics.RPCHandled("AppendEntries", "stale_term")
		lastIndex, _, _ := n.store.LastIndexAndTerm()
		return &raftpb.AppendEntriesResponse{Term: n.currentTerm, Success: false, MatchIndex: lastIndex}, nil
	}
	if req.GetTerm() > n.currentTerm || n.role != RoleFollower {
		n.stepDownLocked(req.GetTerm(), req.GetLeaderId())
	}
	n.leaderID = req.GetLeaderId()
	n.resetElectionDeadlineLocked()

	snapIndex, _, err := n.store.SnapshotIndexTerm()
	if err != nil {
		n.metrics.RPCHandled("AppendEntries", "error")
		return nil, wrapInternal(err)
	}
	if req.GetPrevLogIndex() < snapIndex {
		n.metrics.RPCHandled("AppendEntries", "behind_snapshot")
		return &raftpb.AppendEntriesResponse{Term: n.currentTerm, Success: false, MatchIndex: snapIndex}, nil
	}
	prevTerm, ok, err := n.store.Term(req.GetPrevLogIndex())
	if err != nil {
		n.metrics.RPCHandled("AppendEntries", "error")
		return nil, wrapInternal(err)
	}
	if !ok || prevTerm != req.GetPrevLogTerm() {
		n.metrics.RPCHandled("AppendEntries", "log_mismatch")
		lastIndex, _, _ := n.store.LastIndexAndTerm()
		return &raftpb.AppendEntriesResponse{Term: n.currentTerm, Success: false, MatchIndex: lastIndex}, nil
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
			n.metrics.RPCHandled("AppendEntries", "error")
			return nil, wrapInternal(err)
		}
		if !exists {
			toAppend = incoming[i:]
			break
		}
		if localTerm != entry.Term {
			if err := n.store.DeleteFrom(entry.Index); err != nil {
				n.metrics.RPCHandled("AppendEntries", "error")
				return nil, wrapInternal(err)
			}
			toAppend = incoming[i:]
			break
		}
	}
	if len(toAppend) > 0 {
		if err := n.store.AppendEntries(toAppend); err != nil {
			n.metrics.RPCHandled("AppendEntries", "error")
			return nil, wrapInternal(err)
		}
	}

	lastIndex, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		n.metrics.RPCHandled("AppendEntries", "error")
		return nil, wrapInternal(err)
	}
	if req.GetLeaderCommit() > n.commitIndex {
		n.commitIndex = minUint64(req.GetLeaderCommit(), lastIndex)
		n.applyCommittedLocked()
	}
	n.refreshMetricsLocked()
	n.metrics.RPCHandled("AppendEntries", "success")
	return &raftpb.AppendEntriesResponse{Term: n.currentTerm, Success: true, MatchIndex: matchIndex}, nil
}

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

	currentSnapIndex, _, err := n.store.SnapshotIndexTerm()
	if err != nil {
		n.metrics.RPCHandled("InstallSnapshot", "error")
		return nil, wrapInternal(err)
	}
	if req.GetLastIncludedIndex() <= currentSnapIndex {
		n.metrics.RPCHandled("InstallSnapshot", "ignored")
		return &raftpb.InstallSnapshotResponse{Term: n.currentTerm}, nil
	}

	if err := n.store.InstallSnapshot(storage.Snapshot{
		LastIncludedIndex: req.GetLastIncludedIndex(),
		LastIncludedTerm:  req.GetLastIncludedTerm(),
		Data:              req.GetData(),
	}); err != nil {
		n.metrics.RPCHandled("InstallSnapshot", "error")
		return nil, wrapInternal(err)
	}
	n.commitIndex = maxUint64(n.commitIndex, req.GetLastIncludedIndex())
	n.lastApplied = maxUint64(n.lastApplied, req.GetLastIncludedIndex())
	n.refreshMetricsLocked()
	n.metrics.RPCHandled("InstallSnapshot", "success")
	return &raftpb.InstallSnapshotResponse{Term: n.currentTerm}, nil
}

// Write stores key=value through the replicated log. The generated response no
// longer carries a Success/Error pair, so failures are reported as gRPC errors.
func (n *Node) Write(ctx context.Context, req *raftpb.WriteRequest) (*raftpb.WriteResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	n.mu.Lock()
	if n.role != RoleLeader {
		leaderID := n.leaderID
		client := n.apiClients[leaderID]
		n.mu.Unlock()
		if client != nil {
			return client.Write(ctx, req)
		}
		return nil, notLeaderError(leaderID)
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
			return nil, notLeaderError(leaderID)
		}
		if n.commitIndex >= entry.Index {
			n.mu.Unlock()
			n.logger.Info("client write committed", map[string]any{
				"key":   req.GetKey(),
				"index": entry.Index,
				"term":  term,
			})
			return &raftpb.WriteResponse{LeaderId: n.id, Index: entry.Index, Term: term}, nil
		}
		majority := n.majorityLocked()
		n.mu.Unlock()
		if successes < majority {
			select {
			case <-waitCtx.Done():
				return nil, status.Errorf(codes.DeadlineExceeded, "entry %d was not committed by a majority", entry.Index)
			case <-ticker.C:
			}
			continue
		}
		select {
		case <-waitCtx.Done():
			return nil, status.Errorf(codes.DeadlineExceeded, "entry %d was not committed by a majority", entry.Index)
		case <-ticker.C:
		}
	}
}

func (n *Node) Read(ctx context.Context, req *raftpb.ReadRequest) (*raftpb.ReadResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	n.mu.Lock()
	if n.role != RoleLeader {
		leaderID := n.leaderID
		client := n.apiClients[leaderID]
		n.mu.Unlock()
		if client != nil {
			return client.Read(ctx, req)
		}
		return nil, notLeaderError(leaderID)
	}
	n.mu.Unlock()

	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	successes := n.replicateAllOnce(readCtx)
	cancel()

	n.mu.Lock()
	if n.role != RoleLeader {
		leaderID := n.leaderID
		n.mu.Unlock()
		return nil, notLeaderError(leaderID)
	}
	majority := n.majorityLocked()
	n.mu.Unlock()
	if successes < majority {
		return nil, status.Error(codes.Unavailable, "could not confirm leader quorum")
	}

	value, ok, err := n.store.Get(req.GetKey())
	if err != nil {
		return nil, wrapInternal(err)
	}
	if !ok {
		return nil, status.Errorf(codes.NotFound, "key %q does not exist", req.GetKey())
	}
	n.logger.Info("client read served", map[string]any{"key": req.GetKey()})
	return &raftpb.ReadResponse{LeaderId: n.id, Value: value}, nil
}

func (n *Node) Status(context.Context, *raftpb.StatusRequest) (*raftpb.StatusResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	lastIndex, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		return nil, wrapInternal(err)
	}
	peers := make([]*raftpb.PeerStatus, 0, len(n.peers))
	for _, peer := range n.peers {
		peers = append(peers, &raftpb.PeerStatus{
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
	return &raftpb.StatusResponse{
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

func notLeaderError(leaderID string) error {
	if leaderID == "" {
		return status.Error(codes.Unavailable, "no leader has been elected yet")
	}
	return status.Errorf(codes.Unavailable, "this node is not the leader, try %q", leaderID)
}
