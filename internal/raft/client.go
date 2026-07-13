package raft

import (
	"context"
	"errors"
	"time"

	"github.com/RishatShay/sna-final-project/internal/raftpb"
	"github.com/RishatShay/sna-final-project/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// writeTimeout bounds how long a client write waits for a majority.
	writeTimeout = 5 * time.Second
	// readQuorumTimeout bounds the heartbeat round that confirms leadership
	// before a read is served.
	readQuorumTimeout  = 2 * time.Second
	commitPollInterval = 10 * time.Millisecond
	// forwardedHeader marks a request a follower already passed on.
	forwardedHeader = "x-raft-forwarded"
)

// Write stores key=value through the replicated log.
func (n *Node) Write(ctx context.Context, req *raftpb.WriteRequest) (*raftpb.WriteResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	leader, err := n.leaderClient(ctx)
	if err != nil {
		return nil, err
	}
	if leader != nil {
		return leader.Write(forwardContext(ctx), req)
	}
	return n.propose(ctx, setCommand(req.GetKey(), req.GetValue()))
}

// Delete removes a key through the replicated log.
func (n *Node) Delete(ctx context.Context, req *raftpb.DeleteRequest) (*raftpb.WriteResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	leader, err := n.leaderClient(ctx)
	if err != nil {
		return nil, err
	}
	if leader != nil {
		return leader.Delete(forwardContext(ctx), req)
	}
	return n.propose(ctx, deleteCommand(req.GetKey()))
}

// Read returns the committed value of a key.
func (n *Node) Read(ctx context.Context, req *raftpb.ReadRequest) (*raftpb.ReadResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	leader, err := n.leaderClient(ctx)
	if err != nil {
		return nil, err
	}
	if leader != nil {
		return leader.Read(forwardContext(ctx), req)
	}
	// A leader that lost contact with the cluster may hold stale data, so reads
	// are served only after a fresh heartbeat round reaches a majority.
	if err := n.confirmLeadership(ctx); err != nil {
		return nil, err
	}

	n.mu.Lock()
	value, found, err := n.store.Get(req.GetKey())
	n.mu.Unlock()
	if err != nil {
		return nil, internalError(err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "key %q does not exist", req.GetKey())
	}
	return &raftpb.ReadResponse{LeaderId: n.id, Value: value}, nil
}

// Status reports the Raft state of this node, including per peer progress.
func (n *Node) Status(context.Context, *raftpb.StatusRequest) (*raftpb.StatusResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	lastLogIndex, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		return nil, internalError(err)
	}
	snapshotIndex, _, err := n.store.SnapshotIndexTerm()
	if err != nil {
		return nil, internalError(err)
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
		Id:            n.id,
		Role:          string(n.role),
		Term:          n.currentTerm,
		LeaderId:      leaderID,
		CommitIndex:   n.commitIndex,
		LastApplied:   n.lastApplied,
		LastLogIndex:  lastLogIndex,
		SnapshotIndex: snapshotIndex,
		Peers:         peers,
	}, nil
}

// InspectLog dumps the local log and state machine. Clients use it to check that
// every node holds the same sequence of entries.
func (n *Node) InspectLog(context.Context, *raftpb.InspectLogRequest) (*raftpb.InspectLogResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	snapshotIndex, _, err := n.store.SnapshotIndexTerm()
	if err != nil {
		return nil, internalError(err)
	}
	entries, err := n.store.EntriesFrom(snapshotIndex+1, 0)
	if err != nil {
		return nil, internalError(err)
	}
	state, err := n.store.All()
	if err != nil {
		return nil, internalError(err)
	}

	payload := make([]*raftpb.LogEntry, 0, len(entries))
	for _, entry := range entries {
		payload = append(payload, &raftpb.LogEntry{Index: entry.Index, Term: entry.Term, Command: entry.Command})
	}
	return &raftpb.InspectLogResponse{
		Id:            n.id,
		Role:          string(n.role),
		Term:          n.currentTerm,
		CommitIndex:   n.commitIndex,
		LastApplied:   n.lastApplied,
		SnapshotIndex: snapshotIndex,
		Entries:       payload,
		State:         state,
	}, nil
}

// propose appends a command to the log and returns once a majority stored it.
func (n *Node) propose(ctx context.Context, cmd command) (*raftpb.WriteResponse, error) {
	payload, err := cmd.encode()
	if err != nil {
		return nil, internalError(err)
	}

	entry, term, err := n.appendClientEntry(payload)
	if err != nil {
		return nil, err
	}

	started := time.Now()
	n.signalReplication()
	if err := n.waitForCommit(ctx, entry.Index, term); err != nil {
		return nil, err
	}
	n.metrics.CommitLatency(time.Since(started))

	n.log.Info("entry committed", "index", entry.Index, "term", term, "op", cmd.Op, "key", cmd.Key)
	return &raftpb.WriteResponse{LeaderId: n.id, Index: entry.Index, Term: term}, nil
}

func (n *Node) appendClientEntry(payload []byte) (storage.Entry, uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != RoleLeader {
		return storage.Entry{}, 0, notLeaderError(n.leaderID)
	}
	lastLogIndex, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		return storage.Entry{}, 0, internalError(err)
	}

	entry := storage.Entry{Index: lastLogIndex + 1, Term: n.currentTerm, Command: payload}
	if err := n.store.AppendEntries([]storage.Entry{entry}); err != nil {
		return storage.Entry{}, 0, internalError(err)
	}
	if len(n.peers) == 0 {
		// A single node cluster is its own majority.
		n.advanceCommitLocked()
	}
	n.publishStateLocked()
	return entry, n.currentTerm, nil
}

// waitForCommit blocks until the entry is committed, the node loses leadership or
// the caller gives up.
func (n *Node) waitForCommit(ctx context.Context, index, term uint64) error {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	ticker := time.NewTicker(commitPollInterval)
	defer ticker.Stop()

	for {
		n.mu.Lock()
		committed := n.commitIndex >= index
		lostLeadership := n.role != RoleLeader || n.currentTerm != term
		leaderID := n.leaderID
		n.mu.Unlock()

		switch {
		case committed:
			return nil
		case lostLeadership:
			return notLeaderError(leaderID)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return status.Errorf(codes.DeadlineExceeded,
					"entry %d is stored on the leader but was not committed by a majority", index)
			}
			return status.FromContextError(ctx.Err()).Err()
		}
	}
}

// confirmLeadership replicates once to every peer and fails when the leader
// cannot reach a majority.
func (n *Node) confirmLeadership(ctx context.Context) error {
	roundCtx, cancel := context.WithTimeout(ctx, readQuorumTimeout)
	defer cancel()
	inSync := n.replicateAll(roundCtx)

	n.mu.Lock()
	isLeader := n.role == RoleLeader
	leaderID := n.leaderID
	majority := n.majorityLocked()
	n.mu.Unlock()

	if !isLeader {
		return notLeaderError(leaderID)
	}
	if inSync < majority {
		return status.Errorf(codes.Unavailable, "only %d of %d nodes answered, refusing to serve a stale read", inSync, majority)
	}
	return nil
}

// leaderClient returns a client for the node that should handle a client call, or
// nil when this node is the leader itself.
func (n *Node) leaderClient(ctx context.Context) (raftpb.ClientServiceClient, error) {
	n.mu.Lock()
	isLeader := n.role == RoleLeader
	leaderID := n.leaderID
	n.mu.Unlock()

	if isLeader {
		return nil, nil
	}
	if leaderID == "" {
		return nil, status.Error(codes.Unavailable, "no leader has been elected yet")
	}
	// A request is forwarded at most once: during an election two nodes can
	// briefly believe that the other one is the leader.
	if wasForwarded(ctx) {
		return nil, notLeaderError(leaderID)
	}
	client, known := n.kvClients[leaderID]
	if !known {
		return nil, status.Errorf(codes.Internal, "leader %q is not a configured peer", leaderID)
	}
	return client, nil
}

func forwardContext(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, forwardedHeader, "true")
}

func wasForwarded(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	return ok && len(md.Get(forwardedHeader)) > 0
}

func notLeaderError(leaderID string) error {
	if leaderID == "" {
		return status.Error(codes.Unavailable, "no leader has been elected yet")
	}
	return status.Errorf(codes.Unavailable, "this node is not the leader, try %q", leaderID)
}

func internalError(err error) error {
	return status.Error(codes.Internal, err.Error())
}
