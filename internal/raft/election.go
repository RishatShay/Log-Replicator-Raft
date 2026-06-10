package raft

import (
	"context"

	"github.com/RishatShay/sna-final-project/internal/raftpb"
)

// candidacy is the snapshot of state an election round runs with.
type candidacy struct {
	term         uint64
	lastLogIndex uint64
	lastLogTerm  uint64
	peers        []Peer
	majority     int
}

// startElection promotes the node to candidate and collects votes for the new
// term. It returns once the node wins, loses or learns about a newer term.
func (n *Node) startElection() {
	round, ok := n.becomeCandidate()
	if !ok {
		return
	}

	votes := 1 // a candidate always votes for itself
	if votes >= round.majority {
		n.becomeLeader(round.term)
		return
	}

	replies := make(chan *raftpb.RequestVoteResponse, len(round.peers))
	for _, peer := range round.peers {
		peer := peer
		go func() { replies <- n.requestVote(peer, round) }()
	}

	for range round.peers {
		var reply *raftpb.RequestVoteResponse
		select {
		case <-n.ctx.Done():
			return
		case reply = <-replies:
		}
		if reply == nil {
			continue
		}
		if n.observeHigherTerm(reply.GetTerm()) {
			return
		}
		if !reply.GetVoteGranted() {
			continue
		}
		votes++
		if votes >= round.majority {
			n.becomeLeader(round.term)
			return
		}
	}
	n.log.Info("election lost", "term", round.term, "votes", votes, "needed", round.majority)
}

// becomeCandidate bumps the term, votes for itself and persists both before any
// vote is requested, so a restart can never produce a second vote in one term.
func (n *Node) becomeCandidate() (candidacy, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role == RoleLeader {
		return candidacy{}, false
	}
	n.role = RoleCandidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	n.resetElectionDeadlineLocked()

	if err := n.store.SaveTermVote(n.currentTerm, n.votedFor); err != nil {
		n.log.Error("persist candidacy", "error", err)
		return candidacy{}, false
	}
	lastLogIndex, lastLogTerm, err := n.store.LastIndexAndTerm()
	if err != nil {
		n.log.Error("read last log position", "error", err)
		return candidacy{}, false
	}

	n.metrics.ElectionStarted()
	n.publishStateLocked()
	n.log.Info("election started", "term", n.currentTerm, "last_log_index", lastLogIndex)
	return candidacy{
		term:         n.currentTerm,
		lastLogIndex: lastLogIndex,
		lastLogTerm:  lastLogTerm,
		peers:        n.peers,
		majority:     n.majorityLocked(),
	}, true
}

// becomeLeader takes over the cluster unless the node already moved on to
// another term or role while votes were being collected.
func (n *Node) becomeLeader(term uint64) {
	n.mu.Lock()
	if n.role != RoleCandidate || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	lastLogIndex, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		n.log.Error("read last log position", "error", err)
		n.mu.Unlock()
		return
	}

	n.role = RoleLeader
	n.leaderID = n.id
	for _, peer := range n.peers {
		n.nextIndex[peer.ID] = lastLogIndex + 1
		n.matchIndex[peer.ID] = 0
	}
	n.publishStateLocked()
	n.log.Info("became leader", "term", term, "last_log_index", lastLogIndex)
	singleNode := len(n.peers) == 0
	n.mu.Unlock()

	if singleNode {
		n.commitAndApply()
		return
	}
	// Announce leadership right away instead of waiting for the next heartbeat.
	n.signalReplication()
}

// RequestVote implements the RaftService election RPC.
func (n *Node) RequestVote(_ context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.GetTerm() < n.currentTerm {
		n.metrics.RPCHandled("RequestVote", "stale_term")
		return &raftpb.RequestVoteResponse{Term: n.currentTerm}, nil
	}
	if req.GetTerm() > n.currentTerm {
		n.stepDownLocked(req.GetTerm(), "")
	}

	lastLogIndex, lastLogTerm, err := n.store.LastIndexAndTerm()
	if err != nil {
		n.metrics.RPCHandled("RequestVote", "error")
		return nil, internalError(err)
	}
	// A candidate may only win if its log is at least as complete as ours.
	upToDate := req.GetLastLogTerm() > lastLogTerm ||
		(req.GetLastLogTerm() == lastLogTerm && req.GetLastLogIndex() >= lastLogIndex)
	votedForSomeoneElse := n.votedFor != "" && n.votedFor != req.GetCandidateId()

	if votedForSomeoneElse || !upToDate {
		n.metrics.RPCHandled("RequestVote", "rejected")
		return &raftpb.RequestVoteResponse{Term: n.currentTerm}, nil
	}

	n.votedFor = req.GetCandidateId()
	if err := n.store.SaveTermVote(n.currentTerm, n.votedFor); err != nil {
		n.metrics.RPCHandled("RequestVote", "error")
		return nil, internalError(err)
	}
	n.resetElectionDeadlineLocked()
	n.publishStateLocked()
	n.metrics.RPCHandled("RequestVote", "granted")
	n.log.Info("vote granted", "term", n.currentTerm, "candidate_id", req.GetCandidateId())
	return &raftpb.RequestVoteResponse{Term: n.currentTerm, VoteGranted: true}, nil
}

func (n *Node) requestVote(peer Peer, round candidacy) *raftpb.RequestVoteResponse {
	ctx, cancel := context.WithTimeout(n.ctx, rpcTimeout)
	defer cancel()

	reply, err := n.raftClients[peer.ID].RequestVote(ctx, &raftpb.RequestVoteRequest{
		Term:         round.term,
		CandidateId:  n.id,
		LastLogIndex: round.lastLogIndex,
		LastLogTerm:  round.lastLogTerm,
	})
	if err != nil {
		n.log.Debug("request vote failed", "peer_id", peer.ID, "error", err)
		return nil
	}
	return reply
}

// observeHigherTerm steps down when a peer reports a newer term and reports
// whether that happened.
func (n *Node) observeHigherTerm(term uint64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if term <= n.currentTerm {
		return false
	}
	n.stepDownLocked(term, "")
	return true
}
