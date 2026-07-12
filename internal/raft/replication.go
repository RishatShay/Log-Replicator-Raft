package raft

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RishatShay/sna-final-project/internal/raftpb"
)

// maxAppendRounds bounds how often one replication call may walk nextIndex back
// before giving up until the next heartbeat.
const maxAppendRounds = 32

// snapshotRPCTimeout is larger than rpcTimeout because a snapshot carries the
// whole state machine.
const snapshotRPCTimeout = 3 * time.Second

// appendOutcome is the result of a single AppendEntries round.
type appendOutcome int

const (
	appendAccepted appendOutcome = iota
	appendRejected
	appendFailed
)

// runReplication keeps one peer up to date: on every heartbeat tick, and
// immediately when a client write is waiting for a majority.
func (n *Node) runReplication(peer Peer) {
	ticker := time.NewTicker(n.heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
		case <-n.replicateNow[peer.ID]:
		}
		if n.isLeader() {
			n.replicatePeer(n.ctx, peer.ID)
		}
	}
}

// signalReplication wakes up every peer loop without blocking: a pending wake-up
// is as good as a new one.
func (n *Node) signalReplication() {
	for _, wake := range n.replicateNow {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

// replicateAll runs one replication round against every peer and reports how many
// nodes, including the leader itself, are known to be in sync afterwards.
func (n *Node) replicateAll(ctx context.Context) int {
	var inSync atomic.Int32
	inSync.Store(1)

	var wg sync.WaitGroup
	for _, peer := range n.peers {
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()
			if n.replicatePeer(ctx, peer.ID) {
				inSync.Add(1)
			}
		}()
	}
	wg.Wait()
	return int(inSync.Load())
}

// replicatePeer brings a single peer up to date. When the follower rejects the
// previous log position, nextIndex walks back until the logs match again.
func (n *Node) replicatePeer(ctx context.Context, peerID string) bool {
	n.sendMu[peerID].Lock()
	defer n.sendMu[peerID].Unlock()

	for round := 0; round < maxAppendRounds; round++ {
		if ctx.Err() != nil || !n.isLeader() {
			return false
		}
		needsSnapshot, err := n.peerNeedsSnapshot(peerID)
		if err != nil {
			n.log.Error("check snapshot position", "peer_id", peerID, "error", err)
			return false
		}
		if needsSnapshot {
			return n.sendSnapshot(ctx, peerID)
		}
		switch n.sendEntries(ctx, peerID) {
		case appendAccepted:
			return true
		case appendFailed:
			return false
		case appendRejected:
			// nextIndex was lowered, try again with an earlier position.
		}
	}
	return false
}

// peerNeedsSnapshot reports whether the entries a peer is missing were already
// compacted away, in which case only a snapshot can bring it back in sync.
func (n *Node) peerNeedsSnapshot(peerID string) (bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	snapshotIndex, _, err := n.store.SnapshotIndexTerm()
	if err != nil {
		return false, err
	}
	return n.nextIndexLocked(peerID) <= snapshotIndex, nil
}

func (n *Node) sendEntries(ctx context.Context, peerID string) appendOutcome {
	n.mu.Lock()
	term := n.currentTerm
	req, ok, err := n.buildAppendRequestLocked(peerID)
	n.mu.Unlock()
	if err != nil {
		n.log.Error("build append request", "peer_id", peerID, "error", err)
		return appendFailed
	}
	if !ok {
		return appendRejected
	}

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	reply, err := n.raftClients[peerID].AppendEntries(rpcCtx, req)
	if err != nil {
		n.notePeerUnreachable(peerID, "append entries failed", err)
		return appendFailed
	}
	if n.observeHigherTerm(reply.GetTerm()) {
		return appendFailed
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	n.notePeerReachableLocked(peerID)
	if n.role != RoleLeader || n.currentTerm != term {
		return appendFailed
	}
	if !reply.GetSuccess() {
		n.nextIndex[peerID] = max(n.nextIndexLocked(peerID)-1, 1)
		return appendRejected
	}

	n.recordPeerProgressLocked(peerID, max(reply.GetMatchIndex(), lastEntryIndex(req)))
	n.advanceCommitLocked()
	n.publishStateLocked()
	return appendAccepted
}

// buildAppendRequestLocked prepares the next AppendEntries call for a peer. The
// second result is false when the local log cannot describe the position the peer
// is at; nextIndex is lowered in that case so the caller can retry.
func (n *Node) buildAppendRequestLocked(peerID string) (*raftpb.AppendEntriesRequest, bool, error) {
	next := n.nextIndexLocked(peerID)
	prevLogIndex := next - 1
	prevLogTerm, known, err := n.store.Term(prevLogIndex)
	if err != nil {
		return nil, false, err
	}
	if !known {
		n.nextIndex[peerID] = max(prevLogIndex, 1)
		return nil, false, nil
	}

	entries, err := n.store.EntriesFrom(next, appendBatchSize)
	if err != nil {
		return nil, false, err
	}
	payload := make([]*raftpb.LogEntry, 0, len(entries))
	for _, entry := range entries {
		payload = append(payload, &raftpb.LogEntry{Index: entry.Index, Term: entry.Term, Command: entry.Command})
	}

	return &raftpb.AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderId:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      payload,
		LeaderCommit: n.commitIndex,
	}, true, nil
}

func (n *Node) sendSnapshot(ctx context.Context, peerID string) bool {
	n.mu.Lock()
	term := n.currentTerm
	snapshot, err := n.store.LoadSnapshot()
	n.mu.Unlock()
	if err != nil {
		n.log.Error("load snapshot", "peer_id", peerID, "error", err)
		return false
	}

	rpcCtx, cancel := context.WithTimeout(ctx, snapshotRPCTimeout)
	defer cancel()
	reply, err := n.raftClients[peerID].InstallSnapshot(rpcCtx, &raftpb.InstallSnapshotRequest{
		Term:              term,
		LeaderId:          n.id,
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
		Data:              snapshot.Data,
	})
	if err != nil {
		n.notePeerUnreachable(peerID, "install snapshot failed", err)
		return false
	}
	if n.observeHigherTerm(reply.GetTerm()) {
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	n.notePeerReachableLocked(peerID)
	if n.role != RoleLeader || n.currentTerm != term {
		return false
	}
	n.log.Info("snapshot shipped", "peer_id", peerID, "last_included_index", snapshot.LastIncludedIndex)
	n.recordPeerProgressLocked(peerID, snapshot.LastIncludedIndex)
	n.advanceCommitLocked()
	n.publishStateLocked()
	return true
}

// commitAndApply recomputes the commit index outside of the replication path,
// which is all a single node cluster needs to make progress.
func (n *Node) commitAndApply() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.advanceCommitLocked()
	n.publishStateLocked()
}

// advanceCommitLocked commits every entry of the current term that a majority of
// the cluster stores, then applies whatever became committed.
func (n *Node) advanceCommitLocked() {
	lastLogIndex, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		n.log.Error("read last log position", "error", err)
		return
	}

	majority := n.majorityLocked()
	committed := n.commitIndex
	for index := n.commitIndex + 1; index <= lastLogIndex; index++ {
		term, known, err := n.store.Term(index)
		if err != nil {
			n.log.Error("read term", "index", index, "error", err)
			return
		}
		// Entries from earlier terms are only committed indirectly, together with
		// a later entry of the current term.
		if !known || term != n.currentTerm {
			continue
		}
		if n.replicaCountLocked(index) >= majority {
			committed = index
		}
	}

	if committed == n.commitIndex {
		return
	}
	n.commitIndex = committed
	n.applyCommittedLocked()
}

// replicaCountLocked counts the nodes that store the entry at index, the leader
// included.
func (n *Node) replicaCountLocked(index uint64) int {
	count := 1
	for _, peer := range n.peers {
		if n.matchIndex[peer.ID] >= index {
			count++
		}
	}
	return count
}

// applyCommittedLocked feeds committed entries to the state machine in order.
func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		index := n.lastApplied + 1
		entry, found, err := n.store.Entry(index)
		if err != nil {
			n.log.Error("read committed entry", "index", index, "error", err)
			return
		}
		if !found {
			if !n.skipCompactedLocked(index) {
				return
			}
			continue
		}

		cmd, err := decodeCommand(entry.Command)
		if err != nil {
			n.log.Error("decode committed command", "index", index, "error", err)
			return
		}
		started := time.Now()
		if err := cmd.applyTo(n.store, index); err != nil {
			n.log.Error("apply command", "index", index, "error", err)
			return
		}
		n.metrics.ApplyLatency(time.Since(started))
		n.lastApplied = index
	}
	n.compactIfNeededLocked()
}

// skipCompactedLocked handles a missing entry: it is fine only when a snapshot
// already covers it, for example right after InstallSnapshot.
func (n *Node) skipCompactedLocked(index uint64) bool {
	snapshotIndex, _, err := n.store.SnapshotIndexTerm()
	if err != nil {
		n.log.Error("read snapshot position", "error", err)
		return false
	}
	if index > snapshotIndex {
		n.log.Error("committed entry is missing from the log", "index", index)
		return false
	}
	if err := n.store.SkipTo(snapshotIndex); err != nil {
		n.log.Error("move applied position", "index", snapshotIndex, "error", err)
		return false
	}
	n.lastApplied = snapshotIndex
	return true
}

// compactIfNeededLocked replaces applied entries with a snapshot once enough of
// them piled up since the previous one.
func (n *Node) compactIfNeededLocked() {
	if n.snapshotThreshold == 0 || n.lastApplied == 0 {
		return
	}
	snapshotIndex, _, err := n.store.SnapshotIndexTerm()
	if err != nil {
		n.log.Error("read snapshot position", "error", err)
		return
	}
	if n.lastApplied <= snapshotIndex || n.lastApplied-snapshotIndex < n.snapshotThreshold {
		return
	}

	term, known, err := n.store.Term(n.lastApplied)
	if err != nil || !known {
		n.log.Error("read term for snapshot", "index", n.lastApplied, "error", err)
		return
	}
	if _, err := n.store.CreateSnapshot(n.lastApplied, term); err != nil {
		n.log.Error("create snapshot", "index", n.lastApplied, "error", err)
		return
	}
	n.log.Info("snapshot created", "last_included_index", n.lastApplied, "last_included_term", term)
}

func (n *Node) recordPeerProgressLocked(peerID string, matchIndex uint64) {
	if matchIndex > n.matchIndex[peerID] {
		n.matchIndex[peerID] = matchIndex
	}
	n.nextIndex[peerID] = n.matchIndex[peerID] + 1

	lastLogIndex, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		return
	}
	n.metrics.SetReplicationLag(peerID, lastLogIndex-min(lastLogIndex, n.matchIndex[peerID]))
}

// notePeerUnreachable logs a failing peer once, until it answers again.
func (n *Node) notePeerUnreachable(peerID, what string, cause error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.unreachable[peerID] {
		return
	}
	n.unreachable[peerID] = true
	n.log.Warn(what, "peer_id", peerID, "error", cause)
}

func (n *Node) notePeerReachableLocked(peerID string) {
	if !n.unreachable[peerID] {
		return
	}
	delete(n.unreachable, peerID)
	n.log.Info("peer answers again", "peer_id", peerID)
}

// nextIndexLocked returns the next index to send to a peer, never below 1.
func (n *Node) nextIndexLocked(peerID string) uint64 {
	return max(n.nextIndex[peerID], 1)
}

func lastEntryIndex(req *raftpb.AppendEntriesRequest) uint64 {
	if entries := req.GetEntries(); len(entries) > 0 {
		return entries[len(entries)-1].GetIndex()
	}
	return req.GetPrevLogIndex()
}
