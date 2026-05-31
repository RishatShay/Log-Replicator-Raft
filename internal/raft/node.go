package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RishatShay/sna-final-project/internal/logging"
	"github.com/RishatShay/sna-final-project/internal/metrics"
	"github.com/RishatShay/sna-final-project/internal/raftpb"
	"github.com/RishatShay/sna-final-project/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultElectionMin       = 500 * time.Millisecond
	defaultElectionMax       = 900 * time.Millisecond
	defaultHeartbeatInterval = 100 * time.Millisecond
	appendBatchSize          = 128
)

type Node struct {
	raftpb.UnimplementedRaftServiceServer
	raftpb.UnimplementedClientServiceServer

	mu sync.Mutex

	id       string
	grpcAddr string
	httpAddr string
	peers    []Peer

	store   *storage.Store
	logger  *logging.Logger
	metrics *metrics.Metrics

	role        Role
	currentTerm uint64
	votedFor    string
	leaderID    string

	commitIndex uint64
	lastApplied uint64
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64

	electionMin       time.Duration
	electionMax       time.Duration
	heartbeatInterval time.Duration
	snapshotThreshold uint64
	electionDeadline  time.Time

	grpcServer *grpc.Server
	httpServer *http.Server
	conns      map[string]*grpc.ClientConn
	clients    map[string]raftpb.RaftServiceClient
	apiClients map[string]raftpb.ClientServiceClient

	stop     chan struct{}
	stopOnce sync.Once
}

func New(opts Options) (*Node, error) {
	if opts.NodeID == "" {
		return nil, errors.New("node id is required")
	}
	if opts.GRPCAddr == "" {
		return nil, errors.New("gRPC address is required")
	}
	if opts.DataDir == "" {
		return nil, errors.New("data dir is required")
	}
	if opts.ElectionMin == 0 {
		opts.ElectionMin = defaultElectionMin
	}
	if opts.ElectionMax == 0 {
		opts.ElectionMax = defaultElectionMax
	}
	if opts.HeartbeatInterval == 0 {
		opts.HeartbeatInterval = defaultHeartbeatInterval
	}

	store, err := storage.Open(opts.DataDir)
	if err != nil {
		return nil, err
	}
	term, votedFor, err := store.CurrentTermVote()
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	lastApplied, err := store.LastApplied()
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	node := &Node{
		id:                opts.NodeID,
		grpcAddr:          opts.GRPCAddr,
		httpAddr:          opts.HTTPAddr,
		peers:             opts.Peers,
		store:             store,
		logger:            logging.New(opts.NodeID),
		metrics:           metrics.New(opts.NodeID),
		role:              RoleFollower,
		currentTerm:       term,
		votedFor:          votedFor,
		commitIndex:       lastApplied,
		lastApplied:       lastApplied,
		nextIndex:         map[string]uint64{},
		matchIndex:        map[string]uint64{},
		electionMin:       opts.ElectionMin,
		electionMax:       opts.ElectionMax,
		heartbeatInterval: opts.HeartbeatInterval,
		snapshotThreshold: opts.SnapshotThreshold,
		conns:             map[string]*grpc.ClientConn{},
		clients:           map[string]raftpb.RaftServiceClient{},
		apiClients:        map[string]raftpb.ClientServiceClient{},
		stop:              make(chan struct{}),
	}
	node.resetElectionDeadlineLocked()
	node.refreshMetricsLocked()
	return node, nil
}

func (n *Node) Start() error {
	n.mu.Lock()
	for _, peer := range n.peers {
		conn, err := grpc.NewClient(dialTarget(peer.Address), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			n.mu.Unlock()
			return err
		}
		n.conns[peer.ID] = conn
		n.clients[peer.ID] = raftpb.NewRaftServiceClient(conn)
		n.apiClients[peer.ID] = raftpb.NewClientServiceClient(conn)
	}
	n.mu.Unlock()

	lis, err := net.Listen("tcp", n.grpcAddr)
	if err != nil {
		return err
	}
	n.grpcServer = grpc.NewServer()
	raftpb.RegisterRaftServiceServer(n.grpcServer, n)
	raftpb.RegisterClientServiceServer(n.grpcServer, n)
	go func() {
		n.logger.Info("grpc server started", map[string]any{"addr": n.grpcAddr})
		if err := n.grpcServer.Serve(lis); err != nil {
			n.logger.Warn("grpc server stopped", map[string]any{"error": err.Error()})
		}
	}()

	if n.httpAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", n.metrics)
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("ok\n"))
		})
		n.httpServer = &http.Server{Addr: n.httpAddr, Handler: mux, ReadHeaderTimeout: 2 * time.Second}
		go func() {
			n.logger.Info("http server started", map[string]any{"addr": n.httpAddr})
			if err := n.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				n.logger.Warn("http server stopped", map[string]any{"error": err.Error()})
			}
		}()
	}

	go n.run()
	return nil
}

func (n *Node) Stop(ctx context.Context) error {
	var err error
	n.stopOnce.Do(func() {
		close(n.stop)
		if n.grpcServer != nil {
			stopped := make(chan struct{})
			go func() {
				n.grpcServer.GracefulStop()
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-ctx.Done():
				n.grpcServer.Stop()
				err = ctx.Err()
			}
		}
		if n.httpServer != nil {
			if shutdownErr := n.httpServer.Shutdown(ctx); shutdownErr != nil && err == nil {
				err = shutdownErr
			}
		}
		for _, conn := range n.conns {
			if closeErr := conn.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
		}
		if closeErr := n.store.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	})
	return err
}

func (n *Node) run() {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	nextHeartbeat := time.Now().Add(n.heartbeatInterval)
	for {
		select {
		case <-n.stop:
			return
		case <-ticker.C:
			now := time.Now()
			n.mu.Lock()
			role := n.role
			electionDue := role != RoleLeader && now.After(n.electionDeadline)
			heartbeatDue := role == RoleLeader && !now.Before(nextHeartbeat)
			if heartbeatDue {
				nextHeartbeat = now.Add(n.heartbeatInterval)
			}
			n.mu.Unlock()

			if electionDue {
				go n.startElection()
			}
			if heartbeatDue {
				go n.replicateAllOnce(context.Background())
			}
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	if n.role == RoleLeader {
		n.mu.Unlock()
		return
	}
	n.role = RoleCandidate
	n.currentTerm++
	term := n.currentTerm
	n.votedFor = n.id
	n.leaderID = ""
	if err := n.store.SaveTermVote(n.currentTerm, n.votedFor); err != nil {
		n.logger.Error("failed to persist election term", map[string]any{"error": err.Error()})
		n.resetElectionDeadlineLocked()
		n.mu.Unlock()
		return
	}
	lastLogIndex, lastLogTerm, err := n.store.LastIndexAndTerm()
	if err != nil {
		n.logger.Error("failed to read last log for election", map[string]any{"error": err.Error()})
		n.resetElectionDeadlineLocked()
		n.mu.Unlock()
		return
	}
	n.resetElectionDeadlineLocked()
	n.refreshMetricsLocked()
	n.metrics.IncElection()
	majority := n.majorityLocked()
	peers := append([]Peer(nil), n.peers...)
	n.mu.Unlock()

	n.logger.Info("election started", map[string]any{"term": term})
	var votes int32 = 1
	var won atomic.Bool
	for _, peer := range peers {
		peer := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
			defer cancel()
			resp, err := n.clients[peer.ID].RequestVote(ctx, &raftpb.RequestVoteRequest{
				Term:         term,
				CandidateId:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.GetTerm() > n.currentTerm {
				n.stepDownLocked(resp.GetTerm(), "")
				return
			}
			if n.role != RoleCandidate || n.currentTerm != term || !resp.GetVoteGranted() {
				return
			}
			if int(atomic.AddInt32(&votes, 1)) >= majority && won.CompareAndSwap(false, true) {
				n.becomeLeaderLocked()
			}
		}()
	}

	if majority == 1 {
		n.mu.Lock()
		if n.role == RoleCandidate && n.currentTerm == term {
			n.becomeLeaderLocked()
		}
		n.mu.Unlock()
	}
}

func (n *Node) becomeLeaderLocked() {
	lastIndex, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		n.logger.Error("failed to initialize leader replication state", map[string]any{"error": err.Error()})
		return
	}
	n.role = RoleLeader
	n.leaderID = n.id
	n.votedFor = n.id
	for _, peer := range n.peers {
		n.nextIndex[peer.ID] = lastIndex + 1
		n.matchIndex[peer.ID] = 0
		n.metrics.SetReplicationLag(peer.ID, lastIndex)
	}
	n.matchIndex[n.id] = lastIndex
	n.refreshMetricsLocked()
	n.logger.Info("became leader", map[string]any{"term": n.currentTerm})
	go n.replicateAllOnce(context.Background())
}

func (n *Node) stepDownLocked(term uint64, leaderID string) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		if err := n.store.SaveTermVote(n.currentTerm, n.votedFor); err != nil {
			n.logger.Error("failed to persist higher term", map[string]any{"error": err.Error()})
		}
	}
	n.role = RoleFollower
	n.leaderID = leaderID
	n.resetElectionDeadlineLocked()
	n.refreshMetricsLocked()
}

func (n *Node) resetElectionDeadlineLocked() {
	window := n.electionMax - n.electionMin
	jitter := time.Duration(0)
	if window > 0 {
		jitter = time.Duration(rand.Int63n(int64(window)))
	}
	n.electionDeadline = time.Now().Add(n.electionMin + jitter)
}

func (n *Node) majorityLocked() int {
	return (len(n.peers)+1)/2 + 1
}

func (n *Node) refreshMetricsLocked() {
	lastIndex, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		lastIndex = 0
	}
	n.metrics.SetState(string(n.role), n.currentTerm, n.commitIndex, n.lastApplied, lastIndex)
}

func (n *Node) replicateAllOnce(ctx context.Context) int {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return 0
	}
	peers := append([]Peer(nil), n.peers...)
	n.mu.Unlock()

	var successes int32 = 1
	var wg sync.WaitGroup
	for _, peer := range peers {
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()
			if n.replicatePeer(ctx, peer.ID) {
				atomic.AddInt32(&successes, 1)
			}
		}()
	}
	wg.Wait()
	return int(successes)
}

func (n *Node) replicatePeer(ctx context.Context, peerID string) bool {
	for attempt := 0; attempt < 64; attempt++ {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		n.mu.Lock()
		if n.role != RoleLeader {
			n.mu.Unlock()
			return false
		}
		client := n.clients[peerID]
		term := n.currentTerm
		leaderCommit := n.commitIndex
		next := n.nextIndex[peerID]
		if next == 0 {
			next = 1
			n.nextIndex[peerID] = next
		}
		snapIndex, snapTerm, err := n.store.SnapshotIndexTerm()
		if err != nil {
			n.logger.Error("failed to read snapshot metadata", map[string]any{"peer_id": peerID, "error": err.Error()})
			n.mu.Unlock()
			return false
		}
		if next <= snapIndex {
			snapshot, err := n.store.LoadSnapshot()
			if err != nil {
				n.logger.Error("failed to load snapshot", map[string]any{"peer_id": peerID, "error": err.Error()})
				n.mu.Unlock()
				return false
			}
			n.mu.Unlock()

			rpcCtx, cancel := context.WithTimeout(ctx, 900*time.Millisecond)
			resp, rpcErr := client.InstallSnapshot(rpcCtx, &raftpb.InstallSnapshotRequest{
				Term:              term,
				LeaderId:          n.id,
				LastIncludedIndex: snapshot.LastIncludedIndex,
				LastIncludedTerm:  snapshot.LastIncludedTerm,
				Data:              snapshot.Data,
			})
			cancel()
			if rpcErr != nil {
				return false
			}
			n.mu.Lock()
			if resp.GetTerm() > n.currentTerm {
				n.stepDownLocked(resp.GetTerm(), "")
				n.mu.Unlock()
				return false
			}
			n.matchIndex[peerID] = snapshot.LastIncludedIndex
			n.nextIndex[peerID] = snapshot.LastIncludedIndex + 1
			n.advanceCommitLocked()
			n.refreshMetricsLocked()
			n.mu.Unlock()
			return true
		}

		prevLogIndex := next - 1
		prevLogTerm, ok, err := n.store.Term(prevLogIndex)
		if err != nil {
			n.logger.Error("failed to read previous log term", map[string]any{"peer_id": peerID, "error": err.Error()})
			n.mu.Unlock()
			return false
		}
		if !ok && prevLogIndex <= snapIndex {
			prevLogTerm = snapTerm
			ok = true
		}
		if !ok {
			n.nextIndex[peerID] = maxUint64(1, next-1)
			n.mu.Unlock()
			continue
		}
		entries, err := n.store.EntriesFrom(next, appendBatchSize)
		if err != nil {
			n.logger.Error("failed to read entries for replication", map[string]any{"peer_id": peerID, "error": err.Error()})
			n.mu.Unlock()
			return false
		}
		reqEntries := make([]*raftpb.LogEntry, 0, len(entries))
		lastSent := prevLogIndex
		for _, entry := range entries {
			reqEntries = append(reqEntries, &raftpb.LogEntry{Index: entry.Index, Term: entry.Term, Command: entry.Command})
			lastSent = entry.Index
		}
		n.mu.Unlock()

		rpcCtx, cancel := context.WithTimeout(ctx, 900*time.Millisecond)
		resp, rpcErr := client.AppendEntries(rpcCtx, &raftpb.AppendEntriesRequest{
			Term:         term,
			LeaderId:     n.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      reqEntries,
			LeaderCommit: leaderCommit,
		})
		cancel()
		if rpcErr != nil {
			return false
		}

		n.mu.Lock()
		if resp.GetTerm() > n.currentTerm {
			n.stepDownLocked(resp.GetTerm(), "")
			n.mu.Unlock()
			return false
		}
		if n.role != RoleLeader || n.currentTerm != term {
			n.mu.Unlock()
			return false
		}
		if resp.GetSuccess() {
			match := resp.GetMatchIndex()
			if match < lastSent {
				match = lastSent
			}
			if match > n.matchIndex[peerID] {
				n.matchIndex[peerID] = match
			}
			n.nextIndex[peerID] = n.matchIndex[peerID] + 1
			lastIndex, _, _ := n.store.LastIndexAndTerm()
			if lastIndex >= n.matchIndex[peerID] {
				n.metrics.SetReplicationLag(peerID, lastIndex-n.matchIndex[peerID])
			}
			n.advanceCommitLocked()
			n.refreshMetricsLocked()
			n.mu.Unlock()
			return true
		}
		if n.nextIndex[peerID] > 1 {
			n.nextIndex[peerID]--
		}
		n.mu.Unlock()
	}
	return false
}

func (n *Node) advanceCommitLocked() {
	lastIndex, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		n.logger.Error("failed to read last log while advancing commit", map[string]any{"error": err.Error()})
		return
	}
	majority := n.majorityLocked()
	for idx := n.commitIndex + 1; idx <= lastIndex; idx++ {
		term, ok, err := n.store.Term(idx)
		if err != nil {
			n.logger.Error("failed to read term while advancing commit", map[string]any{"error": err.Error()})
			return
		}
		if !ok || term != n.currentTerm {
			continue
		}
		count := 1
		for _, peer := range n.peers {
			if n.matchIndex[peer.ID] >= idx {
				count++
			}
		}
		if count >= majority {
			n.commitIndex = idx
		}
	}
	n.applyCommittedLocked()
}

func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		next := n.lastApplied + 1
		entry, ok, err := n.store.Entry(next)
		if err != nil {
			n.logger.Error("failed to load committed entry", map[string]any{"index": next, "error": err.Error()})
			return
		}
		if !ok {
			snapIndex, _, err := n.store.SnapshotIndexTerm()
			if err != nil {
				n.logger.Error("failed to read snapshot metadata while applying", map[string]any{"error": err.Error()})
				return
			}
			if next <= snapIndex {
				n.lastApplied = snapIndex
				continue
			}
			n.logger.Error("committed entry missing", map[string]any{"index": next})
			return
		}
		cmd, err := decodeCommand(entry.Command)
		if err != nil {
			n.logger.Error("failed to decode command", map[string]any{"index": entry.Index, "error": err.Error()})
			return
		}
		if cmd.Op == "delete" {
			err = n.store.ApplyDelete(entry.Index, cmd.Key)
		} else {
			err = n.store.ApplySet(entry.Index, cmd.Key, cmd.Value)
		}
		if err != nil {
			n.logger.Error("failed to apply command", map[string]any{"index": entry.Index, "error": err.Error()})
			return
		}
		n.lastApplied = entry.Index
	}
	n.compactIfNeededLocked()
	n.refreshMetricsLocked()
}

func (n *Node) compactIfNeededLocked() {
	if n.snapshotThreshold == 0 || n.lastApplied == 0 {
		return
	}
	snapIndex, _, err := n.store.SnapshotIndexTerm()
	if err != nil {
		n.logger.Error("failed to read snapshot metadata for compaction", map[string]any{"error": err.Error()})
		return
	}
	if n.lastApplied <= snapIndex || n.lastApplied-snapIndex < n.snapshotThreshold {
		return
	}
	term, ok, err := n.store.Term(n.lastApplied)
	if err != nil {
		n.logger.Error("failed to read snapshot term", map[string]any{"error": err.Error()})
		return
	}
	if !ok {
		return
	}
	if _, err := n.store.CreateSnapshot(n.lastApplied, term); err != nil {
		n.logger.Error("failed to create snapshot", map[string]any{"error": err.Error()})
		return
	}
	n.logger.Info("snapshot created", map[string]any{"index": n.lastApplied, "term": term})
}

func (n *Node) WaitForLeader(ctx context.Context) (string, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		n.mu.Lock()
		leaderID := n.leaderID
		if n.role == RoleLeader {
			leaderID = n.id
		}
		n.mu.Unlock()
		if leaderID != "" {
			return leaderID, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

func (n *Node) ID() string {
	return n.id
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func wrapInternal(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("internal raft error: %w", err)
}

// dialTarget keeps plain host:port addresses working with the gRPC resolver,
// which otherwise treats "node1:9001" as a custom scheme.
func dialTarget(address string) string {
	if strings.Contains(address, "://") {
		return address
	}
	return "passthrough:///" + address
}
