// Package raft implements a Raft node: leader election, log replication,
// snapshot installation and a replicated key/value state machine on top of it.
package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/RishatShay/sna-final-project/internal/metrics"
	"github.com/RishatShay/sna-final-project/internal/raftpb"
	"github.com/RishatShay/sna-final-project/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultMinElectionTimeout = 500 * time.Millisecond
	defaultMaxElectionTimeout = 900 * time.Millisecond
	defaultHeartbeatInterval  = 100 * time.Millisecond

	// electionTick is how often a follower checks its election deadline.
	electionTick = 25 * time.Millisecond
	// rpcTimeout bounds a single consensus RPC.
	rpcTimeout = 500 * time.Millisecond
	// appendBatchSize limits how many entries travel in one AppendEntries call.
	appendBatchSize = 128
)

// Node is a single member of the cluster. It serves both gRPC services: the
// consensus API used by other nodes and the key/value API used by clients.
type Node struct {
	raftpb.UnimplementedRaftServiceServer
	raftpb.UnimplementedClientServiceServer

	id       string
	grpcAddr string
	httpAddr string
	peers    []Peer

	store   *storage.Store
	log     *slog.Logger
	metrics *metrics.Metrics

	electionMin       time.Duration
	electionMax       time.Duration
	heartbeat         time.Duration
	snapshotThreshold uint64

	// mu guards the Raft state below and every store access.
	mu               sync.Mutex
	role             Role
	currentTerm      uint64
	votedFor         string
	leaderID         string
	commitIndex      uint64
	lastApplied      uint64
	nextIndex        map[string]uint64
	matchIndex       map[string]uint64
	electionDeadline time.Time

	conns       map[string]*grpc.ClientConn
	raftClients map[string]raftpb.RaftServiceClient
	kvClients   map[string]raftpb.ClientServiceClient
	// replicateNow wakes up the replication loop of a peer ahead of its heartbeat.
	replicateNow map[string]chan struct{}
	// sendMu serialises replication to a peer so rounds cannot interleave.
	sendMu map[string]*sync.Mutex

	grpcServer *grpc.Server
	httpServer *http.Server

	ctx      context.Context
	cancel   context.CancelFunc
	workers  sync.WaitGroup
	stopOnce sync.Once
}

// New opens the data directory and restores the persistent Raft state.
func New(opts Options) (*Node, error) {
	if opts.NodeID == "" {
		return nil, errors.New("node id is required")
	}
	if opts.GRPCAddr == "" {
		return nil, errors.New("grpc address is required")
	}
	if opts.DataDir == "" {
		return nil, errors.New("data directory is required")
	}
	if opts.ElectionMin == 0 {
		opts.ElectionMin = defaultMinElectionTimeout
	}
	if opts.ElectionMax == 0 {
		opts.ElectionMax = defaultMaxElectionTimeout
	}
	if opts.HeartbeatInterval == 0 {
		opts.HeartbeatInterval = defaultHeartbeatInterval
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
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

	ctx, cancel := context.WithCancel(context.Background())
	node := &Node{
		id:                opts.NodeID,
		grpcAddr:          opts.GRPCAddr,
		httpAddr:          opts.HTTPAddr,
		peers:             opts.Peers,
		store:             store,
		log:               opts.Logger.With("node_id", opts.NodeID),
		metrics:           metrics.New(opts.NodeID),
		electionMin:       opts.ElectionMin,
		electionMax:       opts.ElectionMax,
		heartbeat:         opts.HeartbeatInterval,
		snapshotThreshold: opts.SnapshotThreshold,
		role:              RoleFollower,
		currentTerm:       term,
		votedFor:          votedFor,
		commitIndex:       lastApplied,
		lastApplied:       lastApplied,
		nextIndex:         map[string]uint64{},
		matchIndex:        map[string]uint64{},
		conns:             map[string]*grpc.ClientConn{},
		raftClients:       map[string]raftpb.RaftServiceClient{},
		kvClients:         map[string]raftpb.ClientServiceClient{},
		replicateNow:      map[string]chan struct{}{},
		sendMu:            map[string]*sync.Mutex{},
		ctx:               ctx,
		cancel:            cancel,
	}
	for _, peer := range opts.Peers {
		node.replicateNow[peer.ID] = make(chan struct{}, 1)
		node.sendMu[peer.ID] = &sync.Mutex{}
	}

	node.mu.Lock()
	defer node.mu.Unlock()
	node.resetElectionDeadlineLocked()
	node.publishStateLocked()
	return node, nil
}

// Start dials the peers, serves both gRPC services and starts the background
// election and replication loops.
func (n *Node) Start() error {
	if err := n.dialPeers(); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", n.grpcAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", n.grpcAddr, err)
	}
	n.grpcServer = grpc.NewServer()
	raftpb.RegisterRaftServiceServer(n.grpcServer, n)
	raftpb.RegisterClientServiceServer(n.grpcServer, n)
	n.log.Info("grpc server started", "addr", n.grpcAddr, "peers", len(n.peers))
	go func() {
		if err := n.grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			n.log.Error("grpc server stopped", "error", err)
		}
	}()

	if n.httpAddr != "" {
		n.startHTTPServer()
	}

	n.startWorker(n.runElectionTimer)
	for _, peer := range n.peers {
		peer := peer
		n.startWorker(func() { n.runReplication(peer) })
	}
	return nil
}

// Stop shuts the servers down and closes the store. It is safe to call twice.
func (n *Node) Stop(ctx context.Context) error {
	var err error
	n.stopOnce.Do(func() {
		n.cancel()
		if n.grpcServer != nil {
			err = stopGRPCServer(ctx, n.grpcServer)
		}
		if n.httpServer != nil {
			err = errors.Join(err, n.httpServer.Shutdown(ctx))
		}
		n.workers.Wait()
		for _, conn := range n.conns {
			err = errors.Join(err, conn.Close())
		}
		err = errors.Join(err, n.store.Close())
	})
	return err
}

// ID returns the node id.
func (n *Node) ID() string {
	return n.id
}

// Leader returns the leader this node knows about and whether that is the node
// itself. The id is empty while no leader is known.
func (n *Node) Leader() (id string, isSelf bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == RoleLeader {
		return n.id, true
	}
	return n.leaderID, false
}

func (n *Node) dialPeers() error {
	for _, peer := range n.peers {
		conn, err := grpc.NewClient(dialTarget(peer.Address), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("create client for %s: %w", peer.ID, err)
		}
		n.conns[peer.ID] = conn
		n.raftClients[peer.ID] = raftpb.NewRaftServiceClient(conn)
		n.kvClients[peer.ID] = raftpb.NewClientServiceClient(conn)
	}
	return nil
}

func (n *Node) startHTTPServer() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", n.metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})

	n.httpServer = &http.Server{Addr: n.httpAddr, Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	n.log.Info("http server started", "addr", n.httpAddr)
	go func() {
		if err := n.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			n.log.Error("http server stopped", "error", err)
		}
	}()
}

func (n *Node) startWorker(fn func()) {
	n.workers.Add(1)
	go func() {
		defer n.workers.Done()
		fn()
	}()
}

// runElectionTimer starts an election whenever no leader has been heard from for
// longer than the randomized election timeout.
func (n *Node) runElectionTimer() {
	ticker := time.NewTicker(electionTick)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			if n.electionDue() {
				n.startElection()
			}
		}
	}
}

func (n *Node) electionDue() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role != RoleLeader && time.Now().After(n.electionDeadline)
}

func (n *Node) isLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == RoleLeader
}

// stepDownLocked turns the node into a follower of the given term.
func (n *Node) stepDownLocked(term uint64, leaderID string) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		if err := n.store.SaveTermVote(n.currentTerm, n.votedFor); err != nil {
			n.log.Error("persist term", "error", err)
		}
	}
	if n.role != RoleFollower {
		n.log.Info("stepping down", "term", n.currentTerm, "leader_id", leaderID)
	}
	n.role = RoleFollower
	n.leaderID = leaderID
	n.resetElectionDeadlineLocked()
	n.publishStateLocked()
}

// resetElectionDeadlineLocked picks the next election deadline. The random part
// keeps nodes from starting elections at the same moment.
func (n *Node) resetElectionDeadlineLocked() {
	timeout := n.electionMin
	if window := n.electionMax - n.electionMin; window > 0 {
		timeout += time.Duration(rand.Int63n(int64(window)))
	}
	n.electionDeadline = time.Now().Add(timeout)
}

// majorityLocked is the number of nodes, including this one, that have to agree.
func (n *Node) majorityLocked() int {
	return (len(n.peers)+1)/2 + 1
}

// publishStateLocked mirrors the Raft state into the Prometheus metrics.
func (n *Node) publishStateLocked() {
	lastLogIndex, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		n.log.Error("read last log position", "error", err)
		return
	}
	snapshotIndex, _, err := n.store.SnapshotIndexTerm()
	if err != nil {
		n.log.Error("read snapshot position", "error", err)
		return
	}
	n.metrics.SetState(n.role == RoleLeader, n.currentTerm, n.commitIndex, n.lastApplied, lastLogIndex, snapshotIndex)
}

func stopGRPCServer(ctx context.Context, server *grpc.Server) error {
	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		server.Stop()
		return ctx.Err()
	}
}

// dialTarget keeps plain host:port addresses working with the gRPC name
// resolver, which otherwise treats "node1:9001" as a custom scheme.
func dialTarget(address string) string {
	if strings.Contains(address, "://") {
		return address
	}
	return "passthrough:///" + address
}
