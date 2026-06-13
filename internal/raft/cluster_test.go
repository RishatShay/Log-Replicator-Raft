package raft

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/RishatShay/sna-final-project/internal/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestClusterElectsExactlyOneLeader(t *testing.T) {
	cluster := startCluster(t, 3, 0)
	leader := cluster.waitForLeader(t)

	if _, isLeader := cluster.nodes[leader].Leader(); !isLeader {
		t.Fatalf("node %s does not consider itself the leader", leader)
	}
}

func TestWriteReachesEveryNode(t *testing.T) {
	cluster := startCluster(t, 3, 0)
	leader := cluster.waitForLeader(t)

	resp := cluster.write(t, leader, "course", "sna")
	if resp.GetIndex() == 0 {
		t.Fatal("committed entry has index 0")
	}
	cluster.waitForApplied(t, resp.GetIndex())

	for id := range cluster.nodes {
		report := cluster.inspect(t, id)
		if got := report.GetState()["course"]; got != "sna" {
			t.Fatalf("node %s has course=%q, want sna", id, got)
		}
	}
}

func TestFollowerForwardsClientCallsToLeader(t *testing.T) {
	cluster := startCluster(t, 3, 0)
	leader := cluster.waitForLeader(t)
	follower := cluster.anyNodeExcept(t, leader)

	if resp := cluster.write(t, follower, "forwarded", "yes"); resp.GetLeaderId() != leader {
		t.Fatalf("write was served by %q, want the leader %q", resp.GetLeaderId(), leader)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cluster.client(t, follower).Read(ctx, &raftpb.ReadRequest{Key: "forwarded"})
	if err != nil {
		t.Fatalf("read through follower: %v", err)
	}
	if resp.GetValue() != "yes" {
		t.Fatalf("value = %q, want yes", resp.GetValue())
	}
}

func TestDeleteRemovesKeyFromStateMachine(t *testing.T) {
	cluster := startCluster(t, 3, 0)
	leader := cluster.waitForLeader(t)

	cluster.write(t, leader, "temporary", "value")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cluster.client(t, leader).Delete(ctx, &raftpb.DeleteRequest{Key: "temporary"}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err := cluster.client(t, leader).Read(ctx, &raftpb.ReadRequest{Key: "temporary"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("read after delete returned %v, want NotFound", err)
	}
}

func TestClusterElectsNewLeaderAfterLeaderStops(t *testing.T) {
	cluster := startCluster(t, 3, 0)
	oldLeader := cluster.waitForLeader(t)
	cluster.stop(t, oldLeader)

	newLeader := cluster.waitForLeader(t)
	if newLeader == oldLeader {
		t.Fatalf("leader is still %s after it was stopped", oldLeader)
	}
	cluster.write(t, newLeader, "after", "failover")
}

func TestRestartedFollowerCatchesUp(t *testing.T) {
	cluster := startCluster(t, 3, 0)
	leader := cluster.waitForLeader(t)
	follower := cluster.anyNodeExcept(t, leader)

	cluster.stop(t, follower)
	last := cluster.write(t, leader, "written", "while-follower-was-down")
	cluster.start(t, follower)

	cluster.waitForApplied(t, last.GetIndex())
	report := cluster.inspect(t, follower)
	if got := report.GetState()["written"]; got != "while-follower-was-down" {
		t.Fatalf("restarted node has written=%q, want while-follower-was-down", got)
	}
}

func TestLeaderRepairsFollowerWithSnapshot(t *testing.T) {
	// A threshold of two entries makes the leader compact its log while the
	// follower is down, so only InstallSnapshot can bring the follower back.
	cluster := startCluster(t, 3, 2)
	leader := cluster.waitForLeader(t)
	follower := cluster.anyNodeExcept(t, leader)

	cluster.stop(t, follower)
	var last *raftpb.WriteResponse
	for i := range 5 {
		last = cluster.write(t, leader, fmt.Sprintf("key%d", i), "value")
	}
	if report := cluster.inspect(t, leader); report.GetSnapshotIndex() == 0 {
		t.Fatal("leader did not compact its log")
	}
	cluster.start(t, follower)

	cluster.waitForApplied(t, last.GetIndex())
	report := cluster.inspect(t, follower)
	if report.GetSnapshotIndex() == 0 {
		t.Fatal("follower was repaired without a snapshot")
	}
	if got := report.GetState()["key0"]; got != "value" {
		t.Fatalf("follower has key0=%q, want value", got)
	}
}

// cluster is a set of nodes running in one process, connected over loopback.
type cluster struct {
	nodes             map[string]*Node
	addresses         map[string]string
	dirs              map[string]string
	snapshotThreshold uint64
}

// startCluster boots size nodes and stops them when the test ends. A
// snapshotThreshold of zero disables log compaction.
func startCluster(t *testing.T, size int, snapshotThreshold uint64) *cluster {
	t.Helper()

	c := &cluster{
		nodes:             map[string]*Node{},
		addresses:         map[string]string{},
		dirs:              map[string]string{},
		snapshotThreshold: snapshotThreshold,
	}
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("node%d", i)
		c.addresses[id] = freeAddress(t)
		c.dirs[id] = t.TempDir()
	}
	for id := range c.addresses {
		c.start(t, id)
	}

	t.Cleanup(func() {
		for id := range c.nodes {
			c.stop(t, id)
		}
	})
	return c
}

func (c *cluster) start(t *testing.T, id string) {
	t.Helper()

	var peers []Peer
	for peerID, address := range c.addresses {
		if peerID != id {
			peers = append(peers, Peer{ID: peerID, Address: address})
		}
	}

	node, err := New(Options{
		NodeID:            id,
		GRPCAddr:          c.addresses[id],
		DataDir:           c.dirs[id],
		Peers:             peers,
		ElectionMin:       150 * time.Millisecond,
		ElectionMax:       300 * time.Millisecond,
		HeartbeatInterval: 40 * time.Millisecond,
		SnapshotThreshold: c.snapshotThreshold,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
	if err := node.Start(); err != nil {
		t.Fatalf("start %s: %v", id, err)
	}
	c.nodes[id] = node
}

func (c *cluster) stop(t *testing.T, id string) {
	t.Helper()

	node, running := c.nodes[id]
	if !running {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := node.Stop(ctx); err != nil {
		t.Fatalf("stop %s: %v", id, err)
	}
	delete(c.nodes, id)
}

// waitForLeader waits until exactly one running node claims leadership.
func (c *cluster) waitForLeader(t *testing.T) string {
	t.Helper()

	var leader string
	waitFor(t, 10*time.Second, "a single leader", func() bool {
		leaders := map[string]struct{}{}
		for _, node := range c.nodes {
			if id, isLeader := node.Leader(); isLeader {
				leaders[id] = struct{}{}
			}
		}
		if len(leaders) != 1 {
			return false
		}
		for id := range leaders {
			leader = id
		}
		return true
	})
	return leader
}

// waitForApplied waits until every running node applied the given index.
func (c *cluster) waitForApplied(t *testing.T, index uint64) {
	t.Helper()

	waitFor(t, 10*time.Second, fmt.Sprintf("index %d applied everywhere", index), func() bool {
		for id := range c.nodes {
			if c.inspect(t, id).GetLastApplied() < index {
				return false
			}
		}
		return true
	})
}

// write appends a key through the given node, retrying while the cluster has no
// leader yet.
func (c *cluster) write(t *testing.T, id, key, value string) *raftpb.WriteResponse {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := c.client(t, id)
	for {
		resp, err := client.Write(ctx, &raftpb.WriteRequest{Key: key, Value: value})
		if err == nil {
			return resp
		}
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("write %s=%s through %s: %v", key, value, id, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("write %s=%s through %s timed out: %v", key, value, id, err)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (c *cluster) inspect(t *testing.T, id string) *raftpb.InspectLogResponse {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	report, err := c.client(t, id).InspectLog(ctx, &raftpb.InspectLogRequest{})
	if err != nil {
		t.Fatalf("inspect %s: %v", id, err)
	}
	return report
}

func (c *cluster) client(t *testing.T, id string) raftpb.ClientServiceClient {
	t.Helper()

	conn, err := grpc.NewClient(c.addresses[id], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to %s: %v", id, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return raftpb.NewClientServiceClient(conn)
}

func (c *cluster) anyNodeExcept(t *testing.T, id string) string {
	t.Helper()

	for other := range c.nodes {
		if other != id {
			return other
		}
	}
	t.Fatalf("cluster has no node besides %s", id)
	return ""
}

func waitFor(t *testing.T, timeout time.Duration, what string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

func freeAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().String()
}
