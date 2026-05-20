package raft

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/RishatShay/sna-final-project/internal/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestClusterElectsLeaderAndReplicatesWrite(t *testing.T) {
	nodes, addrs := startTestCluster(t, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	leaderID := waitForLeaderID(t, ctx, nodes)
	leaderAddr := addrs[leaderID]

	conn, err := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := wire.NewClientServiceClient(conn)

	writeResp, err := client.Write(ctx, &wire.ClientWriteRequest{Key: "course", Value: "sna"})
	if err != nil {
		t.Fatal(err)
	}
	if !writeResp.GetSuccess() {
		t.Fatalf("write failed: %s leader=%s", writeResp.GetError(), writeResp.GetLeaderId())
	}

	readResp, err := client.Read(ctx, &wire.ClientReadRequest{Key: "course"})
	if err != nil {
		t.Fatal(err)
	}
	if !readResp.GetSuccess() || readResp.GetValue() != "sna" {
		t.Fatalf("read = success:%v value:%q error:%q", readResp.GetSuccess(), readResp.GetValue(), readResp.GetError())
	}

	waitForApplied(t, ctx, nodes, writeResp.GetIndex())
}

func TestFollowerForwardsClientWriteToLeader(t *testing.T) {
	nodes, addrs := startTestCluster(t, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	leaderID := waitForLeaderID(t, ctx, nodes)
	followerID := waitForFollowerWithLeaderHint(t, ctx, nodes, leaderID)

	followerAddr := addrs[followerID]
	conn, err := grpc.NewClient(followerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := wire.NewClientServiceClient(conn)

	resp, err := client.Write(ctx, &wire.ClientWriteRequest{Key: "x", Value: "y"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("follower proxy write failed: %s", resp.GetError())
	}
	if resp.GetLeaderId() != leaderID {
		t.Fatalf("leader hint = %q, want %q", resp.GetLeaderId(), leaderID)
	}
}

func TestClusterFailsOverAfterLeaderStop(t *testing.T) {
	nodes, addrs := startTestCluster(t, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	oldLeaderID := waitForLeaderID(t, ctx, nodes)

	var active []*Node
	for _, node := range nodes {
		if node.ID() == oldLeaderID {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := node.Stop(stopCtx); err != nil {
				t.Fatal(err)
			}
			stopCancel()
			continue
		}
		active = append(active, node)
	}

	newLeaderID := waitForLeaderID(t, ctx, active)
	if newLeaderID == oldLeaderID {
		t.Fatalf("leader did not change after stopping %s", oldLeaderID)
	}

	conn, err := grpc.NewClient(addrs[newLeaderID], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := wire.NewClientServiceClient(conn)
	resp, err := client.Write(ctx, &wire.ClientWriteRequest{Key: "after", Value: "failover"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("write after failover failed: %s", resp.GetError())
	}
}

func waitForFollowerWithLeaderHint(t *testing.T, ctx context.Context, nodes []*Node, leaderID string) string {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, node := range nodes {
			node.mu.Lock()
			id := node.id
			role := node.role
			hint := node.leaderID
			node.mu.Unlock()
			if id != leaderID && role == RoleFollower && hint == leaderID {
				return id
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for follower leader hint")
		case <-ticker.C:
		}
	}
}

func startTestCluster(t *testing.T, size int) ([]*Node, map[string]string) {
	t.Helper()

	addrs := map[string]string{}
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("node%d", i)
		addrs[id] = freeAddress(t)
	}

	nodes := make([]*Node, 0, size)
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("node%d", i)
		var peers []Peer
		for peerID, addr := range addrs {
			if peerID == id {
				continue
			}
			peers = append(peers, Peer{ID: peerID, Address: addr})
		}
		node, err := New(Options{
			NodeID:            id,
			GRPCAddr:          addrs[id],
			DataDir:           t.TempDir(),
			Peers:             peers,
			ElectionMin:       120 * time.Millisecond,
			ElectionMax:       240 * time.Millisecond,
			HeartbeatInterval: 35 * time.Millisecond,
			SnapshotThreshold: 1000,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := node.Start(); err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, node)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for _, node := range nodes {
			_ = node.Stop(ctx)
		}
	})
	return nodes, addrs
}

func waitForLeaderID(t *testing.T, ctx context.Context, nodes []*Node) string {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		leaders := map[string]struct{}{}
		for _, node := range nodes {
			if leaderID, isLeader := node.MustLeaderID(); isLeader {
				leaders[leaderID] = struct{}{}
			}
		}
		if len(leaders) == 1 {
			for leaderID := range leaders {
				return leaderID
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for leader")
		case <-ticker.C:
		}
	}
}

func waitForApplied(t *testing.T, ctx context.Context, nodes []*Node, index uint64) {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		allApplied := true
		for _, node := range nodes {
			node.mu.Lock()
			applied := node.lastApplied
			node.mu.Unlock()
			if applied < index {
				allApplied = false
				break
			}
		}
		if allApplied {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for all nodes to apply index %d", index)
		case <-ticker.C:
		}
	}
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}
