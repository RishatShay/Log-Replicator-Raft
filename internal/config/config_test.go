package config

import (
	"log/slog"
	"testing"
	"time"
)

func TestFromEnvUsesDefaults(t *testing.T) {
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}

	if cfg.NodeID != "node1" || cfg.GRPCAddr != ":9001" {
		t.Fatalf("defaults = (%q, %q), want (node1, :9001)", cfg.NodeID, cfg.GRPCAddr)
	}
	if cfg.ElectionMin != 500*time.Millisecond || cfg.HeartbeatInterval != 100*time.Millisecond {
		t.Fatalf("timers = (%s, %s), want (500ms, 100ms)", cfg.ElectionMin, cfg.HeartbeatInterval)
	}
	if len(cfg.Peers) != 0 {
		t.Fatalf("peers = %v, want none", cfg.Peers)
	}
}

func TestFromEnvReadsPeersAndSkipsSelf(t *testing.T) {
	t.Setenv("NODE_ID", "node2")
	t.Setenv("RAFT_PEERS", "node1=node1:9001, node2=node2:9001 ,node3=node3:9001")
	t.Setenv("RAFT_LOG_LEVEL", "debug")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}

	if len(cfg.Peers) != 2 {
		t.Fatalf("peers = %v, want the two other nodes", cfg.Peers)
	}
	for _, peer := range cfg.Peers {
		if peer.ID == cfg.NodeID {
			t.Fatalf("peers contain the node itself: %v", cfg.Peers)
		}
	}
	if cfg.LogLevel != slog.LevelDebug {
		t.Fatalf("log level = %s, want debug", cfg.LogLevel)
	}
}

func TestFromEnvRejectsInvalidValues(t *testing.T) {
	tests := map[string]map[string]string{
		"malformed peer":              {"RAFT_PEERS": "node1"},
		"duplicate peer":              {"RAFT_PEERS": "node2=node2:9001,node2=node2:9002"},
		"election timeout too short":  {"RAFT_ELECTION_MIN_MS": "50", "RAFT_HEARTBEAT_MS": "100"},
		"election window inverted":    {"RAFT_ELECTION_MIN_MS": "900", "RAFT_ELECTION_MAX_MS": "500"},
		"non numeric heartbeat":       {"RAFT_HEARTBEAT_MS": "fast"},
		"unknown log level":           {"RAFT_LOG_LEVEL": "chatty"},
		"negative snapshot threshold": {"RAFT_SNAPSHOT_THRESHOLD": "-1"},
	}

	for name, env := range tests {
		t.Run(name, func(t *testing.T) {
			for key, value := range env {
				t.Setenv(key, value)
			}
			if _, err := FromEnv(); err == nil {
				t.Fatal("expected an error, got none")
			}
		})
	}
}
