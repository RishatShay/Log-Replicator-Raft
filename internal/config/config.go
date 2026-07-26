// Package config reads the node configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Peer is another node of the cluster.
type Peer struct {
	ID      string
	Address string
}

// Config is everything a node needs to start.
type Config struct {
	NodeID            string
	GRPCAddr          string
	HTTPAddr          string
	DataDir           string
	Peers             []Peer
	ElectionMin       time.Duration
	ElectionMax       time.Duration
	HeartbeatInterval time.Duration
	SnapshotThreshold uint64
	LogLevel          slog.Level
}

// FromEnv builds a validated config. Every variable has a default that works for
// a single node started from the repository root.
func FromEnv() (Config, error) {
	cfg := Config{
		NodeID:   env("NODE_ID", "node1"),
		GRPCAddr: env("RAFT_GRPC_ADDR", ":9001"),
		HTTPAddr: env("RAFT_HTTP_ADDR", ":8001"),
		DataDir:  env("RAFT_DATA_DIR", "data/node1"),
	}

	var err error
	if cfg.ElectionMin, err = envDuration("RAFT_ELECTION_MIN_MS", 500*time.Millisecond); err != nil {
		return Config{}, err
	}
	if cfg.ElectionMax, err = envDuration("RAFT_ELECTION_MAX_MS", 900*time.Millisecond); err != nil {
		return Config{}, err
	}
	if cfg.HeartbeatInterval, err = envDuration("RAFT_HEARTBEAT_MS", 100*time.Millisecond); err != nil {
		return Config{}, err
	}
	if cfg.SnapshotThreshold, err = envUint("RAFT_SNAPSHOT_THRESHOLD", 10_000); err != nil {
		return Config{}, err
	}
	if cfg.LogLevel, err = envLogLevel("RAFT_LOG_LEVEL", slog.LevelInfo); err != nil {
		return Config{}, err
	}
	if cfg.Peers, err = parsePeers(env("RAFT_PEERS", ""), cfg.NodeID); err != nil {
		return Config{}, err
	}

	// A follower must be able to miss a few heartbeats before it calls an election.
	if cfg.ElectionMin <= cfg.HeartbeatInterval {
		return Config{}, errors.New("RAFT_ELECTION_MIN_MS must be greater than RAFT_HEARTBEAT_MS")
	}
	if cfg.ElectionMax < cfg.ElectionMin {
		return Config{}, errors.New("RAFT_ELECTION_MAX_MS must be greater than or equal to RAFT_ELECTION_MIN_MS")
	}
	return cfg, nil
}

// parsePeers reads a "node1=host:port,node2=host:port" list. The entry for the
// node itself is allowed and ignored, so every node can share one peer list.
func parsePeers(raw, selfID string) ([]Peer, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}

	items := strings.Split(raw, ",")
	peers := make([]Peer, 0, len(items))
	seen := map[string]struct{}{}
	for _, item := range items {
		id, address, ok := strings.Cut(strings.TrimSpace(item), "=")
		if !ok || id == "" || address == "" {
			return nil, fmt.Errorf("invalid RAFT_PEERS entry %q, want id=host:port", item)
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("duplicate peer id %q in RAFT_PEERS", id)
		}
		seen[id] = struct{}{}
		if id != selfID {
			peers = append(peers, Peer{ID: id, Address: address})
		}
	}
	return peers, nil
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := env(key, "")
	if raw == "" {
		return fallback, nil
	}
	milliseconds, err := strconv.Atoi(raw)
	if err != nil || milliseconds <= 0 {
		return 0, fmt.Errorf("%s must be a positive number of milliseconds, got %q", key, raw)
	}
	return time.Duration(milliseconds) * time.Millisecond, nil
}

func envUint(key string, fallback uint64) (uint64, error) {
	raw := env(key, "")
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a non-negative integer, got %q", key, raw)
	}
	return value, nil
}

func envLogLevel(key string, fallback slog.Level) (slog.Level, error) {
	raw := env(key, "")
	if raw == "" {
		return fallback, nil
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(raw)); err != nil {
		return 0, fmt.Errorf("%s must be one of debug, info, warn, error, got %q", key, raw)
	}
	return level, nil
}
