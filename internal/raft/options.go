package raft

import (
	"log/slog"
	"time"

	"github.com/RishatShay/sna-final-project/internal/config"
)

// Role is the Raft role of a node.
type Role string

const (
	RoleFollower  Role = "follower"
	RoleCandidate Role = "candidate"
	RoleLeader    Role = "leader"
)

// Peer is another node of the cluster.
type Peer struct {
	ID      string
	Address string
}

// Options configures a node. Only NodeID, GRPCAddr and DataDir are required.
type Options struct {
	NodeID   string
	GRPCAddr string
	HTTPAddr string
	DataDir  string
	Peers    []Peer

	ElectionMin       time.Duration
	ElectionMax       time.Duration
	HeartbeatInterval time.Duration
	SnapshotThreshold uint64
	Logger            *slog.Logger
}

func OptionsFromConfig(cfg config.Config) Options {
	peers := make([]Peer, 0, len(cfg.Peers))
	for _, peer := range cfg.Peers {
		peers = append(peers, Peer{ID: peer.ID, Address: peer.Address})
	}
	return Options{
		NodeID:            cfg.NodeID,
		GRPCAddr:          cfg.GRPCAddr,
		HTTPAddr:          cfg.HTTPAddr,
		DataDir:           cfg.DataDir,
		Peers:             peers,
		ElectionMin:       cfg.ElectionMin,
		ElectionMax:       cfg.ElectionMax,
		HeartbeatInterval: cfg.HeartbeatInterval,
		SnapshotThreshold: cfg.SnapshotThreshold,
	}
}
