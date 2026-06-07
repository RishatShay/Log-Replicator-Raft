package raft

import (
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/RishatShay/sna-final-project/internal/config"
)

type Role string

const (
	RoleFollower  Role = "follower"
	RoleCandidate Role = "candidate"
	RoleLeader    Role = "leader"
)

type Peer struct {
	ID      string
	Address string
}

type Options struct {
	NodeID            string
	GRPCAddr          string
	HTTPAddr          string
	DataDir           string
	Peers             []Peer
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

type command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func encodeSetCommand(key, value string) ([]byte, error) {
	return json.Marshal(command{Op: "set", Key: key, Value: value})
}

func encodeDeleteCommand(key string) ([]byte, error) {
	return json.Marshal(command{Op: "delete", Key: key})
}

func decodeCommand(raw []byte) (command, error) {
	var cmd command
	if err := json.Unmarshal(raw, &cmd); err != nil {
		return command{}, err
	}
	if cmd.Op != "set" && cmd.Op != "delete" {
		return command{}, errors.New("unsupported command")
	}
	return cmd, nil
}
