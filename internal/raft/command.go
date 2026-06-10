package raft

import (
	"encoding/json"
	"fmt"

	"github.com/RishatShay/sna-final-project/internal/storage"
)

const (
	opSet    = "set"
	opDelete = "delete"
)

// command is the JSON payload of a log entry. Keeping it JSON makes the log
// readable with `raftctl log`, which is handy when comparing nodes by hand.
type command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func setCommand(key, value string) command {
	return command{Op: opSet, Key: key, Value: value}
}

func deleteCommand(key string) command {
	return command{Op: opDelete, Key: key}
}

func (c command) encode() ([]byte, error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("encode command: %w", err)
	}
	return raw, nil
}

func decodeCommand(raw []byte) (command, error) {
	var cmd command
	if err := json.Unmarshal(raw, &cmd); err != nil {
		return command{}, fmt.Errorf("decode command: %w", err)
	}
	if cmd.Op != opSet && cmd.Op != opDelete {
		return command{}, fmt.Errorf("unsupported command %q", cmd.Op)
	}
	if cmd.Key == "" {
		return command{}, fmt.Errorf("command %q has an empty key", cmd.Op)
	}
	return cmd, nil
}

// applyTo runs the command against the state machine and moves last_applied to
// index in the same transaction.
func (c command) applyTo(store *storage.Store, index uint64) error {
	if c.Op == opDelete {
		return store.ApplyDelete(index, c.Key)
	}
	return store.ApplySet(index, c.Key, c.Value)
}
