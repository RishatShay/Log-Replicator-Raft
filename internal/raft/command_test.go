package raft

import (
	"testing"

	"github.com/RishatShay/sna-final-project/internal/storage"
)

func TestCommandRoundTrip(t *testing.T) {
	for _, want := range []command{setCommand("course", "sna"), deleteCommand("course")} {
		payload, err := want.encode()
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeCommand(payload)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("decoded %+v, want %+v", got, want)
		}
	}
}

func TestDecodeCommandRejectsBadPayloads(t *testing.T) {
	payloads := map[string]string{
		"not json":       "hello",
		"unknown op":     `{"op":"increment","key":"a"}`,
		"missing key":    `{"op":"set","value":"1"}`,
		"empty document": `{}`,
	}

	for name, payload := range payloads {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeCommand([]byte(payload)); err == nil {
				t.Fatalf("decoded %q without an error", payload)
			}
		})
	}
}

func TestCommandsApplyToStateMachine(t *testing.T) {
	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := setCommand("course", "sna").applyTo(store, 1); err != nil {
		t.Fatal(err)
	}
	value, found, err := store.Get("course")
	if err != nil || !found || value != "sna" {
		t.Fatalf("course = (%q, %v, %v), want (sna, true, nil)", value, found, err)
	}

	if err := deleteCommand("course").applyTo(store, 2); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Get("course"); err != nil || found {
		t.Fatalf("course still exists after delete: found=%v err=%v", found, err)
	}

	applied, err := store.LastApplied()
	if err != nil {
		t.Fatal(err)
	}
	if applied != 2 {
		t.Fatalf("last applied = %d, want 2", applied)
	}
}
