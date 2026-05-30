// Package storage keeps the durable part of a Raft node: persistent metadata,
// the replicated log, the key/value state machine and the latest snapshot.
package storage

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"

	// Pure Go SQLite driver, registered as "sqlite". No cgo, so the binaries stay static.
	_ "modernc.org/sqlite"
)

// Entry is a single record of the replicated log.
type Entry struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// Snapshot is a compacted state machine plus the log position it covers.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// Store is a SQLite backed Raft store. Every method is safe for concurrent use
// because the underlying pool is limited to a single connection.
type Store struct {
	db *sql.DB
}

// Open creates dir if needed and opens raft.db inside it.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	db, err := sql.Open("sqlite", dataSourceName(filepath.Join(dir, "raft.db")))
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)

	store := &Store{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

// dataSourceName enables the write-ahead log and full durability: Raft may only
// answer an RPC after the entry is safely on disk.
func dataSourceName(path string) string {
	pragmas := url.Values{}
	pragmas.Add("_pragma", "busy_timeout(5000)")
	pragmas.Add("_pragma", "journal_mode(WAL)")
	pragmas.Add("_pragma", "synchronous(FULL)")
	return "file:" + path + "?" + pragmas.Encode()
}

func (s *Store) init() error {
	schema := []string{
		`CREATE TABLE IF NOT EXISTS metadata (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS log_entries (
			idx INTEGER PRIMARY KEY,
			term INTEGER NOT NULL,
			command BLOB NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS kv (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			log_index INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS snapshot (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			last_included_index INTEGER NOT NULL,
			last_included_term INTEGER NOT NULL,
			data BLOB NOT NULL
		)`,
	}
	for _, statement := range schema {
		if _, err := s.db.Exec(statement); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
	}

	defaults := map[string]string{
		"current_term":   "0",
		"voted_for":      "",
		"last_applied":   "0",
		"snapshot_index": "0",
		"snapshot_term":  "0",
	}
	for key, value := range defaults {
		if _, err := s.db.Exec(`INSERT OR IGNORE INTO metadata(key, value) VALUES (?, ?)`, key, value); err != nil {
			return fmt.Errorf("seed metadata %s: %w", key, err)
		}
	}
	return nil
}

// CurrentTermVote returns the persisted term and the candidate voted for in it.
func (s *Store) CurrentTermVote() (term uint64, votedFor string, err error) {
	if term, err = s.uintMeta("current_term"); err != nil {
		return 0, "", err
	}
	if votedFor, err = s.stringMeta("voted_for"); err != nil {
		return 0, "", err
	}
	return term, votedFor, nil
}

// SaveTermVote persists term and vote atomically.
func (s *Store) SaveTermVote(term uint64, votedFor string) error {
	return s.inTx(func(tx *sql.Tx) error {
		if err := setMeta(tx, "current_term", strconv.FormatUint(term, 10)); err != nil {
			return err
		}
		return setMeta(tx, "voted_for", votedFor)
	})
}

func (s *Store) LastApplied() (uint64, error) {
	return s.uintMeta("last_applied")
}

// LastIndexAndTerm returns the position of the last log entry, falling back to
// the snapshot position when the log is empty.
func (s *Store) LastIndexAndTerm() (uint64, uint64, error) {
	var index, term sql.NullInt64
	err := s.db.QueryRow(`SELECT idx, term FROM log_entries ORDER BY idx DESC LIMIT 1`).Scan(&index, &term)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !index.Valid) {
		return s.SnapshotIndexTerm()
	}
	if err != nil {
		return 0, 0, fmt.Errorf("read last log entry: %w", err)
	}
	return uint64(index.Int64), uint64(term.Int64), nil
}

func (s *Store) SnapshotIndexTerm() (index uint64, term uint64, err error) {
	if index, err = s.uintMeta("snapshot_index"); err != nil {
		return 0, 0, err
	}
	if term, err = s.uintMeta("snapshot_term"); err != nil {
		return 0, 0, err
	}
	return index, term, nil
}

func (s *Store) LoadSnapshot() (Snapshot, error) {
	var snapshot Snapshot
	var index, term int64
	err := s.db.QueryRow(`SELECT last_included_index, last_included_term, data FROM snapshot WHERE id = 1`).
		Scan(&index, &term, &snapshot.Data)
	if errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, nil
	}
	if err != nil {
		return Snapshot{}, fmt.Errorf("load snapshot: %w", err)
	}
	snapshot.LastIncludedIndex = uint64(index)
	snapshot.LastIncludedTerm = uint64(term)
	return snapshot, nil
}

// Term reports the term stored at index. The second result is false when the
// index is unknown, either because it was never written or already compacted.
func (s *Store) Term(index uint64) (uint64, bool, error) {
	if index == 0 {
		return 0, true, nil
	}
	snapshotIndex, snapshotTerm, err := s.SnapshotIndexTerm()
	if err != nil {
		return 0, false, err
	}
	switch {
	case index == snapshotIndex:
		return snapshotTerm, true, nil
	case index < snapshotIndex:
		return 0, false, nil
	}

	var term int64
	err = s.db.QueryRow(`SELECT term FROM log_entries WHERE idx = ?`, int64(index)).Scan(&term)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read term at %d: %w", index, err)
	}
	return uint64(term), true, nil
}

func (s *Store) Entry(index uint64) (Entry, bool, error) {
	entry := Entry{Index: index}
	var term int64
	err := s.db.QueryRow(`SELECT term, command FROM log_entries WHERE idx = ?`, int64(index)).Scan(&term, &entry.Command)
	if errors.Is(err, sql.ErrNoRows) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, fmt.Errorf("read entry %d: %w", index, err)
	}
	entry.Term = uint64(term)
	return entry, true, nil
}

// EntriesFrom returns at most limit entries starting at start. A limit of zero
// or less means "everything that is left".
func (s *Store) EntriesFrom(start uint64, limit int) ([]Entry, error) {
	snapshotIndex, _, err := s.SnapshotIndexTerm()
	if err != nil {
		return nil, err
	}
	if start <= snapshotIndex {
		start = snapshotIndex + 1
	}
	if limit <= 0 {
		limit = -1
	}

	rows, err := s.db.Query(`SELECT idx, term, command FROM log_entries WHERE idx >= ? ORDER BY idx LIMIT ?`, int64(start), limit)
	if err != nil {
		return nil, fmt.Errorf("read entries from %d: %w", start, err)
	}
	defer rows.Close()

	var entries []Entry
	for rows.Next() {
		var entry Entry
		var index, term int64
		if err := rows.Scan(&index, &term, &entry.Command); err != nil {
			return nil, fmt.Errorf("scan entry: %w", err)
		}
		entry.Index = uint64(index)
		entry.Term = uint64(term)
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

// AppendEntries writes entries, overwriting any entry that already occupies the
// same index.
func (s *Store) AppendEntries(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	return s.inTx(func(tx *sql.Tx) error {
		for _, entry := range entries {
			_, err := tx.Exec(`INSERT INTO log_entries(idx, term, command) VALUES (?, ?, ?)
				ON CONFLICT(idx) DO UPDATE SET term = excluded.term, command = excluded.command`,
				int64(entry.Index), int64(entry.Term), entry.Command)
			if err != nil {
				return fmt.Errorf("append entry %d: %w", entry.Index, err)
			}
		}
		return nil
	})
}

// DeleteFrom drops index and everything after it, used when a follower has to
// discard a conflicting suffix.
func (s *Store) DeleteFrom(index uint64) error {
	if _, err := s.db.Exec(`DELETE FROM log_entries WHERE idx >= ?`, int64(index)); err != nil {
		return fmt.Errorf("delete entries from %d: %w", index, err)
	}
	return nil
}

// ApplySet stores key=value and moves last_applied to index in one transaction.
func (s *Store) ApplySet(index uint64, key, value string) error {
	return s.inTx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO kv(key, value, log_index) VALUES (?, ?, ?)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value, log_index = excluded.log_index`,
			key, value, int64(index))
		if err != nil {
			return fmt.Errorf("apply set %q: %w", key, err)
		}
		return setMeta(tx, "last_applied", strconv.FormatUint(index, 10))
	})
}

// ApplyDelete removes key and moves last_applied to index in one transaction.
func (s *Store) ApplyDelete(index uint64, key string) error {
	return s.inTx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DELETE FROM kv WHERE key = ?`, key); err != nil {
			return fmt.Errorf("apply delete %q: %w", key, err)
		}
		return setMeta(tx, "last_applied", strconv.FormatUint(index, 10))
	})
}

// SkipTo moves last_applied without touching the state machine. It is used when
// applied entries were already compacted into a snapshot.
func (s *Store) SkipTo(index uint64) error {
	return s.inTx(func(tx *sql.Tx) error {
		return setMeta(tx, "last_applied", strconv.FormatUint(index, 10))
	})
}

func (s *Store) Get(key string) (string, bool, error) {
	var value string
	err := s.db.QueryRow(`SELECT value FROM kv WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read key %q: %w", key, err)
	}
	return value, true, nil
}

// All returns the whole state machine.
func (s *Store) All() (map[string]string, error) {
	rows, err := s.db.Query(`SELECT key, value FROM kv ORDER BY key`)
	if err != nil {
		return nil, fmt.Errorf("read state machine: %w", err)
	}
	defer rows.Close()

	state := map[string]string{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("scan key/value: %w", err)
		}
		state[key] = value
	}
	return state, rows.Err()
}

// CreateSnapshot serialises the state machine, records it as the snapshot at the
// given position and compacts every log entry it covers.
func (s *Store) CreateSnapshot(lastIncludedIndex, lastIncludedTerm uint64) (Snapshot, error) {
	state, err := s.All()
	if err != nil {
		return Snapshot{}, err
	}
	data, err := json.Marshal(state)
	if err != nil {
		return Snapshot{}, fmt.Errorf("encode snapshot: %w", err)
	}

	snapshot := Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}
	err = s.inTx(func(tx *sql.Tx) error {
		if err := saveSnapshot(tx, snapshot); err != nil {
			return err
		}
		return compactLog(tx, lastIncludedIndex)
	})
	if err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

// InstallSnapshot replaces the local state machine with the leader snapshot.
func (s *Store) InstallSnapshot(snapshot Snapshot) error {
	state := map[string]string{}
	if len(snapshot.Data) > 0 {
		if err := json.Unmarshal(snapshot.Data, &state); err != nil {
			return fmt.Errorf("decode snapshot: %w", err)
		}
	}

	return s.inTx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DELETE FROM kv`); err != nil {
			return fmt.Errorf("clear state machine: %w", err)
		}
		for key, value := range state {
			_, err := tx.Exec(`INSERT INTO kv(key, value, log_index) VALUES (?, ?, ?)`,
				key, value, int64(snapshot.LastIncludedIndex))
			if err != nil {
				return fmt.Errorf("restore key %q: %w", key, err)
			}
		}
		if err := saveSnapshot(tx, snapshot); err != nil {
			return err
		}
		return compactLog(tx, snapshot.LastIncludedIndex)
	})
}

func saveSnapshot(tx *sql.Tx, snapshot Snapshot) error {
	_, err := tx.Exec(`INSERT INTO snapshot(id, last_included_index, last_included_term, data) VALUES (1, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			last_included_index = excluded.last_included_index,
			last_included_term = excluded.last_included_term,
			data = excluded.data`,
		int64(snapshot.LastIncludedIndex), int64(snapshot.LastIncludedTerm), snapshot.Data)
	if err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	if err := setMeta(tx, "snapshot_index", strconv.FormatUint(snapshot.LastIncludedIndex, 10)); err != nil {
		return err
	}
	if err := setMeta(tx, "snapshot_term", strconv.FormatUint(snapshot.LastIncludedTerm, 10)); err != nil {
		return err
	}
	return setMeta(tx, "last_applied", strconv.FormatUint(snapshot.LastIncludedIndex, 10))
}

func compactLog(tx *sql.Tx, throughIndex uint64) error {
	if _, err := tx.Exec(`DELETE FROM log_entries WHERE idx <= ?`, int64(throughIndex)); err != nil {
		return fmt.Errorf("compact log through %d: %w", throughIndex, err)
	}
	return nil
}

func (s *Store) inTx(fn func(tx *sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *Store) uintMeta(key string) (uint64, error) {
	value, err := s.stringMeta(key)
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("metadata %s has invalid value %q: %w", key, value, err)
	}
	return parsed, nil
}

func (s *Store) stringMeta(key string) (string, error) {
	var value string
	if err := s.db.QueryRow(`SELECT value FROM metadata WHERE key = ?`, key).Scan(&value); err != nil {
		return "", fmt.Errorf("read metadata %s: %w", key, err)
	}
	return value, nil
}

func setMeta(tx *sql.Tx, key, value string) error {
	_, err := tx.Exec(`INSERT INTO metadata(key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	if err != nil {
		return fmt.Errorf("write metadata %s: %w", key, err)
	}
	return nil
}
