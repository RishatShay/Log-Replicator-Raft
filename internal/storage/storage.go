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

	// Pure Go SQLite driver, so the binaries stay static and cgo-free.
	_ "modernc.org/sqlite"
)

type Entry struct {
	Index   uint64
	Term    uint64
	Command []byte
}

type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type Store struct {
	db *sql.DB
}

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", dataSourceName(filepath.Join(dir, "raft.db")))
	if err != nil {
		return nil, err
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

// dataSourceName enables the write-ahead log and full durability: an entry has
// to be on disk before a Raft RPC can answer that it is stored.
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
			return err
		}
	}
	return nil
}

func (s *Store) CurrentTermVote() (uint64, string, error) {
	term, err := s.uintMeta("current_term")
	if err != nil {
		return 0, "", err
	}
	votedFor, err := s.stringMeta("voted_for")
	if err != nil {
		return 0, "", err
	}
	return term, votedFor, nil
}

func (s *Store) SaveTermVote(term uint64, votedFor string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer rollback(tx)

	if err := setMetaTx(tx, "current_term", strconv.FormatUint(term, 10)); err != nil {
		return err
	}
	if err := setMetaTx(tx, "voted_for", votedFor); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) LastApplied() (uint64, error) {
	return s.uintMeta("last_applied")
}

func (s *Store) SetLastApplied(index uint64) error {
	_, err := s.db.Exec(`INSERT INTO metadata(key, value) VALUES ('last_applied', ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, strconv.FormatUint(index, 10))
	return err
}

func (s *Store) LastIndexAndTerm() (uint64, uint64, error) {
	var idx sql.NullInt64
	var term sql.NullInt64
	err := s.db.QueryRow(`SELECT idx, term FROM log_entries ORDER BY idx DESC LIMIT 1`).Scan(&idx, &term)
	if errors.Is(err, sql.ErrNoRows) {
		return s.SnapshotIndexTerm()
	}
	if err != nil {
		return 0, 0, err
	}
	if !idx.Valid {
		return s.SnapshotIndexTerm()
	}
	return uint64(idx.Int64), uint64(term.Int64), nil
}

func (s *Store) SnapshotIndexTerm() (uint64, uint64, error) {
	index, err := s.uintMeta("snapshot_index")
	if err != nil {
		return 0, 0, err
	}
	term, err := s.uintMeta("snapshot_term")
	if err != nil {
		return 0, 0, err
	}
	return index, term, nil
}

func (s *Store) LoadSnapshot() (Snapshot, error) {
	var snap Snapshot
	var idx int64
	var term int64
	err := s.db.QueryRow(`SELECT last_included_index, last_included_term, data FROM snapshot WHERE id = 1`).Scan(&idx, &term, &snap.Data)
	if errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	snap.LastIncludedIndex = uint64(idx)
	snap.LastIncludedTerm = uint64(term)
	return snap, nil
}

func (s *Store) Term(index uint64) (uint64, bool, error) {
	if index == 0 {
		return 0, true, nil
	}
	snapIndex, snapTerm, err := s.SnapshotIndexTerm()
	if err != nil {
		return 0, false, err
	}
	if index == snapIndex {
		return snapTerm, true, nil
	}
	if index < snapIndex {
		return 0, false, nil
	}

	var term int64
	err = s.db.QueryRow(`SELECT term FROM log_entries WHERE idx = ?`, int64(index)).Scan(&term)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return uint64(term), true, nil
}

func (s *Store) Entry(index uint64) (Entry, bool, error) {
	var entry Entry
	var idx int64
	var term int64
	err := s.db.QueryRow(`SELECT idx, term, command FROM log_entries WHERE idx = ?`, int64(index)).Scan(&idx, &term, &entry.Command)
	if errors.Is(err, sql.ErrNoRows) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, err
	}
	entry.Index = uint64(idx)
	entry.Term = uint64(term)
	return entry, true, nil
}

func (s *Store) EntriesFrom(start uint64, limit int) ([]Entry, error) {
	snapIndex, _, err := s.SnapshotIndexTerm()
	if err != nil {
		return nil, err
	}
	if start <= snapIndex {
		start = snapIndex + 1
	}
	rows, err := s.db.Query(`SELECT idx, term, command FROM log_entries WHERE idx >= ? ORDER BY idx ASC LIMIT ?`, int64(start), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []Entry
	for rows.Next() {
		var entry Entry
		var idx int64
		var term int64
		if err := rows.Scan(&idx, &term, &entry.Command); err != nil {
			return nil, err
		}
		entry.Index = uint64(idx)
		entry.Term = uint64(term)
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

func (s *Store) AppendEntries(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer rollback(tx)
	for _, entry := range entries {
		if _, err := tx.Exec(`INSERT INTO log_entries(idx, term, command) VALUES (?, ?, ?)
			ON CONFLICT(idx) DO UPDATE SET term = excluded.term, command = excluded.command`,
			int64(entry.Index), int64(entry.Term), entry.Command); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) DeleteFrom(index uint64) error {
	_, err := s.db.Exec(`DELETE FROM log_entries WHERE idx >= ?`, int64(index))
	return err
}

func (s *Store) ApplySet(index uint64, key, value string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err := tx.Exec(`INSERT INTO kv(key, value, log_index) VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value, log_index = excluded.log_index`,
		key, value, int64(index)); err != nil {
		return err
	}
	if err := setMetaTx(tx, "last_applied", strconv.FormatUint(index, 10)); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Get(key string) (string, bool, error) {
	var value string
	err := s.db.QueryRow(`SELECT value FROM kv WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

func (s *Store) All() (map[string]string, error) {
	rows, err := s.db.Query(`SELECT key, value FROM kv ORDER BY key ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	values := map[string]string{}
	for rows.Next() {
		var key string
		var value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		values[key] = value
	}
	return values, rows.Err()
}

func (s *Store) CreateSnapshot(lastIncludedIndex, lastIncludedTerm uint64) (Snapshot, error) {
	values, err := s.All()
	if err != nil {
		return Snapshot{}, err
	}
	data, err := json.Marshal(values)
	if err != nil {
		return Snapshot{}, err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return Snapshot{}, err
	}
	defer rollback(tx)

	if _, err := tx.Exec(`INSERT INTO snapshot(id, last_included_index, last_included_term, data)
		VALUES (1, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			last_included_index = excluded.last_included_index,
			last_included_term = excluded.last_included_term,
			data = excluded.data`,
		int64(lastIncludedIndex), int64(lastIncludedTerm), data); err != nil {
		return Snapshot{}, err
	}
	if _, err := tx.Exec(`DELETE FROM log_entries WHERE idx <= ?`, int64(lastIncludedIndex)); err != nil {
		return Snapshot{}, err
	}
	if err := setMetaTx(tx, "snapshot_index", strconv.FormatUint(lastIncludedIndex, 10)); err != nil {
		return Snapshot{}, err
	}
	if err := setMetaTx(tx, "snapshot_term", strconv.FormatUint(lastIncludedTerm, 10)); err != nil {
		return Snapshot{}, err
	}
	if err := setMetaTx(tx, "last_applied", strconv.FormatUint(lastIncludedIndex, 10)); err != nil {
		return Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, err
	}

	return Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}, nil
}

func (s *Store) InstallSnapshot(snapshot Snapshot) error {
	values := map[string]string{}
	if len(snapshot.Data) > 0 {
		if err := json.Unmarshal(snapshot.Data, &values); err != nil {
			return err
		}
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer rollback(tx)

	if _, err := tx.Exec(`DELETE FROM kv`); err != nil {
		return err
	}
	for key, value := range values {
		if _, err := tx.Exec(`INSERT INTO kv(key, value, log_index) VALUES (?, ?, ?)`, key, value, int64(snapshot.LastIncludedIndex)); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`DELETE FROM log_entries WHERE idx <= ?`, int64(snapshot.LastIncludedIndex)); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO snapshot(id, last_included_index, last_included_term, data)
		VALUES (1, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			last_included_index = excluded.last_included_index,
			last_included_term = excluded.last_included_term,
			data = excluded.data`,
		int64(snapshot.LastIncludedIndex), int64(snapshot.LastIncludedTerm), snapshot.Data); err != nil {
		return err
	}
	if err := setMetaTx(tx, "snapshot_index", strconv.FormatUint(snapshot.LastIncludedIndex, 10)); err != nil {
		return err
	}
	if err := setMetaTx(tx, "snapshot_term", strconv.FormatUint(snapshot.LastIncludedTerm, 10)); err != nil {
		return err
	}
	if err := setMetaTx(tx, "last_applied", strconv.FormatUint(snapshot.LastIncludedIndex, 10)); err != nil {
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
		return 0, fmt.Errorf("metadata %s has invalid uint value %q: %w", key, value, err)
	}
	return parsed, nil
}

func (s *Store) stringMeta(key string) (string, error) {
	var value string
	err := s.db.QueryRow(`SELECT value FROM metadata WHERE key = ?`, key).Scan(&value)
	if err != nil {
		return "", err
	}
	return value, nil
}

func setMetaTx(tx *sql.Tx, key, value string) error {
	_, err := tx.Exec(`INSERT INTO metadata(key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

func rollback(tx *sql.Tx) {
	_ = tx.Rollback()
}
