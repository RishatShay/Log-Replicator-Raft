package storage

import "testing"

func TestStoreEntriesFromRespectsLimitAndSnapshot(t *testing.T) {
	store := openStore(t)

	entries := make([]Entry, 0, 5)
	for index := uint64(1); index <= 5; index++ {
		entries = append(entries, Entry{Index: index, Term: 1, Command: []byte("command")})
	}
	if err := store.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}

	batch, err := store.EntriesFrom(2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 || batch[0].Index != 2 || batch[1].Index != 3 {
		t.Fatalf("batch = %v, want entries 2 and 3", batch)
	}

	if _, err := store.CreateSnapshot(3, 1); err != nil {
		t.Fatal(err)
	}
	// Entries below the snapshot are gone, so the read starts after it.
	rest, err := store.EntriesFrom(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 2 || rest[0].Index != 4 {
		t.Fatalf("rest = %v, want entries 4 and 5", rest)
	}
}

func TestStoreAppliesAndSkipsIndexes(t *testing.T) {
	store := openStore(t)

	if err := store.ApplySet(1, "key", "value"); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyDelete(2, "key"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Get("key"); err != nil || found {
		t.Fatalf("key still exists: found=%v err=%v", found, err)
	}

	if err := store.SkipTo(7); err != nil {
		t.Fatal(err)
	}
	applied, err := store.LastApplied()
	if err != nil {
		t.Fatal(err)
	}
	if applied != 7 {
		t.Fatalf("last applied = %d, want 7", applied)
	}
}

func openStore(t *testing.T) *Store {
	t.Helper()

	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestStorePersistsMetadataAndLog(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.SaveTermVote(3, "node2"); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendEntries([]Entry{
		{Index: 1, Term: 3, Command: []byte(`{"op":"set","key":"a","value":"1"}`)},
		{Index: 2, Term: 3, Command: []byte(`{"op":"set","key":"b","value":"2"}`)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	term, votedFor, err := reopened.CurrentTermVote()
	if err != nil {
		t.Fatal(err)
	}
	if term != 3 || votedFor != "node2" {
		t.Fatalf("metadata = (%d, %q), want (3, node2)", term, votedFor)
	}
	lastIndex, lastTerm, err := reopened.LastIndexAndTerm()
	if err != nil {
		t.Fatal(err)
	}
	if lastIndex != 2 || lastTerm != 3 {
		t.Fatalf("last log = (%d, %d), want (2, 3)", lastIndex, lastTerm)
	}
}

func TestStoreDeletesConflictingSuffix(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.AppendEntries([]Entry{
		{Index: 1, Term: 1, Command: []byte("a")},
		{Index: 2, Term: 1, Command: []byte("b")},
		{Index: 3, Term: 2, Command: []byte("c")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteFrom(2); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendEntries([]Entry{{Index: 2, Term: 3, Command: []byte("replacement")}}); err != nil {
		t.Fatal(err)
	}

	lastIndex, lastTerm, err := store.LastIndexAndTerm()
	if err != nil {
		t.Fatal(err)
	}
	if lastIndex != 2 || lastTerm != 3 {
		t.Fatalf("last log = (%d, %d), want (2, 3)", lastIndex, lastTerm)
	}
	if _, ok, err := store.Entry(3); err != nil || ok {
		t.Fatalf("entry 3 exists=%v err=%v, want missing", ok, err)
	}
}

func TestStoreSnapshotRoundTrip(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.AppendEntries([]Entry{
		{Index: 1, Term: 1, Command: []byte("ignored")},
		{Index: 2, Term: 1, Command: []byte("ignored")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplySet(1, "alpha", "one"); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplySet(2, "beta", "two"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.CreateSnapshot(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.Entry(1); err != nil || ok {
		t.Fatalf("entry 1 exists=%v err=%v, want compacted", ok, err)
	}

	other, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.InstallSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	value, ok, err := other.Get("beta")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || value != "two" {
		t.Fatalf("beta = (%q, %v), want (two, true)", value, ok)
	}
	index, term, err := other.SnapshotIndexTerm()
	if err != nil {
		t.Fatal(err)
	}
	if index != 2 || term != 1 {
		t.Fatalf("snapshot metadata = (%d, %d), want (2, 1)", index, term)
	}
}
