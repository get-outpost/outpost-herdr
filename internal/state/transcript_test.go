package state

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	outpost "github.com/get-outpost/outpost-go"
)

func testTranscriptCheckpoint(offset int64) TranscriptCheckpoint {
	return TranscriptCheckpoint{
		Initialized: true, Offset: offset, FileID: "file-hash", Anchor: "anchor-hash",
		SeenIDs: []string{"old-backfill", "new-message"},
		Fallbacks: []TranscriptFallback{{
			MessageID: "fallback", CommandHash: "command-hash", TextHash: "text-hash",
			DeliveredAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		}},
	}
}

func TestTranscriptVersionOneCheckpointPersistence(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	store := openTestStore(t, "transcripts")
	legacy := `{"version":1,"cursor":42,"live_sessions":["live"]}`
	if err := os.WriteFile(store.path, []byte(legacy), 0600); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, "transcripts")
	if _, present := store.Transcript("session"); present {
		t.Fatal("legacy file invented a checkpoint")
	}
	checkpoint := testTranscriptCheckpoint(123)
	if err := store.SetTranscript("session", checkpoint); err != nil {
		t.Fatal(err)
	}
	reopened := openTestStore(t, "transcripts")
	got, present := reopened.Transcript("session")
	if !present || !reflect.DeepEqual(got, checkpoint) {
		t.Fatalf("reopened checkpoint = %#v, present %v; want %#v", got, present, checkpoint)
	}
	if cursor, present := reopened.Cursor(); cursor != 42 || !present {
		t.Fatal("checkpoint lost legacy cursor")
	}
	if !reflect.DeepEqual(reopened.LiveSessions(), []string{"live"}) {
		t.Fatal("checkpoint lost legacy lifecycle state")
	}
}

func TestConcurrentCursorLifecycleAndTranscriptWrites(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	store := openTestStore(t, "all-streams")
	const count = 20
	var wg sync.WaitGroup
	errs := make(chan error, count*3)
	start := make(chan struct{})
	for i := 1; i <= count; i++ {
		wg.Add(3)
		go func(i int) {
			defer wg.Done()
			<-start
			if err := store.SetCursor(int64(i)); err != nil {
				errs <- err
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			<-start
			if err := store.RecordPublished(outpost.SessionUpdated, fmt.Sprintf("session-%02d", i)); err != nil {
				errs <- err
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			<-start
			if err := store.SetTranscript(fmt.Sprintf("session-%02d", i), testTranscriptCheckpoint(int64(i))); err != nil {
				errs <- err
			}
			store.Transcript(fmt.Sprintf("session-%02d", i))
			store.Cursor()
			store.LiveSessions()
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	reopened := openTestStore(t, "all-streams")
	if cursor, _ := reopened.Cursor(); cursor != count {
		t.Fatalf("cursor = %d; want %d", cursor, count)
	}
	if len(reopened.LiveSessions()) != count {
		t.Fatal("concurrent transcript writes lost lifecycle state")
	}
	for i := 1; i <= count; i++ {
		checkpoint, present := reopened.Transcript(fmt.Sprintf("session-%02d", i))
		if !present || checkpoint.Offset != int64(i) {
			t.Fatalf("lost transcript %d: %#v, %v", i, checkpoint, present)
		}
	}
}

func TestTranscriptFailedWriteDoesNotAdvanceMemory(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	store := openTestStore(t, "transcript-failure")
	checkpoint := testTranscriptCheckpoint(8)
	if err := store.SetTranscript("session", checkpoint); err != nil {
		t.Fatal(err)
	}
	store.path = filepath.Join(filepath.Dir(store.path), "blocked")
	if err := os.Mkdir(store.path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.SetTranscript("session", testTranscriptCheckpoint(9)); err == nil {
		t.Fatal("checkpoint replacement unexpectedly succeeded")
	}
	if got, _ := store.Transcript("session"); !reflect.DeepEqual(got, checkpoint) {
		t.Fatal("failed replacement advanced checkpoint")
	}
	if err := store.SetTranscript("new-session", checkpoint); err == nil {
		t.Fatal("new checkpoint replacement unexpectedly succeeded")
	}
	if _, present := store.Transcript("new-session"); present {
		t.Fatal("failed replacement installed a new checkpoint")
	}
}

func TestTranscriptRejectsInvalidCheckpointMetadata(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	store := openTestStore(t, "invalid-checkpoint")
	invalid := []TranscriptCheckpoint{
		{Offset: -1}, {Offset: 1}, {Initialized: true},
		{Initialized: true, FileID: "file", Offset: 1},
		{SeenIDs: []string{""}}, {SeenIDs: []string{"duplicate", "duplicate"}},
		{Fallbacks: []TranscriptFallback{{MessageID: "id"}}},
	}
	for i, checkpoint := range invalid {
		if err := store.SetTranscript("session", checkpoint); err == nil {
			t.Errorf("accepted invalid checkpoint %d", i)
		}
	}
	if err := store.SetTranscript(" ", TranscriptCheckpoint{}); err == nil {
		t.Fatal("accepted blank session identity")
	}
	if err := store.SetTranscript("session", testTranscriptCheckpoint(1)); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatal(err)
	}
	bad := strings.Replace(string(data), `"offset":1`, `"offset":-1`, 1)
	if err := os.WriteFile(store.path, []byte(bad), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open("invalid-checkpoint"); err == nil {
		t.Fatal("reopened corrupt checkpoint")
	}
}
