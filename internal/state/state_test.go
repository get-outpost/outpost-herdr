package state

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"

	outpost "github.com/get-outpost/outpost-go"
)

func openTestStore(t *testing.T, namespace string) *Store {
	t.Helper()
	store, err := Open(namespace)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestDurableRestartPreservesCursorAndLiveSessions(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	store := openTestStore(t, "restart")
	if cursor, present := store.Cursor(); cursor != 0 || present {
		t.Fatalf("first cursor = (%d, %v)", cursor, present)
	}
	// Lifecycle persistence must not turn an uninitialized cursor into zero.
	if err := store.RecordPublished(outpost.SessionUpdated, "implicit"); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, "restart")
	if _, present := store.Cursor(); present {
		t.Fatal("lifecycle update initialized cursor")
	}
	if err := store.SetCursor(0); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, "restart")
	if cursor, present := store.Cursor(); cursor != 0 || !present {
		t.Fatalf("initialized zero cursor = (%d, %v)", cursor, present)
	}
	for _, event := range []struct{ kind, id string }{
		{outpost.SessionStarted, "active"},
		{outpost.SessionStarted, "ended"},
		{outpost.SessionEnded, "ended"},
		{outpost.SessionStarted, "deleted"},
		{outpost.SessionDeleted, "deleted"},
	} {
		if err := store.RecordPublished(event.kind, event.id); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.SetCursor(42); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, "restart")
	if cursor, present := store.Cursor(); cursor != 42 || !present {
		t.Fatalf("restart cursor = (%d, %v)", cursor, present)
	}
	want := []string{"active", "implicit"}
	if got := store.LiveSessions(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restart live sessions = %v; want %v", got, want)
	}
	copy := store.LiveSessions()
	copy[0] = "mutated"
	if got := store.LiveSessions(); !reflect.DeepEqual(got, want) {
		t.Fatalf("caller mutated live state: %v", got)
	}
}

func TestCorruptStateIsFatalAndNeverRewritten(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	for _, data := range []string{
		``, `null`, `{}`, `{"version":1,"cursor":`,
		`{"version":2,"cursor":42,"live_sessions":[]}`,
		`{"version":1,"live_sessions":[]}`,
		`{"version":1,"cursor":-1,"live_sessions":[]}`,
		`{"version":1,"cursor":"secret","live_sessions":[]}`,
		`{"version":1,"cursor":1.5,"live_sessions":[]}`,
		`{"version":1,"cursor":9223372036854775808,"live_sessions":[]}`,
		`{"version":1,"cursor":42}`,
		`{"version":1,"cursor":42,"live_sessions":null}`,
		`{"version":1,"cursor":42,"live_sessions":[""]}`,
		`{"version":1,"cursor":42,"live_sessions":["a","a"]}`,
		`{"version":1,"cursor":42,"live_sessions":[]} {}`,
	} {
		t.Run(fmt.Sprintf("case_%x", sha256.Sum256([]byte(data))), func(t *testing.T) {
			store := openTestStore(t, t.Name())
			if err := os.WriteFile(store.path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(t.Name()); err == nil {
				t.Fatal("accepted corrupt state")
			}
			after, err := os.ReadFile(store.path)
			if err != nil || string(after) != data {
				t.Fatalf("corrupt state changed: %q, %v", after, err)
			}
		})
	}
}

func TestConcurrentCursorAndLifecycleUpdatesAreDurable(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	store := openTestStore(t, "concurrent")
	const workers = 24
	var wg sync.WaitGroup
	errs := make(chan error, workers*3)
	start := make(chan struct{})
	for i := 1; i <= workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if err := store.SetCursor(int64(i)); err != nil {
				errs <- err
			}
			if err := store.RecordPublished(outpost.SessionUpdated, fmt.Sprintf("session-%02d", i)); err != nil {
				errs <- err
			}
			if i%2 == 0 {
				if err := store.RecordPublished(outpost.SessionEnded, fmt.Sprintf("session-%02d", i)); err != nil {
					errs <- err
				}
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for range workers * 2 {
			store.Cursor()
			store.LiveSessions()
		}
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	reopened := openTestStore(t, "concurrent")
	if cursor, present := reopened.Cursor(); cursor != workers || !present {
		t.Fatalf("cursor = (%d, %v); want (%d, true)", cursor, present, workers)
	}
	var want []string
	for i := 1; i <= workers; i += 2 {
		want = append(want, fmt.Sprintf("session-%02d", i))
	}
	if got := reopened.LiveSessions(); !reflect.DeepEqual(got, want) {
		t.Fatalf("live set lost concurrent updates: %v; want %v", got, want)
	}
}

func TestNamespaceHashAndHomeFallback(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", "")
	namespace := "../../outside/namespace"
	store := openTestStore(t, namespace)
	hash := sha256.Sum256([]byte(namespace))
	want := filepath.Join(home, ".local", "state", "outpost-herdr", hex.EncodeToString(hash[:])+".json")
	if store.path != want {
		t.Fatalf("state path = %q; want %q", store.path, want)
	}
	if err := store.SetCursor(7); err != nil {
		t.Fatal(err)
	}
	other := openTestStore(t, "other")
	if other.path == store.path {
		t.Fatal("namespaces share a file")
	}
	if _, present := other.Cursor(); present {
		t.Fatal("namespace inherited another cursor")
	}
	for _, blank := range []string{"", " \t\n"} {
		if _, err := Open(blank); err == nil {
			t.Fatal("accepted blank namespace")
		}
	}
	if err := store.SetCursor(-1); err == nil {
		t.Fatal("accepted negative cursor")
	}
	if err := store.RecordPublished(outpost.SessionStarted, " \t"); err == nil {
		t.Fatal("accepted blank lifecycle ID")
	}
}

func TestPrivatePermissionsAndAtomicReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits")
	}
	root := t.TempDir()
	t.Setenv("XDG_STATE_HOME", root)
	dir := filepath.Join(root, "outpost-herdr")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	store := openTestStore(t, "private")
	if err := store.SetCursor(1); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(store.path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetCursor(2); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(store.path)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(before, after) {
		t.Fatal("state was overwritten instead of atomically replaced")
	}
	for path, mode := range map[string]os.FileMode{dir: 0700, store.path: 0600} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != mode {
			t.Fatalf("permissions = %o; want %o", info.Mode().Perm(), mode)
		}
	}
	if err := os.Chmod(store.path, 0644); err != nil {
		t.Fatal(err)
	}
	openTestStore(t, "private")
	info, err := os.Stat(store.path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("existing state not protected: %v, %v", info, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".state-") {
			t.Fatal("temporary state file leaked")
		}
	}
}

func TestFailedWriteDoesNotAdvanceMemoryOrLoseLiveSet(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	store := openTestStore(t, "failure")
	if err := store.SetCursor(5); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordPublished(outpost.SessionStarted, "existing"); err != nil {
		t.Fatal(err)
	}
	// A directory cannot be replaced by a state file, even when running as root.
	store.path = filepath.Join(filepath.Dir(store.path), "blocked")
	if err := os.Mkdir(store.path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.SetCursor(6); err == nil {
		t.Fatal("failed cursor replacement reported success")
	}
	if cursor, _ := store.Cursor(); cursor != 5 {
		t.Fatalf("failed write advanced cursor to %d", cursor)
	}
	if err := store.RecordPublished(outpost.SessionEnded, "existing"); err == nil {
		t.Fatal("failed lifecycle replacement reported success")
	}
	if got := store.LiveSessions(); !reflect.DeepEqual(got, []string{"existing"}) {
		t.Fatalf("failed write lost live set: %v", got)
	}
}

func TestStateFileSymlinkIsRejected(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require privileges")
	}
	root := t.TempDir()
	t.Setenv("XDG_STATE_HOME", root)
	store := openTestStore(t, "symlink")
	outside := filepath.Join(root, "outside.json")
	original := `{"version":1,"cursor":99,"live_sessions":[]}`
	if err := os.WriteFile(outside, []byte(original), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, store.path); err != nil {
		t.Fatal(err)
	}
	if _, err := Open("symlink"); err == nil {
		t.Fatal("accepted a state file symlink")
	}
	data, err := os.ReadFile(outside)
	if err != nil || string(data) != original {
		t.Fatal("rejected symlink changed the outside file")
	}
}
