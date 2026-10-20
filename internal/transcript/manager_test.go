package transcript

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/get-outpost/outpost-herdr/internal/adapter"
	"github.com/get-outpost/outpost-herdr/internal/herdr"
	"github.com/get-outpost/outpost-herdr/internal/state"
)

type tailFakeSink struct {
	mu       sync.Mutex
	events   []adapter.Event
	notify   chan adapter.Event
	commands chan adapter.Command
	failure  error
	blocked  chan struct{}
	entered  chan struct{}
}

func newTailSink() *tailFakeSink {
	return &tailFakeSink{notify: make(chan adapter.Event, 256), commands: make(chan adapter.Command, 8)}
}

func (s *tailFakeSink) Publish(ctx context.Context, event adapter.Event) error {
	if s.entered != nil {
		select {
		case s.entered <- struct{}{}:
		default:
		}
	}
	if s.blocked != nil {
		select {
		case <-s.blocked:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.failure != nil {
		return s.failure
	}
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
	s.notify <- event
	return nil
}

func (s *tailFakeSink) Commands() <-chan adapter.Command { return s.commands }

func (s *tailFakeSink) messages() []adapter.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []adapter.Event
	for _, event := range s.events {
		if event.Type == "message.created" {
			events = append(events, event)
		}
	}
	return events
}

type tailMemoryStore struct {
	mu          sync.Mutex
	checkpoints map[string]state.TranscriptCheckpoint
	failure     error
	readEntered chan struct{}
	readRelease chan struct{}
	writes      chan struct{}
}

func newTailStore() *tailMemoryStore {
	return &tailMemoryStore{checkpoints: make(map[string]state.TranscriptCheckpoint), writes: make(chan struct{}, 256)}
}

func (s *tailMemoryStore) Transcript(id string) (state.TranscriptCheckpoint, bool) {
	if s.readEntered != nil {
		select {
		case s.readEntered <- struct{}{}:
		default:
		}
		<-s.readRelease
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	checkpoint, ok := s.checkpoints[id]
	checkpoint.SeenIDs = append([]string(nil), checkpoint.SeenIDs...)
	checkpoint.Fallbacks = append([]state.TranscriptFallback(nil), checkpoint.Fallbacks...)
	return checkpoint, ok
}

func (s *tailMemoryStore) SetTranscript(id string, checkpoint state.TranscriptCheckpoint) error {
	if s.failure != nil {
		return s.failure
	}
	checkpoint.SeenIDs = append([]string(nil), checkpoint.SeenIDs...)
	checkpoint.Fallbacks = append([]state.TranscriptFallback(nil), checkpoint.Fallbacks...)
	s.mu.Lock()
	s.checkpoints[id] = checkpoint
	s.mu.Unlock()
	s.writes <- struct{}{}
	return nil
}

func newTailManager(t *testing.T, sink adapter.Sink, store Store) *Manager {
	t.Helper()
	manager, err := New(sink, store, "synthetic-tests", Options{
		PollInterval: time.Hour, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		manager.Close(ctx)
	})
	return manager
}

func newTailWorker(t *testing.T, sink *tailFakeSink, store Store) *tailWorker {
	t.Helper()
	manager := newTailManager(t, sink, store)
	worker := &tailWorker{manager: manager, sessions: make(map[string]*tailSession), buffer: make([]byte, transcriptReadSize)}
	t.Cleanup(func() {
		for _, session := range worker.sessions {
			if session.file != nil {
				session.file.Close()
			}
		}
	})
	return worker
}

func tailAgent(path, provider string) herdr.Agent {
	agent := herdr.Agent{PaneID: "synthetic-pane", TerminalID: "synthetic-terminal", Agent: provider, AgentStatus: "idle"}
	if path != "" {
		agent.AgentSession = &herdr.SessionRef{Kind: "path", Agent: provider, Source: provider, Value: path}
	}
	return agent
}

func tailOMPLine(id, role, text string, at time.Time) string {
	entry := map[string]any{
		"type": "message", "id": id,
		"message": map[string]any{"role": role, "content": []any{map[string]any{"type": "text", "text": text}}, "stopReason": "stop"},
	}
	if !at.IsZero() {
		entry["timestamp"] = at.Format(time.RFC3339Nano)
	}
	data, _ := json.Marshal(entry)
	return string(data) + "\n"
}

func tailWrite(t *testing.T, path, text string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
}

func tailAppend(t *testing.T, path, text string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(text); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func tailSessionFor(t *testing.T, worker *tailWorker, agent herdr.Agent) *tailSession {
	t.Helper()
	id, err := adapter.SessionID("synthetic-tests", agent)
	if err != nil {
		t.Fatal(err)
	}
	return worker.session(id, agent)
}

func tailScan(t *testing.T, worker *tailWorker, session *tailSession) {
	t.Helper()
	if _, err := worker.scan(context.Background(), session); err != nil {
		t.Fatal(err)
	}
}

func tailTexts(sink *tailFakeSink) []string {
	var texts []string
	for _, event := range sink.messages() {
		texts = append(texts, event.Payload["text"].(string))
	}
	return texts
}

func tailWait(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not reach expected boundary")
	}
}

func TestTailLastTwentyPartialLinesAndReplacementDedupe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conversation.jsonl")
	var original strings.Builder
	for i := range 25 {
		original.WriteString(tailOMPLine(fmt.Sprint(i), "assistant", fmt.Sprintf("turn-%02d", i), time.Time{}))
	}
	partial := strings.TrimSuffix(tailOMPLine("partial", "user", "completed later", time.Time{}), "\n")
	tailWrite(t, path, original.String()+partial)
	sink, store := newTailSink(), newTailStore()
	worker := newTailWorker(t, sink, store)
	session := tailSessionFor(t, worker, tailAgent(path, "omp"))
	tailScan(t, worker, session)
	var want []string
	for i := 5; i < 25; i++ {
		want = append(want, fmt.Sprintf("turn-%02d", i))
	}
	if got := tailTexts(sink); !reflect.DeepEqual(got, want) {
		t.Fatalf("backfill = %v; want %v", got, want)
	}
	checkpoint, _ := store.Transcript(session.id)
	if len(checkpoint.SeenIDs) != 25 || checkpoint.Offset != int64(original.Len()) {
		t.Fatalf("backfill checkpoint = %#v", checkpoint)
	}
	tailScan(t, worker, session)
	if len(sink.messages()) != 20 {
		t.Fatal("partial record emitted or backfill repeated")
	}
	tailAppend(t, path, "\n")
	tailScan(t, worker, session)
	want = append(want, "completed later")
	rotated := path + ".old"
	if err := os.Rename(path, rotated); err != nil {
		t.Fatal(err)
	}
	tailAppend(t, rotated, tailOMPLine("late-old", "assistant", "old descriptor late write", time.Time{}))
	tailWrite(t, path, original.String()+partial+"\n"+tailOMPLine("new-file", "assistant", "new file turn", time.Time{}))
	tailScan(t, worker, session)
	want = append(want, "old descriptor late write", "new file turn")
	if got := tailTexts(sink); !reflect.DeepEqual(got, want) {
		t.Fatalf("rotation = %v; want %v", got, want)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	tailScan(t, worker, session)
	if got := tailTexts(sink); !reflect.DeepEqual(got, want) {
		t.Fatal("rotation gap reposted content")
	}
	for _, event := range sink.messages() {
		id, ok := event.Payload["message_id"].(string)
		if !ok || id == "" || strings.Contains(id, path) {
			t.Fatal("message ID is absent or exposes a transcript path")
		}
	}
}

func TestTailTruncationAndSameInodeRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rewrite.jsonl")
	tailWrite(t, path, tailOMPLine("old", "user", strings.Repeat("old", 100), time.Time{}))
	sink := newTailSink()
	worker := newTailWorker(t, sink, newTailStore())
	session := tailSessionFor(t, worker, tailAgent(path, "pi"))
	tailScan(t, worker, session)
	tailWrite(t, path, tailOMPLine("short", "assistant", "short", time.Time{}))
	tailScan(t, worker, session)
	// A longer overwrite of the same inode is detected by the committed anchor,
	// not only by comparing file size with the previous offset.
	tailWrite(t, path, tailOMPLine("long", "assistant", strings.Repeat("new", 100), time.Time{}))
	tailScan(t, worker, session)
	if got := tailTexts(sink); len(got) != 3 || got[1] != "short" || got[2] != strings.Repeat("new", 100) {
		t.Fatalf("rewrite messages = %v", got)
	}
}

func TestTailOversizedLineDrainsAndResumes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oversized.jsonl")
	tailWrite(t, path, strings.Repeat("x", maxTranscriptLine+32))
	sink, store := newTailSink(), newTailStore()
	worker := newTailWorker(t, sink, store)
	session := tailSessionFor(t, worker, tailAgent(path, "omp"))
	tailScan(t, worker, session)
	if !session.oversized || len(session.partial) != 0 || session.checkpoint.Offset != 0 {
		t.Fatal("oversized partial line retained or committed")
	}
	tailAppend(t, path, "\n"+tailOMPLine("after-large", "assistant", "after oversized line", time.Time{}))
	tailScan(t, worker, session)
	if got := tailTexts(sink); !reflect.DeepEqual(got, []string{"after oversized line"}) {
		t.Fatalf("messages = %v", got)
	}
	info, _ := os.Stat(path)
	checkpoint, _ := store.Transcript(session.id)
	if checkpoint.Offset != info.Size() || session.oversized {
		t.Fatal("oversized drain did not checkpoint the complete-line boundary")
	}
}

func TestTailDurableRestartAndOffsetHashIDs(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	store, err := state.Open("synthetic-tests")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "restart.jsonl")
	tailWrite(t, path, tailOMPLine("", "user", "no native identifier", time.Time{}))
	firstSink := newTailSink()
	first := newTailWorker(t, firstSink, store)
	agent := tailAgent(path, "omp")
	firstSession := tailSessionFor(t, first, agent)
	tailScan(t, first, firstSession)
	if len(firstSink.messages()) != 1 {
		t.Fatal("first record missing")
	}
	firstSession.file.Close()
	firstSession.file = nil
	reopened, err := state.Open("synthetic-tests")
	if err != nil {
		t.Fatal(err)
	}
	secondSink := newTailSink()
	second := newTailWorker(t, secondSink, reopened)
	secondSession := tailSessionFor(t, second, agent)
	tailScan(t, second, secondSession)
	if len(secondSink.messages()) != 0 {
		t.Fatal("clean restart reposted a record")
	}
	tailAppend(t, path, tailOMPLine("", "assistant", "new record", time.Time{}))
	tailScan(t, second, secondSession)
	if got := tailTexts(secondSink); !reflect.DeepEqual(got, []string{"new record"}) {
		t.Fatalf("restart tail = %v", got)
	}
	if firstSink.messages()[0].Payload["message_id"] == secondSink.messages()[0].Payload["message_id"] {
		t.Fatal("offset fallback IDs collided")
	}
}

func TestTailClaudeAssistantOnlyIncludingMissingTranscriptFallback(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	dir := filepath.Join(home, ".claude", "projects", "synthetic-project")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "synthetic-session.jsonl")
	tailWrite(t, path, `{"type":"user","uuid":"u","message":{"role":"user","content":"hook owns this"}}`+"\n"+`{"type":"assistant","uuid":"a","message":{"role":"assistant","content":[{"type":"text","text":"assistant only"}],"stop_reason":"end_turn"}}`+"\n")
	agent := tailAgent("", "claude")
	agent.AgentSession = &herdr.SessionRef{Kind: "id", Value: "synthetic-session"}
	sink := newTailSink()
	worker := newTailWorker(t, sink, newTailStore())
	session := tailSessionFor(t, worker, agent)
	tailScan(t, worker, session)
	if got := tailTexts(sink); !reflect.DeepEqual(got, []string{"assistant only"}) {
		t.Fatalf("Claude messages = %v", got)
	}
	agent.AgentSession = &herdr.SessionRef{Kind: "id", Value: "missing-session"}
	if err := worker.delivered(context.Background(), deliveredMessage{agent: agent, commandID: "claude-command", text: "no Claude fallback", at: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if len(sink.messages()) != 1 {
		t.Fatal("Claude fallback duplicated hook-owned user content")
	}
}

func TestManagerCallbacksNeverWaitForWorkerStoreAndCloseFinalScan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "asynchronous.jsonl")
	tailWrite(t, path, tailOMPLine("first", "assistant", "first", time.Time{}))
	store, sink := newTailStore(), newTailSink()
	store.readEntered, store.readRelease = make(chan struct{}, 1), make(chan struct{})
	manager := newTailManager(t, sink, store)
	agent := tailAgent(path, "omp")
	manager.UpdateSessions([]herdr.Agent{agent})
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	tailWait(t, store.readEntered)
	callbackDone := make(chan struct{})
	go func() {
		manager.UpdateSessions([]herdr.Agent{agent})
		manager.Delivered(context.Background(), agent, adapter.Command{ID: "admitted", Type: "command.message", Payload: map[string]any{"text": "native owns prompt"}})
		close(callbackDone)
	}()
	select {
	case <-callbackDone:
		close(store.readRelease)
	case <-time.After(3 * time.Second):
		close(store.readRelease)
		t.Fatal("callback waited for checkpoint I/O")
	}
	tailWait(t, store.writes)
	tailAppend(t, path, tailOMPLine("final", "assistant", "final scan", time.Time{}))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if got := tailTexts(sink); !reflect.DeepEqual(got, []string{"first", "final scan"}) {
		t.Fatalf("shutdown messages = %v", got)
	}
	if err := manager.Start(); err == nil {
		t.Fatal("closed manager restarted")
	}
	if err := manager.Delivered(context.Background(), agent, adapter.Command{ID: "late", Type: "command.message", Payload: map[string]any{"text": "late"}}); err == nil {
		t.Fatal("closed manager accepted a delivery")
	}
}

func TestManagerCloseBeforeStartDrainsAdmittedFallback(t *testing.T) {
	sink := newTailSink()
	manager := newTailManager(t, sink, newTailStore())
	agent := tailAgent("", "unknown-provider")
	if err := manager.Delivered(context.Background(), agent, adapter.Command{ID: "before-start", Type: "command.message", Payload: map[string]any{"text": "admitted before start"}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if got := tailTexts(sink); !reflect.DeepEqual(got, []string{"admitted before start"}) {
		t.Fatalf("close-before-start = %v", got)
	}
}

func TestManagerQueueOverflowAndWorkerErrorsAreFatal(t *testing.T) {
	t.Run("overflow", func(t *testing.T) {
		manager := newTailManager(t, newTailSink(), newTailStore())
		for i := range deliveryQueueSize {
			if err := manager.Delivered(context.Background(), tailAgent("", "unknown"), adapter.Command{ID: fmt.Sprint(i), Type: "command.message", Payload: map[string]any{"text": "queued"}}); err != nil {
				t.Fatal(err)
			}
		}
		err := manager.Delivered(context.Background(), tailAgent("", "unknown"), adapter.Command{ID: "overflow", Type: "command.message", Payload: map[string]any{"text": "queued"}})
		if err == nil {
			t.Fatal("overflow silently lost a delivery")
		}
		select {
		case first := <-manager.Errors():
			if first != err {
				t.Fatal("first fatal changed")
			}
		default:
			t.Fatal("overflow not surfaced")
		}
		if err := manager.Start(); err == nil {
			t.Fatal("failed manager started")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := manager.Close(ctx); err == nil {
			t.Fatal("failed startup close discarded fatal error")
		}
	})
	for _, failure := range []string{"sink", "checkpoint"} {
		t.Run(failure, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "failure.jsonl")
			tailWrite(t, path, tailOMPLine("message", "assistant", "content never appears in errors", time.Time{}))
			sink, store := newTailSink(), newTailStore()
			if failure == "sink" {
				sink.failure = errors.New("private path and transcript details")
			} else {
				store.failure = errors.New("private state path")
			}
			manager := newTailManager(t, sink, store)
			manager.UpdateSessions([]herdr.Agent{tailAgent(path, "omp")})
			if err := manager.Start(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-manager.Errors():
				if strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), path) {
					t.Fatal("fatal error exposed private details")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("worker failure not surfaced")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := manager.Close(ctx); err == nil {
				t.Fatal("worker failure missing at close")
			}
			if len(store.checkpoints) != 0 {
				t.Fatal("failed admission/checkpoint advanced durable state")
			}
		})
	}
}

func TestManagerCloseCancellationStopsBlockedPublish(t *testing.T) {
	path := filepath.Join(t.TempDir(), "blocked.jsonl")
	tailWrite(t, path, tailOMPLine("blocked", "assistant", "blocked", time.Time{}))
	sink := newTailSink()
	sink.blocked, sink.entered = make(chan struct{}), make(chan struct{}, 1)
	manager := newTailManager(t, sink, newTailStore())
	manager.UpdateSessions([]herdr.Agent{tailAgent(path, "omp")})
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	tailWait(t, sink.entered)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := manager.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close = %v", err)
	}
	tailWait(t, manager.done)
}

type tailAdmissionSource struct {
	agent   herdr.Agent
	manager *Manager
	reject  bool
}

func (s *tailAdmissionSource) Watch(ctx context.Context, consume func(herdr.Snapshot) error) error {
	if err := consume(herdr.Snapshot{Agents: []herdr.Agent{s.agent}}); err != nil {
		return err
	}
	s.manager.UpdateSessions([]herdr.Agent{s.agent})
	<-ctx.Done()
	return ctx.Err()
}
func (s *tailAdmissionSource) GetAgent(context.Context, string) (herdr.Agent, error) {
	return s.agent, nil
}
func (s *tailAdmissionSource) Prompt(context.Context, string, string) error {
	if s.reject {
		return errors.New("synthetic rejection")
	}
	return nil
}
func (s *tailAdmissionSource) SendKeys(context.Context, string, ...string) error { return nil }

func TestAdapterPromptAdmissionControlsManagerFallback(t *testing.T) {
	for _, rejected := range []bool{false, true} {
		t.Run(fmt.Sprintf("rejected_%v", rejected), func(t *testing.T) {
			sink := newTailSink()
			manager := newTailManager(t, sink, newTailStore())
			if err := manager.Start(); err != nil {
				t.Fatal(err)
			}
			agent := tailAgent("", "unknown-provider")
			source := &tailAdmissionSource{agent: agent, manager: manager, reject: rejected}
			bridge := adapter.New(source, sink, "synthetic-tests")
			bridge.SessionGracePeriod = 0
			bridge.MessageDelivered = manager.Delivered
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- bridge.Run(ctx) }()
			select {
			case <-sink.notify:
			case <-time.After(3 * time.Second):
				t.Fatal("adapter did not publish a session")
			}
			id, _ := adapter.SessionID("synthetic-tests", agent)
			sink.commands <- adapter.Command{ID: "admission-test", Type: "command.message", SessionID: id, Payload: map[string]any{"text": "admitted prompt"}}
			if rejected {
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("rejected prompt succeeded")
					}
				case <-time.After(3 * time.Second):
					t.Fatal("rejected prompt did not return")
				}
			} else {
				deadline := time.NewTimer(3 * time.Second)
				defer deadline.Stop()
				for len(sink.messages()) == 0 {
					select {
					case <-sink.notify:
					case <-deadline.C:
						t.Fatal("admitted prompt fallback missing")
					}
				}
				message := sink.messages()[0]
				if message.SessionID != id || message.Payload["role"] != "user" || message.Payload["text"] != "admitted prompt" {
					t.Fatalf("admitted fallback = %#v", message)
				}
				cancel()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("adapter did not stop")
				}
			}
			flush, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			if err := manager.Close(flush); err != nil {
				t.Fatal(err)
			}
			want := 1
			if rejected {
				want = 0
			}
			if got := len(sink.messages()); got != want {
				t.Fatalf("fallback count = %d; want %d", got, want)
			}
		})
	}
}
