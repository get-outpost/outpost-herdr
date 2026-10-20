package adapter

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/get-outpost/outpost-herdr/internal/herdr"
)

type fakeSnapshot struct {
	snapshot herdr.Snapshot
	applied  chan error
}

type fakeAction struct {
	pane string
	text string
	keys []string
}

type fakeSource struct {
	mu        sync.Mutex
	agents    map[string]herdr.Agent
	getErr    error
	promptErr error
	keysErr   error
	gets      []string
	snapshots chan fakeSnapshot
	actions   chan fakeAction
	started   chan struct{}
	stopped   chan struct{}
}

func newFakeSource() *fakeSource {
	return &fakeSource{
		agents:    make(map[string]herdr.Agent),
		snapshots: make(chan fakeSnapshot),
		actions:   make(chan fakeAction, 16),
		started:   make(chan struct{}),
		stopped:   make(chan struct{}),
	}
}

func (s *fakeSource) Watch(ctx context.Context, consume func(herdr.Snapshot) error) error {
	close(s.started)
	defer close(s.stopped)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case request := <-s.snapshots:
			err := consume(request.snapshot)
			request.applied <- err
			if err != nil {
				return err
			}
		}
	}
}

func (s *fakeSource) GetAgent(_ context.Context, pane string) (herdr.Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets = append(s.gets, pane)
	if s.getErr != nil {
		return herdr.Agent{}, s.getErr
	}
	agent, ok := s.agents[pane]
	if !ok {
		return herdr.Agent{}, errors.New("pane has no agent")
	}
	return agent, nil
}

func (s *fakeSource) Prompt(_ context.Context, pane, text string) error {
	s.mu.Lock()
	err := s.promptErr
	s.mu.Unlock()
	s.actions <- fakeAction{pane: pane, text: text}
	return err
}

func (s *fakeSource) SendKeys(_ context.Context, pane string, keys ...string) error {
	s.mu.Lock()
	err := s.keysErr
	s.mu.Unlock()
	s.actions <- fakeAction{pane: pane, keys: append([]string(nil), keys...)}
	return err
}

func (s *fakeSource) setAgents(agents ...herdr.Agent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.agents = make(map[string]herdr.Agent, len(agents))
	for _, agent := range agents {
		s.agents[agent.PaneID] = agent
	}
}

func (s *fakeSource) apply(t *testing.T, agents ...herdr.Agent) {
	t.Helper()
	s.setAgents(agents...)
	request := fakeSnapshot{snapshot: herdr.Snapshot{Agents: agents}, applied: make(chan error, 1)}
	select {
	case s.snapshots <- request:
	case <-time.After(3 * time.Second):
		t.Fatal("snapshot was not consumed")
	}
	select {
	case err := <-request.applied:
		if err != nil {
			t.Fatalf("apply snapshot: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("snapshot reconciliation did not finish")
	}
}

func (s *fakeSource) getCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.gets)
}

func nativeAgent(pane, id, state string) herdr.Agent {
	return herdr.Agent{
		PaneID: pane, TerminalID: "terminal-" + pane, Agent: "claude", AgentStatus: state,
		AgentSession: &herdr.SessionRef{Source: "claude", Agent: "claude", Kind: "id", Value: id},
	}
}

func runAdapter(t *testing.T, source *fakeSource, sink Sink) (<-chan error, context.CancelFunc) {
	t.Helper()
	return runWithSource(t, source, source, sink)
}

func runWithSource(t *testing.T, source herdr.Source, lifecycle *fakeSource, sink Sink) (<-chan error, context.CancelFunc) {
	t.Helper()
	return runConfiguredAdapter(t, New(source, sink, "test-installation"), lifecycle)
}

func runConfiguredAdapter(t *testing.T, adapter *Adapter, lifecycle *fakeSource) (<-chan error, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- adapter.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-lifecycle.stopped:
		case <-time.After(3 * time.Second):
			t.Error("source did not stop after adapter cancellation")
		}
	})
	select {
	case <-lifecycle.started:
	case <-time.After(3 * time.Second):
		t.Fatal("adapter did not start watching")
	}
	return done, cancel
}

func receiveAction(t *testing.T, source *fakeSource) fakeAction {
	t.Helper()
	select {
	case action := <-source.actions:
		return action
	case <-time.After(3 * time.Second):
		t.Fatal("expected command execution")
		return fakeAction{}
	}
}

func receiveError(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("adapter did not return")
		return nil
	}
}

func send(t *testing.T, sink *MemorySink, command Command) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := sink.Send(ctx, command); err != nil {
		t.Fatalf("queue command: %v", err)
	}
}

func TestReconcileLifecycleAndStatus(t *testing.T) {
	source, sink := newFakeSource(), NewMemorySink(8)
	runAdapter(t, source, sink)
	agent := nativeAgent("pane-1", "native-id", "working")
	agent.Name, agent.Title = "Friendly task", "Lower priority title"
	agent.Cwd, agent.ForegroundCwd = "/shell", "/worktree"
	source.apply(t, agent)
	want := []Event{
		{Type: "session.started", SessionID: "native-id", Payload: map[string]any{"provider": "claude", "cwd": "/worktree", "title": "Friendly task"}},
		{Type: "session.updated", SessionID: "native-id", Payload: map[string]any{"status": "working"}},
	}
	if got := sink.Events(); !reflect.DeepEqual(got, want) {
		t.Fatalf("initial events = %#v, want %#v", got, want)
	}
	source.apply(t, agent)
	agent.AgentStatus = "unknown"
	source.apply(t, agent)
	agent.AgentStatus = "future-state"
	source.apply(t, agent)
	if got := sink.Events(); !reflect.DeepEqual(got, want) {
		t.Fatalf("unchanged/unknown status emitted events: %#v", got)
	}
	for _, state := range []string{"blocked", "done", "unknown", "idle"} {
		agent.AgentStatus = state
		source.apply(t, agent)
	}
	want = append(want,
		Event{Type: "session.updated", SessionID: "native-id", Payload: map[string]any{"status": "blocked"}},
		Event{Type: "session.updated", SessionID: "native-id", Payload: map[string]any{"status": "idle"}},
	)
	if got := sink.Events(); !reflect.DeepEqual(got, want) {
		t.Fatalf("status events = %#v, want %#v", got, want)
	}

	replacement := nativeAgent("pane-1", "replacement", "unknown")
	source.apply(t, replacement)
	events := sink.Events()
	if len(events) != 6 || events[4].Type != "session.ended" || events[4].SessionID != "native-id" || events[5].Type != "session.started" || events[5].SessionID != "replacement" {
		t.Fatalf("replacement lifecycle = %#v", events)
	}
	// A pane that remains without an active agent must not keep the old session.
	replacement.Agent = ""
	source.apply(t, replacement)
	events = sink.Events()
	if len(events) != 7 || events[6].Type != "session.ended" || events[6].SessionID != "replacement" {
		t.Fatalf("agent disappearance = %#v", events)
	}
}

func TestNativeSessionMergeDisappearanceAndReuse(t *testing.T) {
	source, sink := newFakeSource(), NewMemorySink(8)
	runAdapter(t, source, sink)
	first := nativeAgent("pane-a", "same-native-id", "working")
	second := nativeAgent("pane-b", "same-native-id", "working")
	source.apply(t, second, first)
	source.apply(t, first, second)
	if got := sink.Events(); len(got) != 2 {
		t.Fatalf("duplicate native ID created duplicate session events: %#v", got)
	}
	send(t, sink, Command{Type: "command.message", SessionID: "same-native-id", Payload: map[string]any{"text": "first"}})
	if got := receiveAction(t, source); got.pane != "pane-a" {
		t.Fatalf("non-deterministic duplicate target: %#v", got)
	}
	source.apply(t, second)
	if got := sink.Events(); len(got) != 2 {
		t.Fatalf("ended a session with a remaining agent: %#v", got)
	}
	send(t, sink, Command{Type: "command.message", SessionID: "same-native-id", Payload: map[string]any{"text": "remaining"}})
	if got := receiveAction(t, source); got.pane != "pane-b" {
		t.Fatalf("remaining duplicate not targeted: %#v", got)
	}
	source.apply(t)
	source.apply(t, first)
	events := sink.Events()
	if len(events) != 5 || events[2].Type != "session.ended" || events[3].Type != "session.started" || events[4].Type != "session.updated" {
		t.Fatalf("native session reuse = %#v", events)
	}
}

func TestSessionIDFallbacks(t *testing.T) {
	path := herdr.Agent{PaneID: "pane-a", TerminalID: "terminal-a", Agent: "omp", AgentSession: &herdr.SessionRef{Source: "omp", Agent: "omp", Kind: "path", Value: "/sessions/task.json"}}
	terminal := herdr.Agent{PaneID: "pane-a", TerminalID: "terminal-a", Agent: "omp"}
	pane := herdr.Agent{PaneID: "pane-a", Agent: "omp"}
	for _, test := range []struct {
		name  string
		agent herdr.Agent
	}{
		{"path", path}, {"terminal", terminal}, {"pane", pane},
	} {
		t.Run(test.name, func(t *testing.T) {
			id, err := SessionID("installation-a", test.agent)
			if err != nil || !strings.HasPrefix(id, "herdr:") || len(id) != len("herdr:")+64 {
				t.Fatalf("fallback identity = %q, %v", id, err)
			}
			again, _ := SessionID("installation-a", test.agent)
			other, _ := SessionID("installation-b", test.agent)
			if id != again || id == other {
				t.Fatalf("fallback is unstable or not namespaced: %q %q %q", id, again, other)
			}
		})
	}
	pathID, _ := SessionID("installation-a", path)
	path.PaneID, path.TerminalID, path.Revision = "moved-pane", "new-terminal", 99
	if got, _ := SessionID("installation-a", path); got != pathID {
		t.Fatalf("path identity changed when pane moved: %q != %q", got, pathID)
	}
	terminalID, _ := SessionID("installation-a", terminal)
	terminal.PaneID = "moved-pane"
	if got, _ := SessionID("installation-a", terminal); got != terminalID {
		t.Fatal("terminal fallback depends on pane")
	}
	native := nativeAgent("pane", " exact-native-id ", "idle")
	if got, _ := SessionID("namespace", native); got != " exact-native-id " {
		t.Fatalf("native identity changed: %q", got)
	}
	if _, err := SessionID(" \t", native); err == nil {
		t.Fatal("blank namespace accepted")
	}
	if _, err := SessionID("namespace", herdr.Agent{}); err == nil {
		t.Fatal("missing identity accepted")
	}
}

func TestFallbackSessionSurvivesPaneMove(t *testing.T) {
	for _, kind := range []string{"path", "terminal"} {
		t.Run(kind, func(t *testing.T) {
			source, sink := newFakeSource(), NewMemorySink(1)
			runAdapter(t, source, sink)
			agent := herdr.Agent{PaneID: "before", TerminalID: "terminal", Agent: "omp", AgentStatus: "idle"}
			if kind == "path" {
				agent.AgentSession = &herdr.SessionRef{Kind: "path", Source: "omp", Agent: "omp", Value: "/session"}
			}
			source.apply(t, agent)
			agent.PaneID = "after"
			source.apply(t, agent)
			if events := sink.Events(); len(events) != 2 {
				t.Fatalf("pane move restarted fallback session: %#v", events)
			}
			id, _ := SessionID("test-installation", agent)
			send(t, sink, Command{Type: "command.message", SessionID: id, Payload: map[string]any{"text": "moved"}})
			if got := receiveAction(t, source); got.pane != "after" {
				t.Fatalf("command used old pane: %#v", got)
			}
		})
	}
}

func TestCommandsIgnoreForeignUnsupportedApprovalAndEncrypted(t *testing.T) {
	source, sink := newFakeSource(), NewMemorySink(8)
	runAdapter(t, source, sink)
	agent := nativeAgent("pane", "native", "idle")
	source.apply(t, agent)
	for _, command := range []Command{
		{Type: "command.message", SessionID: "foreign"},
		{Type: "command.unsupported", SessionID: "native"},
		{Type: "command.approve", SessionID: "native"},
		{Type: "command.message", SessionID: "native", Enc: "encrypted", Payload: map[string]any{"text": "secret"}},
		{Type: "command.interrupt", SessionID: "native", Enc: "encrypted"},
	} {
		send(t, sink, command)
	}
	// No server owner-device check: a native hook may have created the session.
	send(t, sink, Command{ID: "message", Type: "command.message", SessionID: "native", Payload: map[string]any{"text": "  keep whitespace  ", "owner_device_id": "native-hook"}})
	if got := receiveAction(t, source); got.pane != "pane" || got.text != "  keep whitespace  " || got.keys != nil {
		t.Fatalf("message execution = %#v", got)
	}
	if got := source.getCount(); got != 1 {
		t.Fatalf("ignored commands caused live reads: %d", got)
	}
	// Status/revision changes are not agent replacements.
	agent.Revision, agent.AgentStatus = 100, "working"
	source.setAgents(agent)
	send(t, sink, Command{ID: "interrupt", Type: "command.interrupt", SessionID: "native"})
	if got := receiveAction(t, source); got.pane != "pane" || !reflect.DeepEqual(got.keys, []string{"esc"}) {
		t.Fatalf("interrupt execution = %#v", got)
	}
	if got := source.getCount(); got != 2 {
		t.Fatalf("commands were not individually revalidated: %d", got)
	}
}

func TestCommandsRejectStaleTargets(t *testing.T) {
	for _, change := range []struct {
		name  string
		apply func(*herdr.Agent)
	}{
		{"native session", func(a *herdr.Agent) { a.AgentSession = &herdr.SessionRef{Kind: "id", Value: "replacement"} }},
		{"pane", func(a *herdr.Agent) { a.PaneID = "other-pane" }},
		{"terminal", func(a *herdr.Agent) { a.TerminalID = "other-terminal" }},
		{"agent", func(a *herdr.Agent) { a.Agent = "codex" }},
		{"agent disappeared", func(a *herdr.Agent) { a.Agent = "" }},
	} {
		t.Run(change.name, func(t *testing.T) {
			source, sink := newFakeSource(), NewMemorySink(2)
			done, _ := runAdapter(t, source, sink)
			agent := nativeAgent("pane", "native", "idle")
			source.apply(t, agent)
			live := agent
			change.apply(&live)
			source.mu.Lock()
			source.agents["pane"] = live
			source.mu.Unlock()
			send(t, sink, Command{ID: "stale-command", Type: "command.message", SessionID: "native", Payload: map[string]any{"text": "must not arrive"}})
			err := receiveError(t, done)
			if err == nil || !strings.Contains(err.Error(), "stale-command") || !strings.Contains(err.Error(), "no longer identifies") {
				t.Fatalf("stale target error = %v", err)
			}
			if len(source.actions) != 0 || source.getCount() != 1 {
				t.Fatal("stale command executed or retried")
			}
		})
	}
}

func TestCommandValidationAndVisibleErrors(t *testing.T) {
	for _, payload := range []map[string]any{nil, {"text": " \n\t"}, {"text": 12}} {
		source, sink := newFakeSource(), NewMemorySink(1)
		done, _ := runAdapter(t, source, sink)
		source.apply(t, nativeAgent("pane", "native", "idle"))
		send(t, sink, Command{ID: "invalid", Type: "command.message", SessionID: "native", Payload: payload})
		if err := receiveError(t, done); err == nil || !strings.Contains(err.Error(), "invalid") || !strings.Contains(err.Error(), "nonblank string") {
			t.Fatalf("invalid message error = %v", err)
		}
		if source.getCount() != 0 || len(source.actions) != 0 {
			t.Fatal("invalid payload reached source")
		}
	}
	for _, kind := range []string{"get", "prompt", "interrupt"} {
		t.Run(kind, func(t *testing.T) {
			source, sink := newFakeSource(), NewMemorySink(2)
			done, _ := runAdapter(t, source, sink)
			source.apply(t, nativeAgent("pane", "native", "idle"))
			failure := errors.New("source failed")
			source.mu.Lock()
			switch kind {
			case "get":
				source.getErr = failure
			case "prompt":
				source.promptErr = failure
			case "interrupt":
				source.keysErr = failure
			}
			source.mu.Unlock()
			command := Command{ID: "failure", Type: "command.message", SessionID: "native", Payload: map[string]any{"text": "once"}}
			if kind == "interrupt" {
				command.Type = "command.interrupt"
			}
			send(t, sink, command)
			if err := receiveError(t, done); !errors.Is(err, failure) || !strings.Contains(err.Error(), "failure") {
				t.Fatalf("source error not surfaced: %v", err)
			}
			wantActions := 1
			if kind == "get" {
				wantActions = 0
			}
			if len(source.actions) != wantActions || source.getCount() != 1 {
				t.Fatal("command failure was retried")
			}
		})
	}
}

func TestRunCancellationAndConfiguration(t *testing.T) {
	if err := New(newFakeSource(), NewMemorySink(1), " ").Run(context.Background()); err == nil {
		t.Fatal("blank namespace accepted by Run")
	}
	source, sink := newFakeSource(), NewMemorySink(1)
	done, cancel := runAdapter(t, source, sink)
	cancel()
	if err := receiveError(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
}

func TestMemorySinkSendCancellationWhileFull(t *testing.T) {
	sink := NewMemorySink(1)
	if err := sink.Send(context.Background(), Command{ID: "first"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- sink.Send(ctx, Command{ID: "blocked"})
	}()
	<-started
	select {
	case err := <-done:
		t.Fatalf("send to a full queue did not block: %v", err)
	default:
	}
	cancel()
	if err := receiveError(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked Send = %v", err)
	}
	if got := <-sink.Commands(); got.ID != "first" {
		t.Fatalf("canceled send displaced queued command: %#v", got)
	}
}

func TestCommandsWaitForInitialSnapshot(t *testing.T) {
	source, sink := newFakeSource(), NewMemorySink(1)
	send(t, sink, Command{Type: "command.message", SessionID: "native", Payload: map[string]any{"text": "queued before snapshot"}})
	runAdapter(t, source, sink)
	source.apply(t, nativeAgent("pane", "native", "idle"))
	if got := receiveAction(t, source); got.text != "queued before snapshot" {
		t.Fatalf("bootstrap command was lost: %#v", got)
	}
}

func TestClosedOrNilCommandsKeepMonitoring(t *testing.T) {
	for _, mode := range []string{"closed", "nil"} {
		t.Run(mode, func(t *testing.T) {
			source, sink := newFakeSource(), NewMemorySink(1)
			if mode == "closed" {
				close(sink.commands)
			} else {
				sink.commands = nil
			}
			done, cancel := runAdapter(t, source, sink)
			agent := nativeAgent("pane", "native", "working")
			source.apply(t, agent)
			agent.AgentStatus = "blocked"
			source.apply(t, agent)
			source.apply(t)
			events := sink.Events()
			if len(events) != 4 || events[3].Type != "session.ended" {
				t.Fatalf("command stream stopped reconciliation: %#v", events)
			}
			cancel()
			if err := receiveError(t, done); !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v", err)
			}
		})
	}
}

func TestCommandsRejectChangedFallbackPath(t *testing.T) {
	source, sink := newFakeSource(), NewMemorySink(1)
	done, _ := runAdapter(t, source, sink)
	agent := herdr.Agent{PaneID: "pane", TerminalID: "terminal", Agent: "omp", AgentStatus: "idle", AgentSession: &herdr.SessionRef{Kind: "path", Source: "omp", Agent: "omp", Value: "/session/first"}}
	source.apply(t, agent)
	id, _ := SessionID("test-installation", agent)
	agent.AgentSession = &herdr.SessionRef{Kind: "path", Source: "omp", Agent: "omp", Value: "/session/replacement"}
	source.setAgents(agent)
	send(t, sink, Command{Type: "command.interrupt", SessionID: id})
	if err := receiveError(t, done); err == nil || !strings.Contains(err.Error(), "no longer identifies") {
		t.Fatalf("changed path accepted: %v", err)
	}
	if len(source.actions) != 0 {
		t.Fatal("interrupt reached replacement session")
	}
}

type blockingGetSource struct {
	*fakeSource
	entered chan struct{}
	release chan struct{}
}

func (s *blockingGetSource) GetAgent(ctx context.Context, pane string) (herdr.Agent, error) {
	close(s.entered)
	select {
	case <-s.release:
		return s.fakeSource.GetAgent(ctx, pane)
	case <-ctx.Done():
		return herdr.Agent{}, ctx.Err()
	}
}

func TestPendingSnapshotCannotBypassCommandRevalidation(t *testing.T) {
	base, sink := newFakeSource(), NewMemorySink(1)
	source := &blockingGetSource{fakeSource: base, entered: make(chan struct{}), release: make(chan struct{})}
	done, _ := runWithSource(t, source, base, sink)
	base.apply(t, nativeAgent("pane", "original", "idle"))
	send(t, sink, Command{ID: "queued", Type: "command.message", SessionID: "original", Payload: map[string]any{"text": "must not reach replacement"}})
	select {
	case <-source.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("command was not revalidated")
	}
	replacement := nativeAgent("pane", "replacement", "idle")
	base.setAgents(replacement)
	request := fakeSnapshot{snapshot: herdr.Snapshot{Agents: []herdr.Agent{replacement}}, applied: make(chan error, 1)}
	select {
	case base.snapshots <- request:
	case <-time.After(3 * time.Second):
		t.Fatal("source could not offer concurrent snapshot")
	}
	close(source.release)
	if err := receiveError(t, done); err == nil || !strings.Contains(err.Error(), "no longer identifies") {
		t.Fatalf("stale command not rejected: %v", err)
	}
	select {
	case err := <-request.applied:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pending snapshot = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("snapshot callback remained blocked")
	}
	if len(base.actions) != 0 {
		t.Fatal("replacement received stale command")
	}
}

func TestCancellationStopsCommandAndSnapshotConsumer(t *testing.T) {
	base, sink := newFakeSource(), NewMemorySink(1)
	source := &blockingGetSource{fakeSource: base, entered: make(chan struct{}), release: make(chan struct{})}
	done, cancel := runWithSource(t, source, base, sink)
	base.apply(t, nativeAgent("pane", "native", "idle"))
	send(t, sink, Command{Type: "command.interrupt", SessionID: "native"})
	select {
	case <-source.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("command was not revalidated")
	}
	cancel()
	if err := receiveError(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled command = %v", err)
	}
	if len(base.actions) != 0 {
		t.Fatal("canceled command reached pane")
	}
}

func TestRunRejectsConcurrentConsumers(t *testing.T) {
	source := newFakeSource()
	adapter := New(source, NewMemorySink(1), "namespace")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- adapter.Run(ctx) }()
	select {
	case <-source.started:
	case <-time.After(3 * time.Second):
		t.Fatal("adapter did not start")
	}
	if err := adapter.Run(ctx); err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("second consumer admitted: %v", err)
	}
	cancel()
	if err := receiveError(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
}

type failingSink struct {
	*MemorySink
	err error
}

func (s *failingSink) Publish(context.Context, Event) error { return s.err }

func TestPublishErrorStopsReconciliation(t *testing.T) {
	source := newFakeSource()
	failure := errors.New("sink unavailable")
	sink := &failingSink{MemorySink: NewMemorySink(1), err: failure}
	done, _ := runAdapter(t, source, sink)
	request := fakeSnapshot{snapshot: herdr.Snapshot{Agents: []herdr.Agent{nativeAgent("pane", "native", "idle")}}, applied: make(chan error, 1)}
	select {
	case source.snapshots <- request:
	case <-time.After(3 * time.Second):
		t.Fatal("snapshot was not consumed")
	}
	if err := receiveError(t, done); !errors.Is(err, failure) || !strings.Contains(err.Error(), "session.started") {
		t.Fatalf("publish error not surfaced: %v", err)
	}
	select {
	case err := <-request.applied:
		if err == nil {
			t.Fatal("source callback lost publish failure")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("source callback remained blocked")
	}
}
