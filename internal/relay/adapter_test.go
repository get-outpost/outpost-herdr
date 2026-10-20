package relay

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/get-outpost/outpost-go"
	"github.com/get-outpost/outpost-herdr/internal/adapter"
	"github.com/get-outpost/outpost-herdr/internal/herdr"
)

type fakeSnapshot struct {
	agents  []herdr.Agent
	applied chan error
}

type fakeAction struct {
	pane   string
	text   string
	keys   []string
	cursor int64
}

// This source never launches a terminal or invokes herdr. Like the daemon's
// source wrapper, it refreshes relay ownership before the adapter consumes each
// authoritative snapshot.
type fakeSource struct {
	mu        sync.Mutex
	agents    map[string]herdr.Agent
	sink      *Sink
	state     *memoryState
	promptErr error
	snapshots chan fakeSnapshot
	actions   chan fakeAction
}

func newSource(sink *Sink, state *memoryState) *fakeSource {
	return &fakeSource{sink: sink, state: state, agents: make(map[string]herdr.Agent), snapshots: make(chan fakeSnapshot), actions: make(chan fakeAction, 16)}
}

func (s *fakeSource) Watch(ctx context.Context, consume func(herdr.Snapshot) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case request := <-s.snapshots:
			ids := make([]string, 0, len(request.agents))
			for _, agent := range request.agents {
				id, err := adapter.SessionID("test-installation", agent)
				if err != nil {
					request.applied <- err
					return err
				}
				ids = append(ids, id)
			}
			s.sink.SetLocalSessions(ids)
			err := consume(herdr.Snapshot{Agents: request.agents})
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
	agent, ok := s.agents[pane]
	if !ok {
		return herdr.Agent{}, errors.New("pane has no agent")
	}
	return agent, nil
}

func (s *fakeSource) Prompt(_ context.Context, pane, text string) error {
	cursor, _ := s.state.Cursor()
	s.actions <- fakeAction{pane: pane, text: text, cursor: cursor}
	return s.promptErr
}

func (s *fakeSource) SendKeys(_ context.Context, pane string, keys ...string) error {
	cursor, _ := s.state.Cursor()
	s.actions <- fakeAction{pane: pane, keys: append([]string(nil), keys...), cursor: cursor}
	return nil
}

func (s *fakeSource) apply(t *testing.T, agents ...herdr.Agent) {
	t.Helper()
	s.mu.Lock()
	s.agents = make(map[string]herdr.Agent, len(agents))
	for _, agent := range agents {
		s.agents[agent.PaneID] = agent
	}
	s.mu.Unlock()
	request := fakeSnapshot{agents: agents, applied: make(chan error, 1)}
	select {
	case s.snapshots <- request:
	case <-time.After(3 * time.Second):
		t.Fatal("adapter did not request snapshot")
	}
	if err := receive(t, request.applied); err != nil {
		t.Fatal(err)
	}
}

func runRealAdapter(t *testing.T, source *fakeSource, previous []string) (<-chan error, context.CancelFunc) {
	t.Helper()
	a := adapter.New(source, source.sink, "test-installation")
	a.PreviouslyLive = previous
	a.SessionGracePeriod = 0
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	t.Cleanup(cancel)
	return done, cancel
}

func localAgent() herdr.Agent {
	return herdr.Agent{
		PaneID: "fake-pane", TerminalID: "fake-terminal", Agent: outpost.ProviderOMP, AgentStatus: outpost.StatusIdle,
		Cwd: "/project", Title: "Relay integration", AgentSession: &herdr.SessionRef{Kind: "id", Value: "native", Source: outpost.ProviderOMP, Agent: outpost.ProviderOMP},
	}
}

func TestSDKCommandsReachRealAdapterAndRestartReconcilesMissingSessions(t *testing.T) {
	state := newState(20, true)
	state.live["gone"] = struct{}{}
	state.live["native"] = struct{}{}
	previous := state.LiveSessions()
	server := newServer(t, serverOptions{})
	s, conn := startedSink(t, server, state, Options{})
	source := newSource(s, state)
	done, cancel := runRealAdapter(t, source, previous)
	source.apply(t, localAgent())
	ended, started := receive(t, server.posts), receive(t, server.posts)
	if ended.Type != outpost.SessionEnded || ended.SessionID != "gone" {
		t.Fatalf("missing restart ended event: %#v", ended)
	}
	if started.Type != outpost.SessionStarted || started.SessionID != "native" {
		t.Fatalf("live session not reannounced: %#v", started)
	}
	if !strings.Contains(string(started.Payload), `"provider":"omp"`) {
		t.Fatalf("provider not preserved: %s", started.Payload)
	}

	sendFrame(t, conn, command(21, outpost.CommandMessage, "native"))
	message := receive(t, source.actions)
	if message.pane != "fake-pane" || message.text != "hello" || message.cursor != 21 {
		t.Fatalf("message not durably consumed before execution: %#v", message)
	}
	sendFrame(t, conn, command(22, outpost.CommandInterrupt, "native"))
	interrupt := receive(t, source.actions)
	if interrupt.pane != "fake-pane" || !reflect.DeepEqual(interrupt.keys, []string{"esc"}) || interrupt.cursor != 22 {
		t.Fatalf("interrupt execution = %#v", interrupt)
	}
	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("adapter Run = %v", err)
	}
	if err := shutdown(t, s); err != nil {
		t.Fatal(err)
	}
	if live := state.LiveSessions(); !reflect.DeepEqual(live, []string{"native"}) {
		t.Fatalf("restart live set = %v", live)
	}
}

func TestSDKCommandExecutionFailureIsNotRetried(t *testing.T) {
	state := newState(0, true)
	server := newServer(t, serverOptions{})
	s, conn := startedSink(t, server, state, Options{})
	source := newSource(s, state)
	source.promptErr = errors.New("fake prompt rejected")
	done, _ := runRealAdapter(t, source, nil)
	source.apply(t, localAgent())
	sendFrame(t, conn, command(1, outpost.CommandMessage, "native"))
	if action := receive(t, source.actions); action.cursor != 1 {
		t.Fatal("failed command was not durably consumed")
	}
	err := receive(t, done)
	if err == nil || !strings.Contains(err.Error(), "command-1") || !strings.Contains(err.Error(), "fake prompt rejected") {
		t.Fatalf("command failure = %v", err)
	}
	if err := shutdown(t, s); err != nil {
		t.Fatal(err)
	}
	select {
	case action := <-source.actions:
		t.Fatalf("failed command retried: %#v", action)
	default:
	}
}
