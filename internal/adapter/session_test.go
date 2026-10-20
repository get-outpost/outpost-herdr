package adapter

import (
	"context"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/get-outpost/outpost-herdr/internal/herdr"
)

func TestSessionGracePublishesOnlyNativeIDWhenReferenceArrives(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source, sink := newFakeSource(), NewMemorySink(1)
		runAdapter(t, source, sink)
		agent := herdr.Agent{PaneID: "pane", TerminalID: "terminal", Agent: "claude", AgentStatus: "unknown"}
		source.apply(t, agent)
		time.Sleep(9 * time.Second)
		synctest.Wait()
		if events := sink.Events(); len(events) != 0 {
			t.Fatalf("fallback published inside default grace period: %#v", events)
		}
		agent.AgentSession = &herdr.SessionRef{Kind: "id", Value: "native"}
		source.apply(t, agent)
		time.Sleep(20 * time.Second)
		synctest.Wait()
		want := []Event{{Type: "session.started", SessionID: "native", Payload: map[string]any{"provider": "claude", "cwd": "", "title": "claude"}}}
		if events := sink.Events(); !reflect.DeepEqual(events, want) {
			t.Fatalf("native reference did not replace pending fallback: %#v", events)
		}
	})
}

func TestSessionGraceExpiresWithoutAnotherSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source, sink := newFakeSource(), NewMemorySink(1)
		bridge := New(source, sink, "test-installation")
		bridge.SessionGracePeriod = 2 * time.Second
		runConfiguredAdapter(t, bridge, source)
		agent := herdr.Agent{PaneID: "pane", TerminalID: "terminal", Agent: "claude", AgentStatus: "unknown"}
		source.apply(t, agent)
		time.Sleep(2*time.Second - time.Nanosecond)
		synctest.Wait()
		if events := sink.Events(); len(events) != 0 {
			t.Fatalf("fallback published before configured deadline: %#v", events)
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		id, _ := SessionID("test-installation", agent)
		want := []Event{{Type: "session.started", SessionID: id, Payload: map[string]any{"provider": "claude", "cwd": "", "title": "claude"}}}
		if events := sink.Events(); !reflect.DeepEqual(events, want) {
			t.Fatalf("deadline did not publish fallback without a new snapshot: %#v", events)
		}
	})
}

func TestSessionGraceReleasesOnFirstKnownStatus(t *testing.T) {
	for _, state := range []string{"working", "idle", "blocked", "done"} {
		t.Run(state, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				source, sink := newFakeSource(), NewMemorySink(1)
				runAdapter(t, source, sink)
				agent := herdr.Agent{PaneID: "pane", TerminalID: "terminal", Agent: "claude", AgentStatus: "unknown"}
				source.apply(t, agent)
				time.Sleep(time.Second)
				agent.AgentStatus = state
				source.apply(t, agent)
				id, _ := SessionID("test-installation", agent)
				wantStatus := state
				if state == "done" {
					wantStatus = "idle"
				}
				want := []Event{
					{Type: "session.started", SessionID: id, Payload: map[string]any{"provider": "claude", "cwd": "", "title": "claude"}},
					{Type: "session.updated", SessionID: id, Payload: map[string]any{"status": wantStatus}},
				}
				if events := sink.Events(); !reflect.DeepEqual(events, want) {
					t.Fatalf("known status did not end grace: %#v", events)
				}
				time.Sleep(20 * time.Second)
				synctest.Wait()
				if events := sink.Events(); !reflect.DeepEqual(events, want) {
					t.Fatalf("expired timer restarted an already published session: %#v", events)
				}
			})
		})
	}
}

func TestSessionGraceKeepsDeadlineAcrossRefreshAndPaneMove(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source, sink := newFakeSource(), NewMemorySink(1)
		runAdapter(t, source, sink)
		agent := herdr.Agent{PaneID: "before", TerminalID: "terminal", Agent: "omp", AgentStatus: "unknown"}
		source.apply(t, agent)
		time.Sleep(5 * time.Second)
		agent.PaneID, agent.Title = "after", "Moved task"
		source.apply(t, agent)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		id, _ := SessionID("test-installation", agent)
		want := []Event{{Type: "session.started", SessionID: id, Payload: map[string]any{"provider": "omp", "cwd": "", "title": "Moved task"}}}
		if events := sink.Events(); !reflect.DeepEqual(events, want) {
			t.Fatalf("refresh reset deadline or published stale metadata: %#v", events)
		}
	})
}

func TestSessionGraceForgetsAgentsThatDisappearBeforePublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source, sink := newFakeSource(), NewMemorySink(1)
		runAdapter(t, source, sink)
		agent := herdr.Agent{PaneID: "pane", TerminalID: "terminal", Agent: "claude", AgentStatus: "unknown"}
		source.apply(t, agent)
		time.Sleep(5 * time.Second)
		source.apply(t)
		time.Sleep(20 * time.Second)
		synctest.Wait()
		if events := sink.Events(); len(events) != 0 {
			t.Fatalf("unpublished agent emitted a start or an end: %#v", events)
		}
	})
}

func TestSessionGraceAcceptsNativePathImmediately(t *testing.T) {
	source, sink := newFakeSource(), NewMemorySink(1)
	runAdapter(t, source, sink)
	agent := herdr.Agent{PaneID: "pane", Agent: "omp", AgentStatus: "unknown", AgentSession: &herdr.SessionRef{Kind: "path", Source: "omp", Agent: "omp", Value: "/sessions/native.jsonl"}}
	source.apply(t, agent)
	id, _ := SessionID("test-installation", agent)
	want := []Event{{Type: "session.started", SessionID: id, Payload: map[string]any{"provider": "omp", "cwd": "", "title": "omp"}}}
	if events := sink.Events(); !reflect.DeepEqual(events, want) {
		t.Fatalf("native path reference was unnecessarily delayed: %#v", events)
	}
}

func TestSessionGraceCanBeDisabledAndRejectsNegativeDuration(t *testing.T) {
	source, sink := newFakeSource(), NewMemorySink(1)
	bridge := New(source, sink, "test-installation")
	bridge.SessionGracePeriod = -time.Second
	if err := bridge.Run(context.Background()); err == nil {
		t.Fatal("negative grace period accepted")
	}
	bridge.SessionGracePeriod = 0
	runConfiguredAdapter(t, bridge, source)
	agent := herdr.Agent{PaneID: "pane", Agent: "claude", AgentStatus: "unknown"}
	source.apply(t, agent)
	id, _ := SessionID("test-installation", agent)
	want := []Event{{Type: "session.started", SessionID: id, Payload: map[string]any{"provider": "claude", "cwd": "", "title": "claude"}}}
	if events := sink.Events(); !reflect.DeepEqual(events, want) {
		t.Fatalf("disabled grace still delayed publication: %#v", events)
	}
}

func TestNativePromotionTombstonesPublishedFallbackBeforeStartingNative(t *testing.T) {
	for _, kind := range []string{"terminal", "pane", "path"} {
		t.Run(kind, func(t *testing.T) {
			source, sink := newFakeSource(), NewMemorySink(1)
			runAdapter(t, source, sink)
			agent := herdr.Agent{PaneID: "pane", TerminalID: "terminal", Agent: "claude", AgentStatus: "idle"}
			if kind == "pane" {
				agent.TerminalID = ""
			} else if kind == "path" {
				agent.AgentSession = &herdr.SessionRef{Kind: "path", Value: "/sessions/task.jsonl"}
			}
			fallback, _ := SessionID("test-installation", agent)
			source.apply(t, agent)
			if kind == "terminal" {
				agent.PaneID = "moved-pane"
			}
			agent.AgentSession = &herdr.SessionRef{Kind: "id", Value: "native"}
			source.apply(t, agent)
			source.apply(t, agent)
			source.apply(t)
			want := []Event{
				{Type: "session.started", SessionID: fallback, Payload: map[string]any{"provider": "claude", "cwd": "", "title": "claude"}},
				{Type: "session.updated", SessionID: fallback, Payload: map[string]any{"status": "idle"}},
				{Type: "session.deleted", SessionID: fallback},
				{Type: "session.started", SessionID: "native", Payload: map[string]any{"provider": "claude", "cwd": "", "title": "claude"}},
				{Type: "session.updated", SessionID: "native", Payload: map[string]any{"status": "idle"}},
				{Type: "session.ended", SessionID: "native"},
			}
			if events := sink.Events(); !reflect.DeepEqual(events, want) {
				t.Fatalf("promotion/exit lifecycle = %#v, want %#v", events, want)
			}
		})
	}
}

func TestNativeReplacementDoesNotTombstoneUnrelatedFallback(t *testing.T) {
	source, sink := newFakeSource(), NewMemorySink(1)
	runAdapter(t, source, sink)
	fallback := herdr.Agent{PaneID: "pane", TerminalID: "old-terminal", Agent: "claude", AgentStatus: "idle"}
	source.apply(t, fallback)
	replacement := nativeAgent("pane", "new-native", "idle")
	source.apply(t, replacement)
	id, _ := SessionID("test-installation", fallback)
	want := []Event{
		{Type: "session.started", SessionID: id, Payload: map[string]any{"provider": "claude", "cwd": "", "title": "claude"}},
		{Type: "session.updated", SessionID: id, Payload: map[string]any{"status": "idle"}},
		{Type: "session.ended", SessionID: id},
		{Type: "session.started", SessionID: "new-native", Payload: map[string]any{"provider": "claude", "cwd": "", "title": "claude"}},
		{Type: "session.updated", SessionID: "new-native", Payload: map[string]any{"status": "idle"}},
	}
	if events := sink.Events(); !reflect.DeepEqual(events, want) {
		t.Fatalf("replacement incorrectly treated as promotion: %#v", events)
	}
}
