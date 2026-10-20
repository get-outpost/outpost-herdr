package adapter

import (
	"reflect"
	"testing"

	"github.com/get-outpost/outpost-go"
	"github.com/get-outpost/outpost-herdr/internal/herdr"
)

func TestStartupWaitsForSnapshotThenEndsMissingAndUpdatesLiveWithoutStart(t *testing.T) {
	source, sink := newFakeSource(), NewMemorySink(1)
	bridge := New(source, sink, "test-installation")
	bridge.PreviouslyLive = []string{"gone", "current"}
	runConfiguredAdapter(t, bridge, source)
	if events := sink.Events(); len(events) != 0 {
		t.Fatalf("declared offline exits before the first snapshot: %#v", events)
	}
	agent := nativeAgent("pane", "current", "unknown")
	agent.Cwd, agent.Title = "/current-worktree", "Resumed task"
	source.apply(t, agent)
	want := []Event{
		{Type: outpost.SessionEnded, SessionID: "gone"},
		{Type: outpost.SessionUpdated, SessionID: "current", Payload: map[string]any{"cwd": "/current-worktree", "title": "Resumed task"}},
	}
	if events := sink.Events(); !reflect.DeepEqual(events, want) {
		t.Fatalf("startup reconciliation = %#v, want %#v", events, want)
	}
	source.apply(t, agent)
	if events := sink.Events(); !reflect.DeepEqual(events, want) {
		t.Fatalf("startup reconciliation repeated on refresh: %#v", events)
	}
	source.apply(t)
	want = append(want, Event{Type: outpost.SessionEnded, SessionID: "current"})
	if events := sink.Events(); !reflect.DeepEqual(events, want) {
		t.Fatalf("restarted session exit = %#v", events)
	}
}

func TestStartupUpdatesPublishedFallbackWithoutGraceThenStartsNativePromotion(t *testing.T) {
	source, sink := newFakeSource(), NewMemorySink(1)
	agent := herdr.Agent{PaneID: "pane", TerminalID: "terminal", Agent: outpost.ProviderOMP, AgentStatus: "unknown"}
	fallback, _ := SessionID("test-installation", agent)
	bridge := New(source, sink, "test-installation")
	bridge.PreviouslyLive = []string{fallback}
	runConfiguredAdapter(t, bridge, source)
	source.apply(t, agent)
	want := []Event{{Type: outpost.SessionUpdated, SessionID: fallback, Payload: map[string]any{"cwd": "", "title": outpost.ProviderOMP}}}
	if events := sink.Events(); !reflect.DeepEqual(events, want) {
		t.Fatalf("previously published fallback was delayed again: %#v", events)
	}
	agent.AgentSession = &herdr.SessionRef{Kind: "id", Value: "native"}
	source.apply(t, agent)
	want = append(want,
		Event{Type: outpost.SessionDeleted, SessionID: fallback},
		Event{Type: outpost.SessionStarted, SessionID: "native", Payload: map[string]any{"provider": outpost.ProviderOMP, "cwd": "", "title": outpost.ProviderOMP}},
	)
	if events := sink.Events(); !reflect.DeepEqual(events, want) {
		t.Fatalf("restored fallback promotion = %#v", events)
	}
}
