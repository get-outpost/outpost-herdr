package main

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/get-outpost/outpost-go"
	"github.com/get-outpost/outpost-herdr/internal/adapter"
	"github.com/get-outpost/outpost-herdr/internal/herdr"
)

type snapshotSource struct{ snapshots []herdr.Snapshot }

func (s snapshotSource) Watch(ctx context.Context, consume func(herdr.Snapshot) error) error {
	for _, snapshot := range s.snapshots {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := consume(snapshot); err != nil {
			return err
		}
	}
	return nil
}

func (snapshotSource) GetAgent(context.Context, string) (herdr.Agent, error) {
	return herdr.Agent{}, errors.New("unexpected control during observation")
}
func (snapshotSource) Prompt(context.Context, string, string) error {
	return errors.New("unexpected control during observation")
}
func (snapshotSource) SendKeys(context.Context, string, ...string) error {
	return errors.New("unexpected control during observation")
}

func TestPaneScopeDoesNotPublishOrRetainOtherAgents(t *testing.T) {
	selected := herdr.Agent{PaneID: "owned", Agent: outpost.ProviderOMP, AgentStatus: outpost.StatusIdle, Cwd: "/scratch", Title: "Disposable test", AgentSession: &herdr.SessionRef{Kind: "id", Value: "owned-session"}}
	foreign := herdr.Agent{PaneID: "existing", Agent: outpost.ProviderClaude, AgentStatus: outpost.StatusWorking, AgentSession: &herdr.SessionRef{Kind: "id", Value: "existing-session"}}
	source := scopedSource{Source: snapshotSource{snapshots: []herdr.Snapshot{
		{Agents: []herdr.Agent{foreign, selected}},
		{Agents: []herdr.Agent{foreign}},
	}}, namespace: "isolated", pane: "owned"}
	sink := adapter.NewMemorySink(0)
	if err := adapter.New(source, sink, "isolated").Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []adapter.Event{
		{Type: outpost.SessionStarted, SessionID: "owned-session", Payload: map[string]any{"provider": outpost.ProviderOMP, "cwd": "/scratch", "title": "Disposable test"}},
		{Type: outpost.SessionUpdated, SessionID: "owned-session", Payload: map[string]any{"status": outpost.StatusIdle}},
		{Type: outpost.SessionEnded, SessionID: "owned-session"},
	}
	if events := sink.Events(); !reflect.DeepEqual(events, want) {
		t.Fatalf("scoped session lifecycle = %#v, want %#v", events, want)
	}
}
