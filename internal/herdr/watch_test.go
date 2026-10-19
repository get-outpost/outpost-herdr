package herdr

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

type fakeCall struct {
	conn    net.Conn
	request fakeRequest
	closed  chan struct{}
}

func scriptedSocket(t *testing.T) (*fakeSocket, <-chan *fakeCall) {
	t.Helper()
	calls := make(chan *fakeCall, 32)
	server := newFakeSocket(t, func(conn net.Conn, request fakeRequest) {
		call := &fakeCall{conn: conn, request: request, closed: make(chan struct{})}
		calls <- call
		io.Copy(io.Discard, conn)
		close(call.closed)
	})
	return server, calls
}

func nextCall(t *testing.T, calls <-chan *fakeCall, method string) *fakeCall {
	t.Helper()
	select {
	case call := <-calls:
		if call.request.Method != method {
			t.Fatalf("method = %q, want %q", call.request.Method, method)
		}
		return call
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", method)
		return nil
	}
}

func reply(t *testing.T, call *fakeCall, result any) {
	t.Helper()
	if err := respond(call.conn, call.request.ID, result); err != nil {
		t.Fatal(err)
	}
}

func acknowledge(t *testing.T, call *fakeCall) {
	t.Helper()
	reply(t, call, map[string]string{"type": "subscription_started"})
}

func sendEvent(t *testing.T, call *fakeCall, kind string) {
	t.Helper()
	if err := json.NewEncoder(call.conn).Encode(map[string]any{"event": kind, "data": map[string]any{"pane_id": "w1:p1", "agent_status": "idle"}}); err != nil {
		t.Fatal(err)
	}
}

func requireSubscriptions(t *testing.T, call *fakeCall, want ...string) {
	t.Helper()
	var params struct {
		Subscriptions []subscription `json:"subscriptions"`
	}
	if err := json.Unmarshal(call.request.Params, &params); err != nil {
		t.Fatal(err)
	}
	var got []string
	globals := map[string]bool{}
	for _, entry := range params.Subscriptions {
		if entry.Type == "pane.agent_status_changed" {
			if entry.PaneID == "" {
				t.Fatal("unscoped status subscription")
			}
			got = append(got, entry.PaneID)
		} else {
			if entry.PaneID != "" {
				t.Fatalf("global %s incorrectly scoped to %s", entry.Type, entry.PaneID)
			}
			globals[entry.Type] = true
		}
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("status subscriptions = %v; want %v", got, want)
	}
	for _, kind := range []string{"pane.created", "pane.closed", "pane.updated", "pane.moved", "pane.exited", "pane.agent_detected", "tab.closed", "workspace.closed"} {
		if !globals[kind] {
			t.Fatalf("missing lifecycle subscription %s", kind)
		}
	}
}

func watchClient(path string) *Client {
	client := NewClient(path)
	client.RequestTimeout = time.Second
	client.ReconnectMin = time.Millisecond
	client.ReconnectMax = 4 * time.Millisecond
	client.ReconcileInterval = time.Hour
	return client
}

func runWatch(t *testing.T, client *Client, consume func(Snapshot) error) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() { result <- client.Watch(ctx, consume) }()
	return cancel, result
}

func nextSnapshot(t *testing.T, snapshots <-chan Snapshot) Snapshot {
	t.Helper()
	select {
	case snapshot := <-snapshots:
		return snapshot
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for consumer snapshot")
		return Snapshot{}
	}
}

func TestWatchResubscribesTopologyBeforePostAckSnapshot(t *testing.T) {
	server, calls := scriptedSocket(t)
	snapshots := make(chan Snapshot, 8)
	cancel, result := runWatch(t, watchClient(server.path), func(snapshot Snapshot) error { snapshots <- snapshot; return nil })
	global := nextCall(t, calls, "events.subscribe")
	requireSubscriptions(t, global)
	acknowledge(t, global)
	a := testAgent("w1:p1", "idle")
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot(a)))
	statusA := nextCall(t, calls, "events.subscribe")
	requireSubscriptions(t, statusA, a.PaneID)
	select {
	case <-snapshots:
		t.Fatal("published discovery snapshot before status subscription acknowledgement")
	default:
	}
	acknowledge(t, statusA)
	a.AgentStatus = "working"
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot(a)))
	if got := nextSnapshot(t, snapshots); len(got.Agents) != 1 || got.Agents[0].AgentStatus != "working" {
		t.Fatalf("post-ack snapshot = %#v", got)
	}
	awaitSignal(t, global.closed)

	// A new agent is discovered through a global event. Its unguarded discovery
	// snapshot must not reach the consumer, and its post-ack status wins.
	sendEvent(t, statusA, "pane.created")
	b := testAgent("w1:p2", "idle")
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot(a, b)))
	statusAB := nextCall(t, calls, "events.subscribe")
	requireSubscriptions(t, statusAB, a.PaneID, b.PaneID)
	acknowledge(t, statusAB)
	b.AgentStatus = "blocked"
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot(a, b)))
	if got := nextSnapshot(t, snapshots); len(got.Agents) != 2 || got.Agents[1].AgentStatus != "blocked" {
		t.Fatalf("new-agent post-ack snapshot = %#v", got)
	}
	awaitSignal(t, statusA.closed)

	sendEvent(t, statusAB, "pane_closed")
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot(b)))
	statusB := nextCall(t, calls, "events.subscribe")
	requireSubscriptions(t, statusB, b.PaneID)
	acknowledge(t, statusB)
	b.AgentStatus = "done"
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot(b)))
	if got := nextSnapshot(t, snapshots); len(got.Agents) != 1 || got.Agents[0].AgentStatus != "done" {
		t.Fatalf("removed-pane post-ack snapshot = %#v", got)
	}
	cancel()
	if err := awaitError(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch = %v", err)
	}
	awaitSignal(t, statusB.closed)
}

func TestWatchKeepsReadingAndRefreshesWhenDirtyDuringSnapshot(t *testing.T) {
	server, calls := scriptedSocket(t)
	snapshots := make(chan Snapshot, 8)
	cancel, result := runWatch(t, watchClient(server.path), func(snapshot Snapshot) error { snapshots <- snapshot; return nil })
	stream := nextCall(t, calls, "events.subscribe")
	acknowledge(t, stream)
	pending := nextCall(t, calls, "session.snapshot")
	// The snapshot connection is deliberately stalled while its independent
	// subscription delivers an invalidation. No event payload is replayed.
	sendEvent(t, stream, "pane.agent_status_changed")
	reply(t, pending, snapshotResult(testSnapshot()))
	nextSnapshot(t, snapshots)
	refresh := nextCall(t, calls, "session.snapshot")
	updated := testSnapshot()
	updated.Version = "after-invalidated-read"
	reply(t, refresh, snapshotResult(updated))
	if got := nextSnapshot(t, snapshots); got.Version != updated.Version {
		t.Fatalf("lost invalidation during read: %#v", got)
	}
	cancel()
	if err := awaitError(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestWatchReconnectsAndResynchronizesAfterLossOrEOF(t *testing.T) {
	for _, loss := range []bool{false, true} {
		t.Run(map[bool]string{false: "eof", true: "events_lost"}[loss], func(t *testing.T) {
			server, calls := scriptedSocket(t)
			snapshots := make(chan Snapshot, 8)
			cancel, result := runWatch(t, watchClient(server.path), func(snapshot Snapshot) error { snapshots <- snapshot; return nil })
			first := nextCall(t, calls, "events.subscribe")
			acknowledge(t, first)
			reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot()))
			nextSnapshot(t, snapshots)
			if loss {
				json.NewEncoder(first.conn).Encode(map[string]any{"id": first.request.ID, "error": map[string]string{"code": "events_lost", "message": "subscriber fell behind"}})
			} else {
				first.conn.Close()
			}
			reconnected := nextCall(t, calls, "events.subscribe")
			requireSubscriptions(t, reconnected)
			acknowledge(t, reconnected)
			agent := testAgent("w1:p3", "working")
			reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot(agent)))
			status := nextCall(t, calls, "events.subscribe")
			requireSubscriptions(t, status, agent.PaneID)
			acknowledge(t, status)
			agent.AgentStatus = "done"
			reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot(agent)))
			if got := nextSnapshot(t, snapshots); len(got.Agents) != 1 || got.Agents[0].AgentStatus != "done" {
				t.Fatalf("recovered state = %#v", got)
			}
			cancel()
			if err := awaitError(t, result); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}

func TestWatchRefreshesTopologyAfterEntireSubscriptionRejected(t *testing.T) {
	server, calls := scriptedSocket(t)
	snapshots := make(chan Snapshot, 8)
	cancel, result := runWatch(t, watchClient(server.path), func(snapshot Snapshot) error { snapshots <- snapshot; return nil })
	globals := nextCall(t, calls, "events.subscribe")
	acknowledge(t, globals)
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot(testAgent("vanished", "idle"))))
	rejected := nextCall(t, calls, "events.subscribe")
	requireSubscriptions(t, rejected, "vanished")
	json.NewEncoder(rejected.conn).Encode(map[string]any{"id": rejected.request.ID, "error": map[string]string{"code": "pane_not_found", "message": "pane vanished"}})
	retry := nextCall(t, calls, "events.subscribe")
	requireSubscriptions(t, retry)
	acknowledge(t, retry)
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot()))
	if got := nextSnapshot(t, snapshots); len(got.Agents) != 0 {
		t.Fatalf("published rejected topology: %#v", got)
	}
	cancel()
	if err := awaitError(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestWatchPeriodicallyReconcilesWithoutEvents(t *testing.T) {
	server, calls := scriptedSocket(t)
	client := watchClient(server.path)
	client.ReconcileInterval = 10 * time.Millisecond
	snapshots := make(chan Snapshot, 8)
	cancel, result := runWatch(t, client, func(snapshot Snapshot) error { snapshots <- snapshot; return nil })
	stream := nextCall(t, calls, "events.subscribe")
	acknowledge(t, stream)
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot()))
	nextSnapshot(t, snapshots)
	// No push event is sent for this newly discovered pane.
	agent := testAgent("w1:p4", "idle")
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot(agent)))
	status := nextCall(t, calls, "events.subscribe")
	requireSubscriptions(t, status, agent.PaneID)
	acknowledge(t, status)
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot(agent)))
	if got := nextSnapshot(t, snapshots); len(got.Agents) != 1 || got.Agents[0].PaneID != agent.PaneID {
		t.Fatalf("periodic reconciliation = %#v", got)
	}
	cancel()
	if err := awaitError(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestWatchConsumerErrorReturnsUnchangedAndClosesStream(t *testing.T) {
	server, calls := scriptedSocket(t)
	consumerFailure := errors.New("sink unavailable")
	_, result := runWatch(t, watchClient(server.path), func(Snapshot) error { return consumerFailure })
	stream := nextCall(t, calls, "events.subscribe")
	acknowledge(t, stream)
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot()))
	if err := awaitError(t, result); err != consumerFailure {
		t.Fatalf("consumer error was replaced: %v", err)
	}
	awaitSignal(t, stream.closed)
	select {
	case call := <-calls:
		t.Fatalf("consumer failure reconnected: %s", call.request.Method)
	default:
	}
}

func TestWatchCancellationClosesAcknowledgementAndSnapshotIO(t *testing.T) {
	for _, phase := range []string{"ack", "snapshot", "stream"} {
		t.Run(phase, func(t *testing.T) {
			server, calls := scriptedSocket(t)
			snapshots := make(chan Snapshot, 8)
			cancel, result := runWatch(t, watchClient(server.path), func(snapshot Snapshot) error { snapshots <- snapshot; return nil })
			stream := nextCall(t, calls, "events.subscribe")
			var pending *fakeCall
			if phase != "ack" {
				acknowledge(t, stream)
				pending = nextCall(t, calls, "session.snapshot")
				if phase == "stream" {
					reply(t, pending, snapshotResult(testSnapshot()))
					nextSnapshot(t, snapshots)
				}
			}
			cancel()
			if err := awaitError(t, result); !errors.Is(err, context.Canceled) {
				t.Fatalf("Watch = %v", err)
			}
			awaitSignal(t, stream.closed)
			if pending != nil {
				awaitSignal(t, pending.closed)
			}
		})
	}
}

func TestOpenStreamValidatesAcknowledgementIDTypeAndFrames(t *testing.T) {
	cases := []struct {
		name     string
		response func(string) []byte
	}{
		{"wrong_id", func(string) []byte {
			return []byte("{\"id\":\"wrong\",\"result\":{\"type\":\"subscription_started\"}}\n")
		}},
		{"array_result", func(id string) []byte {
			raw, _ := json.Marshal(map[string]any{"id": id, "result": []any{}})
			return append(raw, '\n')
		}},
		{"wrong_type", func(id string) []byte {
			raw, _ := json.Marshal(map[string]any{"id": id, "result": map[string]string{"type": "ok"}})
			return append(raw, '\n')
		}},
		{"partial", func(id string) []byte {
			raw, _ := json.Marshal(map[string]any{"id": id, "result": map[string]string{"type": "subscription_started"}})
			return raw
		}},
		{"malformed_error", func(id string) []byte {
			raw, _ := json.Marshal(map[string]any{"id": id, "error": "no"})
			return append(raw, '\n')
		}},
		{"oversized", func(string) []byte { return append(make([]byte, MaxFrameBytes+1), '\n') }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newFakeSocket(t, func(conn net.Conn, request fakeRequest) { conn.Write(tc.response(request.ID)) })
			stream, err := watchClient(server.path).openStream(context.Background(), nil)
			if stream != nil {
				stream.close()
			}
			if err == nil {
				t.Fatal("accepted invalid subscription acknowledgement")
			}
		})
	}
}

func TestOpenStreamRejectsPartialEventFrame(t *testing.T) {
	server := newFakeSocket(t, func(conn net.Conn, request fakeRequest) {
		respond(conn, request.ID, map[string]string{"type": "subscription_started"})
		io.WriteString(conn, `{"event":"pane.created","data":{}}`)
	})
	stream, err := watchClient(server.path).openStream(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.close()
	awaitSignal(t, stream.done)
	if err := stream.failure(nil); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("partial event error = %v", err)
	}
	if stream.sequence.Load() != 0 {
		t.Fatal("partial event was treated as an invalidation")
	}
}

func TestWatchRetriesMissingSocketUntilServerAppears(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "outpost-herdr-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	path := filepath.Join(directory, "later.sock")
	client := watchClient(path)
	client.ReconnectMin = 5 * time.Millisecond
	client.ReconnectMax = 10 * time.Millisecond
	consumerFailure := errors.New("stop after recovery")
	_, result := runWatch(t, client, func(Snapshot) error { return consumerFailure })
	// Allow actual failed dials before the listener is created; Watch must not
	// return on ENOENT or spin without the configured bounded backoff.
	select {
	case err := <-result:
		t.Fatalf("Watch returned on initial connection failure: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	listenFakeSocket(t, path, func(conn net.Conn, request fakeRequest) {
		switch request.Method {
		case "events.subscribe":
			respond(conn, request.ID, map[string]string{"type": "subscription_started"})
			io.Copy(io.Discard, conn)
		case "session.snapshot":
			respond(conn, request.ID, snapshotResult(testSnapshot()))
		}
	})
	if err := awaitError(t, result); err != consumerFailure {
		t.Fatalf("did not recover initial connect failure: %v", err)
	}
}

func TestWatchSubscribesUndetectedPanesBeforeAgentsAppear(t *testing.T) {
	server, calls := scriptedSocket(t)
	snapshots := make(chan Snapshot, 8)
	cancel, result := runWatch(t, watchClient(server.path), func(snapshot Snapshot) error { snapshots <- snapshot; return nil })
	globals := nextCall(t, calls, "events.subscribe")
	acknowledge(t, globals)
	shell := testSnapshot()
	shell.Panes = []Pane{{PaneID: "w1:p9", TerminalID: "terminal-shell"}}
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(shell))
	status := nextCall(t, calls, "events.subscribe")
	requireSubscriptions(t, status, "w1:p9")
	acknowledge(t, status)
	agent := testAgent("w1:p9", "working")
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot(agent)))
	if got := nextSnapshot(t, snapshots); len(got.Agents) != 1 || got.Agents[0].PaneID != agent.PaneID {
		t.Fatalf("agent discovered during acknowledgement = %#v", got)
	}
	cancel()
	if err := awaitError(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestWatchSnapshotFailureNeverPublishesEmptyAgents(t *testing.T) {
	server, calls := scriptedSocket(t)
	snapshots := make(chan Snapshot, 8)
	cancel, result := runWatch(t, watchClient(server.path), func(snapshot Snapshot) error { snapshots <- snapshot; return nil })
	globals := nextCall(t, calls, "events.subscribe")
	acknowledge(t, globals)
	agent := testAgent("w1:p1", "idle")
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot(agent)))
	status := nextCall(t, calls, "events.subscribe")
	acknowledge(t, status)
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot(agent)))
	nextSnapshot(t, snapshots)
	sendEvent(t, status, "pane.updated")
	failed := nextCall(t, calls, "session.snapshot")
	if err := json.NewEncoder(failed.conn).Encode(map[string]any{"id": failed.request.ID, "error": map[string]string{"code": "server_busy", "message": "snapshot unavailable"}}); err != nil {
		t.Fatal(err)
	}
	reconnected := nextCall(t, calls, "events.subscribe")
	requireSubscriptions(t, reconnected)
	acknowledge(t, reconnected)
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot(agent)))
	resubscribed := nextCall(t, calls, "events.subscribe")
	requireSubscriptions(t, resubscribed, agent.PaneID)
	acknowledge(t, resubscribed)
	agent.AgentStatus = "working"
	reply(t, nextCall(t, calls, "session.snapshot"), snapshotResult(testSnapshot(agent)))
	if got := nextSnapshot(t, snapshots); len(got.Agents) != 1 || got.Agents[0].AgentStatus != "working" {
		t.Fatalf("snapshot failure fabricated empty state: %#v", got)
	}
	cancel()
	if err := awaitError(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
