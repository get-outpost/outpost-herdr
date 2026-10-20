package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/get-outpost/outpost-go"
	"github.com/get-outpost/outpost-go/client"
	"github.com/get-outpost/outpost-herdr/internal/adapter"
)

type publication struct{ kind, id string }

type memoryState struct {
	mu            sync.Mutex
	cursor        int64
	exists        bool
	writes        []int64
	recorded      []publication
	live          map[string]struct{}
	cursorErr     error
	recordErr     error
	cursorWritten chan int64
	published     chan publication
}

func newState(cursor int64, exists bool) *memoryState {
	return &memoryState{cursor: cursor, exists: exists, live: make(map[string]struct{}), cursorWritten: make(chan int64, 128), published: make(chan publication, 128)}
}

func (s *memoryState) Cursor() (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor, s.exists
}

func (s *memoryState) SetCursor(cursor int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cursorErr != nil {
		return s.cursorErr
	}
	if s.exists && cursor < s.cursor {
		return errors.New("cursor went backwards")
	}
	s.cursor, s.exists = cursor, true
	s.writes = append(s.writes, cursor)
	s.cursorWritten <- cursor
	return nil
}

func (s *memoryState) LiveSessions() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.live))
	for id := range s.live {
		ids = append(ids, id)
	}
	return ids
}

func (s *memoryState) RecordPublished(kind, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recordErr != nil {
		return s.recordErr
	}
	p := publication{kind, id}
	s.recorded = append(s.recorded, p)
	switch kind {
	case outpost.SessionStarted, outpost.SessionUpdated:
		s.live[id] = struct{}{}
	case outpost.SessionEnded, outpost.SessionDeleted:
		delete(s.live, id)
	}
	s.published <- p
	return nil
}

func (s *memoryState) history() ([]int64, []publication) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.writes...), append([]publication(nil), s.recorded...)
}

type serverOptions struct {
	head      int64
	sessions  func(http.ResponseWriter, *http.Request)
	post      func(http.ResponseWriter, *http.Request, outpost.Event)
	handshake func(http.ResponseWriter, *http.Request) bool
}

type connection struct {
	conn  *websocket.Conn
	after int64
}

type fakeServer struct {
	server          *httptest.Server
	connections     chan connection
	posts           chan outpost.Event
	sessionRequests atomic.Int64
	handshakes      atomic.Int64
	stop            chan struct{}
}

func newServer(t *testing.T, options serverOptions) *fakeServer {
	t.Helper()
	f := &fakeServer{connections: make(chan connection, 32), posts: make(chan outpost.Event, 512), stop: make(chan struct{})}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("missing SDK authentication")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/sessions":
			f.sessionRequests.Add(1)
			if r.URL.RawQuery != "" {
				t.Errorf("unexpected snapshot query %q", r.URL.RawQuery)
			}
			if options.sessions != nil {
				options.sessions(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(client.Snapshot{AsOfSeq: options.head})
		case "/v1/events":
			if r.Method != http.MethodPost {
				http.Error(w, "method", 405)
				return
			}
			var event outpost.Event
			if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
				t.Errorf("decode publication: %v", err)
				return
			}
			f.posts <- event
			if options.post != nil {
				options.post(w, r, event)
				return
			}
			postSuccess(w, event)
		case "/v1/ws":
			f.handshakes.Add(1)
			if options.handshake != nil && options.handshake(w, r) {
				return
			}
			after, err := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
			if err != nil {
				t.Errorf("invalid stream cursor: %v", err)
				return
			}
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			f.connections <- connection{conn, after}
			<-f.stop
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(func() { close(f.stop); f.server.Close() })
	return f
}

func postSuccess(w http.ResponseWriter, event outpost.Event) {
	_ = json.NewEncoder(w).Encode(outpost.Envelope{Seq: 9000, Type: event.Type, SessionID: event.SessionID, TS: time.Now(), Payload: event.Payload})
}

func configuredSink(t *testing.T, server *fakeServer, state State, options Options) *Sink {
	t.Helper()
	c, err := client.New(client.Config{URL: server.server.URL, Token: "test-token"})
	if err != nil {
		t.Fatal(err)
	}
	if options.Logger == nil {
		options.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	s, err := New(c, state, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})
	return s
}

func startedSink(t *testing.T, server *fakeServer, state State, options Options) (*Sink, connection) {
	t.Helper()
	s := configuredSink(t, server, state, options)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s, receive(t, server.connections)
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before expected value")
		}
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for value")
		var zero T
		return zero
	}
}

func sendFrame(t *testing.T, conn connection, frame outpost.Frame) {
	t.Helper()
	data, err := json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	sendWire(t, conn, websocket.MessageText, data)
}

func sendWire(t *testing.T, conn connection, kind websocket.MessageType, data []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := conn.conn.Write(ctx, kind, data); err != nil {
		t.Fatal(err)
	}
}

func command(seq int64, kind, id string) outpost.Frame {
	return outpost.Frame{Kind: outpost.FrameEvent, Event: &outpost.Envelope{
		ID: fmt.Sprintf("command-%d", seq), Seq: seq, Type: kind, SessionID: id,
		TS: time.Now(), Payload: json.RawMessage(`{"text":"hello","owner_device_id":"another-device"}`),
	}}
}

func publish(t *testing.T, sink *Sink, id string) {
	t.Helper()
	if err := sink.Publish(context.Background(), adapter.Event{Type: outpost.SessionStarted, SessionID: id, Payload: map[string]any{"provider": outpost.ProviderOMP}}); err != nil {
		t.Fatal(err)
	}
}

func shutdown(t *testing.T, s *Sink) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return s.Shutdown(ctx)
}

func TestFirstRunPersistsHeadBeforeDialAndWaitsForLocalSnapshot(t *testing.T) {
	state := newState(0, false)
	observed := make(chan int64, 1)
	server := newServer(t, serverOptions{head: 41, handshake: func(http.ResponseWriter, *http.Request) bool {
		cursor, exists := state.Cursor()
		if !exists {
			cursor = -1
		}
		observed <- cursor
		return false
	}})
	s, conn := startedSink(t, server, state, Options{})
	if conn.after != 41 || receive(t, observed) != 41 {
		t.Fatal("stream opened before durable head initialization")
	}
	if got := receive(t, state.cursorWritten); got != 41 {
		t.Fatalf("initial cursor = %d", got)
	}
	sendFrame(t, conn, command(42, outpost.CommandMessage, "local"))
	select {
	case cursor := <-state.cursorWritten:
		t.Fatalf("reader consumed %d before local snapshot", cursor)
	case <-time.After(30 * time.Millisecond):
	}
	s.SetLocalSessions([]string{"local"})
	got := receive(t, s.Commands())
	if cursor, _ := state.Cursor(); cursor != 42 {
		t.Fatalf("command dispatched before durable cursor: %d", cursor)
	}
	if got.ID != "command-42" || got.Payload["text"] != "hello" {
		t.Fatalf("unexpected command: %#v", got)
	}
	if err := shutdown(t, s); err != nil {
		t.Fatal(err)
	}
}

func TestResumeFiltersCommandsAndNeverPersistsAckOrReady(t *testing.T) {
	state := newState(100, true)
	var logs bytes.Buffer
	server := newServer(t, serverOptions{})
	s, conn := startedSink(t, server, state, Options{Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	s.SetLocalSessions([]string{"local"})
	if conn.after != 100 || server.sessionRequests.Load() != 0 {
		t.Fatal("restart did not use durable cursor")
	}
	head := int64(9999)
	sendFrame(t, conn, outpost.Frame{Kind: outpost.FrameReady, HeadSeq: &head})
	sendFrame(t, conn, outpost.Frame{Kind: outpost.FrameAck, Event: &outpost.Envelope{Seq: 9998}})
	sendFrame(t, conn, command(101, outpost.CommandMessage, "foreign"))
	sendFrame(t, conn, command(102, outpost.CommandApprove, "local"))
	encrypted := command(103, outpost.CommandMessage, "local")
	encrypted.Event.Enc = "encrypted"
	sendFrame(t, conn, encrypted)
	stale := command(104, outpost.CommandInterrupt, "local")
	stale.Event.TS = time.Now().Add(-3 * time.Minute)
	sendFrame(t, conn, stale)
	sendFrame(t, conn, command(105, outpost.SessionUpdated, "local"))
	sendFrame(t, conn, command(106, outpost.CommandMessage, "local"))
	if got := receive(t, s.Commands()); got.ID != "command-106" {
		t.Fatalf("filtered command escaped: %#v", got)
	}
	s.SetLocalSessions([]string{"new-local"})
	sendFrame(t, conn, command(107, outpost.CommandInterrupt, "local"))
	sendFrame(t, conn, command(108, outpost.CommandInterrupt, "new-local"))
	if got := receive(t, s.Commands()); got.ID != "command-108" {
		t.Fatalf("old local session remained executable: %#v", got)
	}
	if err := shutdown(t, s); err != nil {
		t.Fatal(err)
	}
	writes, _ := state.history()
	if want := []int64{101, 102, 103, 104, 105, 106, 107, 108}; !reflect.DeepEqual(writes, want) {
		t.Fatalf("cursor writes = %v, want %v", writes, want)
	}
	if !strings.Contains(logs.String(), "dropping stale Outpost command") {
		t.Fatal("stale command was not logged")
	}
}

func TestWaitingCommandIsRefilteredWhenLocalOwnershipChanges(t *testing.T) {
	state := newState(1, true)
	server := newServer(t, serverOptions{})
	s, conn := startedSink(t, server, state, Options{})
	s.SetLocalSessions([]string{"old"})
	sendFrame(t, conn, command(2, outpost.CommandMessage, "old"))
	if receive(t, state.cursorWritten) != 2 {
		t.Fatal("pending command not durably consumed")
	}
	s.SetLocalSessions([]string{"new"})
	sendFrame(t, conn, command(3, outpost.CommandInterrupt, "new"))
	if got := receive(t, s.Commands()); got.ID != "command-3" {
		t.Fatalf("pending former-local command executed: %#v", got)
	}
}

func TestWaitingCommandExpiresBeforeAdapterReceivesIt(t *testing.T) {
	state := newState(1, true)
	server := newServer(t, serverOptions{})
	s, conn := startedSink(t, server, state, Options{CommandMaxAge: 250 * time.Millisecond})
	s.SetLocalSessions([]string{"local"})
	sendFrame(t, conn, command(2, outpost.CommandMessage, "local"))
	if receive(t, state.cursorWritten) != 2 {
		t.Fatal("pending command not persisted")
	}
	// The next noncommand can only be persisted after the pending command ages
	// out, providing a deterministic barrier without a sleep.
	sendFrame(t, conn, command(3, outpost.SessionUpdated, "local"))
	if receive(t, state.cursorWritten) != 3 {
		t.Fatal("expired command blocked stream progress")
	}
	sendFrame(t, conn, command(4, outpost.CommandInterrupt, "local"))
	if got := receive(t, s.Commands()); got.ID != "command-4" {
		t.Fatalf("expired pending command escaped: %#v", got)
	}
}

func TestPublicationRetriesNetworkAndServerFailuresInOrder(t *testing.T) {
	state := newState(7, true)
	var attempts atomic.Int64
	var logs bytes.Buffer
	server := newServer(t, serverOptions{post: func(w http.ResponseWriter, r *http.Request, event outpost.Event) {
		switch attempts.Add(1) {
		case 1:
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":"test-token unrelated-secret"}`)
		case 2:
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_ = conn.Close()
		case 4:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":"test-token unrelated-secret"}`)
		default:
			postSuccess(w, event)
		}
	}})
	s, _ := startedSink(t, server, state, Options{Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	s.SetLocalSessions(nil)
	payload := map[string]any{"provider": outpost.ProviderPi, "title": "original"}
	if err := s.Publish(context.Background(), adapter.Event{Type: outpost.SessionUpdated, SessionID: "one", Payload: payload}); err != nil {
		t.Fatal(err)
	}
	payload["title"] = "mutated after admission"
	publish(t, s, "two")
	publish(t, s, "three")
	if err := shutdown(t, s); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for range 5 {
		event := receive(t, server.posts)
		ids = append(ids, event.SessionID)
		if event.SessionID == "one" && !strings.Contains(string(event.Payload), `"title":"original"`) {
			t.Fatalf("mutable publication: %s", event.Payload)
		}
	}
	if want := []string{"one", "one", "one", "two", "three"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("publication order = %v", ids)
	}
	writes, recorded := state.history()
	if len(writes) != 0 {
		t.Fatalf("HTTP acknowledgement changed stream cursor: %v", writes)
	}
	if want := []publication{{outpost.SessionUpdated, "one"}, {outpost.SessionStarted, "three"}}; !reflect.DeepEqual(recorded, want) {
		t.Fatalf("recorded unsuccessful publication: %v", recorded)
	}
	if strings.Contains(logs.String(), "test-token") || strings.Contains(logs.String(), "unrelated-secret") {
		t.Fatal("API response leaked to logs")
	}
	if !strings.Contains(logs.String(), "status=400") {
		t.Fatal("permanent drop not logged")
	}
}

func TestPermanentPublicationRejectionsDropWithoutRetry(t *testing.T) {
	for _, status := range []int{400, 403, 404, 422, 429} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			state := newState(0, true)
			server := newServer(t, serverOptions{post: func(w http.ResponseWriter, r *http.Request, event outpost.Event) {
				if event.SessionID == "rejected" {
					w.WriteHeader(status)
					return
				}
				postSuccess(w, event)
			}})
			s, _ := startedSink(t, server, state, Options{})
			s.SetLocalSessions(nil)
			publish(t, s, "rejected")
			publish(t, s, "accepted")
			if err := shutdown(t, s); err != nil {
				t.Fatal(err)
			}
			_, recorded := state.history()
			if want := []publication{{outpost.SessionStarted, "accepted"}}; !reflect.DeepEqual(recorded, want) {
				t.Fatalf("records = %v", recorded)
			}
			if len(server.posts) != 2 {
				t.Fatalf("permanent rejection retried: %d requests", len(server.posts))
			}
		})
	}
}

func TestPublication401IsFatalOnceAndNeverLeaksResponse(t *testing.T) {
	state := newState(0, true)
	server := newServer(t, serverOptions{post: func(w http.ResponseWriter, r *http.Request, event outpost.Event) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":"test-token unrelated-secret"}`)
	}})
	s, _ := startedSink(t, server, state, Options{})
	s.SetLocalSessions(nil)
	publish(t, s, "one")
	fatal := receive(t, s.Errors())
	if !strings.Contains(fatal.Error(), "401") || strings.Contains(fatal.Error(), "test-token") || strings.Contains(fatal.Error(), "unrelated-secret") {
		t.Fatalf("unsafe authentication error: %v", fatal)
	}
	if err := shutdown(t, s); err != fatal {
		t.Fatalf("Shutdown lost fatal error: %v", err)
	}
	if err := s.Publish(context.Background(), adapter.Event{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("fatal relay accepted publication: %v", err)
	}
	if len(server.posts) != 1 {
		t.Fatal("401 retried")
	}
	_, recorded := state.history()
	if len(recorded) != 0 {
		t.Fatal("401 publication recorded as live")
	}
	select {
	case err := <-s.Errors():
		t.Fatalf("extra fatal error: %v", err)
	default:
	}
}

func TestInitialHandshakeRetriesOnlyTransientErrors(t *testing.T) {
	t.Run("transport then connected", func(t *testing.T) {
		var calls atomic.Int64
		server := newServer(t, serverOptions{handshake: func(w http.ResponseWriter, r *http.Request) bool {
			if calls.Add(1) == 1 {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Errorf("hijack: %v", err)
					return true
				}
				_ = conn.Close()
				return true
			}
			return false
		}})
		s, _ := startedSink(t, server, newState(0, true), Options{})
		if calls.Load() != 2 {
			t.Fatal("initial transport failure did not retry")
		}
		if err := shutdown(t, s); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("503 then connected without ready", func(t *testing.T) {
		var calls atomic.Int64
		server := newServer(t, serverOptions{handshake: func(w http.ResponseWriter, r *http.Request) bool {
			if calls.Add(1) == 1 {
				w.WriteHeader(503)
				return true
			}
			return false
		}})
		s, conn := startedSink(t, server, newState(0, true), Options{})
		if conn.after != 0 || calls.Load() != 2 {
			t.Fatal("initial transient handshake did not retry")
		}
		if err := shutdown(t, s); err != nil {
			t.Fatal(err)
		}
	})
	for _, status := range []int{400, 401, 403, 404, 429} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			server := newServer(t, serverOptions{handshake: func(w http.ResponseWriter, r *http.Request) bool { w.WriteHeader(status); return true }})
			s := configuredSink(t, server, newState(12, true), Options{})
			err := s.Start(context.Background())
			if err == nil || !strings.Contains(err.Error(), strconv.Itoa(status)) {
				t.Fatalf("Start = %v", err)
			}
			if server.handshakes.Load() != 1 {
				t.Fatal("permanent initial rejection retried")
			}
			if shutdown(t, s) != err {
				t.Fatal("startup failure lost during Shutdown")
			}
		})
	}
}

func TestSnapshotRetriesAndInitializationPersistenceFailurePreventsDial(t *testing.T) {
	t.Run("snapshot retries", func(t *testing.T) {
		var calls atomic.Int64
		server := newServer(t, serverOptions{sessions: func(w http.ResponseWriter, r *http.Request) {
			if calls.Add(1) == 1 {
				w.WriteHeader(503)
				return
			}
			_ = json.NewEncoder(w).Encode(client.Snapshot{AsOfSeq: 77})
		}})
		state := newState(0, false)
		_, conn := startedSink(t, server, state, Options{})
		if conn.after != 77 || calls.Load() != 2 {
			t.Fatal("snapshot transient failure did not recover")
		}
	})
	t.Run("durability failure", func(t *testing.T) {
		state := newState(0, false)
		state.cursorErr = errors.New("disk full")
		server := newServer(t, serverOptions{head: 22})
		s := configuredSink(t, server, state, Options{})
		if err := s.Start(context.Background()); err == nil {
			t.Fatal("unpersisted first-run head accepted")
		}
		if server.handshakes.Load() != 0 {
			t.Fatal("stream opened before head persistence")
		}
		if err := shutdown(t, s); err == nil {
			t.Fatal("Shutdown lost failed initialization")
		}
	})
}

func TestCancelDuringInitialRetryAndShutdownBeforeStart(t *testing.T) {
	t.Run("cancel backoff", func(t *testing.T) {
		attempted := make(chan struct{}, 1)
		server := newServer(t, serverOptions{handshake: func(w http.ResponseWriter, r *http.Request) bool {
			w.WriteHeader(503)
			attempted <- struct{}{}
			return true
		}})
		s := configuredSink(t, server, newState(0, true), Options{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- s.Start(ctx) }()
		receive(t, attempted)
		cancel()
		if err := receive(t, done); !errors.Is(err, context.Canceled) {
			t.Fatalf("Start = %v", err)
		}
		if err := shutdown(t, s); err != nil {
			t.Fatal(err)
		}
		if server.handshakes.Load() != 1 {
			t.Fatal("startup kept retrying after cancellation")
		}
	})
	t.Run("before start", func(t *testing.T) {
		server := newServer(t, serverOptions{})
		s := configuredSink(t, server, newState(0, true), Options{})
		if err := shutdown(t, s); err != nil {
			t.Fatal(err)
		}
		if err := s.Start(context.Background()); err == nil {
			t.Fatal("started after shutdown")
		}
		if err := s.Publish(context.Background(), adapter.Event{}); !errors.Is(err, ErrClosed) {
			t.Fatalf("Publish = %v", err)
		}
	})
}

func TestStreamReconnectUsesEventsNotWatermarks(t *testing.T) {
	state := newState(10, true)
	server := newServer(t, serverOptions{})
	s, first := startedSink(t, server, state, Options{})
	s.SetLocalSessions([]string{"local"})
	head := int64(500)
	sendFrame(t, first, outpost.Frame{Kind: outpost.FrameReady, HeadSeq: &head})
	sendFrame(t, first, outpost.Frame{Kind: outpost.FrameAck, Event: &outpost.Envelope{Seq: 400}})
	sendFrame(t, first, command(11, outpost.SessionUpdated, "foreign"))
	if receive(t, state.cursorWritten) != 11 {
		t.Fatal("event cursor missing")
	}
	_ = first.conn.CloseNow()
	second := receive(t, server.connections)
	if second.after != 11 {
		t.Fatalf("reconnect cursor = %d", second.after)
	}
	// A replayed event at the old cursor must not execute.
	sendFrame(t, second, command(11, outpost.CommandMessage, "local"))
	sendFrame(t, second, command(12, outpost.CommandInterrupt, "local"))
	if got := receive(t, s.Commands()); got.ID != "command-12" {
		t.Fatalf("duplicate executed: %#v", got)
	}
}

func TestCommandStreamFailuresAreFatalAndNotExecuted(t *testing.T) {
	for _, kind := range []string{"error-frame", "invalid-json", "binary", "cursor-write", "payload-object"} {
		t.Run(kind, func(t *testing.T) {
			state := newState(10, true)
			if kind == "cursor-write" {
				state.cursorErr = errors.New("disk full")
			}
			server := newServer(t, serverOptions{})
			s, conn := startedSink(t, server, state, Options{})
			s.SetLocalSessions([]string{"local"})
			switch kind {
			case "error-frame":
				sendFrame(t, conn, outpost.Frame{Kind: outpost.FrameError, Error: "test-token unrelated-secret"})
			case "invalid-json":
				sendWire(t, conn, websocket.MessageText, []byte("not JSON test-token"))
			case "binary":
				sendWire(t, conn, websocket.MessageBinary, []byte("test-token"))
			case "cursor-write":
				sendFrame(t, conn, command(11, outpost.CommandMessage, "local"))
			case "payload-object":
				frame := command(11, outpost.CommandMessage, "local")
				frame.Event.Payload = json.RawMessage(`[]`)
				sendFrame(t, conn, frame)
			}
			fatal := receive(t, s.Errors())
			if strings.Contains(fatal.Error(), "test-token") || strings.Contains(fatal.Error(), "unrelated-secret") {
				t.Fatalf("unsafe stream error: %v", fatal)
			}
			if shutdown(t, s) != fatal {
				t.Fatal("Shutdown lost stream failure")
			}
			if command, ok := <-s.Commands(); ok {
				t.Fatalf("failed command escaped: %#v", command)
			}
			if kind == "cursor-write" {
				if cursor, _ := state.Cursor(); cursor != 10 {
					t.Fatal("failed cursor write advanced cursor")
				}
			}
			select {
			case extra := <-s.Errors():
				t.Fatalf("duplicate error: %v", extra)
			default:
			}
		})
	}
}

func TestStreamReconnectAuthenticationFailureIsFatal(t *testing.T) {
	for _, status := range []int{401, 403} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var attempts atomic.Int64
			server := newServer(t, serverOptions{handshake: func(w http.ResponseWriter, r *http.Request) bool {
				if attempts.Add(1) > 1 {
					w.WriteHeader(status)
					return true
				}
				return false
			}})
			s, conn := startedSink(t, server, newState(10, true), Options{})
			s.SetLocalSessions(nil)
			_ = conn.conn.CloseNow()
			fatal := receive(t, s.Errors())
			if !strings.Contains(fatal.Error(), strconv.Itoa(status)) {
				t.Fatalf("fatal = %v", fatal)
			}
			if shutdown(t, s) != fatal {
				t.Fatal("lost reconnect authentication failure")
			}
			if attempts.Load() != 2 {
				t.Fatal("terminal reconnect rejection retried")
			}
		})
	}
}

func TestPublishedStateFailureStopsFurtherPublication(t *testing.T) {
	state := newState(0, true)
	state.recordErr = errors.New("disk full")
	server := newServer(t, serverOptions{})
	s, _ := startedSink(t, server, state, Options{})
	s.SetLocalSessions(nil)
	publish(t, s, "one")
	fatal := receive(t, s.Errors())
	if !strings.Contains(fatal.Error(), "published Outpost session state") {
		t.Fatalf("fatal = %v", fatal)
	}
	if shutdown(t, s) != fatal {
		t.Fatal("lost state failure")
	}
	if len(server.posts) != 1 {
		t.Fatal("publication retried after acknowledged HTTP success")
	}
}

func TestQueueOverflowFailsFastAndDrainsAdmittedTransitions(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	server := newServer(t, serverOptions{post: func(w http.ResponseWriter, r *http.Request, event outpost.Event) {
		if event.SessionID == "one" {
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		postSuccess(w, event)
	}})
	state := newState(0, true)
	s, _ := startedSink(t, server, state, Options{QueueSize: 1})
	s.SetLocalSessions(nil)
	publish(t, s, "one")
	receive(t, entered)
	publish(t, s, "two")
	if err := s.Publish(context.Background(), adapter.Event{Type: outpost.SessionEnded, SessionID: "three"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("overflow = %v", err)
	}
	if err := receive(t, s.Errors()); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("fatal = %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	if err := shutdown(t, s); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Shutdown = %v", err)
	}
	_, recorded := state.history()
	if want := []publication{{outpost.SessionStarted, "one"}, {outpost.SessionStarted, "two"}}; !reflect.DeepEqual(recorded, want) {
		t.Fatalf("admitted transitions lost: %v", recorded)
	}
}

func TestSignalCancellationDoesNotCancelPublicationDrain(t *testing.T) {
	entered, release, canceled := make(chan struct{}, 1), make(chan struct{}), make(chan struct{}, 1)
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	server := newServer(t, serverOptions{post: func(w http.ResponseWriter, r *http.Request, event outpost.Event) {
		if event.SessionID == "one" {
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				canceled <- struct{}{}
				return
			}
		}
		postSuccess(w, event)
	}})
	state := newState(0, true)
	s := configuredSink(t, server, state, Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	s.SetLocalSessions(nil)
	publish(t, s, "one")
	receive(t, entered)
	publish(t, s, "two")
	cancel()
	select {
	case _, ok := <-s.Commands():
		if ok {
			t.Fatal("unexpected command")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("signal did not stop reader")
	}
	releaseOnce.Do(func() { close(release) })
	if err := shutdown(t, s); err != nil {
		t.Fatal(err)
	}
	select {
	case <-canceled:
		t.Fatal("signal canceled active publication")
	default:
	}
	_, recorded := state.history()
	if want := []publication{{outpost.SessionStarted, "one"}, {outpost.SessionStarted, "two"}}; !reflect.DeepEqual(recorded, want) {
		t.Fatalf("signal lost queue: %v", recorded)
	}
}

func TestShutdownDeadlineCancelsActiveHTTPAndRetryBackoff(t *testing.T) {
	for _, kind := range []string{"active-http", "retry-backoff"} {
		t.Run(kind, func(t *testing.T) {
			entered, canceled := make(chan struct{}, 1), make(chan struct{}, 1)
			server := newServer(t, serverOptions{post: func(w http.ResponseWriter, r *http.Request, event outpost.Event) {
				entered <- struct{}{}
				if kind == "retry-backoff" {
					w.WriteHeader(503)
					return
				}
				<-r.Context().Done()
				canceled <- struct{}{}
			}})
			state := newState(0, true)
			s, _ := startedSink(t, server, state, Options{})
			s.SetLocalSessions(nil)
			publish(t, s, "one")
			receive(t, entered)
			publish(t, s, "two")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if err := s.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Shutdown = %v", err)
			}
			if kind == "active-http" {
				receive(t, canceled)
			}
			select {
			case <-s.done:
			default:
				t.Fatal("Shutdown returned before workers joined")
			}
			_, recorded := state.history()
			if len(recorded) != 0 {
				t.Fatal("canceled publication recorded")
			}
		})
	}
}

func TestConcurrentPublishAndShutdownNeverSendOnClosedQueue(t *testing.T) {
	state := newState(0, true)
	server := newServer(t, serverOptions{})
	s, _ := startedSink(t, server, state, Options{QueueSize: 128})
	s.SetLocalSessions(nil)
	var workers sync.WaitGroup
	var admitted atomic.Int64
	start := make(chan struct{})
	for i := range 64 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			err := s.Publish(context.Background(), adapter.Event{Type: outpost.SessionStarted, SessionID: strconv.Itoa(i)})
			if err == nil {
				admitted.Add(1)
			} else if !errors.Is(err, ErrClosed) {
				t.Errorf("Publish = %v", err)
			}
		}()
	}
	close(start)
	if err := shutdown(t, s); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	_, recorded := state.history()
	if int64(len(recorded)) != admitted.Load() {
		t.Fatalf("admitted %d, drained %d", admitted.Load(), len(recorded))
	}
}

func TestNewRejectsNegativeOptionsAndUsesDefaults(t *testing.T) {
	server := newServer(t, serverOptions{})
	c, err := client.New(client.Config{URL: server.server.URL, Token: "test-token"})
	if err != nil {
		t.Fatal(err)
	}
	for _, options := range []Options{{QueueSize: -1}, {CommandMaxAge: -time.Second}} {
		if _, err := New(c, newState(0, true), options); err == nil {
			t.Fatal("negative option accepted")
		}
	}
	s := configuredSink(t, server, newState(0, true), Options{})
	if cap(s.publications) != 256 || s.options.CommandMaxAge != 2*time.Minute {
		t.Fatal("incorrect defaults")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Publish(ctx, adapter.Event{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Publish = %v", err)
	}
}
