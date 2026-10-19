package herdr

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeRequest struct {
	ID     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type fakeSocket struct {
	path        string
	listener    net.Listener
	mu          sync.Mutex
	connections map[net.Conn]struct{}
	wg          sync.WaitGroup
}

func newFakeSocket(t *testing.T, handler func(net.Conn, fakeRequest)) *fakeSocket {
	t.Helper()
	// Keep paths below Darwin's sockaddr_un limit even for long test names.
	directory, err := os.MkdirTemp("/tmp", "outpost-herdr-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	return listenFakeSocket(t, filepath.Join(directory, "herdr.sock"), handler)
}

func listenFakeSocket(t *testing.T, path string, handler func(net.Conn, fakeRequest)) *fakeSocket {
	t.Helper()
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := &fakeSocket{path: path, listener: listener, connections: make(map[net.Conn]struct{})}
	server.wg.Add(1)
	go func() {
		defer server.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			server.mu.Lock()
			server.connections[conn] = struct{}{}
			server.mu.Unlock()
			server.wg.Add(1)
			go func() {
				defer server.wg.Done()
				defer conn.Close()
				defer func() {
					server.mu.Lock()
					delete(server.connections, conn)
					server.mu.Unlock()
				}()
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				frame, err := readFrame(bufio.NewReader(conn), MaxFrameBytes)
				if err != nil {
					return
				}
				var request fakeRequest
				if err := json.Unmarshal(frame, &request); err != nil {
					t.Errorf("fake received invalid request: %v", err)
					return
				}
				handler(conn, request)
			}()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		server.mu.Lock()
		for conn := range server.connections {
			conn.Close()
		}
		server.mu.Unlock()
		server.wg.Wait()
	})
	return server
}

func respond(conn net.Conn, id string, result any) error {
	return json.NewEncoder(conn).Encode(map[string]any{"id": id, "result": result})
}

func snapshotResult(snapshot Snapshot) map[string]any {
	return map[string]any{"type": "session_snapshot", "snapshot": snapshot}
}

func testSnapshot(agents ...Agent) Snapshot {
	if agents == nil {
		agents = []Agent{}
	}
	panes := make([]Pane, 0, len(agents))
	for _, agent := range agents {
		panes = append(panes, Pane{PaneID: agent.PaneID, TerminalID: agent.TerminalID})
	}
	return Snapshot{Version: "0.9.5", Protocol: 22, Agents: agents, Panes: panes}
}

func testAgent(pane, status string) Agent {
	return Agent{PaneID: pane, WorkspaceID: "w1", TabID: "w1:t1", TerminalID: "terminal-" + pane, Agent: "claude", AgentStatus: status, AgentSession: &SessionRef{Source: "hook", Agent: "claude", Kind: "id", Value: "native-" + pane}, Revision: 9007199254740993}
}

func TestClientPublicMethodsPreserveTypedStateAndInput(t *testing.T) {
	agent := testAgent("w1:p1", "working")
	agent.Cwd = "/project"
	agent.ForegroundCwd = "/project/subdir"
	agent.Title = "Investigating"
	agent.TerminalTitleStripped = "claude"
	snapshot := testSnapshot(agent)
	requests := make(chan fakeRequest, 8)
	server := newFakeSocket(t, func(conn net.Conn, request fakeRequest) {
		requests <- request
		var result any
		switch request.Method {
		case "session.snapshot":
			result = snapshotResult(snapshot)
		case "agent.list":
			result = map[string]any{"type": "agent_list", "agents": snapshot.Agents}
		case "pane.list":
			result = map[string]any{"type": "pane_list", "panes": snapshot.Panes}
		case "agent.get":
			result = map[string]any{"type": "agent_info", "agent": agent}
		case "agent.prompt":
			result = map[string]any{"type": "agent_prompted", "agent": agent}
		case "agent.send_keys":
			result = map[string]any{"type": "ok"}
		default:
			t.Errorf("unexpected method %q", request.Method)
			return
		}
		if err := respond(conn, request.ID, result); err != nil {
			t.Errorf("respond: %v", err)
		}
	})
	client := NewClient(server.path)
	ctx := context.Background()
	got, err := client.Snapshot(ctx)
	if err != nil || !reflect.DeepEqual(got, snapshot) {
		t.Fatalf("Snapshot = %#v, %v; want %#v", got, err, snapshot)
	}
	agents, err := client.ListAgents(ctx)
	if err != nil || !reflect.DeepEqual(agents, snapshot.Agents) {
		t.Fatalf("ListAgents = %#v, %v", agents, err)
	}
	panes, err := client.ListPanes(ctx)
	if err != nil || !reflect.DeepEqual(panes, snapshot.Panes) {
		t.Fatalf("ListPanes = %#v, %v", panes, err)
	}
	gotAgent, err := client.GetAgent(ctx, agent.PaneID)
	if err != nil || !reflect.DeepEqual(gotAgent, agent) {
		t.Fatalf("GetAgent = %#v, %v", gotAgent, err)
	}
	text := "literal\nUnicode λ\"\\"
	if err := client.Prompt(ctx, agent.PaneID, text); err != nil {
		t.Fatal(err)
	}
	if err := client.SendKeys(ctx, agent.PaneID, "esc", "ctrl+c"); err != nil {
		t.Fatal(err)
	}
	seenIDs := map[string]bool{}
	for range 6 {
		request := <-requests
		if request.ID == "" || seenIDs[request.ID] {
			t.Fatalf("non-unique request ID %q", request.ID)
		}
		seenIDs[request.ID] = true
		var params map[string]any
		if err := json.Unmarshal(request.Params, &params); err != nil || params == nil {
			t.Fatalf("params must be an object: %s", request.Params)
		}
		switch request.Method {
		case "agent.prompt":
			if params["target"] != agent.PaneID || params["text"] != text || len(params) != 2 {
				t.Fatalf("prompt params = %#v", params)
			}
		case "agent.send_keys":
			if params["target"] != agent.PaneID || !reflect.DeepEqual(params["keys"], []any{"esc", "ctrl+c"}) || len(params) != 2 {
				t.Fatalf("send_keys params = %#v", params)
			}
		}
	}
}

func TestClientRejectsInvalidResponseEnvelopes(t *testing.T) {
	cases := []struct{ name, response string }{
		{"missing_id", `{"result":{}}`},
		{"wrong_id", `{"id":"other","result":{}}`},
		{"numeric_id", `{"id":1,"result":{}}`},
		{"null_id", `{"id":null,"result":{}}`},
		{"array_envelope", `[]`},
		{"null_envelope", `null`},
		{"null_result", `{"id":%s,"result":null}`},
		{"array_result", `{"id":%s,"result":[]}`},
		{"missing_result", `{"id":%s}`},
		{"both_result_error", `{"id":%s,"result":{},"error":{"code":"bad","message":"bad"}}`},
		{"null_error", `{"id":%s,"error":null}`},
		{"string_error", `{"id":%s,"error":"bad"}`},
		{"error_without_code", `{"id":%s,"error":{"message":"bad"}}`},
		{"error_without_message", `{"id":%s,"error":{"code":"bad"}}`},
		{"nonstring_error", `{"id":%s,"error":{"code":1,"message":"bad"}}`},
		{"wrong_result_type", `{"id":%s,"result":{"type":"pane_info","agent":{"pane_id":"p1"}}}`},
		{"missing_agent", `{"id":%s,"result":{"type":"agent_info"}}`},
		{"null_agent", `{"id":%s,"result":{"type":"agent_info","agent":null}}`},
		{"missing_pane", `{"id":%s,"result":{"type":"agent_info","agent":{}}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newFakeSocket(t, func(conn net.Conn, request fakeRequest) {
				response := tc.response
				if strings.Contains(response, "%s") {
					id, _ := json.Marshal(request.ID)
					response = fmt.Sprintf(response, id)
				}
				io.WriteString(conn, response+"\n")
			})
			if _, err := NewClient(server.path).GetAgent(context.Background(), "w1:p1"); err == nil {
				t.Fatal("accepted invalid response")
			}
		})
	}
}

func TestClientReturnsCorrelatedServerError(t *testing.T) {
	server := newFakeSocket(t, func(conn net.Conn, request fakeRequest) {
		json.NewEncoder(conn).Encode(map[string]any{"id": request.ID, "error": map[string]string{"code": "agent_blocked", "message": "Agent needs approval"}})
	})
	err := NewClient(server.path).Prompt(context.Background(), "w1:p1", "do not send")
	var remote *APIError
	if !errors.As(err, &remote) || remote.Code != "agent_blocked" || remote.Message != "Agent needs approval" {
		t.Fatalf("error = %#v", err)
	}
}

func TestReadFrameRequiresDelimiterAndHonorsExactLimit(t *testing.T) {
	cases := []struct {
		name, input string
		limit       int
		want        string
		wantErr     bool
	}{
		{"exact_limit", "12345678\nnext\n", 8, "12345678", false},
		{"over_limit", "123456789\n", 8, "", true},
		{"partial_json", "{\"id\":\"valid\"}", 100, "", true},
		{"partial_limit", "12345678", 8, "", true},
		{"empty_eof", "", 8, "", true},
		{"invalid_utf8", "\xff\n", 8, "", true},
		{"fragmented_exact", strings.Repeat("a", 8192) + "\n", 8192, strings.Repeat("a", 8192), false},
		{"fragmented_over", strings.Repeat("a", 8193) + "\n", 8192, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := readFrame(bufio.NewReaderSize(strings.NewReader(tc.input), 16), tc.limit)
			if (err != nil) != tc.wantErr || string(got) != tc.want {
				t.Fatalf("readFrame = %q, %v", got, err)
			}
		})
	}
}

func TestClientRejectsPartialAndOversizedSocketFrames(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response func(string) []byte
	}{
		{"partial", func(id string) []byte {
			raw, _ := json.Marshal(map[string]any{"id": id, "result": map[string]any{"type": "agent_info", "agent": testAgent("p1", "idle")}})
			return raw
		}},
		{"oversized", func(string) []byte { return append(bytes.Repeat([]byte("x"), MaxFrameBytes+1), '\n') }},
		{"invalid_utf8", func(string) []byte { return []byte("\xff\n") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newFakeSocket(t, func(conn net.Conn, request fakeRequest) { conn.Write(tc.response(request.ID)) })
			if _, err := NewClient(server.path).GetAgent(context.Background(), "p1"); err == nil {
				t.Fatal("accepted malformed frame")
			}
		})
	}
}

func TestOutgoingFrameLimitIncludesJSONEncodingButNotNewline(t *testing.T) {
	base, err := encodeRequest("id", "method", map[string]string{"text": ""})
	if err != nil {
		t.Fatal(err)
	}
	textLength := MaxFrameBytes - (len(base) - 1)
	frame, err := encodeRequest("id", "method", map[string]string{"text": strings.Repeat("x", textLength)})
	if err != nil || len(frame) != MaxFrameBytes+1 {
		t.Fatalf("exact boundary: %d, %v", len(frame), err)
	}
	if _, err := encodeRequest("id", "method", map[string]string{"text": strings.Repeat("x", textLength+1)}); err == nil {
		t.Fatal("accepted oversized outgoing frame")
	}
	var connections atomic.Int32
	server := newFakeSocket(t, func(net.Conn, fakeRequest) { connections.Add(1) })
	if err := NewClient(server.path).Prompt(context.Background(), "p1", strings.Repeat("\n", MaxFrameBytes/2)); err == nil {
		t.Fatal("accepted escaped oversized prompt")
	}
	if connections.Load() != 0 {
		t.Fatal("oversized request reached socket")
	}
}

func TestRequestCancellationAndDeadlineCloseConnections(t *testing.T) {
	for _, cancellation := range []bool{true, false} {
		t.Run(fmt.Sprintf("cancel_%v", cancellation), func(t *testing.T) {
			started := make(chan struct{})
			closed := make(chan struct{})
			server := newFakeSocket(t, func(conn net.Conn, request fakeRequest) {
				close(started)
				io.Copy(io.Discard, conn)
				close(closed)
			})
			client := NewClient(server.path)
			client.RequestTimeout = 40 * time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := client.GetAgent(ctx, "p1"); result <- err }()
			awaitSignal(t, started)
			if cancellation {
				cancel()
			}
			err := awaitError(t, result)
			want := context.DeadlineExceeded
			if cancellation {
				want = context.Canceled
			}
			if !errors.Is(err, want) {
				t.Fatalf("request error = %v; want %v", err, want)
			}
			awaitSignal(t, closed)
		})
	}
}

func TestSnapshotDoesNotTreatMissingStateAsEmpty(t *testing.T) {
	for _, snapshot := range []string{
		`null`, `{}`, `{"version":"x","protocol":22,"agents":[],"panes":null}`,
		`{"version":"x","protocol":22,"panes":[]}`, `{"version":"x","protocol":22,"agents":[{}],"panes":[]}`,
		`{"version":"x","protocol":22,"agents":[],"panes":[{}]}`,
	} {
		t.Run(snapshot, func(t *testing.T) {
			server := newFakeSocket(t, func(conn net.Conn, request fakeRequest) {
				respond(conn, request.ID, map[string]any{"type": "session_snapshot", "snapshot": json.RawMessage(snapshot)})
			})
			if _, err := NewClient(server.path).Snapshot(context.Background()); err == nil {
				t.Fatal("accepted missing snapshot state")
			}
		})
	}
}

func TestSocketDiscoveryUsesNativeEnvironmentAndNamedSessions(t *testing.T) {
	t.Setenv("HOME", "/home/test")
	t.Setenv("HERDR_SOCKET_PATH", "")
	t.Setenv("HERDR_SESSION", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	for _, tc := range []struct{ session, override, want string }{
		{"", "", "/home/test/.config/herdr/herdr.sock"},
		{"default", "", "/home/test/.config/herdr/herdr.sock"},
		{"mobile", "", "/home/test/.config/herdr/sessions/mobile/herdr.sock"},
		{"mobile", "/tmp/override.sock", "/tmp/override.sock"},
	} {
		t.Setenv("HERDR_SESSION", tc.session)
		t.Setenv("HERDR_SOCKET_PATH", tc.override)
		got, err := SocketPath()
		if err != nil || got != tc.want {
			t.Fatalf("SocketPath = %q, %v; want %q", got, err, tc.want)
		}
	}
	if got, err := NewClient("/explicit.sock").path(); err != nil || got != "/explicit.sock" {
		t.Fatalf("explicit path = %q, %v", got, err)
	}
	t.Setenv("HERDR_SOCKET_PATH", "")
	t.Setenv("HERDR_SESSION", "mobile")
	t.Setenv("XDG_CONFIG_HOME", "/custom/config")
	if got, err := SocketPath(); err != nil || got != "/custom/config/herdr/sessions/mobile/herdr.sock" {
		t.Fatalf("XDG socket = %q, %v", got, err)
	}
	for _, name := range []string{"../other", "work session", "日本語", ".", "..", strings.Repeat("x", 65)} {
		t.Setenv("HERDR_SESSION", name)
		if _, err := SocketPath(); err == nil {
			t.Fatalf("accepted invalid session name %q", name)
		}
	}
}

func awaitSignal(t *testing.T, channel <-chan struct{}) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

func awaitError(t *testing.T, channel <-chan error) error {
	t.Helper()
	select {
	case err := <-channel:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
		return nil
	}
}
