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
	"strconv"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// MaxFrameBytes bounds each NDJSON payload, excluding its terminating newline.
const MaxFrameBytes = 16 * 1024 * 1024

// APIError is a correlated error returned by the Herdr server.
type APIError struct {
	Code    string
	Message string
}

func (e *APIError) Error() string { return "herdr " + e.Code + ": " + e.Message }

// Client uses independent request connections and one active event subscription.
// Configure its durations before use; methods otherwise support concurrent use.
type Client struct {
	socketPath        string
	RequestTimeout    time.Duration
	ReconnectMin      time.Duration
	ReconnectMax      time.Duration
	ReconcileInterval time.Duration
	nextID            atomic.Uint64
}

var _ Source = (*Client)(nil)

// NewClient defers socket discovery until use when socketPath is empty.
func NewClient(socketPath string) *Client {
	return &Client{
		socketPath:        socketPath,
		RequestTimeout:    5 * time.Second,
		ReconnectMin:      100 * time.Millisecond,
		ReconnectMax:      5 * time.Second,
		ReconcileInterval: 30 * time.Second,
	}
}

// SocketPath follows Herdr's native environment selection. Explicit client paths
// take precedence over this function. The default session is not a named folder.
func SocketPath() (string, error) {
	if path := os.Getenv("HERDR_SOCKET_PATH"); path != "" {
		return path, nil
	}
	name := os.Getenv("HERDR_SESSION")
	if name != "" && name != "default" {
		if len(name) > 64 || name == "." || name == ".." {
			return "", fmt.Errorf("invalid HERDR_SESSION name %q", name)
		}
		for _, char := range name {
			if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '.' || char == '_' || char == '-') {
				return "", fmt.Errorf("invalid HERDR_SESSION name %q", name)
			}
		}
	}
	base := os.Getenv("XDG_CONFIG_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("discover herdr socket: %w", err)
		}
		base = filepath.Join(home, ".config")
	}
	base = filepath.Join(base, "herdr")
	if name != "" && name != "default" {
		base = filepath.Join(base, "sessions", name)
	}
	return filepath.Join(base, "herdr.sock"), nil
}

func (c *Client) path() (string, error) {
	if c.socketPath != "" {
		return c.socketPath, nil
	}
	return SocketPath()
}

func (c *Client) timeout() time.Duration {
	if c.RequestTimeout > 0 {
		return c.RequestTimeout
	}
	return 5 * time.Second
}

func (c *Client) requestID() string {
	return "outpost-herdr-" + strconv.FormatUint(c.nextID.Add(1), 10)
}

func encodeRequest(id, method string, params any) ([]byte, error) {
	frame, err := json.Marshal(struct {
		ID     string `json:"id"`
		Method string `json:"method"`
		Params any    `json:"params"`
	}{id, method, params})
	if err != nil {
		return nil, fmt.Errorf("encode herdr request: %w", err)
	}
	if len(frame) > MaxFrameBytes {
		return nil, fmt.Errorf("herdr request exceeds %d-byte frame limit", MaxFrameBytes)
	}
	return append(frame, '\n'), nil
}

func writeFrame(conn net.Conn, frame []byte) error {
	for len(frame) > 0 {
		n, err := conn.Write(frame)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		frame = frame[n:]
	}
	return nil
}

// readFrame never accepts a valid-looking JSON document without its NDJSON
// delimiter. ReadSlice keeps memory bounded even if the peer never sends '\n'.
func readFrame(reader *bufio.Reader, limit int) ([]byte, error) {
	var frame []byte
	for {
		part, err := reader.ReadSlice('\n')
		terminated := len(part) > 0 && part[len(part)-1] == '\n'
		if terminated {
			part = part[:len(part)-1]
		}
		if len(part) > limit-len(frame) {
			return nil, fmt.Errorf("herdr response exceeds %d-byte frame limit", limit)
		}
		frame = append(frame, part...)
		if terminated {
			if !utf8.Valid(frame) {
				return nil, errors.New("herdr frame is not valid UTF-8")
			}
			return frame, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) && len(frame) > 0 {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
}

func decodeObject(raw []byte) (map[string]json.RawMessage, error) {
	var value map[string]json.RawMessage
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("invalid herdr JSON object: %w", err)
	}
	if value == nil {
		return nil, errors.New("herdr value must be a JSON object")
	}
	return value, nil
}

func decodeResponse(frame []byte, id string) (json.RawMessage, error) {
	envelope, err := decodeObject(frame)
	if err != nil {
		return nil, err
	}
	var responseID string
	if raw, ok := envelope["id"]; !ok || json.Unmarshal(raw, &responseID) != nil || responseID != id {
		return nil, errors.New("herdr response id does not match request")
	}
	result, hasResult := envelope["result"]
	remote, hasError := envelope["error"]
	if hasResult == hasError {
		return nil, errors.New("herdr response must contain exactly one result or error")
	}
	if hasError {
		body, err := decodeObject(remote)
		if err != nil {
			return nil, fmt.Errorf("invalid herdr error: %w", err)
		}
		var code, message string
		if !decodeString(body["code"], &code) || !decodeString(body["message"], &message) {
			return nil, errors.New("herdr error requires string code and message")
		}
		return nil, &APIError{Code: code, Message: message}
	}
	if _, err := decodeObject(result); err != nil {
		return nil, fmt.Errorf("invalid herdr result: %w", err)
	}
	return result, nil
}

func decodeString(raw json.RawMessage, value *string) bool {
	return len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) && json.Unmarshal(raw, value) == nil
}

func decodeResult(raw json.RawMessage, expected string) (map[string]json.RawMessage, error) {
	body, err := decodeObject(raw)
	if err != nil {
		return nil, err
	}
	var kind string
	if !decodeString(body["type"], &kind) || kind != expected {
		return nil, fmt.Errorf("herdr result type %q, expected %q", kind, expected)
	}
	return body, nil
}

func (c *Client) dial(ctx context.Context) (net.Conn, error) {
	path, err := c.path()
	if err != nil {
		return nil, err
	}
	return (&net.Dialer{}).DialContext(ctx, "unix", path)
}

func (c *Client) request(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id := c.requestID()
	frame, err := encodeRequest(id, method, params)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout())
	defer cancel()
	conn, err := c.dial(ctx)
	if err != nil {
		return nil, fmt.Errorf("herdr %s connect: %w", method, err)
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	if err = conn.SetDeadline(deadline); err == nil {
		err = writeFrame(conn, frame)
	}
	var response []byte
	if err == nil {
		response, err = readFrame(bufio.NewReader(conn), MaxFrameBytes)
	}
	if err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			err = context.DeadlineExceeded
		}
		return nil, fmt.Errorf("herdr %s: %w", method, err)
	}
	return decodeResponse(response, id)
}

// Snapshot retrieves the server's public session.snapshot result.
func (c *Client) Snapshot(ctx context.Context) (Snapshot, error) {
	raw, err := c.request(ctx, "session.snapshot", struct{}{})
	if err != nil {
		return Snapshot{}, err
	}
	body, err := decodeResult(raw, "session_snapshot")
	if err != nil {
		return Snapshot{}, err
	}
	fields, err := decodeObject(body["snapshot"])
	if err != nil {
		return Snapshot{}, fmt.Errorf("invalid herdr snapshot: %w", err)
	}
	var snapshot Snapshot
	if err := json.Unmarshal(body["snapshot"], &snapshot); err != nil {
		return Snapshot{}, fmt.Errorf("decode herdr snapshot: %w", err)
	}
	if !decodeString(fields["version"], &snapshot.Version) || snapshot.Version == "" || snapshot.Protocol <= 0 || snapshot.Agents == nil || snapshot.Panes == nil {
		return Snapshot{}, errors.New("herdr snapshot requires version, protocol, agents and panes")
	}
	for _, agent := range snapshot.Agents {
		if agent.PaneID == "" {
			return Snapshot{}, errors.New("herdr snapshot agent is missing pane_id")
		}
	}
	for _, pane := range snapshot.Panes {
		if pane.PaneID == "" {
			return Snapshot{}, errors.New("herdr snapshot pane is missing pane_id")
		}
	}
	return snapshot, nil
}

func (c *Client) ListAgents(ctx context.Context) ([]Agent, error) {
	raw, err := c.request(ctx, "agent.list", struct{}{})
	if err != nil {
		return nil, err
	}
	body, err := decodeResult(raw, "agent_list")
	if err != nil {
		return nil, err
	}
	var agents []Agent
	if err := json.Unmarshal(body["agents"], &agents); err != nil || agents == nil {
		return nil, errors.New("herdr agent_list requires an agents array")
	}
	return agents, nil
}

func (c *Client) ListPanes(ctx context.Context) ([]Pane, error) {
	raw, err := c.request(ctx, "pane.list", struct{}{})
	if err != nil {
		return nil, err
	}
	body, err := decodeResult(raw, "pane_list")
	if err != nil {
		return nil, err
	}
	var panes []Pane
	if err := json.Unmarshal(body["panes"], &panes); err != nil || panes == nil {
		return nil, errors.New("herdr pane_list requires a panes array")
	}
	return panes, nil
}

func (c *Client) GetAgent(ctx context.Context, target string) (Agent, error) {
	raw, err := c.request(ctx, "agent.get", map[string]string{"target": target})
	if err != nil {
		return Agent{}, err
	}
	body, err := decodeResult(raw, "agent_info")
	if err != nil {
		return Agent{}, err
	}
	if _, err := decodeObject(body["agent"]); err != nil {
		return Agent{}, fmt.Errorf("invalid herdr agent: %w", err)
	}
	var agent Agent
	if err := json.Unmarshal(body["agent"], &agent); err != nil {
		return Agent{}, fmt.Errorf("decode herdr agent: %w", err)
	}
	if agent.PaneID == "" {
		return Agent{}, errors.New("herdr agent is missing pane_id")
	}
	return agent, nil
}

func (c *Client) Prompt(ctx context.Context, target, text string) error {
	raw, err := c.request(ctx, "agent.prompt", map[string]string{"target": target, "text": text})
	if err != nil {
		return err
	}
	_, err = decodeResult(raw, "agent_prompted")
	return err
}

func (c *Client) SendKeys(ctx context.Context, target string, keys ...string) error {
	if keys == nil {
		keys = []string{}
	}
	raw, err := c.request(ctx, "agent.send_keys", struct {
		Target string   `json:"target"`
		Keys   []string `json:"keys"`
	}{target, keys})
	if err != nil {
		return err
	}
	_, err = decodeResult(raw, "ok")
	return err
}
