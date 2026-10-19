package adapter

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/get-outpost/outpost-herdr/internal/herdr"
)

// Adapter translates authoritative herdr snapshots and executes commands for
// sessions currently present in those snapshots. Its state is in memory only.
type Adapter struct {
	source    herdr.Source
	sink      Sink
	namespace string
	running   atomic.Bool
}

// New constructs an adapter. namespace must be a stable, nonblank identifier for
// this herdr installation, so fallback identities do not collide across devices.
// Invalid configuration is reported by Run before any I/O.
func New(source herdr.Source, sink Sink, namespace string) *Adapter {
	return &Adapter{source: source, sink: sink, namespace: namespace}
}

// SessionID preserves a nonblank agent_session kind=id value byte-for-byte,
// allowing native hook events and herdr events to address the same session.
// Otherwise it returns "herdr:" plus a SHA-256 digest of length-prefixed fields:
// namespace, "path", source, reference agent, path for kind=path; or namespace,
// "terminal", agent kind, terminal ID; or namespace, "pane", agent kind, pane ID.
// Paths are not cleaned or resolved, and Revision is not an identity component.
func SessionID(namespace string, agent herdr.Agent) (string, error) {
	if strings.TrimSpace(namespace) == "" {
		return "", errors.New("adapter namespace must not be blank")
	}
	if ref := agent.AgentSession; ref != nil && strings.TrimSpace(ref.Value) != "" {
		switch ref.Kind {
		case "id":
			return ref.Value, nil
		case "path":
			return fallbackID(namespace, "path", ref.Source, ref.Agent, ref.Value), nil
		}
	}
	if agent.TerminalID != "" {
		return fallbackID(namespace, "terminal", agent.Agent, agent.TerminalID), nil
	}
	if agent.PaneID != "" {
		return fallbackID(namespace, "pane", agent.Agent, agent.PaneID), nil
	}
	return "", errors.New("agent has no native session, path, terminal, or pane identity")
}

func fallbackID(parts ...string) string {
	hash := sha256.New()
	var size [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(part))
	}
	return "herdr:" + hex.EncodeToString(hash.Sum(nil))
}

type snapshotRequest struct {
	snapshot herdr.Snapshot
	result   chan error
}

type session struct {
	agents []herdr.Agent
	status string
}

type runner struct {
	adapter  *Adapter
	sessions map[string]session
}

// Run watches snapshots and commands until cancellation or an error. Commands
// remain queued until the first snapshot establishes local session ownership.
// Reconciliation and execution share one consumer, so that local ownership
// cannot change halfway through a command. Every executable command revalidates
// its target with GetAgent. Command errors are returned with their ID; commands
// are never retried. Unsupported, foreign, approval, and encrypted commands are
// ignored. Concurrent Run calls are rejected; each later Run starts fresh.
func (a *Adapter) Run(ctx context.Context) error {
	if strings.TrimSpace(a.namespace) == "" {
		return errors.New("adapter namespace must not be blank")
	}
	if a.source == nil || a.sink == nil {
		return errors.New("adapter requires a source and a sink")
	}
	if !a.running.CompareAndSwap(false, true) {
		return errors.New("adapter is already running")
	}
	defer a.running.Store(false)

	runCtx, cancel := context.WithCancel(ctx)
	requests := make(chan snapshotRequest)
	watchDone := make(chan struct{})
	var watchErr error
	go func() {
		defer close(watchDone)
		watchErr = a.source.Watch(runCtx, func(snapshot herdr.Snapshot) error {
			request := snapshotRequest{snapshot: snapshot, result: make(chan error, 1)}
			select {
			case requests <- request:
			case <-runCtx.Done():
				return runCtx.Err()
			}
			select {
			case err := <-request.result:
				return err
			case <-runCtx.Done():
				return runCtx.Err()
			}
		})
	}()
	defer func() {
		cancel()
		<-watchDone
	}()

	r := runner{adapter: a, sessions: make(map[string]session)}
	var commands <-chan Command
	initialized := false
	for {
		select {
		case <-runCtx.Done():
			return runCtx.Err()
		case <-watchDone:
			return watchErr
		case request := <-requests:
			err := r.reconcile(runCtx, request.snapshot)
			request.result <- err
			if err != nil {
				return err
			}
			if !initialized {
				commands = a.sink.Commands()
				initialized = true
			}
		case command, ok := <-commands:
			if !ok {
				commands = nil
				continue
			}
			if err := r.execute(runCtx, command); err != nil {
				return fmt.Errorf("command %q (%s): %w", command.ID, command.Type, err)
			}
		}
	}
}

func (r *runner) reconcile(ctx context.Context, snapshot herdr.Snapshot) error {
	next := make(map[string]session)
	for _, agent := range snapshot.Agents {
		if strings.TrimSpace(agent.Agent) == "" {
			continue
		}
		if agent.PaneID == "" {
			return errors.New("snapshot contains an agent without a pane ID")
		}
		id, err := SessionID(r.adapter.namespace, agent)
		if err != nil {
			return fmt.Errorf("session identity for pane %q: %w", agent.PaneID, err)
		}
		current := next[id]
		current.agents = append(current.agents, agent)
		next[id] = current
	}
	for id, current := range next {
		sort.Slice(current.agents, func(i, j int) bool {
			return current.agents[i].PaneID < current.agents[j].PaneID
		})
		next[id] = current
	}

	for _, id := range sortedSessionIDs(r.sessions) {
		if _, exists := next[id]; !exists {
			if err := r.publish(ctx, "session.ended", id, nil); err != nil {
				return err
			}
		}
	}
	for _, id := range sortedSessionIDs(next) {
		current := next[id]
		previous, exists := r.sessions[id]
		agent := current.agents[0]
		if !exists {
			payload := map[string]any{
				"provider": agent.Agent,
				"cwd":      firstNonblank(agent.ForegroundCwd, agent.Cwd),
				"title":    firstNonblank(agent.Name, agent.Title, agent.TerminalTitleStripped, agent.TerminalTitle, agent.DisplayAgent, agent.Agent),
			}
			if err := r.publish(ctx, "session.started", id, payload); err != nil {
				return err
			}
		}
		current.status = status(agent.AgentStatus)
		if current.status == "" {
			current.status = previous.status
		}
		if current.status != "" && current.status != previous.status {
			if err := r.publish(ctx, "session.updated", id, map[string]any{"status": current.status}); err != nil {
				return err
			}
		}
		next[id] = current
	}
	r.sessions = next
	return nil
}

func (r *runner) publish(ctx context.Context, kind, id string, payload map[string]any) error {
	if err := r.adapter.sink.Publish(ctx, Event{Type: kind, SessionID: id, Payload: payload}); err != nil {
		return fmt.Errorf("publish %s for session %q: %w", kind, id, err)
	}
	return nil
}

func sortedSessionIDs(sessions map[string]session) []string {
	ids := make([]string, 0, len(sessions))
	for id := range sessions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func status(value string) string {
	switch value {
	case "working", "idle", "blocked":
		return value
	case "done":
		return "idle"
	default:
		return ""
	}
}

func firstNonblank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (r *runner) execute(ctx context.Context, command Command) error {
	if command.Enc != "" {
		return nil
	}
	if command.Type != "command.message" && command.Type != "command.interrupt" {
		return nil
	}
	owned, exists := r.sessions[command.SessionID]
	if !exists {
		return nil
	}
	var text string
	if command.Type == "command.message" {
		var ok bool
		text, ok = command.Payload["text"].(string)
		if !ok || strings.TrimSpace(text) == "" {
			return errors.New("message text must be a nonblank string")
		}
	}

	// A native session can be visible in more than one pane. The sorted first
	// pane is the only target, and a failed revalidation never falls through to
	// a second pane or retries a potentially already-delivered prompt.
	target := owned.agents[0]
	live, err := r.adapter.source.GetAgent(ctx, target.PaneID)
	if err != nil {
		return fmt.Errorf("revalidate pane %q: %w", target.PaneID, err)
	}
	id, err := SessionID(r.adapter.namespace, live)
	if err != nil {
		return fmt.Errorf("revalidate pane %q: %w", target.PaneID, err)
	}
	if live.PaneID != target.PaneID || live.Agent != target.Agent || live.TerminalID != target.TerminalID || id != command.SessionID {
		return fmt.Errorf("pane %q no longer identifies session %q", target.PaneID, command.SessionID)
	}
	if command.Type == "command.message" {
		if err := r.adapter.source.Prompt(ctx, target.PaneID, text); err != nil {
			return fmt.Errorf("prompt pane %q: %w", target.PaneID, err)
		}
		return nil
	}
	if err := r.adapter.source.SendKeys(ctx, target.PaneID, "esc"); err != nil {
		return fmt.Errorf("interrupt pane %q: %w", target.PaneID, err)
	}
	return nil
}
