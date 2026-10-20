package transcript

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	outpost "github.com/get-outpost/outpost-go"
	"github.com/get-outpost/outpost-herdr/internal/adapter"
	"github.com/get-outpost/outpost-herdr/internal/herdr"
	"github.com/get-outpost/outpost-herdr/internal/state"
)

const (
	maxTranscriptLine    = 16 * 1024 * 1024
	transcriptReadSize   = 64 * 1024
	transcriptAnchorSize = 4096
	backfillMessages     = 20
	deliveryQueueSize    = 64
	fallbackEchoWindow   = 2 * time.Minute
)

// Store shares the namespace's atomic file with relay cursor and lifecycle state.
type Store interface {
	Transcript(string) (state.TranscriptCheckpoint, bool)
	SetTranscript(string, state.TranscriptCheckpoint) error
}

type Options struct {
	PollInterval time.Duration
	Logger       *slog.Logger
}

type deliveredMessage struct {
	agent     herdr.Agent
	commandID string
	text      string
	at        time.Time
}

// Manager confines resolution, transcript I/O, publication, and checkpoints to one
// worker. Its lifetime is independent of the watch context, allowing a final scan
// before the relay drains. A successful publish is queue admission, not an HTTP
// acknowledgment; stable IDs cover replay ambiguity, not a durable HTTP outbox.
type Manager struct {
	sink        adapter.Sink
	store       Store
	namespace   string
	options     Options
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	started     bool
	closing     bool
	closed      bool
	fatal       error
	closeErr    error
	latest      []herdr.Agent
	hasSnapshot bool
	wake        chan struct{}
	deliveries  chan deliveredMessage
	shutdown    chan context.Context
	done        chan struct{}
	errors      chan error
}

func New(sink adapter.Sink, store Store, namespace string, options Options) (*Manager, error) {
	if sink == nil || store == nil {
		return nil, errors.New("transcript manager requires a sink and store")
	}
	if strings.TrimSpace(namespace) == "" {
		return nil, errors.New("transcript namespace must not be blank")
	}
	if options.PollInterval < 0 {
		return nil, errors.New("transcript poll interval must not be negative")
	}
	if options.PollInterval == 0 {
		options.PollInterval = 250 * time.Millisecond
	}
	if options.Logger == nil {
		options.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		sink: sink, store: store, namespace: namespace, options: options,
		ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1),
		deliveries: make(chan deliveredMessage, deliveryQueueSize),
		shutdown:   make(chan context.Context, 1), done: make(chan struct{}),
		errors: make(chan error, 1),
	}, nil
}

// Start is idempotent while running; worker failures are also reported by Errors.
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fatal != nil {
		return m.fatal
	}
	if m.closing || m.closed {
		return errors.New("transcript manager is closing")
	}
	m.startLocked()
	return nil
}

func (m *Manager) startLocked() {
	if !m.started {
		m.started = true
		go m.run()
	}
}

// UpdateSessions coalesces authoritative snapshots without disk I/O. Copying the
// session reference protects the worker from caller-owned pointer mutations.
func (m *Manager) UpdateSessions(agents []herdr.Agent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing || m.closed || m.fatal != nil {
		return
	}
	m.latest = make([]herdr.Agent, len(agents))
	for i, agent := range agents {
		m.latest[i] = copyAgent(agent)
	}
	m.hasSnapshot = true
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func copyAgent(agent herdr.Agent) herdr.Agent {
	if agent.AgentSession != nil {
		ref := *agent.AgentSession
		agent.AgentSession = &ref
	}
	return agent
}

// Delivered is called only after native Prompt admission. It does not resolve a
// path, touch the store, or publish. Overflow is fatal rather than a lost prompt.
// Caller cancellation cannot undo native admission, so notification still queues
// when its callback context has been canceled. Manager shutdown bounds delivery.
func (m *Manager) Delivered(_ context.Context, agent herdr.Agent, command adapter.Command) error {
	if command.Type != "command.message" {
		return nil
	}
	text, ok := command.Payload["text"].(string)
	if !ok || strings.TrimSpace(text) == "" || strings.TrimSpace(command.ID) == "" {
		return errors.New("delivered message requires text and command identity")
	}
	notification := deliveredMessage{
		agent: copyAgent(agent), commandID: command.ID,
		text: BoundText(text), at: time.Now().UTC(),
	}
	m.mu.Lock()
	if m.fatal != nil {
		err := m.fatal
		m.mu.Unlock()
		return err
	}
	if m.closing || m.closed {
		m.mu.Unlock()
		return errors.New("transcript manager is closing")
	}
	select {
	case m.deliveries <- notification:
		m.mu.Unlock()
		select {
		case m.wake <- struct{}{}:
		default:
		}
		return nil
	default:
		m.mu.Unlock()
		err := errors.New("transcript delivery queue is full")
		m.fail(err)
		return err
	}
}

// Errors is stable and receives only the first fatal failure. It is not closed.
func (m *Manager) Errors() <-chan error { return m.errors }

func (m *Manager) fail(err error) {
	m.mu.Lock()
	if m.fatal == nil {
		m.fatal = err
		m.errors <- err
	}
	m.mu.Unlock()
	m.cancel()
}

// Close processes queued admissions and a final bounded-memory scan, then joins
// the worker. Call it before closing the relay, with the same fresh flush deadline.
// Close before Start is safe and still handles already queued notifications.
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	if !m.closing && !m.closed {
		m.closing = true
		m.shutdown <- ctx
		m.startLocked()
	}
	m.mu.Unlock()
	select {
	case <-m.done:
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.fatal != nil {
			return m.fatal
		}
		return m.closeErr
	case <-ctx.Done():
		m.cancel()
		return ctx.Err()
	}
}

type tailSession struct {
	id           string
	agent        herdr.Agent
	checkpoint   state.TranscriptCheckpoint
	seen         map[string]struct{}
	file         *os.File
	info         os.FileInfo
	path         string
	reference    transcriptReference
	fileProvider string
	readOffset   int64
	partial      []byte
	oversized    bool
	dirty        bool
	backfill     bool
}

type transcriptReference struct {
	provider string
	hasRef   bool
	ref      herdr.SessionRef
}

type tailWorker struct {
	manager  *Manager
	sessions map[string]*tailSession
	buffer   []byte
}

func (m *Manager) run() {
	worker := &tailWorker{manager: m, sessions: make(map[string]*tailSession), buffer: make([]byte, transcriptReadSize)}
	defer func() {
		for _, session := range worker.sessions {
			if session.file != nil {
				session.file.Close()
			}
		}
		m.cancel()
		m.mu.Lock()
		m.closed = true
		fatal := m.fatal
		m.mu.Unlock()
		if fatal != nil {
			m.options.Logger.Error("transcript worker stopped", "error", fatal)
		}
		close(m.done)
	}()
	ticker := time.NewTicker(m.options.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case flushCtx := <-m.shutdown:
			ctx, cancel := context.WithCancel(flushCtx)
			stop := context.AfterFunc(m.ctx, cancel)
			err := worker.cycle(ctx)
			stop()
			cancel()
			if err != nil {
				if flushCtx.Err() == nil && m.ctx.Err() == nil {
					m.fail(err)
				} else {
					m.mu.Lock()
					m.closeErr = err
					m.mu.Unlock()
				}
			}
			return
		case <-m.wake:
		case <-ticker.C:
		}
		if err := worker.cycle(m.ctx); err != nil {
			if m.ctx.Err() == nil {
				m.fail(err)
			}
			return
		}
	}
}

func (w *tailWorker) cycle(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := w.applySnapshot(ctx); err != nil {
		return err
	}
	// Notifications are bounded. A producer can keep filling the queue without
	// delaying a polling scan or shutdown indefinitely.
	for range deliveryQueueSize {
		select {
		case delivery := <-w.manager.deliveries:
			if err := w.delivered(ctx, delivery); err != nil {
				return err
			}
		default:
			return w.scanAll(ctx)
		}
	}
	return w.scanAll(ctx)
}

func (w *tailWorker) applySnapshot(ctx context.Context) error {
	m := w.manager
	m.mu.Lock()
	agents, changed := m.latest, m.hasSnapshot
	m.latest, m.hasSnapshot = nil, false
	m.mu.Unlock()
	if !changed {
		return nil
	}
	// Match adapter routing's deterministic representative for shared sessions.
	sort.Slice(agents, func(i, j int) bool { return agents[i].PaneID < agents[j].PaneID })
	active := make(map[string]struct{}, len(agents))
	for _, agent := range agents {
		if strings.TrimSpace(agent.Agent) == "" {
			continue
		}
		id, err := adapter.SessionID(m.namespace, agent)
		if err != nil {
			return errors.New("transcript agent has no session identity")
		}
		if _, exists := active[id]; exists {
			continue
		}
		active[id] = struct{}{}
		w.session(id, agent)
	}
	for id, session := range w.sessions {
		if _, exists := active[id]; exists {
			continue
		}
		if _, err := w.scan(ctx, session); err != nil {
			return err
		}
		if session.file != nil {
			session.file.Close()
		}
		delete(w.sessions, id)
	}
	return nil
}

func (w *tailWorker) session(id string, agent herdr.Agent) *tailSession {
	if session := w.sessions[id]; session != nil {
		session.agent = agent
		return session
	}
	checkpoint, _ := w.manager.store.Transcript(id)
	session := &tailSession{id: id, agent: agent, checkpoint: checkpoint, seen: make(map[string]struct{}, len(checkpoint.SeenIDs))}
	for _, messageID := range checkpoint.SeenIDs {
		session.seen[messageID] = struct{}{}
	}
	w.sessions[id] = session
	return session
}

func (w *tailWorker) scanAll(ctx context.Context) error {
	ids := make([]string, 0, len(w.sessions))
	for id := range w.sessions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if _, err := w.scan(ctx, w.sessions[id]); err != nil {
			return err
		}
	}
	return nil
}

func (w *tailWorker) delivered(ctx context.Context, delivery deliveredMessage) error {
	id, err := adapter.SessionID(w.manager.namespace, delivery.agent)
	if err != nil {
		return errors.New("delivered message has no session identity")
	}
	session := w.session(id, delivery.agent)
	available, err := w.scan(ctx, session)
	if err != nil {
		return err
	}
	// Claude's hook owns every user bubble, including when no file is available.
	if available || provider(delivery.agent) == outpost.ProviderClaude {
		return nil
	}
	messageID := "herdr-command:" + digest(fmt.Sprintf("%d:%s%s", len(id), id, delivery.commandID))
	if _, exists := session.seen[messageID]; exists {
		return nil
	}
	if err := w.publish(ctx, session.id, Message{Role: "user", Text: delivery.text}, messageID); err != nil {
		return err
	}
	session.remember(messageID)
	session.checkpoint.Fallbacks = append(session.checkpoint.Fallbacks, state.TranscriptFallback{
		MessageID: messageID, CommandHash: digest(delivery.commandID),
		TextHash: digest(delivery.text), DeliveredAt: delivery.at,
	})
	return w.persist(session)
}

func provider(agent herdr.Agent) string { return strings.ToLower(strings.TrimSpace(agent.Agent)) }

// scan drains an already-open descriptor before switching a renamed/replaced
// path. Each read is limited to the stat size captured at scan entry, so a busy
// writer cannot turn one polling pass into an unbounded chase of file growth.
func (w *tailWorker) scan(ctx context.Context, session *tailSession) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	reference := transcriptReference{provider: provider(session.agent)}
	if session.agent.AgentSession != nil {
		reference.hasRef = true
		reference.ref = *session.agent.AgentSession
	}
	if session.file != nil {
		if err := w.read(ctx, session); err != nil {
			return false, err
		}
	}
	if session.reference != reference {
		if session.file != nil {
			session.file.Close()
			session.file = nil
		}
		session.path, session.reference = "", reference
	}
	path, unchanged := session.path, false
	if path != "" {
		info, err := os.Stat(path)
		switch {
		case errors.Is(err, os.ErrNotExist):
			path, session.path = "", ""
		case err != nil:
			return false, errors.New("inspect transcript path failed")
		case !info.Mode().IsRegular():
			path, session.path = "", ""
		default:
			unchanged = session.file != nil && os.SameFile(session.info, info)
		}
	}
	if path == "" {
		var err error
		path, err = Resolve(session.agent)
		if err != nil {
			return false, errors.New("resolve transcript failed")
		}
		session.path = path
	}
	if path != "" && !unchanged {
		// Nonblocking open also makes a regular-file-to-FIFO replacement safe.
		file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, errors.New("open transcript failed")
		}
		if err == nil {
			info, statErr := file.Stat()
			if statErr != nil {
				file.Close()
				return false, errors.New("inspect transcript failed")
			}
			if !info.Mode().IsRegular() {
				file.Close()
			} else if session.file != nil && os.SameFile(session.info, info) {
				file.Close()
			} else {
				if session.file != nil {
					session.file.Close()
				}
				session.file, session.info = file, info
				session.fileProvider = reference.provider
				session.readOffset = session.checkpoint.Offset
				session.partial, session.oversized = nil, false
				if err := w.read(ctx, session); err != nil {
					return false, err
				}
			}
		}
	}
	w.expireFallbacks(session, time.Now())
	if err := w.persist(session); err != nil {
		return false, err
	}
	return session.file != nil, nil
}

func fileIdentity(info os.FileInfo) string {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return digest(fmt.Sprintf("%d:%d", stat.Dev, stat.Ino))
	}
	return digest(fmt.Sprintf("%d:%d", info.Size(), info.ModTime().UnixNano()))
}

func digest(text string) string {
	hash := sha256.Sum256([]byte(text))
	return hex.EncodeToString(hash[:])
}

func anchor(file *os.File, offset int64) (string, error) {
	if offset == 0 {
		return "", nil
	}
	size := min(offset, int64(transcriptAnchorSize))
	var buffer [transcriptAnchorSize]byte
	if _, err := file.ReadAt(buffer[:size], offset-size); err != nil {
		if errors.Is(err, io.EOF) {
			return "", io.EOF
		}
		return "", errors.New("read transcript anchor failed")
	}
	hash := sha256.Sum256(buffer[:size])
	return hex.EncodeToString(hash[:]), nil
}

func (w *tailWorker) read(ctx context.Context, session *tailSession) error {
	info, err := session.file.Stat()
	if err != nil {
		return errors.New("inspect open transcript failed")
	}
	identity := fileIdentity(info)
	checkpoint := &session.checkpoint
	reset := !checkpoint.Initialized || checkpoint.FileID != identity || info.Size() < session.readOffset
	if !reset && checkpoint.Offset > 0 {
		current, err := anchor(session.file, checkpoint.Offset)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			reset = true
		} else if checkpoint.Anchor != current {
			reset = true
		}
	}
	if reset {
		session.backfill = !checkpoint.Initialized
		checkpoint.Initialized, checkpoint.Offset, checkpoint.FileID, checkpoint.Anchor = true, 0, identity, ""
		session.readOffset = 0
		session.partial, session.oversized = nil, false
		session.dirty = true
	}
	var recent [backfillMessages]queuedMessage
	count := 0
	for session.readOffset < info.Size() {
		if err := ctx.Err(); err != nil {
			return err
		}
		amount := min(int64(len(w.buffer)), info.Size()-session.readOffset)
		n, err := session.file.ReadAt(w.buffer[:amount], session.readOffset)
		if err != nil && err != io.EOF {
			return errors.New("read transcript failed")
		}
		if n == 0 {
			break
		}
		chunk := w.buffer[:n]
		position := session.readOffset
		for len(chunk) > 0 {
			newline := bytes.IndexByte(chunk, '\n')
			length := len(chunk)
			if newline >= 0 {
				length = newline
			}
			segment := chunk[:length]
			if !session.oversized && len(session.partial)+len(segment) > maxTranscriptLine {
				session.oversized = true
				session.partial = nil
			}
			if newline < 0 {
				if !session.oversized {
					session.partial = append(session.partial, segment...)
				}
				break
			}
			if !session.oversized {
				line := segment
				if len(session.partial) > 0 {
					session.partial = append(session.partial, segment...)
					line = session.partial
				}
				message, ok := Parse(session.fileProvider, line)
				if ok && (session.fileProvider != outpost.ProviderClaude || message.Role == "assistant") {
					messageID := StableMessageID(session.id, message, checkpoint.Offset, line)
					if _, exists := session.seen[messageID]; !exists {
						if !w.suppressEcho(session, message) {
							if session.backfill {
								recent[count%backfillMessages] = queuedMessage{message: message, id: messageID}
								count++
							} else if err := w.publish(ctx, session.id, message, messageID); err != nil {
								return err
							}
						}
						session.remember(messageID)
					}
				}
			}
			position += int64(length + 1)
			checkpoint.Offset = position
			session.dirty = true
			session.partial = session.partial[:0]
			session.oversized = false
			chunk = chunk[length+1:]
		}
		session.readOffset += int64(n)
	}
	if session.backfill {
		for i := max(0, count-backfillMessages); i < count; i++ {
			queued := recent[i%backfillMessages]
			if err := w.publish(ctx, session.id, queued.message, queued.id); err != nil {
				return err
			}
		}
		session.backfill = false
	}
	if session.dirty {
		checkpoint.Anchor, err = anchor(session.file, checkpoint.Offset)
		if errors.Is(err, io.EOF) {
			// A concurrent copy-truncate may race either stat or the final
			// anchor read. Preserve admitted IDs, but rescan the new file.
			checkpoint.Offset, checkpoint.Anchor = 0, ""
			session.readOffset = 0
			session.partial, session.oversized = nil, false
		} else if err != nil {
			return err
		}
	}
	session.info = info
	return nil
}

type queuedMessage struct {
	message Message
	id      string
}

func (session *tailSession) remember(id string) {
	session.seen[id] = struct{}{}
	session.checkpoint.SeenIDs = append(session.checkpoint.SeenIDs, id)
	session.dirty = true
}

// Matching text and nearby timestamps alone are ambiguous: two legitimate turns
// can be identical. Only a native ID equal to the admitted command ID, plus its
// bounded-text hash and delivery window, proves an echo. Providers with unrelated
// native IDs may therefore display both fallback and later native user bubbles.
func (w *tailWorker) suppressEcho(session *tailSession, message Message) bool {
	if message.Role != "user" || message.ID == "" || message.Timestamp.IsZero() {
		return false
	}
	for i, fallback := range session.checkpoint.Fallbacks {
		if digest(message.ID) != fallback.CommandHash || digest(message.Text) != fallback.TextHash {
			continue
		}
		if message.Timestamp.Before(fallback.DeliveredAt.Add(-2*time.Second)) || message.Timestamp.After(fallback.DeliveredAt.Add(fallbackEchoWindow)) {
			continue
		}
		session.checkpoint.Fallbacks = append(session.checkpoint.Fallbacks[:i], session.checkpoint.Fallbacks[i+1:]...)
		session.dirty = true
		return true
	}
	return false
}

func (w *tailWorker) expireFallbacks(session *tailSession, now time.Time) {
	fallbacks := session.checkpoint.Fallbacks
	kept := fallbacks[:0]
	for _, fallback := range fallbacks {
		if !now.After(fallback.DeliveredAt.Add(fallbackEchoWindow)) {
			kept = append(kept, fallback)
		}
	}
	if len(kept) != len(fallbacks) {
		session.checkpoint.Fallbacks = kept
		session.dirty = true
	}
}

func (w *tailWorker) publish(ctx context.Context, sessionID string, message Message, messageID string) error {
	if err := w.manager.sink.Publish(ctx, adapter.Event{
		Type: outpost.MessageCreated, SessionID: sessionID,
		Payload: map[string]any{"role": message.Role, "text": BoundText(message.Text), "message_id": messageID},
	}); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("publish transcript message failed")
	}
	return nil
}

func (w *tailWorker) persist(session *tailSession) error {
	if !session.dirty {
		return nil
	}
	if err := w.manager.store.SetTranscript(session.id, session.checkpoint); err != nil {
		return errors.New("persist transcript checkpoint failed")
	}
	session.dirty = false
	return nil
}
