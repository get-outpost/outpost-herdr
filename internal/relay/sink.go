// Package relay connects the adapter to the public Outpost SDK.
package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/get-outpost/outpost-go"
	"github.com/get-outpost/outpost-go/client"
	"github.com/get-outpost/outpost-herdr/internal/adapter"
)

const (
	defaultQueueSize     = 256
	defaultCommandMaxAge = 2 * time.Minute
	minRetryDelay        = 200 * time.Millisecond
	maxRetryDelay        = 5 * time.Second
)

var (
	ErrQueueFull = errors.New("Outpost publication queue is full; stopping to avoid losing session transitions")
	ErrClosed    = errors.New("Outpost relay is not accepting publications")
)

// State durably stores the consumed event cursor and successfully published live
// sessions. Its implementation must serialize cursor and live-session updates.
type State interface {
	Cursor() (int64, bool)
	SetCursor(int64) error
	LiveSessions() []string
	RecordPublished(string, string) error
}

// Options configures bounded publication buffering and command freshness. Zero
// values select a 256-event queue, a two-minute age limit, and a stderr logger.
type Options struct {
	QueueSize     int
	CommandMaxAge time.Duration
	Logger        *slog.Logger
}

// Sink publishes in order on a single worker, independently of Start's context.
// Commands are unbuffered: their ownership and age are checked while waiting for
// the adapter, rather than accumulating commands for formerly local sessions.
type Sink struct {
	client          *client.Client
	state           State
	options         Options
	publications    chan outpost.Event
	commands        chan adapter.Command
	errors          chan error
	publisherCtx    context.Context
	cancelPublisher context.CancelFunc

	mu           sync.Mutex
	started      bool
	stopping     bool
	accepting    bool
	queueClosed  bool
	cancelReader context.CancelFunc
	fatalErr     error
	fatalOnce    sync.Once
	commandsOnce sync.Once
	doneOnce     sync.Once
	done         chan struct{}
	workers      sync.WaitGroup

	localMu          sync.Mutex
	local            map[string]struct{}
	localReady       chan struct{}
	localInitialized bool
	localChanged     chan struct{}
	localPending     chan struct{}
}

var _ adapter.Sink = (*Sink)(nil)

// New constructs an idle sink. Start must establish the command stream before
// Publish accepts events. The client is safe to share with other SDK callers.
func New(c *client.Client, state State, options Options) (*Sink, error) {
	if c == nil || state == nil {
		return nil, errors.New("Outpost relay requires a client and durable state")
	}
	if options.QueueSize < 0 || options.CommandMaxAge < 0 {
		return nil, errors.New("Outpost queue size and command maximum age must not be negative")
	}
	if options.QueueSize == 0 {
		options.QueueSize = defaultQueueSize
	}
	if options.CommandMaxAge == 0 {
		options.CommandMaxAge = defaultCommandMaxAge
	}
	if options.Logger == nil {
		options.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	// In particular, do not derive this context from the daemon's signal context:
	// SIGTERM stops command execution, but Shutdown still has time to flush.
	publisherCtx, cancelPublisher := context.WithCancel(context.Background())
	return &Sink{
		client: c, state: state, options: options,
		publications: make(chan outpost.Event, options.QueueSize),
		commands:     make(chan adapter.Command), errors: make(chan error, 1),
		publisherCtx: publisherCtx, cancelPublisher: cancelPublisher,
		done: make(chan struct{}), localReady: make(chan struct{}),
		localChanged: make(chan struct{}), local: make(map[string]struct{}),
	}, nil
}

// Start initializes a missing cursor at the current server head, persists it,
// and opens the stream before returning. It does not wait for a ready frame.
// Initial HTTP/stream transport and 5xx failures retry with 200ms–5s backoff;
// permanent HTTP rejections do not retry. A Sink can only be started once.
func (s *Sink) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started || s.stopping {
		s.mu.Unlock()
		return errors.New("Outpost relay cannot be started more than once or after shutdown")
	}
	s.started = true
	readerCtx, cancel := context.WithCancel(ctx)
	s.cancelReader = cancel
	s.mu.Unlock()

	after, exists := s.state.Cursor()
	if !exists {
		var snapshot client.Snapshot
		err := retry(readerCtx, func() error {
			var err error
			snapshot, err = s.client.ListSessions(readerCtx, false)
			return err
		})
		if err != nil {
			return s.startFailed(operationError("initialize stream cursor", err))
		}
		after = snapshot.AsOfSeq
		if after < 0 {
			return s.startFailed(errors.New("Outpost snapshot contains an invalid stream cursor"))
		}
		if err := s.state.SetCursor(after); err != nil {
			return s.startFailed(errors.New("persist initial Outpost stream cursor failed"))
		}
	}
	if after < 0 {
		return s.startFailed(errors.New("durable Outpost stream cursor must not be negative"))
	}

	var stream *client.Stream
	err := retry(readerCtx, func() error {
		var err error
		stream, err = s.client.Stream(readerCtx, after)
		return err
	})
	if err != nil {
		return s.startFailed(operationError("open command stream", err))
	}
	s.mu.Lock()
	if s.stopping || readerCtx.Err() != nil {
		s.mu.Unlock()
		_ = stream.Close()
		err := readerCtx.Err()
		if err == nil {
			err = context.Canceled
		}
		return s.startFailed(err)
	}
	s.accepting = true
	s.workers.Add(2)
	s.mu.Unlock()
	go func() {
		defer s.workers.Done()
		s.publishLoop()
	}()
	go func() {
		defer s.workers.Done()
		s.readLoop(readerCtx, stream)
	}()
	go func() {
		s.workers.Wait()
		s.cancelPublisher()
		s.doneOnce.Do(func() { close(s.done) })
	}()
	return nil
}

func (s *Sink) startFailed(err error) error {
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		s.fail(err)
	}
	s.mu.Lock()
	s.stopping = true
	s.stopAdmissionLocked()
	if s.cancelReader != nil {
		s.cancelReader()
	}
	s.mu.Unlock()
	s.cancelPublisher()
	s.closeCommands()
	s.doneOnce.Do(func() { close(s.done) })
	return err
}

// SetLocalSessions replaces the current authoritative local session identities.
// The first call, including an empty snapshot, releases the command reader.
func (s *Sink) SetLocalSessions(ids []string) {
	local := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id != "" {
			local[id] = struct{}{}
		}
	}
	s.localMu.Lock()
	s.local = local
	close(s.localChanged)
	s.localChanged = make(chan struct{})
	if !s.localInitialized {
		s.localInitialized = true
		close(s.localReady)
	}
	pending := s.localPending
	s.localMu.Unlock()
	// A command already handed off can execute concurrently with this call,
	// but none can be handed off using the old registry after it returns.
	if pending != nil {
		<-pending
	}
}

// Publish captures an immutable JSON payload and admits it without waiting for
// network I/O or queue space. Overflow is fatal instead of losing transitions.
func (s *Sink) Publish(ctx context.Context, event adapter.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var payload json.RawMessage
	if event.Payload != nil {
		var err error
		payload, err = json.Marshal(event.Payload)
		if err != nil {
			return errors.New("encode Outpost publication payload failed")
		}
	}
	publication := outpost.Event{Type: event.Type, SessionID: event.SessionID, Payload: payload}
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	if !s.accepting {
		s.mu.Unlock()
		return ErrClosed
	}
	select {
	case s.publications <- publication:
		s.mu.Unlock()
		return nil
	default:
		s.mu.Unlock()
		s.fail(ErrQueueFull)
		return ErrQueueFull
	}
}

// Commands returns a stable channel, closed when command reading stops.
func (s *Sink) Commands() <-chan adapter.Command { return s.commands }

// Errors delivers the first fatal error exactly once. The stable channel remains
// open, including after Shutdown; a closed channel is not an extra fatal signal.
func (s *Sink) Errors() <-chan error { return s.errors }

func (s *Sink) publishLoop() {
	for event := range s.publications {
		if s.publisherCtx.Err() != nil {
			return
		}
		err := retry(s.publisherCtx, func() error {
			_, err := s.client.PostEvent(s.publisherCtx, event)
			return err
		})
		if s.publisherCtx.Err() != nil {
			return
		}
		if err != nil {
			var apiError *client.APIError
			if errors.As(err, &apiError) && apiError.StatusCode != http.StatusUnauthorized {
				// Never log server messages, request URLs, payloads, or credentials.
				s.options.Logger.Warn("dropping permanently rejected Outpost publication", "status", apiError.StatusCode)
				continue
			}
			s.fail(operationError("publish event", err))
			return
		}
		// HTTP acknowledgements are not consumed stream events. In particular,
		// their sequence must never advance the durable stream cursor.
		if err := s.state.RecordPublished(event.Type, event.SessionID); err != nil {
			s.fail(errors.New("persist published Outpost session state failed"))
			return
		}
	}
}

func (s *Sink) readLoop(ctx context.Context, stream *client.Stream) {
	defer s.closeCommands()
	defer stream.Close()
	select {
	case <-s.localReady:
	case <-ctx.Done():
		return
	}
	for {
		// SDK Read owns reconnection and in-memory event deduplication after
		// the initial dial. It terminates on 401/403 and invalid frames; its
		// reconnect loop retries other handshake failures internally.
		frame, err := stream.Read(ctx)
		if err != nil {
			if ctx.Err() == nil {
				s.fail(operationError("read command stream", err))
			}
			return
		}
		if frame.Kind == outpost.FrameError {
			s.fail(errors.New("Outpost command stream reported an error"))
			return
		}
		if frame.Kind != outpost.FrameEvent {
			// Neither ready.HeadSeq nor ack.Event.Seq is a consumed event.
			continue
		}
		if frame.Event == nil {
			s.fail(errors.New("Outpost command stream event has no envelope"))
			return
		}
		event := frame.Event
		// Persist BEFORE dispatch for at-most-once execution across restarts.
		// A crash after this write but before execution can lose a command;
		// replaying an interrupt or prompt is intentionally less safe.
		if err := s.state.SetCursor(event.Seq); err != nil {
			s.fail(errors.New("persist Outpost stream cursor failed"))
			return
		}
		if event.Type != outpost.CommandMessage && event.Type != outpost.CommandInterrupt || event.Enc != "" {
			continue
		}
		if !s.dispatch(ctx, event) {
			return
		}
	}
}

func (s *Sink) dispatch(ctx context.Context, event *outpost.Envelope) bool {
	var payload map[string]any
	decoded := false
	for {
		s.localMu.Lock()
		_, local := s.local[event.SessionID]
		if !local {
			s.localMu.Unlock()
			return true
		}
		remaining := time.Until(event.TS.Add(s.options.CommandMaxAge))
		if remaining <= 0 {
			s.localMu.Unlock()
			s.options.Logger.Warn("dropping stale Outpost command", "type", event.Type, "age", time.Since(event.TS))
			return true
		}
		if !decoded {
			if len(event.Payload) != 0 {
				if err := json.Unmarshal(event.Payload, &payload); err != nil {
					s.localMu.Unlock()
					s.fail(errors.New("decode Outpost command payload failed"))
					return false
				}
			}
			decoded = true
		}
		remaining = time.Until(event.TS.Add(s.options.CommandMaxAge))
		if remaining <= 0 {
			s.localMu.Unlock()
			continue
		}
		changed := s.localChanged
		pending := make(chan struct{})
		s.localPending = pending
		s.localMu.Unlock()
		command := adapter.Command{ID: event.ID, Type: event.Type, SessionID: event.SessionID, Payload: payload}
		timer := time.NewTimer(remaining)
		select {
		case s.commands <- command:
			timer.Stop()
			s.finishDispatch(pending)
			return true
		case <-changed:
			timer.Stop()
		case <-timer.C:
			// Recheck freshness and log the same stale-drop path above.
		case <-ctx.Done():
			timer.Stop()
			s.finishDispatch(pending)
			return false
		}
		s.finishDispatch(pending)
	}
}

func (s *Sink) finishDispatch(pending chan struct{}) {
	s.localMu.Lock()
	s.localPending = nil
	close(pending)
	s.localMu.Unlock()
}

func (s *Sink) closeCommands() { s.commandsOnce.Do(func() { close(s.commands) }) }

func (s *Sink) stopAdmissionLocked() {
	s.accepting = false
	if !s.queueClosed {
		s.queueClosed = true
		close(s.publications)
	}
}

func (s *Sink) fail(err error) {
	s.fatalOnce.Do(func() {
		s.mu.Lock()
		s.fatalErr = err
		s.stopping = true
		s.stopAdmissionLocked()
		if s.cancelReader != nil {
			s.cancelReader()
		}
		s.mu.Unlock()
		s.errors <- err
	})
}

// Shutdown stops command reading and publication admission, then drains admitted
// events in order. Its context, not Start's signal context, bounds publication
// retries and in-flight HTTP calls. Expiration cancels both workers and joins
// them before returning. Concurrent calls and concurrent Publish are safe.
func (s *Sink) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.stopping = true
	s.stopAdmissionLocked()
	if s.cancelReader != nil {
		s.cancelReader()
	}
	if !s.started {
		s.cancelPublisher()
		s.closeCommands()
		s.doneOnce.Do(func() { close(s.done) })
	}
	s.mu.Unlock()
	select {
	case <-s.done:
		s.mu.Lock()
		err := s.fatalErr
		s.mu.Unlock()
		return err
	case <-ctx.Done():
		s.cancelPublisher()
		<-s.done
		return ctx.Err()
	}
}

func retry(ctx context.Context, operation func() error) error {
	delay := minRetryDelay
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := operation()
		if err == nil || !retryable(err) {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, maxRetryDelay)
	}
}

func retryable(err error) bool {
	var apiError *client.APIError
	if errors.As(err, &apiError) {
		return apiError.StatusCode >= 500 && apiError.StatusCode <= 599
	}
	var networkError net.Error
	return errors.As(err, &networkError) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// Do not propagate even SDK-redacted API messages: malicious response bodies and
// transport URLs can contain unrelated credentials. Status and operation suffice.
func operationError(operation string, err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	var apiError *client.APIError
	if errors.As(err, &apiError) {
		if apiError.StatusCode == http.StatusUnauthorized {
			return fmt.Errorf("Outpost %s failed: authentication rejected (HTTP 401); check device credentials", operation)
		}
		return fmt.Errorf("Outpost %s failed (HTTP %d)", operation, apiError.StatusCode)
	}
	return fmt.Errorf("Outpost %s failed", operation)
}
