package herdr

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"sort"
	"sync/atomic"
	"time"
)

type subscription struct {
	Type   string `json:"type"`
	PaneID string `json:"pane_id,omitempty"`
}

// These lifecycle subscriptions are global in the installed protocol 22 schema.
// In particular pane.exited must not carry the per-pane status filter.
func subscriptions(panes []string) []subscription {
	result := make([]subscription, 0, 8+len(panes))
	for _, kind := range []string{
		"workspace.closed", "tab.closed", "pane.created", "pane.closed",
		"pane.updated", "pane.moved", "pane.exited", "pane.agent_detected",
	} {
		result = append(result, subscription{Type: kind})
	}
	for _, pane := range panes {
		result = append(result, subscription{Type: "pane.agent_status_changed", PaneID: pane})
	}
	return result
}

func snapshotPanes(snapshot Snapshot) []string {
	set := make(map[string]struct{}, len(snapshot.Panes)+len(snapshot.Agents))
	for _, pane := range snapshot.Panes {
		set[pane.PaneID] = struct{}{}
	}
	for _, agent := range snapshot.Agents {
		set[agent.PaneID] = struct{}{}
	}
	panes := make([]string, 0, len(set))
	for pane := range set {
		panes = append(panes, pane)
	}
	sort.Strings(panes)
	return panes
}

type eventStream struct {
	ctx      context.Context
	cancel   context.CancelFunc
	conn     net.Conn
	stop     func() bool
	id       string
	panes    []string
	dirty    chan struct{}
	failed   chan error
	done     chan struct{}
	sequence atomic.Uint64
}

func (s *eventStream) close() {
	s.cancel()
	s.conn.Close()
	<-s.done
	s.stop()
}

func (s *eventStream) failure(fallback error) error {
	select {
	case err := <-s.failed:
		return err
	default:
		return fallback
	}
}

// read runs independently from authoritative reads and slow consumers. Events
// are coalesced invalidations, not queued payloads that could overwrite a newer
// snapshot. The sequence detects invalidations received during a snapshot read.
func (s *eventStream) read(reader *bufio.Reader) {
	defer close(s.done)
	for {
		frame, err := readFrame(reader, MaxFrameBytes)
		if err == nil {
			var envelope map[string]json.RawMessage
			envelope, err = decodeObject(frame)
			if err == nil {
				if event, ok := envelope["event"]; ok {
					var kind string
					if !decodeString(event, &kind) || kind == "" {
						err = errors.New("herdr event requires a nonempty event name")
					} else if _, err = decodeObject(envelope["data"]); err == nil {
						s.sequence.Add(1)
						select {
						case s.dirty <- struct{}{}:
						default:
						}
					}
				} else {
					// events_lost is a response correlated to the original
					// subscribe request, and always requires a new stream.
					_, err = decodeResponse(frame, s.id)
					if err == nil {
						err = errors.New("unexpected herdr response after subscription acknowledgement")
					}
				}
			}
		}
		if err != nil {
			s.failed <- err
			s.cancel()
			return
		}
	}
}

func (c *Client) openStream(ctx context.Context, panes []string) (*eventStream, error) {
	id := c.requestID()
	frame, err := encodeRequest(id, "events.subscribe", struct {
		Subscriptions []subscription `json:"subscriptions"`
	}{subscriptions(panes)})
	if err != nil {
		return nil, err
	}
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout())
	defer cancel()
	conn, err := c.dial(requestCtx)
	if err != nil {
		return nil, fmt.Errorf("herdr subscription connect: %w", err)
	}
	stop := context.AfterFunc(requestCtx, func() { conn.Close() })
	defer stop()
	ok := false
	defer func() {
		if !ok {
			conn.Close()
		}
	}()
	deadline, _ := requestCtx.Deadline()
	if err = conn.SetDeadline(deadline); err == nil {
		err = writeFrame(conn, frame)
	}
	reader := bufio.NewReader(conn)
	var response []byte
	if err == nil {
		response, err = readFrame(reader, MaxFrameBytes)
	}
	if err != nil {
		if requestCtx.Err() != nil {
			err = requestCtx.Err()
		} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			err = context.DeadlineExceeded
		}
		return nil, fmt.Errorf("herdr subscription acknowledgement: %w", err)
	}
	raw, err := decodeResponse(response, id)
	if err != nil {
		return nil, err
	}
	if _, err = decodeResult(raw, "subscription_started"); err != nil {
		return nil, err
	}
	if err = conn.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	streamCtx, streamCancel := context.WithCancel(ctx)
	s := &eventStream{
		ctx:    streamCtx,
		cancel: streamCancel,
		conn:   conn,
		stop:   context.AfterFunc(streamCtx, func() { conn.Close() }),
		id:     id,
		panes:  slices.Clone(panes),
		dirty:  make(chan struct{}, 1),
		failed: make(chan error, 1),
		done:   make(chan struct{}),
	}
	go s.read(reader)
	ok = true
	return s, nil
}

type consumerError struct{ err error }

func (e *consumerError) Error() string { return e.err.Error() }
func (e *consumerError) Unwrap() error { return e.err }

// watchConnected starts with globals only, avoiding a stale pane list that can
// reject the entire subscription. A changed topology replaces the stream and
// receives an acknowledgement BEFORE taking the snapshot delivered to consumers.
func (c *Client) watchConnected(ctx context.Context, consume func(Snapshot) error) error {
	stream, err := c.openStream(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { stream.close() }()
	interval := c.ReconcileInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stream.dirty:
		default:
		}
		before := stream.sequence.Load()
		snapshot, err := c.Snapshot(stream.ctx)
		if err != nil {
			return stream.failure(err)
		}
		if stream.ctx.Err() != nil {
			return stream.failure(stream.ctx.Err())
		}
		panes := snapshotPanes(snapshot)
		if !slices.Equal(stream.panes, panes) {
			replacement, err := c.openStream(ctx, panes)
			if err != nil {
				// A pane may have vanished between discovery and subscribe.
				// Reconnect with globals rather than retrying stale targets.
				return err
			}
			stream.close()
			stream = replacement
			continue
		}
		if err := consume(snapshot); err != nil {
			return &consumerError{err: err}
		}
		if stream.sequence.Load() != before {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stream.ctx.Done():
			return stream.failure(stream.ctx.Err())
		case <-stream.dirty:
		case <-ticker.C:
			// The server has no shared event/snapshot watermark. Periodic
			// authoritative reconciliation also catches silent state loss.
		}
	}
}

// Watch reconnects indefinitely after transport/subscribe failures. A consumer
// error is returned unchanged, never retried. Cancellation closes all stream and
// in-flight snapshot I/O; recovery restores current state, not missed history.
func (c *Client) Watch(ctx context.Context, consume func(Snapshot) error) error {
	if consume == nil {
		return errors.New("herdr watch requires a consumer")
	}
	minimum, maximum := c.ReconnectMin, c.ReconnectMax
	if minimum <= 0 {
		minimum = 100 * time.Millisecond
	}
	if maximum <= 0 {
		maximum = 5 * time.Second
	}
	if maximum < minimum {
		maximum = minimum
	}
	backoff := minimum
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		delivered := false
		err := c.watchConnected(ctx, func(snapshot Snapshot) error {
			delivered = true
			return consume(snapshot)
		})
		var consumer *consumerError
		if errors.As(err, &consumer) {
			return consumer.err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if delivered {
			backoff = minimum
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if backoff > maximum/2 {
			backoff = maximum
		} else {
			backoff *= 2
		}
	}
}
