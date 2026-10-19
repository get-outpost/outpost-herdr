// Package adapter reconciles herdr agents with Outpost session events.
package adapter

import (
	"context"
	"sync"
)

// Event is an Outpost session event, independent of a server transport.
type Event struct {
	Type      string         `json:"type"`
	SessionID string         `json:"session_id"`
	Payload   map[string]any `json:"payload,omitempty"`
}

// Command is a command delivered by an Outpost transport. Encrypted commands
// cannot be executed by this adapter and are ignored.
type Command struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	SessionID string         `json:"session_id"`
	Payload   map[string]any `json:"payload,omitempty"`
	Enc       string         `json:"enc,omitempty"`
}

// Sink publishes events and delivers commands. Publish must respect ctx, and
// Commands must return a stable channel. Closing that channel stops command
// consumption without stopping session reconciliation.
type Sink interface {
	Publish(context.Context, Event) error
	Commands() <-chan Command
}

// MemorySink is a concurrency-safe, in-memory Sink for local integrations and
// tests. Events and commands are copied at its boundaries. Payloads should use
// JSON-compatible values; no events or commands are persisted.
type MemorySink struct {
	mu       sync.RWMutex
	events   []Event
	commands chan Command
}

// NewMemorySink creates a sink with the requested command-channel capacity.
func NewMemorySink(buffer int) *MemorySink {
	return &MemorySink{commands: make(chan Command, buffer)}
}

// Publish records an event unless ctx has already been canceled.
func (s *MemorySink) Publish(ctx context.Context, event Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	event.Payload = copyPayload(event.Payload)
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
	return nil
}

// Commands is the stream consumed by an Adapter.
func (s *MemorySink) Commands() <-chan Command {
	return s.commands
}

// Send queues a command, blocking until it is accepted or ctx is canceled.
func (s *MemorySink) Send(ctx context.Context, command Command) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	command.Payload = copyPayload(command.Payload)
	select {
	case s.commands <- command:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Events returns an independent snapshot in publication order.
func (s *MemorySink) Events() []Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	events := make([]Event, len(s.events))
	for i, event := range s.events {
		event.Payload = copyPayload(event.Payload)
		events[i] = event
	}
	return events
}

func copyPayload(payload map[string]any) map[string]any {
	if payload == nil {
		return nil
	}
	result := make(map[string]any, len(payload))
	for key, value := range payload {
		result[key] = copyValue(value)
	}
	return result
}

func copyValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		return copyPayload(value)
	case []any:
		result := make([]any, len(value))
		for i, item := range value {
			result[i] = copyValue(item)
		}
		return result
	case []string:
		return append([]string(nil), value...)
	case []byte:
		return append([]byte(nil), value...)
	default:
		return value
	}
}
