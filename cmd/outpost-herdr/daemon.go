package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/get-outpost/outpost-go/client"
	"github.com/get-outpost/outpost-herdr/internal/adapter"
	"github.com/get-outpost/outpost-herdr/internal/config"
	"github.com/get-outpost/outpost-herdr/internal/herdr"
	"github.com/get-outpost/outpost-herdr/internal/relay"
	"github.com/get-outpost/outpost-herdr/internal/state"
)

type daemonOptions struct {
	socket        string
	namespace     string
	pane          string
	sessionGrace  time.Duration
	commandMaxAge time.Duration
	flushTimeout  time.Duration
	queueSize     int
}

func runDaemon(ctx context.Context, source herdr.Source, options daemonOptions) error {
	configuration, err := config.Load()
	if err != nil {
		return fmt.Errorf("load Outpost login: %w", err)
	}
	remote, err := client.New(configuration)
	if err != nil {
		return fmt.Errorf("configure Outpost client: %w", err)
	}
	store, err := state.Open(options.namespace)
	if err != nil {
		return fmt.Errorf("open daemon state: %w", err)
	}
	previouslyLive := store.LiveSessions()
	logger := slog.Default()
	sink, err := relay.New(remote, store, relay.Options{
		QueueSize: options.queueSize, CommandMaxAge: options.commandMaxAge, Logger: logger,
	})
	if err != nil {
		return err
	}
	if err := sink.Start(ctx); err != nil {
		flushCtx, cancel := context.WithTimeout(context.Background(), options.flushTimeout)
		defer cancel()
		shutdownErr := sink.Shutdown(flushCtx)
		if ctx.Err() != nil {
			return shutdownErr
		}
		return errors.Join(err, shutdownErr)
	}
	logger.Info("Outpost Herdr daemon started", "namespace", options.namespace)
	watched := scopedSource{Source: source, namespace: options.namespace, pane: options.pane, localSessions: sink.SetLocalSessions}
	bridge := adapter.New(watched, sink, options.namespace)
	bridge.SessionGracePeriod = options.sessionGrace
	bridge.PreviouslyLive = previouslyLive
	adapterCtx, cancelAdapter := context.WithCancel(ctx)
	defer cancelAdapter()
	finished := make(chan error, 1)
	go func() { finished <- bridge.Run(adapterCtx) }()
	var result error
	select {
	case result = <-finished:
		if errors.Is(result, context.Canceled) && ctx.Err() != nil {
			result = nil
		}
	case result = <-sink.Errors():
		cancelAdapter()
		<-finished
	}
	// A signal can race a fatal worker error. Do not turn a rejected credential
	// into a successful shutdown just because the adapter stopped first.
	if result == nil {
		select {
		case result = <-sink.Errors():
		default:
		}
	}
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), options.flushTimeout)
	defer cancelFlush()
	flushErr := sink.Shutdown(flushCtx)
	logger.Info("Outpost Herdr daemon stopped")
	return errors.Join(result, flushErr)
}

// scopedSource exposes only the selected local agents and updates the command
// ownership map before the adapter consumes each authoritative snapshot.
// Controls still delegate to the original Source and retain GetAgent revalidation.
type scopedSource struct {
	herdr.Source
	namespace     string
	pane          string
	localSessions func([]string)
}

func (s scopedSource) Watch(ctx context.Context, consume func(herdr.Snapshot) error) error {
	return s.Source.Watch(ctx, func(snapshot herdr.Snapshot) error {
		snapshot = scopeSnapshot(snapshot, s.pane)
		if s.localSessions != nil {
			ids := make([]string, 0, len(snapshot.Agents))
			for _, agent := range snapshot.Agents {
				if strings.TrimSpace(agent.Agent) == "" {
					continue
				}
				id, err := adapter.SessionID(s.namespace, agent)
				if err != nil {
					return err
				}
				ids = append(ids, id)
			}
			s.localSessions(ids)
		}
		return consume(snapshot)
	})
}

func scopeSnapshot(snapshot herdr.Snapshot, pane string) herdr.Snapshot {
	if pane == "" {
		return snapshot
	}
	agents := make([]herdr.Agent, 0, 1)
	for _, agent := range snapshot.Agents {
		if agent.PaneID == pane {
			agents = append(agents, agent)
		}
	}
	panes := make([]herdr.Pane, 0, 1)
	for _, candidate := range snapshot.Panes {
		if candidate.PaneID == pane {
			panes = append(panes, candidate)
		}
	}
	snapshot.Agents, snapshot.Panes = agents, panes
	return snapshot
}
