// outpost-herdr bridges a local Herdr session to a JSON-lines Sink.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/get-outpost/outpost-herdr/internal/adapter"
	"github.com/get-outpost/outpost-herdr/internal/herdr"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "outpost-herdr:", err)
		os.Exit(1)
	}
}

func run() error {
	socket := flag.String("socket", "", "Herdr Unix socket (default: native Herdr environment/config)")
	namespace := flag.String("namespace", "", "stable host/session identity for fallback session IDs (default: hostname:socket)")
	commands := flag.Bool("commands", false, "execute command JSON lines from stdin; disabled by default")
	snapshot := flag.Bool("snapshot", false, "print one read-only Herdr snapshot and exit")
	sessionGrace := flag.Duration("session-grace", adapter.DefaultSessionGracePeriod, "wait for an agent session reference or known status before starting a fallback session (0 disables)")
	flag.Parse()
	if flag.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *snapshot && *commands {
		return errors.New("--snapshot cannot be combined with --commands")
	}
	if *sessionGrace < 0 {
		return errors.New("--session-grace must not be negative")
	}
	if *socket == "" {
		var err error
		*socket, err = herdr.SocketPath()
		if err != nil {
			return err
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := herdr.NewClient(*socket)
	if *snapshot {
		state, err := client.Snapshot(ctx)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(state)
	}
	if *namespace == "" {
		host, err := os.Hostname()
		if err != nil {
			return err
		}
		*namespace = host + ":" + *socket
	}
	sink := &lineSink{encoder: json.NewEncoder(os.Stdout)}
	inputErrors := make(chan error, 1)
	if *commands {
		sink.commands = make(chan adapter.Command, 64)
		go readCommands(ctx, os.Stdin, sink.commands, inputErrors)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	finished := make(chan error, 1)
	bridge := adapter.New(client, sink, *namespace)
	bridge.SessionGracePeriod = *sessionGrace
	go func() { finished <- bridge.Run(ctx) }()
	select {
	case err := <-finished:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case err := <-inputErrors:
		cancel()
		<-finished
		return err
	}
}

type lineSink struct {
	encoder  *json.Encoder
	commands chan adapter.Command
}

func (s *lineSink) Publish(ctx context.Context, event adapter.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.encoder.Encode(event)
}

func (s *lineSink) Commands() <-chan adapter.Command { return s.commands }

func readCommands(ctx context.Context, input io.Reader, commands chan<- adapter.Command, failures chan<- error) {
	defer close(commands)
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		var command adapter.Command
		if err := json.Unmarshal(scanner.Bytes(), &command); err != nil {
			failures <- fmt.Errorf("command line %d: %w", line, err)
			return
		}
		select {
		case commands <- command:
		case <-ctx.Done():
			return
		}
	}
	if err := scanner.Err(); err != nil {
		failures <- fmt.Errorf("read commands: %w", err)
	}
}
