// outpost-herdr relays local Herdr agents to Outpost or a JSON-lines dry run.
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
	"time"

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
	args := os.Args[1:]
	daemon := false
	if len(args) > 0 {
		switch args[0] {
		case "run":
			daemon, args = true, args[1:]
		case "dry-run":
			args = args[1:]
		}
	}
	flags := flag.NewFlagSet("outpost-herdr", flag.ContinueOnError)
	var options daemonOptions
	flags.StringVar(&options.socket, "socket", "", "Herdr Unix socket (default: native Herdr environment/config)")
	flags.StringVar(&options.namespace, "namespace", "", "stable machine/session identity (default: hostname:socket)")
	flags.StringVar(&options.pane, "pane", "", "observe only this pane; use a distinct namespace for each scope")
	flags.DurationVar(&options.sessionGrace, "session-grace", adapter.DefaultSessionGracePeriod, "wait for a session reference or known status before fallback publication (0 disables)")
	commands := flags.Bool("commands", false, "dry-run only: execute command JSON lines from stdin")
	snapshot := flags.Bool("snapshot", false, "dry-run only: print one Herdr snapshot and exit")
	if daemon {
		flags.IntVar(&options.queueSize, "queue-size", 256, "maximum queued publications; overflow stops the daemon")
		flags.DurationVar(&options.commandMaxAge, "command-max-age", 2*time.Minute, "drop older commands on resume")
		flags.DurationVar(&options.flushTimeout, "flush-timeout", 5*time.Second, "maximum shutdown publication drain")
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("expected run, dry-run, or flags")
	}
	if *snapshot && *commands {
		return errors.New("--snapshot cannot be combined with --commands")
	}
	if daemon && (*snapshot || *commands) {
		return errors.New("run consumes SDK commands; --snapshot and --commands are dry-run only")
	}
	if options.sessionGrace < 0 {
		return errors.New("--session-grace must not be negative")
	}
	if daemon && (options.queueSize <= 0 || options.commandMaxAge <= 0 || options.flushTimeout <= 0) {
		return errors.New("--queue-size, --command-max-age and --flush-timeout must be positive")
	}
	if options.socket == "" {
		var err error
		options.socket, err = herdr.SocketPath()
		if err != nil {
			return err
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := herdr.NewClient(options.socket)
	if *snapshot {
		state, err := client.Snapshot(ctx)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(scopeSnapshot(state, options.pane))
	}
	if options.namespace == "" {
		host, err := os.Hostname()
		if err != nil {
			return err
		}
		options.namespace = host + ":" + options.socket
	}
	if daemon {
		return runDaemon(ctx, client, options)
	}
	source := scopedSource{Source: client, namespace: options.namespace, pane: options.pane}
	return runDryRun(ctx, source, options, *commands)
}

func runDryRun(ctx context.Context, source herdr.Source, options daemonOptions, commands bool) error {
	sink := &lineSink{encoder: json.NewEncoder(os.Stdout)}
	inputErrors := make(chan error, 1)
	if commands {
		sink.commands = make(chan adapter.Command, 64)
		go readCommands(ctx, os.Stdin, sink.commands, inputErrors)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	finished := make(chan error, 1)
	bridge := adapter.New(source, sink, options.namespace)
	bridge.SessionGracePeriod = options.sessionGrace
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
