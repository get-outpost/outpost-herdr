# Outpost Herdr

Go adapter connecting Herdr's agent lifecycle and terminal controls to Outpost's
event vocabulary. Phase 1 implements the **Herdr side only**, without importing
Outpost Server internals or depending on the client SDK being extracted there.

## What works

- Bounded Unix-socket requests and typed agent/pane/native-session models.
- Event subscription, reconnect, topology-aware status coverage, and authoritative
  re-sync after subscription. Transport failure never means every session ended.
- `session.started`, `session.updated`, and `session.ended`; `done` maps to `idle`.
- Native session IDs passed through unchanged, allowing Claude's hook and this
  adapter to publish to one Outpost session. Stable namespaced hashed fallbacks
  for session paths and terminal/pane identities.
- `command.message` through `agent.prompt`; `command.interrupt` through one Escape.
  Commands target only currently mapped local sessions and re-check live identity
  before input. No command retries after ambiguous transport failure.
- A context-aware `Sink` boundary, an in-memory fake, and a runnable JSON-lines
  bridge. No real Herdr instance is needed for the tests.

`command.approve` stays with the Claude hook. No terminal transcript streaming,
structured tool events, HTTP/WebSocket relay connection, durable command cursor,
service installer, or Windows named-pipe transport is provided in this phase.
The JSON-lines command input is an explicit local integration boundary, not a
claim that the phone already connects through this executable.

## Run

Requires Go 1.27 and a running Herdr session. Protocol discovery was performed on
Herdr 0.9.5 (private protocol 22). No third-party Go dependencies.

```sh
go build -o /tmp/outpost-herdr ./cmd/outpost-herdr

# Read-only: one typed Herdr snapshot (agents and panes), then exit.
/tmp/outpost-herdr --snapshot

# Read-only: watch and print Outpost events, one JSON object per line.
/tmp/outpost-herdr --namespace my-mac:default
```

The default socket follows `HERDR_SOCKET_PATH`, then `HERDR_SESSION` under
`$XDG_CONFIG_HOME/herdr` (falling back to `~/.config/herdr`).
`--socket /path/to/herdr.sock` overrides discovery. Select named sessions with
`HERDR_SESSION=mobile` after unsetting any inherited `HERDR_SOCKET_PATH`.
Keep `--namespace` stable and unique for
each machine/Herdr session; the default is `hostname:socket-path`. It affects
fallback IDs only, never native IDs. Stop with SIGINT or SIGTERM.

### Opt-in commands

`--commands` enables command JSON lines on stdin while event JSON lines continue
on stdout. Without that flag stdin is ignored and no input is sent to agents.
Only use session IDs from the bridge's currently emitted events; avoid replaying
old command input. For intentional control of an agent you own:

```sh
/tmp/outpost-herdr --namespace my-mac:default --commands
```

Then write a command on stdin, substituting that agent's actual session ID:

```json
{"id":"local-1","type":"command.message","session_id":"<actual-session-id>","payload":{"text":"Please run the tests"}}
{"id":"local-2","type":"command.interrupt","session_id":"<actual-session-id>","payload":{}}
```

Do not paste example input targeting someone else's agent. Escape interrupts
Claude, Codex, Pi and OMP under their default keymaps; other agent kinds use the
same best-effort baseline. It may dismiss or decline a dialog. User-remapped
bindings are not detected. A successful response is input queue admission, not
proof the agent has completed or stopped. State updates come from observation.

Bad command JSON, actionable invalid commands, execution failures and publish
failures stop the bridge with a stderr diagnostic/nonzero exit. Unsupported,
encrypted and foreign-session commands are ignored. EOF on stdin leaves
monitoring running. No automatic mutation retry or durable deduplication is
claimed. External integration must supply a non-replaying command stream.

## Packages

| Package | Responsibility |
| --- | --- |
| `internal/herdr` | `Client`, socket discovery, typed models, request bounds, `Source.Watch` and authoritative snapshots |
| `internal/adapter` | Snapshot reconciliation, `SessionID`, `Adapter.Run`, `Sink`, `Event`, `Command`, `MemorySink` |
| `cmd/outpost-herdr` | Read-only snapshot/watch and opt-in stdin/stdout bridge |

`Sink.Publish(context.Context, Event) error` publishes events.
`Sink.Commands() <-chan Command` supplies commands (nil means read-only).
`adapter.New(source, sink, namespace).Run(ctx)` owns reconciliation and command
routing. `herdr.Source` permits fake snapshots and controls in tests.
`MemorySink.Send(ctx, command)` queues a command and `Events()` returns a snapshot
of emitted events. A future SDK-backed Sink can implement these contracts
without depending on the server's internal packages.

## Verification

```sh
go test -race ./...
go vet ./...
```

Fake Unix servers test framing, deadlines, cancellation, subscription races,
reconnect and re-sync. Adapter fakes cover identity, lifecycle and stale-target
command safety. Live input testing must create its **own scratch pane and
agent**; never send test input to existing panes.

Phase 1 verification passed `go test -race ./...`, `go vet ./...`, and a CLI build.
The built bridge was exercised against an isolated fake Unix server: native-ID
start, idle → working → idle, prompt submission, one `esc`, session end and clean
SIGTERM exit. Separate CLI smoke checks confirmed stdin cannot control agents
without `--commands` and malformed command JSON exits nonzero. Live
`--snapshot` succeeded against Herdr 0.9.5/protocol 22; no live agent input was
used for verification.

## Deployment decision and open questions

Use an **external daemon per Herdr session**, not an event hook pretending to be
a daemon. Herdr startup hooks are one-shot, not supervised services; plugin panes
can host long-lived programs but consume a terminal surface. A hybrid plugin
installer/action is possible once its supervisor lifecycle is specified. See
[the protocol investigation](docs/herdr-protocol.md) for framing, exact fallback
hash inputs, observed schema differences, keybindings and primary sources.

Remaining integration decisions:

1. **SDK command routing and ownership:** the server's `owner_device_id` is the
   first publisher, and a Claude hook may own the shared native session before
   this adapter. An SDK Sink must deliver message/interrupt commands for locally
   managed sessions even when the hook is the first publisher; approval stays
   with the hook. Server device ownership is not local execution capability.
2. **Durability:** snapshots restore current state, not missed history. Adapter
   state and the memory fake are process-local; a daemon restart republishes
   active starts, and cannot end sessions that vanished while it was stopped.
   SDK reconnect/backfill must not replay already executed prompts.
3. **Fallback promotion:** a late native session reference ends the earlier
   fallback session and starts the native ID. No server alias/merge API exists.
   Install native Herdr integrations before starting agents to reduce this gap.
4. **Input identity race:** revalidation reduces pane-reuse risk but the public
   Herdr API cannot atomically compare an expected native session when admitting
   input. Queue admission also cannot prove agent execution.
5. **Dual publisher fields:** Herdr calls Claude's provider `claude`; the existing
   hook uses `claude-code`. Same session ID avoids duplicate sessions, but the
   latest start can change the provider label. Device routing, shared lifecycle
   ownership and canonical provider naming need agreement with the SDK/server.
