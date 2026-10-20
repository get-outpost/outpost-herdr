# Outpost Herdr

A Go daemon translating local Herdr agents into Outpost sessions and routing
phone messages and interrupts back to their terminal harnesses. Uses the public
[`outpost-go v0.1.0`](https://github.com/get-outpost/outpost-go) SDK; no server
internal imports or local module replacements.

## What works

- Bounded Herdr Unix-socket requests, topology-aware event subscriptions,
  reconnect and authoritative post-subscription re-sync.
- Native session IDs shared with harness hooks, stable namespaced fallback IDs,
  startup grace, and fallback tombstones on late native-ID promotion.
- `session.started`, semantic status updates, and `session.ended` on exit.
  Herdr `done` maps to `idle`; unknown state never fabricates idle.
- SDK HTTP publication through a bounded FIFO queue, transient-failure retries,
  a resumable command stream, and deadline-bounded shutdown flushing.
- Durable command cursor and published-live session set, including exits missed
  while the daemon was stopped.
- `command.message` through Herdr's native prompt handling;
  `command.interrupt` through one Escape. Each command revalidates live identity.
- Shared `outpost login` credentials and a separate JSON-lines dry-run mode.

Approval commands remain with the native Claude hook. No terminal transcript
streaming, structured tool telemetry, Windows named pipes, or service
installation is included.

## Run the daemon

Requires Go 1.27, a running Herdr session, an Outpost server, and a **plugin**
device login. Herdr protocol discovery was performed on 0.9.5/private protocol 22.

```sh
go build -o /tmp/outpost-herdr ./cmd/outpost-herdr

# Uses the same saved outpost login as the machine's Claude hook.
/tmp/outpost-herdr run --namespace my-machine:default
```

The SDK URL and token resolve independently: nonempty `OUTPOST_URL` and
`OUTPOST_TOKEN` override the corresponding saved login field. The login file is
`$XDG_CONFIG_HOME/outpost/config.json`, falling back to
`~/.config/outpost/config.json`, with JSON keys `url` and `token`. If both values
are supplied through the environment, the login file is not accessed. The daemon
never writes that file or logs the token. Use HTTPS outside localhost.

Recommended deployment: **one plugin device per machine**, shared by the Claude
hook and Herdr daemon through the same login config. Do not create separate
per-adapter logins. Never commit tokens or put them in service manifests.

The Herdr socket follows `HERDR_SOCKET_PATH`, then `HERDR_SESSION` under
`$XDG_CONFIG_HOME/herdr` (fallback `~/.config/herdr`). An explicit `--socket`
overrides discovery. Unset an inherited `HERDR_SOCKET_PATH` before selecting a
different named session with `HERDR_SESSION`.

Keep `--namespace` stable and unique per machine, Herdr session and observation
scope. It defaults to `hostname:socket-path`. Run **only one daemon per
namespace**. Use a new namespace when changing accounts or relay deployments;
a cursor is meaningful only within its original account log.

| Flag | Default | Behavior |
| --- | --- | --- |
| `--socket` | Native Herdr discovery | Explicit Unix socket |
| `--namespace` | Hostname and socket | Fallback identity and durable state scope |
| `--pane` | All agents | Observe only this pane; use a distinct namespace |
| `--session-grace` | `10s` | Delay unpublished agents without a native session reference; `0` disables |
| `--queue-size` | `256` | Maximum queued publications; overflow stops the daemon rather than blocking the watcher |
| `--command-max-age` | `2m` | Drop older message/interrupt commands based on envelope `ts` |
| `--flush-timeout` | `5s` | Maximum publication drain on SIGINT/SIGTERM |

Queue size, maximum command age and flush timeout must be positive; grace cannot
be negative. SDK requests have their own bounded request budget. SIGINT/SIGTERM
stop observation and command intake, then drain queued publications under a fresh
flush deadline. A deadline/fatal error is reported as a nonzero exit.

## Publication and command safety

`Sink.Publish` admits an immutable event to a bounded queue without waiting for
the relay. One worker publishes in order through SDK `PostEvent`, retrying
network/server failures with bounded exponential backoff. HTTP 4xx responses are
logged and dropped, except **401**, which stops the daemon with an authentication
error. Queue overflow and durable-state write failures also stop it. There is no
unbounded memory queue or promise of delivery after a crash.

The command stream starts at `ListSessions().as_of_seq` on the first run, so
historical commands are never executed. The head is persisted before connecting.
Restarts resume from the durable cursor. Initial stream connection failures are
retried with backoff; established-stream reconnect/deduplication comes from the
SDK. HTTP 401/403 on stream handshakes are terminal.

Only event-frame sequences advance the durable cursor. `ready.head_seq` and ack
sequences do not. Unsupported, encrypted, foreign-session and over-age commands
are ignored; stale-command drops are logged without their payloads.

**Command delivery is at-most-once across daemon restarts, not exactly-once.**
The cursor is durably saved **before** a command is handed to the adapter. A crash
between those steps can lose that command, but will not replay a potentially
already executed prompt or interrupt. There are no automatic command retries.
Input queue admission does not prove execution or cancellation.

Owned means **currently mapped to a live local agent**, not matching server
`owner_device_id`. A hook-first Claude session must still accept the daemon's
message/interrupt commands. Before input the adapter performs another
`agent.get` identity check. The public Herdr API has no atomic expected-session
guard, so a small read/input-queue race remains.

Escape interrupts Claude, Codex, Pi and OMP under their default bindings. Other
kinds use the same best-effort baseline; Escape may dismiss/decline a dialog or
do nothing. User-remapped bindings are not detected. Native approval commands
are deliberately not interpreted here.

## Durable lifecycle recovery

State lives at `$XDG_STATE_HOME/outpost-herdr/<sha256(namespace)>.json`, falling
back to `~/.local/state/outpost-herdr/`. The private directory is mode `0700`,
files `0600`. Atomic replacement stores the cursor and successfully published
live-session IDs together. Corrupt state is fatal, not silently reset to zero.
State contains session IDs/cursors in plaintext, but no token or event payloads.

Starts and updates accepted by the server add a live ID; accepted ends and
tombstones remove it. After the first authoritative Herdr snapshot on startup,
previously live IDs absent locally receive `session.ended`. Current agents get
`session.started` again. Previously published fallbacks are re-announced without
another grace period, preserving subsequent promotion/exit tracking.

Agents without a nonempty native `id` or `path` reference wait until grace
expires or their first known status (`working`, `idle`, `blocked`, `done`),
whichever comes first. A known initial status releases immediately. A timer
expires grace even without another event; refreshes and pane moves retain the
deadline. Agents disappearing before publication emit nothing.

A late native ID for the same local agent causes `session.deleted` for the
published fallback **before** the native start, hiding the duplicate in default
client listings. Ordinary exits and unrelated replacements still end normally.
Native IDs pass through unchanged, so Claude's hook and daemon share one session.
Fallback hashing and promotion matching are specified in
[the protocol investigation](docs/herdr-protocol.md).

Canonical provider names are Herdr kinds (`claude`, `codex`, `pi`, `omp`,
`opencode`); unknown kinds pass through unchanged. Protocol vocabulary/status
constants come from the SDK. There is no server alias/merge API.

## JSON-lines dry run

```sh
# Read-only typed Herdr snapshot, then exit.
/tmp/outpost-herdr dry-run --snapshot

# Read-only Outpost event JSON lines; no server, credentials or state file.
/tmp/outpost-herdr dry-run --namespace my-machine:preview
```

Bare legacy invocations without a subcommand retain this dry-run behavior.
Stdin is ignored unless `--commands` is explicitly passed. With that opt-in,
stdin command JSON lines can intentionally control a locally mapped session:

```json
{"id":"local-1","type":"command.message","session_id":"<actual-session-id>","payload":{"text":"Please run the tests"}}
{"id":"local-2","type":"command.interrupt","session_id":"<actual-session-id>","payload":{}}
```

Use IDs emitted by the current bridge, not an old command log. EOF leaves
monitoring running. Bad command JSON, invalid actionable commands, execution and
output failures stop it. Approval, encrypted and foreign commands are ignored.
No stdin command input is used by `run`; it consumes the authenticated SDK
stream instead.

## Service examples

Use an external supervised daemon per Herdr session. Herdr startup hooks are
one-shot initialization, not daemon supervision; plugin panes consume a terminal.

- [launchd LaunchAgent](docs/services/org.get-outpost.herdr.plist): edit the
  absolute executable/log paths and unique namespace for the intended machine.
  Create a user-writable log directory first; `/usr/local` paths are examples,
  not a requirement. The example allows ten seconds for the default five-second
  flush and throttles failure restarts.
- [systemd user unit](docs/services/outpost-herdr.service): expects the binary in
  `~/.local/bin`, uses `%H:default` as namespace, and sends logs to the journal.

Both examples read the shared login file; neither embeds credentials. Validate
and adapt them before registering them with your user service manager. **No
service is installed by this repository or by running the daemon.** If increasing
`--flush-timeout`, also increase the service manager's stop timeout.

## Packages and verification

| Package | Responsibility |
| --- | --- |
| `internal/herdr` | Socket transport, typed models, authoritative subscription recovery |
| `internal/adapter` | Identity, grace/promotion, lifecycle reconciliation, live command revalidation, unchanged `Sink` and `MemorySink` |
| `internal/relay` | SDK publication queue, command stream, durable dispatch safety, shutdown |
| `internal/state` | Atomic namespace-scoped cursor and live-session storage |
| `internal/config` | Environment-first shared login loading |
| `cmd/outpost-herdr` | Daemon, scoped observation and JSON-lines dry run |

```sh
go test -race ./...
go vet ./...
plutil -lint docs/services/org.get-outpost.herdr.plist
```

Phase-two unit verification passed the race suite, vet, a CLI build and plist
validation. An isolated executable smoke run verified HTTP 503 retry, first-run
head initialization, fresh command execution, stale-command logging/drop,
event-only cursor persistence, clean signal flushing, restart without command
replay, and ended reconciliation for an agent that exited while offline.

Tests use fake Herdr sockets, in-memory agent sources and HTTP/WebSocket relay
servers. Timer tests use `testing/synctest`. Live E2E must use its **own scratch
pane/agent** and an isolated namespace/state directory; never send test input to
existing panes. Live test credentials and personal paths must stay outside Git.
