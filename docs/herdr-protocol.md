# Herdr protocol and deployment

Investigated against installed **Herdr 0.9.5, private protocol 22**, using
`herdr --version`, `herdr status server`, `herdr api schema --json`,
`herdr agent list`, and `herdr session list --json`. These were read-only calls;
no existing agent received test input. The CLI's bundled schema is authoritative
for this build. The JSON API is not the private binary terminal-attach protocol;
clients should ignore unknown JSON fields rather than require binary version equality.

## Endpoint and framing

On this installation the default Unix socket is
`~/.config/herdr/herdr.sock`; named sessions use
`~/.config/herdr/sessions/<name>/herdr.sock`. `HERDR_SOCKET_PATH` is the native
explicit endpoint override and is injected into managed panes and plugin commands.
`HERDR_SESSION` selects a named session when no explicit socket is supplied.
With `XDG_CONFIG_HOME` set, the root is `$XDG_CONFIG_HOME/herdr` instead.
`HERDR_CONFIG_PATH` changes the configuration file, not the socket root.
The Go bridge also accepts `--socket`. macOS and Linux Unix sockets are in scope;
Windows named pipes and remote-machine multiplexing are not.

Each request uses a fresh Unix stream connection and UTF-8 newline-delimited JSON:

```json
{"id":"request-1","method":"agent.list","params":{}}
{"id":"request-1","result":{"type":"agent_list","agents":[]}}
```

The newline terminates a frame; EOF in the middle of JSON is not a complete frame.
There is no `jsonrpc` member or length prefix. IDs are strings and must match.
Errors are correlated envelopes, not successful empty results:

```json
{"id":"request-1","error":{"code":"not_found","message":"agent not found"}}
```

ccgram's `herdr_socket.py` uses a 16 MiB frame cap and bounded one-shot calls.
This client likewise bounds request, response and subscription frames and the
request/subscribe handshake duration. Quiet live subscriptions do not have a
request deadline; cancellation closes their connection. A timed-out mutation is
ambiguous: admission may have occurred. Never automatically retry a prompt or key.

## Calls used by the bridge

| Method | Parameters | Relevant result |
| --- | --- | --- |
| `session.snapshot` | `{}` | `{type, snapshot:{version, protocol, agents, panes, ...}}` |
| `agent.list` | `{}` | `{type, agents:[AgentInfo]}` |
| `pane.list` | `{}` or `{workspace_id}` | `{type, panes:[PaneInfo]}` |
| `agent.get` | `{target:"w1:p1"}` | `{type, agent:AgentInfo}` |
| `events.subscribe` | `{subscriptions:[...]}` | `{type:"subscription_started"}`, then pushed frames |
| `agent.prompt` | `{target:"w1:p1", text:"..."}` | Queue-admission acknowledgment, not completion |
| `agent.send_keys` | `{target:"w1:p1", keys:["esc"]}` | Queue-admission acknowledgment |
| `pane.send_text` | `{pane_id:"w1:p1", text:"..."}` | Raw text, no implicit submit |
| `pane.send_keys` | `{pane_id:"w1:p1", keys:["enter"]}` | Keys using Herdr names |

Agent targets accept unique agent names or pane IDs, **not** terminal IDs or bare
agent kinds. The adapter uses explicit pane IDs. `agent.prompt` performs Herdr's
native submit handling; raw text plus Enter is not substituted. Herdr refuses
`agent.prompt` for a currently blocked agent with `agent_blocked` without input.
The optional prompt `wait` object is not used: Outpost commands are admission,
not waiting for an agent to finish.

Agent records expose `agent`, `agent_status`, `pane_id`, `terminal_id`,
`workspace_id`, `tab_id`, `cwd`, `foreground_cwd`, optional `name`, `title`,
`display_agent`, terminal titles, and an optional native session reference:

```json
{"source":"herdr:claude","agent":"claude","kind":"id","value":"native-session-uuid"}
```

An OMP record was observed with `kind:"path"` pointing to a JSONL session file.
Missing/null optional strings are ordinary absent data. Metadata display labels
and `state_labels` are not lifecycle state.

## Subscriptions, races and reconnect

```json
{"id":"watch-1","method":"events.subscribe","params":{"subscriptions":[{"type":"pane.created"},{"type":"pane.exited"},{"type":"pane.agent_detected"},{"type":"pane.agent_status_changed","pane_id":"w1:p1"}]}}
{"id":"watch-1","result":{"type":"subscription_started"}}
{"event":"pane.agent_status_changed","data":{"pane_id":"w1:p1","workspace_id":"w1","agent":"claude","agent_status":"working"}}
{"event":"pane_exited","data":{"type":"pane_exited","pane_id":"w1:p1","workspace_id":"w1"}}
```

Herdr exposes two related event envelope shapes. The general schema uses
underscore event names and a tagged `data.type`; subscription-specific status
frames use dotted names and untagged data. Do not depend on one spelling.
Events invalidate cached state; they are not a durable ordered log.

**Difference from ccgram's protocol 20/22 notes:** its `herdr_events.py` says pane
status and exit subscriptions need `pane_id`. The installed protocol-22 schema
requires a pane ID for `pane.agent_status_changed`, but `pane.exited` is global,
as are `pane.created`, `pane.closed`, `pane.updated`, `pane.moved`, and
`pane.agent_detected`. `tab.closed` is also global. The bridge uses global
lifecycle events and per-pane status subscriptions.

The correct recovery sequence is:

1. Establish global lifecycle coverage, receive and validate the subscribe ACK.
2. Start/keep reading events while reading an authoritative `session.snapshot`
   over a separate connection.
3. Establish status coverage for discovered pane IDs; after every replacement
   subscription ACK, take another snapshot. Subscribe-before-snapshot closes
   the discovery race. Pane moves can change public pane IDs without killing
   the terminal, so rebuild coverage when topology changes.
4. Treat buffered events as invalidations, not deltas to blindly replay over
   the snapshot: there is no common snapshot/event sequence boundary. If an
   event arrives during a read, refresh again.
5. On EOF, malformed frames, transport failure or `events_lost`, reconnect with
   bounded exponential backoff, resubscribe, then re-sync. A failed read never
   means all agents have exited.

A subscription referencing even one nonexistent pane is rejected as a whole;
refresh the pane set and retry. Herdr can send a correlated
`error.code:"events_lost"` after the initial ACK and close the stream when shared
non-durable history is overrun. Recovery reconstructs current state, not every
missed transition. A session that both starts and ends while disconnected may
never be observed. Periodic reconciliation also catches missed invalidations.

## Session and command mapping

Native session IDs (`agent_session.kind == "id"`, nonempty value) are passed
through unchanged. Claude therefore uses the **same** `session_id` as its hook,
not an adapter-prefixed ID. Providers remain the Herdr kind (for example
`claude`); the existing Claude hook calls its provider `claude-code`, so mixed
publishers can overwrite the provider label without creating a second session.

Fallback identity is `herdr:` followed by lowercase hex SHA-256. Each input is a
UTF-8 string prefixed by its uint64 big-endian byte length; hash these fields:

- Path: `[namespace, "path", ref.source, ref.agent, ref.value]`.
- Terminal: `[namespace, "terminal", agent.agent, agent.terminal_id]`.
- Pane (only without terminal ID): `[namespace, "pane", agent.agent, agent.pane_id]`.

Paths are not normalized. Revisions and display names are not identity. Keep
`--namespace` stable and unique per machine/Herdr session. Native ID appearance
later is an identity change: the fallback session ends and the native session
starts. There is no server alias/merge API to hide that earlier fallback row.
Install `herdr integration install <agent>` **before starting the agent** to
improve native reference availability; this bridge does not alter integrations.

The start event chooses cwd from `foreground_cwd`, then `cwd`; title from
`name`, `title`, `terminal_title_stripped`, `terminal_title`, `display_agent`,
then agent kind, taking the first nonblank value.

The adapter publishes `session.started {provider,cwd,title}`, then known semantic
status through `session.updated {status}`. `done` means unseen idle completion in
Herdr and maps to `idle`, not session end. `unknown` does not fabricate idle.
A disappeared/replaced agent causes `session.ended`; duplicate native IDs are
one session and do not end until the last matching local agent disappears.

`command.message {text}` submits through `agent.prompt`. Whitespace-only text is
invalid. `command.interrupt` sends exactly one `esc`, not Ctrl+C, double Escape,
or a process signal. `command.approve` is ignored; Claude approvals stay with
its native hook. Encrypted commands cannot be interpreted and are ignored.
Only currently mapped local session IDs are actionable. Before sending, read
`agent.get` again and compare identity to reject pane reuse. This reduces but
cannot atomically eliminate the race between that read and Herdr's input queue:
the public input API has no expected-session guard.

| Herdr agent kind | Escape evidence and limits |
| --- | --- |
| `claude` | Claude's interactive-mode docs: Esc interrupts a response/tool call, closes dialogs, and declines a permission prompt. Double Esc is different. |
| `codex` | Codex's `tui/src/keymap.rs`: default `chat.interrupt_turn` is `KeyCode::Esc`; configurable by the user. |
| `pi` | Pi's terminal usage docs: Escape stops the current task; queued messages return to the editor. |
| `omp` | Installed `@oh-my-pi/pi-coding-agent/src/config/keybindings.ts`: `app.interrupt` defaults to `escape`; configurable. |
| Other recognized kinds | Single Escape is the requested baseline, not a verified universal cancellation contract. It may dismiss a dialog or do nothing. No guessed Ctrl+C fallback. |

Input queue admission does not prove cancellation or prompt execution. The
bridge waits for observed Herdr state to publish status; it never synthesizes
`idle` merely because an interrupt was accepted.

## Daemon versus plugin

**Recommendation: an external long-running daemon, one per Herdr session.**
It can retain subscriptions, reconnect, reconcile, and later hold the Outpost
SDK command stream without a terminal UI. Phase 1 supplies the runnable
foreground JSON-lines bridge; service installation and SDK transport are not
part of this phase.

Herdr 0.9.5 plugins are not limited to event hooks: a manifest supports
`[[startup]]`, `[[actions]]`, `[[events]]`, `[[panes]]`, builds and link handlers.
However, official documentation explicitly calls startup hooks **one-shot
initialization commands rather than supervised daemons**. They run after socket
readiness and again on live handoff, not on link/enable/config reload. Event
commands run per event; the renamer example detaches its cold phase with
`setsid`, which is not daemon supervision. Managed installation cleanup also
requires detached work to stop before its parent command/pane exits.

A plugin **pane** can run a long-lived process, but spends a terminal surface and
couples lifetime to that pane. Pure event hooks cannot service phone commands
while no Herdr event is occurring. A hybrid installer/action could control an
external supervisor later, but it needs singleton, handoff, shutdown, credential
and upgrade ownership rules; per-event detached spawning is not that design.
Do not ship a misleading `[[events]]` daemon launcher.

## Sources

- Installed `herdr api schema --json`, `herdr --help`, `herdr agent`, `herdr pane`,
  `herdr status server`, `herdr session list --json` (0.9.5).
- [Herdr 0.9.5 socket API](https://raw.githubusercontent.com/herdrdev/herdr/v0.9.5/docs/next/website/src/content/docs/socket-api.mdx).
- [Herdr 0.9.5 config root](https://github.com/herdrdev/herdr/blob/v0.9.5/src/config/io.rs)
  and [session endpoint selection](https://github.com/herdrdev/herdr/blob/v0.9.5/src/session.rs).
- [Herdr 0.9.5 plugins and lifecycle](https://raw.githubusercontent.com/herdrdev/herdr/v0.9.5/docs/next/website/src/content/docs/plugins.mdx).
- [Herdr 0.9.5 agent status semantics](https://raw.githubusercontent.com/herdrdev/herdr/v0.9.5/docs/next/website/src/content/docs/agents.mdx).
- Local reference: `ccgram/src/ccgram/multiplexer/herdr_socket.py`,
  `herdr_events.py`, `herdr_commands.py`; `herdr-plugin-renamer/herdr-plugin.toml`
  and `src/main.rs` (`spawn_cold_phase`).
- [Claude interactive controls](https://code.claude.com/docs/en/interactive-mode).
- [Codex interrupt keymap](https://github.com/openai/codex/blob/main/codex-rs/tui/src/keymap.rs).
- [Pi terminal usage](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/usage.md).
