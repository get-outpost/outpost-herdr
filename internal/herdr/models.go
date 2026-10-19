package herdr

import "context"

// SessionRef is the agent-reported identity. Only kind "id" is a native ID;
// kind "path" identifies a transcript path rather than a globally unique ID.
type SessionRef struct {
	Source string `json:"source"`
	Agent  string `json:"agent"`
	Kind   string `json:"kind"`
	Value  string `json:"value"`
}

// Agent contains the public agent fields used by the bridge. Nullable wire
// strings decode to the zero value; an absent session remains nil.
type Agent struct {
	PaneID                string      `json:"pane_id"`
	WorkspaceID           string      `json:"workspace_id"`
	TabID                 string      `json:"tab_id"`
	TerminalID            string      `json:"terminal_id"`
	Agent                 string      `json:"agent"`
	AgentStatus           string      `json:"agent_status"`
	Cwd                   string      `json:"cwd"`
	ForegroundCwd         string      `json:"foreground_cwd"`
	Name                  string      `json:"name"`
	DisplayAgent          string      `json:"display_agent"`
	Title                 string      `json:"title"`
	TerminalTitle         string      `json:"terminal_title"`
	TerminalTitleStripped string      `json:"terminal_title_stripped"`
	AgentSession          *SessionRef `json:"agent_session"`
	Revision              uint64      `json:"revision"`
}

// Pane retains terminal identity even before an agent is detected.
type Pane struct {
	PaneID                string      `json:"pane_id"`
	WorkspaceID           string      `json:"workspace_id"`
	TabID                 string      `json:"tab_id"`
	TerminalID            string      `json:"terminal_id"`
	Agent                 string      `json:"agent"`
	AgentStatus           string      `json:"agent_status"`
	Cwd                   string      `json:"cwd"`
	ForegroundCwd         string      `json:"foreground_cwd"`
	Label                 string      `json:"label"`
	DisplayAgent          string      `json:"display_agent"`
	Title                 string      `json:"title"`
	TerminalTitle         string      `json:"terminal_title"`
	TerminalTitleStripped string      `json:"terminal_title_stripped"`
	AgentSession          *SessionRef `json:"agent_session"`
	Revision              uint64      `json:"revision"`
}

// Snapshot is authoritative current state, not an event-history boundary.
type Snapshot struct {
	Version  string  `json:"version"`
	Protocol int     `json:"protocol"`
	Agents   []Agent `json:"agents"`
	Panes    []Pane  `json:"panes"`
}

// Source separates the adapter from transport and permits fake-backed tests.
type Source interface {
	Watch(context.Context, func(Snapshot) error) error
	GetAgent(context.Context, string) (Agent, error)
	Prompt(context.Context, string, string) error
	SendKeys(context.Context, string, ...string) error
}
