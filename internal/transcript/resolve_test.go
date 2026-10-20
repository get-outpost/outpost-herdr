package transcript

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/get-outpost/outpost-herdr/internal/herdr"
)

func resolverAgent(provider, kind, value string) herdr.Agent {
	return herdr.Agent{Agent: provider, AgentSession: &herdr.SessionRef{Agent: provider, Kind: kind, Value: value}}
}

func resolverWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func resolverWant(t *testing.T, agent herdr.Agent, want string) {
	t.Helper()
	got, err := Resolve(agent)
	if err != nil || got != want {
		t.Fatalf("Resolve = %q, %v; want %q, nil", got, err, want)
	}
}

func TestResolveExplicitPiPaths(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	path := filepath.Join(home, "native", "synthetic.jsonl")
	resolverWriteFile(t, path, "{}\n")
	for _, provider := range []string{"omp", "pi"} {
		resolverWant(t, resolverAgent(provider, "path", path), path)
		resolverWant(t, resolverAgent(provider, "path", "~/native/synthetic.jsonl"), path)
		resolverWant(t, resolverAgent(provider, "path", "native/synthetic.jsonl"), "")
		resolverWant(t, resolverAgent(provider, "id", path), "")
		resolverWant(t, resolverAgent(provider, "path", home), "")
		resolverWant(t, resolverAgent(provider, "path", filepath.Join(home, "missing.jsonl")), "")
		resolverWant(t, resolverAgent(provider, "path", "~someone/native.jsonl"), "")
		resolverWant(t, resolverAgent(provider, "path", "~/../escape.jsonl"), "")
		resolverWant(t, resolverAgent(provider, "path", path+"\x00"), "")
	}
	extensionless := filepath.Join(home, "native", "extensionless")
	resolverWriteFile(t, extensionless, "{}\n")
	resolverWant(t, resolverAgent("omp", "path", extensionless), extensionless)
	resolverWant(t, herdr.Agent{Agent: "omp"}, "")
	resolverWant(t, resolverAgent("other", "path", path), "")
	mismatch := resolverAgent("omp", "path", path)
	mismatch.AgentSession.Agent = "claude"
	resolverWant(t, mismatch, "")
	resolverWant(t, resolverAgent(" OMP ", "path", path), path)
}

func TestResolveClaudeExactIDAndHomeIsolation(t *testing.T) {
	home := t.TempDir()
	otherHome := t.TempDir()
	t.Setenv("HOME", home)
	id := "native-demo_123"
	path := filepath.Join(home, ".claude", "projects", "synthetic-project", id+".jsonl")
	resolverWriteFile(t, path, "{}\n")
	resolverWriteFile(t, filepath.Join(home, ".claude", "projects", "synthetic-project", id+"-suffix.jsonl"), "{}\n")
	resolverWriteFile(t, filepath.Join(otherHome, ".claude", "projects", "synthetic-project", "other-id.jsonl"), "{}\n")
	resolverWant(t, resolverAgent("claude", "id", id), path)
	resolverWant(t, resolverAgent("claude", "id", "native-demo"), "")
	resolverWant(t, resolverAgent("claude", "id", "other-id"), "")
	resolverWant(t, resolverAgent("claude", "path", path), "")
	for _, unsafe := range []string{"", "../native-demo_123", "../../outside", "/absolute", "*", "native?", "[abc]", "nested/id", `nested\id`, ".", "..", "id\x00", strings.Repeat("x", 201)} {
		resolverWant(t, resolverAgent("claude", "id", unsafe), "")
	}
	t.Setenv("HOME", otherHome)
	resolverWant(t, resolverAgent("claude", "id", id), "")
	t.Setenv("HOME", "")
	resolverWant(t, resolverAgent("claude", "id", id), "")
}

func TestResolveClaudeRejectsAmbiguousAndEscapingFiles(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	root := filepath.Join(home, ".claude", "projects")
	resolverWriteFile(t, filepath.Join(root, "project-a", "duplicate.jsonl"), "{}\n")
	resolverWriteFile(t, filepath.Join(root, "project-b", "duplicate.jsonl"), "{}\n")
	resolverWant(t, resolverAgent("claude", "id", "duplicate"), "")
	external := filepath.Join(t.TempDir(), "outside.jsonl")
	resolverWriteFile(t, external, "{}\n")
	if err := os.Symlink(external, filepath.Join(root, "project-a", "escaped.jsonl")); err != nil {
		t.Fatal(err)
	}
	resolverWant(t, resolverAgent("claude", "id", "escaped"), "")
	externalProject := t.TempDir()
	resolverWriteFile(t, filepath.Join(externalProject, "linked-project.jsonl"), "{}\n")
	if err := os.Symlink(externalProject, filepath.Join(root, "linked-project")); err != nil {
		t.Fatal(err)
	}
	resolverWant(t, resolverAgent("claude", "id", "linked-project"), "")
	resolverWriteFile(t, filepath.Join(root, "project-a", "subagents", "nested.jsonl"), "{}\n")
	resolverWant(t, resolverAgent("claude", "id", "nested"), "")
}

func TestResolveCodexMetadataAndConfiguredRoot(t *testing.T) {
	home := t.TempDir()
	codexHome := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CODEX_HOME", "")
	id := "codex-demo_123"
	fallbackPath := filepath.Join(home, ".codex", "sessions", "2023", "11", "14", "rollout-2023-11-14T22-13-20-"+id+".jsonl")
	metadata := `{"type":"session_meta","payload":{"id":"` + id + `","cwd":"/synthetic/project"}}` + "\n"
	resolverWriteFile(t, fallbackPath, metadata)
	resolverWant(t, resolverAgent("codex", "id", id), fallbackPath)
	configuredPath := filepath.Join(codexHome, "sessions", "2023", "11", "14", "rollout-2023-11-14T22-13-20-"+id+".jsonl")
	resolverWriteFile(t, configuredPath, metadata)
	t.Setenv("CODEX_HOME", codexHome)
	resolverWant(t, resolverAgent("codex", "id", id), configuredPath)
	resolverWant(t, resolverAgent("codex", "id", "codex-demo"), "")
	resolverWant(t, resolverAgent("codex", "path", configuredPath), "")
	for _, unsafe := range []string{"../codex-demo_123", "*", "codex?", "[abc]", "/absolute", `nested\id`, "id\x00"} {
		resolverWant(t, resolverAgent("codex", "id", unsafe), "")
	}
	mismatch := filepath.Join(codexHome, "sessions", "rollout-synthetic-mismatch.jsonl")
	resolverWriteFile(t, mismatch, `{"type":"session_meta","payload":{"id":"not-mismatch"}}`+"\n")
	resolverWant(t, resolverAgent("codex", "id", "mismatch"), "")
	malformed := filepath.Join(codexHome, "sessions", "rollout-synthetic-malformed.jsonl")
	resolverWriteFile(t, malformed, "not JSON\n")
	resolverWant(t, resolverAgent("codex", "id", "malformed"), "")
	wrongType := filepath.Join(codexHome, "sessions", "rollout-synthetic-wrong-type.jsonl")
	resolverWriteFile(t, wrongType, `{"type":"response_item","payload":{"id":"wrong-type"}}`+"\n")
	resolverWant(t, resolverAgent("codex", "id", "wrong-type"), "")
	overlong := filepath.Join(codexHome, "sessions", "rollout-synthetic-oversized.jsonl")
	resolverWriteFile(t, overlong, strings.Repeat("x", 1024*1024+1)+"\n")
	resolverWant(t, resolverAgent("codex", "id", "oversized"), "")
	// Configured roots never fall back to another tree if the ID is absent.
	t.Setenv("CODEX_HOME", t.TempDir())
	resolverWant(t, resolverAgent("codex", "id", id), "")
	t.Setenv("CODEX_HOME", "relative-root")
	resolverWant(t, resolverAgent("codex", "id", id), "")
}

func TestResolveCodexRejectsAmbiguousAndEscapingFiles(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("CODEX_HOME", home)
	root := filepath.Join(home, "sessions")
	metadata := `{"type":"session_meta","payload":{"id":"duplicate"}}` + "\n"
	resolverWriteFile(t, filepath.Join(root, "day-a", "rollout-a-duplicate.jsonl"), metadata)
	resolverWriteFile(t, filepath.Join(root, "day-b", "rollout-b-duplicate.jsonl"), metadata)
	resolverWant(t, resolverAgent("codex", "id", "duplicate"), "")
	external := t.TempDir()
	outside := filepath.Join(external, "rollout-synthetic-escaped.jsonl")
	resolverWriteFile(t, outside, `{"type":"session_meta","payload":{"id":"escaped"}}`+"\n")
	if err := os.Symlink(outside, filepath.Join(root, "rollout-linked-escaped.jsonl")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(root, "linked-directory")); err != nil {
		t.Fatal(err)
	}
	resolverWant(t, resolverAgent("codex", "id", "escaped"), "")
}

func TestResolveErrorsDoNotExposePaths(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	resolverWriteFile(t, filepath.Join(home, ".claude"), "not a directory")
	_, err := Resolve(resolverAgent("claude", "id", "private-session-id"))
	if err == nil || strings.Contains(err.Error(), home) || strings.Contains(err.Error(), "private-session-id") {
		t.Fatalf("resolution error is missing or exposes native data: %v", err)
	}
}
