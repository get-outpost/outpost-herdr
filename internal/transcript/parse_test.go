package transcript

import (
	"bufio"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestParseNativeFixtures(t *testing.T) {
	tests := []struct {
		provider string
		fixture  string
		want     []Message
	}{
		{"pi", "pi.jsonl", []Message{
			{ID: "pi-user-1", Role: "user", Text: "Explain the sample.", Timestamp: time.UnixMilli(1700000000000).UTC()},
			{ID: "pi-assistant-1", Role: "assistant", Text: "The sample is ready.", Timestamp: time.UnixMilli(1700000001000).UTC()},
			{ID: "pi-user-2", Role: "user", Text: "A second question.", Timestamp: time.UnixMilli(1700000002000).UTC()},
			{ID: "pi-assistant-2", Role: "assistant", Text: "A second answer.", Timestamp: time.UnixMilli(1700000003000).UTC()},
		}},
		{"omp", "pi.jsonl", []Message{
			{ID: "pi-user-1", Role: "user", Text: "Explain the sample.", Timestamp: time.UnixMilli(1700000000000).UTC()},
			{ID: "pi-assistant-1", Role: "assistant", Text: "The sample is ready.", Timestamp: time.UnixMilli(1700000001000).UTC()},
			{ID: "pi-user-2", Role: "user", Text: "A second question.", Timestamp: time.UnixMilli(1700000002000).UTC()},
			{ID: "pi-assistant-2", Role: "assistant", Text: "A second answer.", Timestamp: time.UnixMilli(1700000003000).UTC()},
		}},
		{"claude", "claude.jsonl", []Message{
			{ID: "claude-user-1", Role: "user", Text: "Explain the sample.", Timestamp: time.UnixMilli(1700000000123).UTC()},
			{ID: "claude-assistant-1", Role: "assistant", Text: "The sample is ready.", Timestamp: time.UnixMilli(1700000001000).UTC()},
			{ID: "claude-assistant-2", Role: "assistant", Text: "Persisted answer without a stop marker.", Timestamp: time.UnixMilli(1700000002000).UTC()},
		}},
		{"codex", "codex.jsonl", []Message{
			{ID: "codex-user-1", Role: "user", Text: "Explain the sample.", Timestamp: time.UnixMilli(1700000000000).UTC()},
			{ID: "codex-assistant-1", Role: "assistant", Text: "The sample is ready.", Timestamp: time.UnixMilli(1700000001000).UTC()},
			{ID: "codex-assistant-2", Role: "assistant", Text: "A channel-final answer.", Timestamp: time.UnixMilli(1700000002000).UTC()},
		}},
	}
	for _, test := range tests {
		t.Run(test.provider, func(t *testing.T) {
			file, err := os.Open(filepath.Join("testdata", test.fixture))
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			var got []Message
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				if message, ok := Parse(test.provider, scanner.Bytes()); ok {
					got = append(got, message)
				}
			}
			if err := scanner.Err(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("messages = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestParseMalformedAndMixedBlocks(t *testing.T) {
	tests := []struct {
		name     string
		provider string
		line     string
		text     string
	}{
		{"empty", "omp", "", ""},
		{"broken JSON", "omp", `{"type":"message"`, ""},
		{"scalar", "omp", `123`, ""},
		{"array", "omp", `[]`, ""},
		{"null", "omp", `null`, ""},
		{"missing message", "omp", `{"type":"message"}`, ""},
		{"invalid message", "omp", `{"type":"message","message":[]}`, ""},
		{"invalid content", "omp", `{"type":"message","message":{"role":"user","content":{"text":"hidden"}}}`, ""},
		{"blank content", "omp", `{"type":"message","message":{"role":"user","content":" \n\t "}}`, ""},
		{"unknown provider", "unknown", `{"type":"message","message":{"role":"user","content":"hidden"}}`, ""},
		{"mixed blocks", "omp", `{"type":"message","message":{"role":"user","content":[null,17,"bad",{"type":"text","text":17},{"type":"text","text":"Keep "},{"type":"thinking","text":"hidden"},{"type":"tool_result","text":"hidden"},{"type":"text","text":"this."}]}}`, "Keep this."},
		{"tool preamble despite stop", "omp", `{"type":"message","message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"hidden"},{"type":"toolCall"}]}}`, ""},
		{"Claude tool preamble despite end", "claude", `{"type":"assistant","message":{"role":"assistant","stop_reason":"end_turn","content":[{"type":"text","text":"hidden"},{"type":"tool_use"}]}}`, ""},
		{"Claude contradictory role", "claude", `{"type":"assistant","message":{"role":"user","content":"hidden"}}`, ""},
		{"Claude tool result plus user text", "claude", `{"type":"user","message":{"role":"user","content":[{"type":"tool_result","content":"hidden"},{"type":"text","text":"User clarification."}]}}`, "User clarification."},
		{"Codex conflicting markers", "codex", `{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","channel":"analysis","content":[{"type":"output_text","text":"hidden"}]}}`, ""},
		{"Codex wrong block type", "codex", `{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"text","text":"hidden"}]}}`, ""},
		{"Codex tool preamble", "codex", `{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"hidden"},{"type":"function_call"}]}}`, ""},
		{"provider normalized", " Pi ", `{"type":"message","message":{"role":"user","content":"Keep whitespace.\n"}}`, "Keep whitespace.\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message, ok := Parse(test.provider, []byte(test.line))
			if ok != (test.text != "") || message.Text != test.text {
				t.Fatalf("Parse = %#v, %t; want text %q", message, ok, test.text)
			}
		})
	}
}

func TestParseStopReasons(t *testing.T) {
	for _, reason := range []string{"toolUse", "tool_use", "error", "aborted", "abort", "pause_turn", "unknown"} {
		line := `{"type":"message","message":{"role":"assistant","content":"Do not publish.","stopReason":"` + reason + `"}}`
		if message, ok := Parse("omp", []byte(line)); ok {
			t.Fatalf("reason %q published %#v", reason, message)
		}
	}
	for _, reason := range []string{"stop", "length"} {
		line := `{"type":"message","message":{"role":"assistant","content":"Final output.","stopReason":"` + reason + `"}}`
		if message, ok := Parse("omp", []byte(line)); !ok || message.Text != "Final output." {
			t.Fatalf("reason %q = %#v, %t", reason, message, ok)
		}
	}
}

func TestParseTimestamps(t *testing.T) {
	cases := []struct {
		line string
		want time.Time
	}{
		{`{"type":"message","timestamp":"2023-11-14T22:13:20.123456789Z","message":{"role":"user","content":"Sample","timestamp":1700000000123}}`, time.UnixMilli(1700000000123).UTC()},
		{`{"type":"message","timestamp":"2023-11-14T22:13:20.123456789Z","message":{"role":"user","content":"Sample","timestamp":"bad"}}`, time.Unix(1700000000, 123456789).UTC()},
		{`{"type":"message","message":{"role":"user","content":"Sample","timestamp":null}}`, time.Time{}},
	}
	for _, test := range cases {
		got, ok := Parse("omp", []byte(test.line))
		if !ok || !got.Timestamp.Equal(test.want) {
			t.Fatalf("timestamp = %v, %t; want %v", got.Timestamp, ok, test.want)
		}
	}
}

func TestBoundTextUTF8AndByteCap(t *testing.T) {
	for _, text := range []string{"", "Short sample.", strings.Repeat("a", MaxMessageBytes)} {
		if got := BoundText(text); got != text {
			t.Fatalf("within-cap text changed: length %d -> %d", len(text), len(got))
		}
	}
	for _, runeText := range []string{"é", "界", "🙂"} {
		for padding := range len(runeText) {
			text := strings.Repeat("a", padding) + strings.Repeat(runeText, MaxMessageBytes)
			got := BoundText(text)
			if !utf8.ValidString(got) || len(got) > MaxMessageBytes || !strings.HasSuffix(got, truncationMarker) {
				t.Fatalf("invalid bounded text: valid=%t bytes=%d", utf8.ValidString(got), len(got))
			}
			prefix := strings.TrimSuffix(got, truncationMarker)
			if !strings.HasPrefix(text, prefix) || len(got) <= MaxMessageBytes-len(runeText) {
				t.Fatal("truncation did not retain the maximal complete-rune prefix")
			}
		}
	}
	if got := BoundText("sample\xfftext"); !utf8.ValidString(got) || got != "sample\uFFFDtext" {
		t.Fatalf("invalid UTF-8 not repaired: %q", got)
	}
	line := `{"type":"message","message":{"role":"user","content":"` + strings.Repeat("x", MaxMessageBytes+1) + `"}}`
	message, ok := Parse("omp", []byte(line))
	if !ok || len(message.Text) != MaxMessageBytes || !strings.HasSuffix(message.Text, truncationMarker) {
		t.Fatalf("Parse did not bound text: %t, %d bytes", ok, len(message.Text))
	}
}

func TestStableMessageID(t *testing.T) {
	native := Message{ID: "native-entry-demo"}
	first := StableMessageID("session-a", native, 10, []byte("first record"))
	if first != StableMessageID("session-a", native, 100, []byte("rewritten record")) {
		t.Fatal("native ID changed after rotation or offset change")
	}
	if first == StableMessageID("session-b", native, 10, []byte("first record")) || first == StableMessageID("session-a", Message{ID: "different-entry"}, 10, nil) {
		t.Fatal("native IDs collided across sessions or entries")
	}
	fallback := StableMessageID("session-a", Message{}, 10, []byte("record"))
	if fallback != StableMessageID("session-a", Message{}, 10, []byte("record")) {
		t.Fatal("fallback ID is nondeterministic")
	}
	for _, changed := range []string{
		StableMessageID("session-b", Message{}, 10, []byte("record")),
		StableMessageID("session-a", Message{}, 11, []byte("record")),
		StableMessageID("session-a", Message{}, 10, []byte("different record")),
		StableMessageID("session-a", Message{ID: "record"}, 10, []byte("record")),
	} {
		if fallback == changed {
			t.Fatal("fallback ID ignored identity input")
		}
	}
	pathID := StableMessageID("/synthetic/session", Message{ID: "/synthetic/private-entry"}, 0, nil)
	if strings.Contains(pathID, "synthetic") || len(pathID) != len("transcript:")+64 {
		t.Fatalf("message ID exposes native identity: %q", pathID)
	}
	if StableMessageID("ab", Message{ID: "c"}, 0, nil) == StableMessageID("a", Message{ID: "bc"}, 0, nil) {
		t.Fatal("identity tuple boundaries are ambiguous")
	}
}
