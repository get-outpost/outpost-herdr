package transcript

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	outpost "github.com/get-outpost/outpost-go"
	"github.com/get-outpost/outpost-herdr/internal/herdr"
	"github.com/get-outpost/outpost-herdr/internal/state"
)

func TestDeclaredSourceBeforeFileCreationDoesNotFallback(t *testing.T) {
	for _, kind := range []string{outpost.ProviderOMP, outpost.ProviderPi, outpost.ProviderCodex} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "lazy.jsonl")
			agent := tailAgent(path, kind)
			body := tailOMPLine("native-user", "user", "late prompt", time.Time{}) + tailOMPLine("native-assistant", "assistant", "late answer", time.Time{})
			if kind == outpost.ProviderCodex {
				t.Setenv("CODEX_HOME", root)
				path = filepath.Join(root, "sessions", "rollout-test-native-session.jsonl")
				agent.AgentSession = &herdr.SessionRef{Kind: "id", Agent: kind, Value: "native-session"}
				body = `{"type":"session_meta","payload":{"id":"native-session"}}` + "\n" +
					`{"type":"response_item","payload":{"type":"message","id":"native-user","role":"user","content":[{"type":"input_text","text":"late prompt"}]}}` + "\n" +
					`{"type":"response_item","payload":{"type":"message","id":"native-assistant","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"late answer"}]}}` + "\n"
			}
			sink := newTailSink()
			worker := newTailWorker(t, sink, newTailStore())
			// Prompt admission precedes the harness creating its native file.
			if err := worker.delivered(context.Background(), deliveredMessage{agent: agent, commandID: "unrelated-command-id", text: "late prompt", at: time.Now()}); err != nil {
				t.Fatal(err)
			}
			if got := tailTexts(sink); len(got) != 0 {
				t.Fatalf("declared source produced premature fallback: %v", got)
			}
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			tailWrite(t, path, body)
			tailScan(t, worker, tailSessionFor(t, worker, agent))
			if got := tailTexts(sink); !reflect.DeepEqual(got, []string{"late prompt", "late answer"}) {
				t.Fatalf("native conversation = %v; want exactly one user and assistant turn", got)
			}
			messages := sink.messages()
			if messages[0].Payload["role"] != "user" || messages[1].Payload["role"] != "assistant" {
				t.Fatalf("conversation roles = %v, %v", messages[0].Payload["role"], messages[1].Payload["role"])
			}
		})
	}
}

func TestOMPWithoutDeclaredSourceStillUsesAdmittedFallback(t *testing.T) {
	sink := newTailSink()
	worker := newTailWorker(t, sink, newTailStore())
	delivery := deliveredMessage{agent: tailAgent("", outpost.ProviderOMP), commandID: "no-source-command", text: "fallback prompt", at: time.Now()}
	for range 2 {
		if err := worker.delivered(context.Background(), delivery); err != nil {
			t.Fatal(err)
		}
	}
	if got := tailTexts(sink); !reflect.DeepEqual(got, []string{"fallback prompt"}) {
		t.Fatalf("no-source admitted conversation = %v", got)
	}
	if sink.messages()[0].Payload["role"] != "user" {
		t.Fatal("admitted fallback must be a user turn")
	}
}

func TestPersistedFallbackEchoRequiresPositiveIdentity(t *testing.T) {
	for _, test := range []struct {
		name, nativeID string
		untimed        bool
		want           []string
	}{
		{"matching-command", "prior-command", false, []string{"same prompt", "answer"}},
		{"unrelated-identical-turn", "unrelated-native-id", false, []string{"same prompt", "same prompt", "answer"}},
		{"untimed-turn", "prior-command", true, []string{"same prompt", "same prompt", "answer"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "existing.jsonl")
			agent := tailAgent(path, "omp")
			sink, store := newTailSink(), newTailStore()
			worker := newTailWorker(t, sink, store)
			session := tailSessionFor(t, worker, agent)
			at := time.Now().UTC()
			// A checkpoint from a previous run may contain a published fallback.
			// Loading it must neither repost that fallback nor swallow a repeat.
			checkpoint := state.TranscriptCheckpoint{
				SeenIDs: []string{"prior-fallback"},
				Fallbacks: []state.TranscriptFallback{{
					MessageID: "prior-fallback", CommandHash: digest("prior-command"),
					TextHash: digest("same prompt"), DeliveredAt: at,
				}},
			}
			if err := store.SetTranscript(session.id, checkpoint); err != nil {
				t.Fatal(err)
			}
			delete(worker.sessions, session.id)
			nativeTime := at
			if test.untimed {
				nativeTime = time.Time{}
			}
			tailWrite(t, path, tailOMPLine(test.nativeID, "user", "same prompt", nativeTime)+
				tailOMPLine("legitimate-repeat", "user", "same prompt", at)+
				tailOMPLine("answer", "assistant", "answer", at))
			tailScan(t, worker, tailSessionFor(t, worker, agent))
			if got := tailTexts(sink); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("checkpoint conversation = %v; want %v", got, test.want)
			}
		})
	}
}
