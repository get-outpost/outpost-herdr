package transcript

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/get-outpost/outpost-herdr/internal/adapter"
	"github.com/get-outpost/outpost-herdr/internal/state"
)

func TestTailFallbackCheckpointSurvivesCleanRestart(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	store, err := state.Open("synthetic-tests")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "not-created-yet.jsonl")
	agent := tailAgent(path, "omp")
	at := time.Now().UTC()
	delivery := deliveredMessage{agent: agent, commandID: "durable-command", text: "durable fallback", at: at}
	firstSink := newTailSink()
	first := newTailWorker(t, firstSink, store)
	if err := first.delivered(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	if got := tailTexts(firstSink); !reflect.DeepEqual(got, []string{"durable fallback"}) {
		t.Fatalf("first fallback = %v", got)
	}
	reopened, err := state.Open("synthetic-tests")
	if err != nil {
		t.Fatal(err)
	}
	secondSink := newTailSink()
	second := newTailWorker(t, secondSink, reopened)
	if err := second.delivered(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	if len(secondSink.messages()) != 0 {
		t.Fatal("restart reposted the admitted command fallback")
	}
	tailWrite(t, path, tailOMPLine(delivery.commandID, "user", delivery.text, at)+tailOMPLine("durable-answer", "assistant", "after restart", at))
	tailScan(t, second, tailSessionFor(t, second, agent))
	if got := tailTexts(secondSink); !reflect.DeepEqual(got, []string{"after restart"}) {
		t.Fatalf("restart echo merge = %v", got)
	}
}

func TestManagerFallbackTextIsBoundedAndRejectedNotificationsAreNotPublished(t *testing.T) {
	sink := newTailSink()
	manager := newTailManager(t, sink, newTailStore())
	agent := tailAgent("", "unknown")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := manager.Delivered(canceled, agent, adapter.Command{ID: "canceled", Type: "command.message", Payload: map[string]any{"text": "admitted before cancellation"}}); err != nil {
		t.Fatal("lost admitted notification on caller cancellation")
	}
	for _, command := range []adapter.Command{
		{ID: "blank", Type: "command.message", Payload: map[string]any{"text": " \t"}},
		{ID: "wrong-type", Type: "command.message", Payload: map[string]any{"text": 42}},
		{Type: "command.message", Payload: map[string]any{"text": "missing command identity"}},
	} {
		if err := manager.Delivered(context.Background(), agent, command); err == nil {
			t.Fatal("accepted malformed admission notification")
		}
	}
	text := strings.Repeat("界", MaxMessageBytes)
	if err := manager.Delivered(context.Background(), agent, adapter.Command{ID: "large", Type: "command.message", Payload: map[string]any{"text": text}}); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
	messages := sink.messages()
	if len(messages) != 2 {
		t.Fatalf("fallback count = %d; want 2", len(messages))
	}
	if messages[0].Payload["text"] != "admitted before cancellation" {
		t.Fatal("canceled callback lost an admitted prompt")
	}
	got := messages[1].Payload["text"].(string)
	if len(got) > MaxMessageBytes || !utf8.ValidString(got) || !strings.Contains(got, "[truncated]") {
		t.Fatal("fallback text lacks a valid bounded truncation marker")
	}
}

func TestManagerRejectsInvalidConfiguration(t *testing.T) {
	sink, store := newTailSink(), newTailStore()
	for _, test := range []struct {
		sink      adapter.Sink
		store     Store
		namespace string
		options   Options
	}{
		{nil, store, "namespace", Options{}},
		{sink, nil, "namespace", Options{}},
		{sink, store, " \t", Options{}},
		{sink, store, "namespace", Options{PollInterval: -time.Second}},
	} {
		if _, err := New(test.sink, test.store, test.namespace, test.options); err == nil {
			t.Fatal("accepted invalid manager configuration")
		}
	}
}
