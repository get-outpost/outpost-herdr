package transcript

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/get-outpost/outpost-go"
)

const MaxMessageBytes = 32 * 1024

const truncationMarker = "\n[truncated]"

type Message struct {
	ID        string
	Role      string
	Text      string
	Timestamp time.Time
}

type nativeMessage struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	StopReason string          `json:"stopReason"`
	Stop       string          `json:"stop_reason"`
	Phase      string          `json:"phase"`
	Channel    string          `json:"channel"`
	Timestamp  json.RawMessage `json:"timestamp"`
}

type nativeEntry struct {
	Type      string          `json:"type"`
	ID        string          `json:"id"`
	UUID      string          `json:"uuid"`
	Timestamp json.RawMessage `json:"timestamp"`
	Message   nativeMessage   `json:"message"`
	Payload   nativeMessage   `json:"payload"`
	IsMeta    bool            `json:"isMeta"`
	IsError   bool            `json:"isApiErrorMessage"`
}

// Parse reads persisted native records, not stream deltas. Tool-call preambles
// are discarded as a whole, even if they contain otherwise visible text.
func Parse(provider string, line []byte) (Message, bool) {
	var entry nativeEntry
	if json.Unmarshal(line, &entry) != nil {
		return Message{}, false
	}
	provider = strings.ToLower(strings.TrimSpace(provider))
	message := entry.Message
	id := entry.ID
	codex := false
	switch provider {
	case outpost.ProviderOMP, outpost.ProviderPi:
		if entry.Type != "message" {
			return Message{}, false
		}
	case outpost.ProviderClaude:
		if (entry.Type != "user" && entry.Type != "assistant") || entry.IsMeta || entry.IsError {
			return Message{}, false
		}
		if message.Role != entry.Type {
			return Message{}, false
		}
		id = entry.UUID
	case outpost.ProviderCodex:
		// event_msg and input_item may mirror response_item; accepting both
		// would duplicate turns. Only the canonical persisted records count.
		if entry.Type != "response_item" || entry.Payload.Type != "message" {
			return Message{}, false
		}
		message = entry.Payload
		id = message.ID
		codex = true
	default:
		return Message{}, false
	}
	if message.Role != "user" && message.Role != "assistant" {
		return Message{}, false
	}
	if message.Role == "assistant" {
		if !finalStop(message.StopReason) || !finalStop(message.Stop) {
			return Message{}, false
		}
		if codex {
			// Explicit final markers are required: older unmarked responses
			// cannot reliably distinguish final answers from preambles.
			if message.Phase != "" && message.Phase != "final_answer" {
				return Message{}, false
			}
			if message.Channel != "" && message.Channel != "final" {
				return Message{}, false
			}
			if message.Phase != "final_answer" && message.Channel != "final" {
				return Message{}, false
			}
		}
	}
	text, toolCall := visibleText(message.Content, codex)
	if (message.Role == "assistant" && toolCall) || strings.TrimSpace(text) == "" {
		return Message{}, false
	}
	timestamp := parseTimestamp(message.Timestamp)
	if timestamp.IsZero() {
		timestamp = parseTimestamp(entry.Timestamp)
	}
	return Message{ID: id, Role: message.Role, Text: BoundText(text), Timestamp: timestamp}, true
}

func finalStop(reason string) bool {
	switch reason {
	case "", "stop", "length", "end_turn", "max_tokens", "stop_sequence":
		return true
	default:
		return false
	}
}

func visibleText(content json.RawMessage, codex bool) (string, bool) {
	var plain string
	if json.Unmarshal(content, &plain) == nil {
		return plain, false
	}
	var blocks []json.RawMessage
	if json.Unmarshal(content, &blocks) != nil {
		return "", false
	}
	var text strings.Builder
	toolCall := false
	for _, raw := range blocks {
		var block struct {
			Type string          `json:"type"`
			Text json.RawMessage `json:"text"`
		}
		if json.Unmarshal(raw, &block) != nil {
			continue
		}
		switch block.Type {
		case "toolCall", "tool_use", "function_call", "custom_tool_call":
			toolCall = true
		case "text", "input_text", "output_text":
			if (codex && block.Type == "text") || (!codex && block.Type != "text") {
				continue
			}
			var part string
			if json.Unmarshal(block.Text, &part) == nil {
				text.WriteString(part)
			}
		}
	}
	return text.String(), toolCall
}

func parseTimestamp(raw json.RawMessage) time.Time {
	var value string
	if json.Unmarshal(raw, &value) == nil {
		timestamp, _ := time.Parse(time.RFC3339Nano, value)
		return timestamp
	}
	// Pi's AgentMessage timestamp uses JavaScript epoch milliseconds.
	milliseconds, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(milliseconds).UTC()
}

// BoundText preserves UTF-8 and keeps the visible truncation marker inside the
// byte cap. It is also used for command fallback text not passed through Parse.
func BoundText(text string) string {
	if !utf8.ValidString(text) {
		text = strings.ToValidUTF8(text, "\uFFFD")
	}
	if len(text) <= MaxMessageBytes {
		return text
	}
	end := MaxMessageBytes - len(truncationMarker)
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end] + truncationMarker
}

// StableMessageID is namespaced to the public session ID. Neither native IDs
// (which can contain paths) nor transcript filenames are exposed in payloads.
func StableMessageID(sessionID string, message Message, offset int64, raw []byte) string {
	hash := sha256.New()
	writePart := func(value []byte) {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		hash.Write(length[:])
		hash.Write(value)
	}
	writePart([]byte(sessionID))
	if message.ID != "" {
		writePart([]byte("native"))
		writePart([]byte(message.ID))
	} else {
		writePart([]byte("offset"))
		var position [8]byte
		binary.BigEndian.PutUint64(position[:], uint64(offset))
		writePart(position[:])
		writePart(raw)
	}
	return "transcript:" + hex.EncodeToString(hash.Sum(nil))
}
