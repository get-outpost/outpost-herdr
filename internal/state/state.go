// Package state durably tracks a namespace's stream, sessions, and transcripts.
package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	outpost "github.com/get-outpost/outpost-go"
)

const formatVersion = 1

// Store serializes cursor, lifecycle, and transcript updates to one atomic file.
// A namespace must have only one active Store (and daemon) at a time; the mutex
// protects goroutines using this instance, not separate processes or instances.
type Store struct {
	mu          sync.Mutex
	path        string
	cursor      int64
	hasCursor   bool
	live        map[string]struct{}
	transcripts map[string]TranscriptCheckpoint
}

type diskState struct {
	Version      int                             `json:"version"`
	Cursor       *int64                          `json:"cursor"`
	LiveSessions []string                        `json:"live_sessions"`
	Transcripts  map[string]TranscriptCheckpoint `json:"transcripts,omitempty"`
}

// TranscriptCheckpoint records complete JSONL lines, never conversation text or
// transcript paths. SeenIDs includes deliberately skipped older backfill entries.
// FileID and Anchor are hashes used to detect replacement and in-place rewrites.
type TranscriptCheckpoint struct {
	Initialized bool                 `json:"initialized"`
	Offset      int64                `json:"offset"`
	FileID      string               `json:"file_id,omitempty"`
	Anchor      string               `json:"anchor,omitempty"`
	SeenIDs     []string             `json:"seen_ids,omitempty"`
	Fallbacks   []TranscriptFallback `json:"fallbacks,omitempty"`
}

// TranscriptFallback retains only hashes and a timestamp. Native echo suppression
// requires positive command identity, not merely matching conversational text.
type TranscriptFallback struct {
	MessageID   string    `json:"message_id"`
	CommandHash string    `json:"command_hash"`
	TextHash    string    `json:"text_hash"`
	DeliveredAt time.Time `json:"delivered_at"`
}

// Open loads the namespace's state, refusing malformed or unsupported files.
// A missing file is the only first-run case. Namespace text never enters paths.
func Open(namespace string) (*Store, error) {
	if strings.TrimSpace(namespace) == "" {
		return nil, errors.New("state namespace must not be blank")
	}
	root := os.Getenv("XDG_STATE_HOME")
	if root == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("resolve state home: %w", err)
		}
		root = filepath.Join(home, ".local", "state")
	}
	dir := filepath.Join(root, "outpost-herdr")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return nil, fmt.Errorf("protect state directory: %w", err)
	}
	hash := sha256.Sum256([]byte(namespace))
	s := &Store{
		path:        filepath.Join(dir, hex.EncodeToString(hash[:])+".json"),
		live:        make(map[string]struct{}),
		transcripts: make(map[string]TranscriptCheckpoint),
	}
	info, err := os.Lstat(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect state: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("state file must be a regular file")
	}
	if err := os.Chmod(s.path, 0600); err != nil {
		return nil, fmt.Errorf("protect state file: %w", err)
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		return nil, fmt.Errorf("read state: %w", err)
	}
	// RawMessage distinguishes an absent cursor (corruption) from an explicit
	// null cursor (lifecycle data written before stream initialization).
	var disk struct {
		Version      int                             `json:"version"`
		Cursor       json.RawMessage                 `json:"cursor"`
		LiveSessions []string                        `json:"live_sessions"`
		Transcripts  map[string]TranscriptCheckpoint `json:"transcripts"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&disk); err != nil {
		return nil, errors.New("invalid state JSON")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("invalid trailing state data")
	}
	if disk.Version != formatVersion {
		return nil, fmt.Errorf("unsupported state version %d", disk.Version)
	}
	if len(disk.Cursor) == 0 {
		return nil, errors.New("state cursor is missing")
	}
	var cursor *int64
	if err := json.Unmarshal(disk.Cursor, &cursor); err != nil {
		return nil, errors.New("invalid state cursor")
	}
	if cursor != nil {
		if *cursor < 0 {
			return nil, errors.New("state cursor must not be negative")
		}
		s.cursor, s.hasCursor = *cursor, true
	}
	if disk.LiveSessions == nil {
		return nil, errors.New("state live_sessions must be an array")
	}
	for _, id := range disk.LiveSessions {
		if strings.TrimSpace(id) == "" {
			return nil, errors.New("state contains a blank session ID")
		}
		if _, exists := s.live[id]; exists {
			return nil, errors.New("state contains duplicate session IDs")
		}
		s.live[id] = struct{}{}
	}
	for id, checkpoint := range disk.Transcripts {
		if strings.TrimSpace(id) == "" {
			return nil, errors.New("state contains a blank transcript session ID")
		}
		if err := validateTranscript(checkpoint); err != nil {
			return nil, err
		}
		s.transcripts[id] = checkpoint
	}
	return s, nil
}

// Cursor returns the durable cursor and whether it has been initialized. Zero
// is a valid initialized cursor and is distinct from a first-run missing cursor.
func (s *Store) Cursor() (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor, s.hasCursor
}

// SetCursor persists a nonnegative cursor. Equal or older cursors are no-ops.
func (s *Store) SetCursor(cursor int64) error {
	if cursor < 0 {
		return errors.New("state cursor must not be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hasCursor && cursor <= s.cursor {
		return nil
	}
	if err := s.persist(&cursor); err != nil {
		return err
	}
	s.cursor, s.hasCursor = cursor, true
	return nil
}

// LiveSessions returns a sorted independent snapshot of the durable live set.
func (s *Store) LiveSessions() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.liveSessions()
}

func (s *Store) liveSessions() []string {
	ids := make([]string, 0, len(s.live))
	for id := range s.live {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// RecordPublished records only successfully published lifecycle events. Starts
// and updates add a session; ended and deleted remove it. Other events are no-ops.
func (s *Store) RecordPublished(eventType, sessionID string) error {
	var add bool
	switch eventType {
	case outpost.SessionStarted, outpost.SessionUpdated:
		add = true
	case outpost.SessionEnded, outpost.SessionDeleted:
	default:
		return nil
	}
	if strings.TrimSpace(sessionID) == "" {
		return errors.New("published lifecycle event requires a session ID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.live[sessionID]
	if exists == add {
		return nil
	}
	if add {
		s.live[sessionID] = struct{}{}
	} else {
		delete(s.live, sessionID)
	}
	var cursor *int64
	if s.hasCursor {
		cursor = &s.cursor
	}
	if err := s.persist(cursor); err != nil {
		if exists {
			s.live[sessionID] = struct{}{}
		} else {
			delete(s.live, sessionID)
		}
		return err
	}
	return nil
}

// Transcript returns an independent copy, including its metadata slices.
func (s *Store) Transcript(sessionID string) (TranscriptCheckpoint, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	checkpoint, ok := s.transcripts[sessionID]
	return cloneTranscript(checkpoint), ok
}

// SetTranscript commits under the same mutex as cursor and lifecycle writes.
// Failed persistence leaves the in-memory checkpoint unchanged.
func (s *Store) SetTranscript(sessionID string, checkpoint TranscriptCheckpoint) error {
	if strings.TrimSpace(sessionID) == "" {
		return errors.New("transcript requires a session ID")
	}
	if err := validateTranscript(checkpoint); err != nil {
		return err
	}
	checkpoint = cloneTranscript(checkpoint)
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, exists := s.transcripts[sessionID]
	if s.transcripts == nil {
		s.transcripts = make(map[string]TranscriptCheckpoint)
	}
	s.transcripts[sessionID] = checkpoint
	var cursor *int64
	if s.hasCursor {
		cursor = &s.cursor
	}
	if err := s.persist(cursor); err != nil {
		if exists {
			s.transcripts[sessionID] = previous
		} else {
			delete(s.transcripts, sessionID)
		}
		return err
	}
	return nil
}

func cloneTranscript(checkpoint TranscriptCheckpoint) TranscriptCheckpoint {
	checkpoint.SeenIDs = append([]string(nil), checkpoint.SeenIDs...)
	checkpoint.Fallbacks = append([]TranscriptFallback(nil), checkpoint.Fallbacks...)
	return checkpoint
}

func validateTranscript(checkpoint TranscriptCheckpoint) error {
	if checkpoint.Offset < 0 || (!checkpoint.Initialized && checkpoint.Offset != 0) {
		return errors.New("invalid transcript offset")
	}
	if checkpoint.Initialized && checkpoint.FileID == "" {
		return errors.New("initialized transcript has no file identity")
	}
	if checkpoint.Offset > 0 && checkpoint.Anchor == "" {
		return errors.New("transcript offset has no anchor")
	}
	seen := make(map[string]struct{}, len(checkpoint.SeenIDs))
	for _, id := range checkpoint.SeenIDs {
		if strings.TrimSpace(id) == "" {
			return errors.New("transcript contains a blank message ID")
		}
		if _, exists := seen[id]; exists {
			return errors.New("transcript contains duplicate message IDs")
		}
		seen[id] = struct{}{}
	}
	for _, fallback := range checkpoint.Fallbacks {
		if fallback.MessageID == "" || fallback.CommandHash == "" || fallback.TextHash == "" || fallback.DeliveredAt.IsZero() {
			return errors.New("invalid transcript fallback metadata")
		}
	}
	return nil
}

// persist is called with mu held. Sync the file before replacement, and the
// directory afterwards so a successful call survives a crash after rename.
func (s *Store) persist(cursor *int64) error {
	data, err := json.Marshal(diskState{
		Version: formatVersion, Cursor: cursor, LiveSessions: s.liveSessions(),
		Transcripts: s.transcripts,
	})
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	dir := filepath.Dir(s.path)
	file, err := os.CreateTemp(dir, ".state-*")
	if err != nil {
		return fmt.Errorf("create temporary state: %w", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write state: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync state: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close state: %w", err)
	}
	if err := os.Rename(file.Name(), s.path); err != nil {
		return fmt.Errorf("replace state: %w", err)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open state directory for sync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync state directory: %w", err)
	}
	return nil
}
