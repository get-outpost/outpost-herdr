// Package state durably tracks a namespace's stream cursor and published sessions.
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

	outpost "github.com/get-outpost/outpost-go"
)

const formatVersion = 1

// Store serializes cursor and lifecycle updates to one atomic state file.
// A namespace must have only one active Store (and daemon) at a time; the mutex
// protects goroutines using this instance, not separate processes or instances.
type Store struct {
	mu        sync.Mutex
	path      string
	cursor    int64
	hasCursor bool
	live      map[string]struct{}
}

type diskState struct {
	Version      int      `json:"version"`
	Cursor       *int64   `json:"cursor"`
	LiveSessions []string `json:"live_sessions"`
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
	s := &Store{path: filepath.Join(dir, hex.EncodeToString(hash[:])+".json"), live: make(map[string]struct{})}
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
		Version      int             `json:"version"`
		Cursor       json.RawMessage `json:"cursor"`
		LiveSessions []string        `json:"live_sessions"`
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

// persist is called with mu held. Sync the file before replacement, and the
// directory afterwards so a successful call survives a crash after rename.
func (s *Store) persist(cursor *int64) error {
	data, err := json.Marshal(diskState{Version: formatVersion, Cursor: cursor, LiveSessions: s.liveSessions()})
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
