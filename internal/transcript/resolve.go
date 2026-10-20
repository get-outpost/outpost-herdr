package transcript

import (
	"bufio"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/get-outpost/outpost-go"
	"github.com/get-outpost/outpost-herdr/internal/herdr"
)

// Resolve locates only the explicitly identified native session. Missing,
// unsupported and unsafe references are unavailable rather than guessed from cwd.
// Errors deliberately omit native IDs and filesystem paths.
func Resolve(agent herdr.Agent) (string, error) {
	ref := agent.AgentSession
	if ref == nil || ref.Value == "" {
		return "", nil
	}
	provider := strings.ToLower(strings.TrimSpace(agent.Agent))
	if ref.Agent != "" && strings.ToLower(strings.TrimSpace(ref.Agent)) != provider {
		return "", nil
	}
	switch provider {
	case outpost.ProviderOMP, outpost.ProviderPi:
		if ref.Kind != "path" {
			return "", nil
		}
		path := ref.Value
		if strings.HasPrefix(path, "~/") {
			home, err := os.UserHomeDir()
			if err != nil || !filepath.IsAbs(home) {
				return "", nil
			}
			path = filepath.Join(home, path[2:])
			relative, err := filepath.Rel(home, path)
			if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
				return "", nil
			}
		}
		if !filepath.IsAbs(path) || strings.ContainsRune(path, 0) {
			return "", nil
		}
		regular, err := regularFile(path)
		if err != nil || !regular {
			return "", err
		}
		return filepath.Clean(path), nil
	case outpost.ProviderClaude, outpost.ProviderCodex:
		if ref.Kind != "id" || !safeNativeID(ref.Value) {
			return "", nil
		}
	default:
		return "", nil
	}
	home, err := os.UserHomeDir()
	if provider == outpost.ProviderClaude {
		if err != nil || !filepath.IsAbs(home) {
			return "", nil
		}
		return resolveClaude(filepath.Join(home, ".claude", "projects"), ref.Value)
	}
	codexHome := os.Getenv("CODEX_HOME")
	if codexHome == "" {
		if err != nil || !filepath.IsAbs(home) {
			return "", nil
		}
		codexHome = filepath.Join(home, ".codex")
	}
	// Relative configuration would depend on daemon cwd and could select a
	// different user's tree. An explicit absolute CODEX_HOME is authoritative.
	if !filepath.IsAbs(codexHome) || strings.ContainsRune(codexHome, 0) {
		return "", nil
	}
	return resolveCodex(filepath.Join(codexHome, "sessions"), ref.Value)
}

func safeNativeID(id string) bool {
	if len(id) == 0 || len(id) > 200 {
		return false
	}
	for _, char := range id {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '-' || char == '_' {
			continue
		}
		return false
	}
	return true
}

func resolveClaude(root, id string) (string, error) {
	projects, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", resolutionError()
	}
	match := ""
	for _, project := range projects {
		// Do not follow symlinked project directories or recurse into subagents.
		if !project.IsDir() || project.Type()&os.ModeSymlink != 0 {
			continue
		}
		candidate := filepath.Join(root, project.Name(), id+".jsonl")
		regular, err := containedRegular(root, candidate)
		if err != nil {
			return "", err
		}
		if regular {
			if match != "" {
				// Ambiguous identity must not expose an arbitrary project.
				return "", nil
			}
			match = candidate
		}
	}
	return match, nil
}

func resolveCodex(root, id string) (string, error) {
	match := ""
	ambiguous := false
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "rollout-") || !strings.HasSuffix(name, "-"+id+".jsonl") {
			return nil
		}
		regular, err := containedRegular(root, path)
		if err != nil || !regular {
			return err
		}
		matches, err := codexMetadataMatches(path, id)
		if err != nil {
			return err
		}
		if matches {
			if match != "" {
				ambiguous = true
				return fs.SkipAll
			}
			match = path
		}
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", resolutionError()
	}
	if ambiguous {
		return "", nil
	}
	return match, nil
}

func codexMetadataMatches(path, id string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, resolutionError()
	}
	defer file.Close()
	// Metadata is small; malformed or oversized first records are unsupported.
	// Scanner bounds memory even when a non-transcript file is encountered.
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil && !errors.Is(err, bufio.ErrTooLong) {
			return false, resolutionError()
		}
		return false, nil
	}
	var metadata struct {
		Type    string `json:"type"`
		Payload struct {
			ID string `json:"id"`
		} `json:"payload"`
	}
	if json.Unmarshal(scanner.Bytes(), &metadata) != nil {
		return false, nil
	}
	return metadata.Type == "session_meta" && metadata.Payload.ID == id, nil
}

func containedRegular(root, candidate string) (bool, error) {
	info, err := os.Lstat(candidate)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, resolutionError()
	}
	if !info.Mode().IsRegular() {
		return false, nil
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return false, resolutionError()
	}
	resolvedCandidate, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return false, resolutionError()
	}
	relative, err := filepath.Rel(resolvedRoot, resolvedCandidate)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return false, nil
	}
	return true, nil
}

func regularFile(path string) (bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrInvalid) {
		return false, nil
	}
	if err != nil {
		return false, resolutionError()
	}
	return info.Mode().IsRegular(), nil
}

func resolutionError() error {
	return errors.New("resolve transcript: filesystem access failed")
}
