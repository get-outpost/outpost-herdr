// Package config loads the shared Outpost login configuration without exposing
// credential values in errors.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/get-outpost/outpost-go/client"
)

// Load resolves URL and token independently: nonempty environment variables win
// over the corresponding shared login fields. With both variables present, the
// login file is never accessed. Missing files are allowed if the resulting SDK
// configuration is valid; unreadable or malformed files are errors when needed.
func Load() (client.Config, error) {
	url, token := os.Getenv("OUTPOST_URL"), os.Getenv("OUTPOST_TOKEN")
	var cfg client.Config
	if url == "" || token == "" {
		root := os.Getenv("XDG_CONFIG_HOME")
		if root == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return client.Config{}, errors.New("cannot resolve Outpost configuration home")
			}
			root = filepath.Join(home, ".config")
		}
		data, err := os.ReadFile(filepath.Join(root, "outpost", "config.json"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			// Path errors may contain user-controlled path text. Neither paths nor
			// parser input belong in credential-loading diagnostics.
			return client.Config{}, errors.New("cannot read Outpost login configuration")
		}
		if err == nil {
			if err := json.Unmarshal(data, &cfg); err != nil {
				return client.Config{}, errors.New("invalid JSON in Outpost login configuration")
			}
		}
	}
	if url != "" {
		cfg.URL = url
	}
	if token != "" {
		cfg.Token = token
	}
	if err := cfg.Validate(); err != nil {
		// SDK validation reports field requirements, never field values.
		return client.Config{}, fmt.Errorf("invalid Outpost configuration: %w", err)
	}
	return cfg, nil
}
