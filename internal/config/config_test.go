package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/get-outpost/outpost-go/client"
)

func configureTestHome(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("OUTPOST_URL", "")
	t.Setenv("OUTPOST_TOKEN", "")
	return filepath.Join(root, "outpost", "config.json")
}

func writeLogin(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestIndependentEnvironmentPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		url   string
		token string
		want  client.Config
	}{
		{"file_only", "", "", client.Config{URL: "https://file.example/relay", Token: "file-token"}},
		{"url_override", "https://env.example/prefix", "", client.Config{URL: "https://env.example/prefix", Token: "file-token"}},
		{"token_override", "", "env-token", client.Config{URL: "https://file.example/relay", Token: "env-token"}},
		{"both_override", "https://env.example", "env-token", client.Config{URL: "https://env.example", Token: "env-token"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := configureTestHome(t)
			writeLogin(t, path, `{"url":"https://file.example/relay","token":"file-token","other_login_field":true}`)
			t.Setenv("OUTPOST_URL", tc.url)
			t.Setenv("OUTPOST_TOKEN", tc.token)
			got, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatal("configuration did not independently resolve environment overrides")
			}
		})
	}
}

func TestCompleteEnvironmentDoesNotReadLoginFile(t *testing.T) {
	for _, fileState := range []string{"missing", "malformed", "directory"} {
		t.Run(fileState, func(t *testing.T) {
			path := configureTestHome(t)
			switch fileState {
			case "malformed":
				writeLogin(t, path, `{"token":`)
			case "directory":
				if err := os.MkdirAll(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("OUTPOST_URL", "https://env.example")
			t.Setenv("OUTPOST_TOKEN", "env-token")
			got, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if got.URL != "https://env.example" || got.Token != "env-token" {
				t.Fatal("complete environment not used")
			}
		})
	}
}

func TestPartialEnvironmentCanCompletePartialLogin(t *testing.T) {
	for _, tc := range []struct {
		name, file, url, token string
	}{
		{"file_token", `{"token":"file-token"}`, "https://env.example", ""},
		{"file_url", `{"url":"https://file.example"}`, "", "env-token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := configureTestHome(t)
			writeLogin(t, path, tc.file)
			t.Setenv("OUTPOST_URL", tc.url)
			t.Setenv("OUTPOST_TOKEN", tc.token)
			got, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if err := got.Validate(); err != nil {
				t.Fatal("partial values did not form valid configuration")
			}
		})
	}
}

func TestMissingLoginAndPartialEnvironmentFail(t *testing.T) {
	for _, tc := range []struct{ name, url, token string }{
		{"nothing", "", ""},
		{"url_only", "https://env.example", ""},
		{"token_only", "", "env-token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configureTestHome(t)
			t.Setenv("OUTPOST_URL", tc.url)
			t.Setenv("OUTPOST_TOKEN", tc.token)
			got, err := Load()
			if err == nil {
				t.Fatal("accepted incomplete configuration without a login file")
			}
			if got != (client.Config{}) {
				t.Fatal("failed load returned credential-bearing configuration")
			}
		})
	}
}

func TestHomeFallbackUsesSharedLoginPath(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("OUTPOST_URL", "")
	t.Setenv("OUTPOST_TOKEN", "")
	writeLogin(t, filepath.Join(home, ".config", "outpost", "config.json"), `{"url":"http://localhost:8080","token":"home-token"}`)
	got, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.URL != "http://localhost:8080" || got.Token != "home-token" {
		t.Fatal("shared home login path was not loaded")
	}
}

func TestMalformedOrInvalidConfigNeverExposesSecrets(t *testing.T) {
	const secret = "sentinel-private-credential"
	for _, tc := range []struct {
		name, file, url, token string
	}{
		{"malformed", `{"url":"https://file.example","token":"` + secret + `"`, "", ""},
		{"wrong_token_type", `{"url":"https://file.example","token":{"` + secret + `":true}}`, "", ""},
		{"invalid_file_url", `{"url":"https://user:` + secret + `@file.example","token":"` + secret + `"}`, "", ""},
		{"invalid_file_token", `{"url":"https://file.example","token":"` + secret + ` with-whitespace"}`, "", ""},
		{"invalid_env_url", "", "https://user:" + secret + "@env.example", secret},
		{"invalid_env_token", "", "https://env.example", secret + "\n"},
		{"partial_env_malformed_file", `{"token":"` + secret + `"`, "https://env.example", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := configureTestHome(t)
			if tc.file != "" {
				writeLogin(t, path, tc.file)
			}
			t.Setenv("OUTPOST_URL", tc.url)
			t.Setenv("OUTPOST_TOKEN", tc.token)
			got, err := Load()
			if err == nil {
				t.Fatal("accepted malformed or invalid configuration")
			}
			if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), tc.file) && tc.file != "" {
				t.Fatal("error exposed configuration content")
			}
			if got != (client.Config{}) {
				t.Fatal("failed load returned credentials")
			}
		})
	}
}

func TestLoginReadErrorsDoNotExposePaths(t *testing.T) {
	const secret = "sentinel-private-credential"
	root := filepath.Join(t.TempDir(), secret)
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("OUTPOST_URL", "https://env.example")
	t.Setenv("OUTPOST_TOKEN", "")
	// Reading a directory as config fails without relying on Unix permissions.
	if err := os.MkdirAll(filepath.Join(root, "outpost", "config.json"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(); err == nil {
		t.Fatal("accepted unreadable login configuration")
	} else if strings.Contains(err.Error(), secret) {
		t.Fatal("configuration error exposed a path containing a secret")
	}
}
