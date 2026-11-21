package dynarbiter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeCredsFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "env")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("writing test fixture: %s", err)
	}
	return path
}

func TestLoadCredsParsesKamalStyle(t *testing.T) {
	path := writeCredsFile(t, "# kamal accessory env file\n"+
		"\n"+
		"AWS_ACCESS_KEY_ID=AKIAEXAMPLE\n"+
		"AWS_SECRET_ACCESS_KEY=test-secret\n"+
		"\n"+
		"# trailing comment\n")

	id, secret, err := LoadCreds(path)
	if err != nil {
		t.Fatalf("LoadCreds: unexpected error: %s", err)
	}
	if id != "AKIAEXAMPLE" {
		t.Errorf("id = %q, want AKIAEXAMPLE", id)
	}
	if secret != "test-secret" {
		t.Errorf("secret = %q, want test-secret", secret)
	}
}

func TestLoadCredsRejectsExportPrefix(t *testing.T) {
	path := writeCredsFile(t, "export AWS_ACCESS_KEY_ID=AKIAEXAMPLE\n"+
		"AWS_SECRET_ACCESS_KEY=test-secret\n")

	_, _, err := LoadCreds(path)
	if err == nil {
		t.Fatal("LoadCreds: expected an error for an export-prefixed line, got nil")
	}
	if !strings.Contains(err.Error(), "export") {
		t.Errorf("error %q does not mention the export prefix", err)
	}
}

func TestLoadCredsMissingKeyFails(t *testing.T) {
	tests := []struct {
		name     string
		contents string
	}{
		{"missing both", "FOO=bar\n"},
		{"missing secret", "AWS_ACCESS_KEY_ID=AKIAEXAMPLE\n"},
		{"missing id", "AWS_SECRET_ACCESS_KEY=test-secret\n"},
		{"empty id", "AWS_ACCESS_KEY_ID=\nAWS_SECRET_ACCESS_KEY=test-secret\n"},
		{"empty secret", "AWS_ACCESS_KEY_ID=AKIAEXAMPLE\nAWS_SECRET_ACCESS_KEY=\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeCredsFile(t, tt.contents)
			_, _, err := LoadCreds(path)
			if err == nil {
				t.Fatal("LoadCreds: expected an error for a missing key, got nil")
			}
		})
	}
}

// TestLoadCredsMalformedLineNeverLeaksTheLine pins a real failure the first
// draft had: a typo'd "KEY value" (space, not "=") lands in the no-"="
// branch, and the value half of that typo is often a secret. The error must
// name the line number without ever echoing the line's content to stderr.
func TestLoadCredsMalformedLineNeverLeaksTheLine(t *testing.T) {
	path := writeCredsFile(t, "AWS_ACCESS_KEY_ID AKIAEXAMPLE\nAWS_SECRET_ACCESS_KEY=test-secret\n")

	_, _, err := LoadCreds(path)
	if err == nil {
		t.Fatal("LoadCreds: expected an error for a malformed line, got nil")
	}
	if strings.Contains(err.Error(), "AKIAEXAMPLE") {
		t.Errorf("error %q echoes the malformed line's content", err)
	}
}

func TestLoadCredsUnreadableFileFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist")

	_, _, err := LoadCreds(path)
	if err == nil {
		t.Fatal("LoadCreds: expected an error for an unreadable file, got nil")
	}
	if !strings.Contains(err.Error(), path) {
		t.Errorf("error %q does not name the path", err)
	}
}
