package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	err := os.WriteFile(path, []byte(`{"max-sessions": 1000000, "resume-timeout": "30s", "listen": ":9000"}`), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	maxSessions := fs.Int("max-sessions", 0, "")
	resume := fs.Duration("resume-timeout", 0, "")
	listen := fs.String("listen", "", "")
	if err := fs.Parse([]string{"-listen", ":8000"}); err != nil {
		t.Fatal(err)
	}
	if err := loadConfig(fs, path); err != nil {
		t.Fatal(err)
	}

	if *maxSessions != 1000000 {
		t.Errorf("max-sessions = %d, want 1000000", *maxSessions)
	}
	if *resume != 30*time.Second {
		t.Errorf("resume-timeout = %v, want 30s", *resume)
	}
	if *listen != ":8000" {
		t.Errorf("listen = %q, want the command line's :8000", *listen)
	}
}

func TestLoadConfigRejectsUnknownSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"listne": ":9000"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.String("listen", "", "")
	if err := loadConfig(fs, path); err == nil {
		t.Fatal("accepted a misspelled setting")
	}
}
