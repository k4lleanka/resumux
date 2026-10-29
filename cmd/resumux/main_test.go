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

func TestServerURLs(t *testing.T) {
	got, err := serverURLs("wss://a.example/ , wss://a.example:8443/")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "wss://a.example/" || got[1] != "wss://a.example:8443/" {
		t.Fatalf("got %q", got)
	}
	for _, bad := range []string{"", "https://a.example/", "wss://a.example/,", "a.example"} {
		if _, err := serverURLs(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestListenAddrs(t *testing.T) {
	got, err := listenAddrs(":443, :2053,[::1]:8443")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != ":443" || got[1] != ":2053" || got[2] != "[::1]:8443" {
		t.Fatalf("got %q", got)
	}
	for _, bad := range []string{"", ":443,", "443", ":443,:443"} {
		if _, err := listenAddrs(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestLoadConfigList(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"server": ["wss://a.example/", "wss://a.example:8443/"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	server := fs.String("server", "", "")
	if err := loadConfig(fs, path); err != nil {
		t.Fatal(err)
	}
	if *server != "wss://a.example/,wss://a.example:8443/" {
		t.Fatalf("server = %q", *server)
	}
}

func TestParsePrefixes(t *testing.T) {
	got, err := parsePrefixes("127.0.0.1, 10.0.0.0/8, ::1, 2001:db8::/32")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"127.0.0.1/32", "10.0.0.0/8", "::1/128", "2001:db8::/32"}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i].String() != want[i] {
			t.Errorf("got %s, want %s", got[i], want[i])
		}
	}
	for _, bad := range []string{"10.0.0.0/8, example.com", "0.0.0.0/0", "::/0", "10.0.0.1/8", "fe80::1%eth0", "::ffff:0:0/64"} {
		if _, err := parsePrefixes(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	mapped, err := parsePrefixes("::ffff:127.0.0.1, ::ffff:10.0.0.0/104")
	if err != nil {
		t.Fatal(err)
	}
	if mapped[0].String() != "127.0.0.1/32" || mapped[1].String() != "10.0.0.0/8" {
		t.Errorf("mapped addresses became %v", mapped)
	}
}
