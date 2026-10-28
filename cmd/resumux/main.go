// Command resumux forwards TCP connections over resumable WebSocket sessions.
//
//	resumux server -listen :8080 -target 127.0.0.1:22 -token-file token
//	resumux client -listen 127.0.0.1:2222 -server wss://example.com/ -token-file token
//
// Every flag can also be given in a JSON file passed with -config, using the
// flag names as keys. Flags on the command line win over the file.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"github.com/k4lleanka/resumux/tunnel"
)

// version is set at build time with -ldflags "-X main.version=v1.0.0".
var version = "dev"

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("resumux: ")

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "server":
		err = runServer(os.Args[2:])
	case "client":
		err = runClient(os.Args[2:])
	case "version", "-version", "--version":
		fmt.Println("resumux", version)
		return
	case "-h", "-help", "--help", "help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  resumux server [flags]   accept tunnels and forward them to a target
  resumux client [flags]   forward local connections through a server
  resumux version          print the version

Run "resumux server -h" or "resumux client -h" for flags.`)
}

// loadConfig applies a JSON config file to every flag not set on the command
// line.
func loadConfig(fs *flag.FlagSet, path string) error {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	// UseNumber keeps numbers as written; as floats, 1000000 would print as
	// 1e+06 and fail to parse as an int.
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var values map[string]any
	if err := dec.Decode(&values); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	for k, v := range values {
		if k == "config" {
			return fmt.Errorf("%s: config files cannot include each other", path)
		}
		if fs.Lookup(k) == nil {
			return fmt.Errorf("%s: unknown setting %q", path, k)
		}
		if set[k] {
			continue
		}
		if err := fs.Set(k, fmt.Sprint(v)); err != nil {
			return fmt.Errorf("%s: invalid value %v for %q", path, v, k)
		}
	}
	return nil
}

// readToken reads the shared token from a file, or from RESUMUX_TOKEN. It is
// deliberately not a flag: command lines are visible to other users.
func readToken(file string) (string, error) {
	if file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return "", err
		}
		tok := strings.TrimSpace(string(b))
		if tok == "" {
			return "", fmt.Errorf("%s is empty", file)
		}
		return tok, nil
	}
	if tok := os.Getenv("RESUMUX_TOKEN"); tok != "" {
		return tok, nil
	}
	return "", errors.New("no token: use -token-file or set RESUMUX_TOKEN")
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func runServer(args []string) error {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	config := fs.String("config", "", "JSON file with settings")
	listen := fs.String("listen", ":8080", "address to accept WebSocket connections on")
	path := fs.String("path", "/", "HTTP path that accepts tunnels")
	target := fs.String("target", "", "host:port every session is forwarded to (required)")
	tokenFile := fs.String("token-file", "", "file with the shared token (or set RESUMUX_TOKEN)")
	tlsCert := fs.String("tls-cert", "", "TLS certificate file; serves plain HTTP if empty")
	tlsKey := fs.String("tls-key", "", "TLS key file")
	maxSessions := fs.Int("max-sessions", 1024, "maximum concurrent sessions")
	maxPaths := fs.Int("max-paths", 8, "maximum paths per session")
	resume := fs.Duration("resume-timeout", 2*time.Minute, "how long a session without paths is kept")
	fs.Parse(args)
	if err := loadConfig(fs, *config); err != nil {
		return err
	}
	if *target == "" {
		return errors.New("-target is required")
	}
	if !strings.HasPrefix(*path, "/") {
		return fmt.Errorf("-path must start with /, got %q", *path)
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		return errors.New("-tls-cert and -tls-key must be given together")
	}
	token, err := readToken(*tokenFile)
	if err != nil {
		return err
	}

	sv, err := tunnel.NewServer(tunnel.ServerConfig{
		Token:              token,
		Target:             *target,
		MaxSessions:        *maxSessions,
		MaxPathsPerSession: *maxPaths,
		ResumeTimeout:      *resume,
	})
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle(*path, sv)
	hs := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		// Hijacked WebSocket connections are not affected; this only drops
		// idle keep-alive connections, such as after a rejected handshake.
		IdleTimeout: time.Minute,
	}

	ctx, stop := signalContext()
	defer stop()
	go func() {
		<-ctx.Done()
		sv.Close()
		hs.Close()
	}()

	log.Printf("server listening on %s%s, forwarding to %s", *listen, *path, *target)
	if *tlsCert != "" {
		err = hs.ListenAndServeTLS(*tlsCert, *tlsKey)
	} else {
		err = hs.ListenAndServe()
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func runClient(args []string) error {
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	config := fs.String("config", "", "JSON file with settings")
	listen := fs.String("listen", "", "local address to accept connections on (required)")
	server := fs.String("server", "", "server URL, e.g. wss://example.com/ (required)")
	tokenFile := fs.String("token-file", "", "file with the shared token (or set RESUMUX_TOKEN)")
	paths := fs.Int("paths", 2, "WebSocket paths kept open per session")
	host := fs.String("host", "", "Host header to send, if it differs from the server URL")
	caFile := fs.String("ca-file", "", "PEM file with CA certificates to trust instead of the system ones")
	resume := fs.Duration("resume-timeout", 2*time.Minute, "how long to keep trying to resume a session")
	fs.Parse(args)
	if err := loadConfig(fs, *config); err != nil {
		return err
	}
	if *listen == "" || *server == "" {
		return errors.New("-listen and -server are required")
	}
	token, err := readToken(*tokenFile)
	if err != nil {
		return err
	}

	var header http.Header
	if *host != "" {
		header = http.Header{"Host": {*host}}
	}
	dialer, err := wsDialer(*caFile)
	if err != nil {
		return err
	}
	cl, err := tunnel.NewClient(tunnel.ClientConfig{
		ServerURL:     *server,
		Token:         token,
		Paths:         *paths,
		ResumeTimeout: *resume,
		Header:        header,
		Dialer:        dialer,
	})
	if err != nil {
		return err
	}
	l, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}

	ctx, stop := signalContext()
	defer stop()
	go func() {
		<-ctx.Done()
		l.Close()
	}()

	log.Printf("client listening on %s, tunneling to %s", l.Addr(), *server)
	err = cl.Serve(l)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

// wsDialer returns a WebSocket dialer that trusts the certificates in caFile,
// or the system's if caFile is empty.
func wsDialer(caFile string) (*websocket.Dialer, error) {
	d := *websocket.DefaultDialer
	if caFile == "" {
		return &d, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s: no PEM certificates found", caFile)
	}
	d.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return &d, nil
}
