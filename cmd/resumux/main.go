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
	"net/netip"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"github.com/k4lleanka/resumux/tunnel"
	"github.com/k4lleanka/resumux/wspath"
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
		// A list, such as several server URLs, is the comma-separated form
		// the flag takes.
		if list, ok := v.([]any); ok {
			parts := make([]string, len(list))
			for i, e := range list {
				parts[i] = fmt.Sprint(e)
			}
			v = strings.Join(parts, ",")
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
	listen := fs.String("listen", ":8080", "address to accept WebSocket connections on, or several separated by commas")
	path := fs.String("path", "/", "HTTP path that accepts tunnels")
	target := fs.String("target", "", "host:port every session is forwarded to (required)")
	tokenFile := fs.String("token-file", "", "file with the shared token (or set RESUMUX_TOKEN)")
	tlsCert := fs.String("tls-cert", "", "TLS certificate file; serves plain HTTP if empty")
	tlsKey := fs.String("tls-key", "", "TLS key file")
	maxSessions := fs.Int("max-sessions", 1024, "maximum concurrent sessions")
	maxPaths := fs.Int("max-paths", 8, "maximum paths per session")
	resume := fs.Duration("resume-timeout", 2*time.Minute, "how long a session without paths is kept")
	deadAfter := fs.Duration("dead-after", 5*time.Second, "how long a path may stay silent before it is dropped")
	proxyProto := fs.Bool("proxy-protocol", false, "begin each connection to the target with a PROXY protocol v2 header naming the client")
	trustedProxies := fs.String("trusted-proxies", "", "comma-separated addresses or CIDR ranges of proxies allowed to name the client in -client-ip-header")
	clientIPHeader := fs.String("client-ip-header", "", "header in which trusted proxies name the client, e.g. CF-Connecting-IP")
	perClient := fs.Int("max-sessions-per-client", 0, "sessions one client address may hold at once (0: no limit)")
	newPerClient := fs.Int("max-new-sessions-per-client", 0, "sessions one client address may open per minute (0: no limit)")
	refuseRedundant := fs.Bool("refuse-redundant", false, "give clients that ask for -redundant plain sessions instead")
	fs.Parse(args)
	if err := loadConfig(fs, *config); err != nil {
		return err
	}
	if *target == "" {
		return errors.New("-target is required")
	}
	if *deadAfter < time.Second {
		return errors.New("-dead-after must be at least 1s")
	}
	proxies, err := parsePrefixes(*trustedProxies)
	if err != nil {
		return err
	}
	if (len(proxies) == 0) != (*clientIPHeader == "") {
		return errors.New("-trusted-proxies and -client-ip-header must be given together")
	}
	if *perClient < 0 || *newPerClient < 0 {
		return errors.New("per-client limits cannot be negative")
	}
	if (*perClient > 0 || *newPerClient > 0) && len(proxies) == 0 {
		log.Printf("per-client limits see only the address each request comes from. Behind a proxy or CDN connector, set -trusted-proxies and -client-ip-header, or every client looks like the proxy: unlimited if it is on this machine, limited together if it is not")
	}
	addrs, err := listenAddrs(*listen)
	if err != nil {
		return err
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
		Token:                   token,
		Target:                  *target,
		MaxSessions:             *maxSessions,
		MaxPathsPerSession:      *maxPaths,
		ResumeTimeout:           *resume,
		Path:                    pathOptions(*deadAfter),
		ProxyProtocol:           *proxyProto,
		TrustedProxies:          proxies,
		ClientIPHeader:          *clientIPHeader,
		MaxSessionsPerClient:    *perClient,
		MaxNewSessionsPerClient: *newPerClient,
		RefuseRedundant:         *refuseRedundant,
	})
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle(*path, sv)
	hs := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		// Hijacked WebSocket connections are not affected; this only drops
		// idle keep-alive connections, such as after a rejected handshake.
		IdleTimeout: time.Minute,
	}
	// All are opened before any is served, so a taken port fails at once.
	var listeners []net.Listener
	for _, a := range addrs {
		l, err := net.Listen("tcp", a)
		if err != nil {
			for _, l := range listeners {
				l.Close()
			}
			return err
		}
		listeners = append(listeners, l)
	}

	ctx, stop := signalContext()
	defer stop()
	go func() {
		<-ctx.Done()
		sv.Close()
		hs.Close()
	}()

	log.Printf("server listening on %s, path %s, forwarding to %s", strings.Join(addrs, ", "), *path, *target)
	errc := make(chan error, len(listeners))
	for _, l := range listeners {
		go func() {
			if *tlsCert != "" {
				errc <- hs.ServeTLS(l, *tlsCert, *tlsKey)
			} else {
				errc <- hs.Serve(l)
			}
		}()
	}
	// One listener failing stops the others, so the server never runs on
	// fewer addresses than it was told.
	err = <-errc
	sv.Close()
	hs.Close()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func runClient(args []string) error {
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	config := fs.String("config", "", "JSON file with settings")
	listen := fs.String("listen", "", "local address to accept connections on (required)")
	server := fs.String("server", "", "server URL, e.g. wss://example.com/, or several separated by commas that reach the same server (required)")
	tokenFile := fs.String("token-file", "", "file with the shared token (or set RESUMUX_TOKEN)")
	paths := fs.Int("paths", 2, "WebSocket paths kept open per session")
	host := fs.String("host", "", "Host header to send, if it differs from the server URL")
	caFile := fs.String("ca-file", "", "PEM file with CA certificates to trust instead of the system ones")
	resume := fs.Duration("resume-timeout", 2*time.Minute, "how long to keep trying to resume a session")
	deadAfter := fs.Duration("dead-after", 5*time.Second, "how long a path may stay silent before the session moves off it")
	redundant := fs.Bool("redundant", false, "send frames on two paths at once while little is in flight, so a lost or stalled packet does not freeze the stream (for games; uses more bandwidth)")
	fs.Parse(args)
	if err := loadConfig(fs, *config); err != nil {
		return err
	}
	if *listen == "" || *server == "" {
		return errors.New("-listen and -server are required")
	}
	urls, err := serverURLs(*server)
	if err != nil {
		return err
	}
	if *deadAfter < time.Second {
		return errors.New("-dead-after must be at least 1s")
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
		ServerURLs:    urls,
		Token:         token,
		Paths:         *paths,
		ResumeTimeout: *resume,
		Path:          pathOptions(*deadAfter),
		Header:        header,
		Dialer:        dialer,
		Redundant:     *redundant,
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

// pathOptions pings five times per dead-after interval, so a few lost pings
// alone never drop a healthy path.
func pathOptions(deadAfter time.Duration) wspath.Options {
	return wspath.Options{PingInterval: deadAfter / 5, DeadAfter: deadAfter}
}

// serverURLs splits a comma-separated -server value and checks each URL.
func serverURLs(s string) ([]string, error) {
	var urls []string
	for _, u := range strings.Split(s, ",") {
		u = strings.TrimSpace(u)
		parsed, err := url.Parse(u)
		if err != nil || (parsed.Scheme != "ws" && parsed.Scheme != "wss") || parsed.Host == "" {
			return nil, fmt.Errorf("-server: %q is not a ws:// or wss:// URL", u)
		}
		urls = append(urls, u)
	}
	return urls, nil
}

// listenAddrs parses a comma-separated list of addresses to listen on.
func listenAddrs(s string) ([]string, error) {
	var addrs []string
	seen := map[string]bool{}
	for _, a := range strings.Split(s, ",") {
		a = strings.TrimSpace(a)
		if _, _, err := net.SplitHostPort(a); err != nil {
			return nil, fmt.Errorf("-listen: %q is not host:port", a)
		}
		if seen[a] {
			return nil, fmt.Errorf("-listen: %q is listed twice", a)
		}
		seen[a] = true
		addrs = append(addrs, a)
	}
	return addrs, nil
}

// parsePrefixes parses a comma-separated list of addresses and CIDR ranges.
// A bare address stands for itself alone. IPv4 written as IPv6 is turned
// back into IPv4, since the server compares unmapped addresses. A range
// covering every address is refused: it would let anyone name any client.
func parsePrefixes(s string) ([]netip.Prefix, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var out []netip.Prefix
	for _, f := range strings.Split(s, ",") {
		f = strings.TrimSpace(f)
		var p netip.Prefix
		if strings.Contains(f, "/") {
			var err error
			if p, err = netip.ParsePrefix(f); err != nil {
				return nil, fmt.Errorf("-trusted-proxies: %q is not an address or CIDR range", f)
			}
			if p != p.Masked() {
				return nil, fmt.Errorf("-trusted-proxies: %q has host bits set; did you mean %s?", f, p.Masked())
			}
		} else {
			a, err := netip.ParseAddr(f)
			if err != nil || a.Zone() != "" {
				return nil, fmt.Errorf("-trusted-proxies: %q is not an address or CIDR range", f)
			}
			p = netip.PrefixFrom(a, a.BitLen())
		}
		if a := p.Addr(); a.Is4In6() {
			if p.Bits() < 96 {
				return nil, fmt.Errorf("-trusted-proxies: %q mixes IPv4 and IPv6", f)
			}
			p = netip.PrefixFrom(a.Unmap(), p.Bits()-96)
		}
		if p.Bits() == 0 {
			return nil, fmt.Errorf("-trusted-proxies: %q trusts every address, which would let any client name any address", f)
		}
		out = append(out, p)
	}
	return out, nil
}
