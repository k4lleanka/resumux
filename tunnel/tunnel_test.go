package tunnel

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/k4lleanka/resumux/session"
	"github.com/k4lleanka/resumux/wspath"
)

const testToken = "test-token"

var quiet = log.New(io.Discard, "", 0)

// echoServer accepts TCP connections and echoes everything back.
func echoServer(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				io.Copy(c, c)
				c.(*net.TCPConn).CloseWrite()
			}()
		}
	}()
	return l.Addr().String()
}

// trackingListener remembers accepted connections so a test can cut them.
type trackingListener struct {
	net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func (l *trackingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.mu.Lock()
		l.conns = append(l.conns, c)
		l.mu.Unlock()
	}
	return c, err
}

// cutOne closes one random connection, like a network path going away.
func (l *trackingListener) cutOne() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.conns) == 0 {
		return false
	}
	i := mrand.IntN(len(l.conns))
	l.conns[i].Close()
	l.conns = append(l.conns[:i], l.conns[i+1:]...)
	return true
}

type harness struct {
	server   *Server
	listener *trackingListener
	url      string
	// blocked makes the server refuse every handshake, as if it were
	// unreachable.
	blocked atomic.Bool
}

func startServer(t testing.TB, cfg ServerConfig) *harness {
	t.Helper()
	if cfg.Token == "" {
		cfg.Token = testToken
	}
	if cfg.Target == "" {
		cfg.Target = echoServer(t)
	}
	cfg.Logger = testLogger(t, "server: ")
	sv, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{server: sv}
	hs := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.blocked.Load() {
			http.Error(w, "unreachable", http.StatusServiceUnavailable)
			return
		}
		sv.ServeHTTP(w, r)
	}))
	h.listener = &trackingListener{Listener: hs.Listener}
	hs.Listener = h.listener
	hs.Start()
	t.Cleanup(func() {
		sv.Close()
		hs.Close()
	})
	h.url = "ws" + strings.TrimPrefix(hs.URL, "http")
	return h
}

func startClient(t testing.TB, cfg ClientConfig) string {
	t.Helper()
	if cfg.Token == "" {
		cfg.Token = testToken
	}
	cfg.Logger = testLogger(t, "client: ")
	cl, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go cl.Serve(l)
	return l.Addr().String()
}

// roundTrip sends data through the tunnel to the echo server and returns
// what came back.
func roundTrip(t *testing.T, addr string, data []byte) []byte {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(60 * time.Second))

	go func() {
		for rest := data; len(rest) > 0; {
			n := min(len(rest), 1+mrand.IntN(32<<10))
			if _, err := c.Write(rest[:n]); err != nil {
				return
			}
			rest = rest[n:]
		}
		c.(*net.TCPConn).CloseWrite()
	}()
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("read after %d of %d bytes: %v", len(got), len(data), err)
	}
	return got
}

func randomData(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestEcho(t *testing.T) {
	h := startServer(t, ServerConfig{})
	addr := startClient(t, ClientConfig{ServerURLs: []string{h.url}})

	want := randomData(t, 2<<20)
	if got := roundTrip(t, addr, want); !bytes.Equal(got, want) {
		t.Fatalf("got %d bytes back, want %d", len(got), len(want))
	}
}

// TestPathsCutDuringTransfer cuts WebSocket connections on the server side
// while data flows. The TCP connection through the tunnel must not notice.
func TestPathsCutDuringTransfer(t *testing.T) {
	h := startServer(t, ServerConfig{})
	addr := startClient(t, ClientConfig{ServerURLs: []string{h.url}, Paths: 3})

	stop := make(chan struct{})
	cuts := make(chan int, 1)
	go func() {
		n := 0
		for {
			select {
			case <-stop:
				cuts <- n
				return
			case <-time.After(time.Duration(5+mrand.IntN(20)) * time.Millisecond):
				if h.listener.cutOne() {
					n++
				}
			}
		}
	}()

	want := randomData(t, 32<<20)
	if got := roundTrip(t, addr, want); !bytes.Equal(got, want) {
		t.Fatalf("got %d bytes back, want %d, or contents differ", len(got), len(want))
	}
	close(stop)
	if n := <-cuts; n < 10 {
		t.Fatalf("only %d paths were cut during the transfer", n)
	} else {
		t.Logf("%d paths cut during the transfer", n)
	}
}

func TestWrongTokenIsRejected(t *testing.T) {
	h := startServer(t, ServerConfig{})
	cl, err := NewClient(ClientConfig{ServerURLs: []string{h.url}, Token: "wrong", Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	a, b := net.Pipe()
	defer b.Close()
	err = cl.Handle(context.Background(), a)
	if err == nil || !strings.Contains(err.Error(), "token") {
		t.Fatalf("Handle with a wrong token returned %v", err)
	}
	if n := h.server.NumSessions(); n != 0 {
		t.Fatalf("server has %d sessions after a rejected handshake", n)
	}
}

// TestResumeNeedsSecret checks that knowing a session id is not enough to
// attach a path to it.
func TestResumeNeedsSecret(t *testing.T) {
	h := startServer(t, ServerConfig{})

	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+testToken)
	hdr.Set(headerVersion, protocolVersion)
	c, resp, err := websocket.DefaultDialer.Dial(h.url, hdr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id := resp.Header.Get(headerSession)

	hdr.Set(headerSession, id)
	hdr.Set(headerSecret, strings.Repeat("0", 64))
	_, resp, err = websocket.DefaultDialer.Dial(h.url, hdr)
	if err == nil {
		t.Fatal("resumed a session with a wrong secret")
	}
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("wrong secret: got %v, want 404", resp)
	}
}

func TestSessionExpiresWithoutPaths(t *testing.T) {
	h := startServer(t, ServerConfig{ResumeTimeout: 300 * time.Millisecond})

	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+testToken)
	hdr.Set(headerVersion, protocolVersion)
	c, _, err := websocket.DefaultDialer.Dial(h.url, hdr)
	if err != nil {
		t.Fatal(err)
	}
	if n := h.server.NumSessions(); n != 1 {
		t.Fatalf("got %d sessions, want 1", n)
	}
	c.Close()
	c.Close()

	deadline := time.Now().Add(5 * time.Second)
	for h.server.NumSessions() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("session was not closed after its paths went away")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestTooManySessions(t *testing.T) {
	h := startServer(t, ServerConfig{MaxSessions: 1})

	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+testToken)
	hdr.Set(headerVersion, protocolVersion)
	c, _, err := websocket.DefaultDialer.Dial(h.url, hdr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, resp, err := websocket.DefaultDialer.Dial(h.url, hdr)
	if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second session: err=%v resp=%v, want 503", err, resp)
	}
}

// testLogger collects log output and prints it only if the test fails.
// Client and server goroutines can outlive a test, so writing straight to
// t.Log would race with the test finishing.
func testLogger(t testing.TB, prefix string) *log.Logger {
	var mu sync.Mutex
	var buf strings.Builder
	t.Cleanup(func() {
		if t.Failed() {
			mu.Lock()
			t.Log("\n" + buf.String())
			mu.Unlock()
		}
	})
	return log.New(writerFunc(func(b []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(b)
	}), prefix, log.Lmicroseconds)
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(b []byte) (int, error) { return f(b) }

func TestManyConnectionsWhilePathsAreCut(t *testing.T) {
	h := startServer(t, ServerConfig{})
	addr := startClient(t, ClientConfig{ServerURLs: []string{h.url}})

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(10+mrand.IntN(30)) * time.Millisecond):
				h.listener.cutOne()
			}
		}
	}()

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			want := randomData(t, 256<<10)
			if got := roundTrip(t, addr, want); !bytes.Equal(got, want) {
				t.Errorf("got %d bytes back, want %d, or contents differ", len(got), len(want))
			}
		}()
	}
	wg.Wait()
}

// listenTarget runs handle for every connection to a new TCP listener.
func listenTarget(t testing.TB, handle func(*net.TCPConn)) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go handle(c.(*net.TCPConn))
		}
	}()
	return l.Addr().String()
}

// TestHalfClose checks that closing the client's write side reaches the
// target as EOF while the reply can still flow back.
func TestHalfClose(t *testing.T) {
	target := listenTarget(t, func(c *net.TCPConn) {
		defer c.Close()
		b, _ := io.ReadAll(c)
		fmt.Fprintf(c, "got %d bytes", len(b))
	})
	h := startServer(t, ServerConfig{Target: target})
	addr := startClient(t, ClientConfig{ServerURLs: []string{h.url}})

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	c.Write(make([]byte, 12345))
	c.(*net.TCPConn).CloseWrite()
	reply, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	if string(reply) != "got 12345 bytes" {
		t.Fatalf("reply %q", reply)
	}
}

// TestTargetClosesFirst checks that a target hanging up reaches the client as
// EOF, and that the session is freed once the client closes too. Until then
// the client may still be sending, exactly as with a direct TCP connection.
func TestTargetClosesFirst(t *testing.T) {
	target := listenTarget(t, func(c *net.TCPConn) {
		c.Write([]byte("bye"))
		c.Close()
	})
	h := startServer(t, ServerConfig{Target: target})
	addr := startClient(t, ClientConfig{ServerURLs: []string{h.url}})

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	got, err := io.ReadAll(c)
	if err != nil || string(got) != "bye" {
		t.Fatalf("got %q, %v", got, err)
	}
	c.Close()

	deadline := time.Now().Add(10 * time.Second)
	for h.server.NumSessions() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("session still open after both ends closed")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func startTLSServer(t testing.TB) (*httptest.Server, string) {
	t.Helper()
	sv, err := NewServer(ServerConfig{Token: testToken, Target: echoServer(t), Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewTLSServer(sv)
	t.Cleanup(func() {
		sv.Close()
		hs.Close()
	})
	return hs, "wss" + strings.TrimPrefix(hs.URL, "https")
}

func TestTLSWithTrustedCertificate(t *testing.T) {
	hs, url := startTLSServer(t)
	pool := x509.NewCertPool()
	pool.AddCert(hs.Certificate())
	addr := startClient(t, ClientConfig{
		ServerURLs: []string{url},
		Dialer:     &websocket.Dialer{TLSClientConfig: &tls.Config{RootCAs: pool}},
	})

	want := randomData(t, 1<<20)
	if got := roundTrip(t, addr, want); !bytes.Equal(got, want) {
		t.Fatalf("got %d bytes back, want %d", len(got), len(want))
	}
}

// An untrusted certificate will not become trusted by retrying, so the
// client must fail at once instead of retrying until the resume timeout.
func TestTLSWithUntrustedCertificateFailsFast(t *testing.T) {
	_, url := startTLSServer(t)
	cl, err := NewClient(ClientConfig{ServerURLs: []string{url}, Token: testToken, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	a, b := net.Pipe()
	defer b.Close()

	start := time.Now()
	err = cl.Handle(context.Background(), a)
	var certErr *tls.CertificateVerificationError
	if !errors.As(err, &certErr) {
		t.Fatalf("got %v, want a certificate verification error", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("took %s to give up", d)
	}
}

// cutAll closes every connection accepted so far.
func (l *trackingListener) cutAll() {
	for l.cutOne() {
	}
}

// TestTailSurvivesOutageAfterServerFinishes reproduces data loss seen
// behind a CDN: the server had sent everything and was closing, waiting for
// the client to acknowledge the last data, while the client's paths were
// down. Closing must wait as long as a resume could take, so nothing is lost.
func TestTailSurvivesOutageAfterServerFinishes(t *testing.T) {
	want := randomData(t, 2<<20)
	target := listenTarget(t, func(c *net.TCPConn) {
		io.Copy(io.Discard, c)
		c.Write(want)
		c.Close()
	})
	h := startServer(t, ServerConfig{Target: target})
	addr := startClient(t, ClientConfig{ServerURLs: []string{h.url}})

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.(*net.TCPConn).CloseWrite()

	// Let the reply reach the client without reading it, so it stays
	// unacknowledged on the server while the server finishes and closes.
	time.Sleep(500 * time.Millisecond)
	h.blocked.Store(true)
	h.listener.cutAll()
	time.Sleep(7 * time.Second)
	h.blocked.Store(false)

	c.SetDeadline(time.Now().Add(30 * time.Second))
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %d bytes, want %d", len(got), len(want))
	}
}

// TestLostSessionIsAnErrorNotEOF checks that when a session cannot be
// resumed, the application sees its connection fail, not a normal end of
// data that would hide the loss.
func TestLostSessionIsAnErrorNotEOF(t *testing.T) {
	target := listenTarget(t, func(c *net.TCPConn) {
		defer c.Close()
		buf := make([]byte, 1024)
		for {
			if _, err := c.Write(buf); err != nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	h := startServer(t, ServerConfig{Target: target, ResumeTimeout: 500 * time.Millisecond})
	addr := startClient(t, ClientConfig{ServerURLs: []string{h.url}, ResumeTimeout: 500 * time.Millisecond})

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(20 * time.Second))
	if _, err := io.ReadFull(c, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}

	h.blocked.Store(true)
	h.listener.cutAll()
	_, err = io.Copy(io.Discard, c)
	if err == nil {
		t.Fatal("connection ended with a clean EOF after its session was lost")
	}
	t.Logf("application saw: %v", err)
}

// TestResumeFinishedSession checks that a client resuming a session the
// server finished cleanly learns that it is done (410), while a wrong secret
// still cannot tell a finished session from an unknown one (404).
func TestResumeFinishedSession(t *testing.T) {
	h := startServer(t, ServerConfig{})

	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+testToken)
	hdr.Set(headerVersion, protocolVersion)
	c, resp, err := websocket.DefaultDialer.Dial(h.url, hdr)
	if err != nil {
		t.Fatal(err)
	}
	id, secret := resp.Header.Get(headerSession), resp.Header.Get(headerSecret)

	s := session.New(session.Config{})
	defer s.Abort(nil)
	s.AddPath(wspath.New(c, wspath.Options{}))
	s.Write([]byte("hello"))
	s.CloseWrite()
	if got, err := io.ReadAll(s); err != nil || string(got) != "hello" {
		t.Fatalf("echo: %q, %v", got, err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for h.server.NumSessions() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("session did not finish")
		}
		time.Sleep(20 * time.Millisecond)
	}

	resumeStatus := func(secret string) int {
		hdr.Set(headerSession, id)
		hdr.Set(headerSecret, secret)
		_, resp, err := websocket.DefaultDialer.Dial(h.url, hdr)
		if err == nil || resp == nil {
			t.Fatalf("resume of a finished session: err=%v resp=%v", err, resp)
		}
		return resp.StatusCode
	}
	if got := resumeStatus(secret); got != http.StatusGone {
		t.Errorf("right secret: got %d, want 410", got)
	}
	if got := resumeStatus(strings.Repeat("0", 64)); got != http.StatusNotFound {
		t.Errorf("wrong secret: got %d, want 404", got)
	}
}

// freezeConn can go silent without closing, like a path whose packets stop
// arriving: reads block and writes vanish until it is closed.
type freezeConn struct {
	net.Conn
	frozen    atomic.Bool
	closed    chan struct{}
	closeOnce sync.Once
}

func (c *freezeConn) Read(b []byte) (int, error) {
	for {
		if c.frozen.Load() {
			<-c.closed
			return 0, net.ErrClosed
		}
		n, err := c.Conn.Read(b)
		if !c.frozen.Load() || err != nil {
			return n, err
		}
	}
}

func (c *freezeConn) Write(b []byte) (int, error) {
	if c.frozen.Load() {
		return len(b), nil
	}
	return c.Conn.Write(b)
}

func (c *freezeConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// freezingListener wraps every accepted connection so a test can freeze it.
type freezingListener struct {
	net.Listener
	mu    sync.Mutex
	conns []*freezeConn
}

func (l *freezingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	fc := &freezeConn{Conn: c, closed: make(chan struct{})}
	l.mu.Lock()
	l.conns = append(l.conns, fc)
	l.mu.Unlock()
	return fc, nil
}

// TestSilentPathFailsOverQuickly freezes the path the client sends on while a
// second path is healthy. The session must move to the healthy path within a
// few seconds, not wait for a long keepalive timeout.
func TestSilentPathFailsOverQuickly(t *testing.T) {
	fl, addr := startFreezable(t)

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(60 * time.Second))
	echo := func(msg string) {
		t.Helper()
		if _, err := c.Write([]byte(msg)); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(msg))
		if _, err := io.ReadFull(c, got); err != nil || string(got) != msg {
			t.Fatalf("got %q, %v", got, err)
		}
	}
	echo("before")
	fl.freezeFirst(t)
	start := time.Now()
	echo("after")
	if d := time.Since(start); d > 3500*time.Millisecond {
		t.Fatalf("took %s to move off a silent path", d.Round(time.Millisecond))
	} else {
		t.Logf("moved off the silent path in %s", d.Round(time.Millisecond))
	}
}

// startFreezable starts a server whose connections can be frozen, and a
// client for it, and returns the listener and the client's address.
func startFreezable(t *testing.T) (*freezingListener, string) {
	t.Helper()
	sv, err := NewServer(ServerConfig{Token: testToken, Target: echoServer(t), Logger: testLogger(t, "server: ")})
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewUnstartedServer(sv)
	fl := &freezingListener{Listener: hs.Listener}
	hs.Listener = fl
	hs.Start()
	t.Cleanup(func() {
		sv.Close()
		hs.Close()
	})
	return fl, startClient(t, ClientConfig{ServerURLs: []string{"ws" + strings.TrimPrefix(hs.URL, "http")}})
}

// freezeFirst waits for a second path, then freezes the first, which is the
// one the client sends on.
func (l *freezingListener) freezeFirst(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		l.mu.Lock()
		if len(l.conns) >= 2 {
			l.conns[0].frozen.Store(true)
			l.mu.Unlock()
			return
		}
		l.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("second path never connected")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSilentPathDuringTransfer freezes the active path in the middle of a
// transfer. Every byte must still arrive once and in order.
func TestSilentPathDuringTransfer(t *testing.T) {
	fl, addr := startFreezable(t)
	data := randomData(t, 8<<20)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(30 * time.Second))

	start := time.Now()
	go func() {
		// Freeze between the two halves, with the first still in flight.
		c.Write(data[:len(data)/2])
		fl.freezeFirst(t)
		c.Write(data[len(data)/2:])
		c.(*net.TCPConn).CloseWrite()
	}()
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %d bytes, want %d, or contents differ", len(got), len(data))
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("8 MiB with a frozen path took %s", d.Round(time.Millisecond))
	}
}

// serveOn starts another HTTP listener for sv and returns its URL and a
// count of the connections it accepted.
func serveOn(t *testing.T, sv *Server) (string, *atomic.Int32) {
	t.Helper()
	var n atomic.Int32
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		sv.ServeHTTP(w, r)
	}))
	t.Cleanup(hs.Close)
	return "ws" + strings.TrimPrefix(hs.URL, "http"), &n
}

// echoOn sends msg on c and checks that it comes back.
func echoOn(t *testing.T, c net.Conn, msg string) {
	t.Helper()
	if _, err := c.Write([]byte(msg)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(c, got); err != nil || string(got) != msg {
		t.Fatalf("got %q, %v", got, err)
	}
}

// TestPathsSpreadOverURLs checks that a session uses every URL it is given,
// which is what puts its paths on different routes.
func TestPathsSpreadOverURLs(t *testing.T) {
	h := startServer(t, ServerConfig{})
	url2, hits := serveOn(t, h.server)
	addr := startClient(t, ClientConfig{ServerURLs: []string{h.url, url2}})

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	echoOn(t, c, "hello")

	deadline := time.Now().Add(5 * time.Second)
	for hits.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the second URL was never used")
		}
		time.Sleep(10 * time.Millisecond)
	}
	echoOn(t, c, "still here")
}

// TestURLReachingAnotherServerDoesNotEndSession gives the client a second URL
// that reaches a different server, which answers 404 for the session. That
// must not end a session whose other path is fine.
func TestURLReachingAnotherServerDoesNotEndSession(t *testing.T) {
	h := startServer(t, ServerConfig{})
	other := startServer(t, ServerConfig{})
	addr := startClient(t, ClientConfig{ServerURLs: []string{h.url, other.url}})

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	echoOn(t, c, "hello")
	// Long enough for the second path to be refused several times.
	time.Sleep(2 * time.Second)
	echoOn(t, c, "still here")
	if n := h.server.NumSessions(); n != 1 {
		t.Fatalf("got %d sessions on the real server, want 1", n)
	}
}

// TestFirstDialTriesNextURL checks that a session opens through the second
// URL at once when the first does not answer.
func TestFirstDialTriesNextURL(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := "ws://" + l.Addr().String() + "/"
	l.Close()

	h := startServer(t, ServerConfig{})
	addr := startClient(t, ClientConfig{ServerURLs: []string{dead, h.url}})

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	start := time.Now()
	echoOn(t, c, "hello")
	if d := time.Since(start); d > time.Second {
		t.Fatalf("took %s to open through the second URL", d.Round(time.Millisecond))
	}
}

// TestStrayURLDuringFullOutage reproduces a kick: every path is down, the
// URL that holds the session is not answering yet, and a URL that reaches
// another server answers 404. That 404 proves nothing, so the session must
// wait for the right URL instead of ending.
func TestStrayURLDuringFullOutage(t *testing.T) {
	h := startServer(t, ServerConfig{})
	other := startServer(t, ServerConfig{})
	addr := startClient(t, ClientConfig{ServerURLs: []string{h.url, other.url}})

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(20 * time.Second))
	echoOn(t, c, "before")

	h.blocked.Store(true)
	h.listener.cutAll()
	time.Sleep(2 * time.Second)
	h.blocked.Store(false)

	echoOn(t, c, "after")
}

// TestFirstDialSkipsCDNError checks that an error page from one URL, as a
// CDN shows when its own connection to the server fails, does not stop the
// client from opening the session through the next URL.
func TestFirstDialSkipsCDNError(t *testing.T) {
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "origin unreachable", 522)
	}))
	t.Cleanup(cdn.Close)
	h := startServer(t, ServerConfig{})
	addr := startClient(t, ClientConfig{ServerURLs: []string{"ws" + strings.TrimPrefix(cdn.URL, "http"), h.url}})

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	echoOn(t, c, "hello")
}

// TestFirstDialSkipsSilentURL checks that a URL which accepts the connection
// but never answers holds up a new session for seconds, not for the
// dialer's whole handshake timeout.
func TestFirstDialSkipsSilentURL(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { c.Close() })
		}
	}()
	h := startServer(t, ServerConfig{})
	addr := startClient(t, ClientConfig{ServerURLs: []string{"ws://" + l.Addr().String() + "/", h.url}})

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(30 * time.Second))
	start := time.Now()
	echoOn(t, c, "hello")
	if d := time.Since(start); d > 8*time.Second {
		t.Fatalf("took %s to get past a silent URL", d.Round(time.Millisecond))
	}
}

// TestFirstDialSkipsForeignServer checks that a URL reaching a server with a
// different token, which answers 401, does not stop the session from opening
// through the next URL.
func TestFirstDialSkipsForeignServer(t *testing.T) {
	foreign := startServer(t, ServerConfig{Token: "someone-elses-token"})
	h := startServer(t, ServerConfig{})
	addr := startClient(t, ClientConfig{ServerURLs: []string{foreign.url, h.url}})

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	echoOn(t, c, "hello")
}

// TestStray410DoesNotFinishSession checks that a 410 from a URL that never
// carried a path of the session, such as a web server answering 410 for
// unknown pages, neither ends the session nor passes for a clean finish.
func TestStray410DoesNotFinishSession(t *testing.T) {
	stray := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "gone", http.StatusGone)
	}))
	t.Cleanup(stray.Close)
	h := startServer(t, ServerConfig{})
	addr := startClient(t, ClientConfig{ServerURLs: []string{h.url, "ws" + strings.TrimPrefix(stray.URL, "http")}})

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(20 * time.Second))
	echoOn(t, c, "before")

	h.blocked.Store(true)
	h.listener.cutAll()
	time.Sleep(2 * time.Second)
	h.blocked.Store(false)

	echoOn(t, c, "after")
}
