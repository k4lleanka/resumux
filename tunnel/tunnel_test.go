package tunnel

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"log"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const testToken = "test-token"

var quiet = log.New(io.Discard, "", 0)

// echoServer accepts TCP connections and echoes everything back.
func echoServer(t *testing.T) string {
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
}

func startServer(t *testing.T, cfg ServerConfig) *harness {
	t.Helper()
	if cfg.Token == "" {
		cfg.Token = testToken
	}
	if cfg.Target == "" {
		cfg.Target = echoServer(t)
	}
	cfg.Logger = quiet
	sv, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewUnstartedServer(sv)
	tl := &trackingListener{Listener: hs.Listener}
	hs.Listener = tl
	hs.Start()
	t.Cleanup(func() {
		sv.Close()
		hs.Close()
	})
	return &harness{server: sv, listener: tl, url: "ws" + strings.TrimPrefix(hs.URL, "http")}
}

func startClient(t *testing.T, cfg ClientConfig) string {
	t.Helper()
	if cfg.Token == "" {
		cfg.Token = testToken
	}
	cfg.Logger = log.New(testWriter{t}, "", 0)
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
	addr := startClient(t, ClientConfig{ServerURL: h.url})

	want := randomData(t, 2<<20)
	if got := roundTrip(t, addr, want); !bytes.Equal(got, want) {
		t.Fatalf("got %d bytes back, want %d", len(got), len(want))
	}
}

// TestPathsCutDuringTransfer cuts WebSocket connections on the server side
// while data flows. The TCP connection through the tunnel must not notice.
func TestPathsCutDuringTransfer(t *testing.T) {
	h := startServer(t, ServerConfig{})
	addr := startClient(t, ClientConfig{ServerURL: h.url, Paths: 3})

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
	cl, err := NewClient(ClientConfig{ServerURL: h.url, Token: "wrong", Logger: quiet})
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

type testWriter struct{ t *testing.T }

func (w testWriter) Write(b []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(b)))
	return len(b), nil
}
