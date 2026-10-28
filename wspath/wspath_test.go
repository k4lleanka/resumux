package wspath

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// pair returns a client-side Path and the raw server-side connection.
func pair(t *testing.T, opts Options) (*Path, *websocket.Conn) {
	t.Helper()
	conns := make(chan *websocket.Conn, 1)
	up := websocket.Upgrader{}
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		conns <- c
	}))
	t.Cleanup(hs.Close)

	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(hs.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	p := New(c, opts)
	peer := <-conns
	t.Cleanup(func() {
		p.Close()
		peer.Close()
	})
	return p, peer
}

func TestFramesRoundTrip(t *testing.T) {
	p, peer := pair(t, Options{})
	if err := p.WriteFrame([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	typ, b, err := peer.ReadMessage()
	if err != nil || typ != websocket.BinaryMessage || string(b) != "hello" {
		t.Fatalf("peer got %d %q %v", typ, b, err)
	}
	if err := peer.WriteMessage(websocket.BinaryMessage, []byte("world")); err != nil {
		t.Fatal(err)
	}
	if b, err := p.ReadFrame(); err != nil || string(b) != "world" {
		t.Fatalf("ReadFrame = %q, %v", b, err)
	}
}

// A peer that stops reading never answers pings, so the path must give up.
func TestSilentPeerIsDetected(t *testing.T) {
	p, _ := pair(t, Options{PingInterval: 50 * time.Millisecond, DeadAfter: 200 * time.Millisecond})

	errc := make(chan error, 1)
	go func() {
		_, err := p.ReadFrame()
		errc <- err
	}()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("ReadFrame returned without error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("silent peer was not detected")
	}
}

// A peer that keeps reading answers pings, so an idle path must stay up.
func TestIdlePathStaysAlive(t *testing.T) {
	p, peer := pair(t, Options{PingInterval: 50 * time.Millisecond, DeadAfter: 200 * time.Millisecond})
	go func() {
		for {
			if _, _, err := peer.ReadMessage(); err != nil {
				return
			}
		}
	}()

	errc := make(chan error, 1)
	go func() {
		_, err := p.ReadFrame()
		errc <- err
	}()
	select {
	case err := <-errc:
		t.Fatalf("idle path closed: %v", err)
	case <-time.After(1 * time.Second):
	}
}

func TestTextMessageIsRejected(t *testing.T) {
	p, peer := pair(t, Options{})
	peer.WriteMessage(websocket.TextMessage, []byte("nope"))
	if _, err := p.ReadFrame(); err == nil {
		t.Fatal("text message accepted")
	}
}
