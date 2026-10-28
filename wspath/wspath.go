// Package wspath carries session frames over a WebSocket connection.
package wspath

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Options control keepalive and timeouts. Zero values select the defaults.
type Options struct {
	// PingInterval is how often a ping is sent. Proxies and CDNs close idle
	// WebSockets, so this must be shorter than their idle timeout.
	PingInterval time.Duration
	// DeadAfter is how long a path may go without receiving anything,
	// including pongs, before it is considered dead.
	DeadAfter time.Duration
	// WriteTimeout bounds a single write.
	WriteTimeout time.Duration
	// MaxFrameSize is the largest message accepted from the peer.
	MaxFrameSize int64
}

func (o Options) withDefaults() Options {
	if o.PingInterval <= 0 {
		o.PingInterval = time.Second
	}
	// Long enough for one full-size frame on a slow link, since only a
	// complete message counts as received, and short enough that a session
	// leaves a silent path within seconds.
	if o.DeadAfter <= 0 {
		o.DeadAfter = 5 * o.PingInterval
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 10 * time.Second
	}
	if o.MaxFrameSize <= 0 {
		o.MaxFrameSize = 1 << 20
	}
	return o
}

// Path is a session.Path backed by a WebSocket.
type Path struct {
	c    *websocket.Conn
	opts Options

	wmu       sync.Mutex
	closeOnce sync.Once
	done      chan struct{}
	// heard is when the last frame or pong arrived, plus one, measured from
	// start on the monotonic clock so wall-clock jumps cannot fake silence.
	// Zero means the peer has not been heard from yet.
	start time.Time
	heard atomic.Int64
	// halfClosed stops keepalive pings once CloseWrite has been called.
	halfClosed atomic.Bool
}

// New wraps c and starts its keepalive.
func New(c *websocket.Conn, opts Options) *Path {
	p := &Path{c: c, opts: opts.withDefaults(), done: make(chan struct{}), start: time.Now()}
	c.SetReadLimit(p.opts.MaxFrameSize)
	c.SetReadDeadline(time.Now().Add(p.opts.DeadAfter))
	c.SetPongHandler(func(string) error {
		return p.markHeard()
	})
	go p.keepalive()
	return p
}

func (p *Path) keepalive() {
	// Checking four times per interval keeps an idle path pinged close to
	// every interval; checking once would let up to two pass between pings,
	// which makes a healthy idle path look silent.
	t := time.NewTicker(p.opts.PingInterval / 4)
	defer t.Stop()
	var lastPing time.Time
	for {
		select {
		case <-p.done:
			return
		case <-t.C:
		}
		// A path that is delivering frames needs no proof that it works, and a
		// ping would only compete with them for the connection. One ping per
		// interval is enough while the answer is on its way.
		if p.halfClosed.Load() || p.Quiet() < p.opts.PingInterval || time.Since(lastPing) < p.opts.PingInterval {
			continue
		}
		p.wmu.Lock()
		err := p.c.WriteControl(websocket.PingMessage, nil, time.Now().Add(p.opts.WriteTimeout))
		p.wmu.Unlock()
		if err != nil {
			p.Close()
			return
		}
		lastPing = time.Now()
	}
}

var errUnexpectedMessage = errors.New("wspath: unexpected text message")

// ReadFrame returns the next binary message.
func (p *Path) ReadFrame() ([]byte, error) {
	typ, b, err := p.c.ReadMessage()
	if err != nil {
		return nil, err
	}
	if typ != websocket.BinaryMessage {
		return nil, errUnexpectedMessage
	}
	p.markHeard()
	return b, nil
}

// markHeard records that the peer was just heard from on this path. A pong
// counts, since it proves both directions work; a ping from the peer does
// not, since it only proves one.
func (p *Path) markHeard() error {
	p.heard.Store(int64(time.Since(p.start)) + 1)
	return p.c.SetReadDeadline(time.Now().Add(p.opts.DeadAfter))
}

// Quiet reports how long ago the peer was last heard from on this path. A
// path the peer has not been heard from on yet has not shown that it works,
// so it reports the longest possible time.
func (p *Path) Quiet() time.Duration {
	h := p.heard.Load()
	if h == 0 {
		return math.MaxInt64
	}
	return time.Since(p.start) - time.Duration(h-1)
}

// WriteFrame sends b as one binary message.
func (p *Path) WriteFrame(b []byte) error {
	p.wmu.Lock()
	defer p.wmu.Unlock()
	p.c.SetWriteDeadline(time.Now().Add(p.opts.WriteTimeout))
	return p.c.WriteMessage(websocket.BinaryMessage, b)
}

// Done is closed when the path is closed.
func (p *Path) Done() <-chan struct{} { return p.done }

// CloseWrite tells the peer, after everything written so far, that nothing
// more follows: a TCP FIN, or a TLS close_notify. The path keeps reading.
// Closing a socket that still has unread data makes the kernel send a reset,
// which can destroy the last frames on their way to the peer; a half-close
// lets them arrive. No pings are sent afterwards.
func (p *Path) CloseWrite() error {
	p.wmu.Lock()
	defer p.wmu.Unlock()
	p.halfClosed.Store(true)
	if cw, ok := p.c.NetConn().(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}

// Close closes the underlying connection.
func (p *Path) Close() error {
	var err error
	p.closeOnce.Do(func() {
		close(p.done)
		err = p.c.Close()
	})
	return err
}
