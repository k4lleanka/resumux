// Package wspath carries session frames over a WebSocket connection.
package wspath

import (
	"errors"
	"sync"
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
		o.PingInterval = 10 * time.Second
	}
	if o.DeadAfter <= 0 {
		o.DeadAfter = 3 * o.PingInterval
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
}

// New wraps c and starts its keepalive.
func New(c *websocket.Conn, opts Options) *Path {
	p := &Path{c: c, opts: opts.withDefaults(), done: make(chan struct{})}
	c.SetReadLimit(p.opts.MaxFrameSize)
	c.SetReadDeadline(time.Now().Add(p.opts.DeadAfter))
	c.SetPongHandler(func(string) error {
		return c.SetReadDeadline(time.Now().Add(p.opts.DeadAfter))
	})
	go p.keepalive()
	return p
}

func (p *Path) keepalive() {
	t := time.NewTicker(p.opts.PingInterval)
	defer t.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-t.C:
			p.wmu.Lock()
			err := p.c.WriteControl(websocket.PingMessage, nil, time.Now().Add(p.opts.WriteTimeout))
			p.wmu.Unlock()
			if err != nil {
				p.Close()
				return
			}
		}
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
	p.c.SetReadDeadline(time.Now().Add(p.opts.DeadAfter))
	return b, nil
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

// Close closes the underlying connection.
func (p *Path) Close() error {
	var err error
	p.closeOnce.Do(func() {
		close(p.done)
		err = p.c.Close()
	})
	return err
}
