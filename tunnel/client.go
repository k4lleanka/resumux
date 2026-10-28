package tunnel

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/k4lleanka/resumux/session"
	"github.com/k4lleanka/resumux/wspath"
)

// ClientConfig configures a Client.
type ClientConfig struct {
	// ServerURL is the server's WebSocket URL, e.g. wss://example.com/tunnel.
	ServerURL string
	// Token is the shared secret the server expects. Required.
	Token string
	// Paths is how many WebSocket paths each session keeps open. More paths
	// make a switch instant when one dies.
	Paths int
	// ResumeTimeout is how long a session may go without any path before the
	// client gives up on it.
	ResumeTimeout time.Duration
	// Header is sent with every handshake, e.g. a Host override.
	Header http.Header
	// Dialer dials the WebSocket. websocket.DefaultDialer if nil.
	Dialer *websocket.Dialer

	Path    wspath.Options
	Session session.Config
	Logger  *log.Logger
}

func (c ClientConfig) withDefaults() ClientConfig {
	if c.Paths <= 0 {
		c.Paths = 2
	}
	if c.ResumeTimeout <= 0 {
		c.ResumeTimeout = 2 * time.Minute
	}
	if c.Dialer == nil {
		c.Dialer = websocket.DefaultDialer
	}
	if c.Logger == nil {
		c.Logger = log.Default()
	}
	return c
}

// Client forwards local TCP connections to a Server.
type Client struct {
	cfg ClientConfig
}

// NewClient validates cfg and returns a Client.
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.ServerURL == "" {
		return nil, errors.New("tunnel: client server URL is required")
	}
	if cfg.Token == "" {
		return nil, errors.New("tunnel: client token is required")
	}
	return &Client{cfg: cfg.withDefaults()}, nil
}

// Serve accepts connections on l and tunnels each one until l is closed.
func (cl *Client) Serve(l net.Listener) error {
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		go func() {
			if err := cl.Handle(context.Background(), c); err != nil {
				cl.cfg.Logger.Printf("resumux: %v", err)
			}
		}()
	}
}

// errSessionGone means the server no longer knows the session, so resuming is
// pointless.
var errSessionGone = errors.New("tunnel: server closed the session")

type sessionAuth struct{ id, secret string }

func (cl *Client) dial(ctx context.Context, auth *sessionAuth) (*websocket.Conn, *http.Response, error) {
	h := http.Header{}
	for k, v := range cl.cfg.Header {
		h[k] = v
	}
	h.Set("Authorization", "Bearer "+cl.cfg.Token)
	h.Set(headerVersion, protocolVersion)
	if auth != nil {
		h.Set(headerSession, auth.id)
		h.Set(headerSecret, auth.secret)
	}
	c, resp, err := cl.cfg.Dialer.DialContext(ctx, cl.cfg.ServerURL, h)
	if err != nil && resp != nil {
		switch resp.StatusCode {
		case http.StatusNotFound:
			return nil, resp, errSessionGone
		case http.StatusUnauthorized:
			return nil, resp, errors.New("tunnel: server rejected the token")
		}
		return nil, resp, fmt.Errorf("tunnel: handshake: %s", resp.Status)
	}
	return c, resp, err
}

// Handle tunnels one local connection and returns when it is finished.
func (cl *Client) Handle(ctx context.Context, local net.Conn) error {
	defer local.Close()

	first, resp, err := cl.dialFirst(ctx)
	if err != nil {
		return err
	}
	auth := &sessionAuth{
		id:     resp.Header.Get(headerSession),
		secret: resp.Header.Get(headerSecret),
	}
	if len(auth.id) < 8 || auth.secret == "" {
		first.Close()
		return errors.New("tunnel: server did not issue a valid session")
	}
	cl.cfg.Logger.Printf("session %s: opened for %s", auth.id[:8], local.RemoteAddr())

	s := session.New(cl.cfg.Session)
	firstPath := wspath.New(first, cl.cfg.Path)
	s.AddPath(firstPath)

	go expireWithoutPaths(s, cl.cfg.ResumeTimeout, ErrSessionExpired)
	go cl.keepPath(s, auth, firstPath)
	for i := 1; i < cl.cfg.Paths; i++ {
		go cl.keepPath(s, auth, nil)
	}

	// A stream that ended cleanly is not an error, even if a path redial then
	// found the session already gone on the server.
	if err := pipe(s, local); err != nil {
		if serr := s.Err(); serr != nil {
			return serr
		}
	}
	return nil
}

// dialFirst opens the path that creates the session. Network errors are
// retried with backoff for up to ResumeTimeout; an answer from the server,
// such as a rejected token, is final.
func (cl *Client) dialFirst(ctx context.Context) (*websocket.Conn, *http.Response, error) {
	deadline := time.Now().Add(cl.cfg.ResumeTimeout)
	backoff := 250 * time.Millisecond
	for {
		c, resp, err := cl.dial(ctx, nil)
		if err == nil || resp != nil || time.Now().After(deadline) {
			return c, resp, err
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// keepPath keeps one path slot filled for the lifetime of the session,
// redialing with backoff whenever its path dies.
func (cl *Client) keepPath(s *session.Session, auth *sessionAuth, p *wspath.Path) {
	backoff := 250 * time.Millisecond
	for {
		if p != nil {
			select {
			case <-p.Done():
			case <-s.Done():
				p.Close()
				return
			}
		}

		select {
		case <-s.Done():
			return
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		c, _, err := cl.dial(ctx, auth)
		cancel()
		if errors.Is(err, errSessionGone) {
			s.Abort(err)
			return
		}
		if err != nil {
			p = nil
			select {
			case <-time.After(backoff):
			case <-s.Done():
				return
			}
			backoff = min(backoff*2, 5*time.Second)
			continue
		}
		backoff = 250 * time.Millisecond
		p = wspath.New(c, cl.cfg.Path)
		if err := s.AddPath(p); err != nil {
			return
		}
		cl.cfg.Logger.Printf("session %s: path connected, %d open", auth.id[:8], s.NumPaths())
	}
}
