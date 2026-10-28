package tunnel

import (
	"context"
	"crypto/tls"
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
	// ServerURLs are WebSocket URLs that all reach the same server, e.g.
	// wss://example.com/tunnel. Paths take them in turn, so URLs that differ in
	// port or address can spread a session over different routes, such as
	// different CDN edges. At least one is required.
	ServerURLs []string
	// Token is the shared secret the server expects. Required.
	Token string
	// Paths is how many WebSocket paths each session keeps open. More paths
	// make a switch instant when one dies. It is at least 2, and at least the
	// number of ServerURLs.
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
	c.Paths = max(c.Paths, 2, len(c.ServerURLs))
	if c.ResumeTimeout <= 0 {
		c.ResumeTimeout = 2 * time.Minute
	}
	if c.Dialer == nil {
		c.Dialer = websocket.DefaultDialer
	}
	if c.Logger == nil {
		c.Logger = log.Default()
	}
	// A closing session waits for its last data to be acknowledged; that can
	// take as long as a resume, so it must not give up sooner.
	if c.Session.Linger <= 0 {
		c.Session.Linger = c.ResumeTimeout
	}
	c.Session.FollowPeer = false
	// An idle path is heard from once per ping plus a round trip, so it only
	// counts as quiet once it is late by half an interval, and by at least
	// half a second, whatever the ping interval.
	if c.Session.SwitchAfter <= 0 {
		ping := c.Path.PingInterval
		if ping <= 0 {
			ping = time.Second
		}
		c.Session.SwitchAfter = ping + max(ping/2, 500*time.Millisecond)
	}
	return c
}

// Client forwards local TCP connections to a Server.
type Client struct {
	cfg ClientConfig
}

// NewClient validates cfg and returns a Client.
func NewClient(cfg ClientConfig) (*Client, error) {
	if len(cfg.ServerURLs) == 0 {
		return nil, errors.New("tunnel: client server URL is required")
	}
	for _, u := range cfg.ServerURLs {
		if u == "" {
			return nil, errors.New("tunnel: client server URL is empty")
		}
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
				cl.cfg.Logger.Printf("%v", err)
			}
		}()
	}
}

// errSessionGone means the server no longer knows the session, so resuming is
// pointless.
var errSessionGone = errors.New("tunnel: server closed the session")

// errSessionFinished means the server ended the session after everything was
// delivered in both directions.
var errSessionFinished = errors.New("tunnel: session finished")

type sessionAuth struct{ id, secret string }

func (cl *Client) dial(ctx context.Context, url string, auth *sessionAuth) (*websocket.Conn, *http.Response, error) {
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
	c, resp, err := cl.cfg.Dialer.DialContext(ctx, url, h)
	if err != nil && resp != nil {
		switch resp.StatusCode {
		case http.StatusNotFound:
			return nil, resp, errSessionGone
		case http.StatusGone:
			return nil, resp, errSessionFinished
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

	first, resp, firstURL, err := cl.dialFirst(ctx)
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
	// The first path keeps the URL that worked; the others take the rest in
	// turn, so every URL is in use when there are enough paths.
	urls := cl.cfg.ServerURLs
	go cl.keepPath(s, auth, urls[firstURL], firstPath)
	for i := 1; i < cl.cfg.Paths; i++ {
		go cl.keepPath(s, auth, urls[(firstURL+i)%len(urls)], nil)
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

// dialFirst opens the path that creates the session, trying each URL in turn,
// and returns the index of the one that worked. Network errors are retried,
// with backoff after every full round, for up to ResumeTimeout; an answer
// from the server, such as a rejected token, is final.
func (cl *Client) dialFirst(ctx context.Context) (*websocket.Conn, *http.Response, int, error) {
	urls := cl.cfg.ServerURLs
	deadline := time.Now().Add(cl.cfg.ResumeTimeout)
	backoff := 250 * time.Millisecond
	for attempt := 0; ; attempt++ {
		i := attempt % len(urls)
		c, resp, err := cl.dial(ctx, urls[i], nil)
		var certErr *tls.CertificateVerificationError
		if err == nil || resp != nil || errors.As(err, &certErr) || time.Now().After(deadline) {
			return c, resp, i, err
		}
		if i < len(urls)-1 {
			continue
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil, nil, 0, ctx.Err()
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// keepPath keeps one path slot filled for the lifetime of the session,
// redialing with backoff whenever its path dies.
func (cl *Client) keepPath(s *session.Session, auth *sessionAuth, url string, p *wspath.Path) {
	backoff := 250 * time.Millisecond
	failures := 0
	strayWarned := false
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
		c, _, err := cl.dial(ctx, url, auth)
		cancel()
		if errors.Is(err, errSessionFinished) {
			s.Abort(nil)
			return
		}
		if errors.Is(err, errSessionGone) && s.NumPaths() > 0 {
			// Another path is still attached, so the server does know the
			// session: this URL reaches a server that does not. Ending the
			// session over it would drop a healthy connection, so keep
			// retrying it like any other failure. If the other paths are in
			// fact stale, they die within seconds and the next 404 ends it.
			if !strayWarned {
				cl.cfg.Logger.Printf("session %s: %s reaches a server that does not know this session", auth.id[:8], url)
				strayWarned = true
			}
			err = errors.New("tunnel: session unknown at this URL")
		} else if errors.Is(err, errSessionGone) {
			// The server forgets a session once it has finished with it. If
			// nothing is missing on this side either, that is a normal end,
			// not a loss, and there is no peer left to wait for.
			if s.Complete() {
				s.Abort(nil)
			} else {
				s.Abort(err)
			}
			return
		}
		if err != nil {
			// Log the first failure of a streak; the success that ends it is
			// logged below.
			if failures == 0 {
				cl.cfg.Logger.Printf("session %s: reconnecting: %v", auth.id[:8], err)
			}
			failures++
			p = nil
			select {
			case <-time.After(backoff):
			case <-s.Done():
				return
			}
			backoff = min(backoff*2, 5*time.Second)
			continue
		}
		if failures > 0 {
			cl.cfg.Logger.Printf("session %s: reconnected after %d failed attempts", auth.id[:8], failures)
			failures = 0
		}
		backoff = 250 * time.Millisecond
		p = wspath.New(c, cl.cfg.Path)
		if err := s.AddPath(p); err != nil {
			return
		}
		cl.cfg.Logger.Printf("session %s: path connected, %d open", auth.id[:8], s.NumPaths())
	}
}
