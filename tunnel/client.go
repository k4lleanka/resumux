package tunnel

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

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
	// Redundant asks the server to send each small frame on two paths, and
	// does the same, so that a path that loses or holds up a frame does not
	// stall the stream. Frames are copied while at most 64 KiB is
	// unacknowledged, which covers interactive traffic, such as games, but
	// also a moderate stream on a fast link: either may cost twice the
	// bandwidth. Bulk transfers that keep more in flight are sent once.
	Redundant bool
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
	// Set per session, once the server has agreed.
	c.Session.Redundant = false
	// An idle path is heard from about once per ping interval, plus a round
	// trip, so it only counts as quiet once it has missed a whole answer: two
	// intervals, and at least half a second more than one.
	if c.Session.SwitchAfter <= 0 {
		ping := c.Path.PingInterval
		if ping <= 0 {
			ping = time.Second
		}
		c.Session.SwitchAfter = max(2*ping, ping+500*time.Millisecond)
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

// errBadToken means the server refused the token.
var errBadToken = errors.New("tunnel: server rejected the token")

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
	} else if cl.cfg.Redundant {
		h.Set(headerRedundant, "1")
	}
	c, resp, err := cl.cfg.Dialer.DialContext(ctx, url, h)
	if err != nil && resp != nil {
		switch resp.StatusCode {
		case http.StatusNotFound:
			return nil, resp, errSessionGone
		case http.StatusGone:
			return nil, resp, errSessionFinished
		case http.StatusUnauthorized:
			return nil, resp, errBadToken
		}
		// The body says why, e.g. which limit a 429 hit.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
		if msg := strings.TrimSpace(string(body)); msg != "" && utf8.ValidString(msg) {
			return nil, resp, fmt.Errorf("tunnel: handshake: %s: %s", resp.Status, msg)
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

	cfg := cl.cfg.Session
	cfg.Redundant = cl.cfg.Redundant && resp.Header.Get(headerRedundant) == "1"
	s := session.New(cfg)
	firstPath := wspath.New(first, cl.cfg.Path)
	s.AddPath(firstPath)

	go expireWithoutPaths(s, cl.cfg.ResumeTimeout, ErrSessionExpired)
	// The first path keeps the URL that worked; the others take the rest in
	// turn, so every URL is in use when there are enough paths.
	urls := cl.cfg.ServerURLs
	trust := &urlTrust{ok: map[string]bool{urls[firstURL]: true}}
	go cl.keepPath(s, auth, trust, urls[firstURL], firstPath)
	for i := 1; i < cl.cfg.Paths; i++ {
		go cl.keepPath(s, auth, trust, urls[(firstURL+i)%len(urls)], nil)
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
// and returns the index of the one that worked. Any failure moves on to the
// next URL, since one URL can fail for reasons that say nothing about the
// others, such as a CDN error page, a certificate for another name, or a
// server that rejects the token because it is not ours. After a round in
// which some URL failed at the network level, the round is repeated with
// backoff for up to ResumeTimeout; a round in which every URL gave an
// answer ends with that answer.
func (cl *Client) dialFirst(ctx context.Context) (*websocket.Conn, *http.Response, int, error) {
	urls := cl.cfg.ServerURLs
	deadline := time.Now().Add(cl.cfg.ResumeTimeout)
	backoff := 250 * time.Millisecond
	// An answer from a server explains a failure better than a network
	// error does, so it is what gets reported when every URL fails.
	var answerErr, netErr error
	var answerResp *http.Response
	failed := func() (*websocket.Conn, *http.Response, int, error) {
		if answerErr != nil {
			return nil, answerResp, 0, answerErr
		}
		return nil, nil, 0, netErr
	}
	for round := 1; ; round++ {
		retry := false
		for i, u := range urls {
			// Short enough that a URL which never answers does not hold up
			// the others for long, and longer each round for slow links.
			actx, cancel := context.WithTimeout(ctx, min(time.Duration(round)*5*time.Second, 15*time.Second))
			c, resp, err := cl.dial(actx, u, nil)
			cancel()
			if err == nil {
				return c, resp, i, nil
			}
			if ctx.Err() != nil {
				return nil, nil, 0, ctx.Err()
			}
			var certErr *tls.CertificateVerificationError
			if resp != nil || errors.As(err, &certErr) {
				answerErr, answerResp = err, resp
			} else {
				netErr = err
				retry = true
			}
			if time.Now().After(deadline) {
				return failed()
			}
		}
		if !retry {
			return failed()
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
func (cl *Client) keepPath(s *session.Session, auth *sessionAuth, trust *urlTrust, url string, p *wspath.Path) {
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
		// Both directions are finished and acknowledged: the session is only
		// closing its paths, and a new one would just delay that.
		if s.Drained() {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		c, _, err := cl.dial(ctx, url, auth)
		cancel()
		gone := errors.Is(err, errSessionGone) || errors.Is(err, errSessionFinished)
		if gone && !trust.trusted(url) {
			// This URL has never carried a path of this session, so it may
			// reach a server that does not know it, and its 404 or 410
			// proves nothing. Ending the session over it would drop a
			// connection that a trusted URL can still resume, as when every
			// path dies at once, or call a cut stream finished. If the
			// session really is gone, it expires.
			if !strayWarned {
				cl.cfg.Logger.Printf("session %s: %s reaches a server that does not know this session", auth.id[:8], url)
				strayWarned = true
			}
			err = errors.New("tunnel: session unknown at this URL")
		} else if errors.Is(err, errSessionFinished) {
			s.Abort(nil)
			return
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
		trust.mark(url)
		cl.cfg.Logger.Printf("session %s: path connected, %d open", auth.id[:8], s.NumPaths())
	}
}

// urlTrust remembers which URLs have carried a path of one session, and so
// are known to reach the server that holds it.
type urlTrust struct {
	mu sync.Mutex
	ok map[string]bool
}

func (u *urlTrust) mark(url string) {
	u.mu.Lock()
	u.ok[url] = true
	u.mu.Unlock()
}

func (u *urlTrust) trusted(url string) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.ok[url]
}
