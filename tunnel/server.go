package tunnel

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/k4lleanka/resumux/session"
	"github.com/k4lleanka/resumux/wspath"
)

// ServerConfig configures a Server.
type ServerConfig struct {
	// Token is the shared secret clients must present. Required.
	Token string
	// Target is the host:port every session is forwarded to.
	Target string
	// MaxSessions limits concurrent sessions.
	MaxSessions int
	// MaxPathsPerSession limits concurrent paths in one session.
	MaxPathsPerSession int
	// ResumeTimeout is how long a session without any path is kept before it
	// is closed.
	ResumeTimeout time.Duration
	// DialTimeout bounds connecting to Target.
	DialTimeout time.Duration
	// ProxyProtocol makes the server begin every connection to Target with a
	// PROXY protocol version 2 header naming the client's address, for
	// targets that would otherwise see every client as this server.
	ProxyProtocol bool
	// TrustedProxies are the addresses of proxies in front of the server,
	// such as a CDN connector on the same machine. Only requests from them
	// may name the client's address in ClientIPHeader.
	TrustedProxies []netip.Prefix
	// ClientIPHeader is the request header in which trusted proxies give
	// the client's address, e.g. CF-Connecting-IP.
	ClientIPHeader string
	// MaxSessionsPerClient limits how many sessions one client address, or
	// one IPv6 /64, may hold at once. Zero means no limit.
	MaxSessionsPerClient int
	// MaxNewSessionsPerClient limits how many sessions one client may open
	// per minute. Zero means no limit. Neither limit applies to resuming a
	// session, nor to loopback or trusted proxy addresses, which stand for
	// many clients.
	MaxNewSessionsPerClient int

	Path    wspath.Options
	Session session.Config
	Logger  *log.Logger
}

func (c ServerConfig) withDefaults() ServerConfig {
	if c.MaxSessions <= 0 {
		c.MaxSessions = 1024
	}
	if c.MaxPathsPerSession <= 0 {
		c.MaxPathsPerSession = 8
	}
	if c.ResumeTimeout <= 0 {
		c.ResumeTimeout = 2 * time.Minute
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 10 * time.Second
	}
	if c.Logger == nil {
		c.Logger = log.Default()
	}
	// A closing session waits for its last data to be acknowledged; that can
	// take as long as a resume, so it must not give up sooner.
	if c.Session.Linger <= 0 {
		c.Session.Linger = c.ResumeTimeout
	}
	// The client picks the path; the server follows it.
	c.Session.FollowPeer = true
	return c
}

type serverSession struct {
	secret string
	s      *session.Session
	// joining counts paths being upgraded. Guarded by Server.mu.
	joining int
}

// Server accepts WebSocket paths and forwards each session to the target.
type Server struct {
	cfg      ServerConfig
	upgrader websocket.Upgrader

	mu       sync.Mutex
	sessions map[string]*serverSession
	// finished remembers the secrets of sessions that ended with everything
	// delivered, so a client resuming one learns it is done, not lost.
	finished map[string]string
	closed   bool

	limits *clientLimits
}

// ErrSessionExpired is the reason a session closes after ResumeTimeout
// without a path.
var ErrSessionExpired = errors.New("tunnel: no path within resume timeout")

// ErrServerClosed is the reason sessions end when the server is closed.
var ErrServerClosed = errors.New("tunnel: server closed")

// NewServer validates cfg and returns a Server. Serve it with net/http.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Token == "" {
		return nil, errors.New("tunnel: server token is required")
	}
	if cfg.Target == "" {
		return nil, errors.New("tunnel: server target is required")
	}
	cfg = cfg.withDefaults()
	return &Server{
		cfg: cfg,
		upgrader: websocket.Upgrader{
			// Clients are not browsers; the token is what authenticates them.
			CheckOrigin: func(*http.Request) bool { return true },
		},
		sessions: make(map[string]*serverSession),
		finished: make(map[string]string),
		limits:   newClientLimits(cfg),
	}, nil
}

func bearerToken(r *http.Request) string {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, prefix) {
		return ""
	}
	return strings.TrimPrefix(h, prefix)
}

func (sv *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !equalSecret(bearerToken(r), sv.cfg.Token) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.Header.Get(headerVersion) != protocolVersion {
		http.Error(w, "unsupported protocol version", http.StatusBadRequest)
		return
	}
	if id := r.Header.Get(headerSession); id != "" {
		sv.resume(w, r, id)
		return
	}
	sv.create(w, r)
}

func (sv *Server) resume(w http.ResponseWriter, r *http.Request, id string) {
	secret := r.Header.Get(headerSecret)
	sv.mu.Lock()
	ss, ok := sv.sessions[id]
	doneSecret, done := sv.finished[id]
	sv.mu.Unlock()
	if !ok && done && equalSecret(secret, doneSecret) {
		http.Error(w, "session finished", http.StatusGone)
		return
	}
	// Unknown session and wrong secret look the same, in status and in time,
	// so neither reveals whether an id exists.
	known := ""
	if ok {
		known = ss.secret
	}
	if !equalSecret(secret, known) || !ok {
		http.Error(w, "unknown session", http.StatusNotFound)
		return
	}
	// Upgrades still in progress count too, so concurrent ones cannot exceed
	// the limit together.
	sv.mu.Lock()
	full := ss.s.NumPaths()+ss.joining >= sv.cfg.MaxPathsPerSession
	if !full {
		ss.joining++
	}
	sv.mu.Unlock()
	if full {
		http.Error(w, "too many paths", http.StatusTooManyRequests)
		return
	}
	c, err := sv.upgrader.Upgrade(w, r, nil)
	if err == nil {
		err = ss.s.AddPath(wspath.New(c, sv.cfg.Path))
	}
	sv.mu.Lock()
	ss.joining--
	sv.mu.Unlock()
	if err != nil {
		return
	}
	sv.cfg.Logger.Printf("session %s: path from %s, %d open", id[:8], r.RemoteAddr, ss.s.NumPaths())
}

func (sv *Server) create(w http.ResponseWriter, r *http.Request) {
	sv.mu.Lock()
	full := sv.closed || len(sv.sessions) >= sv.cfg.MaxSessions
	sv.mu.Unlock()
	if full {
		http.Error(w, "too many sessions", http.StatusServiceUnavailable)
		return
	}

	client := clientAddr(r, sv.cfg)
	slot, report, err := sv.limits.acquire(client.Addr(), time.Now())
	if err != nil {
		if report {
			sv.cfg.Logger.Printf("refused %s: %v (if that address is a proxy, add it to the trusted proxies)", client.Addr(), err)
		}
		http.Error(w, err.Error(), http.StatusTooManyRequests)
		return
	}
	// Every way out below gives the count back, and refunds the attempt,
	// except the one that starts the session, which hands it to the session.
	started := false
	defer func() {
		if !started {
			slot.cancel()
		}
	}()

	target, err := net.DialTimeout("tcp", sv.cfg.Target, sv.cfg.DialTimeout)
	if err != nil {
		sv.cfg.Logger.Printf("dial target: %v", err)
		http.Error(w, "target unavailable", http.StatusBadGateway)
		return
	}
	if sv.cfg.ProxyProtocol {
		target.SetWriteDeadline(time.Now().Add(sv.cfg.DialTimeout))
		_, err := target.Write(proxyHeader(client, localAddr(r)))
		target.SetWriteDeadline(time.Time{})
		if err != nil {
			target.Close()
			sv.cfg.Logger.Printf("dial target: %v", err)
			http.Error(w, "target unavailable", http.StatusBadGateway)
			return
		}
	}
	id, err1 := randomHex(16)
	secret, err2 := randomHex(32)
	if err1 != nil || err2 != nil {
		target.Close()
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Registered before the client learns the id: it may resume on a second
	// path the moment the handshake response arrives.
	s := session.New(sv.cfg.Session)
	ss := &serverSession{secret: secret, s: s}
	sv.mu.Lock()
	// Checked again: the target dial above can take seconds, and other
	// sessions may have been created meanwhile.
	if sv.closed || len(sv.sessions) >= sv.cfg.MaxSessions {
		sv.mu.Unlock()
		target.Close()
		http.Error(w, "server busy or closing", http.StatusServiceUnavailable)
		return
	}
	sv.sessions[id] = ss
	sv.mu.Unlock()

	h := http.Header{}
	h.Set(headerSession, id)
	h.Set(headerSecret, secret)
	c, err := sv.upgrader.Upgrade(w, r, h)
	if err != nil {
		sv.mu.Lock()
		delete(sv.sessions, id)
		sv.mu.Unlock()
		s.Abort(err)
		target.Close()
		return
	}

	s.AddPath(wspath.New(c, sv.cfg.Path))
	sv.cfg.Logger.Printf("session %s: opened from %s", id[:8], client)
	start := time.Now()
	started = true
	go expireWithoutPaths(s, sv.cfg.ResumeTimeout, ErrSessionExpired)
	go func() {
		pipe(s, target)
		slot.release()
		sv.mu.Lock()
		delete(sv.sessions, id)
		if s.Drained() {
			sv.finished[id] = secret
			time.AfterFunc(sv.cfg.ResumeTimeout, func() {
				sv.mu.Lock()
				delete(sv.finished, id)
				sv.mu.Unlock()
			})
		}
		sv.mu.Unlock()
		reason := "closed"
		if err := s.Err(); err != nil {
			reason = err.Error()
		}
		sv.cfg.Logger.Printf("session %s: %s after %s", id[:8], reason, time.Since(start).Round(time.Second))
	}()
}

// NumSessions reports how many sessions are open.
func (sv *Server) NumSessions() int {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	return len(sv.sessions)
}

// Close aborts every session and refuses new ones.
func (sv *Server) Close() error {
	sv.mu.Lock()
	sv.closed = true
	all := make([]*serverSession, 0, len(sv.sessions))
	for _, ss := range sv.sessions {
		all = append(all, ss)
	}
	sv.mu.Unlock()
	for _, ss := range all {
		ss.s.Abort(ErrServerClosed)
	}
	return nil
}
