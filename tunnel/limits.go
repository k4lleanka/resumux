package tunnel

import (
	"errors"
	"net/netip"
	"sync"
	"time"
)

// clientLimits bounds how many sessions one client may hold and open. A
// shared token handed out with a public client stops nobody, so without
// this one address could take every session the server allows.
//
// Only opening a session is limited. Resuming one never is, since a client
// that moves to another network arrives from a new address, and refusing it
// there would cut off a session that is already running.
type clientLimits struct {
	maxSessions int // per client at once; 0 means no limit
	maxPerMin   int // new sessions per client per minute; 0 means no limit
	trusted     []netip.Prefix

	mu        sync.Mutex
	clients   map[netip.Prefix]*clientState
	lastPrune time.Time
}

type clientState struct {
	sessions    int
	windowStart time.Time
	opened      int  // sessions opened since windowStart
	refusalSeen bool // a refusal was already reported in this window
}

var (
	errClientSessions = errors.New("too many sessions from this address")
	errClientRate     = errors.New("too many new sessions from this address")
)

const (
	// limitWindow is the period maxPerMin counts over.
	limitWindow = time.Minute
	// maxTrackedClients bounds the memory spent on counting. Past it, new
	// addresses go uncounted rather than refused: a flood of addresses must
	// not lock out the clients it crowds out of the table.
	maxTrackedClients = 65536
)

func newClientLimits(cfg ServerConfig) *clientLimits {
	return &clientLimits{
		maxSessions: cfg.MaxSessionsPerClient,
		maxPerMin:   cfg.MaxNewSessionsPerClient,
		trusted:     cfg.TrustedProxies,
		clients:     make(map[netip.Prefix]*clientState),
	}
}

// clientKey returns what one client is counted as, and false for addresses
// that do not identify a client: loopback, link-local, an unknown address,
// or a trusted proxy whose client could not be told. Counting those would
// lump every client behind them together and lock them all out at once.
// IPv6 is counted per /64, the block one subscriber usually gets.
func (l *clientLimits) clientKey(a netip.Addr) (netip.Prefix, bool) {
	if !a.IsValid() || a.IsLoopback() || a.IsUnspecified() || a.IsLinkLocalUnicast() || trusted(a, l.trusted) {
		return netip.Prefix{}, false
	}
	if a.Is4() {
		return netip.PrefixFrom(a, 32), true
	}
	p, _ := a.Prefix(64)
	return p, true
}

// clientSlot is one counted session. Exactly one of its methods takes
// effect, once; later calls do nothing.
type clientSlot struct {
	l      *clientLimits
	st     *clientState
	window time.Time // the window the session was counted in
	once   sync.Once
}

// release gives the slot back when a session that started has ended.
func (s *clientSlot) release() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.l.mu.Lock()
		s.st.sessions--
		s.l.mu.Unlock()
	})
}

// cancel gives the slot back for a session that never started, and refunds
// it from the per-minute count too: clients retrying while the target is
// down must not use up their allowance on attempts that failed.
func (s *clientSlot) cancel() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.l.mu.Lock()
		s.st.sessions--
		if s.st.windowStart.Equal(s.window) {
			s.st.opened--
		}
		s.l.mu.Unlock()
	})
}

// acquire counts a new session for client, or says why it may not open, and
// whether this is the first refusal for client in this window, so callers
// can log once rather than for every attempt. A nil slot with a nil error
// means the client is not counted.
func (l *clientLimits) acquire(client netip.Addr, now time.Time) (slot *clientSlot, report bool, err error) {
	key, ok := l.clientKey(client)
	if !ok || (l.maxSessions <= 0 && l.maxPerMin <= 0) {
		return nil, false, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pruneLocked(now)
	st := l.clients[key]
	if st == nil {
		if len(l.clients) >= maxTrackedClients {
			return nil, false, nil
		}
		st = &clientState{windowStart: now}
		l.clients[key] = st
	}
	if now.Sub(st.windowStart) >= limitWindow {
		st.windowStart, st.opened, st.refusalSeen = now, 0, false
	}
	refuse := func(e error) (*clientSlot, bool, error) {
		first := !st.refusalSeen
		st.refusalSeen = true
		return nil, first, e
	}
	if l.maxSessions > 0 && st.sessions >= l.maxSessions {
		return refuse(errClientSessions)
	}
	if l.maxPerMin > 0 && st.opened >= l.maxPerMin {
		return refuse(errClientRate)
	}
	st.sessions++
	st.opened++
	return &clientSlot{l: l, st: st, window: st.windowStart}, false, nil
}

// pruneLocked forgets clients with no sessions whose window has passed, so
// addresses that came and went do not pile up. It runs once per window, or
// once a second while the table is full.
func (l *clientLimits) pruneLocked(now time.Time) {
	since := now.Sub(l.lastPrune)
	if since < limitWindow && (len(l.clients) < maxTrackedClients || since < time.Second) {
		return
	}
	l.lastPrune = now
	for k, st := range l.clients {
		if st.sessions == 0 && now.Sub(st.windowStart) >= limitWindow {
			delete(l.clients, k)
		}
	}
}

// tracked reports how many clients are being counted, for tests.
func (l *clientLimits) tracked() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.clients)
}
