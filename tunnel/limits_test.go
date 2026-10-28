package tunnel

import (
	"errors"
	"net"
	"net/http"
	"net/netip"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func mustAcquire(t *testing.T, l *clientLimits, a string, now time.Time) *clientSlot {
	t.Helper()
	s, _, err := l.acquire(netip.MustParseAddr(a), now)
	if err != nil {
		t.Fatalf("%s at %s: %v", a, now.Sub(t0), err)
	}
	return s
}

func refused(l *clientLimits, a string, now time.Time) error {
	_, _, err := l.acquire(netip.MustParseAddr(a), now)
	return err
}

func TestClientLimitsSessions(t *testing.T) {
	l := newClientLimits(ServerConfig{MaxSessionsPerClient: 2})
	s1 := mustAcquire(t, l, "203.0.113.9", t0)
	mustAcquire(t, l, "203.0.113.9", t0)
	if err := refused(l, "203.0.113.9", t0); !errors.Is(err, errClientSessions) {
		t.Fatalf("third session: %v, want refused", err)
	}
	mustAcquire(t, l, "203.0.113.10", t0)
	// Releasing twice frees one session, not two.
	s1.release()
	s1.release()
	s1.cancel()
	mustAcquire(t, l, "203.0.113.9", t0)
	if err := refused(l, "203.0.113.9", t0); !errors.Is(err, errClientSessions) {
		t.Fatalf("a repeated release freed more than one session: %v", err)
	}
}

func TestClientLimitsRate(t *testing.T) {
	l := newClientLimits(ServerConfig{MaxNewSessionsPerClient: 3})
	for i := range 3 {
		mustAcquire(t, l, "203.0.113.9", t0.Add(time.Duration(i)*time.Second)).release()
	}
	if err := refused(l, "203.0.113.9", t0.Add(10*time.Second)); !errors.Is(err, errClientRate) {
		t.Fatalf("fourth in a minute: %v, want refused", err)
	}
	mustAcquire(t, l, "203.0.113.9", t0.Add(limitWindow))
}

// TestFailedAttemptsCostNothing covers clients retrying while the target is
// down: attempts that never started a session must not use up the
// allowance, or everyone behind one address is locked out when it is back.
func TestFailedAttemptsCostNothing(t *testing.T) {
	l := newClientLimits(ServerConfig{MaxNewSessionsPerClient: 3})
	for i := range 50 {
		mustAcquire(t, l, "203.0.113.9", t0.Add(time.Duration(i)*time.Second/10)).cancel()
	}
	for range 3 {
		mustAcquire(t, l, "203.0.113.9", t0.Add(6*time.Second))
	}
}

// TestCancelInLaterWindowRefundsNothing checks that an attempt counted in one
// window cannot refund another window's count.
func TestCancelInLaterWindowRefundsNothing(t *testing.T) {
	l := newClientLimits(ServerConfig{MaxNewSessionsPerClient: 1})
	old := mustAcquire(t, l, "203.0.113.9", t0)
	mustAcquire(t, l, "203.0.113.9", t0.Add(limitWindow)) // counted in a new window
	// The first attempt fails late. It was counted in the old window, so it
	// must not refund the new one.
	old.cancel()
	if err := refused(l, "203.0.113.9", t0.Add(limitWindow+time.Second)); !errors.Is(err, errClientRate) {
		t.Fatalf("a stale cancel refunded the new window: %v", err)
	}
}

func TestClientLimitsReportOncePerWindow(t *testing.T) {
	l := newClientLimits(ServerConfig{MaxSessionsPerClient: 1})
	mustAcquire(t, l, "203.0.113.9", t0)
	reports := 0
	for range 100 {
		if _, report, _ := l.acquire(netip.MustParseAddr("203.0.113.9"), t0); report {
			reports++
		}
	}
	if reports != 1 {
		t.Fatalf("reported %d refusals in one window, want 1", reports)
	}
}

// TestClientLimitsSkipSharedAddresses checks that addresses standing for
// many clients are never counted: limiting them would lock out everyone
// behind them at once.
func TestClientLimitsSkipSharedAddresses(t *testing.T) {
	l := newClientLimits(ServerConfig{
		MaxSessionsPerClient: 1, MaxNewSessionsPerClient: 1,
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")},
	})
	for _, a := range []string{"127.0.0.1", "::1", "10.1.2.3", "fe80::1", "169.254.1.1"} {
		for range 10 {
			mustAcquire(t, l, a, t0)
		}
	}
	for range 10 {
		if _, _, err := l.acquire(netip.Addr{}, t0); err != nil {
			t.Fatalf("unknown address was limited: %v", err)
		}
	}
	if n := l.tracked(); n != 0 {
		t.Fatalf("tracked %d shared addresses", n)
	}
}

func TestClientLimitsIPv6Per64(t *testing.T) {
	l := newClientLimits(ServerConfig{MaxSessionsPerClient: 1})
	mustAcquire(t, l, "2001:db8::1", t0)
	if err := refused(l, "2001:db8::2", t0); !errors.Is(err, errClientSessions) {
		t.Fatalf("another address in the same /64: %v, want refused", err)
	}
	mustAcquire(t, l, "2001:db8:0:1::1", t0)
}

func TestClientLimitsForgetIdleClients(t *testing.T) {
	l := newClientLimits(ServerConfig{MaxNewSessionsPerClient: 5})
	for i := range 100 {
		mustAcquire(t, l, netip.AddrFrom4([4]byte{198, 51, 100, byte(i)}).String(), t0).release()
	}
	mustAcquire(t, l, "203.0.113.9", t0)
	mustAcquire(t, l, "203.0.113.10", t0.Add(2*limitWindow))
	// Only the client still holding a session and the newest one remain.
	if n := l.tracked(); n != 2 {
		t.Fatalf("tracking %d clients after they left, want 2", n)
	}
}

// TestFullTableFailsOpen checks that once the table is full, new addresses
// are let through uncounted rather than refused.
func TestFullTableFailsOpen(t *testing.T) {
	l := newClientLimits(ServerConfig{MaxSessionsPerClient: 1})
	for i := range maxTrackedClients {
		mustAcquire(t, l, netip.AddrFrom4([4]byte{10, byte(i >> 16), byte(i >> 8), byte(i)}).String(), t0)
	}
	for range 5 {
		if s := mustAcquire(t, l, "203.0.113.9", t0); s != nil {
			t.Fatal("counted a client past the table limit")
		}
	}
}

func TestClientLimitsOff(t *testing.T) {
	l := newClientLimits(ServerConfig{})
	for range 100 {
		mustAcquire(t, l, "203.0.113.9", t0)
	}
	if n := l.tracked(); n != 0 {
		t.Fatalf("tracked %d clients with limits off", n)
	}
}

// TestClientLimitNeverCutsARunningSession limits a client to one session,
// then cuts all its paths. Resuming must work, a second session must be
// refused, and the slot must come back once the first session ends.
func TestClientLimitNeverCutsARunningSession(t *testing.T) {
	h := startServer(t, ServerConfig{
		MaxSessionsPerClient: 1,
		TrustedProxies:       []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")},
		ClientIPHeader:       "CF-Connecting-IP",
	})
	cfg := ClientConfig{ServerURLs: []string{h.url}, Header: http.Header{"Cf-Connecting-Ip": {"203.0.113.9"}}}
	addr := startClient(t, cfg)

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(20 * time.Second))
	echoOn(t, c, "before")
	h.listener.cutAll()
	echoOn(t, c, "after the cut")

	second, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	second.SetDeadline(time.Now().Add(10 * time.Second))
	second.Write([]byte("x"))
	if _, err := second.Read(make([]byte, 1)); err == nil {
		t.Fatal("a second session from the same client was allowed")
	}
	second.Close()
	echoOn(t, c, "still the first")

	c.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		third, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		third.SetDeadline(time.Now().Add(5 * time.Second))
		third.Write([]byte("y"))
		_, err = third.Read(make([]byte, 1))
		third.Close()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the slot never came back after the session ended")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestClientLimitIgnoresLoopback checks that with limits on but no way to
// tell clients apart, as behind a local connector without a client header,
// clients are not lumped together and refused.
func TestClientLimitIgnoresLoopback(t *testing.T) {
	h := startServer(t, ServerConfig{MaxSessionsPerClient: 1, MaxNewSessionsPerClient: 1})
	addr := startClient(t, ClientConfig{ServerURLs: []string{h.url}})
	for i := range 3 {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(10 * time.Second))
		echoOn(t, c, "hello")
		_ = i
	}
}
