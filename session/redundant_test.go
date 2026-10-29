package session

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// holdPair is a memPair that delivers nothing, in either direction, while
// hold is set, and then everything it kept back in order: a TCP connection
// waiting out a retransmission timeout.
func holdPair(hold *atomic.Bool, wire *atomic.Int64) (*memPath, *memPath) {
	once := new(sync.Once)
	dead := make(chan struct{})
	link := func() (chan []byte, chan []byte) {
		w, r := make(chan []byte, 1024), make(chan []byte, 1024)
		go func() {
			for b := range w {
				wire.Add(int64(len(b)))
				for hold.Load() {
					select {
					case <-dead:
						return
					case <-time.After(time.Millisecond):
					}
				}
				select {
				case r <- b:
				case <-dead:
					return
				}
			}
		}()
		return w, r
	}
	aw, br := link()
	bw, ar := link()
	return &memPath{in: ar, out: aw, once: once, dead: dead},
		&memPath{in: br, out: bw, once: once, dead: dead}
}

// worstEcho sends messages from the leader, has the follower echo them,
// and holds up the path both sides send on for a while in the middle. It
// returns the slowest round trip.
func worstEcho(t *testing.T, redundant bool, size, count int) time.Duration {
	t.Helper()
	leader := New(Config{Redundant: redundant})
	follower := New(Config{FollowPeer: true, Redundant: redundant})
	defer leader.Abort(nil)
	defer follower.Abort(nil)

	var hold1, hold2 atomic.Bool
	var wire atomic.Int64
	a1, b1 := holdPair(&hold1, &wire)
	a2, b2 := holdPair(&hold2, &wire)
	leader.AddPath(a1)
	leader.AddPath(a2)
	follower.AddPath(b1)
	follower.AddPath(b2)

	go func() {
		msg := make([]byte, size)
		for {
			if _, err := io.ReadFull(follower, msg); err != nil {
				return
			}
			if _, err := follower.Write(msg); err != nil {
				return
			}
		}
	}()

	var worst time.Duration
	msg, back := make([]byte, size), make([]byte, size)
	for i := range count {
		if i == count/3 {
			hold1.Store(true)
			time.AfterFunc(300*time.Millisecond, func() { hold1.Store(false) })
		}
		copy(msg, fmt.Sprintf("message %d", i))
		start := time.Now()
		if _, err := leader.Write(msg); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(leader, back); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(back, msg) {
			t.Fatalf("message %d came back as %q", i, back)
		}
		worst = max(worst, time.Since(start))
		time.Sleep(2 * time.Millisecond)
	}
	return worst
}

// TestRedundantHidesStalledPath holds up the path both sides send on. A plain
// session waits for it; a redundant one carries on over the other path.
func TestRedundantHidesStalledPath(t *testing.T) {
	if d := worstEcho(t, false, 32, 300); d < 250*time.Millisecond {
		t.Fatalf("plain session: worst round trip %s, want the stall to show", d)
	}
	if d := worstEcho(t, true, 32, 300); d > 150*time.Millisecond {
		t.Errorf("redundant session: worst round trip %s, want the stall hidden", d)
	}
}

// TestRedundantHidesStalledPathAtHighRate does the same with enough traffic to
// pass copyBudget within the stall, unless acknowledgements are copied too.
func TestRedundantHidesStalledPathAtHighRate(t *testing.T) {
	if d := worstEcho(t, true, 4<<10, 300); d > 150*time.Millisecond {
		t.Errorf("worst round trip %s, want the stall hidden", d)
	}
}

// TestRedundantBulkIsSentOnce sends far more than copyBudget, each way. Only
// the start may be copied.
func TestRedundantBulkIsSentOnce(t *testing.T) {
	leader := New(Config{Redundant: true})
	follower := New(Config{FollowPeer: true, Redundant: true})
	defer leader.Abort(nil)
	defer follower.Abort(nil)

	var wire atomic.Int64
	a1, b1 := slowPair(time.Millisecond, &wire)
	a2, b2 := slowPair(time.Millisecond, &wire)
	leader.AddPath(a1)
	leader.AddPath(a2)
	follower.AddPath(b1)
	follower.AddPath(b2)

	const total = 16 << 20
	for _, tc := range []struct {
		name     string
		from, to *Session
	}{{"leader to follower", leader, follower}, {"follower to leader", follower, leader}} {
		wire.Store(0)
		want := randomBytes(t, total)
		errc := make(chan error, 1)
		go func() {
			_, err := tc.from.Write(want)
			errc <- err
		}()
		got := make([]byte, total)
		done := make(chan error, 1)
		go func() {
			_, err := io.ReadFull(tc.to, got)
			done <- err
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("%s: transfer did not finish", tc.name)
		}
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: contents differ", tc.name)
		}
		if w := wire.Load(); w > total+total/8 {
			t.Errorf("%s: %d MiB on the wire for %d MiB of data", tc.name, w>>20, total>>20)
		}
	}
}

// TestRedundantCopiesDoNotMoveFollower has the follower send in bulk while the
// leader sends small, copied messages. A copy that arrives first on the spare
// path must not move the follower there, or every one would make it resend
// its whole window.
func TestRedundantCopiesDoNotMoveFollower(t *testing.T) {
	leader := New(Config{Redundant: true})
	follower := New(Config{FollowPeer: true, Redundant: true})
	defer leader.Abort(nil)
	defer follower.Abort(nil)

	var wire atomic.Int64
	a1, b1 := slowPair(time.Millisecond, &wire)
	a2, b2 := slowPair(0, &wire)
	leader.AddPath(a1)
	leader.AddPath(a2)
	follower.AddPath(b1)
	follower.AddPath(b2)

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			if _, err := leader.Write([]byte("tick")); err != nil {
				return
			}
		}
	}()
	go io.Copy(io.Discard, follower)

	const total = 16 << 20
	errc := make(chan error, 1)
	go func() {
		_, err := follower.Write(make([]byte, total))
		errc <- err
	}()
	got := make([]byte, total)
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(leader, got)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("transfer did not finish, %d MiB on the wire", wire.Load()>>20)
	}
	if w := wire.Load(); w > total+total/4 {
		t.Errorf("%d MiB on the wire for %d MiB of data", w>>20, total>>20)
	}
}

// TestRedundantNewPathGetsUnackedCopies sends while only a stalled path
// exists. A second path attached after that must get copies of what is
// still unacknowledged, not only of what is sent later.
func TestRedundantNewPathGetsUnackedCopies(t *testing.T) {
	leader := New(Config{Redundant: true})
	follower := New(Config{FollowPeer: true, Redundant: true})
	defer leader.Abort(nil)
	defer follower.Abort(nil)

	var hold1, hold2 atomic.Bool
	var wire atomic.Int64
	a1, b1 := holdPair(&hold1, &wire)
	leader.AddPath(a1)
	follower.AddPath(b1)
	hold1.Store(true)
	if _, err := leader.Write([]byte("sent before the spare")); err != nil {
		t.Fatal(err)
	}

	a2, b2 := holdPair(&hold2, &wire)
	follower.AddPath(b2)
	leader.AddPath(a2)
	got := make([]byte, len("sent before the spare"))
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(follower, got)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil || string(got) != "sent before the spare" {
			t.Fatalf("got %q, %v", got, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("frame sent before the spare path stayed behind the stalled one")
	}
}

func TestDecodeCopy(t *testing.T) {
	f, err := decodeFrame(encodeFrame(frameData|copyFlag, 7, []byte("x")))
	if err != nil || !f.copy || f.kind != frameData || f.seq != 7 || string(f.payload) != "x" {
		t.Fatalf("decoded copy as %+v, %v", f, err)
	}
	if f, err := decodeFrame(encodeFrame(frameAck|copyFlag, 7, nil)); err != nil || !f.copy || f.kind != frameAck {
		t.Errorf("decoded copied acknowledgement as %+v, %v", f, err)
	}
}

// ackingPath acknowledges every DATA and FIN frame written to it at once.
type ackingPath struct {
	in   chan []byte
	done chan struct{}
}

func (p *ackingPath) ReadFrame() ([]byte, error) {
	select {
	case b := <-p.in:
		return b, nil
	case <-p.done:
		return nil, ErrClosed
	}
}

func (p *ackingPath) WriteFrame(b []byte) error {
	if k := b[0] &^ copyFlag; k == frameData || k == frameFin {
		select {
		case p.in <- encodeFrame(frameAck, binary.BigEndian.Uint64(b[1:frameHeaderLen])+1, nil):
		default:
		}
	}
	return nil
}

func (p *ackingPath) Close() error         { return nil }
func (p *ackingPath) CloseWrite() error    { return nil }
func (p *ackingPath) Quiet() time.Duration { return 0 }

// stalledPath looks freshly heard from, but each write takes delay, like a
// TCP connection whose peer has stopped reading.
type stalledPath struct {
	delay  time.Duration
	writes atomic.Int64
	done   chan struct{}
}

func (p *stalledPath) ReadFrame() ([]byte, error) { <-p.done; return nil, ErrClosed }

func (p *stalledPath) WriteFrame([]byte) error {
	p.writes.Add(1)
	select {
	case <-time.After(p.delay):
	case <-p.done:
	}
	return nil
}

func (p *stalledPath) Close() error         { return nil }
func (p *stalledPath) CloseWrite() error    { return nil }
func (p *stalledPath) Quiet() time.Duration { return 0 }

// A peer that acknowledges on one path and stalls another must not make
// copies pile up beyond copyBudget, outside the send window.
func TestRedundantCopyQueueIsBoundedInBytes(t *testing.T) {
	done := make(chan struct{})
	defer close(done)
	s := New(Config{Redundant: true, FollowPeer: true})
	defer s.Abort(nil)
	s.AddPath(&ackingPath{in: make(chan []byte, 1024), done: done})
	s.AddPath(&stalledPath{delay: time.Hour, done: done})
	buf := make([]byte, s.cfg.MaxFrameSize)
	for range 500 {
		if _, err := s.Write(buf); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Microsecond)
	}
	s.mu.Lock()
	spare := s.paths[1]
	s.mu.Unlock()
	if n := spare.copyBytes.Load(); n > copyBudget {
		t.Fatalf("%d bytes of copies queued for a stalled path, budget %d", n, copyBudget)
	}
}

// Copies queued for a stalled path are dropped when the session closes,
// instead of each waiting out the stall first.
func TestRedundantCloseDoesNotWaitOutQueuedCopies(t *testing.T) {
	done := make(chan struct{})
	defer close(done)
	s := New(Config{Redundant: true, FollowPeer: true})
	s.AddPath(&ackingPath{in: make(chan []byte, 1024), done: done})
	spare := &stalledPath{delay: time.Second, done: done}
	s.AddPath(spare)
	for range 5 {
		s.Write([]byte("x"))
	}
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	s.Close()
	// One write was already under way when Close began; it may finish.
	if d := time.Since(start); d > 2500*time.Millisecond {
		t.Fatalf("Close took %s", d)
	}
	if n := spare.writes.Load(); n > 2 {
		t.Fatalf("%d copies written on the stalled path, want at most 2", n)
	}
}

// TestRedundantFlappingPaths has every path keep switching between working
// and stalled while data flows both ways, sometimes in a trickle that is
// copied and sometimes in bursts that are not, and now and then replaces
// a path outright. Every byte has to arrive once and in order, and no
// path may hold more copies than the budget.
func TestRedundantFlappingPaths(t *testing.T) {
	leader := New(Config{Redundant: true, MaxFrameSize: 4 << 10, SendWindow: 256 << 10})
	follower := New(Config{Redundant: true, FollowPeer: true, MaxFrameSize: 4 << 10, SendWindow: 256 << 10})
	defer leader.Abort(nil)
	defer follower.Abort(nil)

	var wire atomic.Int64
	holds := make([]*atomic.Bool, 3)
	ends := make([]*memPath, 3)
	for i := range holds {
		holds[i] = new(atomic.Bool)
		a, b := holdPair(holds[i], &wire)
		leader.AddPath(a)
		follower.AddPath(b)
		ends[i] = a
	}

	var flaps atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(1+mrand.IntN(4)) * time.Millisecond):
			}
			i := mrand.IntN(len(holds))
			if mrand.IntN(10) == 0 {
				// Replace the path, stalled or not, as a dead one would be.
				ends[i].Close()
				holds[i] = new(atomic.Bool)
				a, b := holdPair(holds[i], &wire)
				leader.AddPath(a)
				follower.AddPath(b)
				ends[i] = a
				continue
			}
			holds[i].Store(!holds[i].Load())
			flaps.Add(1)
			for _, s := range []*Session{leader, follower} {
				s.mu.Lock()
				for _, ps := range s.paths {
					if n := ps.copyBytes.Load(); n > copyBudget {
						t.Errorf("%d bytes of copies queued, budget %d", n, copyBudget)
					}
				}
				s.mu.Unlock()
			}
		}
	}()

	// Alternate between a trickle, which stays under copyBudget and is
	// copied, and bursts, which pass it.
	send := func(s *Session, data []byte, errc chan<- error) {
		for len(data) > 0 {
			n := 1 + mrand.IntN(512)
			if mrand.IntN(8) == 0 {
				n = 1 + mrand.IntN(64<<10)
			}
			n = min(n, len(data))
			if _, err := s.Write(data[:n]); err != nil {
				errc <- err
				return
			}
			data = data[n:]
			if mrand.IntN(2) == 0 {
				time.Sleep(time.Millisecond)
			}
		}
		errc <- s.CloseWrite()
	}
	there, back := randomBytes(t, 4<<20), randomBytes(t, 4<<20)
	errc := make(chan error, 4)
	var gotThere, gotBack []byte
	go send(leader, there, errc)
	go send(follower, back, errc)
	go readAll(follower, &gotThere, errc)
	go readAll(leader, &gotBack, errc)

	timeout := time.After(60 * time.Second)
	for range 4 {
		select {
		case err := <-errc:
			if err != nil {
				t.Fatal(err)
			}
		case <-timeout:
			t.Fatalf("transfer did not finish\n%s\n%s", leader.debugState("leader"), follower.debugState("follower"))
		}
	}
	close(stop)
	wg.Wait()
	for _, h := range holds {
		h.Store(false)
	}

	if n := flaps.Load(); n < 100 {
		t.Errorf("paths flapped only %d times", n)
	}
	if !bytes.Equal(gotThere, there) {
		t.Errorf("leader->follower: got %d bytes, want %d, or contents differ", len(gotThere), len(there))
	}
	if !bytes.Equal(gotBack, back) {
		t.Errorf("follower->leader: got %d bytes, want %d, or contents differ", len(gotBack), len(back))
	}
}
