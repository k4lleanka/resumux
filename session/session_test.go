package session

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errPathClosed = errors.New("path closed")

// memPath is one end of an in-memory path. Closing either end kills both, and
// frames still in flight are lost, like a dropped network connection.
type memPath struct {
	in   chan []byte
	out  chan []byte
	once *sync.Once
	dead chan struct{}
}

func memPair() (*memPath, *memPath) {
	a2b := make(chan []byte, 64)
	b2a := make(chan []byte, 64)
	once := new(sync.Once)
	dead := make(chan struct{})
	return &memPath{in: b2a, out: a2b, once: once, dead: dead},
		&memPath{in: a2b, out: b2a, once: once, dead: dead}
}

func (p *memPath) ReadFrame() ([]byte, error) {
	select {
	case b := <-p.in:
		return b, nil
	case <-p.dead:
		return nil, errPathClosed
	}
}

func (p *memPath) WriteFrame(b []byte) error {
	select {
	case <-p.dead:
		return errPathClosed
	default:
	}
	select {
	case p.out <- append([]byte(nil), b...):
		return nil
	case <-p.dead:
		return errPathClosed
	}
}

func (p *memPath) Close() error {
	p.once.Do(func() { close(p.dead) })
	return nil
}

func connect(t *testing.T, a, b *Session) *memPath {
	t.Helper()
	pa, pb := memPair()
	if err := a.AddPath(pa); err != nil {
		t.Fatal(err)
	}
	if err := b.AddPath(pb); err != nil {
		t.Fatal(err)
	}
	return pa
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// pump writes data in random-sized pieces and then closes the write side.
func pump(s *Session, data []byte, errc chan<- error) {
	for len(data) > 0 {
		n := 1 + mrand.IntN(64<<10)
		if n > len(data) {
			n = len(data)
		}
		if _, err := s.Write(data[:n]); err != nil {
			errc <- err
			return
		}
		data = data[n:]
	}
	errc <- s.CloseWrite()
}

func readAll(s *Session, out *[]byte, errc chan<- error) {
	b, err := io.ReadAll(s)
	*out = b
	errc <- err
}

func TestTransferWithoutFailures(t *testing.T) {
	a, b := New(Config{}), New(Config{})
	defer a.Abort(nil)
	defer b.Abort(nil)
	connect(t, a, b)

	want := randomBytes(t, 3<<20)
	errc := make(chan error, 2)
	var got []byte
	go pump(a, want, errc)
	go readAll(b, &got, errc)
	for range 2 {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %d bytes, want %d, or contents differ", len(got), len(want))
	}
}

// TestPathsDieDuringTransfer kills the active path over and over in both
// directions while data flows, attaching a fresh one each time. Every byte
// must arrive once and in order.
func TestPathsDieDuringTransfer(t *testing.T) {
	a := New(Config{MaxFrameSize: 4 << 10, SendWindow: 256 << 10})
	b := New(Config{MaxFrameSize: 4 << 10, SendWindow: 256 << 10})
	defer a.Abort(nil)
	defer b.Abort(nil)

	var mu sync.Mutex
	current := connect(t, a, b)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(2+mrand.IntN(15)) * time.Millisecond):
			}
			mu.Lock()
			current.Close()
			pa, pb := memPair()
			a.AddPath(pa)
			b.AddPath(pb)
			current = pa
			mu.Unlock()
		}
	}()

	aToB := randomBytes(t, 4<<20)
	bToA := randomBytes(t, 4<<20)
	errc := make(chan error, 4)
	var gotB, gotA []byte
	go pump(a, aToB, errc)
	go pump(b, bToA, errc)
	go readAll(b, &gotB, errc)
	go readAll(a, &gotA, errc)

	timeout := time.After(20 * time.Second)
	for range 4 {
		select {
		case err := <-errc:
			if err != nil {
				t.Fatal(err)
			}
		case <-timeout:
			t.Fatalf("transfer did not finish\n%s\n%s", a.debugState("a"), b.debugState("b"))
		}
	}
	close(stop)
	wg.Wait()

	if !bytes.Equal(gotB, aToB) {
		t.Errorf("a->b: got %d bytes, want %d, or contents differ", len(gotB), len(aToB))
	}
	if !bytes.Equal(gotA, bToA) {
		t.Errorf("b->a: got %d bytes, want %d, or contents differ", len(gotA), len(bToA))
	}
}

// TestNoPathForAWhile writes while no path is attached; the data must be sent
// once a path appears.
func TestNoPathForAWhile(t *testing.T) {
	a, b := New(Config{}), New(Config{})
	defer a.Abort(nil)
	defer b.Abort(nil)

	want := randomBytes(t, 100<<10)
	if _, err := a.Write(want); err != nil {
		t.Fatal(err)
	}
	a.CloseWrite()

	connect(t, a, b)
	got, err := io.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %d bytes, want %d", len(got), len(want))
	}
}

// TestSendWindowBlocksWriter checks that an unread receiver stops the sender
// instead of letting memory grow without bound.
func TestSendWindowBlocksWriter(t *testing.T) {
	a := New(Config{SendWindow: 64 << 10, MaxFrameSize: 16 << 10})
	b := New(Config{})
	defer a.Abort(nil)
	defer b.Abort(nil)
	connect(t, a, b)

	wrote := make(chan int, 1)
	go func() {
		n, _ := a.Write(make([]byte, 1<<20))
		wrote <- n
	}()

	select {
	case <-wrote:
		t.Fatal("write of 1 MiB finished although nothing was read")
	case <-time.After(200 * time.Millisecond):
	}

	go io.Copy(io.Discard, b)
	select {
	case n := <-wrote:
		if n != 1<<20 {
			t.Fatalf("wrote %d bytes", n)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("write did not finish after the receiver started reading")
	}
}

func TestDecodeFrameRejectsGarbage(t *testing.T) {
	for _, b := range [][]byte{
		nil,
		{frameData},
		append([]byte{0x7f}, make([]byte, 8)...),
		append([]byte{frameAck}, make([]byte, 9)...),
	} {
		if _, err := decodeFrame(b); err == nil {
			t.Errorf("decodeFrame(%x) succeeded", b)
		}
	}
}

func (s *Session) debugState(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	first := uint64(0)
	if len(s.unacked) > 0 {
		first = s.unacked[0].seq
	}
	minPending := ^uint64(0)
	for k := range s.pending {
		if k < minPending {
			minPending = k
		}
	}
	return fmt.Sprintf("%s: nextSeq=%d unacked=%d (first %d, %d bytes) writeClosed=%v | expected=%d pending=%d (min %d) readQ=%d consumed=%d | paths=%d active=%v",
		name, s.nextSeq, len(s.unacked), first, s.unackedBytes, s.writeClosed,
		s.expected, len(s.pending), minPending, len(s.readQ), s.consumed, len(s.paths), s.active != nil)
}

// hostilePeer attaches one end of a path to s and returns the other end for
// the test to write raw frames into.
func hostilePeer(t *testing.T, s *Session) *memPath {
	t.Helper()
	ours, theirs := memPair()
	if err := s.AddPath(ours); err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			if _, err := theirs.ReadFrame(); err != nil {
				return
			}
		}
	}()
	return theirs
}

func waitAborted(t *testing.T, s *Session, want error) {
	t.Helper()
	select {
	case <-s.Done():
		if !errors.Is(s.Err(), want) {
			t.Fatalf("session closed with %v, want %v", s.Err(), want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session was not aborted")
	}
}

// A peer that ignores flow control and keeps sending must not make the
// receiver buffer without limit.
func TestPeerIgnoringWindowIsAborted(t *testing.T) {
	s := New(Config{RecvWindow: 1 << 20})
	defer s.Abort(nil)
	peer := hostilePeer(t, s)

	payload := make([]byte, 32<<10)
	for seq := uint64(0); seq < 1000; seq++ {
		if err := peer.WriteFrame(encodeFrame(frameData, seq, payload)); err != nil {
			break
		}
	}
	waitAborted(t, s, ErrPeerOverrun)
}

// Many tiny frames are counted with their overhead, so they cannot slip
// under a window meant for bytes.
func TestTinyFramesCountAgainstWindow(t *testing.T) {
	s := New(Config{RecvWindow: 64 << 10})
	defer s.Abort(nil)
	peer := hostilePeer(t, s)

	for seq := uint64(0); seq < 100000; seq++ {
		if err := peer.WriteFrame(encodeFrame(frameData, seq, []byte{1})); err != nil {
			break
		}
	}
	waitAborted(t, s, ErrPeerOverrun)
}

// A frame far beyond anything a well-behaved sender could have in flight is
// rejected instead of being parked in the reorder buffer.
func TestFrameFarAheadIsAborted(t *testing.T) {
	s := New(Config{})
	defer s.Abort(nil)
	peer := hostilePeer(t, s)

	peer.WriteFrame(encodeFrame(frameData, 1<<40, []byte("x")))
	waitAborted(t, s, ErrPeerOverrun)
}

func FuzzDecodeFrame(f *testing.F) {
	f.Add(encodeFrame(frameData, 7, []byte("hello")))
	f.Add(encodeFrame(frameFin, 1, nil))
	f.Add(encodeFrame(frameAck, 3, nil))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		fr, err := decodeFrame(b)
		if err != nil {
			return
		}
		if got := encodeFrame(fr.kind, fr.seq, fr.payload); !bytes.Equal(got, b) {
			t.Fatalf("decode then encode changed the frame: %x -> %x", b, got)
		}
	})
}

// FuzzSessionInput feeds arbitrary frames to a session. Whatever arrives, it
// must not panic or deadlock; misbehaviour may only close the session.
func FuzzSessionInput(f *testing.F) {
	f.Add(encodeFrame(frameData, 0, []byte("a")), encodeFrame(frameAck, 5, nil))
	f.Add(encodeFrame(frameFin, 0, nil), []byte{0xff})
	f.Fuzz(func(t *testing.T, a, b []byte) {
		s := New(Config{RecvWindow: 1 << 16})
		defer s.Abort(nil)
		ours, theirs := memPair()
		s.AddPath(ours)
		go func() {
			for {
				if _, err := theirs.ReadFrame(); err != nil {
					return
				}
			}
		}()
		theirs.WriteFrame(a)
		theirs.WriteFrame(b)
		theirs.Close()

		done := make(chan struct{})
		go func() {
			buf := make([]byte, 1024)
			s.mu.Lock()
			n := len(s.readQ)
			s.mu.Unlock()
			for range n {
				s.Read(buf)
			}
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("session stuck")
		}
	})
}

// slowPair is a memPair whose frames take delay to arrive, and which counts
// the bytes written into it.
func slowPair(delay time.Duration, wire *atomic.Int64) (*memPath, *memPath) {
	once := new(sync.Once)
	dead := make(chan struct{})
	link := func() (chan []byte, chan []byte) {
		w, r := make(chan []byte, 1024), make(chan []byte, 1024)
		go func() {
			for b := range w {
				wire.Add(int64(len(b)))
				time.Sleep(delay)
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

// TestSidesPickingDifferentPathsConverge kills the active path when the two
// sides list the remaining ones in a different order, so each fails over to a
// different path. The follower must move to the leader's choice once, not
// chase it.
func TestSidesPickingDifferentPathsConverge(t *testing.T) {
	leader := New(Config{})
	follower := New(Config{FollowPeer: true})
	defer leader.Abort(nil)
	defer follower.Abort(nil)

	var wire atomic.Int64
	a1, a2 := slowPair(time.Millisecond, &wire)
	b1, b2 := slowPair(time.Millisecond, &wire)
	c1, c2 := slowPair(time.Millisecond, &wire)
	leader.AddPath(a1)
	leader.AddPath(b1)
	leader.AddPath(c1)
	follower.AddPath(a2)
	follower.AddPath(c2)
	follower.AddPath(b2)

	const total = 16 << 20
	errc := make(chan error, 1)
	go pump(leader, make([]byte, total), errc)
	var got []byte
	readc := make(chan error, 1)
	go readAll(follower, &got, readc)

	time.Sleep(50 * time.Millisecond)
	a1.Close()

	select {
	case err := <-readc:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("transfer did not finish, %d MiB on the wire\n%s\n%s",
			wire.Load()>>20, leader.debugState("leader"), follower.debugState("follower"))
	}
	if len(got) != total {
		t.Fatalf("got %d bytes, want %d", len(got), total)
	}
	if w := wire.Load(); w > 2*total {
		t.Errorf("%d MiB on the wire for %d MiB of data", w>>20, total>>20)
	}
}
