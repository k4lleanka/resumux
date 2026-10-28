// Package session turns a set of unreliable, message-oriented paths into one
// reliable, ordered byte stream that survives the loss of any path.
//
// Data is split into numbered frames. The sender keeps every frame until the
// receiver acknowledges it, and the receiver puts frames back in order and
// drops duplicates. A path can therefore die at any moment, or a new one can be
// attached, without the application seeing a gap, a duplicate or a reordering:
// whatever was not acknowledged is simply sent again on the next path.
package session

import (
	"errors"
	"io"
	"sync"
	"time"
)

// Path is one connection that carries frames, such as a WebSocket. Frames must
// arrive whole or not at all; a path is not required to deliver every frame.
// ReadFrame and WriteFrame are each called from one goroutine at a time.
type Path interface {
	ReadFrame() ([]byte, error)
	WriteFrame([]byte) error
	Close() error
}

// Config tunes a Session. Zero values select the defaults.
type Config struct {
	// MaxFrameSize is the largest payload put in one frame.
	MaxFrameSize int
	// SendWindow is how much may be unacknowledged before Write blocks,
	// counting each frame as its payload plus frameCost.
	SendWindow int
	// RecvWindow is how much received data may be buffered before the peer is
	// considered misbehaving and the session is aborted, counted the same
	// way. It must be at least the peer's SendWindow.
	RecvWindow int
	// AckDelay is how long the receiver waits to batch acknowledgements.
	AckDelay time.Duration
	// Linger is how long Close waits for the peer to acknowledge outstanding
	// data before tearing the paths down.
	Linger time.Duration
}

func (c Config) withDefaults() Config {
	if c.MaxFrameSize <= 0 {
		c.MaxFrameSize = 32 << 10
	}
	if c.SendWindow <= 0 {
		c.SendWindow = 4 << 20
	}
	if c.RecvWindow <= 0 {
		c.RecvWindow = 8 << 20
	}
	if c.AckDelay <= 0 {
		c.AckDelay = 10 * time.Millisecond
	}
	if c.Linger <= 0 {
		c.Linger = 5 * time.Second
	}
	return c
}

var (
	// ErrClosed is returned by operations on a closed session.
	ErrClosed = errors.New("session: closed")
	// ErrWriteClosed is returned by Write after CloseWrite.
	ErrWriteClosed = errors.New("session: write side closed")
	// ErrPeerOverrun means the peer sent more than the receive window allows.
	ErrPeerOverrun = errors.New("session: peer exceeded the receive window")
)

// frameCost is added to every frame's payload when windows are counted, so
// that many tiny frames cannot use more memory than the window suggests.
const frameCost = 64

func cost(payload []byte) int { return len(payload) + frameCost }

type sentFrame struct {
	seq     uint64
	kind    byte
	payload []byte
}

type recvItem struct {
	seq  uint64
	data []byte
	fin  bool
}

type pathState struct {
	p    Path
	wmu  sync.Mutex
	dead bool
}

// Session is a reliable byte stream over a changing set of paths.
type Session struct {
	cfg Config

	mu        sync.Mutex
	readCond  *sync.Cond
	writeCond *sync.Cond

	nextSeq      uint64
	unacked      []sentFrame
	unackedBytes int
	writeClosed  bool

	expected uint64
	pending  map[uint64]frame
	buffered int // cost of everything in pending and readQ
	readQ    []recvItem
	readOff  int
	consumed uint64
	ackTimer *time.Timer

	paths  []*pathState
	active *pathState

	closed bool
	err    error
	done   chan struct{}
}

// New returns a session with no paths. Writes are buffered, up to the send
// window, until a path is added.
func New(cfg Config) *Session {
	s := &Session{
		cfg:     cfg.withDefaults(),
		pending: make(map[uint64]frame),
		done:    make(chan struct{}),
	}
	s.readCond = sync.NewCond(&s.mu)
	s.writeCond = sync.NewCond(&s.mu)
	return s
}

// Done is closed when the session is closed.
func (s *Session) Done() <-chan struct{} { return s.done }

// Err reports why the session closed, or nil while it is open.
func (s *Session) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// NumPaths reports how many paths are attached.
func (s *Session) NumPaths() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.paths)
}

// AddPath attaches a path and starts reading from it. The peer is told what
// has been received so far, and if no other path is active, everything still
// unacknowledged is sent again on this one.
func (s *Session) AddPath(p Path) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		p.Close()
		return ErrClosed
	}
	ps := &pathState{p: p}
	s.paths = append(s.paths, ps)
	becameActive := s.active == nil
	if becameActive {
		s.active = ps
	}
	s.mu.Unlock()

	go s.readLoop(ps)
	// Asynchronous so AddPath never waits for the peer to start reading.
	go s.resync(ps, becameActive)
	return nil
}

// Write sends b, blocking while the send window is full.
func (s *Session) Write(b []byte) (int, error) {
	n := 0
	for len(b) > 0 {
		chunk := b
		if len(chunk) > s.cfg.MaxFrameSize {
			chunk = chunk[:s.cfg.MaxFrameSize]
		}

		s.mu.Lock()
		for !s.closed && !s.writeClosed && s.unackedBytes > 0 &&
			s.unackedBytes+cost(chunk) > s.cfg.SendWindow {
			s.writeCond.Wait()
		}
		if s.closed {
			s.mu.Unlock()
			return n, ErrClosed
		}
		if s.writeClosed {
			s.mu.Unlock()
			return n, ErrWriteClosed
		}
		payload := append([]byte(nil), chunk...)
		enc := s.enqueueLocked(frameData, payload)
		ps := s.active
		s.mu.Unlock()

		if ps != nil {
			s.send(ps, enc)
		}
		n += len(chunk)
		b = b[len(chunk):]
	}
	return n, nil
}

// enqueueLocked numbers a frame and keeps it until it is acknowledged.
func (s *Session) enqueueLocked(kind byte, payload []byte) []byte {
	seq := s.nextSeq
	s.nextSeq++
	s.unacked = append(s.unacked, sentFrame{seq: seq, kind: kind, payload: payload})
	s.unackedBytes += cost(payload)
	return encodeFrame(kind, seq, payload)
}

// Read reads data in the order it was written. It returns io.EOF once the peer
// has called CloseWrite and everything before that has been read.
func (s *Session) Read(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.readQ) == 0 && !s.closed {
		s.readCond.Wait()
	}
	if len(s.readQ) == 0 {
		return 0, s.closedErrLocked()
	}
	it := s.readQ[0]
	if it.fin {
		s.markConsumedLocked(it.seq)
		return 0, io.EOF
	}
	n := copy(b, it.data[s.readOff:])
	s.readOff += n
	if s.readOff == len(it.data) {
		s.readQ = s.readQ[1:]
		s.readOff = 0
		s.buffered -= cost(it.data)
		s.markConsumedLocked(it.seq)
	}
	return n, nil
}

func (s *Session) markConsumedLocked(seq uint64) {
	if seq+1 > s.consumed {
		s.consumed = seq + 1
		s.scheduleAckLocked()
	}
}

func (s *Session) closedErrLocked() error {
	if s.err != nil {
		return s.err
	}
	return ErrClosed
}

// CloseWrite tells the peer no more data follows. Reads continue to work.
func (s *Session) CloseWrite() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.writeClosed {
		s.mu.Unlock()
		return nil
	}
	s.writeClosed = true
	enc := s.enqueueLocked(frameFin, nil)
	ps := s.active
	s.writeCond.Broadcast()
	s.mu.Unlock()

	if ps != nil {
		s.send(ps, enc)
	}
	return nil
}

// Close sends any remaining data, waits up to the linger time for the peer to
// acknowledge it, and then closes every path.
func (s *Session) Close() error {
	s.CloseWrite()

	deadline := time.Now().Add(s.cfg.Linger)
	s.mu.Lock()
	for !s.closed && len(s.unacked) > 0 && time.Now().Before(deadline) {
		s.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		s.mu.Lock()
	}
	s.mu.Unlock()

	s.Abort(nil)
	return nil
}

// Abort closes the session immediately. Unacknowledged data is dropped.
func (s *Session) Abort(err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.err = err
	paths := s.paths
	s.paths = nil
	s.active = nil
	if s.ackTimer != nil {
		s.ackTimer.Stop()
		s.ackTimer = nil
	}
	s.readCond.Broadcast()
	s.writeCond.Broadcast()
	close(s.done)
	s.mu.Unlock()

	for _, ps := range paths {
		ps.p.Close()
	}
}

func (s *Session) send(ps *pathState, enc []byte) {
	ps.wmu.Lock()
	err := ps.p.WriteFrame(enc)
	ps.wmu.Unlock()
	if err != nil {
		s.pathFailed(ps)
	}
}

// resync tells the peer how far this side has read, and optionally sends
// everything the peer has not acknowledged. Frames the peer already has are
// dropped there by sequence number, so sending too much is harmless.
func (s *Session) resync(ps *pathState, retransmit bool) {
	s.mu.Lock()
	ack := encodeFrame(frameAck, s.consumed, nil)
	var frames [][]byte
	if retransmit {
		frames = make([][]byte, 0, len(s.unacked))
		for _, f := range s.unacked {
			frames = append(frames, encodeFrame(f.kind, f.seq, f.payload))
		}
	}
	s.mu.Unlock()

	ps.wmu.Lock()
	defer ps.wmu.Unlock()
	if err := ps.p.WriteFrame(ack); err != nil {
		go s.pathFailed(ps)
		return
	}
	for _, f := range frames {
		if err := ps.p.WriteFrame(f); err != nil {
			go s.pathFailed(ps)
			return
		}
	}
}

// pathFailed removes a path. If it was the active one, the next path takes
// over and everything unacknowledged is sent again on it.
func (s *Session) pathFailed(ps *pathState) {
	s.mu.Lock()
	if ps.dead {
		s.mu.Unlock()
		return
	}
	ps.dead = true
	for i, q := range s.paths {
		if q == ps {
			s.paths = append(s.paths[:i:i], s.paths[i+1:]...)
			break
		}
	}
	var next *pathState
	if s.active == ps {
		s.active = nil
		if len(s.paths) > 0 {
			s.active = s.paths[0]
			next = s.active
		}
	}
	s.mu.Unlock()

	ps.p.Close()
	if next != nil {
		go s.resync(next, true)
	}
}

func (s *Session) readLoop(ps *pathState) {
	for {
		b, err := ps.p.ReadFrame()
		if err != nil {
			s.pathFailed(ps)
			return
		}
		f, err := decodeFrame(b)
		if err != nil {
			s.Abort(err)
			return
		}
		s.handle(ps, f)
	}
}

func (s *Session) handle(ps *pathState, f frame) {
	s.mu.Lock()
	if s.closed || ps.dead {
		s.mu.Unlock()
		return
	}
	// The peer sends on the path it considers best. Following it means a path
	// that only fails in one direction still gets abandoned on both sides.
	switched := false
	if s.active != ps {
		s.active = ps
		switched = true
	}

	var err error
	switch f.kind {
	case frameAck:
		s.ackLocked(f.seq)
	case frameData, frameFin:
		err = s.receiveLocked(f)
	}
	s.mu.Unlock()

	if err != nil {
		s.Abort(err)
		return
	}

	// Never write from the read loop: if both sides did, each could block on a
	// full path while the other is not reading, and neither would recover.
	if switched {
		go s.resync(ps, true)
	}
}

func (s *Session) ackLocked(next uint64) {
	i := 0
	for i < len(s.unacked) && s.unacked[i].seq < next {
		s.unackedBytes -= cost(s.unacked[i].payload)
		i++
	}
	if i > 0 {
		s.unacked = s.unacked[i:]
		s.writeCond.Broadcast()
	}
}

// receiveLocked accepts a DATA or FIN frame. It returns ErrPeerOverrun if
// keeping the frame would exceed the receive window, which a peer that
// respects its send window never causes.
func (s *Session) receiveLocked(f frame) error {
	if f.seq < s.expected {
		// Already have it; the peer missed our acknowledgement.
		s.scheduleAckLocked()
		return nil
	}
	if _, ok := s.pending[f.seq]; ok {
		return nil
	}
	maxAhead := uint64(s.cfg.RecvWindow / frameCost)
	if f.seq-s.expected > maxAhead || s.buffered+cost(f.payload) > s.cfg.RecvWindow {
		return ErrPeerOverrun
	}
	s.buffered += cost(f.payload)

	if f.seq > s.expected {
		f.payload = append([]byte(nil), f.payload...)
		s.pending[f.seq] = f
		return nil
	}

	s.deliverLocked(f)
	for {
		next, ok := s.pending[s.expected]
		if !ok {
			break
		}
		delete(s.pending, s.expected)
		s.deliverLocked(next)
	}
	s.readCond.Broadcast()
	return nil
}

func (s *Session) deliverLocked(f frame) {
	s.expected = f.seq + 1
	if f.kind == frameFin {
		s.readQ = append(s.readQ, recvItem{seq: f.seq, fin: true})
		return
	}
	if len(f.payload) == 0 {
		s.buffered -= cost(nil)
		s.markConsumedLocked(f.seq)
		return
	}
	s.readQ = append(s.readQ, recvItem{seq: f.seq, data: append([]byte(nil), f.payload...)})
}

func (s *Session) scheduleAckLocked() {
	if s.ackTimer != nil || s.closed {
		return
	}
	s.ackTimer = time.AfterFunc(s.cfg.AckDelay, s.flushAck)
}

func (s *Session) flushAck() {
	s.mu.Lock()
	s.ackTimer = nil
	if s.closed || s.active == nil {
		s.mu.Unlock()
		return
	}
	enc := encodeFrame(frameAck, s.consumed, nil)
	ps := s.active
	s.mu.Unlock()
	s.send(ps, enc)
}
