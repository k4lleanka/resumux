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
	"math"
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

// A QuietPath also reports how long ago the peer was last heard from on it.
// A leading session uses this to move off a path that has gone silent
// before the path itself gives up.
type QuietPath interface {
	Path
	Quiet() time.Duration
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
	// way. It must exceed the peer's SendWindow by at least 64 bytes, which
	// is what the final FIN counts as.
	RecvWindow int
	// AckDelay is how long the receiver waits to batch acknowledgements.
	AckDelay time.Duration
	// Linger is how long Close waits for the peer to acknowledge outstanding
	// data before tearing the paths down.
	Linger time.Duration
	// FollowPeer makes the session send on whichever path the peer last sent
	// on. Set it on exactly one of the two sides.
	FollowPeer bool
	// SwitchAfter is how long the path a leading session sends on may stay
	// quiet before the session moves to another path that is not. It only
	// applies to QuietPaths, and must exceed their keepalive interval plus a
	// round trip, or idle paths would look quiet.
	SwitchAfter time.Duration
	// Redundant also sends each frame on a second path while little is in
	// flight, so a frame that a path loses or holds up still arrives on time
	// over the other. Set it on both sides.
	Redundant bool
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
	if c.SwitchAfter <= 0 {
		c.SwitchAfter = 2 * time.Second
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

// copyBudget is how much may be unacknowledged for a redundant session to
// still copy a frame. Interactive traffic stays far below it; bulk transfers
// pass it at once and are then sent only once.
const copyBudget = 64 << 10

// copyQueue is how many copies may wait for a path. Copies beyond it are
// dropped, so a stalled path never holds up the writer.
const copyQueue = 64

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
	p      Path
	wmu    sync.Mutex
	dead   bool
	gone   chan struct{} // closed when dead is set
	copies chan []byte
	// draining is set, under wmu, once the path is half-closed; nothing may
	// be written on it after that.
	draining bool
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

	finRead bool // the peer's FIN has been returned by Read

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
	if !s.cfg.FollowPeer {
		go s.watchActive()
	}
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

// AddPath attaches a path and starts reading from it. If no other path is
// active, the peer is told what has been received so far and everything
// still unacknowledged is sent again on this one. A follower also tells the
// peer on every other path it adds.
func (s *Session) AddPath(p Path) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		p.Close()
		return ErrClosed
	}
	ps := &pathState{p: p, gone: make(chan struct{})}
	if s.cfg.Redundant {
		ps.copies = make(chan []byte, copyQueue)
		go s.copyLoop(ps)
	}
	s.paths = append(s.paths, ps)
	becameActive := s.active == nil
	if becameActive {
		s.active = ps
	}
	s.mu.Unlock()

	go s.readLoop(ps)
	// The side that leads sends nothing on a spare path, or the follower would
	// move to it. Asynchronous so AddPath never waits for the peer to read.
	if becameActive || s.cfg.FollowPeer {
		go s.resync(ps, becameActive)
	}
	if !becameActive {
		s.copyUnacked(ps)
	}
	return nil
}

// copyUnacked gives a new spare path copies of what is still unacknowledged,
// so frames sent while it was missing are as safe as those sent after.
func (s *Session) copyUnacked(ps *pathState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.cfg.Redundant || s.unackedBytes > copyBudget {
		return
	}
	for _, f := range s.unacked {
		select {
		case ps.copies <- encodeFrame(f.kind|copyFlag, f.seq, f.payload):
		default:
			return
		}
	}
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
		cp := s.copyPathLocked()
		s.mu.Unlock()

		// The copy goes first: the original may block on a stalled path.
		s.sendCopy(cp, enc)
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
		s.finRead = true
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
	cp := s.copyPathLocked()
	s.writeCond.Broadcast()
	s.mu.Unlock()

	s.sendCopy(cp, enc)
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
	// A delayed acknowledgement would die with the paths, and the peer would
	// then wait out its own linger for it.
	ackPending := s.ackTimer != nil && !s.closed
	s.mu.Unlock()
	if ackPending {
		s.flushAck()
	}

	s.drainPaths()
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
	if ps.draining {
		ps.wmu.Unlock()
		return
	}
	err := ps.p.WriteFrame(enc)
	ps.wmu.Unlock()
	if err != nil {
		s.pathFailed(ps)
	}
}

// copyPathLocked returns the path a redundant session copies the frame just
// queued to, or nil if it should be sent only once.
func (s *Session) copyPathLocked() *pathState {
	if !s.cfg.Redundant || s.active == nil || s.unackedBytes > copyBudget {
		return nil
	}
	ps, _ := s.leastQuietLocked(s.active)
	return ps
}

// sendCopy queues a copy of enc for ps, or drops it if ps is behind. The
// original stays queued for retransmission, so a dropped copy loses nothing.
func (s *Session) sendCopy(ps *pathState, enc []byte) {
	if ps == nil {
		return
	}
	c := append([]byte(nil), enc...)
	c[0] |= copyFlag
	select {
	case ps.copies <- c:
	default:
	}
}

func (s *Session) copyLoop(ps *pathState) {
	for {
		select {
		case <-s.done:
			return
		case <-ps.gone:
			return
		case c := <-ps.copies:
			s.send(ps, c)
		}
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
	if ps.draining {
		return
	}
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
	close(ps.gone)
	for i, q := range s.paths {
		if q == ps {
			s.paths = append(s.paths[:i:i], s.paths[i+1:]...)
			break
		}
	}
	var next *pathState
	if s.active == ps {
		s.active = nil
		s.active, _ = s.leastQuietLocked(nil)
		next = s.active
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
	// that only fails toward the leader still gets abandoned on both sides.
	// Only one side may follow: if both did after picking different paths,
	// each would chase the other forever, resending everything every time.
	switched := false
	if s.cfg.FollowPeer && s.active != ps && !f.copy {
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
	// Acknowledgements are copied whatever is in flight: without them, a
	// stalled path would let unacknowledged data pass copyBudget and end the
	// copies just when they are needed.
	var cp *pathState
	if s.cfg.Redundant {
		cp, _ = s.leastQuietLocked(ps)
	}
	s.mu.Unlock()
	s.sendCopy(cp, enc)
	s.send(ps, enc)
}

// Complete reports whether the session's work is done: everything the peer
// sent, up to its CloseWrite, has been read, and the peer has acknowledged
// all data sent to it. Only the acknowledgement of this side's own
// CloseWrite may still be outstanding, which loses nothing.
func (s *Session) Complete() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.finRead {
		return false
	}
	for _, f := range s.unacked {
		if f.kind != frameFin {
			return false
		}
	}
	return true
}

// Drained reports whether everything was delivered in both directions: the
// peer's CloseWrite has been read and the peer acknowledged everything sent
// to it, including this side's CloseWrite.
func (s *Session) Drained() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finRead && s.writeClosed && len(s.unacked) == 0
}

// maxSwitchGap bounds how far repeated switches push back the next one.
const maxSwitchGap = 30 * time.Second

// watchActive moves a leading session off a path that has gone quiet while
// another has not, and drops the quiet one: a write blocked on it would
// otherwise hold the writer until the path gave up by itself. If the path was
// only slow, the cost is a redial and some harmless retransmission.
//
// A congested link can make every path look quiet in turn, since a ping waits
// behind the data queued on its own path. That needs a lot of data in flight,
// so a switch made with more than a quarter of the send window unacknowledged
// doubles the time before the next one, and only a path that stays healthy
// for maxSwitchGap resets it. With little in flight, as in interactive use,
// silence means a dead path, and every switch may happen as soon as needed.
func (s *Session) watchActive() {
	t := time.NewTicker(s.cfg.SwitchAfter / 4)
	defer t.Stop()
	gap := s.cfg.SwitchAfter
	var last, healthySince time.Time
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		}
		old, next, quiet, inFlight := s.leaveQuietPath(last, gap)
		switch {
		case next != nil:
			go s.resync(next, true)
			s.pathFailed(old)
			last, healthySince = time.Now(), time.Time{}
			if inFlight > s.cfg.SendWindow/4 {
				gap = min(2*gap, maxSwitchGap)
			} else {
				gap = s.cfg.SwitchAfter
			}
		case quiet:
			healthySince = time.Time{}
		case healthySince.IsZero():
			healthySince = time.Now()
		case time.Since(healthySince) >= maxSwitchGap:
			gap = s.cfg.SwitchAfter
		}
	}
}

// leaveQuietPath makes the least quiet other path active if the active one
// has been quiet for SwitchAfter and the last switch was at least gap ago. It
// returns the old and the new path if it switched, whether the active path is
// quiet, and how much was unacknowledged at the time.
func (s *Session) leaveQuietPath(last time.Time, gap time.Duration) (old, next *pathState, quiet bool, inFlight int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.active == nil {
		return nil, nil, false, 0
	}
	q, ok := s.active.p.(QuietPath)
	if !ok || q.Quiet() < s.cfg.SwitchAfter {
		return nil, nil, false, 0
	}
	if time.Since(last) < gap {
		return nil, nil, true, 0
	}
	// The other path need not be fresh, only heard from more recently than
	// this one and recently enough to be trusted.
	best, d := s.leastQuietLocked(s.active)
	if best == nil || d >= q.Quiet() || d >= 2*s.cfg.SwitchAfter {
		return nil, nil, true, 0
	}
	old = s.active
	s.active = best
	return old, best, true, s.unackedBytes
}

// leastQuietLocked returns the path, other than skip, that the peer was most
// recently heard from on, and how long ago that was. Among paths that cannot
// tell, it prefers the oldest.
func (s *Session) leastQuietLocked(skip *pathState) (*pathState, time.Duration) {
	var best *pathState
	bestQuiet := time.Duration(math.MaxInt64)
	for _, ps := range s.paths {
		if ps == skip {
			continue
		}
		d := time.Duration(math.MaxInt64)
		if q, ok := ps.p.(QuietPath); ok {
			d = q.Quiet()
		}
		if best == nil || d < bestQuiet {
			best, bestQuiet = ps, d
		}
	}
	return best, bestQuiet
}

// drainTimeout bounds how long Close waits for the peer to close paths that
// this side has half-closed.
const drainTimeout = time.Second

// drainPaths half-closes every path that supports it and waits, briefly, for
// the peer to close them in turn. Until then the read loops keep consuming,
// so nothing unread is left when the paths are torn down.
func (s *Session) drainPaths() {
	s.mu.Lock()
	paths := append([]*pathState(nil), s.paths...)
	s.mu.Unlock()
	half := false
	for _, ps := range paths {
		if hc, ok := ps.p.(interface{ CloseWrite() error }); ok {
			ps.wmu.Lock()
			ps.draining = true
			hc.CloseWrite()
			ps.wmu.Unlock()
			half = true
		}
	}
	if !half {
		return
	}
	deadline := time.Now().Add(drainTimeout)
	for s.NumPaths() > 0 && time.Now().Before(deadline) {
		select {
		case <-s.done:
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}
