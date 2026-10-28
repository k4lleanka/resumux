// Package tunnel forwards TCP connections over resumable WebSocket sessions.
//
// A client accepts local TCP connections and opens one session per
// connection. Each session runs over one or more WebSocket paths to the
// server, which dials the target and forwards the bytes. When a path dies the
// client dials a new one and the session resumes where it stopped.
package tunnel

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"io"
	"net"
	"sync"
	"time"

	"github.com/k4lleanka/resumux/session"
)

// HTTP headers used during the WebSocket handshake.
const (
	headerVersion = "Resumux-Version"
	headerSession = "Resumux-Session"
	headerSecret  = "Resumux-Secret"

	protocolVersion = "1"
)

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// equalSecret compares in constant time. Hashing first keeps the time
// independent of the lengths, too.
func equalSecret(a, b string) bool {
	ha, hb := sha256.Sum256([]byte(a)), sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(ha[:], hb[:]) == 1
}

// pipe copies between a session and a TCP connection until both directions
// are finished, then closes both. It returns the error that cut the stream
// from the session short, or nil if the peer closed it cleanly.
func pipe(s *session.Session, c net.Conn) error {
	go func() {
		<-s.Done()
		c.Close()
	}()

	var wg sync.WaitGroup
	var recvErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(s, c)
		s.CloseWrite()
	}()
	go func() {
		defer wg.Done()
		_, recvErr = io.Copy(c, s)
		if cw, ok := c.(interface{ CloseWrite() error }); ok {
			cw.CloseWrite()
		} else {
			c.Close()
		}
	}()
	wg.Wait()
	s.Close()
	c.Close()
	return recvErr
}

// expireWithoutPaths aborts s once it has had no path for longer than timeout.
func expireWithoutPaths(s *session.Session, timeout time.Duration, err error) {
	t := time.NewTicker(timeout / 10)
	defer t.Stop()
	var since time.Time
	for {
		select {
		case <-s.Done():
			return
		case now := <-t.C:
			if s.NumPaths() > 0 {
				since = time.Time{}
				continue
			}
			if since.IsZero() {
				since = now
			} else if now.Sub(since) >= timeout {
				s.Abort(err)
				return
			}
		}
	}
}
