// Command flakyproxy forwards TCP connections and breaks them at a fixed
// interval, to show what a resumux session does when its paths keep dying.
// By default it cuts every connection. With -freeze it instead silences the
// oldest one, dropping its data both ways without closing it, the way a
// path that a proxy loses without telling anyone behaves. -delay, -jitter
// and -hiccup-every make it act like a long, lossy route, where TCP turns
// lost packets into stalls that hit every connection at once.
//
//	flakyproxy -listen 127.0.0.1:9000 -upstream 127.0.0.1:8080 -cut-every 3s
package main

import (
	"flag"
	"log"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// pair is one forwarded connection: the accepted side and the upstream side.
type pair struct {
	in, out net.Conn
	frozen  atomic.Bool
}

func (p *pair) close() {
	p.in.Close()
	p.out.Close()
}

// route shapes what a long path does to data: a fixed delay, some jitter, and
// hiccups that hold everything back at once, the way packet loss on a long
// route shows up inside TCP.
type route struct {
	delay, jitter time.Duration
	stallUntil    atomic.Int64 // unix nanoseconds; all connections wait until then
}

// pump copies src to dst until either fails, dropping data while p is frozen
// and delaying it as r says, in order.
func (p *pair) pump(dst, src net.Conn, r *route) {
	type chunk struct {
		b   []byte
		due time.Time
	}
	q := make(chan chunk, 1024)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for c := range q {
			if d := time.Until(c.due); d > 0 {
				time.Sleep(d)
			}
			if stall := time.Until(time.Unix(0, r.stallUntil.Load())); stall > 0 {
				time.Sleep(stall)
			}
			if _, err := dst.Write(c.b); err != nil {
				p.close()
				for range q {
				}
				return
			}
		}
	}()
	var last time.Time
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 && !p.frozen.Load() {
			due := time.Now().Add(r.delay)
			if r.jitter > 0 {
				due = due.Add(time.Duration(rand.Int64N(int64(r.jitter))))
			}
			// Jitter must not reorder a byte stream.
			if due.Before(last) {
				due = last
			}
			last = due
			q <- chunk{append([]byte(nil), buf[:n]...), due}
		}
		if err != nil {
			break
		}
	}
	close(q)
	<-done
	p.close()
}

func main() {
	listen := flag.String("listen", "127.0.0.1:9000", "address to accept connections on")
	upstream := flag.String("upstream", "", "address to forward to (required)")
	every := flag.Duration("cut-every", 3*time.Second, "how often connections are broken (0: never)")
	freeze := flag.Bool("freeze", false, "silence the oldest connection instead of cutting all of them")
	delay := flag.Duration("delay", 0, "extra delay for all data, each way")
	jitter := flag.Duration("jitter", 0, "up to this much more delay, varying")
	hiccupEvery := flag.Duration("hiccup-every", 0, "how often all connections stall at once (0: never)")
	hiccup := flag.Duration("hiccup", time.Second, "how long each stall lasts")
	flag.Parse()
	r := &route{delay: *delay, jitter: *jitter}
	if *upstream == "" {
		log.Fatal("-upstream is required")
	}

	l, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}

	var mu sync.Mutex
	var open []*pair // oldest first
	remove := func(p *pair) {
		mu.Lock()
		defer mu.Unlock()
		for i, q := range open {
			if q == p {
				open = append(open[:i], open[i+1:]...)
				return
			}
		}
	}

	if *hiccupEvery > 0 {
		go func() {
			for range time.Tick(*hiccupEvery) {
				r.stallUntil.Store(time.Now().Add(*hiccup).UnixNano())
				log.Printf("route stalled for %s", *hiccup)
			}
		}()
	}

	if *every > 0 {
		go func() {
			for range time.Tick(*every) {
				mu.Lock()
				if *freeze {
					for _, p := range open {
						if !p.frozen.Load() {
							p.frozen.Store(true)
							log.Printf("froze 1 of %d connections", len(open))
							break
						}
					}
					mu.Unlock()
					continue
				}
				n := len(open)
				for _, p := range open {
					p.close()
				}
				open = nil
				mu.Unlock()
				if n > 0 {
					log.Printf("cut %d connections", n)
				}
			}
		}()
	}

	switch {
	case *every <= 0:
		log.Printf("forwarding %s -> %s, never breaking connections", *listen, *upstream)
	case *freeze:
		log.Printf("forwarding %s -> %s, freezing the oldest connection every %s", *listen, *upstream, *every)
	default:
		log.Printf("forwarding %s -> %s, cutting every connection every %s", *listen, *upstream, *every)
	}
	if *delay > 0 || *jitter > 0 || *hiccupEvery > 0 {
		log.Printf("route: %s delay, up to %s jitter, %s stall every %s", *delay, *jitter, *hiccup, *hiccupEvery)
	}
	for {
		in, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			out, err := net.Dial("tcp", *upstream)
			if err != nil {
				in.Close()
				return
			}
			p := &pair{in: in, out: out}
			mu.Lock()
			open = append(open, p)
			mu.Unlock()
			go p.pump(out, in, r)
			p.pump(in, out, r)
			remove(p)
		}()
	}
}
