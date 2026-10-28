// Command flakyproxy forwards TCP connections and breaks them at a fixed
// interval, to show what a resumux session does when its paths keep dying.
// By default it cuts every connection. With -freeze it instead silences the
// oldest one, dropping its data both ways without closing it, the way a
// path that a proxy loses without telling anyone behaves.
//
//	flakyproxy -listen 127.0.0.1:9000 -upstream 127.0.0.1:8080 -cut-every 3s
package main

import (
	"flag"
	"log"
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

// pump copies src to dst until either fails, dropping data while p is frozen.
func (p *pair) pump(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 && !p.frozen.Load() {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	p.close()
}

func main() {
	listen := flag.String("listen", "127.0.0.1:9000", "address to accept connections on")
	upstream := flag.String("upstream", "", "address to forward to (required)")
	every := flag.Duration("cut-every", 3*time.Second, "how often connections are broken")
	freeze := flag.Bool("freeze", false, "silence the oldest connection instead of cutting all of them")
	flag.Parse()
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

	mode := "cutting every connection"
	if *freeze {
		mode = "freezing the oldest connection"
	}
	log.Printf("forwarding %s -> %s, %s every %s", *listen, *upstream, mode, *every)
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
			go p.pump(out, in)
			p.pump(in, out)
			remove(p)
		}()
	}
}
