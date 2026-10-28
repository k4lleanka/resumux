// Command flakyproxy forwards TCP connections and cuts all of them at a fixed
// interval, to show what a resumux session does when its paths keep dying.
//
//	flakyproxy -listen 127.0.0.1:9000 -upstream 127.0.0.1:8080 -cut-every 3s
package main

import (
	"flag"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:9000", "address to accept connections on")
	upstream := flag.String("upstream", "", "address to forward to (required)")
	every := flag.Duration("cut-every", 3*time.Second, "how often every connection is cut")
	flag.Parse()
	if *upstream == "" {
		log.Fatal("-upstream is required")
	}

	l, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}

	var mu sync.Mutex
	open := map[net.Conn]bool{}
	track := func(c net.Conn, add bool) {
		mu.Lock()
		defer mu.Unlock()
		if add {
			open[c] = true
		} else {
			delete(open, c)
		}
	}

	go func() {
		for range time.Tick(*every) {
			mu.Lock()
			n := len(open)
			for c := range open {
				c.Close()
				delete(open, c)
			}
			mu.Unlock()
			if n > 0 {
				log.Printf("cut %d connections", n)
			}
		}
	}()

	log.Printf("forwarding %s -> %s, cutting every %s", *listen, *upstream, *every)
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
			track(in, true)
			track(out, true)
			go func() { io.Copy(out, in); out.Close() }()
			io.Copy(in, out)
			in.Close()
			track(in, false)
			track(out, false)
		}()
	}
}
