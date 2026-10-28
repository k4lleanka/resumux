package tunnel

import (
	"io"
	"net"
	"testing"
	"time"
)

// pingPong sends one byte and waits for the echo, n times, and returns the
// mean round trip.
func pingPong(b *testing.B, addr string) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	buf := []byte{1}
	// Warm up so the session and its paths are fully established.
	for range 100 {
		c.Write(buf)
		io.ReadFull(c, buf)
	}
	b.ResetTimer()
	start := time.Now()
	for b.Loop() {
		if _, err := c.Write(buf); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(c, buf); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(time.Since(start).Microseconds())/float64(b.N), "µs/roundtrip")
}

func BenchmarkRoundTripDirect(b *testing.B) {
	pingPong(b, echoServer(b))
}

func BenchmarkRoundTripTunnel(b *testing.B) {
	h := startServer(b, ServerConfig{})
	addr := startClient(b, ClientConfig{ServerURLs: []string{h.url}})
	pingPong(b, addr)
}

func throughput(b *testing.B, addr string) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	const chunk = 64 << 10
	data := make([]byte, chunk)
	go io.Copy(io.Discard, c)
	b.SetBytes(chunk)
	b.ResetTimer()
	for b.Loop() {
		if _, err := c.Write(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkThroughputDirect(b *testing.B) {
	throughput(b, echoServer(b))
}

func BenchmarkThroughputTunnel(b *testing.B) {
	h := startServer(b, ServerConfig{})
	addr := startClient(b, ClientConfig{ServerURLs: []string{h.url}})
	throughput(b, addr)
}
