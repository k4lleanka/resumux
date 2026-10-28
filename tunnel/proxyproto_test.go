package tunnel

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"
)

func TestProxyHeaderIPv4(t *testing.T) {
	got := proxyHeader(netip.MustParseAddrPort("192.0.2.1:56324"), netip.MustParseAddrPort("198.51.100.7:443"))
	want := append([]byte(proxySignature),
		0x21, 0x11, 0x00, 0x0c, // v2 PROXY, TCP over IPv4, 12 bytes
		192, 0, 2, 1, // source
		198, 51, 100, 7, // destination
		0xdc, 0x04, // source port 56324
		0x01, 0xbb, // destination port 443
	)
	if !bytes.Equal(got, want) {
		t.Fatalf("got  % x\nwant % x", got, want)
	}
}

func TestProxyHeaderIPv6AndMixed(t *testing.T) {
	got := proxyHeader(netip.MustParseAddrPort("[2001:db8::1]:1000"), netip.MustParseAddrPort("198.51.100.7:443"))
	if got[13] != 0x21 || binary.BigEndian.Uint16(got[14:16]) != 36 || len(got) != 16+36 {
		t.Fatalf("not a TCP over IPv6 header: % x", got[:16])
	}
	src, _ := netip.AddrFromSlice(got[16:32])
	dst, _ := netip.AddrFromSlice(got[32:48])
	if src != netip.MustParseAddr("2001:db8::1") || dst.Unmap() != netip.MustParseAddr("198.51.100.7") {
		t.Fatalf("got %s -> %s", src, dst)
	}
	if binary.BigEndian.Uint16(got[48:50]) != 1000 || binary.BigEndian.Uint16(got[50:52]) != 443 {
		t.Fatalf("wrong ports: % x", got[48:52])
	}
}

func TestProxyHeaderUnknownAddressIsLocal(t *testing.T) {
	got := proxyHeader(netip.AddrPort{}, netip.MustParseAddrPort("198.51.100.7:443"))
	want := append([]byte(proxySignature), 0x20, 0x00, 0x00, 0x00)
	if !bytes.Equal(got, want) {
		t.Fatalf("got % x", got)
	}
}

func TestClientAddr(t *testing.T) {
	trustedLocal := ServerConfig{
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")},
		ClientIPHeader: "CF-Connecting-IP",
	}
	chain := ServerConfig{
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32"), netip.MustParsePrefix("10.0.0.0/8")},
		ClientIPHeader: "X-Forwarded-For",
	}
	for _, tc := range []struct {
		name   string
		cfg    ServerConfig
		remote string
		header []string
		want   string
	}{
		{"no proxy configured, header ignored", ServerConfig{}, "198.51.100.9:4000", []string{"203.0.113.9"}, "198.51.100.9:4000"},
		{"untrusted sender, forged header ignored", trustedLocal, "198.51.100.9:4000", []string{"203.0.113.9"}, "198.51.100.9:4000"},
		{"trusted proxy", trustedLocal, "127.0.0.1:5000", []string{"203.0.113.9"}, "203.0.113.9:0"},
		{"trusted proxy, IPv6 client", trustedLocal, "127.0.0.1:5000", []string{"2001:db8::7"}, "[2001:db8::7]:0"},
		{"trusted proxy, list takes the last entry", trustedLocal, "127.0.0.1:5000", []string{"10.0.0.1, 203.0.113.9"}, "203.0.113.9:0"},
		{"trusted proxy, repeated header takes the last", trustedLocal, "127.0.0.1:5000", []string{"10.0.0.1", "203.0.113.9"}, "203.0.113.9:0"},
		{"chain of trusted proxies is skipped", chain, "127.0.0.1:5000", []string{"198.51.100.5, 10.1.2.3"}, "198.51.100.5:0"},
		{"entries the client added are ignored", chain, "127.0.0.1:5000", []string{"6.6.6.6, 198.51.100.5, 10.1.2.3"}, "198.51.100.5:0"},
		{"zone is dropped", trustedLocal, "127.0.0.1:5000", []string{"fe80::1%eth0"}, "[fe80::1]:0"},
		{"trusted proxy, garbage falls back", trustedLocal, "127.0.0.1:5000", []string{"not-an-ip"}, "127.0.0.1:5000"},
		{"trusted proxy, no header falls back", trustedLocal, "127.0.0.1:5000", nil, "127.0.0.1:5000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			r.RemoteAddr = tc.remote
			for _, v := range tc.header {
				r.Header.Add(tc.cfg.ClientIPHeader, v)
			}
			if got := clientAddr(r, tc.cfg).String(); got != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}

// readProxySource reads a PROXY v2 header from c and returns its source.
func readProxySource(c net.Conn) (netip.AddrPort, error) {
	head := make([]byte, 16)
	if _, err := io.ReadFull(c, head); err != nil {
		return netip.AddrPort{}, err
	}
	body := make([]byte, binary.BigEndian.Uint16(head[14:16]))
	if _, err := io.ReadFull(c, body); err != nil {
		return netip.AddrPort{}, err
	}
	if string(head[:12]) != proxySignature || head[12] != 0x21 {
		return netip.AddrPort{}, io.ErrUnexpectedEOF
	}
	if head[13] == 0x11 {
		ip, _ := netip.AddrFromSlice(body[0:4])
		return netip.AddrPortFrom(ip, binary.BigEndian.Uint16(body[8:10])), nil
	}
	ip, _ := netip.AddrFromSlice(body[0:16])
	return netip.AddrPortFrom(ip.Unmap(), binary.BigEndian.Uint16(body[32:34])), nil
}

// TestProxyProtocolReachesTarget sends a header naming a client through the
// whole chain, once from a trusted proxy and once from an untrusted one.
func TestProxyProtocolReachesTarget(t *testing.T) {
	for _, tc := range []struct {
		name    string
		trusted []netip.Prefix
		want    string
	}{
		{"trusted", []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}, "203.0.113.9"},
		{"untrusted", []netip.Prefix{netip.MustParsePrefix("192.0.2.0/24")}, "127.0.0.1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seen := make(chan netip.AddrPort, 1)
			target := listenTarget(t, func(c *net.TCPConn) {
				src, err := readProxySource(c)
				if err != nil {
					c.Close()
					return
				}
				seen <- src
				io.Copy(c, c)
			})
			h := startServer(t, ServerConfig{
				Target:         target,
				ProxyProtocol:  true,
				TrustedProxies: tc.trusted,
				ClientIPHeader: "CF-Connecting-IP",
			})
			addr := startClient(t, ClientConfig{
				ServerURLs: []string{h.url},
				Header:     http.Header{"Cf-Connecting-Ip": {"203.0.113.9"}},
			})
			c, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(10 * time.Second))
			echoOn(t, c, "hello")
			select {
			case src := <-seen:
				if src.Addr().String() != tc.want {
					t.Fatalf("target saw %s, want %s", src.Addr(), tc.want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("target got no PROXY header")
			}
		})
	}
}
