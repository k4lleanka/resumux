package tunnel

import (
	"encoding/binary"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// proxySignature starts every PROXY protocol version 2 header.
const proxySignature = "\r\n\r\n\x00\r\nQUIT\n"

// proxyHeader returns a PROXY protocol version 2 header saying that a TCP
// connection came from src to dst. Mixed address families are sent as IPv6,
// with the IPv4 address mapped. If either address is unknown, it sends the
// LOCAL command instead, which carries no address, rather than a made-up one.
func proxyHeader(src, dst netip.AddrPort) []byte {
	b := []byte(proxySignature)
	if !src.IsValid() || !dst.IsValid() {
		return append(b, 0x20, 0x00, 0x00, 0x00) // version 2, LOCAL, no address
	}
	b = append(b, 0x21) // version 2, PROXY command
	s, d := src.Addr().Unmap(), dst.Addr().Unmap()
	if s.Is4() && d.Is4() {
		b = append(b, 0x11) // TCP over IPv4
		b = binary.BigEndian.AppendUint16(b, 12)
		b = append(b, s.AsSlice()...)
		b = append(b, d.AsSlice()...)
	} else {
		b = append(b, 0x21) // TCP over IPv6
		b = binary.BigEndian.AppendUint16(b, 36)
		s16, d16 := s.As16(), d.As16()
		b = append(b, s16[:]...)
		b = append(b, d16[:]...)
	}
	b = binary.BigEndian.AppendUint16(b, src.Port())
	return binary.BigEndian.AppendUint16(b, dst.Port())
}

// clientAddr returns where the client behind r connects from. The address
// in cfg.ClientIPHeader is used only when the request itself comes from one
// of cfg.TrustedProxies: anyone can send that header, so believing it from
// anywhere else would let a client claim any address.
func clientAddr(r *http.Request, cfg ServerConfig) netip.AddrPort {
	remote, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		return netip.AddrPort{}
	}
	remote = netip.AddrPortFrom(remote.Addr().Unmap(), remote.Port())
	if cfg.ClientIPHeader == "" || !trusted(remote.Addr(), cfg.TrustedProxies) {
		return remote
	}
	values := r.Header.Values(cfg.ClientIPHeader)
	if len(values) == 0 {
		return remote
	}
	// Proxies that append to a list, as with X-Forwarded-For, each add the
	// address they saw last. Walking back from the end, entries added by
	// trusted proxies are skipped; the first other entry is the client.
	// Anything before it could have come from the client itself.
	list := strings.Split(strings.Join(values, ","), ",")
	var ip netip.Addr
	for i := len(list) - 1; i >= 0; i-- {
		a, err := netip.ParseAddr(strings.TrimSpace(list[i]))
		if err != nil {
			return remote
		}
		ip = a.Unmap().WithZone("")
		if !trusted(ip, cfg.TrustedProxies) {
			break
		}
	}
	// The proxy does not tell us the client's port.
	return netip.AddrPortFrom(ip, 0)
}

func trusted(a netip.Addr, proxies []netip.Prefix) bool {
	for _, p := range proxies {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// localAddr returns the address r arrived on, if the server recorded it.
func localAddr(r *http.Request) netip.AddrPort {
	a, ok := r.Context().Value(http.LocalAddrContextKey).(net.Addr)
	if !ok {
		return netip.AddrPort{}
	}
	ap, err := netip.ParseAddrPort(a.String())
	if err != nil {
		return netip.AddrPort{}
	}
	return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
}
