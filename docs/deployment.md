# Running a server

Details for running resumux in front of a public service. The
[README](../README.md) covers the basics.

## Client addresses

Without help, the target sees every connection as coming from the resumux
server, so per-address limits and bans on the target stop working. Targets
that speak the [PROXY protocol](https://www.haproxy.org/download/2.9/doc/proxy-protocol.txt)
can be told the client's address instead:

```sh
resumux server -listen 127.0.0.1:8080 -target 127.0.0.1:7000 -token-file token \
  -proxy-protocol
```

Each connection to the target then starts with a version 2 header naming the
address the WebSocket came from.

### Behind a proxy or CDN

There the WebSocket comes from the proxy, and the client's address is in a
header the proxy adds. Name both, and the server reads the header, but only
on requests from that proxy:

```sh
  -proxy-protocol -trusted-proxies 127.0.0.1 -client-ip-header CF-Connecting-IP
```

Anyone can send that header, so on requests from anywhere else it is
ignored. It also has to be a header the proxy always sets itself and
overwrites, as Cloudflare does with `CF-Connecting-IP`. A header the proxy
just passes along, such as `X-Real-IP` behind Cloudflare, lets every client
claim any address. With a chain of proxies that append to
`X-Forwarded-For`, trust all of them; the server skips their entries and
takes the one before.

The safest setup is a server that listens on loopback only, with the CDN's
connector, such as cloudflared, on the same machine. Then nothing can reach
the server except through the CDN.

### Header sizes and IPv6

The header is 28 bytes for an IPv4 client and 52 for IPv6. When the
client's address is unknown it is 16 bytes, and tells the target to use the
connection's own.

Some targets with hand-written support read a fixed 28 bytes and only
understand IPv4. An IPv6 client then corrupts the start of their stream.
Such a target should read the length field, as the specification requires.
Until it does, keep everything on IPv4: have the server listen on an IPv4
address such as 127.0.0.1, since the header names both ends in one family,
and keep clients on IPv4 too. Behind Cloudflare, Pseudo IPv4 set to
"Overwrite headers" puts an IPv4 address in `CF-Connecting-IP`.

### Clients that change networks

The target learns the client's address when the session opens. If the
client moves to another network mid-session, the target keeps the first
address. The resumux server logs every path with the address it came from,
so the change shows up there.

## Public servers

A token shipped with a public client, such as a game launcher, is an access
key, not a secret: anyone can pull it out. Then nothing stops one address
from opening every session the server allows. Limit what each client may
hold and open:

```sh
  -max-sessions-per-client 8 -max-new-sessions-per-client 30
```

The first is how many sessions one address, or one IPv6 /64, may hold at
once. The second is how many it may open per minute, counted in fixed
one-minute windows, so briefly up to twice that around a window's end.

- Only sessions that actually start count. Failed attempts, such as while
  the target restarts, cost nothing.
- Both limits only refuse new sessions, with 429. Resuming one never counts,
  so a client that switches networks is never cut off.
- Many people can share one address: a household, or a mobile network
  behind carrier-grade NAT. Set the limits for the most players you expect
  behind one address, with headroom. A session whose handshake answer was
  lost keeps its slot until it expires.
- Loopback, link-local and trusted proxies whose client could not be told
  are never counted, since they stand for many clients.

These limits stop one address, not many. Someone with many IPv4 addresses,
or an IPv6 /48 (65,536 /64s, which some hosts hand out freely), can still
open sessions up to `-max-sessions`. Pair them with rate limiting at the
proxy or CDN, such as a Cloudflare rate limiting rule on the WebSocket path.

### Resume timeout

The default is two minutes. For a game, `-resume-timeout 30s` on the server
still rides out CDN restarts and short drops, but lets a player whose
connection is really gone leave sooner. While a session waits, the target
sees an idle connection, as under heavy lag, and its own timeouts still
apply.

## Several routes

Paths to the same URL can take the same route and fail together. Give the
client several URLs that reach the same server, and its paths take them in
turn. They can differ in port, address or provider, such as one through a
CDN and one direct.

Cloudflare proxies HTTPS on ports 443, 2053, 2083, 2087, 2096 and 8443, and
connects to the server on the port the client used, so let the server
listen on each:

```sh
resumux server -listen :443,:2053,:8443 -tls-cert cert.pem -tls-key key.pem \
  -target 127.0.0.1:22 -token-file token
```

A session gets at least one path per URL, and a new connection falls
through to the next URL when one fails or is slow. A URL that says it does
not know the session ends it only if that URL has carried one of its paths
before; any other may reach a different server, so it is logged and
retried.
