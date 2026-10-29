# resumux

[![CI](https://github.com/k4lleanka/resumux/actions/workflows/ci.yml/badge.svg)](https://github.com/k4lleanka/resumux/actions/workflows/ci.yml)

Forward TCP over WebSockets, and keep the connection alive when the network
underneath it drops.

A TCP connection through resumux survives what usually kills it on the way:
a CDN or proxy restarting, a WebSocket that dies or goes silent, a route that
drops for a moment. The applications on both ends see a pause, not a
disconnect. A client that changes networks, from Wi-Fi to mobile data, keeps
its connections too, as long as a new path gets through within the resume
timeout: two minutes by default, set with `-resume-timeout`.

It is meant for long-lived TCP connections that have to cross a
WebSocket-friendly proxy or CDN: SSH, game servers, databases, internal tools.

> **Status:** new and pre-1.0. The protocol may still change between versions.
> It has been tested through Cloudflare with idle connections, repeated path
> cuts and silent paths, but has no production use yet.

## How it works

```
app ──TCP──▶ resumux client ══ WebSocket paths ══▶ resumux server ──TCP──▶ target
```

Each TCP connection becomes a session, and each session runs over one or more
WebSocket paths at once.

- Every frame carries a sequence number, in both directions.
- The sender keeps each frame until the other side acknowledges it.
- The receiver puts frames back in order and drops duplicates.
- When a path dies, the client dials a new one, and everything not yet
  acknowledged is sent again.
- A path that goes silent without closing, as when a proxy drops it, is
  left in about two seconds if another path is healthy, and counts as dead
  after 5 seconds otherwise (`-dead-after`). Idle paths are pinged
  every second to keep both checks honest; busy ones are not, so pings never
  compete with traffic.

A path can therefore fail at any moment without the stream losing, repeating
or reordering a byte. Acknowledgements only count data the application has
read, which doubles as flow control: a slow reader stops the sender instead of
filling memory.

[docs/protocol.md](docs/protocol.md) specifies the handshake and frames.

## Install

Download a binary for Linux, macOS or Windows from the
[releases page](https://github.com/k4lleanka/resumux/releases). Each release
lists SHA-256 checksums and has a build provenance attestation, which you can
check with the GitHub CLI:

```sh
gh attestation verify resumux-v0.1.0-linux-amd64 --repo k4lleanka/resumux
```

Or build from source with Go 1.24 or newer:

```sh
go install github.com/k4lleanka/resumux/cmd/resumux@latest
```

Or build the container image, which runs as a non-root user:

```sh
docker build -t resumux .
docker run --rm resumux version
```

## Quick start

Create a shared token and copy it to both machines:

```sh
openssl rand -hex 32 > token
```

On the server, next to the service you want to reach (SSH in this example):

```sh
resumux server -listen :443 -tls-cert cert.pem -tls-key key.pem \
  -target 127.0.0.1:22 -token-file token
```

On the client:

```sh
resumux client -listen 127.0.0.1:2222 -server wss://example.com/ -token-file token
ssh -p 2222 user@127.0.0.1
```

If a reverse proxy or CDN in front of the server terminates TLS, leave out the
certificate flags and let the server listen on plain HTTP behind it. Either
way, the client should use a `wss://` URL: the token and the session secret
travel in HTTP headers. For a certificate from your own CA, give the client
`-ca-file ca.pem`.

Settings can also go in a JSON file, with the flag names as keys. Flags on the
command line take precedence.

```json
{ "listen": "127.0.0.1:2222", "server": "wss://example.com/", "token-file": "token", "paths": 2 }
```

```sh
resumux client -config client.json
```

Run `resumux server -h` and `resumux client -h` for all flags.

## Spread paths over several routes

Paths to the same URL can take the same route and fail together. Give the
client several URLs that reach the same server, separated by commas, and its
paths take them in turn:

```sh
resumux client -listen 127.0.0.1:2222 -token-file token \
  -server wss://example.com/,wss://example.com:8443/,wss://direct.example.com/
```

They can differ in port, so that a CDN serves them as separate connections,
in address, or in provider, such as one URL through a CDN and one straight to
the server. The server has to be reachable on each. Cloudflare, for example,
proxies HTTPS on ports 443, 2053, 2083, 2087, 2096 and 8443. A session gets at
least one path per URL, and a new connection falls through to the next URL
when one fails or is slow to answer. A URL that answers that it does not know
the session ends it only if that URL has carried one of its paths before; any
other such URL may reach a different server, so it is logged and retried.

## Games and other interactive traffic

TCP hides a lost packet by sending it again, but when only a few small
packets are in flight, as in a game, it first waits for a timer: often
200–300 ms, and twice that if the resend is lost too. A path that goes silent
stalls the stream for about two seconds, until the client moves off it.

With `-redundant` on the client, both sides send each frame on a second path
as well, and the receiver keeps whichever copy arrives first:

```sh
resumux client -listen 127.0.0.1:7000 -server wss://example.com/ -token-file token -redundant
```

A packet lost or held up on one path then arrives on time over the other.
Copies are only sent while at most 64 KiB is unacknowledged, so a large
download is sent once; a game's traffic stays far below that and costs
about twice its usual bandwidth, a few kilobytes a second per player. It does
not help when the client's own connection stalls, since every path crosses
it. A server can turn the feature down with `-refuse-redundant`.

## Try it on a flaky link

`examples/flakyproxy` forwards TCP and cuts every connection at a fixed
interval. Put it between the client and the server and keep a connection open
through the tunnel:

```sh
go build ./cmd/resumux && go build ./examples/flakyproxy
resumux server -listen 127.0.0.1:8080 -target 127.0.0.1:22 -token-file token
flakyproxy -listen 127.0.0.1:9000 -upstream 127.0.0.1:8080 -cut-every 3s
resumux client -listen 127.0.0.1:2222 -server ws://127.0.0.1:9000/ -token-file token
ssh -p 2222 user@127.0.0.1
```

Every three seconds flakyproxy logs `cut 2 connections`, which takes down both
paths of the session at once, and the SSH session carries on. Put flakyproxy
straight in front of port 22 instead, without resumux, and SSH drops at the
first cut.

With `-freeze`, flakyproxy instead silences its oldest connection each time,
dropping data both ways without closing it, like a proxy that loses a path
without telling anyone. The session then moves to its other path in about
two seconds.

`-delay`, `-jitter` and `-hiccup-every` make it act like a long, lossy route
instead, where lost packets show up as stalls that hit every connection at
once:

```sh
flakyproxy -listen 127.0.0.1:9000 -upstream 127.0.0.1:8080 -cut-every 0 \
  -delay 150ms -jitter 50ms -hiccup-every 15s -hiccup 1500ms
```

[examples/sshdemo](examples/sshdemo) runs the whole comparison in one Docker
container with a real OpenSSH server.

## Overhead

Measured on one machine over loopback, so this is the cost of resumux itself,
not of the network:

| | Direct TCP | Through resumux |
|---|---|---|
| Round trip of one byte | 0.04 ms | 0.16 ms |
| Throughput | ~990 MB/s | ~120 MB/s |

Over the internet the path to the server, and through a CDN its nearest edge,
adds far more than this. Run the benchmarks yourself with
`go test -run '^$' -bench . ./tunnel/`.

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
address the WebSocket came from. Behind a proxy or CDN, that is the proxy's
address, and the client's is in a header the proxy adds. Name both the proxy
and the header, and the server uses the header, but only on requests from
that proxy:

```sh
  -proxy-protocol -trusted-proxies 127.0.0.1 -client-ip-header CF-Connecting-IP
```

Anyone can send that header, so a request from any other address is judged
by where it really comes from. The header must also be one the trusted proxy
always sets itself, replacing whatever the client sent, as Cloudflare does
with CF-Connecting-IP. A header the proxy only passes through, such as
X-Real-IP behind Cloudflare, lets every client name any address. With a
chain of proxies that append to X-Forwarded-For, trust all of them: the
server skips entries they added and takes the first one before them.

The safest setup is a server that listens on loopback only, with the CDN's
connector, such as cloudflared, on the same machine. Then nothing off that
machine can reach the server except through the CDN.

## Public servers

A token handed out with a public client, such as in a game launcher, is an
access key, not a secret: anyone can take it from the client. Then nothing
stops one address from opening every session the server allows. Limit what
each client may hold and open:

```sh
  -max-sessions-per-client 8 -max-new-sessions-per-client 30
```

The first is how many sessions one address, or one IPv6 /64, may hold at
once; the second, how many it may open per minute, counted in fixed
one-minute windows, so briefly up to twice that around a window's end. Only
sessions that actually start count: attempts that fail, as while the
target is restarting, cost nothing. Both limits only refuse new sessions,
with 429. Resuming one never counts, so a client that moves to another
network mid-session is never cut off.

For a game, a shorter resume timeout, such as `-resume-timeout 30s` on the
server, still rides out CDN restarts and brief drops, but lets a player whose
own connection is really gone leave the game sooner. While a session waits,
the target sees an idle connection, as it would under heavy lag, and its own
timeouts still apply.

Many people can share one address: a household, or a whole mobile network
behind carrier-grade NAT. Set the limits for the most players you expect
behind one address, not for one player, and leave headroom: a session
whose handshake answer was lost keeps its slot until it expires, two
minutes by default. Addresses that stand for many clients are never
counted: loopback, link-local, and trusted proxies whose client could not
be told, including when the client address header holds something that is
not an address. Behind a proxy, configure it as described above, or every
client looks like the proxy.

These limits stop one address, not many. An attacker with many IPv4
addresses, or with an IPv6 /48, which holds 65,536 /64s and some hosting
providers hand out freely, can still open sessions up to `-max-sessions`.
Pair them with rate limiting at the proxy or CDN, such as a Cloudflare rate
limiting rule on the WebSocket path.

## Measured behind Cloudflare

One client and server, ten small messages a second each way, through a
Cloudflare quick tunnel from a home connection in Europe, on pre-release
builds of this version:

| | Messages | Median round trip | Slowest |
|---|---|---|---|
| No faults | 200 of 200, in order | 26 ms | 226 ms |
| Every path cut, every 5 s | 300 of 300, in order | 24 ms | 194 ms |
| The path in use silenced, every 8 s | 400 of 400, in order | 26 ms | 2.1 s |
| Network cable pulled for about 40 s | 1,200 of 1,200, in order | 26 ms | 43 s |

A 16 MiB transfer arrived intact in 3.7 s, and a connection left idle for
150 s stayed up. When the cable was pulled, both the client and the
server's Cloudflare connector lost the network, and the session carried on
once it was back. One run on one connection is not a benchmark; it shows
what the mechanisms do on a real CDN.

For `-redundant`, 16 sessions ran side by side for an hour from the same
home connection, half of them with it, each with ten 300-byte messages a
second and a 16 KiB burst every five seconds. The server ran on a VPS in
France, behind a Cloudflare quick tunnel served from Frankfurt:

| | Messages | Median round trip | 99th percentile | Stalls over 200 ms | Longest |
|---|---|---|---|---|---|
| Plain | 288,000 of 288,000, in order | 29–33 ms | 43–47 ms | 28 | 742 ms |
| `-redundant` | 288,000 of 288,000, in order | 24–30 ms | 36–44 ms | 1 | 200 ms |

Each stall without it hit one session at a time, the kind a lost packet
causes; 742 ms is about what TCP waits when the resend is lost too. The
same comparison with the server on the home connection gave 29 such stalls
against none.


## Security

- Every path must present the shared token (`Authorization: Bearer`). It is
  read from a file or the `RESUMUX_TOKEN` environment variable, never from the
  command line, and compared in constant time.
- A new session gets a random 128-bit id and a 256-bit secret. Joining a
  session takes both; an unknown id and a wrong secret get the same answer.
- The server limits sessions (`-max-sessions`), paths per session
  (`-max-paths`) and buffered data per session, and closes a session that has
  had no path for `-resume-timeout`. A peer that sends more than it is allowed
  to have in flight is disconnected.
- Frames have no protection of their own beyond the WebSocket's TLS. Do not
  use plain `ws://` across a network you do not trust.

The server logs when sessions open, gain a path and close, with the first
eight characters of the session id. Secrets are never logged.

## Limitations

- TCP only. UDP, which many games and voice chat use, is not carried.
- Paths provide failover; they are not combined for more bandwidth.
- Sessions live in the server's memory, so restarting the server ends them.
- One server forwards to one target.
- With `-proxy-protocol`, the target learns the client's address when the
  session opens. If the client moves to another network mid-session, the
  target keeps seeing the first address.
- A client with more URLs than the server allows paths per session
  (`-max-paths`, 8 by default) has its extra paths refused.
- A connection reset on one end reaches the other end as a normal close. The
  reverse case is handled: a stream resumux itself loses is reset, never
  closed normally.
- If you run it through a CDN, check its terms. Cloudflare, for example,
  restricts using its CDN for video and large files hosted elsewhere.

## Compared with

| | WebSocket / CDN | Open connections survive a reconnect | Several paths per connection |
|---|---|---|---|
| resumux | yes | yes | yes (failover, or redundant copies) |
| [kptunnel](https://github.com/ifritJP/kptunnel) | WebSocket; CDN not documented | yes, per its README | not documented |
| [chisel](https://github.com/jpillora/chisel) | yes | no | no |
| [wstunnel](https://github.com/erebe/wstunnel) | yes | no | no |
| [Cloudflare Tunnel with Access](https://developers.cloudflare.com/cloudflare-one/) | Cloudflare only | not documented | no |
| [Eternal Terminal](https://eternalterminal.dev) | no | yes, for terminal sessions | no |

Based on each project's documentation in October 2026. chisel and wstunnel
reconnect the WebSocket, but neither documents keeping connections that were
open when it dropped. kptunnel does, though it does not say how.

Cloudflare's own way to reach TCP services through its network needs
cloudflared or the Cloudflare One client on each user's machine and a login
through Cloudflare Access; its free plan covers 50 users. That suits a team.
resumux is meant for services many people reach, such as a public server
shipped with its own client, and for connections that must outlive the
network underneath them.

## Development

```sh
go test -race ./...
go test -run '^$' -fuzz FuzzSessionInput -fuzztime 30s ./session/
```

The tests send data through the whole chain while WebSocket connections are
cut every few milliseconds, and every byte has to arrive once and in order.
Others attack a session with a peer that ignores flow control. CI runs the
tests with the race detector on Linux, macOS and Windows, plus staticcheck and
fuzzing.

## License

Apache-2.0. See [LICENSE](LICENSE).
