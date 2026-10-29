# resumux

[![CI](https://github.com/k4lleanka/resumux/actions/workflows/ci.yml/badge.svg)](https://github.com/k4lleanka/resumux/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/k4lleanka/resumux)](https://github.com/k4lleanka/resumux/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/k4lleanka/resumux.svg)](https://pkg.go.dev/github.com/k4lleanka/resumux)
[![License](https://img.shields.io/github/license/k4lleanka/resumux)](LICENSE)

**A TCP tunnel over WebSockets where the connection survives the WebSocket.**

Tunnel SSH, a game or a database through Cloudflare with chisel or wstunnel,
and every open connection dies the moment the WebSocket does: when the CDN
restarts a server, when a proxy silently drops it, when your Wi-Fi hiccups.
They reconnect, but the connection you were using is gone.

resumux keeps it. The WebSocket can die, go silent or be replaced, and the
TCP connection on top just sees a pause.

```
app ──TCP──▶ resumux client ══ WebSocket paths ══▶ resumux server ──TCP──▶ target
```

> **Status:** pre-1.0. The protocol may still change. Tested through
> Cloudflare with cut, silenced and unplugged paths, but not yet in
> production.

## Features

- **Connections survive** dropped, silent and replaced WebSockets, and the
  network going away for a while.
- **Nothing lost, nothing doubled.** Data is numbered and resent until the
  other side has read it.
- **Several paths per session**, with failover in about two seconds when one
  goes silent.
- **`-redundant` for games**: small packets go over two paths at once, which
  hides the lag spikes of TCP retransmits.
- **Works behind any CDN or proxy that passes WebSockets**, such as
  Cloudflare or nginx.
- **Made for public servers**: per-client limits, and the real client
  address passed on with the PROXY protocol.
- **One static binary** for Linux, macOS and Windows. No dependencies.

## What happens when…

| | A plain WebSocket tunnel | resumux | resumux `-redundant` |
|---|---|---|---|
| The WebSocket closes | connection lost | pause, under 0.2 s | pause, under 0.2 s |
| The WebSocket goes silent | connection lost, or hangs | pause of about 2 s | no pause |
| The network is down for 40 s | connection lost | waits, then carries on | waits, then carries on |
| A packet is lost (TCP retransmit) | stall of up to ~750 ms | stall of up to ~750 ms | almost always hidden |

Measured through Cloudflare. See [Numbers](#numbers) for the details.

## Install

Download a binary from the
[releases page](https://github.com/k4lleanka/resumux/releases). Each comes
with a SHA-256 checksum and build provenance you can verify:

```sh
gh attestation verify resumux-v0.1.0-linux-amd64 --repo k4lleanka/resumux
```

Or build it with Go 1.24 or newer:

```sh
go install github.com/k4lleanka/resumux/cmd/resumux@latest
```

Or as a container that runs as non-root: `docker build -t resumux .`

## Quick start

Make a token and copy it to both machines:

```sh
openssl rand -hex 32 > token
```

On the server, next to SSH:

```sh
resumux server -listen :443 -tls-cert cert.pem -tls-key key.pem \
  -target 127.0.0.1:22 -token-file token
```

On your machine:

```sh
resumux client -listen 127.0.0.1:2222 -server wss://example.com/ -token-file token
ssh -p 2222 user@127.0.0.1
```

Now cut the connection in between, restart the proxy or pull the network
for a minute, and the SSH session is still there.

- Behind a CDN or reverse proxy that handles TLS, leave out the certificate
  flags.
- Always use `wss://` from the client, since the token travels in a header.
- `resumux server -h` and `resumux client -h` list all flags. They can also
  go in a JSON file with the flag names as keys: `-config client.json`.

## How it works

Your app connects to the resumux client as if it were the real server. The
client opens WebSockets to the resumux server, which connects to the real
server. The two TCP connections at the ends never break. Only the
WebSockets in the middle do, and those are replaceable.

**Sessions and paths.** Each TCP connection becomes a *session*, carried by
one or more WebSockets, called *paths*, two by default. The first path
creates the session and gets an id and a secret. Any later path that shows
both joins the same session, from any address.

**Nothing is lost.** Data goes in numbered frames. The sender keeps each
frame until the other side acknowledges it, and an acknowledgement means
the app over there has actually read the data. Say frames 1 to 10 are on
their way and the server has read up to 7 when the WebSocket dies:

```
client                                    server
  ── frames 1–10 ──▶  (path 1 dies)        has read 1–7, got 8
  ◀── ack: read up to 7 ──
  ── path 2: resend 8, 9, 10 ──▶           drops 8 (duplicate), reads 9, 10
```

The receiver puts frames in order and throws away anything it already has,
so resending too much is harmless. Since acknowledgements count only what
was read, they double as flow control: a slow reader holds the sender back
instead of filling memory.

**Dead paths.** A WebSocket that closes is easy. The hard case is one that
goes silent, which proxies do when they drop a connection without telling
anyone. Each side pings a path that has been quiet for a second. If the
path in use stays silent while another one answers, the client switches
after about two seconds and resends what is unacknowledged. A dead path is
redialed in the background, so a spare is ready for the next failure.

**Who decides.** The client picks the path, and the server follows. If both
chose on their own, they could keep chasing each other.

A session with no path at all waits up to two minutes for one
(`-resume-timeout`), then closes both ends. The wire format is specified in
[docs/protocol.md](docs/protocol.md).

## Games and other interactive traffic

When a packet is lost and little else is in flight, as in a game, TCP waits
for a timer before resending it: 200–300 ms, more if the resend is lost too.
That is the lag spike players feel. A silent path costs even more, about two
seconds, before resumux can tell it is dead.

`-redundant` fixes both:

```sh
resumux client -listen 127.0.0.1:7000 -server wss://example.com/ -token-file token -redundant
```

Both sides then also send every frame on a second path, and the first copy
to arrive wins. A packet stuck on one path arrives on time over the other.

- Frames are copied only while at most 64 KiB is unacknowledged, so
  downloads are still sent once. A game costs about twice its usual
  bandwidth, a few KB/s per player.
- It cannot help when the player's own line stalls, since both paths cross
  it.
- A server can turn it down with `-refuse-redundant`.

## Running a public server

[docs/deployment.md](docs/deployment.md) covers:

- passing the real client address to the target with the PROXY protocol
- per-client limits, for when the token ships with a public client
- listening on several ports and spreading paths over several routes

## Numbers

All runs through a Cloudflare quick tunnel from a home connection in
Europe. They are single runs on one connection, meant to show what the
mechanisms do on a real CDN, not a benchmark.

**Faults, without `-redundant`.** Ten small messages a second each way:

| Fault | Arrived, in order | Median | Slowest |
|---|---|---|---|
| None | 200 / 200 | 26 ms | 226 ms |
| Every path cut every 5 s | 300 / 300 | 24 ms | 194 ms |
| The path in use silenced every 8 s | 400 / 400 | 26 ms | 2.1 s |
| Network cable pulled for 40 s | 1,200 / 1,200 | 26 ms | 43 s |

The cable pull took down both the client and the server's Cloudflare
connector, and the same session carried on. Messages sent during the
outage waited and arrived when the network came back, which is the 43 s.
The rest, about two thirds, went through as usual, so the median stays at
26 ms.

**Silenced path, with `-redundant`.** The same fault in the test suite: the
worst round trip is a few milliseconds instead of over two seconds.

**`-redundant` against packet loss.** 16 sessions for an hour, half of them
redundant, against a server on a VPS in France. Each sent ten 300-byte
messages a second plus a 16 KiB burst every 5 s:

| | Arrived, in order | Median | p99 | Stalls over 200 ms | Longest |
|---|---|---|---|---|---|
| Without | 288,000 / 288,000 | 29–33 ms | 43–47 ms | 28 | 742 ms |
| `-redundant` | 288,000 / 288,000 | 24–30 ms | 36–44 ms | 1 | 200 ms |

**Overhead.** Over loopback, resumux adds about 0.12 ms per round trip and
moves about 120 MB/s. Run `go test -run '^$' -bench . ./tunnel/` to measure
on your machine.

## Try it yourself

`flakyproxy` cuts every connection through it on a timer. Put it between
client and server:

```sh
go build ./cmd/resumux && go build ./examples/flakyproxy
resumux server -listen 127.0.0.1:8080 -target 127.0.0.1:22 -token-file token
flakyproxy -listen 127.0.0.1:9000 -upstream 127.0.0.1:8080 -cut-every 3s
resumux client -listen 127.0.0.1:2222 -server ws://127.0.0.1:9000/ -token-file token
ssh -p 2222 user@127.0.0.1
```

SSH keeps going while flakyproxy logs a cut every three seconds. Point
flakyproxy straight at port 22 instead, and SSH dies at the first one.

`-freeze` silences connections instead of closing them, and `-delay`,
`-jitter` and `-hiccup-every` imitate a long, lossy route.
[examples/sshdemo](examples/sshdemo) runs the whole comparison in one Docker
container.

## FAQ

**Why WebSockets?** They get through CDNs, corporate proxies and firewalls
that let nothing else through, and Cloudflare proxies them on its free plan.

**Why not a VPN such as WireGuard or Tailscale?** If you can run one, do:
WireGuard roams between networks, and TCP inside it survives. resumux is
for when you can only reach the server over HTTP(S), through a CDN or a
proxy, or when you don't want every user on a VPN, as with a public game
server.

**Why not mosh?** mosh is great for terminals, but it is not a TCP tunnel,
and it runs over UDP, which an HTTP CDN does not carry.

**Why not MPTCP?** It needs kernel support on both ends and raw TCP between
them. A CDN ends the TCP connection at its edge, so MPTCP cannot cross it.

**Does the target see the client's address?** With `-proxy-protocol`, yes.
See [docs/deployment.md](docs/deployment.md).

## Limitations

- TCP only. No UDP, which many games and voice chat need.
- Paths give failover and redundancy, not more bandwidth.
- Sessions live in the server's memory, so restarting the server ends them.
- One server forwards to one target.
- A connection reset on one end reaches the other as a normal close.
- Check your CDN's terms. Cloudflare, for one, restricts serving video and
  large files through it.

## Security

- Every WebSocket needs the shared token. It is read from a file or
  `RESUMUX_TOKEN`, never the command line, and compared in constant time.
- Each session has a random 128-bit id and a 256-bit secret. Joining takes
  both, and a wrong secret looks exactly like an unknown id.
- The server caps sessions, paths per session and buffered data, and drops
  peers that send more than they may.
- Frames rely on the WebSocket's TLS. Do not use `ws://` across a network
  you do not trust.

Logs show session events with the first eight characters of the id, never
secrets. To report a vulnerability, see [SECURITY.md](SECURITY.md).

## Compared with

| | Through a CDN | Connections survive a reconnect | Several paths |
|---|---|---|---|
| resumux | yes | yes | yes, failover or redundant |
| [chisel](https://github.com/jpillora/chisel) | yes | no | no |
| [wstunnel](https://github.com/erebe/wstunnel) | yes | no | no |
| [kptunnel](https://github.com/ifritJP/kptunnel) | not documented | yes | not documented |
| [Cloudflare Tunnel + Access](https://developers.cloudflare.com/cloudflare-one/) | Cloudflare only | not documented | no |
| [Eternal Terminal](https://eternalterminal.dev) | no | terminal sessions | no |

From each project's docs, October 2026. Cloudflare's own TCP access needs
cloudflared or its client on every user's machine and a login, which suits
a team more than a public service.

Similar ideas elsewhere: [resumeproxy](https://github.com/erfanara/resumeproxy)
resumes a TCP stream over one replaceable connection,
[BAMF](https://github.com/mattrobinsonsre/bamf) does it inside an access
platform, and [multipath](https://github.com/micro-teams/multipath) offers
resumable, duplicated streams over WebSockets as a library.

## Development

```sh
go test -race ./...
go test -run '^$' -fuzz FuzzSessionInput -fuzztime 30s ./session/
```

The tests push data through the whole chain while WebSockets are cut every
few milliseconds, and every byte has to arrive once and in order. CI runs
them with the race detector on Linux, macOS and Windows, plus staticcheck
and fuzzing.

## License

Apache-2.0. See [LICENSE](LICENSE).
