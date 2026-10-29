# resumux

[![CI](https://github.com/k4lleanka/resumux/actions/workflows/ci.yml/badge.svg)](https://github.com/k4lleanka/resumux/actions/workflows/ci.yml)

A TCP tunnel over WebSockets where the connection survives the WebSocket.

Put SSH, a game server or a database behind Cloudflare with chisel or
wstunnel, and every open connection dies the moment the WebSocket does: when
the CDN restarts a server, when a proxy silently drops it, when your Wi-Fi
hiccups. They reconnect, but the connection you were using is gone.

resumux puts a small session layer between the two. The WebSocket can die,
go silent or be replaced, and the TCP connection on top just sees a pause.

```
app ──TCP──▶ resumux client ══ WebSocket paths ══▶ resumux server ──TCP──▶ target
```

> **Status:** pre-1.0. The protocol may still change. Tested through
> Cloudflare with cut, silenced and unplugged paths, but not yet in
> production.

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

Behind a CDN or reverse proxy that handles TLS, drop the certificate flags.
Always use `wss://` from the client: the token travels in a header.

All flags are in `resumux server -h` and `resumux client -h`. They can also
go in a JSON file, `resumux client -config client.json`, with the flag names
as keys.

## How it works

Your app connects to the resumux client like it would to the real server.
The client opens a WebSocket to the resumux server, which connects to the
real server. Those two TCP connections, app to client and server to target,
never break. Only the WebSockets in the middle do, and they are replaceable.

**Sessions and paths.** Each TCP connection becomes a *session*. A session
runs over one or more WebSockets, called *paths*, two by default. The
first WebSocket creates the session and gets back an id and a secret.
Any later WebSocket that shows both joins the same session, from any
address, so a client on a new network just carries on.

**Nothing is lost.** Every chunk of data is sent as a numbered frame. The
sender keeps each frame until the other side acknowledges it, and an
acknowledgement means the app on that side has actually read the data. Say
the client has sent frames 1 to 10 and the server has acknowledged up to 7
when the WebSocket dies:

```
client                                    server
  ── frames 1–10 ──▶  (path 1 dies)        has read 1–7, got 8
  ◀── ack: read up to 7 ──
  ── path 2: resend 8, 9, 10 ──▶           drops 8 (duplicate), reads 9, 10
```

The receiver puts frames in order and throws away anything it already has,
so resending too much is harmless and nothing is ever missing or doubled.
Because acknowledgements count only what was read, they also work as flow
control: a slow reader holds the sender back instead of filling memory.

**Noticing dead paths.** A WebSocket that closes is easy. The hard case is
one that goes silent, which proxies do when they drop a connection without
telling anyone. Each side pings a path that has been quiet for a second. If
the path in use stays silent while another one answers, the client switches
after about two seconds and resends what is unacknowledged. A dead path is
redialed in the background, so a spare is ready for the next failure.

**Who decides.** The client picks the path, and the server follows it. If
both sides chose independently they could keep chasing each other across
paths.

A session with no path at all waits up to two minutes for one
(`-resume-timeout`), then closes both ends. The full wire format is in
[docs/protocol.md](docs/protocol.md).

## Games: `-redundant`

TCP resends a lost packet, but when little is in flight, as in a game, it
waits for a timer first: 200–300 ms, more if the resend is lost too. That
is the lag spike players feel.

```sh
resumux client -listen 127.0.0.1:7000 -server wss://example.com/ -token-file token -redundant
```

With `-redundant` both sides also send every frame on a second WebSocket,
and the first copy to arrive wins. A packet stuck on one path arrives on
time over the other, and a frozen path goes unnoticed.

- It only copies while at most 64 KiB is unacknowledged, so downloads are
  still sent once. A game costs about twice its usual bandwidth, a few
  KB/s per player.
- It cannot help when the player's own line stalls, since both paths
  cross it.
- A server can say no with `-refuse-redundant`.

## Running a public server

[docs/deployment.md](docs/deployment.md) covers:

- giving the target the real client address with the PROXY protocol
  (`-proxy-protocol`, `-trusted-proxies`, `-client-ip-header`)
- limits per client address, for when the token ships with a public client
- listening on several ports and spreading paths over several routes

## Numbers

Through a Cloudflare quick tunnel from a home connection in Europe, ten small
messages a second each way:

| | Arrived | Median | Slowest |
|---|---|---|---|
| No faults | 200 / 200 | 26 ms | 226 ms |
| Every path cut every 5 s | 300 / 300 | 24 ms | 194 ms |
| The path in use silenced every 8 s | 400 / 400 | 26 ms | 2.1 s |
| Network cable pulled for 40 s | 1,200 / 1,200 | 26 ms | 43 s |

Everything arrived in order. The cable pull took down the client and the
server's Cloudflare connector at once, and the same session carried on.
The messages sent during the outage, about a third of that run, waited
and arrived when the network came back, up to 43 s late. The rest went
through as usual, so the median stays at 26 ms.

`-redundant`, with 16 sessions over an hour, half of them redundant, against
a server on a VPS in France. Each sent ten 300-byte messages a second plus a
16 KiB burst every 5 s:

| | Arrived | Median | p99 | Stalls > 200 ms | Longest |
|---|---|---|---|---|---|
| Plain | 288,000 / 288,000 | 29–33 ms | 43–47 ms | 28 | 742 ms |
| `-redundant` | 288,000 / 288,000 | 24–30 ms | 36–44 ms | 1 | 200 ms |

Over loopback, resumux itself adds about 0.12 ms per round trip and moves
about 120 MB/s. Run `go test -run '^$' -bench . ./tunnel/` to check on your
machine.

These are single runs on one connection. They show what the mechanisms do on
a real CDN, not a benchmark.

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
flakyproxy straight at port 22 instead and SSH dies at the first one.

`-freeze` silences a connection instead of closing it. `-delay`, `-jitter`
and `-hiccup-every` imitate a long, lossy route.
[examples/sshdemo](examples/sshdemo) runs the comparison in one Docker
container.

## Install

Binaries for Linux, macOS and Windows are on the
[releases page](https://github.com/k4lleanka/resumux/releases), with SHA-256
checksums and build provenance:

```sh
gh attestation verify resumux-v0.1.0-linux-amd64 --repo k4lleanka/resumux
```

From source, with Go 1.24 or newer:

```sh
go install github.com/k4lleanka/resumux/cmd/resumux@latest
```

Or as a container, running as non-root: `docker build -t resumux .`

## Limitations

- TCP only. No UDP, which many games and voice chat need.
- Paths give failover and redundancy, not more bandwidth.
- Sessions live in the server's memory. Restarting the server ends them.
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
