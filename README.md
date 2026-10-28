# resumux

[![CI](https://github.com/k4lleanka/resumux/actions/workflows/ci.yml/badge.svg)](https://github.com/k4lleanka/resumux/actions/workflows/ci.yml)

Forward TCP over WebSockets, and keep the connection alive when the network
underneath it drops.

A TCP connection through resumux survives a dead WebSocket, a switch from
Wi-Fi to mobile data, a new IP address, or a proxy restarting, as long as a new
path gets through within the resume timeout (two minutes by default). The
applications on both ends see a pause, not a disconnect.

It is meant for long-lived TCP connections that have to cross a
WebSocket-friendly proxy or CDN: SSH, game servers, databases, internal tools.

> **Status:** new and pre-1.0. The protocol may still change between versions,
> and resumux has not yet been tested behind a production CDN.

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

Every three seconds flakyproxy logs `cut 4 connections`, which takes down both
paths of the session at once, and the SSH session carries on. Put flakyproxy
straight in front of port 22 instead, without resumux, and SSH drops at the
first cut.

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
- A connection reset on one end reaches the other end as a normal close. The
  reverse case is handled: a stream resumux itself loses is reset, never
  closed normally.
- If you run it through a CDN, check its terms. Cloudflare, for example,
  restricts using its CDN for video and large files hosted elsewhere.

## Compared with

| | WebSocket / CDN | Open connections survive a reconnect | Several paths per connection |
|---|---|---|---|
| resumux | yes | yes | yes (failover) |
| [chisel](https://github.com/jpillora/chisel) | yes | no | no |
| [wstunnel](https://github.com/erebe/wstunnel) | yes | no | no |
| [Eternal Terminal](https://eternalterminal.dev) | no | yes, for terminal sessions | no |

Based on each project's documentation in October 2026. chisel and wstunnel
reconnect the WebSocket, but neither documents keeping connections that were
open when it dropped.

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
