# resumux

Forward TCP over WebSockets, and keep the connection alive when the network
underneath it drops.

A TCP connection through resumux survives a dead WebSocket, a network switch
(Wi-Fi to mobile), a new IP address, or a CDN restarting its servers. The
application on either end sees a pause, not a disconnect.

It is useful for anything long-lived over TCP behind a WebSocket-friendly proxy
or CDN: SSH, game servers, databases, internal tools.

## How it works

```
app ──TCP──▶ resumux client ══ WebSocket paths ══▶ resumux server ──TCP──▶ target
```

Each TCP connection becomes a session. A session runs over one or more
WebSocket paths at once.

- Every frame carries a sequence number, in both directions.
- The sender keeps each frame until the other side acknowledges it.
- The receiver puts frames back in order and drops duplicates.
- When a path dies, the client dials a new one, and everything not yet
  acknowledged is sent again on it.

So a path can fail at any moment without the stream losing, repeating or
reordering a byte. Acknowledgements only count data the application has read,
which also gives flow control: a slow reader stops the sender instead of
filling memory.

## Quick start

On the server, next to the service you want to reach:

```sh
openssl rand -hex 32 > token
resumux server -listen :8080 -target 127.0.0.1:22 -token-file token
```

On the client, with a copy of the same `token` file:

```sh
resumux client -listen 127.0.0.1:2222 -server wss://example.com/ -token-file token
ssh -p 2222 user@127.0.0.1
```

Put the server behind TLS, either with `-tls-cert` and `-tls-key` or behind a
reverse proxy or CDN that terminates TLS, and use a `wss://` URL. The token and
the session secret travel in HTTP headers.

Settings can also go in a JSON file, using the flag names as keys:

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
through the tunnel, for example SSH:

```sh
go build ./cmd/resumux && go build ./examples/flakyproxy
resumux server -listen 127.0.0.1:8080 -target 127.0.0.1:22 -token-file token
flakyproxy -listen 127.0.0.1:9000 -upstream 127.0.0.1:8080 -cut-every 3s
resumux client -listen 127.0.0.1:2222 -server ws://127.0.0.1:9000/ -token-file token
ssh -p 2222 user@127.0.0.1
```

flakyproxy logs `cut 4 connections` every three seconds, which takes down
every path of the session at once, and the SSH session carries on. Put
flakyproxy straight in front of port 22 instead, without resumux, and SSH drops
at the first cut.

## Security

- Every connection to the server must present the shared token
  (`Authorization: Bearer`). The token is read from a file or the
  `RESUMUX_TOKEN` environment variable, never from the command line.
- A new session gets a random 128-bit id and a 256-bit secret. A path can only
  join a session with both. An unknown id and a wrong secret get the same
  answer.
- The server limits sessions (`-max-sessions`) and paths per session
  (`-max-paths`), and closes a session that has had no path for
  `-resume-timeout`.

## Limitations

- TCP only. UDP traffic, used by many games and by voice chat, is not carried.
- Paths are used for failover, not combined for more bandwidth.
- Sessions live in the server's memory. Restarting the server ends them.
- One server forwards to one target.
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

## Building

```sh
go build ./cmd/resumux
go test -race ./...
```

The tests include a transfer through the whole chain while WebSocket
connections are cut every few milliseconds; every byte has to arrive once and
in order.

## License

Apache-2.0. See [LICENSE](LICENSE).
