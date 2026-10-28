# resumux protocol, version 1

This document describes what goes over the wire between a resumux client and
server. The Go packages in this repository are the reference implementation.

## Overview

A **session** carries one TCP connection. It runs over one or more **paths**,
each a WebSocket connection. Either side may lose a path at any time; the
client opens new ones, and the session continues as long as at least one path
reappears within the resume timeout.

## Handshake

Every path starts as an HTTP request that upgrades to a WebSocket. The client
sends:

| Header | Value |
|---|---|
| `Authorization` | `Bearer <token>` |
| `Resumux-Version` | `1` |
| `Resumux-Session` | session id, only when joining an existing session |
| `Resumux-Secret` | session secret, only when joining an existing session |

Without `Resumux-Session` the server creates a session. It connects to its
target first, then answers `101 Switching Protocols` with:

| Header | Value |
|---|---|
| `Resumux-Session` | 32 hex characters, 128 random bits |
| `Resumux-Secret` | 64 hex characters, 256 random bits |

The server registers the session before it sends this response, so the client
may open further paths as soon as it arrives.

With `Resumux-Session` the server adds the path to that session if the secret
matches.

Errors are plain HTTP responses:

| Status | Meaning |
|---|---|
| 400 | missing or unsupported `Resumux-Version` |
| 401 | wrong or missing token |
| 404 | unknown session, or wrong secret (deliberately indistinguishable) |
| 410 | the session ended after everything was delivered in both directions |
| 429 | the session already has the maximum number of paths |
| 502 | the server could not connect to its target |
| 503 | a new session was refused: the server has the maximum number of sessions, or is shutting down |

A client that gets 404 while resuming must treat the session as lost, since
data it sent may not have arrived. The exception is a client with several
URLs, when the URL that answered has never carried a path of this session:
it may reach a different server, so its 404 proves nothing, and the client
keeps retrying it. A session that is really gone then expires.

410, which the server only answers to the correct secret, means the session
finished cleanly and nothing was lost. The server remembers finished
sessions for the resume timeout.

## Frames

After the handshake, each binary WebSocket message carries exactly one frame.
A text message, or a message larger than 1 MiB, closes the path it arrived on.
Integers are big-endian.

```
DATA  0x01 | seq (8 bytes) | payload
FIN   0x02 | seq (8 bytes)
ACK   0x03 | next (8 bytes)
```

Senders never send a DATA frame with an empty payload; receivers accept one
and treat it as already read.

Any other frame type, a frame shorter than 9 bytes, or a FIN or ACK with
trailing bytes is a protocol error and ends the session.

### Sequence numbers

Each direction has its own sequence, starting at 0. DATA and FIN frames take
the next number; ACK frames do not.

FIN means the sender will send no more data, like a TCP half-close. The other
direction keeps working.

### Acknowledgements

`ACK next` means the receiver's application has **read** every frame with a
sequence number below `next`. The sender may then forget those frames.

Acknowledging only what has been read, not merely received, is what makes the
send window double as flow control.

ACKs are cumulative and may be delayed and batched. A receiver that gets a
frame it already has re-sends its current ACK, since the sender evidently
missed the last one.

### Ordering and duplicates

The receiver hands frames to the application strictly in sequence order. A
frame ahead of the next expected one is held until the gap is filled. A frame
behind it is a duplicate and is dropped.

### Retransmission

A sender keeps every DATA and FIN frame until it is acknowledged. It sends all
of them again, in order, on a path when:

- that path becomes the one it sends on, after the previous one failed, or
- it is the first path of a session that had none, or
- on the server only: frames start arriving from the client on a different
  path than before. The client moved for a reason, so whatever was sent on
  the old path may be lost.

The client chooses the path and the server follows it. Each side fails over
to a path of its own choosing, and their choices can differ. If both followed
each other they could keep swapping paths, resending everything each time.
For the same reason the client sends nothing on a spare path until it starts
using it. A path that stops delivering only toward the client is left once
the client notices the silence, as described under Liveness.

When the server adds a path, and when the client adds the path it will send
on, that side sends an ACK with its current position on it, so the peer can
drop what is already delivered.

Sending too much is harmless, because duplicates are dropped by sequence
number. Sending too little is impossible as long as unacknowledged frames are
kept.

## Windows

Each frame is counted as its payload length plus a fixed overhead of 64 bytes,
so many tiny frames cannot use more memory than the window suggests.

- **Send window.** A sender stops accepting writes while the counted size of
  its unacknowledged frames would exceed its send window (default 4 MiB).
- **Receive window.** A receiver aborts the session if buffered frames, read or
  waiting for a gap, would exceed its receive window (default 8 MiB), or if a
  frame's sequence number is further ahead of the next expected one than the
  window could hold. The receive window must exceed the peer's send window by
  at least 64 bytes, since the FIN is sent even when the window is full; a
  peer that respects its own window then never triggers this.

## Liveness

Each side sends a WebSocket ping on a path once it has heard nothing on it
for a second by default, and treats a path as dead after 5 seconds without
receiving a complete message, pongs included. A path that is carrying
frames is not pinged, so pings never compete with traffic. CDNs and proxies
close idle WebSockets, which the pings also prevent.

The client does not wait that long when it has a choice. An idle path is
heard from about once per ping interval, so one that has been silent for two
intervals, or for one interval and half a second if that is more, has missed
an answer. When the path the client sends on has, and another path has been
heard from more recently, within twice that time, the client closes the
silent path, switches, and sends everything unacknowledged again. A path a
proxy drops without closing is thus left in about two seconds. Only frames
and pongs count as hearing from the peer, since they prove both directions
work; a ping from the peer only proves one, and a path the peer has not been
heard from on yet is never switched to.

A congested link can delay pongs behind queued data, so every path may look
silent in turn. That takes a lot of data in flight, so a switch made with
more than a quarter of the send window unacknowledged doubles the time before
the next one, up to 30 seconds, until a path has stayed healthy that long.
With less in flight, as in interactive use, each switch may follow the last
as soon as it is needed. When a path fails, the client moves to the path it
heard from most recently.

A session with no path for the resume timeout (default 2 minutes) is closed on
both sides. A session that is closing waits up to the same time for its
last data to be acknowledged, since that can take as long as a resume.

A side that closes cleanly first sends any acknowledgement it still owes,
then half-closes each path, with a TCP FIN or a TLS close_notify, and keeps
reading until the peer closes it too, for at most a second. Closing a
socket outright while the peer's data is still unread makes the kernel reset
the connection, which can destroy that last acknowledgement in flight.

## Security considerations

- The token and the session secret are sent in HTTP headers. Use `wss://`, or
  terminate TLS in front of the server; on plain `ws://` anyone on the network
  can read both.
- The token is compared in constant time over its SHA-256 hash.
- The session secret is what lets a path join a session. The id alone is not
  enough, and a wrong secret is reported exactly like an unknown id.
- Frames carry no integrity protection of their own; they rely on the
  WebSocket's TLS.
- The server bounds sessions, paths per session and buffered data per
  session, and closes sessions that lose all paths for longer than the
  resume timeout.
