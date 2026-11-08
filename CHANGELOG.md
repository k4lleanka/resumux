# Changelog

## v0.2.0 (2026-11-08)

Security fixes, found in a review. Each needed only the token, which ships
with a public client:

- A client alternating paths made the server send its whole send window
  again on every move, and keep a copy of it per move until the write
  finished, so one session could exhaust the server's memory. The server
  now follows the client to another path at most once a second, and a path
  has at most one such resend waiting.
- A client that answered pings but never closed its side kept its session
  forever after the target hung up, so a few dozen addresses could fill
  `-max-sessions`. The server now ends such a session once the resume
  timeout and then `-half-open-timeout` (one minute by default) have
  passed, so a client in an outage still gets the last of the data.
- A path accepted messages of up to 1 MiB, which the windows do not count
  while they arrive, so the memory bound in the docs was about four times
  too low. The protocol limit is now 64 KiB, and
  [deployment](docs/deployment.md#memory-per-session) gives the corrected
  bound.
- `-max-sessions-per-network` caps the sessions of a whole IPv6 /48, which
  holds 65,536 /64s that `-max-sessions-per-client` would each count apart.

- `-ack-delay` on server and client sets how long acknowledgements wait to
  be sent together (10 ms by default). Acknowledgements only free the
  sender's buffer, so 100 ms costs no latency and cut resumux CPU by about
  12% under a game load.
- `-copy-all` makes a redundant session copy each frame to every path
  instead of a second one, so a frame is late only if every path holds it
  up. With 15% loss on the paths it cut samples over 250 ms from 7–12 to
  1–3. It only changes the side that sets it.
- A redundant session copies acknowledgements to one other path, not all of
  them, and paths move their deadlines less often. With `-copy-all` and
  `-ack-delay 250ms` on both sides, 500 game sessions on 2 CPUs went from
  1.25 to 1.0 cores, with the same latency.
- `-send-window` and `-recv-window` on server and client set how much one
  session may buffer, as a size such as `512KiB`. The defaults stay 4 MiB and 8 MiB,
  but a server for low-rate clients, such as a game, can bound what a hostile
  client makes it hold: with `-recv-window 512KiB -send-window 1MiB`, 40
  sessions trying to hold 7 MiB each were ended at 512 KiB and the server
  stayed under 50 MB, where the defaults let it grow to 485 MB.

## v0.1.0 (2026-10-29)

First version.

- Resumable sessions: a TCP connection survives the loss of every WebSocket
  path as long as a new one connects within the resume timeout.
- Several paths per session for instant failover.
- Shared-token authentication, per-session secrets, and limits on sessions,
  paths and buffered data.
- `resumux server` and `resumux client`, with JSON config files.
- Several server URLs per client, so a session's paths can take different
  routes, such as different CDN edges or a CDN and a direct address.
- A clean close half-closes each path and waits briefly for the peer, so the
  last acknowledgement is not lost to a connection reset.
- `-proxy-protocol` tells the target each client's address with a PROXY
  protocol v2 header. A proxy's client-address header is used only on
  requests from `-trusted-proxies`.
- `flakyproxy -freeze` silences a connection without closing it.
- `-max-sessions-per-client` and `-max-new-sessions-per-client` stop one
  address from taking every session, which matters when the token ships
  with a public client. They never apply to resuming a session.
- A refused handshake reports the server's reason, such as which limit was
  hit.
- `flakyproxy -delay -jitter -hiccup-every` imitates a long, lossy route.
- `-redundant` on the client copies frames to a second path while at most
  64 KiB is unacknowledged, so a lost or stalled packet does not freeze an
  interactive stream. It is negotiated with the `Resumux-Redundant` header;
  `-refuse-redundant` lets a server decline it.
- `-listen` on the server takes several addresses, such as `:443,:8443`.
