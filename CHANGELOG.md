# Changelog

## Unreleased

- `-ack-delay` on server and client sets how long acknowledgements wait to
  be sent together (10 ms by default). Acknowledgements only free the
  sender's buffer, so 100 ms costs no latency and cut resumux CPU by about
  12% under a game load.
- `-copy-all` makes a redundant session copy each frame to every path
  instead of a second one, so a frame is late only if every path holds it
  up. With 15% loss on the paths it cut samples over 250 ms from 7–12 to
  1–3. It only changes the side that sets it.

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
