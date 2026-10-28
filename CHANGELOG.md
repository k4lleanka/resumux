# Changelog

## Unreleased

First version.

- Resumable sessions: a TCP connection survives the loss of every WebSocket
  path as long as a new one connects within the resume timeout.
- Several paths per session for instant failover.
- Shared-token authentication, per-session secrets, and limits on sessions,
  paths and buffered data.
- `resumux server` and `resumux client`, with JSON config files.
- A session whose handshake response never reached the client is closed
  after 10 seconds, so it does not hold a target connection for the whole
  resume timeout.
