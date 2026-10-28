# Changelog

## Unreleased

First version.

- Resumable sessions: a TCP connection survives the loss of every WebSocket
  path as long as a new one connects within the resume timeout.
- Several paths per session for instant failover.
- Shared-token authentication, per-session secrets, and limits on sessions,
  paths and buffered data.
- `resumux server` and `resumux client`, with JSON config files.
