# Changelog

## Unreleased

First version.

- Resumable sessions: a TCP connection survives the loss of every WebSocket
  path as long as a new one connects within the resume timeout.
- Several paths per session for instant failover.
- Shared-token authentication, per-session secrets, and limits on sessions,
  paths and buffered data.
- `resumux server` and `resumux client`, with JSON config files.
- Several server URLs per client, so a session's paths can take different
  routes, such as different CDN edges or a CDN and a direct address.
