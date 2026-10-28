# Contributing

Bug reports and pull requests are welcome.

- For a bug, include the resumux version (`resumux version`), how client and
  server were started, and what happened. Logs help.
- For anything larger than a small fix, open an issue first so we can agree on
  the approach.
- Security issues go through [SECURITY.md](SECURITY.md), not public issues.

Before sending a pull request:

```sh
gofmt -l .                # must print nothing
go vet ./...
go test -race ./...
```

Changes to the wire format must update [docs/protocol.md](docs/protocol.md) in
the same pull request.
