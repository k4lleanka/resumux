# SSH over a link that keeps dropping

This runs, in one container, an SSH server, a resumux server and client, and
`flakyproxy` cutting every connection every three seconds. It then runs the
same 20-second SSH command twice: once through flakyproxy alone, and once
through resumux with flakyproxy between client and server.

From the repository root, with Go and Docker:

```sh
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o examples/sshdemo/ ./cmd/resumux ./examples/flakyproxy
docker run --rm -v "$PWD/examples/sshdemo:/d:ro" alpine:3.22 sh /d/run.sh
```

Without resumux, SSH is closed at the first cut. With resumux, all 20 lines
arrive one second apart while flakyproxy logs `cut 4 connections` every three
seconds.
