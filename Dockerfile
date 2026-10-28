FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /resumux ./cmd/resumux

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /resumux /resumux
ENTRYPOINT ["/resumux"]
