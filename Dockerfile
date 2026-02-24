# syntax=docker/dockerfile:1.7
FROM golang:1.25 AS builder
WORKDIR /src

COPY go.mod go.sum* ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 GOFLAGS=-buildvcs=false go build -trimpath -o /out/dbsp ./cmd/dbsp

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/dbsp /usr/local/bin/dbsp

ENTRYPOINT ["/usr/local/bin/dbsp"]
