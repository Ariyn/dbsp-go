FROM golang:1.25 AS builder
WORKDIR /src

COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN go build -o /out/dbsp ./cmd/dbsp

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/dbsp /usr/local/bin/dbsp

ENTRYPOINT ["/usr/local/bin/dbsp"]
