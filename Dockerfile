FROM golang:1.23-alpine AS builder

WORKDIR /build

RUN apk add --no-cache git make

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w -extldflags '-static'" -o /csi-driver-zerofs ./cmd/csi-driver-zerofs

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    nfs-common \
    e2fsprogs \
    xfsprogs \
    util-linux \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /csi-driver-zerofs /usr/local/bin/csi-driver-zerofs
RUN chmod +x /usr/local/bin/csi-driver-zerofs

ENTRYPOINT ["/usr/local/bin/csi-driver-zerofs"]
