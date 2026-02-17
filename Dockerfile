FROM golang:1.23-alpine AS builder

WORKDIR /build

RUN apk add --no-cache git make

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w -extldflags '-static'" -o /zerofs-csi-driver ./cmd/zerofs-csi-driver

FROM alpine:3.20

RUN apk add --no-cache ca-certificates nfs-utils e2fsprogs xfsprogs util-linux

COPY --from=builder /zerofs-csi-driver /usr/local/bin/zerofs-csi-driver
COPY zerofs-linux-amd64-pgo /usr/local/bin/zerofs-linux-amd64
COPY zerofs-linux-arm64-pgo /usr/local/bin/zerofs-linux-arm64

RUN chmod +x /usr/local/bin/zerofs-csi-driver /usr/local/bin/zerofs-linux-* && \
    ln -sf /usr/local/bin/zerofs-linux-$(uname -m | sed 's/x86_64/amd64/' | sed 's/aarch64/arm64/') /usr/local/bin/zerofs

ENTRYPOINT ["/usr/local/bin/zerofs-csi-driver"]
