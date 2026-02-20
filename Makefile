.PHONY: all build test clean docker-build docker-push install lint fmt vet

REGISTRY ?= ghcr.io/sorend
IMAGE_NAME ?= csi-driver-zerofs
TAG ?= latest
LDFLAGS ?= -s -w -extldflags "-static"
GOOS ?= linux
GOARCH ?= $(shell go env GOARCH)

all: build

build:
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -ldflags "$(LDFLAGS)" -o bin/csi-driver-zerofs ./cmd/csi-driver-zerofs

build-linux-amd64:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o bin/csi-driver-zerofs-linux-amd64 ./cmd/csi-driver-zerofs

build-linux-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o bin/csi-driver-zerofs-linux-arm64 ./cmd/csi-driver-zerofs

build-all: build-linux-amd64 build-linux-arm64

test:
	go test -v -race -coverprofile=coverage.out ./...

test-coverage: test
	go tool cover -html=coverage.out -o coverage.html

lint:
	golangci-lint run ./...

fmt:
	go fmt ./...

vet:
	go vet ./...

clean:
	rm -rf bin/
	rm -f coverage.out coverage.html

docker-build:
	docker build -t $(REGISTRY)/$(IMAGE_NAME):$(TAG) .

docker-build-all:
	docker buildx build --platform linux/amd64,linux/arm64 -t $(REGISTRY)/$(IMAGE_NAME):$(TAG) .

docker-push:
	docker push $(REGISTRY)/$(IMAGE_NAME):$(TAG)

docker-build-push:
	docker buildx build --platform linux/amd64,linux/arm64 -t $(REGISTRY)/$(IMAGE_NAME):$(TAG) --push .

install:
	kubectl apply -f deploy/install.yaml

uninstall:
	kubectl delete -f deploy/install.yaml --ignore-not-found

help:
	@echo "Available targets:"
	@echo "  build             - Build the CSI driver binary"
	@echo "  build-all         - Build binaries for linux/amd64 and linux/arm64"
	@echo "  test              - Run unit tests"
	@echo "  test-coverage     - Run tests with coverage report"
	@echo "  lint              - Run golangci-lint"
	@echo "  fmt               - Format Go code"
	@echo "  vet               - Run go vet"
	@echo "  clean             - Clean build artifacts"
	@echo "  docker-build      - Build Docker image for current arch"
	@echo "  docker-build-all  - Build multi-arch Docker images (amd64, arm64)"
	@echo "  docker-push       - Push Docker image to registry"
	@echo "  docker-build-push - Build and push multi-arch Docker image"
	@echo "  install           - Install CSI driver to Kubernetes"
	@echo "  uninstall         - Uninstall CSI driver from Kubernetes"
