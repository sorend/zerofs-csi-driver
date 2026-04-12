.PHONY: all build test clean docker-build docker-push install lint fmt vet release release-snapshot release-check

REGISTRY ?= ghcr.io/sorend
IMAGE_NAME ?= csi-driver-zerofs
TAG ?= latest
GITHUB_REPOSITORY ?= sorend/csi-driver-zerofs
LDFLAGS ?= -s -w -extldflags "-static"
GOOS ?= linux
GOARCH ?= $(shell go env GOARCH)
GORELEASER ?= goreleaser
GORELEASER_IMAGE ?= goreleaser/goreleaser:latest
GORELEASER_RUN = sh -ec 'if command -v "$(GORELEASER)" >/dev/null 2>&1; then exec "$(GORELEASER)" "$$@"; fi; exec docker run --rm -v "$(CURDIR):/workspace" -w /workspace -e GITHUB_REPOSITORY="$$GITHUB_REPOSITORY" -e GITHUB_TOKEN "$(GORELEASER_IMAGE)" "$$@"' --

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

release-check:
	GITHUB_REPOSITORY=$(GITHUB_REPOSITORY) $(GORELEASER_RUN) check

release-snapshot:
	GITHUB_REPOSITORY=$(GITHUB_REPOSITORY) $(GORELEASER_RUN) release --snapshot --clean --skip=publish

release:
	GITHUB_REPOSITORY=$(GITHUB_REPOSITORY) $(GORELEASER_RUN) release --clean

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
	@echo "  release-check     - Validate the GoReleaser configuration"
	@echo "  release-snapshot  - Build a local GoReleaser snapshot without publishing"
	@echo "  release           - Publish the tagged container release with GoReleaser"
	@echo "  install           - Install CSI driver to Kubernetes"
	@echo "  uninstall         - Uninstall CSI driver from Kubernetes"
