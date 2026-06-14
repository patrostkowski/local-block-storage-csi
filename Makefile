IMAGE ?= docker.io/patrostkowski/local-block-storage-csi
TAG ?= latest
GIT_SHA ?= $(shell git rev-parse --short=7 HEAD)
GIT_TAG ?= $(shell git describe --tags --exact-match 2>/dev/null)
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
PLATFORMS ?= linux/amd64,linux/arm64
BUILD_ARGS = --build-arg GIT_COMMIT=$(GIT_SHA) --build-arg BUILD_TIME=$(BUILD_TIME) --build-arg GIT_TAG=$(GIT_TAG)

.PHONY: build help kind-load
build:
	docker buildx build --platform $(PLATFORMS) $(BUILD_ARGS) -t $(IMAGE):$(TAG) -t $(IMAGE):$(GIT_SHA) --push .

kind-load:
	docker buildx build --platform linux/amd64 $(BUILD_ARGS) -t $(IMAGE):$(TAG) -t $(IMAGE):$(GIT_SHA) --load .
	kind load docker-image $(IMAGE):$(TAG)

help:
	@printf "Available targets:\n"
	@printf "  build      Build and push multi-arch image\n"
	@printf "  kind-load  Build single-arch image and load into kind cluster\n"
