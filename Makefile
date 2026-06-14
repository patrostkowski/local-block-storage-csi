IMAGE ?= docker.io/patrostkowski/local-block-storage-csi
TAG ?= latest
GIT_SHA ?= $(shell git rev-parse --short=7 HEAD)
PLATFORMS ?= linux/amd64,linux/arm64

.PHONY: build help kind-load
build:
	docker buildx build --platform $(PLATFORMS) -t $(IMAGE):$(TAG) -t $(IMAGE):$(GIT_SHA) --push .

kind-load:
	docker buildx build --platform linux/amd64 -t $(IMAGE):$(TAG) -t $(IMAGE):$(GIT_SHA) --load .
	kind load docker-image $(IMAGE):$(TAG)

help:
	@printf "Available targets:\n"
	@printf "  build      Build and push multi-arch image\n"
	@printf "  kind-load  Build single-arch image and load into kind cluster\n"
