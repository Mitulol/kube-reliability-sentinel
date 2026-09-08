VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
IMAGE   ?= ghcr.io/mitulol/kube-reliability-sentinel:$(VERSION)
KIND_CLUSTER ?= sentinel-dev

LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT)

.PHONY: build test race vet cover lint image kind-up kind-down deploy chaos clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o bin/sentinel ./cmd/sentinel

test:
	go test ./...

race:
	go test ./... -race

vet:
	go vet ./...

cover:
	go test ./... -covermode=atomic -coverprofile=cover.out
	@echo
	@go test ./... -covermode=atomic 2>/dev/null | grep -E 'coverage:' || true
	@grep -v '/cmd/sentinel/' cover.out > cover.internal
	@printf 'internal packages aggregate:  %s\n' "$$(go tool cover -func=cover.internal | awk 'END{print $$NF}')"
	@printf 'whole tree aggregate:         %s\n' "$$(go tool cover -func=cover.out      | awk 'END{print $$NF}')"

image:
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) -t $(IMAGE) .

kind-up:
	kind create cluster --name $(KIND_CLUSTER) --wait 120s

kind-down:
	kind delete cluster --name $(KIND_CLUSTER)

# Build, side-load into kind (no registry needed), and apply. The image is
# built and loaded under the exact tag manifests/deployment.yaml references,
# and imagePullPolicy is IfNotPresent, so `kubectl apply -k` just works with
# no patching and nothing is pulled from a registry.
MANIFEST_IMAGE := $(shell grep -oE 'ghcr.io/mitulol/kube-reliability-sentinel:[^ ]+' manifests/base/deployment.yaml | head -1)
deploy:
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) -t $(MANIFEST_IMAGE) .
	kind load docker-image $(MANIFEST_IMAGE) --name $(KIND_CLUSTER)
	kubectl apply -k manifests/base
	kubectl -n sentinel rollout status deploy/kube-reliability-sentinel --timeout=180s

chaos:
	kubectl apply -f hack/chaos-workloads.yaml

clean:
	rm -rf bin cover.out cover.internal
