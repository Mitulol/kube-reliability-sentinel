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
	go test ./... -coverprofile=cover.out
	go tool cover -func=cover.out | tail -1

image:
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) -t $(IMAGE) .

kind-up:
	kind create cluster --name $(KIND_CLUSTER) --wait 120s

kind-down:
	kind delete cluster --name $(KIND_CLUSTER)

# Build, side-load into kind (no registry needed), and apply.
deploy: image
	kind load docker-image $(IMAGE) --name $(KIND_CLUSTER)
	kubectl apply -k manifests/
	kubectl -n sentinel set image deploy/kube-reliability-sentinel sentinel=$(IMAGE)
	kubectl -n sentinel rollout status deploy/kube-reliability-sentinel --timeout=90s

chaos:
	kubectl apply -f hack/chaos-workloads.yaml

clean:
	rm -rf bin cover.out
