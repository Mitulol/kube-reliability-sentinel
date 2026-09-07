# Build a static binary, ship it on distroless nonroot. No shell, no libc,
# no package manager in the final image: nothing for a compromised watcher
# to pivot with.
FROM golang:1.27 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
ARG COMMIT=none
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
    -o /out/sentinel ./cmd/sentinel

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/sentinel /usr/local/bin/sentinel
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/sentinel"]
