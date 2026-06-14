FROM golang:1.26.4-trixie AS builder

WORKDIR /app

COPY . .

RUN go mod download

ARG GIT_COMMIT=unknown
ARG BUILD_TIME=unknown
ARG GIT_TAG=""

RUN go build -ldflags "\
  -X github.com/patrostkowski/local-block-storage-csi/internal.GitCommit=${GIT_COMMIT} \
  -X github.com/patrostkowski/local-block-storage-csi/internal.BuildTime=${BUILD_TIME} \
  -X github.com/patrostkowski/local-block-storage-csi/internal.GitTag=${GIT_TAG}" \
  -o local-block-storage-csi ./cmd

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/local-block-storage-csi /usr/local/bin/local-block-storage-csi

ENTRYPOINT ["/usr/local/bin/local-block-storage-csi"]
