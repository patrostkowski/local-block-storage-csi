FROM golang:1.25-trixie AS builder

WORKDIR /app

COPY . .

RUN go mod download

RUN go build -o local-block-storage-csi ./cmd

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/local-block-storage-csi /usr/local/bin/local-block-storage-csi

ENTRYPOINT ["/usr/local/bin/local-block-storage-csi"]
