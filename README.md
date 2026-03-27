# local-block-storage-csi

Small CSI driver that provisions local loop-backed raw block volumes for Kubernetes.

## Quick start

```bash
kubectl apply -f https://raw.githubusercontent.com/patrostkowski/local-block-storage-csi/main/deploy/manifest.yaml
```

This creates a `StorageClass` named `local-block-storage-csi`.

## Disclaimer

Not recommended for production. This project was created purely for learning purposes, fun and for testing Rook with PVCs.

## License

Apache License 2.0. Copyright 2026 Patryk Rostkowski.
