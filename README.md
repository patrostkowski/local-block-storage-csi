# local-block-storage-csi

CSI driver for raw block volumes backed by local storage on Kubernetes nodes.

## Install

```bash
kubectl apply -f https://raw.githubusercontent.com/patrostkowski/local-block-storage-csi/main/deploy/manifest.yaml
```

This deploys a `StorageClass` named `local-block-storage-csi` with topology-aware provisioning and volume expansion enabled.

## Usage

Create a PVC and reference it in any pod using `volumeDevices`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-volume
spec:
  storageClassName: local-block-storage-csi
  volumeMode: Block
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
```

```yaml
containers:
- name: app
  volumeDevices:
  - name: data
    devicePath: /dev/xvda
volumes:
- name: data
  persistentVolumeClaim:
    claimName: my-volume
```

## Features

| Feature | How |
|---------|-----|
| Expand volume | `kubectl patch pvc <name> -p '{"spec":{"resources":{"requests":{"storage":"2Gi"}}}}'` |
| Clone from PVC | Set `.spec.dataSource.kind: PersistentVolumeClaim` on the new PVC |
| Clone from snapshot | Install [snapshot CRDs](https://github.com/kubernetes-csi/external-snapshotter), create a `VolumeSnapshot`, then set `.spec.dataSource.kind: VolumeSnapshot` |
| Ephemeral volumes | Use `ephemeral.volumeClaimTemplate` in the pod spec — no PVC needed |

## Examples

```bash
kubectl apply -f example/                        # 3-replica StatefulSet + standalone Deployment
kubectl apply -f example/snapshots/              # VolumeSnapshotClass + VolumeSnapshot
kubectl apply -f example/cloning/restore-pvc.yaml # Clone from existing PVC
kubectl apply -f example/cloning/restore-from-snapshot.yaml  # Restore from snapshot
kubectl apply -f example/ephemeral/              # Pod with inline ephemeral volume
kubectl apply -f example/kubevirt/               # CDI DataVolume + KubeVirt VM
```

See the `example/` directory for all manifests.


## KubeVirt + CDI

```bash
# Install KubeVirt (v1.8.3)
kubectl apply -f https://github.com/kubevirt/kubevirt/releases/download/v1.8.3/kubevirt-operator.yaml
kubectl apply -f https://github.com/kubevirt/kubevirt/releases/download/v1.8.3/kubevirt-cr.yaml
# Install CDI (v1.62.0)
kubectl apply -f https://github.com/kubevirt/containerized-data-importer/releases/download/v1.62.0/cdi-operator.yaml
kubectl apply -f https://github.com/kubevirt/containerized-data-importer/releases/download/v1.62.0/cdi-cr.yaml
# Deploy a VM backed by this driver
kubectl apply -f example/kubevirt/
```
## Build

```bash
make build      # Multi-arch push (linux/amd64 + linux/arm64)
make kind-load  # Single-arch load into kind cluster
```

## Disclaimer

Not recommended for production. Created for learning and testing Rook with PVCs.

## License

Apache License 2.0. Copyright 2026 Patryk Rostkowski.
