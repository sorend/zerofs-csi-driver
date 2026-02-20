
# WORK IN PROGRESS

Totally vibe coded, not for any kind of production alike use, probably not even for test use.

# ZeroFS CSI Driver

A Container Storage Interface (CSI) driver for Kubernetes that provides persistent storage backed by object storage (S3-compatible). ZeroFS CSI Driver enables dynamic provisioning of volumes that can be mounted via NFS (ReadWriteMany) or 9P (ReadWriteOnce) protocols.

## Features

- **Dynamic Volume Provisioning** - Automatically creates and manages persistent volumes
- **Multiple Protocols** - NFS for ReadWriteMany (RWX) workloads, 9P for ReadWriteOnce (RWO)
- **Object Storage Backend** - Uses S3-compatible storage as the backing store
- **Encryption** - Built-in encryption support for data at rest
- **Caching** - Configurable disk and memory caching for performance
- **Volume Expansion** - Support for online volume resizing
- **Multi-Architecture** - Supports linux/amd64 and linux/arm64

## Requirements

- Kubernetes 1.24+
- S3-compatible object storage (AWS S3, MinIO, etc.)
- Nodes with NFS client utilities (for NFS protocol)
- Nodes with 9P kernel support (for 9P protocol)

## Installation

### Quick Install

```bash
kubectl apply -f https://raw.githubusercontent.com/zerofs/csi-driver-zerofs/main/deploy/install.yaml
```

### From Source

```bash
# Clone the repository
git clone https://github.com/zerofs/csi-driver-zerofs.git
cd csi-driver-zerofs

# Build and install
make build
make install
```

### Prerequisites

1. **Set up Object Storage**

   Deploy MinIO or configure your S3-compatible storage:

   ```yaml
   apiVersion: apps/v1
   kind: Deployment
   metadata:
     name: minio
     namespace: zerofs-csi
   spec:
     replicas: 1
     selector:
       matchLabels:
         app: minio
     template:
       metadata:
         labels:
           app: minio
       spec:
         containers:
           - name: minio
             image: minio/minio:latest
             args: ["server", "/data"]
             env:
               - name: MINIO_ROOT_USER
                 value: "minioadmin"
               - name: MINIO_ROOT_PASSWORD
                 value: "minioadmin123"
             ports:
               - containerPort: 9000
   ---
   apiVersion: v1
   kind: Service
   metadata:
     name: minio
     namespace: zerofs-csi
   spec:
     selector:
       app: minio
     ports:
       - port: 9000
         targetPort: 9000
   ```

2. **Create the bucket**

   ```bash
   kubectl exec -n zerofs-csi deployment/minio -- \
     mc alias set local http://localhost:9000 minioadmin minioadmin123 && \
     mc mb local/zerofs-data
   ```

## Usage

### Create Credentials Secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: zerofs-aws-credentials
  namespace: zerofs-csi
type: Opaque
stringData:
  awsAccessKeyID: "YOUR_ACCESS_KEY_ID"
  awsSecretAccessKey: "YOUR_SECRET_ACCESS_KEY"
```

### Create a StorageClass

#### NFS StorageClass (ReadWriteMany)

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: zerofs-nfs
provisioner: zerofs.csi.sorend.github.com
parameters:
  storageUrl: "s3://your-bucket/zerofs-data"
  awsSecretName: "zerofs-aws-credentials"
  awsEndpoint: "http://minio.zerofs-csi.svc.cluster.local:9000"
  awsAllowHTTP: "true"
reclaimPolicy: Delete
volumeBindingMode: Immediate
allowVolumeExpansion: true
mountOptions:
  - vers=4.1
  - noatime
```

#### 9P StorageClass (ReadWriteOnce)

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: zerofs-ninep
provisioner: zerofs.csi.sorend.github.com
parameters:
  storageUrl: "s3://your-bucket/zerofs-data"
  awsSecretName: "zerofs-aws-credentials"
  awsEndpoint: "http://minio.zerofs-csi.svc.cluster.local:9000"
  awsAllowHTTP: "true"
  protocol: "ninep"
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
mountOptions:
  - trans=tcp
  - version=9p2000.L
  - msize=65536
```

### Create a PersistentVolumeClaim

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: zerofs-pvc
  annotations:
    zerofs.csi.sorend.github.com/storage-url: "s3://per-pvc-bucket/zerofs-data"
    zerofs.csi.sorend.github.com/aws-secret-name: "zerofs-aws-credentials"
    zerofs.csi.sorend.github.com/aws-endpoint: "http://minio.zerofs-csi.svc.cluster.local:9000"
    zerofs.csi.sorend.github.com/aws-allow-http: "true"
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: zerofs-nfs
  resources:
    requests:
      storage: 10Gi
```

### Use in a Pod

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: my-app
spec:
  containers:
    - name: app
      image: nginx:alpine
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: zerofs-pvc
```

## Configuration Parameters

### StorageClass Parameters

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| `storageUrl` | S3 URL for backing storage (e.g., `s3://bucket/path`) | - | Yes |
| `awsSecretName` | Kubernetes secret containing AWS credentials | - | Yes* |
| `awsAccessKeyID` | AWS access key ID (ignored; use secret) | - | No |
| `awsSecretAccessKey` | AWS secret access key (ignored; use secret) | - | No |
| `awsEndpoint` | Custom S3 endpoint URL | - | No |
| `awsAllowHTTP` | Allow HTTP connections (for non-HTTPS endpoints) | `true` | No |
| `protocol` | Mount protocol: `nfs` or `ninep` | `nfs` | No |
| `encryptionPassword` | Password for data encryption | `default-zerofs-encryption-key` | No |
| `cacheDir` | Directory for local cache | `/var/lib/zerofs/cache` | No |
| `cacheSizeGB` | Maximum cache size in GB | `10` | No |

*Prefer `awsSecretName` for credentials. Direct `awsAccessKeyID`/`awsSecretAccessKey` parameters are ignored by the driver.

### PVC Annotation Overrides

PVCs can override select StorageClass parameters using the same `zerofs.csi.sorend.github.com/` prefix. Only `awsSecretName` is allowed for credentials; raw access keys are ignored.

| PVC Annotation | Maps to Parameter | Notes |
|---------------|-------------------|-------|
| `zerofs.csi.sorend.github.com/storage-url` | `storageUrl` | Required when overriding bucket/path |
| `zerofs.csi.sorend.github.com/aws-secret-name` | `awsSecretName` | Use a Secret for credentials |
| `zerofs.csi.sorend.github.com/aws-endpoint` | `awsEndpoint` | Optional |
| `zerofs.csi.sorend.github.com/aws-allow-http` | `awsAllowHTTP` | Optional, defaults to `true` |

The driver stores the effective S3 parameters on the ZeroFS deployment and secret annotations for debugging.

### CLI Flags

#### Controller Service

```bash
csi-driver-zerofs controller [flags]
```

| Flag | Description | Default |
|------|-------------|---------|
| `--driver-name` | CSI driver name | `zerofs.csi.sorend.github.com` |
| `--endpoint` | CSI endpoint | `unix:///csi/csi.sock` |
| `--namespace` | Namespace to run in | `default` |
| `--kubeconfig` | Path to kubeconfig file | (in-cluster) |
| `--work-dir` | Working directory | `/var/lib/zerofs-csi` |
| `--zerofs-image` | ZeroFS server container image | `ghcr.io/barre/zerofs:1.0.4` |

#### Node Service

```bash
csi-driver-zerofs node [flags]
```

| Flag | Description | Default |
|------|-------------|---------|
| `--driver-name` | CSI driver name | `zerofs.csi.sorend.github.com` |
| `--node-id` | Node ID | (hostname) |
| `--endpoint` | CSI endpoint | `unix:///csi/csi.sock` |

## Protocol Comparison

| Feature | NFS | 9P |
|---------|-----|-----|
| Access Mode | ReadWriteMany (RWX) | ReadWriteOnce (RWO) |
| Volume Binding | Immediate | WaitForFirstConsumer |
| Performance | Better for concurrent access | Lower latency for single node |
| Requirements | NFS client utilities | 9P kernel support |
| Use Case | Shared workloads, CI/CD | Databases, single-pod apps |

## Building

```bash
# Build binary
make build

# Build for all platforms
make build-all

# Run tests
make test

# Build Docker image
make docker-build

# Build multi-arch Docker image
make docker-build-all

# Build and push to registry
make docker-build-push REGISTRY=your-registry TAG=v1.0.0
```

## Development

### Project Structure

```
.
├── cmd/csi-driver-zerofs/    # Main entrypoint
├── pkg/
│   ├── driver/               # CSI driver implementation
│   │   ├── controller.go     # Controller server
│   │   ├── node.go           # Node server
│   │   └── identity.go       # Identity server
│   └── zerofs/               # ZeroFS management
│       ├── manager.go        # Deployment manager
│       └── server.go         # ZeroFS server
├── deploy/
│   ├── install.yaml          # Installation manifests
│   └── examples.yaml         # Example resources
├── Dockerfile
├── Makefile
└── go.mod
```

### Running Tests

```bash
make test

# With coverage report
make test-coverage
```

### Code Quality

```bash
make fmt     # Format code
make vet     # Run go vet
make lint    # Run golangci-lint
```

## Uninstallation

```bash
kubectl delete -f deploy/install.yaml
```

Or using make:

```bash
make uninstall
```

## Troubleshooting

### Check CSI Driver Logs

```bash
# Controller logs
kubectl logs -n zerofs-csi deployment/zerofs-csi-controller -c zerofs-plugin

# Node logs
kubectl logs -n zerofs-csi daemonset/zerofs-csi-node -c zerofs-plugin
```

### Common Issues

1. **Volume stuck in pending state**
   - Check if the CSI controller pod is running
   - Verify S3 credentials are correct
   - Ensure the S3 bucket exists

2. **Mount failures on node**
   - Verify NFS client utilities are installed on nodes
   - Check node can reach the ZeroFS server pods
   - Review mount options in StorageClass

3. **9P volumes not scheduling**
   - Ensure `volumeBindingMode: WaitForFirstConsumer` is set
   - Verify nodes have 9P kernel support (`CONFIG_9P_FS`)

## License

Apache License 2.0

## Contributing

Contributions are welcome! Please read our contributing guidelines and submit pull requests to the main repository.
