#!/usr/bin/env bats
#
# Integration tests for zerofs-csi-driver
#
# Prerequisites:
#   - kind cluster "zerofs-integration" running with zerofs-csi-driver deployed
#   - MinIO deployed in the zerofs-csi namespace
#   - zerofs-data bucket created in MinIO
#
# Setup (run once before tests):
#   kind create cluster --name zerofs-integration --config test/kind-config.yaml
#   docker build -t ghcr.io/sorend/csi-driver-zerofs:latest .
#   kind load docker-image ghcr.io/sorend/csi-driver-zerofs:latest --name zerofs-integration
#   docker exec zerofs-integration-control-plane ctr --namespace=k8s.io images pull ghcr.io/barre/zerofs:1.0.4
#   kubectl --context kind-zerofs-integration apply -f deploy/install.yaml
#   # patch imagePullPolicy to Never for local image
#   kubectl --context kind-zerofs-integration patch deployment zerofs-csi-controller -n zerofs-csi \
#     --type=json -p='[{"op":"replace","path":"/spec/template/spec/containers/2/imagePullPolicy","value":"Never"}]'
#   kubectl --context kind-zerofs-integration patch daemonset zerofs-csi-node -n zerofs-csi \
#     --type=json -p='[{"op":"replace","path":"/spec/template/spec/containers/2/imagePullPolicy","value":"Never"}]'
#   # Deploy MinIO
#   kubectl --context kind-zerofs-integration apply -f test/minio.yaml
#   # Create bucket
#   kubectl --context kind-zerofs-integration run minio-init \
#     --image=quay.io/minio/mc:latest --restart=Never -n zerofs-csi \
#     --command -- /bin/sh -c "mc alias set local http://minio.zerofs-csi.svc.cluster.local:9000 minioadmin minioadmin123 && mc mb --ignore-existing local/zerofs-data"
#
# Teardown (run once after all tests):
#   kind delete cluster --name zerofs-integration
#
# Usage:
#   bats test/integration-test.bats

KUBE_CONTEXT="kind-zerofs-integration"
KUBECTL="kubectl --context ${KUBE_CONTEXT}"
TEST_NAMESPACE="zerofs-test"
NFS_PVC="zerofs-nfs-pvc"
NINEP_PVC="zerofs-ninep-pvc"
NFS_POD="zerofs-nfs-test-pod"
NINEP_POD="zerofs-ninep-test-pod"

# Timeout values (seconds)
PVC_BIND_TIMEOUT=180
POD_READY_TIMEOUT=300
POD_DELETE_TIMEOUT=120
PVC_DELETE_TIMEOUT=120

setup_file() {
    # Create test namespace
    ${KUBECTL} create namespace ${TEST_NAMESPACE} --dry-run=client -o yaml | ${KUBECTL} apply -f -
}

teardown_file() {
    # Clean up test namespace (best-effort)
    ${KUBECTL} delete namespace ${TEST_NAMESPACE} --ignore-not-found --timeout=120s || true
}

# Helper: wait for PVC to be Bound
wait_for_pvc_bound() {
    local ns="$1"
    local pvc="$2"
    local timeout="${3:-${PVC_BIND_TIMEOUT}}"
    local elapsed=0
    local interval=5
    while [ $elapsed -lt $timeout ]; do
        local phase
        phase=$(${KUBECTL} get pvc "${pvc}" -n "${ns}" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
        if [ "${phase}" = "Bound" ]; then
            return 0
        fi
        sleep $interval
        elapsed=$((elapsed + interval))
    done
    echo "PVC ${pvc} not Bound after ${timeout}s (phase: $(${KUBECTL} get pvc "${pvc}" -n "${ns}" -o jsonpath='{.status.phase}' 2>/dev/null))"
    return 1
}

# Helper: wait for pod to be Running
wait_for_pod_running() {
    local ns="$1"
    local pod="$2"
    local timeout="${3:-${POD_READY_TIMEOUT}}"
    ${KUBECTL} wait pod "${pod}" -n "${ns}" \
        --for=condition=ready \
        --timeout="${timeout}s" 2>&1
}

# Helper: wait for pod to be deleted
wait_for_pod_deleted() {
    local ns="$1"
    local pod="$2"
    local timeout="${3:-${POD_DELETE_TIMEOUT}}"
    ${KUBECTL} wait pod "${pod}" -n "${ns}" \
        --for=delete \
        --timeout="${timeout}s" 2>&1 || true
}

# Helper: dump debug info on failure
dump_debug() {
    local ns="$1"
    echo "=== Pods in ${ns} ==="
    ${KUBECTL} get pods -n "${ns}" -o wide 2>&1 || true
    echo "=== Pods in zerofs-csi ==="
    ${KUBECTL} get pods -n zerofs-csi -o wide 2>&1 || true
    echo "=== PVCs in ${ns} ==="
    ${KUBECTL} get pvc -n "${ns}" 2>&1 || true
    echo "=== PVs ==="
    ${KUBECTL} get pv 2>&1 || true
    echo "=== Deployments in zerofs-csi ==="
    ${KUBECTL} get deployments -n zerofs-csi 2>&1 || true
    echo "=== CSI controller logs (last 50 lines) ==="
    ${KUBECTL} logs -n zerofs-csi deployment/zerofs-csi-controller -c zerofs-plugin --tail=50 2>&1 || true
    echo "=== CSI node logs (last 50 lines) ==="
    ${KUBECTL} logs -n zerofs-csi daemonset/zerofs-csi-node -c zerofs-plugin --tail=50 2>&1 || true
}

# ============================================================================
# Test: CSI driver infrastructure is running
# ============================================================================

@test "CSI controller deployment is available" {
    run ${KUBECTL} get deployment zerofs-csi-controller -n zerofs-csi -o jsonpath='{.status.availableReplicas}'
    [ "$status" -eq 0 ]
    [ "$output" = "1" ]
}

@test "CSI node daemonset is ready" {
    run ${KUBECTL} get daemonset zerofs-csi-node -n zerofs-csi -o jsonpath='{.status.numberReady}'
    [ "$status" -eq 0 ]
    [ "$output" -ge 1 ]
}

@test "CSI driver is registered" {
    run ${KUBECTL} get csidriver zerofs.csi.sorend.github.com -o jsonpath='{.metadata.name}'
    [ "$status" -eq 0 ]
    [ "$output" = "zerofs.csi.sorend.github.com" ]
}

@test "NFS StorageClass exists" {
    run ${KUBECTL} get storageclass zerofs -o jsonpath='{.provisioner}'
    [ "$status" -eq 0 ]
    [ "$output" = "zerofs.csi.sorend.github.com" ]
}

@test "9P StorageClass exists" {
    run ${KUBECTL} get storageclass zerofs-ninep -o jsonpath='{.provisioner}'
    [ "$status" -eq 0 ]
    [ "$output" = "zerofs.csi.sorend.github.com" ]
}

@test "MinIO is running and accessible" {
    run ${KUBECTL} get deployment minio -n zerofs-csi -o jsonpath='{.status.availableReplicas}'
    [ "$status" -eq 0 ]
    [ "$output" = "1" ]
}

# ============================================================================
# Test: NFS PVC lifecycle - create, write, read, delete
# ============================================================================

@test "NFS: PVC can be created" {
    ${KUBECTL} apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${NFS_PVC}
  namespace: ${TEST_NAMESPACE}
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: zerofs
  resources:
    requests:
      storage: 1Gi
EOF
    run ${KUBECTL} get pvc ${NFS_PVC} -n ${TEST_NAMESPACE} -o jsonpath='{.metadata.name}'
    [ "$status" -eq 0 ]
    [ "$output" = "${NFS_PVC}" ]
}

@test "NFS: PVC becomes Bound" {
    # zerofs NFS uses Immediate binding mode, so PVC should bind without a pod
    run wait_for_pvc_bound "${TEST_NAMESPACE}" "${NFS_PVC}" "${PVC_BIND_TIMEOUT}"
    if [ "$status" -ne 0 ]; then
        dump_debug "${TEST_NAMESPACE}"
    fi
    [ "$status" -eq 0 ]
}

@test "NFS: per-volume zerofs deployment is created in zerofs-csi namespace" {
    # Get the volume name from PVC
    local pv_name
    pv_name=$(${KUBECTL} get pvc ${NFS_PVC} -n ${TEST_NAMESPACE} -o jsonpath='{.spec.volumeName}')
    [ -n "$pv_name" ]
    # The zerofs deployment is named zerofs-<volumeID>
    run ${KUBECTL} get deployment -n zerofs-csi -l "zerofs.csi.sorend.github.com/volume-id" --no-headers 2>/dev/null
    # Fallback: list all deployments and check for one that's not the CSI controller
    run ${KUBECTL} get deployments -n zerofs-csi --no-headers
    [ "$status" -eq 0 ]
    # Should have at least: zerofs-csi-controller + minio + the volume deployment
    local count
    count=$(echo "$output" | grep -v "zerofs-csi-controller\|minio" | grep -c "zerofs-" || true)
    [ "$count" -ge 1 ]
}

@test "NFS: pod can mount the PVC and write data" {
    ${KUBECTL} apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${NFS_POD}
  namespace: ${TEST_NAMESPACE}
spec:
  containers:
    - name: app
      image: busybox:latest
      command:
        - sh
        - -c
        - |
          echo "hello from zerofs nfs" > /data/test.txt
          echo "written: $(date)" >> /data/test.txt
          echo "write complete"
          sleep 3600
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${NFS_PVC}
EOF

    run wait_for_pod_running "${TEST_NAMESPACE}" "${NFS_POD}" "${POD_READY_TIMEOUT}"
    if [ "$status" -ne 0 ]; then
        dump_debug "${TEST_NAMESPACE}"
    fi
    [ "$status" -eq 0 ]
}

@test "NFS: data written to PVC can be read back" {
    run ${KUBECTL} exec -n ${TEST_NAMESPACE} ${NFS_POD} -- cat /data/test.txt
    [ "$status" -eq 0 ]
    [[ "$output" =~ "hello from zerofs nfs" ]]
}

@test "NFS: a second pod can read data from the same RWX PVC" {
    # NFS supports ReadWriteMany - a second pod on the same PVC should be able to read
    ${KUBECTL} apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${NFS_POD}-reader
  namespace: ${TEST_NAMESPACE}
spec:
  containers:
    - name: reader
      image: busybox:latest
      command:
        - sh
        - -c
        - sleep 3600
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${NFS_PVC}
EOF

    run wait_for_pod_running "${TEST_NAMESPACE}" "${NFS_POD}-reader" "${POD_READY_TIMEOUT}"
    if [ "$status" -ne 0 ]; then
        dump_debug "${TEST_NAMESPACE}"
    fi
    [ "$status" -eq 0 ]

    run ${KUBECTL} exec -n ${TEST_NAMESPACE} ${NFS_POD}-reader -- cat /data/test.txt
    [ "$status" -eq 0 ]
    [[ "$output" =~ "hello from zerofs nfs" ]]
}

@test "NFS: pods and PVC can be deleted" {
    # Delete pods first
    ${KUBECTL} delete pod ${NFS_POD} ${NFS_POD}-reader -n ${TEST_NAMESPACE} --ignore-not-found --timeout=60s
    wait_for_pod_deleted "${TEST_NAMESPACE}" "${NFS_POD}"
    wait_for_pod_deleted "${TEST_NAMESPACE}" "${NFS_POD}-reader"

    # Delete PVC
    run ${KUBECTL} delete pvc ${NFS_PVC} -n ${TEST_NAMESPACE} --timeout=60s
    [ "$status" -eq 0 ]

    # Verify PVC is gone
    sleep 5
    run ${KUBECTL} get pvc ${NFS_PVC} -n ${TEST_NAMESPACE} 2>&1
    [[ "$output" =~ "not found" ]]
}

@test "NFS: per-volume zerofs deployment is cleaned up after PVC deletion" {
    # Wait for zerofs deployment cleanup (DeleteVolume is called after PVC deletion)
    local timeout=120
    local elapsed=0
    local interval=5
    while [ $elapsed -lt $timeout ]; do
        local count
        count=$(${KUBECTL} get deployments -n zerofs-csi --no-headers 2>/dev/null | \
            grep -v "zerofs-csi-controller\|minio" | grep -c "zerofs-" || true)
        if [ "$count" -eq 0 ]; then
            break
        fi
        sleep $interval
        elapsed=$((elapsed + interval))
    done

    local remaining
    remaining=$(${KUBECTL} get deployments -n zerofs-csi --no-headers 2>/dev/null | \
        grep -v "zerofs-csi-controller\|minio" | grep "zerofs-" || true)
    [ -z "$remaining" ]
}

# ============================================================================
# Test: 9P PVC lifecycle - create, write, read, delete
# ============================================================================

@test "9P: PVC can be created" {
    ${KUBECTL} apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${NINEP_PVC}
  namespace: ${TEST_NAMESPACE}
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: zerofs-ninep
  resources:
    requests:
      storage: 1Gi
EOF
    run ${KUBECTL} get pvc ${NINEP_PVC} -n ${TEST_NAMESPACE} -o jsonpath='{.metadata.name}'
    [ "$status" -eq 0 ]
    [ "$output" = "${NINEP_PVC}" ]
}

@test "9P: PVC is in Pending state until a pod consumes it (WaitForFirstConsumer)" {
    # zerofs-ninep uses WaitForFirstConsumer, so PVC should stay Pending without a pod
    sleep 5
    run ${KUBECTL} get pvc ${NINEP_PVC} -n ${TEST_NAMESPACE} -o jsonpath='{.status.phase}'
    [ "$status" -eq 0 ]
    [ "$output" = "Pending" ]
}

@test "9P: pod can mount the PVC (triggers binding) and write data" {
    ${KUBECTL} apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${NINEP_POD}
  namespace: ${TEST_NAMESPACE}
spec:
  containers:
    - name: app
      image: busybox:latest
      command:
        - sh
        - -c
        - |
          echo "hello from zerofs 9p" > /data/test.txt
          echo "written: $(date)" >> /data/test.txt
          echo "write complete"
          sleep 3600
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${NINEP_PVC}
EOF

    run wait_for_pod_running "${TEST_NAMESPACE}" "${NINEP_POD}" "${POD_READY_TIMEOUT}"
    if [ "$status" -ne 0 ]; then
        dump_debug "${TEST_NAMESPACE}"
    fi
    [ "$status" -eq 0 ]
}

@test "9P: PVC becomes Bound after pod is scheduled" {
    run ${KUBECTL} get pvc ${NINEP_PVC} -n ${TEST_NAMESPACE} -o jsonpath='{.status.phase}'
    [ "$status" -eq 0 ]
    [ "$output" = "Bound" ]
}

@test "9P: data written to PVC can be read back" {
    run ${KUBECTL} exec -n ${TEST_NAMESPACE} ${NINEP_POD} -- cat /data/test.txt
    [ "$status" -eq 0 ]
    [[ "$output" =~ "hello from zerofs 9p" ]]
}

@test "9P: additional writes to PVC persist" {
    run ${KUBECTL} exec -n ${TEST_NAMESPACE} ${NINEP_POD} -- sh -c "echo 'second write' >> /data/test.txt && cat /data/test.txt"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "hello from zerofs 9p" ]]
    [[ "$output" =~ "second write" ]]
}

@test "9P: pod and PVC can be deleted" {
    # Delete pod first
    ${KUBECTL} delete pod ${NINEP_POD} -n ${TEST_NAMESPACE} --ignore-not-found --timeout=60s
    wait_for_pod_deleted "${TEST_NAMESPACE}" "${NINEP_POD}"

    # Delete PVC
    run ${KUBECTL} delete pvc ${NINEP_PVC} -n ${TEST_NAMESPACE} --timeout=60s
    [ "$status" -eq 0 ]

    # Verify PVC is gone
    sleep 5
    run ${KUBECTL} get pvc ${NINEP_PVC} -n ${TEST_NAMESPACE} 2>&1
    [[ "$output" =~ "not found" ]]
}

@test "9P: per-volume zerofs deployment is cleaned up after PVC deletion" {
    # Wait for all zerofs volume deployments to be cleaned up
    local timeout=120
    local elapsed=0
    local interval=5
    while [ $elapsed -lt $timeout ]; do
        local count
        count=$(${KUBECTL} get deployments -n zerofs-csi --no-headers 2>/dev/null | \
            grep -v "zerofs-csi-controller\|minio" | grep -c "zerofs-" || true)
        if [ "$count" -eq 0 ]; then
            break
        fi
        sleep $interval
        elapsed=$((elapsed + interval))
    done

    local remaining
    remaining=$(${KUBECTL} get deployments -n zerofs-csi --no-headers 2>/dev/null | \
        grep -v "zerofs-csi-controller\|minio" | grep "zerofs-" || true)
    [ -z "$remaining" ]
}
