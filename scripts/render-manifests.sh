#!/bin/sh

set -eu

VERSION="${1:-}"
OUTPUT_DIR="${2:-deploy}"
IMAGE_REPO="${IMAGE_REPO:-ghcr.io/sorend/csi-driver-zerofs}"

if [ -z "$VERSION" ]; then
    echo "usage: $0 <version> [output-dir]" >&2
    exit 1
fi

ROOT_DIR=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)

case "$OUTPUT_DIR" in
    /*) ABS_OUTPUT_DIR="$OUTPUT_DIR" ;;
    *) ABS_OUTPUT_DIR="$ROOT_DIR/$OUTPUT_DIR" ;;
esac

mkdir -p "$ABS_OUTPUT_DIR"

CSI_DRIVER_IMAGE="${IMAGE_REPO}:${VERSION}"

sed "s|__CSI_DRIVER_IMAGE__|$CSI_DRIVER_IMAGE|g" \
    "$ROOT_DIR/deploy/install.yaml.tpl" > "$ABS_OUTPUT_DIR/install.yaml"

cp "$ROOT_DIR/deploy/storageclasses.yaml" "$ABS_OUTPUT_DIR/storageclasses.yaml"
cp "$ROOT_DIR/deploy/examples.yaml" "$ABS_OUTPUT_DIR/examples.yaml"
cp "$ROOT_DIR/test/minio.yaml" "$ABS_OUTPUT_DIR/minio.yaml"
