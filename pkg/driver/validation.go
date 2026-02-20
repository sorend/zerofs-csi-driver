package driver

import (
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/zerofs/csi-driver-zerofs/pkg/zerofs"
)

func validateVolumeCapabilities(volumeCaps []*csi.VolumeCapability, protocol zerofs.Protocol) error {
	if len(volumeCaps) == 0 {
		return fmt.Errorf("volume capabilities missing in request")
	}

	for _, cap := range volumeCaps {
		if cap.GetBlock() != nil {
			return fmt.Errorf("block volume capability not supported")
		}
		accessMode := cap.GetAccessMode()
		if accessMode == nil {
			return fmt.Errorf("access mode missing in volume capability")
		}
		mode := accessMode.GetMode()
		if protocol == zerofs.ProtocolNinep {
			if mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER &&
				mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY {
				return fmt.Errorf("9P protocol only supports RWO and RO access modes")
			}
			continue
		}

		switch mode {
		case csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
			csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY:
		default:
			return fmt.Errorf("unsupported access mode")
		}
	}

	return nil
}
