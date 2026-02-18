package driver

import (
	"context"
	"fmt"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/zerofs/zerofs-csi-driver/pkg/zerofs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

type ControllerServer struct {
	driver  *Driver
	manager *zerofs.Manager
	csi.UnimplementedControllerServer
}

func NewControllerServer(d *Driver) *ControllerServer {
	return &ControllerServer{
		driver:  d,
		manager: zerofs.NewManager(d.options.Namespace, d.options.WorkDir, d.options.ZerofsImage),
	}
}

func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	klog.Infof("CreateVolume called with req: %+v", req)
	klog.Infof("AccessibilityRequirements: %+v", req.GetAccessibilityRequirements())

	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume name missing in request")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities missing in request")
	}

	volumeID := req.GetName()
	params := req.GetParameters()
	secrets := req.GetSecrets()

	reqSize := int64(0)
	if req.GetCapacityRange() != nil {
		reqSize = req.GetCapacityRange().GetRequiredBytes()
	}

	storageURL := params["storageUrl"]
	if storageURL == "" {
		return nil, status.Error(codes.InvalidArgument, "storageUrl parameter is required")
	}

	protocol := zerofs.ProtocolNFS
	if p, ok := params["protocol"]; ok {
		switch zerofs.Protocol(p) {
		case zerofs.ProtocolNFS:
			protocol = zerofs.ProtocolNFS
		case zerofs.ProtocolNinep:
			protocol = zerofs.ProtocolNinep
		default:
			return nil, status.Errorf(codes.InvalidArgument, "invalid protocol %q, must be 'nfs' or 'ninep'", p)
		}
	}

	if protocol == zerofs.ProtocolNinep {
		for _, cap := range req.GetVolumeCapabilities() {
			if cap.GetAccessMode() != nil {
				mode := cap.GetAccessMode().GetMode()
				if mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER &&
					mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY {
					return nil, status.Error(codes.InvalidArgument, "9P protocol only supports RWO (ReadWriteOnce) access mode")
				}
			}
		}
	}

	var nodeName string
	if protocol == zerofs.ProtocolNinep {
		if req.GetAccessibilityRequirements() != nil {
			preferred := req.GetAccessibilityRequirements().GetPreferred()
			if len(preferred) > 0 {
				for _, seg := range preferred {
					if seg.GetSegments() != nil {
						segments := seg.GetSegments()
						if node, ok := segments[TopologyKeyNode]; ok {
							nodeName = node
							break
						}
						if node, ok := segments["kubernetes.io/hostname"]; ok {
							nodeName = node
							break
						}
						if node, ok := segments["topology.kubernetes.io/hostname"]; ok {
							nodeName = node
							break
						}
					}
				}
			}
		}
		if nodeName == "" {
			return nil, status.Error(codes.InvalidArgument, "9P protocol requires WaitForFirstConsumer binding mode to determine node placement")
		}
	}

	serviceIP, podIP, err := cs.manager.CreateZerofsDeployment(ctx, volumeID, storageURL, protocol, nodeName, params, secrets, reqSize)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create zerofs deployment: %v", err)
	}

	serviceName := cs.manager.GetServiceName(volumeID)
	port := zerofs.NFSPort
	if protocol == zerofs.ProtocolNinep {
		port = zerofs.NinepPort
	}

	volumeContext := map[string]string{
		"server":   serviceName + "." + cs.driver.options.Namespace + ".svc.cluster.local",
		"serverIP": serviceIP,
		"port":     fmt.Sprintf("%d", port),
		"share":    "/",
		"protocol": string(protocol),
	}
	if podIP != "" {
		volumeContext["podIP"] = podIP
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: reqSize,
			VolumeContext: volumeContext,
		},
	}, nil
}

func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	klog.V(4).Infof("DeleteVolume called with req: %+v", req)

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	if err := cs.manager.DeleteZerofsDeployment(ctx, volumeID); err != nil {
		if strings.Contains(err.Error(), "not found") {
			return &csi.DeleteVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to delete zerofs deployment: %v", err)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	klog.V(4).Infof("ControllerPublishVolume called with req: %+v", req)
	return nil, status.Error(codes.Unimplemented, "ControllerPublishVolume is not implemented")
}

func (cs *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	klog.V(4).Infof("ControllerUnpublishVolume called with req: %+v", req)
	return nil, status.Error(codes.Unimplemented, "ControllerUnpublishVolume is not implemented")
}

func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	klog.V(4).Infof("ValidateVolumeCapabilities called with req: %+v", req)

	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities missing in request")
	}

	for _, cap := range req.GetVolumeCapabilities() {
		if cap.GetAccessMode() != nil {
			mode := cap.GetAccessMode().GetMode()
			if mode != csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER &&
				mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER &&
				mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY &&
				mode != csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
				return &csi.ValidateVolumeCapabilitiesResponse{
					Message: "Only RWX, RWO, ROX access modes are supported",
				}, nil
			}
		}
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}

func (cs *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	klog.V(4).Infof("ListVolumes called with req: %+v", req)
	return nil, status.Error(codes.Unimplemented, "ListVolumes is not implemented")
}

func (cs *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	klog.V(4).Infof("GetCapacity called with req: %+v", req)
	return nil, status.Error(codes.Unimplemented, "GetCapacity is not implemented")
}

func (cs *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	klog.V(4).Infof("ControllerGetCapabilities called with req: %+v", req)

	caps := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	}

	var cscs []*csi.ControllerServiceCapability
	for _, cap := range caps {
		cscs = append(cscs, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		})
	}

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: cscs,
	}, nil
}

func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	klog.V(4).Infof("CreateSnapshot called with req: %+v", req)
	return nil, status.Error(codes.Unimplemented, "CreateSnapshot is not implemented")
}

func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	klog.V(4).Infof("DeleteSnapshot called with req: %+v", req)
	return nil, status.Error(codes.Unimplemented, "DeleteSnapshot is not implemented")
}

func (cs *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	klog.V(4).Infof("ListSnapshots called with req: %+v", req)
	return nil, status.Error(codes.Unimplemented, "ListSnapshots is not implemented")
}

func (cs *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	klog.V(4).Infof("ControllerExpandVolume called with req: %+v", req)

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         req.GetCapacityRange().GetRequiredBytes(),
		NodeExpansionRequired: false,
	}, nil
}

func (cs *ControllerServer) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	klog.V(4).Infof("ControllerGetVolume called with req: %+v", req)
	return nil, status.Error(codes.Unimplemented, "ControllerGetVolume is not implemented")
}
