package driver

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/zerofs/csi-driver-zerofs/pkg/zerofs"
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

	volumeID := req.GetName()
	params := req.GetParameters()
	secrets := req.GetSecrets()
	params = mergeMapCopy(params)
	cs.applyPVCOverrides(ctx, params)
	cs.warnIfCredentialsProvided(params)
	params = filterDisallowedParams(params)

	reqSize := int64(0)
	if req.GetCapacityRange() != nil {
		reqSize = req.GetCapacityRange().GetRequiredBytes()
	}

	storageURL := params["storageUrl"]
	if storageURL == "" {
		return nil, status.Error(codes.InvalidArgument, "storageUrl parameter is required")
	}

	if !strings.HasSuffix(storageURL, "/") {
		storageURL += "/"
	}
	storageURL += "volumes/" + volumeID

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

	if err := validateVolumeCapabilities(req.GetVolumeCapabilities(), protocol); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if existing, err := cs.manager.GetVolumeMetadata(ctx, volumeID); err == nil {
		if existing.StorageURL != "" && existing.StorageURL != storageURL {
			return nil, status.Errorf(codes.AlreadyExists, "volume %s already exists with different storage URL", volumeID)
		}
		if existing.Protocol != "" && existing.Protocol != protocol {
			return nil, status.Errorf(codes.AlreadyExists, "volume %s already exists with different protocol", volumeID)
		}
		if existing.CapacityBytes > 0 && reqSize > existing.CapacityBytes {
			return nil, status.Errorf(codes.AlreadyExists, "volume %s already exists with smaller capacity", volumeID)
		}

		port := zerofs.NFSPort
		if protocol == zerofs.ProtocolNinep {
			port = zerofs.NinepPort
		}
		volumeContext := map[string]string{
			"server":   existing.ServerName,
			"serverIP": existing.ServiceIP,
			"port":     fmt.Sprintf("%d", port),
			"share":    "/",
			"protocol": string(protocol),
		}
		if existing.ServiceIP == "" {
			delete(volumeContext, "serverIP")
		}

		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: existing.CapacityBytes,
				VolumeContext: volumeContext,
			},
		}, nil
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

func (cs *ControllerServer) applyPVCOverrides(ctx context.Context, params map[string]string) {
	pvcName := params["csi.storage.k8s.io/pvc/name"]
	pvcNamespace := params["csi.storage.k8s.io/pvc/namespace"]
	if pvcName == "" || pvcNamespace == "" {
		return
	}
	manager := cs.manager
	if manager == nil {
		return
	}
	annotations, err := manager.GetPVCAnnotations(ctx, pvcNamespace, pvcName)
	if err != nil {
		klog.Warningf("Failed to read PVC annotations for %s/%s: %v", pvcNamespace, pvcName, err)
		return
	}
	for paramKey, annotationKey := range pvcOverrideableParams() {
		annotationValue, ok := annotations[annotationKey]
		if !ok || annotationValue == "" {
			continue
		}
		params[paramKey] = annotationValue
	}
}

func pvcOverrideableParams() map[string]string {
	return map[string]string{
		"storageUrl":    zerofs.AnnotationStorageURL,
		"awsEndpoint":   zerofs.AnnotationAWSEndpoint,
		"awsAllowHTTP":  zerofs.AnnotationAWSAllowHTTP,
		"awsSecretName": zerofs.AnnotationAWSSecretName,
	}
}

func filterDisallowedParams(params map[string]string) map[string]string {
	if params == nil {
		return map[string]string{}
	}
	filtered := make(map[string]string, len(params))
	for key, value := range params {
		switch key {
		case "awsAccessKeyID", "awsSecretAccessKey":
			continue
		default:
			filtered[key] = value
		}
	}
	return filtered
}

func (cs *ControllerServer) warnIfCredentialsProvided(params map[string]string) {
	if params == nil {
		return
	}
	if _, ok := params["awsAccessKeyID"]; ok {
		klog.Warning("awsAccessKeyID parameter is ignored; use awsSecretName instead")
	}
	if _, ok := params["awsSecretAccessKey"]; ok {
		klog.Warning("awsSecretAccessKey parameter is ignored; use awsSecretName instead")
	}
}

func mergeMapCopy(input map[string]string) map[string]string {
	if input == nil {
		return map[string]string{}
	}
	copy := make(map[string]string, len(input))
	for key, value := range input {
		copy[key] = value
	}
	return copy
}

func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	klog.V(4).Infof("DeleteVolume called with req: %+v", req)

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	if err := cs.manager.DeleteZerofsDeployment(ctx, volumeID); err != nil {
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

	metadata, err := cs.manager.GetVolumeMetadata(ctx, req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", req.GetVolumeId())
	}

	protocol := metadata.Protocol
	if protocol == "" {
		protocol = zerofs.ProtocolNFS
	}
	if err := validateVolumeCapabilities(req.GetVolumeCapabilities(), protocol); err != nil {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: err.Error()}, nil
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}

func (cs *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	klog.V(4).Infof("ListVolumes called with req: %+v", req)

	volumes, err := cs.manager.ListVolumeMetadata(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list volumes: %v", err)
	}

	startingIndex := 0
	if req.GetStartingToken() != "" {
		parsed, err := strconv.Atoi(req.GetStartingToken())
		if err != nil || parsed < 0 {
			return nil, status.Error(codes.InvalidArgument, "invalid starting token")
		}
		startingIndex = parsed
	}

	if startingIndex > len(volumes) {
		return nil, status.Error(codes.InvalidArgument, "starting token beyond volume list")
	}

	maxEntries := len(volumes) - startingIndex
	if req.GetMaxEntries() > 0 && int(req.GetMaxEntries()) < maxEntries {
		maxEntries = int(req.GetMaxEntries())
	}

	entries := make([]*csi.ListVolumesResponse_Entry, 0, maxEntries)
	for _, record := range volumes[startingIndex : startingIndex+maxEntries] {
		port := zerofs.NFSPort
		if record.Protocol == zerofs.ProtocolNinep {
			port = zerofs.NinepPort
		}
		volumeContext := map[string]string{
			"server":   record.ServerName,
			"serverIP": record.ServiceIP,
			"port":     fmt.Sprintf("%d", port),
			"share":    "/",
			"protocol": string(record.Protocol),
		}
		if record.ServiceIP == "" {
			delete(volumeContext, "serverIP")
		}

		entries = append(entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				VolumeId:      record.VolumeID,
				CapacityBytes: record.CapacityBytes,
				VolumeContext: volumeContext,
			},
		})
	}

	nextToken := ""
	if startingIndex+maxEntries < len(volumes) {
		nextToken = strconv.Itoa(startingIndex + maxEntries)
	}

	return &csi.ListVolumesResponse{
		Entries:   entries,
		NextToken: nextToken,
	}, nil
}

func (cs *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	klog.V(4).Infof("GetCapacity called with req: %+v", req)
	return &csi.GetCapacityResponse{AvailableCapacity: 0}, nil
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
	return nil, status.Error(codes.Unimplemented, "snapshots are not supported")
}

func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	klog.V(4).Infof("DeleteSnapshot called with req: %+v", req)
	return nil, status.Error(codes.Unimplemented, "snapshots are not supported")
}

func (cs *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	klog.V(4).Infof("ListSnapshots called with req: %+v", req)
	return nil, status.Error(codes.Unimplemented, "snapshots are not supported")
}

func (cs *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	klog.V(4).Infof("ControllerExpandVolume called with req: %+v", req)

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	if req.GetCapacityRange() == nil {
		return nil, status.Error(codes.InvalidArgument, "Capacity range missing in request")
	}

	if err := cs.manager.UpdateVolumeCapacity(ctx, volumeID, req.GetCapacityRange().GetRequiredBytes()); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update volume metadata: %v", err)
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         req.GetCapacityRange().GetRequiredBytes(),
		NodeExpansionRequired: false,
	}, nil
}

func (cs *ControllerServer) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	klog.V(4).Infof("ControllerGetVolume called with req: %+v", req)
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	metadata, err := cs.manager.GetVolumeMetadata(ctx, volumeID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", volumeID)
	}

	protocol := metadata.Protocol
	if protocol == "" {
		protocol = zerofs.ProtocolNFS
	}
	port := zerofs.NFSPort
	if protocol == zerofs.ProtocolNinep {
		port = zerofs.NinepPort
	}
	volumeContext := map[string]string{
		"server":   metadata.ServerName,
		"serverIP": metadata.ServiceIP,
		"port":     fmt.Sprintf("%d", port),
		"share":    "/",
		"protocol": string(protocol),
	}
	if metadata.ServiceIP == "" {
		delete(volumeContext, "serverIP")
	}

	return &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: metadata.CapacityBytes,
			VolumeContext: volumeContext,
		},
	}, nil
}
