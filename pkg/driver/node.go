package driver

import (
	"context"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/zerofs/csi-driver-zerofs/pkg/zerofs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
)

type NodeServer struct {
	driver  *Driver
	mounter *mount.SafeFormatAndMount
	csi.UnimplementedNodeServer
}

func NewNodeServer(d *Driver) *NodeServer {
	return &NodeServer{
		driver: d,
		mounter: &mount.SafeFormatAndMount{
			Interface: mount.New(""),
		},
	}
}

func (ns *NodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	klog.V(4).Infof("NodeStageVolume called with req: %+v", req)

	volumeID := req.GetVolumeId()
	stagingTargetPath := req.GetStagingTargetPath()
	volumeContext := req.GetVolumeContext()

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Staging target path missing in request")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}

	server := volumeContext["server"]
	port := volumeContext["port"]
	if port == "" {
		port = "2049"
	}
	share := volumeContext["share"]
	if share == "" {
		share = "/"
	}

	protocol := zerofs.ProtocolNFS
	if p, ok := volumeContext["protocol"]; ok {
		protocol = zerofs.Protocol(p)
	}

	if err := validateVolumeCapabilities([]*csi.VolumeCapability{req.GetVolumeCapability()}, protocol); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := os.MkdirAll(stagingTargetPath, 0750); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create staging directory: %v", err)
	}

	isMnt, err := ns.mounter.IsLikelyNotMountPoint(stagingTargetPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "failed to check mount point: %v", err)
	}
	if !isMnt {
		klog.V(4).Infof("Volume %s already staged at %s", volumeID, stagingTargetPath)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	switch protocol {
	case zerofs.ProtocolNinep:
		return ns.stageNinepVolume(volumeID, server, port, share, stagingTargetPath, volumeContext)
	default:
		return ns.stageNFSVolume(volumeID, server, port, share, stagingTargetPath, volumeContext)
	}
}

func (ns *NodeServer) stageNFSVolume(volumeID, server, port, share, stagingTargetPath string, volumeContext map[string]string) (*csi.NodeStageVolumeResponse, error) {
	source := server
	if serverIP, ok := volumeContext["serverIP"]; ok && serverIP != "" {
		source = serverIP
	}
	source = fmt.Sprintf("%s:%s", source, share)

	mountOptions := []string{"nolock", "vers=3", "tcp", "port=2049", "mountport=2049"}
	if mo, ok := volumeContext["mountOptions"]; ok && mo != "" {
		mountOptions = strings.Split(mo, ",")
	}

	options := append([]string{}, mountOptions...)
	if err := ns.mounter.Mount(source, stagingTargetPath, "nfs", options); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to mount NFS volume %s at %s: %v", volumeID, stagingTargetPath, err)
	}

	klog.V(4).Infof("Successfully staged NFS volume %s at %s", volumeID, stagingTargetPath)
	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *NodeServer) stageNinepVolume(volumeID, server, port, share, stagingTargetPath string, volumeContext map[string]string) (*csi.NodeStageVolumeResponse, error) {
	source := server
	if podIP, ok := volumeContext["podIP"]; ok && podIP != "" {
		source = podIP
	} else if serverIP, ok := volumeContext["serverIP"]; ok && serverIP != "" {
		source = serverIP
	}

	mountOptions := []string{
		fmt.Sprintf("trans=tcp"),
		fmt.Sprintf("port=%s", port),
		"version=9p2000.L",
		"msize=65536",
	}
	if mo, ok := volumeContext["mountOptions"]; ok && mo != "" {
		mountOptions = strings.Split(mo, ",")
	}

	options := append([]string{}, mountOptions...)
	if err := ns.mounter.Mount(source, stagingTargetPath, "9p", options); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to mount 9P volume %s at %s: %v", volumeID, stagingTargetPath, err)
	}

	klog.V(4).Infof("Successfully staged 9P volume %s at %s", volumeID, stagingTargetPath)
	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *NodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	klog.V(4).Infof("NodeUnstageVolume called with req: %+v", req)

	volumeID := req.GetVolumeId()
	stagingTargetPath := req.GetStagingTargetPath()

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Staging target path missing in request")
	}

	isMnt, err := ns.mounter.IsLikelyNotMountPoint(stagingTargetPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "failed to check mount point: %v", err)
	}
	if isMnt {
		klog.V(4).Infof("Volume %s already unstaged from %s", volumeID, stagingTargetPath)
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	if err := ns.mounter.Unmount(stagingTargetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unmount staging target path %s: %v", stagingTargetPath, err)
	}

	if err := os.Remove(stagingTargetPath); err != nil && !os.IsNotExist(err) {
		klog.Warningf("Failed to remove staging directory %s: %v", stagingTargetPath, err)
	}

	klog.V(4).Infof("Successfully unstaged volume %s from %s", volumeID, stagingTargetPath)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	klog.V(4).Infof("NodePublishVolume called with req: %+v", req)

	volumeID := req.GetVolumeId()
	stagingTargetPath := req.GetStagingTargetPath()
	targetPath := req.GetTargetPath()

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Staging target path missing in request")
	}
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}

	if err := os.MkdirAll(targetPath, 0750); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create target directory: %v", err)
	}

	notMnt, err := ns.mounter.IsLikelyNotMountPoint(targetPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "failed to check mount point: %v", err)
	}

	if !notMnt {
		klog.V(4).Infof("Volume %s already published at %s", volumeID, targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	options := []string{"bind"}
	if req.GetReadonly() {
		options = append(options, "ro")
	}

	if err := ns.mounter.Mount(stagingTargetPath, targetPath, "", options); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to bind mount %s to %s: %v", stagingTargetPath, targetPath, err)
	}

	klog.V(4).Infof("Successfully published volume %s at %s", volumeID, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	klog.V(4).Infof("NodeUnpublishVolume called with req: %+v", req)

	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	notMnt, err := ns.mounter.IsLikelyNotMountPoint(targetPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "failed to check mount point: %v", err)
	}
	if notMnt {
		klog.V(4).Infof("Volume %s already unpublished from %s", volumeID, targetPath)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	if err := ns.mounter.Unmount(targetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unmount target path %s: %v", targetPath, err)
	}

	if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
		klog.Warningf("Failed to remove target directory %s: %v", targetPath, err)
	}

	klog.V(4).Infof("Successfully unpublished volume %s from %s", volumeID, targetPath)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *NodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	klog.V(4).Infof("NodeGetVolumeStats called with req: %+v", req)

	volumePath := req.GetVolumePath()
	if volumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume path missing in request")
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	var stat os.FileInfo
	stat, err := os.Stat(volumePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "volume path %s does not exist", volumePath)
		}
		return nil, status.Errorf(codes.Internal, "failed to stat volume path %s: %v", volumePath, err)
	}

	if !stat.IsDir() {
		return &csi.NodeGetVolumeStatsResponse{
			Usage: []*csi.VolumeUsage{
				{
					Unit:  csi.VolumeUsage_BYTES,
					Total: stat.Size(),
				},
			},
		}, nil
	}

	var fsStats syscall.Statfs_t
	if err := syscall.Statfs(volumePath, &fsStats); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to statfs volume path %s: %v", volumePath, err)
	}

	blockSize := int64(fsStats.Bsize)
	available := int64(fsStats.Bavail) * blockSize
	total := int64(fsStats.Blocks) * blockSize
	used := total - available

	inodeAvailable := int64(fsStats.Ffree)
	inodeTotal := int64(fsStats.Files)
	inodeUsed := inodeTotal - inodeAvailable

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Available: available,
				Total:     total,
				Used:      used,
				Unit:      csi.VolumeUsage_BYTES,
			},
			{
				Available: inodeAvailable,
				Total:     inodeTotal,
				Used:      inodeUsed,
				Unit:      csi.VolumeUsage_INODES,
			},
		},
	}, nil
}

func (ns *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	klog.V(4).Infof("NodeExpandVolume called with req: %+v", req)

	volumeID := req.GetVolumeId()
	volumePath := req.GetVolumePath()

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if volumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume path missing in request")
	}
	if req.GetCapacityRange() == nil {
		return nil, status.Error(codes.InvalidArgument, "Capacity range missing in request")
	}

	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
	}, nil
}

func (ns *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	klog.V(5).Infof("NodeGetCapabilities called with req: %+v", req)

	caps := []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
		csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
		csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
	}

	var nscs []*csi.NodeServiceCapability
	for _, cap := range caps {
		nscs = append(nscs, &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: cap,
				},
			},
		})
	}

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: nscs,
	}, nil
}

func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	klog.V(5).Infof("NodeGetInfo called with req: %+v", req)

	nodeID := ns.driver.options.NodeID
	if nodeID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get hostname: %v", err)
		}
		nodeID = hostname
	}

	return &csi.NodeGetInfoResponse{
		NodeId:            nodeID,
		MaxVolumesPerNode: 1000,
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{
				"kubernetes.io/hostname": nodeID,
			},
		},
	}, nil
}
