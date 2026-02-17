package driver

import (
	"context"
	"os"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = ginkgo.Describe("NodeServer", func() {
	var (
		ns  *NodeServer
		drv *Driver
	)

	ginkgo.BeforeEach(func() {
		drv = NewDriver(&DriverOptions{
			DriverName: "zerofs.csi.k8s.io",
			NodeID:     "test-node",
		})
		ns = NewNodeServer(drv)
	})

	ginkgo.Context("NodeGetCapabilities", func() {
		ginkgo.It("should return capabilities", func() {
			resp, err := ns.NodeGetCapabilities(context.Background(), &csi.NodeGetCapabilitiesRequest{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(resp.Capabilities).NotTo(gomega.BeEmpty())

			capTypes := make(map[csi.NodeServiceCapability_RPC_Type]bool)
			for _, cap := range resp.Capabilities {
				if rpc := cap.GetRpc(); rpc != nil {
					capTypes[rpc.GetType()] = true
				}
			}
			gomega.Expect(capTypes[csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME]).To(gomega.BeTrue())
			gomega.Expect(capTypes[csi.NodeServiceCapability_RPC_EXPAND_VOLUME]).To(gomega.BeTrue())
			gomega.Expect(capTypes[csi.NodeServiceCapability_RPC_GET_VOLUME_STATS]).To(gomega.BeTrue())
		})
	})

	ginkgo.Context("NodeGetInfo", func() {
		ginkgo.It("should return node info with configured node ID", func() {
			resp, err := ns.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(resp.NodeId).To(gomega.Equal("test-node"))
			gomega.Expect(resp.MaxVolumesPerNode).To(gomega.Equal(int64(1000)))
		})

		ginkgo.It("should return hostname as node ID when not configured", func() {
			ns.driver.options.NodeID = ""
			resp, err := ns.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			hostname, _ := os.Hostname()
			gomega.Expect(resp.NodeId).To(gomega.Equal(hostname))
		})
	})

	ginkgo.Context("NodeStageVolume", func() {
		ginkgo.It("should return error when volume ID is empty", func() {
			_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})

		ginkgo.It("should return error when staging target path is empty", func() {
			_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
				VolumeId: "test-volume",
			})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})
	})

	ginkgo.Context("NodeUnstageVolume", func() {
		ginkgo.It("should return error when volume ID is empty", func() {
			_, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})

		ginkgo.It("should return error when staging target path is empty", func() {
			_, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
				VolumeId: "test-volume",
			})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})
	})

	ginkgo.Context("NodePublishVolume", func() {
		ginkgo.It("should return error when volume ID is empty", func() {
			_, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})

		ginkgo.It("should return error when staging target path is empty", func() {
			_, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
				VolumeId: "test-volume",
			})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})

		ginkgo.It("should return error when target path is empty", func() {
			_, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
				VolumeId:          "test-volume",
				StagingTargetPath: "/tmp/staging",
			})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})
	})

	ginkgo.Context("NodeUnpublishVolume", func() {
		ginkgo.It("should return error when volume ID is empty", func() {
			_, err := ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})

		ginkgo.It("should return error when target path is empty", func() {
			_, err := ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
				VolumeId: "test-volume",
			})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})
	})

	ginkgo.Context("NodeGetVolumeStats", func() {
		ginkgo.It("should return error when volume path is empty", func() {
			_, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})

		ginkgo.It("should return error when volume path does not exist", func() {
			_, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
				VolumePath: "/nonexistent/path",
			})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.NotFound))
		})

		ginkgo.It("should return stats for existing directory", func() {
			tmpDir := filepath.Join(os.TempDir(), "zerofs-test-stats")
			os.MkdirAll(tmpDir, 0755)
			defer os.RemoveAll(tmpDir)

			resp, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
				VolumePath: tmpDir,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(resp.Usage).NotTo(gomega.BeEmpty())
		})
	})

	ginkgo.Context("NodeExpandVolume", func() {
		ginkgo.It("should return error when volume ID is empty", func() {
			_, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})

		ginkgo.It("should return error when volume path is empty", func() {
			_, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
				VolumeId: "test-volume",
			})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})
	})
})
