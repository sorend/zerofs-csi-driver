package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDriver(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Driver Suite")
}

var _ = ginkgo.Describe("Driver", func() {
	var (
		drv  *Driver
		opts *DriverOptions
	)

	ginkgo.BeforeEach(func() {
		opts = &DriverOptions{
			DriverName:  "zerofs.csi.k8s.io",
			NodeID:      "test-node",
			Endpoint:    "unix:///tmp/csi-test.sock",
			Namespace:   "default",
			WorkDir:     "/tmp/zerofs-csi",
			ZerofsImage: "zerofs:latest",
		}
		drv = NewDriver(opts)
	})

	ginkgo.Context("NewDriver", func() {
		ginkgo.It("should create a driver with default name", func() {
			emptyOpts := &DriverOptions{}
			d := NewDriver(emptyOpts)
			gomega.Expect(d.options.DriverName).To(gomega.Equal(DriverName))
		})

		ginkgo.It("should create a driver with custom name", func() {
			gomega.Expect(drv.options.DriverName).To(gomega.Equal("zerofs.csi.k8s.io"))
		})

		ginkgo.It("should initialize all servers", func() {
			gomega.Expect(drv.ids).NotTo(gomega.BeNil())
			gomega.Expect(drv.Cs).NotTo(gomega.BeNil())
			gomega.Expect(drv.ns).NotTo(gomega.BeNil())
		})
	})

	ginkgo.Context("GetManager", func() {
		ginkgo.It("should return the manager from controller server", func() {
			manager := drv.GetManager()
			gomega.Expect(manager).NotTo(gomega.BeNil())
		})
	})
})

var _ = ginkgo.Describe("IdentityServer", func() {
	var (
		ids *IdentityServer
		drv *Driver
	)

	ginkgo.BeforeEach(func() {
		drv = NewDriver(&DriverOptions{
			DriverName: "zerofs.csi.k8s.io",
		})
		ids = NewIdentityServer(drv)
	})

	ginkgo.Context("GetPluginInfo", func() {
		ginkgo.It("should return driver info", func() {
			resp, err := ids.GetPluginInfo(context.Background(), &csi.GetPluginInfoRequest{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(resp.Name).To(gomega.Equal("zerofs.csi.k8s.io"))
			gomega.Expect(resp.VendorVersion).To(gomega.Equal(DriverVersion))
		})

		ginkgo.It("should return error when driver name is empty", func() {
			ids.driver.options.DriverName = ""
			_, err := ids.GetPluginInfo(context.Background(), &csi.GetPluginInfoRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.Unavailable))
		})
	})

	ginkgo.Context("Probe", func() {
		ginkgo.It("should return ready", func() {
			resp, err := ids.Probe(context.Background(), &csi.ProbeRequest{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(resp).NotTo(gomega.BeNil())
		})
	})

	ginkgo.Context("GetPluginCapabilities", func() {
		ginkgo.It("should return controller service capability", func() {
			resp, err := ids.GetPluginCapabilities(context.Background(), &csi.GetPluginCapabilitiesRequest{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(resp.Capabilities).NotTo(gomega.BeEmpty())

			hasController := false
			for _, cap := range resp.Capabilities {
				if svc := cap.GetService(); svc != nil {
					if svc.GetType() == csi.PluginCapability_Service_CONTROLLER_SERVICE {
						hasController = true
					}
				}
			}
			gomega.Expect(hasController).To(gomega.BeTrue())
		})
	})
})

var _ = ginkgo.Describe("ControllerServer", func() {
	var (
		cs  *ControllerServer
		drv *Driver
	)

	ginkgo.BeforeEach(func() {
		drv = NewDriver(&DriverOptions{
			DriverName:  "zerofs.csi.k8s.io",
			Namespace:   "default",
			WorkDir:     "/tmp/zerofs-csi",
			ZerofsImage: "zerofs:latest",
		})
		cs = NewControllerServer(drv)
	})

	ginkgo.Context("ControllerGetCapabilities", func() {
		ginkgo.It("should return capabilities", func() {
			resp, err := cs.ControllerGetCapabilities(context.Background(), &csi.ControllerGetCapabilitiesRequest{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(resp.Capabilities).NotTo(gomega.BeEmpty())
		})
	})

	ginkgo.Context("CreateVolume", func() {
		ginkgo.It("should return error when volume name is empty", func() {
			_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})

		ginkgo.It("should return error when capabilities are missing", func() {
			_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name: "test-volume",
			})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})

		ginkgo.It("should return error when storageUrl is missing", func() {
			_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name: "test-volume",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessType: &csi.VolumeCapability_Mount{
							Mount: &csi.VolumeCapability_MountVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
						},
					},
				},
			})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})
	})

	ginkgo.Context("DeleteVolume", func() {
		ginkgo.It("should return error when volume ID is empty", func() {
			_, err := cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})
	})

	ginkgo.Context("ValidateVolumeCapabilities", func() {
		ginkgo.It("should return error when volume ID is empty", func() {
			_, err := cs.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})

		ginkgo.It("should return error when capabilities are empty", func() {
			_, err := cs.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: "test-volume",
			})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})

		ginkgo.It("should accept RWX access mode", func() {
			resp, err := cs.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: "test-volume",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
						},
					},
				},
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(resp.Confirmed).NotTo(gomega.BeNil())
		})

		ginkgo.It("should accept RWO access mode", func() {
			resp, err := cs.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: "test-volume",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
				},
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(resp.Confirmed).NotTo(gomega.BeNil())
		})
	})

	ginkgo.Context("ControllerExpandVolume", func() {
		ginkgo.It("should return error when volume ID is empty", func() {
			_, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})

		ginkgo.It("should return success with valid request", func() {
			resp, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
				VolumeId: "test-volume",
				CapacityRange: &csi.CapacityRange{
					RequiredBytes: 1024 * 1024 * 1024,
				},
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(resp.CapacityBytes).To(gomega.Equal(int64(1024 * 1024 * 1024)))
			gomega.Expect(resp.NodeExpansionRequired).To(gomega.BeFalse())
		})
	})

	ginkgo.Context("Unimplemented methods", func() {
		ginkgo.It("ControllerPublishVolume should return unimplemented", func() {
			_, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.Unimplemented))
		})

		ginkgo.It("ControllerUnpublishVolume should return unimplemented", func() {
			_, err := cs.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.Unimplemented))
		})

		ginkgo.It("ListVolumes should return unimplemented", func() {
			_, err := cs.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.Unimplemented))
		})

		ginkgo.It("GetCapacity should return unimplemented", func() {
			_, err := cs.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.Unimplemented))
		})

		ginkgo.It("CreateSnapshot should return unimplemented", func() {
			_, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.Unimplemented))
		})

		ginkgo.It("DeleteSnapshot should return unimplemented", func() {
			_, err := cs.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.Unimplemented))
		})

		ginkgo.It("ListSnapshots should return unimplemented", func() {
			_, err := cs.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.Unimplemented))
		})

		ginkgo.It("ControllerGetVolume should return unimplemented", func() {
			_, err := cs.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.Unimplemented))
		})
	})
})
