package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/zerofs/csi-driver-zerofs/pkg/zerofs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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
			DriverName:  "zerofs.csi.sorend.github.com",
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
			gomega.Expect(drv.options.DriverName).To(gomega.Equal("zerofs.csi.sorend.github.com"))
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
			DriverName: "zerofs.csi.sorend.github.com",
		})
		ids = NewIdentityServer(drv)
	})

	ginkgo.Context("GetPluginInfo", func() {
		ginkgo.It("should return driver info", func() {
			resp, err := ids.GetPluginInfo(context.Background(), &csi.GetPluginInfoRequest{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(resp.Name).To(gomega.Equal("zerofs.csi.sorend.github.com"))
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
			DriverName:  "zerofs.csi.sorend.github.com",
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

		ginkgo.It("should allow pvc annotations to override storageUrl", func() {
			fakeClient := fake.NewSimpleClientset()
			cs.manager.SetClient(fakeClient)
			cs.driver.options.Namespace = "default"
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "default",
					Annotations: map[string]string{
						zerofs.AnnotationStorageURL: "s3://override-bucket/data",
					},
				},
			}
			_, err := fakeClient.CoreV1().PersistentVolumeClaims("default").Create(context.Background(), pvc, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			_, err = cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name: "test-volume",
				Parameters: map[string]string{
					"storageUrl":                       "s3://base-bucket/data",
					"csi.storage.k8s.io/pvc/name":      "test-pvc",
					"csi.storage.k8s.io/pvc/namespace": "default",
				},
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
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			metadata, err := cs.manager.GetVolumeMetadata(context.Background(), "test-volume")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(metadata.StorageURL).To(gomega.Equal("s3://override-bucket/data/volumes/test-volume"))
		})

		ginkgo.It("should ignore inline access key parameters", func() {
			fakeClient := fake.NewSimpleClientset()
			cs.manager.SetClient(fakeClient)
			cs.driver.options.Namespace = "default"
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "default",
					Annotations: map[string]string{
						zerofs.AnnotationStorageURL: "s3://base-bucket/data",
					},
				},
			}
			_, err := fakeClient.CoreV1().PersistentVolumeClaims("default").Create(context.Background(), pvc, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "zerofs-aws-credentials",
					Namespace: "default",
				},
				Data: map[string][]byte{
					"awsAccessKeyID":     []byte("secret-access-key"),
					"awsSecretAccessKey": []byte("secret-secret-key"),
				},
			}
			_, err = fakeClient.CoreV1().Secrets("default").Create(context.Background(), secret, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			_, err = cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name: "test-volume-keys",
				Parameters: map[string]string{
					"storageUrl":                       "s3://base-bucket/data",
					"csi.storage.k8s.io/pvc/name":      "test-pvc",
					"csi.storage.k8s.io/pvc/namespace": "default",
					"awsSecretName":                    "zerofs-aws-credentials",
					"awsAccessKeyID":                   "param-access-key",
					"awsSecretAccessKey":               "param-secret-key",
				},
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
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			secretName := cs.manager.GetSecretName("test-volume-keys")
			createdSecret, err := fakeClient.CoreV1().Secrets("default").Get(context.Background(), secretName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			configData := string(createdSecret.Data["zerofs.toml"])
			gomega.Expect(configData).To(gomega.ContainSubstring("secret-access-key"))
			gomega.Expect(configData).To(gomega.ContainSubstring("secret-secret-key"))
			gomega.Expect(configData).NotTo(gomega.ContainSubstring("param-access-key"))
			gomega.Expect(configData).NotTo(gomega.ContainSubstring("param-secret-key"))
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

		ginkgo.It("should return not found when volume metadata is missing", func() {
			_, err := cs.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: "test-volume",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
						},
					},
				},
			})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.NotFound))
		})

		ginkgo.It("should accept RWO access mode when metadata exists", func() {
			fakeClient := fake.NewSimpleClientset()
			cs.manager.SetClient(fakeClient)
			_, _, err := cs.manager.CreateZerofsDeployment(context.Background(), "test-volume", "s3://bucket/volumes/test-volume", zerofs.ProtocolNFS, "", map[string]string{}, map[string]string{}, 1024)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
			fakeClient := fake.NewSimpleClientset()
			cs.manager.SetClient(fakeClient)
			_, _, err := cs.manager.CreateZerofsDeployment(context.Background(), "test-volume", "s3://bucket/volumes/test-volume", zerofs.ProtocolNFS, "", map[string]string{}, map[string]string{}, 1024)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

		ginkgo.It("ListVolumes should not return unimplemented", func() {
			_, err := cs.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
			if err != nil {
				st, ok := status.FromError(err)
				gomega.Expect(ok).To(gomega.BeTrue())
				gomega.Expect(st.Code()).NotTo(gomega.Equal(codes.Unimplemented))
			}
		})

		ginkgo.It("GetCapacity should not return unimplemented", func() {
			_, err := cs.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ginkgo.It("ControllerGetVolume should return invalid argument when volume ID missing", func() {
			_, err := cs.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{})
			gomega.Expect(err).To(gomega.HaveOccurred())
			st, ok := status.FromError(err)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(st.Code()).To(gomega.Equal(codes.InvalidArgument))
		})
	})
})
