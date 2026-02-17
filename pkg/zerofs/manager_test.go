package zerofs

import (
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

func TestZerofs(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Zerofs Suite")
}

var _ = ginkgo.Describe("Manager", func() {
	var (
		manager *Manager
	)

	ginkgo.BeforeEach(func() {
		manager = NewManager("default", "/var/lib/zerofs-csi", "zerofs:latest")
	})

	ginkgo.Context("NewManager", func() {
		ginkgo.It("should create a manager with correct settings", func() {
			gomega.Expect(manager.namespace).To(gomega.Equal("default"))
			gomega.Expect(manager.workDir).To(gomega.Equal("/var/lib/zerofs-csi"))
			gomega.Expect(manager.zerofsImage).To(gomega.Equal("zerofs:latest"))
		})
	})

	ginkgo.Context("GetServiceName", func() {
		ginkgo.It("should generate correct service name", func() {
			name := manager.GetServiceName("pvc-12345")
			gomega.Expect(name).To(gomega.Equal("zerofs-pvc-12345"))
		})
	})

	ginkgo.Context("GetDeploymentName", func() {
		ginkgo.It("should generate correct deployment name", func() {
			name := manager.GetDeploymentName("pvc-12345")
			gomega.Expect(name).To(gomega.Equal("zerofs-pvc-12345"))
		})
	})

	ginkgo.Context("GetConfigMapName", func() {
		ginkgo.It("should generate correct configmap name", func() {
			name := manager.GetConfigMapName("pvc-12345")
			gomega.Expect(name).To(gomega.Equal("zerofs-config-pvc-12345"))
		})
	})

	ginkgo.Context("generateConfig", func() {
		ginkgo.It("should generate valid config with defaults", func() {
			config := manager.generateConfig("s3://my-bucket/data", nil, nil)
			gomega.Expect(config).To(gomega.ContainSubstring("s3://my-bucket/data"))
			gomega.Expect(config).To(gomega.ContainSubstring("0.0.0.0:2049"))
		})

		ginkgo.It("should include encryption password from secrets", func() {
			secrets := map[string]string{
				"encryptionPassword": "my-secret-password",
			}
			config := manager.generateConfig("s3://my-bucket/data", nil, secrets)
			gomega.Expect(config).To(gomega.ContainSubstring("my-secret-password"))
		})

		ginkgo.It("should use custom cache settings from params", func() {
			params := map[string]string{
				"cacheDir":    "/custom/cache",
				"cacheSizeGB": "20",
			}
			config := manager.generateConfig("s3://my-bucket/data", params, nil)
			gomega.Expect(config).To(gomega.ContainSubstring("/custom/cache"))
			gomega.Expect(config).To(gomega.ContainSubstring("disk_size_gb = 20"))
		})
	})

	ginkgo.Context("buildDeployment", func() {
		ginkgo.It("should build a valid deployment", func() {
			deployment := manager.buildDeployment("zerofs-pvc-12345", "pvc-12345", "zerofs-config-pvc-12345", 1024*1024*1024)

			gomega.Expect(deployment.Name).To(gomega.Equal("zerofs-pvc-12345"))
			gomega.Expect(deployment.Namespace).To(gomega.Equal("default"))
			gomega.Expect(*deployment.Spec.Replicas).To(gomega.Equal(int32(1)))
			gomega.Expect(deployment.Spec.Template.Spec.Containers).To(gomega.HaveLen(1))

			container := deployment.Spec.Template.Spec.Containers[0]
			gomega.Expect(container.Name).To(gomega.Equal("zerofs"))
			gomega.Expect(container.Image).To(gomega.Equal("zerofs:latest"))
			gomega.Expect(container.Ports).To(gomega.HaveLen(2))
		})

		ginkgo.It("should set correct labels on deployment", func() {
			deployment := manager.buildDeployment("zerofs-pvc-12345", "pvc-12345", "zerofs-config-pvc-12345", 0)

			labels := deployment.Labels
			gomega.Expect(labels[AppLabel]).To(gomega.Equal("zerofs-server"))
			gomega.Expect(labels[ComponentLabel]).To(gomega.Equal("server"))
			gomega.Expect(labels[VolumeLabel]).To(gomega.Equal("pvc-12345"))
		})

		ginkgo.It("should configure probes", func() {
			deployment := manager.buildDeployment("zerofs-pvc-12345", "pvc-12345", "zerofs-config-pvc-12345", 0)

			container := deployment.Spec.Template.Spec.Containers[0]
			gomega.Expect(container.ReadinessProbe).NotTo(gomega.BeNil())
			gomega.Expect(container.LivenessProbe).NotTo(gomega.BeNil())
			gomega.Expect(container.ReadinessProbe.HTTPGet.Path).To(gomega.Equal("/healthz"))
			gomega.Expect(container.LivenessProbe.HTTPGet.Path).To(gomega.Equal("/healthz"))
		})

		ginkgo.It("should mount config and data volumes", func() {
			deployment := manager.buildDeployment("zerofs-pvc-12345", "pvc-12345", "zerofs-config-pvc-12345", 0)

			container := deployment.Spec.Template.Spec.Containers[0]
			gomega.Expect(container.VolumeMounts).To(gomega.HaveLen(2))

			volumeMounts := make(map[string]string)
			for _, vm := range container.VolumeMounts {
				volumeMounts[vm.Name] = vm.MountPath
			}
			gomega.Expect(volumeMounts[ConfigVolume]).To(gomega.Equal(ConfigPath))
			gomega.Expect(volumeMounts[DataVolume]).To(gomega.Equal(DataPath))
		})
	})

	ginkgo.Context("buildService", func() {
		ginkgo.It("should build a valid service", func() {
			service := manager.buildService("zerofs-pvc-12345", "pvc-12345")

			gomega.Expect(service.Name).To(gomega.Equal("zerofs-pvc-12345"))
			gomega.Expect(service.Namespace).To(gomega.Equal("default"))
			gomega.Expect(service.Spec.Type).To(gomega.Equal(corev1.ServiceTypeClusterIP))
			gomega.Expect(service.Spec.Ports).To(gomega.HaveLen(2))
		})

		ginkgo.It("should expose NFS and health ports", func() {
			service := manager.buildService("zerofs-pvc-12345", "pvc-12345")

			ports := make(map[string]int32)
			for _, p := range service.Spec.Ports {
				ports[p.Name] = p.Port
			}
			gomega.Expect(ports["nfs"]).To(gomega.Equal(int32(NFSPort)))
			gomega.Expect(ports["health"]).To(gomega.Equal(int32(HealthPort)))
		})

		ginkgo.It("should set correct labels on service", func() {
			service := manager.buildService("zerofs-pvc-12345", "pvc-12345")

			labels := service.Labels
			gomega.Expect(labels[AppLabel]).To(gomega.Equal("zerofs-server"))
			gomega.Expect(labels[ComponentLabel]).To(gomega.Equal("server"))
			gomega.Expect(labels[VolumeLabel]).To(gomega.Equal("pvc-12345"))
		})
	})
})
