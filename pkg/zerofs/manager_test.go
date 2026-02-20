package zerofs

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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
		manager = NewManager("default", "/var/lib/zerofs-csi", "ghcr.io/barre/zerofs:1.0.4")
	})

	ginkgo.Context("NewManager", func() {
		ginkgo.It("should create a manager with correct settings", func() {
			gomega.Expect(manager.namespace).To(gomega.Equal("default"))
			gomega.Expect(manager.workDir).To(gomega.Equal("/var/lib/zerofs-csi"))
			gomega.Expect(manager.zerofsImage).To(gomega.Equal("ghcr.io/barre/zerofs:1.0.4"))
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

	ginkgo.Context("GetSecretName", func() {
		ginkgo.It("should generate correct secret name", func() {
			name := manager.GetSecretName("pvc-12345")
			gomega.Expect(name).To(gomega.Equal("zerofs-config-pvc-12345"))
		})
	})

	ginkgo.Context("generateConfig", func() {
		ginkgo.It("should generate valid config with defaults for NFS", func() {
			config := manager.generateConfig("s3://my-bucket/data", ProtocolNFS, nil, nil)
			gomega.Expect(config).To(gomega.ContainSubstring("s3://my-bucket/data"))
			gomega.Expect(config).To(gomega.ContainSubstring("0.0.0.0:2049"))
		})

		ginkgo.It("should generate valid config for 9P protocol", func() {
			config := manager.generateConfig("s3://my-bucket/data", ProtocolNinep, nil, nil)
			gomega.Expect(config).To(gomega.ContainSubstring("s3://my-bucket/data"))
			gomega.Expect(config).To(gomega.ContainSubstring("0.0.0.0:5564"))
		})

		ginkgo.It("should include encryption password from secrets", func() {
			secrets := map[string]string{
				"encryptionPassword": "my-secret-password",
			}
			config := manager.generateConfig("s3://my-bucket/data", ProtocolNFS, nil, secrets)
			gomega.Expect(config).To(gomega.ContainSubstring("my-secret-password"))
		})

		ginkgo.It("should use custom cache settings from params", func() {
			params := map[string]string{
				"cacheDir":    "/custom/cache",
				"cacheSizeGB": "20",
			}
			config := manager.generateConfig("s3://my-bucket/data", ProtocolNFS, params, nil)
			gomega.Expect(config).To(gomega.ContainSubstring("/custom/cache"))
			gomega.Expect(config).To(gomega.ContainSubstring("disk_size_gb = 20"))
		})

		ginkgo.It("should include AWS credentials from params", func() {
			params := map[string]string{
				"awsAccessKeyID":     "test-access-key",
				"awsSecretAccessKey": "test-secret-key",
				"awsEndpoint":        "http://minio:9000",
			}
			config := manager.generateConfig("s3://my-bucket/data", ProtocolNFS, params, nil)
			gomega.Expect(config).To(gomega.ContainSubstring("test-access-key"))
			gomega.Expect(config).To(gomega.ContainSubstring("test-secret-key"))
			gomega.Expect(config).To(gomega.ContainSubstring("http://minio:9000"))
		})

		ginkgo.It("should fetch AWS credentials from secret when awsSecretName is specified", func() {
			fakeClient := fake.NewSimpleClientset()
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "aws-credentials",
					Namespace: "default",
				},
				Data: map[string][]byte{
					"awsAccessKeyID":     []byte("secret-access-key"),
					"awsSecretAccessKey": []byte("secret-secret-key"),
				},
			}
			_, err := fakeClient.CoreV1().Secrets("default").Create(context.Background(), secret, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			manager.SetClient(fakeClient)
			params := map[string]string{
				"awsSecretName": "aws-credentials",
			}
			config := manager.generateConfigWithContext(context.Background(), "s3://my-bucket/data", ProtocolNFS, params, nil)
			gomega.Expect(config).To(gomega.ContainSubstring("secret-access-key"))
			gomega.Expect(config).To(gomega.ContainSubstring("secret-secret-key"))
		})

		ginkgo.It("should fallback to params when secret fetch fails", func() {
			fakeClient := fake.NewSimpleClientset()
			manager.SetClient(fakeClient)

			params := map[string]string{
				"awsSecretName":      "non-existent-secret",
				"awsAccessKeyID":     "fallback-access-key",
				"awsSecretAccessKey": "fallback-secret-key",
			}
			config := manager.generateConfigWithContext(context.Background(), "s3://my-bucket/data", ProtocolNFS, params, nil)
			gomega.Expect(config).To(gomega.ContainSubstring("fallback-access-key"))
			gomega.Expect(config).To(gomega.ContainSubstring("fallback-secret-key"))
		})

		ginkgo.It("should prefer secret over params for AWS credentials", func() {
			fakeClient := fake.NewSimpleClientset()
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "aws-credentials",
					Namespace: "default",
				},
				Data: map[string][]byte{
					"awsAccessKeyID":     []byte("from-secret-key"),
					"awsSecretAccessKey": []byte("from-secret-value"),
				},
			}
			_, err := fakeClient.CoreV1().Secrets("default").Create(context.Background(), secret, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			manager.SetClient(fakeClient)
			params := map[string]string{
				"awsSecretName":      "aws-credentials",
				"awsAccessKeyID":     "from-param-key",
				"awsSecretAccessKey": "from-param-value",
			}
			config := manager.generateConfigWithContext(context.Background(), "s3://my-bucket/data", ProtocolNFS, params, nil)
			gomega.Expect(config).To(gomega.ContainSubstring("from-secret-key"))
			gomega.Expect(config).To(gomega.ContainSubstring("from-secret-value"))
			gomega.Expect(config).NotTo(gomega.ContainSubstring("from-param-key"))
		})
	})

	ginkgo.Context("buildDeployment", func() {
		ginkgo.It("should build a valid deployment for NFS", func() {
			deployment := manager.buildDeployment("zerofs-pvc-12345", "pvc-12345", "zerofs-config-pvc-12345", ProtocolNFS, "", 1024*1024*1024)

			gomega.Expect(deployment.Name).To(gomega.Equal("zerofs-pvc-12345"))
			gomega.Expect(deployment.Namespace).To(gomega.Equal("default"))
			gomega.Expect(*deployment.Spec.Replicas).To(gomega.Equal(int32(1)))
			gomega.Expect(deployment.Spec.Template.Spec.Containers).To(gomega.HaveLen(1))

			container := deployment.Spec.Template.Spec.Containers[0]
			gomega.Expect(container.Name).To(gomega.Equal("zerofs"))
			gomega.Expect(container.Image).To(gomega.Equal("ghcr.io/barre/zerofs:1.0.4"))
			gomega.Expect(container.Ports).To(gomega.HaveLen(1))
		})

		ginkgo.It("should build a valid deployment for 9P", func() {
			deployment := manager.buildDeployment("zerofs-pvc-12345", "pvc-12345", "zerofs-config-pvc-12345", ProtocolNinep, "", 1024*1024*1024)

			container := deployment.Spec.Template.Spec.Containers[0]
			gomega.Expect(container.Ports[0].Name).To(gomega.Equal("ninep"))
			gomega.Expect(container.Ports[0].ContainerPort).To(gomega.Equal(int32(NinepPort)))
		})

		ginkgo.It("should set nodeName for 9P protocol", func() {
			deployment := manager.buildDeployment("zerofs-pvc-12345", "pvc-12345", "zerofs-config-pvc-12345", ProtocolNinep, "node-1", 1024*1024*1024)
			gomega.Expect(deployment.Spec.Template.Spec.NodeName).To(gomega.Equal("node-1"))
		})

		ginkgo.It("should set correct labels on deployment", func() {
			deployment := manager.buildDeployment("zerofs-pvc-12345", "pvc-12345", "zerofs-config-pvc-12345", ProtocolNFS, "", 0)

			labels := deployment.Labels
			gomega.Expect(labels[AppLabel]).To(gomega.Equal("zerofs-server"))
			gomega.Expect(labels[ComponentLabel]).To(gomega.Equal("server"))
			gomega.Expect(labels[VolumeLabel]).To(gomega.Equal("pvc-12345"))
			gomega.Expect(labels[ProtocolLabel]).To(gomega.Equal("nfs"))
		})

		ginkgo.It("should configure probes", func() {
			deployment := manager.buildDeployment("zerofs-pvc-12345", "pvc-12345", "zerofs-config-pvc-12345", ProtocolNFS, "", 0)

			container := deployment.Spec.Template.Spec.Containers[0]
			gomega.Expect(container.ReadinessProbe).NotTo(gomega.BeNil())
			gomega.Expect(container.LivenessProbe).NotTo(gomega.BeNil())
			gomega.Expect(container.ReadinessProbe.TCPSocket).NotTo(gomega.BeNil())
		})

		ginkgo.It("should mount config and data volumes", func() {
			deployment := manager.buildDeployment("zerofs-pvc-12345", "pvc-12345", "zerofs-config-pvc-12345", ProtocolNFS, "", 0)

			container := deployment.Spec.Template.Spec.Containers[0]
			gomega.Expect(container.VolumeMounts).To(gomega.HaveLen(2))

			volumeMounts := make(map[string]string)
			for _, vm := range container.VolumeMounts {
				volumeMounts[vm.Name] = vm.MountPath
			}
			gomega.Expect(volumeMounts[ConfigVolume]).To(gomega.Equal(ConfigPath))
			gomega.Expect(volumeMounts[DataVolume]).To(gomega.Equal(DataPath))
		})

		ginkgo.It("should use Secret volume source for config", func() {
			deployment := manager.buildDeployment("zerofs-pvc-12345", "pvc-12345", "zerofs-config-pvc-12345", ProtocolNFS, "", 0)

			var configVolume *corev1.Volume
			for _, v := range deployment.Spec.Template.Spec.Volumes {
				if v.Name == ConfigVolume {
					configVolume = &v
					break
				}
			}
			gomega.Expect(configVolume).NotTo(gomega.BeNil())
			gomega.Expect(configVolume.Secret).NotTo(gomega.BeNil())
			gomega.Expect(configVolume.Secret.SecretName).To(gomega.Equal("zerofs-config-pvc-12345"))
			gomega.Expect(configVolume.ConfigMap).To(gomega.BeNil())
		})
	})

	ginkgo.Context("buildService", func() {
		ginkgo.It("should build a valid service for NFS", func() {
			service := manager.buildService("zerofs-pvc-12345", "pvc-12345", ProtocolNFS)

			gomega.Expect(service.Name).To(gomega.Equal("zerofs-pvc-12345"))
			gomega.Expect(service.Namespace).To(gomega.Equal("default"))
			gomega.Expect(service.Spec.Type).To(gomega.Equal(corev1.ServiceTypeClusterIP))
			gomega.Expect(service.Spec.Ports).To(gomega.HaveLen(3))
		})

		ginkgo.It("should expose NFS and health ports", func() {
			service := manager.buildService("zerofs-pvc-12345", "pvc-12345", ProtocolNFS)

			ports := make(map[string]int32)
			for _, p := range service.Spec.Ports {
				ports[p.Name] = p.Port
			}
			gomega.Expect(ports["nfs"]).To(gomega.Equal(int32(NFSPort)))
			gomega.Expect(ports["rpc"]).To(gomega.Equal(int32(RPCPort)))
			gomega.Expect(ports["health"]).To(gomega.Equal(int32(HealthPort)))
		})

		ginkgo.It("should expose 9P port for ninep protocol", func() {
			service := manager.buildService("zerofs-pvc-12345", "pvc-12345", ProtocolNinep)

			ports := make(map[string]int32)
			for _, p := range service.Spec.Ports {
				ports[p.Name] = p.Port
			}
			gomega.Expect(ports["ninep"]).To(gomega.Equal(int32(NinepPort)))
		})

		ginkgo.It("should set correct labels on service", func() {
			service := manager.buildService("zerofs-pvc-12345", "pvc-12345", ProtocolNFS)

			labels := service.Labels
			gomega.Expect(labels[AppLabel]).To(gomega.Equal("zerofs-server"))
			gomega.Expect(labels[ComponentLabel]).To(gomega.Equal("server"))
			gomega.Expect(labels[VolumeLabel]).To(gomega.Equal("pvc-12345"))
			gomega.Expect(labels[ProtocolLabel]).To(gomega.Equal("nfs"))
		})
	})
})
