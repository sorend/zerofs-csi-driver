package zerofs

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

const (
	NFSPort        = 2049
	HealthPort     = 8080
	ConfigVolume   = "zerofs-config"
	DataVolume     = "zerofs-data"
	ConfigPath     = "/etc/zerofs"
	DataPath       = "/data"
	AppLabel       = "app.kubernetes.io/name"
	ComponentLabel = "app.kubernetes.io/component"
	VolumeLabel    = "zerofs.csi.k8s.io/volume-id"
)

type Manager struct {
	namespace   string
	workDir     string
	zerofsImage string
	k8sClient   *kubernetes.Clientset
}

func NewManager(namespace, workDir, zerofsImage string) *Manager {
	return &Manager{
		namespace:   namespace,
		workDir:     workDir,
		zerofsImage: zerofsImage,
	}
}

func (m *Manager) SetClient(client *kubernetes.Clientset) {
	m.k8sClient = client
}

func (m *Manager) GetServiceName(volumeID string) string {
	return fmt.Sprintf("zerofs-%s", volumeID)
}

func (m *Manager) GetDeploymentName(volumeID string) string {
	return fmt.Sprintf("zerofs-%s", volumeID)
}

func (m *Manager) GetConfigMapName(volumeID string) string {
	return fmt.Sprintf("zerofs-config-%s", volumeID)
}

func (m *Manager) CreateZerofsDeployment(ctx context.Context, volumeID, storageURL string, params, secrets map[string]string, size int64) error {
	klog.V(4).Infof("Creating ZeroFS deployment for volume %s", volumeID)

	deploymentName := m.GetDeploymentName(volumeID)
	serviceName := m.GetServiceName(volumeID)
	configMapName := m.GetConfigMapName(volumeID)

	configData := m.generateConfig(storageURL, params, secrets)
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: m.namespace,
			Labels: map[string]string{
				AppLabel:       "zerofs-server",
				ComponentLabel: "server",
				VolumeLabel:    volumeID,
			},
		},
		Data: map[string]string{
			"zerofs.toml": configData,
		},
	}

	_, err := m.k8sClient.CoreV1().ConfigMaps(m.namespace).Create(ctx, configMap, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create configmap: %w", err)
	}

	deployment := m.buildDeployment(deploymentName, volumeID, configMapName, size)
	_, err = m.k8sClient.AppsV1().Deployments(m.namespace).Create(ctx, deployment, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create deployment: %w", err)
	}

	service := m.buildService(serviceName, volumeID)
	_, err = m.k8sClient.CoreV1().Services(m.namespace).Create(ctx, service, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create service: %w", err)
	}

	klog.V(4).Infof("Successfully created ZeroFS deployment for volume %s", volumeID)
	return nil
}

func (m *Manager) DeleteZerofsDeployment(ctx context.Context, volumeID string) error {
	klog.V(4).Infof("Deleting ZeroFS deployment for volume %s", volumeID)

	deploymentName := m.GetDeploymentName(volumeID)
	serviceName := m.GetServiceName(volumeID)
	configMapName := m.GetConfigMapName(volumeID)

	err := m.k8sClient.CoreV1().Services(m.namespace).Delete(ctx, serviceName, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete service: %w", err)
	}

	err = m.k8sClient.AppsV1().Deployments(m.namespace).Delete(ctx, deploymentName, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete deployment: %w", err)
	}

	err = m.k8sClient.CoreV1().ConfigMaps(m.namespace).Delete(ctx, configMapName, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete configmap: %w", err)
	}

	klog.V(4).Infof("Successfully deleted ZeroFS deployment for volume %s", volumeID)
	return nil
}

func (m *Manager) generateConfig(storageURL string, params, secrets map[string]string) string {
	encryptionPassword := ""
	if secrets != nil {
		encryptionPassword = secrets["encryptionPassword"]
	}

	cacheDir := params["cacheDir"]
	if cacheDir == "" {
		cacheDir = "/var/lib/zerofs/cache"
	}

	cacheSizeGB := params["cacheSizeGB"]
	if cacheSizeGB == "" {
		cacheSizeGB = "10"
	}

	config := fmt.Sprintf(`[cache]
dir = "%s"
disk_size_gb = %s

[storage]
url = "%s"
encryption_password = "%s"

[servers.nfs]
addresses = ["0.0.0.0:2049"]

[servers.rpc]
addresses = ["0.0.0.0:7000"]
`, cacheDir, cacheSizeGB, storageURL, encryptionPassword)

	return config
}

func (m *Manager) buildDeployment(name, volumeID, configMapName string, size int64) *appsv1.Deployment {
	labels := map[string]string{
		AppLabel:       "zerofs-server",
		ComponentLabel: "server",
		VolumeLabel:    volumeID,
	}

	replicas := int32(1)

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: m.namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "zerofs",
							Image: m.zerofsImage,
							Command: []string{
								"/usr/local/bin/zerofs",
								"server",
								"--config",
								"/etc/zerofs/zerofs.toml",
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          "nfs",
									ContainerPort: NFSPort,
									Protocol:      corev1.ProtocolTCP,
								},
								{
									Name:          "health",
									ContainerPort: HealthPort,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      ConfigVolume,
									MountPath: ConfigPath,
									ReadOnly:  true,
								},
								{
									Name:      DataVolume,
									MountPath: DataPath,
								},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/healthz",
										Port: intstr.FromInt(HealthPort),
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       10,
								FailureThreshold:    3,
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/healthz",
										Port: intstr.FromInt(HealthPort),
									},
								},
								InitialDelaySeconds: 10,
								PeriodSeconds:       30,
								FailureThreshold:    3,
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("256Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("1000m"),
									corev1.ResourceMemory: resource.MustParse("1Gi"),
								},
							},
							SecurityContext: &corev1.SecurityContext{
								Capabilities: &corev1.Capabilities{
									Add: []corev1.Capability{"SYS_ADMIN"},
								},
								Privileged: ptr.To(true),
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: ConfigVolume,
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: configMapName,
									},
								},
							},
						},
						{
							Name: DataVolume,
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
		},
	}
}

func (m *Manager) buildService(name, volumeID string) *corev1.Service {
	labels := map[string]string{
		AppLabel:       "zerofs-server",
		ComponentLabel: "server",
		VolumeLabel:    volumeID,
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: m.namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{
				{
					Name:       "nfs",
					Port:       NFSPort,
					TargetPort: intstr.FromInt(NFSPort),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "health",
					Port:       HealthPort,
					TargetPort: intstr.FromInt(HealthPort),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Type: corev1.ServiceTypeClusterIP,
		},
	}
}
