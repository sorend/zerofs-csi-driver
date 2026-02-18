package zerofs

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

const (
	NFSPort        = 2049
	NinepPort      = 5564
	HealthPort     = 8080
	ConfigVolume   = "zerofs-config"
	DataVolume     = "zerofs-data"
	ConfigPath     = "/etc/zerofs"
	DataPath       = "/data"
	AppLabel       = "app.kubernetes.io/name"
	ComponentLabel = "app.kubernetes.io/component"
	VolumeLabel    = "zerofs.csi.k8s.io/volume-id"
	ProtocolLabel  = "zerofs.csi.k8s.io/protocol"
)

type Protocol string

const (
	ProtocolNFS   Protocol = "nfs"
	ProtocolNinep Protocol = "ninep"
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

func (m *Manager) CreateZerofsDeployment(ctx context.Context, volumeID, storageURL string, protocol Protocol, nodeName string, params, secrets map[string]string, size int64) (string, string, error) {
	klog.V(4).Infof("Creating ZeroFS deployment for volume %s with protocol %s", volumeID, protocol)

	deploymentName := m.GetDeploymentName(volumeID)
	serviceName := m.GetServiceName(volumeID)
	configMapName := m.GetConfigMapName(volumeID)

	configData := m.generateConfig(storageURL, protocol, params, secrets)
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: m.namespace,
			Labels: map[string]string{
				AppLabel:       "zerofs-server",
				ComponentLabel: "server",
				VolumeLabel:    volumeID,
				ProtocolLabel:  string(protocol),
			},
		},
		Data: map[string]string{
			"zerofs.toml": configData,
		},
	}

	_, err := m.k8sClient.CoreV1().ConfigMaps(m.namespace).Create(ctx, configMap, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return "", "", fmt.Errorf("failed to create configmap: %w", err)
	}

	deployment := m.buildDeployment(deploymentName, volumeID, configMapName, protocol, nodeName, size)
	_, err = m.k8sClient.AppsV1().Deployments(m.namespace).Create(ctx, deployment, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return "", "", fmt.Errorf("failed to create deployment: %w", err)
	}

	service := m.buildService(serviceName, volumeID, protocol)
	svc, err := m.k8sClient.CoreV1().Services(m.namespace).Create(ctx, service, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return "", "", fmt.Errorf("failed to create service: %w", err)
	}

	var podIP string
	if protocol == ProtocolNinep {
		podIP, err = m.waitForPodIP(ctx, volumeID)
		if err != nil {
			return "", "", fmt.Errorf("failed to get pod IP: %w", err)
		}
	}

	klog.V(4).Infof("Successfully created ZeroFS deployment for volume %s", volumeID)
	return svc.Spec.ClusterIP, podIP, nil
}

func (m *Manager) waitForPodIP(ctx context.Context, volumeID string) (string, error) {
	labels := fmt.Sprintf("%s=%s,%s=%s,%s=%s",
		AppLabel, "zerofs-server",
		ComponentLabel, "server",
		VolumeLabel, volumeID)

	var podIP string
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 60*time.Second, true, func(ctx context.Context) (bool, error) {
		pods, err := m.k8sClient.CoreV1().Pods(m.namespace).List(ctx, metav1.ListOptions{
			LabelSelector: labels,
		})
		if err != nil {
			return false, err
		}
		if len(pods.Items) == 0 {
			return false, nil
		}
		pod := pods.Items[0]
		if pod.Status.PodIP == "" {
			return false, nil
		}
		podIP = pod.Status.PodIP
		return true, nil
	})
	return podIP, err
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

func (m *Manager) generateConfig(storageURL string, protocol Protocol, params, secrets map[string]string) string {
	encryptionPassword := ""
	if secrets != nil {
		encryptionPassword = secrets["encryptionPassword"]
	}
	if encryptionPassword == "" {
		encryptionPassword = params["encryptionPassword"]
	}
	if encryptionPassword == "" {
		encryptionPassword = "default-zerofs-encryption-key"
	}

	cacheDir := params["cacheDir"]
	if cacheDir == "" {
		cacheDir = "/var/lib/zerofs/cache"
	}

	cacheSizeGB := params["cacheSizeGB"]
	if cacheSizeGB == "" {
		cacheSizeGB = "10"
	}

	awsAccessKey := params["awsAccessKeyID"]
	awsSecretKey := params["awsSecretAccessKey"]
	awsEndpoint := params["awsEndpoint"]
	awsAllowHTTP := params["awsAllowHTTP"]
	if awsAllowHTTP == "" {
		awsAllowHTTP = "true"
	}

	awsSection := ""
	if awsAccessKey != "" && awsSecretKey != "" {
		awsSection = fmt.Sprintf(`
[aws]
access_key_id = "%s"
secret_access_key = "%s"
`, awsAccessKey, awsSecretKey)
		if awsEndpoint != "" {
			awsSection += fmt.Sprintf(`endpoint = "%s"
`, awsEndpoint)
		}
		awsSection += fmt.Sprintf(`allow_http = "%s"
`, awsAllowHTTP)
	}

	var serverSection string
	switch protocol {
	case ProtocolNinep:
		serverSection = `[servers.ninep]
addresses = ["0.0.0.0:5564"]

[servers.rpc]
addresses = ["0.0.0.0:7000"]
`
	default:
		serverSection = `[servers.nfs]
addresses = ["0.0.0.0:2049"]

[servers.rpc]
addresses = ["0.0.0.0:7000"]
`
	}

	config := fmt.Sprintf(`[cache]
dir = "%s"
disk_size_gb = %s

[storage]
url = "%s"
encryption_password = "%s"
%s%s`, cacheDir, cacheSizeGB, storageURL, encryptionPassword, awsSection, serverSection)

	return config
}

func (m *Manager) buildDeployment(name, volumeID, configMapName string, protocol Protocol, nodeName string, size int64) *appsv1.Deployment {
	labels := map[string]string{
		AppLabel:       "zerofs-server",
		ComponentLabel: "server",
		VolumeLabel:    volumeID,
		ProtocolLabel:  string(protocol),
	}

	replicas := int32(1)

	containerPort := int32(NFSPort)
	portName := "nfs"
	if protocol == ProtocolNinep {
		containerPort = NinepPort
		portName = "ninep"
	}

	podSpec := corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:  "zerofs",
				Image: m.zerofsImage,
				Command: []string{
					"/usr/local/bin/zerofs",
					"run",
					"--config",
					"/etc/zerofs/zerofs.toml",
				},
				Ports: []corev1.ContainerPort{
					{
						Name:          portName,
						ContainerPort: containerPort,
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
						TCPSocket: &corev1.TCPSocketAction{
							Port: intstr.FromInt(int(containerPort)),
						},
					},
					InitialDelaySeconds: 5,
					PeriodSeconds:       10,
					FailureThreshold:    3,
				},
				LivenessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						TCPSocket: &corev1.TCPSocketAction{
							Port: intstr.FromInt(int(containerPort)),
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
	}

	if nodeName != "" {
		podSpec.NodeName = nodeName
	}

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
				Spec: podSpec,
			},
		},
	}
}

func (m *Manager) buildService(name, volumeID string, protocol Protocol) *corev1.Service {
	labels := map[string]string{
		AppLabel:       "zerofs-server",
		ComponentLabel: "server",
		VolumeLabel:    volumeID,
		ProtocolLabel:  string(protocol),
	}

	servicePort := int32(NFSPort)
	portName := "nfs"
	if protocol == ProtocolNinep {
		servicePort = NinepPort
		portName = "ninep"
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
					Name:       portName,
					Port:       servicePort,
					TargetPort: intstr.FromInt(int(servicePort)),
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
