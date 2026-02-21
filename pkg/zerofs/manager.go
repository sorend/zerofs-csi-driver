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
	RPCPort        = 7000
	HealthPort     = 8080
	ConfigVolume   = "zerofs-config"
	DataVolume     = "zerofs-data"
	ConfigPath     = "/etc/zerofs"
	DataPath       = "/data"
	AppLabel       = "app.kubernetes.io/name"
	ComponentLabel = "app.kubernetes.io/component"
	VolumeLabel    = "zerofs.csi.sorend.github.com/volume-id"
	ProtocolLabel  = "zerofs.csi.sorend.github.com/protocol"
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
	k8sClient   kubernetes.Interface
	// newS3ClientFn constructs an S3 API client from the given config.
	// It is a field so tests can inject a mock without a real S3 endpoint.
	newS3ClientFn func(cfg S3ClientConfig) s3API
}

func NewManager(namespace, workDir, zerofsImage string) *Manager {
	return &Manager{
		namespace:     namespace,
		workDir:       workDir,
		zerofsImage:   zerofsImage,
		newS3ClientFn: func(cfg S3ClientConfig) s3API { return newS3Client(cfg) },
	}
}

func (m *Manager) SetClient(client kubernetes.Interface) {
	m.k8sClient = client
}

func (m *Manager) GetServiceName(volumeID string) string {
	return fmt.Sprintf("zerofs-%s", volumeID)
}

func (m *Manager) GetDeploymentName(volumeID string) string {
	return fmt.Sprintf("zerofs-%s", volumeID)
}

func (m *Manager) GetSecretName(volumeID string) string {
	return fmt.Sprintf("zerofs-config-%s", volumeID)
}

func (m *Manager) CreateZerofsDeployment(ctx context.Context, volumeID, storageURL string, protocol Protocol, nodeName string, params, secrets map[string]string, size int64) (string, string, error) {
	klog.V(4).Infof("Creating ZeroFS deployment for volume %s with protocol %s", volumeID, protocol)

	deploymentName := m.GetDeploymentName(volumeID)
	serviceName := m.GetServiceName(volumeID)
	secretName := m.GetSecretName(volumeID)

	configData := m.generateConfigWithContext(ctx, storageURL, protocol, params, secrets)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: m.namespace,
			Labels: map[string]string{
				AppLabel:       "zerofs-server",
				ComponentLabel: "server",
				VolumeLabel:    volumeID,
				ProtocolLabel:  string(protocol),
			},
		},
		Data: map[string][]byte{
			"zerofs.toml": []byte(configData),
		},
	}
	metadataAnnotations := m.buildVolumeAnnotations(storageURL, protocol, nodeName, size, params)
	m.mergeMetadata(&secret.ObjectMeta, secret.Labels, metadataAnnotations)

	if err := m.upsertSecret(ctx, secret); err != nil {
		return "", "", fmt.Errorf("failed to create secret: %w", err)
	}

	deployment := m.buildDeployment(deploymentName, volumeID, secretName, protocol, nodeName, size)
	m.mergeMetadata(&deployment.ObjectMeta, deployment.Labels, metadataAnnotations)
	if err := m.upsertDeployment(ctx, deployment); err != nil {
		return "", "", fmt.Errorf("failed to create deployment: %w", err)
	}

	// Create the Service; if it already exists (idempotent retry), fetch its ClusterIP instead.
	// A nil svc would cause a panic below, so we must always have a valid object.
	service := m.buildService(serviceName, volumeID, protocol)
	svc, err := m.k8sClient.CoreV1().Services(m.namespace).Create(ctx, service, metav1.CreateOptions{})
	if err != nil {
		if !errors.IsAlreadyExists(err) {
			return "", "", fmt.Errorf("failed to create service: %w", err)
		}
		// Service already exists — fetch it so we can read the ClusterIP.
		svc, err = m.k8sClient.CoreV1().Services(m.namespace).Get(ctx, serviceName, metav1.GetOptions{})
		if err != nil {
			return "", "", fmt.Errorf("failed to get existing service: %w", err)
		}
	}

	var podIP string
	if protocol == ProtocolNinep {
		podIP, err = m.waitForPodIP(ctx, volumeID)
		if err != nil {
			// waitForPodIP failed (e.g. context cancelled or pod never became ready).
			// Roll back the resources we just created so we don't leave orphans behind.
			klog.Warningf("waitForPodIP failed for volume %s, rolling back created resources: %v", volumeID, err)
			if rbErr := m.rollbackDeployment(ctx, volumeID); rbErr != nil {
				klog.Errorf("Rollback after waitForPodIP failure also failed for volume %s: %v", volumeID, rbErr)
			}
			return "", "", fmt.Errorf("failed to get pod IP: %w", err)
		}
	}

	klog.V(4).Infof("Successfully created ZeroFS deployment for volume %s", volumeID)
	return svc.Spec.ClusterIP, podIP, nil
}

// rollbackDeployment deletes the Service, Deployment, and Secret created for a volume.
// It is called during CreateZerofsDeployment when a late-stage step fails, in order to
// avoid leaving orphaned resources. Errors are logged but not propagated so callers
// always see the original creation error.
func (m *Manager) rollbackDeployment(ctx context.Context, volumeID string) error {
	deploymentName := m.GetDeploymentName(volumeID)
	serviceName := m.GetServiceName(volumeID)
	secretName := m.GetSecretName(volumeID)

	var firstErr error
	if err := m.k8sClient.CoreV1().Services(m.namespace).Delete(ctx, serviceName, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
		klog.Warningf("Rollback: failed to delete service %s: %v", serviceName, err)
		firstErr = err
	}
	if err := m.k8sClient.AppsV1().Deployments(m.namespace).Delete(ctx, deploymentName, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
		klog.Warningf("Rollback: failed to delete deployment %s: %v", deploymentName, err)
		if firstErr == nil {
			firstErr = err
		}
	}
	if err := m.k8sClient.CoreV1().Secrets(m.namespace).Delete(ctx, secretName, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
		klog.Warningf("Rollback: failed to delete secret %s: %v", secretName, err)
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
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
	secretName := m.GetSecretName(volumeID)

	// Delete S3 data before removing Kubernetes resources so that we can still
	// read the stored storageURL and AWS credential annotations from the Deployment.
	// deleteS3Data returns nil immediately if the Deployment is already gone, which
	// makes the whole function idempotent.
	if err := m.deleteS3Data(ctx, volumeID); err != nil {
		// Log but do not block deletion of the Kubernetes resources.
		klog.Warningf("Failed to delete S3 data for volume %s: %v", volumeID, err)
	}

	// Scale the Deployment to 0 replicas before deleting it so that the ZeroFS
	// server pod gets a graceful shutdown window.  Active NFS/9P clients will see
	// their connections closed cleanly rather than the pod disappearing mid-request.
	// We wait up to 30 seconds for the pods to terminate; if the wait times out we
	// log a warning and proceed anyway so that deletion is never blocked permanently.
	if err := m.scaleDeploymentToZero(ctx, deploymentName); err != nil {
		// Non-fatal: the Deployment may already be gone or the scale may fail for
		// a transient reason.  We still carry on with the delete.
		klog.Warningf("Failed to scale deployment %s to zero before deletion: %v", deploymentName, err)
	} else {
		if err := m.waitForPodsGone(ctx, volumeID, 30*time.Second); err != nil {
			klog.Warningf("Timed out waiting for pods of volume %s to terminate; proceeding with deletion: %v", volumeID, err)
		}
	}

	err := m.k8sClient.CoreV1().Services(m.namespace).Delete(ctx, serviceName, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete service: %w", err)
	}

	err = m.k8sClient.AppsV1().Deployments(m.namespace).Delete(ctx, deploymentName, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete deployment: %w", err)
	}

	err = m.k8sClient.CoreV1().Secrets(m.namespace).Delete(ctx, secretName, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete secret: %w", err)
	}

	klog.V(4).Infof("Successfully deleted ZeroFS deployment for volume %s", volumeID)
	return nil
}

// scaleDeploymentToZero patches the named Deployment's replica count to 0.
// If the Deployment is not found it returns nil (idempotent).
func (m *Manager) scaleDeploymentToZero(ctx context.Context, deploymentName string) error {
	deployment, err := m.k8sClient.AppsV1().Deployments(m.namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get deployment %s: %w", deploymentName, err)
	}

	zero := int32(0)
	deployment.Spec.Replicas = &zero
	_, err = m.k8sClient.AppsV1().Deployments(m.namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to scale deployment %s to zero: %w", deploymentName, err)
	}
	return nil
}

// waitForPodsGone polls until no running pods exist for the given volumeID, or
// until the timeout elapses.  It uses a detached context with its own deadline
// so that caller's context cancellation does not prevent us from waiting the
// full window we promised.
func (m *Manager) waitForPodsGone(ctx context.Context, volumeID string, timeout time.Duration) error {
	labels := fmt.Sprintf("%s=%s,%s=%s,%s=%s",
		AppLabel, "zerofs-server",
		ComponentLabel, "server",
		VolumeLabel, volumeID)

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return wait.PollUntilContextTimeout(waitCtx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		pods, err := m.k8sClient.CoreV1().Pods(m.namespace).List(ctx, metav1.ListOptions{
			LabelSelector: labels,
		})
		if err != nil {
			// Transient API error — keep polling.
			klog.V(4).Infof("Transient error listing pods for volume %s during drain wait: %v", volumeID, err)
			return false, nil
		}
		if len(pods.Items) == 0 {
			return true, nil
		}
		klog.V(4).Infof("Waiting for %d pod(s) of volume %s to terminate", len(pods.Items), volumeID)
		return false, nil
	})
}

// deleteS3Data reads the volume metadata stored on the Kubernetes Deployment
// (written during CreateVolume) and uses it to recursively delete all S3
// objects under the volume's storage prefix.
func (m *Manager) deleteS3Data(ctx context.Context, volumeID string) error {
	deploymentName := m.GetDeploymentName(volumeID)
	deployment, err := m.k8sClient.AppsV1().Deployments(m.namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// Deployment already gone; nothing to clean up.
			return nil
		}
		return fmt.Errorf("failed to get deployment to read S3 metadata: %w", err)
	}

	annotations := deployment.Annotations
	if annotations == nil {
		klog.V(4).Infof("No annotations on deployment for volume %s; skipping S3 deletion", volumeID)
		return nil
	}

	storageURL := annotations[AnnotationStorageURL]
	if storageURL == "" {
		klog.V(4).Infof("No storageURL annotation on deployment for volume %s; skipping S3 deletion", volumeID)
		return nil
	}

	loc, err := parseS3URL(storageURL)
	if err != nil {
		return fmt.Errorf("cannot parse storage URL %q: %w", storageURL, err)
	}

	// Reconstruct S3 client config from annotations stored at CreateVolume time.
	s3Cfg := S3ClientConfig{
		Endpoint:  annotations[AnnotationAWSEndpoint],
		AllowHTTP: annotations[AnnotationAWSAllowHTTP] != "false",
	}

	// Fetch credentials from the referenced K8s Secret if one was recorded.
	if secretName := annotations[AnnotationAWSSecretName]; secretName != "" {
		accessKey, secretKey, err := m.getAWSCredentialsFromSecret(ctx, secretName)
		if err != nil {
			klog.Warningf("Failed to fetch AWS credentials from secret %s for volume %s deletion: %v", secretName, volumeID, err)
		} else {
			s3Cfg.AccessKeyID = accessKey
			s3Cfg.SecretAccessKey = secretKey
		}
	}

	client := m.newS3ClientFn(s3Cfg)
	klog.V(4).Infof("Deleting S3 data for volume %s at s3://%s/%s", volumeID, loc.Bucket, loc.Prefix)
	return deleteS3Prefix(ctx, client, loc.Bucket, loc.Prefix)
}

func (m *Manager) getAWSCredentialsFromSecret(ctx context.Context, secretName string) (string, string, error) {
	if m.k8sClient == nil {
		return "", "", fmt.Errorf("kubernetes client not initialized")
	}
	secret, err := m.k8sClient.CoreV1().Secrets(m.namespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return "", "", fmt.Errorf("failed to get secret %s: %w", secretName, err)
	}
	accessKey := string(secret.Data["awsAccessKeyID"])
	secretKey := string(secret.Data["awsSecretAccessKey"])
	if accessKey == "" || secretKey == "" {
		return "", "", fmt.Errorf("secret %s must contain awsAccessKeyID and awsSecretAccessKey keys", secretName)
	}
	return accessKey, secretKey, nil
}

func (m *Manager) generateConfig(storageURL string, protocol Protocol, params, secrets map[string]string) string {
	return m.generateConfigWithContext(context.Background(), storageURL, protocol, params, secrets)
}

func (m *Manager) generateConfigWithContext(ctx context.Context, storageURL string, protocol Protocol, params, secrets map[string]string) string {
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

	var awsAccessKey, awsSecretKey string
	awsEndpoint := params["awsEndpoint"]
	awsAllowHTTP := params["awsAllowHTTP"]
	if awsAllowHTTP == "" {
		awsAllowHTTP = "true"
	}

	if secretName := params["awsSecretName"]; secretName != "" {
		var err error
		awsAccessKey, awsSecretKey, err = m.getAWSCredentialsFromSecret(ctx, secretName)
		if err != nil {
			klog.Warningf("Failed to get AWS credentials from secret %s: %v", secretName, err)
		}
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

func (m *Manager) buildDeployment(name, volumeID, secretName string, protocol Protocol, nodeName string, size int64) *appsv1.Deployment {
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
					Secret: &corev1.SecretVolumeSource{
						SecretName: secretName,
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
					Name:       "rpc",
					Port:       RPCPort,
					TargetPort: intstr.FromInt(RPCPort),
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
