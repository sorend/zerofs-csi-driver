package zerofs

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	AnnotationStorageURL = "zerofs.csi.sorend.github.com/storage-url"
	AnnotationCapacity   = "zerofs.csi.sorend.github.com/capacity-bytes"
	AnnotationNodeName   = "zerofs.csi.sorend.github.com/node-name"
	AnnotationProtocol   = "zerofs.csi.sorend.github.com/protocol"
)

type VolumeMetadata struct {
	VolumeID      string
	StorageURL    string
	Protocol      Protocol
	CapacityBytes int64
	NodeName      string
	ServiceIP     string
	ServerName    string
}

func (m *Manager) buildVolumeAnnotations(storageURL string, protocol Protocol, nodeName string, size int64) map[string]string {
	annotations := map[string]string{}
	if storageURL != "" {
		annotations[AnnotationStorageURL] = storageURL
	}
	annotations[AnnotationCapacity] = strconv.FormatInt(size, 10)
	annotations[AnnotationProtocol] = string(protocol)
	if nodeName != "" {
		annotations[AnnotationNodeName] = nodeName
	}
	return annotations
}

func (m *Manager) mergeMetadata(meta *metav1.ObjectMeta, labels, annotations map[string]string) {
	if meta.Labels == nil {
		meta.Labels = map[string]string{}
	}
	for key, value := range labels {
		meta.Labels[key] = value
	}

	if meta.Annotations == nil {
		meta.Annotations = map[string]string{}
	}
	for key, value := range annotations {
		meta.Annotations[key] = value
	}
}

func (m *Manager) GetVolumeMetadata(ctx context.Context, volumeID string) (*VolumeMetadata, error) {
	if m.k8sClient == nil {
		return nil, fmt.Errorf("kubernetes client not initialized")
	}

	deploymentName := m.GetDeploymentName(volumeID)
	deployment, err := m.k8sClient.AppsV1().Deployments(m.namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	metadata := volumeMetadataFromDeployment(deployment)
	metadata.VolumeID = volumeID
	metadata.ServerName = fmt.Sprintf("%s.%s.svc.cluster.local", m.GetServiceName(volumeID), m.namespace)

	serviceName := m.GetServiceName(volumeID)
	service, err := m.k8sClient.CoreV1().Services(m.namespace).Get(ctx, serviceName, metav1.GetOptions{})
	if err == nil {
		metadata.ServiceIP = service.Spec.ClusterIP
	}

	return &metadata, nil
}

func (m *Manager) ListVolumeMetadata(ctx context.Context) ([]VolumeMetadata, error) {
	if m.k8sClient == nil {
		return nil, fmt.Errorf("kubernetes client not initialized")
	}

	selector := fmt.Sprintf("%s=%s,%s=%s", AppLabel, "zerofs-server", ComponentLabel, "server")
	deployments, err := m.k8sClient.AppsV1().Deployments(m.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(deployments.Items, func(i, j int) bool {
		return deployments.Items[i].Name < deployments.Items[j].Name
	})

	records := make([]VolumeMetadata, 0, len(deployments.Items))
	for _, deployment := range deployments.Items {
		volumeID := deployment.Labels[VolumeLabel]
		record := volumeMetadataFromDeployment(&deployment)
		record.VolumeID = volumeID
		record.ServerName = fmt.Sprintf("%s.%s.svc.cluster.local", m.GetServiceName(volumeID), m.namespace)

		if volumeID != "" {
			serviceName := m.GetServiceName(volumeID)
			service, err := m.k8sClient.CoreV1().Services(m.namespace).Get(ctx, serviceName, metav1.GetOptions{})
			if err == nil {
				record.ServiceIP = service.Spec.ClusterIP
			}
		}

		records = append(records, record)
	}

	return records, nil
}

func (m *Manager) UpdateVolumeCapacity(ctx context.Context, volumeID string, size int64) error {
	if m.k8sClient == nil {
		return fmt.Errorf("kubernetes client not initialized")
	}

	deploymentName := m.GetDeploymentName(volumeID)
	deployment, err := m.k8sClient.AppsV1().Deployments(m.namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if deployment.Annotations == nil {
		deployment.Annotations = map[string]string{}
	}
	deployment.Annotations[AnnotationCapacity] = strconv.FormatInt(size, 10)

	_, err = m.k8sClient.AppsV1().Deployments(m.namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	return err
}

func (m *Manager) upsertSecret(ctx context.Context, secret *corev1.Secret) error {
	existing, err := m.k8sClient.CoreV1().Secrets(m.namespace).Get(ctx, secret.Name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = m.k8sClient.CoreV1().Secrets(m.namespace).Create(ctx, secret, metav1.CreateOptions{})
			return err
		}
		return err
	}

	existing.Data = secret.Data
	m.mergeMetadata(&existing.ObjectMeta, secret.Labels, secret.Annotations)
	_, err = m.k8sClient.CoreV1().Secrets(m.namespace).Update(ctx, existing, metav1.UpdateOptions{})
	return err
}

func (m *Manager) upsertDeployment(ctx context.Context, deployment *appsv1.Deployment) error {
	existing, err := m.k8sClient.AppsV1().Deployments(m.namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = m.k8sClient.AppsV1().Deployments(m.namespace).Create(ctx, deployment, metav1.CreateOptions{})
			return err
		}
		return err
	}

	deployment.ResourceVersion = existing.ResourceVersion
	m.mergeMetadata(&deployment.ObjectMeta, deployment.Labels, deployment.Annotations)
	_, err = m.k8sClient.AppsV1().Deployments(m.namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	return err
}

func volumeMetadataFromDeployment(deployment *appsv1.Deployment) VolumeMetadata {
	record := VolumeMetadata{}
	if deployment == nil {
		return record
	}

	if protocol, ok := deployment.Labels[ProtocolLabel]; ok {
		record.Protocol = Protocol(protocol)
	}

	if deployment.Annotations != nil {
		if storageURL, ok := deployment.Annotations[AnnotationStorageURL]; ok {
			record.StorageURL = storageURL
		}
		if nodeName, ok := deployment.Annotations[AnnotationNodeName]; ok {
			record.NodeName = nodeName
		}
		if protocol, ok := deployment.Annotations[AnnotationProtocol]; ok && record.Protocol == "" {
			record.Protocol = Protocol(protocol)
		}
		if capacity, ok := deployment.Annotations[AnnotationCapacity]; ok {
			if parsed, err := strconv.ParseInt(capacity, 10, 64); err == nil {
				record.CapacityBytes = parsed
			}
		}
	}

	return record
}
