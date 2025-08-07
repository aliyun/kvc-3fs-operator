package native_resources

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DelpoyConfig struct {
	Deployment *appsv1.Deployment
}

func NewDeployConfig() *DelpoyConfig {
	return &DelpoyConfig{
		Deployment: &appsv1.Deployment{},
	}
}

func (dc *DelpoyConfig) WithDeployMeta(name, namespace string, labels map[string]string) *DelpoyConfig {
	if dc.Deployment == nil {
		dc.Deployment = &appsv1.Deployment{}
	}
	dc.Deployment.TypeMeta = metav1.TypeMeta{
		Kind:       "Deployment",
		APIVersion: "apps/v1",
	}
	dc.Deployment.ObjectMeta = metav1.ObjectMeta{
		Name:      name,
		Namespace: namespace,
	}
	dc.Deployment.Labels = labels
	return dc
}

func (dc *DelpoyConfig) WithDeploySpec(deployLabels, podLabels map[string]string, replicaNum int32, hostNetwork bool, selectors map[string]string, stragegyType appsv1.DeploymentStrategyType) *DelpoyConfig {
	dc.Deployment.Spec = appsv1.DeploymentSpec{
		Replicas: &replicaNum,
		Selector: &metav1.LabelSelector{
			MatchLabels: deployLabels,
		},
		Strategy: appsv1.DeploymentStrategy{
			Type: stragegyType,
		},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: podLabels,
			},
			Spec: corev1.PodSpec{
				NodeSelector: selectors,
				HostNetwork:  hostNetwork,
			},
		},
	}
	return dc
}

func (dc *DelpoyConfig) WithVolumes(volumes []corev1.Volume) *DelpoyConfig {
	dc.Deployment.Spec.Template.Spec.Volumes = volumes
	return dc
}

func (dc *DelpoyConfig) WithContainer(Containername, image string, envs []corev1.EnvVar, envFrom []corev1.EnvFromSource, ports []corev1.ContainerPort, resources corev1.ResourceRequirements, volumeMounts []corev1.VolumeMount, command []string) *DelpoyConfig {
	containerConfig := NewContainerConfig()
	containerConfig.
		WithContainer(Containername, image, corev1.PullIfNotPresent, command).
		WithContainerPorts(ports).
		WithContainerEnvs(envs, envFrom).
		WithContainerResources(resources).
		WithContainerVolumeMounts(volumeMounts).
		WithContainerPrivileged(true)

	dc.Deployment.Spec.Template.Spec.Containers = append(dc.Deployment.Spec.Template.Spec.Containers, *containerConfig.Container)
	return dc
}
