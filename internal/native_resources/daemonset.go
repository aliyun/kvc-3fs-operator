package native_resources

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DsConfig struct {
	Ds *appsv1.DaemonSet
}

func NewDsConfig() *DsConfig {
	return &DsConfig{
		Ds: &appsv1.DaemonSet{},
	}
}

func (dc *DsConfig) WithDsMeta(name, namespace string) *DsConfig {
	dc.Ds.ObjectMeta = metav1.ObjectMeta{
		Name:      name,
		Namespace: namespace,
	}
	return dc
}

func (dc *DsConfig) WithDsSpec(deployLabels, podLabels map[string]string, hostNetwork bool, selectors map[string]string) *DsConfig {
	items := make([]corev1.NodeSelectorRequirement, 0)
	for k, v := range selectors {
		items = append(items, corev1.NodeSelectorRequirement{
			Key:      k,
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{v},
		})
	}

	dc.Ds.Spec = appsv1.DaemonSetSpec{
		Selector: &metav1.LabelSelector{
			MatchLabels: deployLabels,
		},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: podLabels,
			},
			Spec: corev1.PodSpec{
				NodeSelector: selectors,
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: items,
								},
							},
						},
					},
				},
				HostNetwork: hostNetwork,
			},
		},
	}
	return dc
}

func (dc *DsConfig) WithVolumes(volumes []corev1.Volume) *DsConfig {
	dc.Ds.Spec.Template.Spec.Volumes = volumes
	return dc
}

func (dc *DsConfig) WithContainer(Containername, image string, envs []corev1.EnvVar, envFrom []corev1.EnvFromSource, ports []corev1.ContainerPort, resources corev1.ResourceRequirements, volumeMounts []corev1.VolumeMount, command []string) *DsConfig {
	containerConfig := NewContainerConfig()
	containerConfig.
		WithContainer(Containername, image, corev1.PullIfNotPresent, command).
		WithContainerPorts(ports).
		WithContainerEnvs(envs, envFrom).
		WithContainerResources(resources).
		WithContainerVolumeMounts(volumeMounts).
		WithContainerPrivileged(true)

	dc.Ds.Spec.Template.Spec.Containers = append(dc.Ds.Spec.Template.Spec.Containers, *containerConfig.Container)
	return dc
}
