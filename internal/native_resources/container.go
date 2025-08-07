package native_resources

import corev1 "k8s.io/api/core/v1"

type ContainerConfig struct {
	Container *corev1.Container
}

func NewContainerConfig() *ContainerConfig {
	return &ContainerConfig{
		Container: &corev1.Container{},
	}
}

func (cc *ContainerConfig) WithContainer(Containername, image string, imagePolicy corev1.PullPolicy, command []string) *ContainerConfig {
	cc.Container = &corev1.Container{
		Name:            Containername,
		Image:           image,
		ImagePullPolicy: imagePolicy,
		Command:         command,
	}

	return cc
}

func (cc *ContainerConfig) WithContainerEnvs(envs []corev1.EnvVar, envFrom []corev1.EnvFromSource) *ContainerConfig {
	cc.Container.Env = envs
	cc.Container.EnvFrom = envFrom
	return cc
}

func (dc *ContainerConfig) WithContainerPorts(ports []corev1.ContainerPort) *ContainerConfig {
	dc.Container.Ports = ports
	return dc
}

func (cc *ContainerConfig) WithContainerResources(resources corev1.ResourceRequirements) *ContainerConfig {
	cc.Container.Resources = resources
	return cc
}

func (cc *ContainerConfig) WithContainerVolumeMounts(volumeMounts []corev1.VolumeMount) *ContainerConfig {
	cc.Container.VolumeMounts = volumeMounts
	return cc
}

func (cc *ContainerConfig) WithContainerLifeCycle(postStart *corev1.LifecycleHandler, preStop *corev1.LifecycleHandler) *ContainerConfig {
	cc.Container.Lifecycle = &corev1.Lifecycle{
		PostStart: postStart,
		PreStop:   preStop,
	}
	return cc
}

func (cc *ContainerConfig) WithContainerPrivileged(privileged bool) *ContainerConfig {
	cc.Container.SecurityContext = &corev1.SecurityContext{
		Privileged: &privileged,
		Capabilities: &corev1.Capabilities{
			Add: []corev1.Capability{"SYS_ADMIN"},
		},
	}
	return cc
}
