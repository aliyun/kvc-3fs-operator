package native_resources

import corev1 "k8s.io/api/core/v1"

type ServiceConfig struct {
	Service *corev1.Service
}

func NewServiceConfig() *ServiceConfig {
	return &ServiceConfig{
		Service: &corev1.Service{},
	}
}

func (c *ServiceConfig) WithServiceMeta(name, namespace string) *ServiceConfig {
	c.Service.Name = name
	c.Service.Namespace = namespace
	return c
}

func (c *ServiceConfig) WithServiceSpec(svcLabels map[string]string, ports []corev1.ServicePort, serviceType corev1.ServiceType, clusterIp string) *ServiceConfig {
	c.Service.Spec = corev1.ServiceSpec{
		Selector:  svcLabels,
		Ports:     ports,
		Type:      serviceType,
		ClusterIP: clusterIp,
	}
	return c
}
