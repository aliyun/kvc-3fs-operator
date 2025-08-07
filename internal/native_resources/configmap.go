package native_resources

import (
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ConfigmapConfig struct {
	ConfigMap *corev1.ConfigMap
	rclient   client.Client
}

func NewConfigmapConfig(rclient client.Client) *ConfigmapConfig {
	return &ConfigmapConfig{
		ConfigMap: &corev1.ConfigMap{},
		rclient:   rclient,
	}
}

func (cc *ConfigmapConfig) WithMeta(name, namespace string) *ConfigmapConfig {
	cc.ConfigMap.Name = name
	cc.ConfigMap.Namespace = namespace
	cc.ConfigMap.Data = make(map[string]string)
	return cc
}

func (cc *ConfigmapConfig) WithData(data map[string]string) *ConfigmapConfig {
	cc.ConfigMap.Data = data
	return cc
}
