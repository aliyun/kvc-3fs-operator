package native_resources

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type NodeConfig struct {
	Node    *corev1.Node
	rclient client.Client
}

func NewNodeConfig(rclient client.Client) *NodeConfig {
	return &NodeConfig{
		Node:    &corev1.Node{},
		rclient: rclient,
	}
}

func (nc *NodeConfig) ParseNodeIp(name string) (string, error) {
	if err := nc.rclient.Get(context.Background(), client.ObjectKey{Name: name}, nc.Node); err != nil {
		return "", err
	}
	for _, addr := range nc.Node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP {
			return addr.Address, nil
		}
	}
	return "", nil
}
