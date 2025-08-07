package meta

import (
	"context"
	"fmt"
	"github.com/aliyun/kvc-3fs-operator/internal/constant"
	"github.com/aliyun/kvc-3fs-operator/internal/fdb"
	"github.com/aliyun/kvc-3fs-operator/internal/native_resources"
	"github.com/aliyun/kvc-3fs-operator/internal/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sort"
	"strconv"
)

type MetaConfig struct {
	RdmaPort       int
	TcpPort        int
	Replica        int
	Name           string
	Namespace      string
	Nodes          []string
	MgmtdAddresses string
	Resources      corev1.ResourceRequirements
	FdbConfig      *fdb.FdbConfig
	Deploys        map[string]*native_resources.DelpoyConfig
	DsConfig       *native_resources.DsConfig
	rclient        client.Client
}

func NewMetaConfig(name, namespace string, nodes []string, mgmtdAddress string, rdmaPort, TcpPort, replica int, resources corev1.ResourceRequirements, fdbConfig *fdb.FdbConfig, rclient client.Client) *MetaConfig {
	deploys := make(map[string]*native_resources.DelpoyConfig)
	for _, node := range nodes {
		deploys[node] = native_resources.NewDeployConfig()
	}

	sort.Strings(nodes)
	return &MetaConfig{
		Name:           name,
		Namespace:      namespace,
		RdmaPort:       rdmaPort,
		TcpPort:        TcpPort,
		Replica:        replica,
		Nodes:          nodes,
		MgmtdAddresses: mgmtdAddress,
		Resources:      resources,
		FdbConfig:      fdbConfig,
		Deploys:        deploys,
		DsConfig:       native_resources.NewDsConfig(),
		rclient:        rclient,
	}
}

func GetMetaDeployName(name string) string {
	return fmt.Sprintf("%s-%s", name, "meta")
}

func (mc *MetaConfig) TagNodeLabel() error {
	tagNodeList := &corev1.NodeList{}
	if err := mc.rclient.List(context.Background(), tagNodeList, client.MatchingLabels{constant.ThreeFSMetaNodeKey: "true"}); err != nil && !k8serror.IsNotFound(err) {
		klog.Errorf("list node with meta label failed: %v", err)
		return err
	}
	if len(tagNodeList.Items) == mc.Replica {
		klog.Infof("node num with meta label already satisfy in k8s cluster")
		return nil
	} else if len(tagNodeList.Items) > mc.Replica {
		// delete one each time
		times := len(tagNodeList.Items) - mc.Replica
		klog.Infof("node num with meta label exceed replica(%d) in k8s cluster", mc.Replica)
		node := tagNodeList.Items[0]
		delete(node.Labels, constant.ThreeFSMetaNodeKey)
		if err := mc.rclient.Update(context.Background(), &node); err != nil {
			klog.Errorf("delete node %s meta label failed: %v", node.Name, err)
			return err
		} else {
			klog.Infof("delete node %s meta lebel success", node.Name)
			times--
			if times == 0 {
				return nil
			}
		}
	}

	allNodeList := &corev1.NodeList{}
	if err := mc.rclient.List(context.Background(), allNodeList); err != nil {
		klog.Errorf("list node failed: %v", err)
		return err
	}

	times := len(tagNodeList.Items)
	for _, node := range allNodeList.Items {
		if !utils.StrListContains(mc.Nodes, node.Name) {
			continue
		}
		if _, ok := node.Labels[constant.ThreeFSMetaNodeKey]; ok {
			continue
		}
		node.Labels[constant.ThreeFSMetaNodeKey] = "true"
		if err := mc.rclient.Update(context.Background(), &node); err != nil {
			klog.Errorf("update node %s failed: %v", node.Name, err)
			return err
		} else {
			klog.Infof("tag node %s with meta lebel success", node.Name)
			times += 1
			if times == mc.Replica {
				break
			}
		}
	}

	if !mc.CheckMetaTagNode() {
		return fmt.Errorf("tag meta node number is not enough")
	}

	return nil
}

func (mc *MetaConfig) CheckMetaTagNode() bool {
	nodeList := &corev1.NodeList{}
	_ = mc.rclient.List(context.Background(), nodeList, client.MatchingLabels{constant.ThreeFSMetaNodeKey: "true"})
	tagedNodes := make([]string, 0)
	for _, node := range nodeList.Items {
		tagedNodes = append(tagedNodes, node.Name)
	}
	klog.Infof("meta tagedNodes is: %v", tagedNodes)
	return len(nodeList.Items) == mc.Replica
}

func (mc *MetaConfig) CreateMetaEnvConfigIfNotExist() error {
	allNodeList := &corev1.NodeList{}
	if err := mc.rclient.List(context.Background(), allNodeList); err != nil {
		klog.Errorf("list node failed: %v", err)
		return err
	}
	nodeSet := make([]string, 0)
	for _, node := range allNodeList.Items {
		nodeSet = append(nodeSet, node.Name)
	}
	nodeStrList := utils.StrListReplace(nodeSet, ".", "_")
	nodeStrList = utils.StrListReplace(nodeStrList, "-", "_")

	configName := GetMetaDeployName(mc.Name)
	configMap := &corev1.ConfigMap{}
	envMap := make(map[string]string)
	if err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: configName, Namespace: mc.Namespace}, configMap); err == nil {
		klog.Infof("configmap %s already exist", configName)

		envMap = configMap.Data
		idxMax := constant.ThreeFSMetaStartNodeId
		for _, val := range envMap {
			idx, _ := strconv.Atoi(val)
			if idx > idxMax {
				idxMax = idx
			}
		}
		for _, nodeStr := range nodeStrList {
			if _, ok := envMap[nodeStr]; ok {
				continue
			} else {
				envMap[nodeStr] = strconv.Itoa(idxMax + 1)
				idxMax++
			}
		}
		configMap.Data = envMap
		if err := mc.rclient.Update(context.Background(), configMap); err != nil {
			klog.Errorf("update configmap %s failed: %v", configName, err)
			return err
		}
		return nil
	} else if !k8serror.IsNotFound(err) {
		klog.Errorf("get configmap %s failed: %v", configName, err)
		return err
	}

	base := constant.ThreeFSMetaStartNodeId
	for idx, node := range nodeStrList {
		envMap[node] = strconv.Itoa(base + idx)
	}

	newMetaConfig := native_resources.NewConfigmapConfig(mc.rclient).
		WithMeta(configName, mc.Namespace).
		WithData(envMap)

	if err := mc.rclient.Create(context.Background(), newMetaConfig.ConfigMap); err != nil {
		klog.Errorf("create configmap %s failed: %v", configName, err)
		return err
	}

	return nil
}

func (mc *MetaConfig) DeleteMetaConfigIfExist() error {
	configMap := &corev1.ConfigMap{}
	err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: GetMetaDeployName(mc.Name), Namespace: mc.Namespace}, configMap)
	if err != nil {
		if k8serror.IsNotFound(err) {
			return nil
		}
		klog.Errorf("get configmap %s err: %+v", GetMetaDeployName(mc.Name), err)
		return err
	}

	if err := mc.rclient.Delete(context.Background(), configMap); err != nil {
		klog.Errorf("delete configmap %s failed: %v", GetMetaDeployName(mc.Name), err)
		return err
	}
	return nil
}

func (mc *MetaConfig) DeleteDeployIfExist() error {

	deployList := &appsv1.DeploymentList{}
	if err := mc.rclient.List(context.Background(), deployList, client.InNamespace(mc.Namespace), client.MatchingLabels{constant.ThreeFSMetaDeployKey: mc.Name}); err != nil {
		klog.Errorf("list deployment %s failed: %v", GetMetaDeployName(mc.Name), err)
		return err
	}

	for _, deploy := range deployList.Items {
		if err := mc.rclient.Delete(context.Background(), &deploy); err != nil {
			klog.Errorf("delete deployment %s failed: %v", deploy.Name, err)
			return err
		}
	}
	klog.Infof("delete meta deploy success")
	return nil
}

func (mc *MetaConfig) CreateDeployIfNotExist() error {
	nodeList := &corev1.NodeList{}
	if err := mc.rclient.List(context.Background(), nodeList, client.MatchingLabels{constant.ThreeFSMetaNodeKey: "true"}); err != nil {
		klog.Errorf("list node failed: %v", err)
		return err
	}
	// add new deployment
	for _, node := range nodeList.Items {
		deploy := appsv1.Deployment{}
		deployName := fmt.Sprintf("%s-%s", GetMetaDeployName(mc.Name), utils.TranslatePlainNodeNameValid(node.Name))
		if err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: deployName, Namespace: mc.Namespace}, &deploy); err == nil {
			klog.Infof("deployment %s already exist", deployName)
			continue
		} else if !k8serror.IsNotFound(err) {
			klog.Errorf("get deployment %s failed: %v", deployName, err)
			return err
		}

		newdeploy := mc.WithDeployMeta(node.Name).
			WithDeploySpec(node.Name).
			WithDeployVolumes(node.Name).
			WithDeployContainers(node.Name).Deploys[node.Name].Deployment
		if err := mc.rclient.Create(context.Background(), newdeploy); err != nil {
			klog.Errorf("create deployment %s failed: %v", deployName, err)
			return err
		}
	}

	// delete deploy
	deployList := &appsv1.DeploymentList{}
	if err := mc.rclient.List(context.Background(), deployList, client.InNamespace(mc.Namespace), client.MatchingLabels{constant.ThreeFSMetaDeployKey: mc.Name}); err != nil {
		klog.Errorf("list deployment %s failed: %v", GetMetaDeployName(mc.Name), err)
	}
	for _, deploy := range deployList.Items {
		deployNodeName := deploy.Spec.Template.Spec.NodeSelector[constant.KubernetesHostnameKey]
		nodeObj := &corev1.Node{}
		if err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: deployNodeName, Namespace: mc.Namespace}, nodeObj); err != nil {
			klog.Errorf("get node %s failed: %v", deployNodeName, err)
			return err
		}
		if _, ok := nodeObj.Labels[constant.ThreeFSMetaNodeKey]; !ok {
			if err := mc.rclient.Delete(context.Background(), &deploy); err != nil {
				klog.Errorf("delete deployment %s failed: %v", deploy.Name, err)
				return err
			}
		}
	}

	return nil
}

func (mc *MetaConfig) WithDeployMeta(nodeName string) *MetaConfig {
	DsLabels := map[string]string{
		constant.ThreeFSMetaDeployKey: mc.Name,
	}

	if mc.Deploys[nodeName] == nil {
		klog.Infof("WithDeployMeta %s not exist", nodeName)
		mc.Deploys[nodeName] = native_resources.NewDeployConfig()
	}
	mc.Deploys[nodeName] = mc.Deploys[nodeName].WithDeployMeta(fmt.Sprintf("%s-%s", GetMetaDeployName(mc.Name), utils.TranslatePlainNodeNameValid(nodeName)), mc.Namespace, DsLabels)
	return mc
}

func (mc *MetaConfig) WithDeploySpec(nodeName string) *MetaConfig {
	dsLabels := map[string]string{
		constant.ThreeFSMetaDeployKey: mc.Name,
	}
	podLabels := map[string]string{
		constant.ThreeFSMetaDeployKey:  mc.Name,
		constant.ThreeFSComponentLabel: "meta",
		constant.ThreeFSPodLabel:       "true",
	}

	nodemaps := map[string]string{
		constant.KubernetesHostnameKey: nodeName,
	}

	hostNetwork := utils.GetUseHostNetworkEnv()
	mc.Deploys[nodeName] = mc.Deploys[nodeName].WithDeploySpec(dsLabels, podLabels, 1, hostNetwork, nodemaps, appsv1.RecreateDeploymentStrategyType)
	return mc
}

func (mc *MetaConfig) WithDeployVolumes(nodeName string) *MetaConfig {
	HostPathDirectoryOrCreate := corev1.HostPathDirectoryOrCreate
	volumes := []corev1.Volume{
		{
			Name: "log",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/opt/3fs/meta/logs",
					Type: &HostPathDirectoryOrCreate,
				},
			},
		},
		{
			Name: "dev",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/dev",
					Type: &HostPathDirectoryOrCreate,
				},
			},
		},
	}
	mc.Deploys[nodeName] = mc.Deploys[nodeName].WithVolumes(volumes)
	return mc
}

func (mc *MetaConfig) CheckResources() corev1.ResourceRequirements {
	if mc.Resources.Requests == nil {
		mc.Resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("10Gi"),
		}
	}
	if mc.Resources.Limits == nil {
		mc.Resources.Limits = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("20Gi"),
		}
	}
	hostNetwork := utils.GetUseHostNetworkEnv()
	if !hostNetwork {
		mc.Resources.Requests[constant.ErdmaResourceKey] = resource.MustParse("1")
		mc.Resources.Limits[constant.ErdmaResourceKey] = resource.MustParse("1")
	}
	return mc.Resources
}

func (mc *MetaConfig) WithDeployContainers(nodeName string) *MetaConfig {
	metaImage := os.Getenv("META_IMAGE")
	content, _ := mc.FdbConfig.GetConfigContent()
	configName := GetMetaDeployName(mc.Name)
	envFrom := []corev1.EnvFromSource{
		{
			ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: configName,
				},
			},
		},
	}

	envs := []corev1.EnvVar{
		{
			Name: "NODE_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "spec.nodeName",
				},
			},
		},
		{
			Name:  "COMPONENT",
			Value: "meta",
		},
		{
			Name:  constant.ENVFdbClusterFileContent,
			Value: content,
		},
		{
			Name:  "MGMTD_SERVER_ADDRESS",
			Value: mc.MgmtdAddresses,
		},
	}

	volumeMount := []corev1.VolumeMount{
		{
			Name:      "log",
			MountPath: "/var/log/3fs",
		},
		{
			Name:      "dev",
			MountPath: "/dev",
		},
	}

	command := []string{
		"/setup.sh",
	}
	mc.Deploys[nodeName] = mc.Deploys[nodeName].
		WithContainer("meta", metaImage, envs, envFrom, nil, mc.CheckResources(), volumeMount, command)

	return mc
}
