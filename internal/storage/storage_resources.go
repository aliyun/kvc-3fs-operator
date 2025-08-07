package storage

import (
	"context"
	"fmt"
	threefsv1 "github.com/aliyun/kvc-3fs-operator/api/v1"
	"github.com/aliyun/kvc-3fs-operator/internal/constant"
	"github.com/aliyun/kvc-3fs-operator/internal/native_resources"
	"github.com/aliyun/kvc-3fs-operator/internal/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
)

type StorageConfig struct {
	RdmaPort       int
	TcpPort        int
	Name           string
	Namespace      string
	Nodes          []string
	MgmtdAddresses string
	TargetPaths    []string
	Resources      corev1.ResourceRequirements
	DsConfig       *native_resources.DsConfig
	Deploys        map[string]*native_resources.DelpoyConfig
	rclient        client.Client
}

func NewStorageConfig(name, namespace string, nodes []string, mgmtdAddress string, rdmaPort, tcpPort int, targetPaths []string, resources corev1.ResourceRequirements, rclient client.Client) *StorageConfig {

	deploys := make(map[string]*native_resources.DelpoyConfig)
	for _, node := range nodes {
		deploys[node] = native_resources.NewDeployConfig()
	}

	return &StorageConfig{
		Name:           name,
		Namespace:      namespace,
		RdmaPort:       rdmaPort,
		TcpPort:        tcpPort,
		Nodes:          nodes,
		TargetPaths:    targetPaths,
		MgmtdAddresses: mgmtdAddress,
		Resources:      resources,
		DsConfig:       native_resources.NewDsConfig(),
		Deploys:        deploys,
		rclient:        rclient,
	}
}

func GetStorageDeployName(name string) string {
	return fmt.Sprintf("%s-%s", name, "storage")
}

func FilterStorageNode(rclient client.Client) ([]string, error) {
	nodeList := &corev1.NodeList{}
	if err := rclient.List(context.Background(), nodeList); err != nil {
		klog.Errorf("list node failed: %v", err)
		return nil, err
	}

	newNodes := make([]string, 0)
	for _, node := range nodeList.Items {
		if _, faultok := node.Labels[constant.ThreeFSStorageFaultNodeKey]; faultok {
			klog.Infof("node %s is fault node, skip from storage node list", node.Name)
			continue
		}

		if _, ok := node.Labels[constant.ThreeFSStorageNodeKey]; ok {
			newNodes = append(newNodes, node.Name)
		}
	}
	return newNodes, nil
}

func (mc *StorageConfig) TagNodeLabel(vfsc *threefsv1.ThreeFsCluster) error {

	newNodes, err := FilterStorageNode(mc.rclient)
	if err != nil {
		return err
	}
	oldObj := vfsc.DeepCopy()
	if vfsc.Status.Phase == constant.ThreeFSClusterEmptyStatus || vfsc.Status.Phase == constant.ThreeFSClusterInitStatus {
		mc.Nodes = newNodes
		vfsc.Status.NodesInfo.StorageNodes = newNodes
	} else {
		// if add new storage node, add to back up nodes list
		if len(mc.Nodes) == 0 {
			return fmt.Errorf("storage node is not enough")
		}
		backupNodes := make([]string, 0)
		storageNodes := vfsc.Status.NodesInfo.StorageNodes
		// check backup nodes
		for _, node := range newNodes {
			if !utils.StrListContains(oldObj.Status.NodesInfo.StorageNodes, node) {
				backupNodes = append(backupNodes, node)
			}
		}
		// check node that user remove node label
		for _, node := range oldObj.Status.NodesInfo.StorageNodes {
			if !utils.StrListContains(newNodes, node) {
				storageNodes = utils.StrListRemove(storageNodes, node)
			}
		}
		vfsc.Status.NodesInfo.StorageBackupNodes = backupNodes
		vfsc.Status.NodesInfo.StorageNodes = storageNodes
	}

	klog.Infof("update vfsc storage nodes list: %+v, backup node list: %+v", vfsc.Status.NodesInfo.StorageNodes, vfsc.Status.NodesInfo.StorageBackupNodes)
	if err := mc.rclient.Status().Patch(context.Background(), vfsc, client.MergeFrom(oldObj)); err != nil {
		klog.Errorf("update vfsc storage nodes list %s failed: %v", vfsc.Name, err)
		return err
	}

	return nil
}

func (mc *StorageConfig) CreateStorageEnvConfigIfNotExist() error {
	nodeStrList := utils.StrListReplace(mc.Nodes, ".", "_")
	nodeStrList = utils.StrListReplace(nodeStrList, "-", "_")

	configName := GetStorageDeployName(mc.Name)
	configMap := &corev1.ConfigMap{}
	if err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: configName, Namespace: mc.Namespace}, configMap); err == nil {
		klog.Infof("configmap %s already exist", configName)
		nodeidMaps := configMap.Data
		nodeIdMapsKeys := make([]string, 0)
		for k, _ := range nodeidMaps {
			nodeIdMapsKeys = append(nodeIdMapsKeys, k)
		}

		for _, k := range nodeIdMapsKeys {
			if !utils.StrListContains(nodeStrList, k) {
				delete(nodeidMaps, k)
			}
		}

		nodeidMax := constant.ThreeFSStorageStartNodeId
		for _, nodeStr := range nodeStrList {
			val, ok := configMap.Data[nodeStr]
			if !ok {
				continue
			}
			tmpNodeId, _ := strconv.Atoi(val)
			if tmpNodeId > nodeidMax {
				nodeidMax = tmpNodeId
			}
		}

		for _, nodeStr := range nodeStrList {
			if _, ok := nodeidMaps[nodeStr]; ok {
				continue
			} else {
				nodeidMaps[nodeStr] = strconv.Itoa(nodeidMax + 1)
				klog.Infof("add node %s,%d to configmap %s", nodeStr, nodeidMax+1, configName)
				nodeidMax += 1
			}
		}
		configMap.Data = nodeidMaps
		if err := mc.rclient.Update(context.Background(), configMap); err != nil {
			klog.Errorf("update configmap %s failed: %v", configName, err)
			return err
		}
		return nil
	} else if !k8serror.IsNotFound(err) {
		klog.Errorf("get configmap %s failed: %v", configName, err)
		return err
	}

	envMap := make(map[string]string)
	base := constant.ThreeFSStorageStartNodeId
	for idx, node := range nodeStrList {
		envMap[node] = strconv.Itoa(base + idx)
	}

	newStorageConfig := native_resources.NewConfigmapConfig(mc.rclient).
		WithMeta(configName, mc.Namespace).
		WithData(envMap)

	if err := mc.rclient.Create(context.Background(), newStorageConfig.ConfigMap); err != nil {
		klog.Errorf("create configmap %s failed: %v", configName, err)
		return err
	}

	return nil
}

func (mc *StorageConfig) DeleteStorageConfigIfExist() error {
	configMap := &corev1.ConfigMap{}
	err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: GetStorageDeployName(mc.Name), Namespace: mc.Namespace}, configMap)
	if err != nil {
		if k8serror.IsNotFound(err) {
			return nil
		}
		klog.Errorf("get configmap %s err: %+v", GetStorageDeployName(mc.Name), err)
		return err
	}

	if err := mc.rclient.Delete(context.Background(), configMap); err != nil {
		klog.Errorf("delete configmap %s failed: %v", GetStorageDeployName(mc.Name), err)
		return err
	}
	return nil
}

func (mc *StorageConfig) DeleteDeployIfExist() error {
	deployList := &appsv1.DeploymentList{}
	if err := mc.rclient.List(context.Background(), deployList, client.InNamespace(mc.Namespace), client.MatchingLabels{constant.ThreeFSStorageDeployKey: mc.Name}); err != nil {
		klog.Errorf("list deployment %s failed: %v", GetStorageDeployName(mc.Name), err)
		return err
	}

	for _, deploy := range deployList.Items {
		if err := mc.rclient.Delete(context.Background(), &deploy); err != nil {
			klog.Errorf("delete deployment %s failed: %v", deploy.Name, err)
			return err
		}
	}

	klog.Infof("delete storage deploy success")
	return nil
}

func (mc *StorageConfig) CreateDeployIfNotExist() error {

	// add new deployment
	for _, node := range mc.Nodes {
		deploy := appsv1.Deployment{}
		deployName := fmt.Sprintf("%s-%s", GetStorageDeployName(mc.Name), utils.TranslatePlainNodeNameValid(node))
		if err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: deployName, Namespace: mc.Namespace}, &deploy); err == nil {
			klog.Infof("deployment %s already exist", deployName)
			continue
		} else if !k8serror.IsNotFound(err) {
			klog.Errorf("get deployment %s failed: %v", deployName, err)
			return err
		}

		newdeploy := mc.WithDeployMeta(node).
			WithDeploySpec(node).
			WithDeployVolumes(node).
			WithDeployContainers(node).Deploys[node].Deployment
		if err := mc.rclient.Create(context.Background(), newdeploy); err != nil {
			klog.Errorf("create deployment %s failed: %v", deployName, err)
			return err
		}
	}

	// delete deploy
	deployList := &appsv1.DeploymentList{}
	if err := mc.rclient.List(context.Background(), deployList, client.InNamespace(mc.Namespace), client.MatchingLabels{constant.ThreeFSStorageDeployKey: mc.Name}); err != nil {
		klog.Errorf("list deployment %s failed: %v", GetStorageDeployName(mc.Name), err)
	}
	for _, deploy := range deployList.Items {
		deployNodeName := deploy.Spec.Template.Spec.NodeSelector[constant.KubernetesHostnameKey]
		if !utils.StrListContains(mc.Nodes, deployNodeName) {
			if err := mc.rclient.Delete(context.Background(), &deploy); err != nil {
				klog.Errorf("delete deployment %s failed: %v", deploy.Name, err)
				return err
			}
		}
	}

	return nil
}

func (mc *StorageConfig) WithDeployMeta(nodeName string) *StorageConfig {
	if mc.Deploys[nodeName] == nil {
		mc.Deploys[nodeName] = native_resources.NewDeployConfig()
	}
	DsLabels := map[string]string{
		constant.ThreeFSStorageDeployKey: mc.Name,
	}
	mc.Deploys[nodeName] = mc.Deploys[nodeName].WithDeployMeta(fmt.Sprintf("%s-%s", GetStorageDeployName(mc.Name), utils.TranslatePlainNodeNameValid(nodeName)), mc.Namespace, DsLabels)
	return mc
}

func (mc *StorageConfig) WithDeploySpec(nodeName string) *StorageConfig {
	dsLabels := map[string]string{
		constant.ThreeFSStorageDeployKey: mc.Name,
	}
	podLabels := map[string]string{
		constant.ThreeFSStorageDeployKey: mc.Name,
		constant.ThreeFSComponentLabel:   "storage",
		constant.ThreeFSPodLabel:         "true",
	}

	nodemaps := map[string]string{
		constant.KubernetesHostnameKey: nodeName,
	}
	hostNetwork := utils.GetUseHostNetworkEnv()
	mc.Deploys[nodeName] = mc.Deploys[nodeName].WithDeploySpec(dsLabels, podLabels, 1, hostNetwork, nodemaps, appsv1.RecreateDeploymentStrategyType)
	return mc
}

func (mc *StorageConfig) WithDeployVolumes(nodeName string) *StorageConfig {
	HostPathDirectoryOrCreate := corev1.HostPathDirectoryOrCreate
	HostPathDirectory := corev1.HostPathDirectory
	volumes := []corev1.Volume{
		{
			Name: "log",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/opt/3fs/storage/logs",
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

	for idx, targetPath := range mc.TargetPaths {
		volumes = append(volumes, corev1.Volume{
			Name: fmt.Sprintf("data-%d", idx),
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: targetPath,
					Type: &HostPathDirectory,
				},
			},
		})
	}
	mc.Deploys[nodeName] = mc.Deploys[nodeName].WithVolumes(volumes)
	return mc
}

func (mc *StorageConfig) CheckResources() corev1.ResourceRequirements {
	if mc.Resources.Requests == nil {
		mc.Resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("3"),
			corev1.ResourceMemory: resource.MustParse("10Gi"),
		}
	}
	if mc.Resources.Limits == nil {
		mc.Resources.Limits = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("10"),
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

func (mc *StorageConfig) WithDeployContainers(nodeName string) *StorageConfig {
	storageImage := os.Getenv("STORAGE_IMAGE")
	configName := GetStorageDeployName(mc.Name)
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
			Value: "storage",
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

	for idx, targetPath := range mc.TargetPaths {
		volumeMount = append(volumeMount, corev1.VolumeMount{
			Name:      fmt.Sprintf("data-%d", idx),
			MountPath: targetPath,
		})
	}

	command := []string{
		"/setup.sh",
	}
	mc.Deploys[nodeName] = mc.Deploys[nodeName].
		WithContainer("storage", storageImage, envs, envFrom, nil, mc.CheckResources(), volumeMount, command)

	return mc
}
