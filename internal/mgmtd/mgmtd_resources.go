package mgmtd

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

type MgmtdConfig struct {
	RdmaPort       int
	TcpPort        int
	Replica        int
	Name           string
	Namespace      string
	Nodes          []string
	MgmtdAddresses string
	Resources      corev1.ResourceRequirements
	FdbConfig      *fdb.FdbConfig
	DsConfig       *native_resources.DsConfig
	SvcConfig      *native_resources.ServiceConfig
	Deploys        map[string]*native_resources.DelpoyConfig
	rclient        client.Client
}

func NewMgmtdConfig(name, namespace string, nodes []string, RdmaPort, Tcpport, replica int, resources corev1.ResourceRequirements, fdbConfig *fdb.FdbConfig, rclient client.Client) *MgmtdConfig {

	deploys := make(map[string]*native_resources.DelpoyConfig)
	for _, node := range nodes {
		deploys[node] = native_resources.NewDeployConfig()
	}

	sort.Strings(nodes)
	return &MgmtdConfig{
		Name:      name,
		Namespace: namespace,
		RdmaPort:  RdmaPort,
		TcpPort:   Tcpport,
		Replica:   replica,
		Nodes:     nodes,
		Resources: resources,
		FdbConfig: fdbConfig,
		DsConfig:  native_resources.NewDsConfig(),
		SvcConfig: native_resources.NewServiceConfig(),
		Deploys:   deploys,
		rclient:   rclient,
	}
}

func GetMgmtdDeployName(name string) string {
	return fmt.Sprintf("%s-%s", name, "mgmtd")
}

func (mc *MgmtdConfig) TagNodeLabel(nodes []string) error {
	for _, node := range nodes {
		nodeObj := &corev1.Node{}
		if err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: node}, nodeObj); err != nil {
			klog.Errorf("get node %s failed: %v", node, err)
			return err
		}
		newnodeObj := nodeObj.DeepCopy()
		newnodeObj.Labels[constant.ThreeFSMgmtdNodeKey] = "true"
		if err := mc.rclient.Patch(context.Background(), newnodeObj, client.MergeFrom(nodeObj)); err != nil {
			klog.Errorf("patch node %s failed: %v", node, err)
			return err
		}
	}
	return nil
}

func (mc *MgmtdConfig) CheckMgmtdTagNode() bool {
	nodeList := &corev1.NodeList{}
	_ = mc.rclient.List(context.Background(), nodeList, client.MatchingLabels{constant.ThreeFSMgmtdNodeKey: "true"})
	tagedNodes := make([]string, 0)
	for _, node := range nodeList.Items {
		tagedNodes = append(tagedNodes, node.Name)
	}
	klog.Infof("mgmtd tagedNodes is: %v", tagedNodes)
	return len(nodeList.Items) == mc.Replica
}

func (mc *MgmtdConfig) buildMgmtdHeadlessService() *corev1.Service {
	svcLabels := map[string]string{
		constant.ThreeFSMgmtdDeployKey: mc.Name,
	}

	mc.SvcConfig.
		WithServiceMeta(GetMgmtdDeployName(mc.Name), mc.Namespace).
		WithServiceSpec(svcLabels, nil, "", corev1.ClusterIPNone)

	return mc.SvcConfig.Service
}

func (mc *MgmtdConfig) CreateServiceIfNotExist() error {
	svc := &corev1.Service{}
	err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: GetMgmtdDeployName(mc.Name), Namespace: mc.Namespace}, svc)
	if err == nil {
		klog.Infof("service %s already exist", GetMgmtdDeployName(mc.Name))
		return nil
	} else if !k8serror.IsNotFound(err) {
		klog.Errorf("get svc %s failed: %v", GetMgmtdDeployName(mc.Name), err)
		return err
	}

	if err := mc.rclient.Create(context.Background(), mc.buildMgmtdHeadlessService()); err != nil {
		klog.Errorf("create svc %s failed: %v", GetMgmtdDeployName(mc.Name), err)
		return err
	}
	return nil
}

func (mc *MgmtdConfig) DeleteServiceIfExist() error {
	svc := &corev1.Service{}
	err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: GetMgmtdDeployName(mc.Name), Namespace: mc.Namespace}, svc)
	if err != nil {
		if k8serror.IsNotFound(err) {
			return nil
		}
		klog.Errorf("get service %s err: %+v", GetMgmtdDeployName(mc.Name), err)
		return err
	}

	if err := mc.rclient.Delete(context.Background(), svc); err != nil {
		klog.Errorf("delete svc %s failed: %v", GetMgmtdDeployName(mc.Name), err)
		return err
	}
	return nil
}

func (mc *MgmtdConfig) CreateMgmtdEnvConfigIfNotExist() error {
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

	configName := GetMgmtdDeployName(mc.Name)
	configMap := &corev1.ConfigMap{}
	envMap := make(map[string]string)
	if err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: configName, Namespace: mc.Namespace}, configMap); err == nil {
		klog.Infof("configmap %s already exist", configName)

		envMap = configMap.Data
		idxMax := constant.ThreeFSMgmtdStartNodeId
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

	base := constant.ThreeFSMgmtdStartNodeId
	for idx, node := range nodeStrList {
		envMap[node] = strconv.Itoa(base + idx)
	}

	newMgmtdConfig := native_resources.NewConfigmapConfig(mc.rclient).
		WithMeta(configName, mc.Namespace).
		WithData(envMap)

	if err := mc.rclient.Create(context.Background(), newMgmtdConfig.ConfigMap); err != nil {
		klog.Errorf("create configmap %s failed: %v", configName, err)
		return err
	}

	return nil
}

func (mc *MgmtdConfig) DeleteMgmtdConfigIfExist() error {
	configMap := &corev1.ConfigMap{}
	err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: GetMgmtdDeployName(mc.Name), Namespace: mc.Namespace}, configMap)
	if err != nil {
		if k8serror.IsNotFound(err) {
			return nil
		}
		klog.Errorf("get configmap %s err: %+v", GetMgmtdDeployName(mc.Name), err)
		return err
	}

	if err := mc.rclient.Delete(context.Background(), configMap); err != nil {
		klog.Errorf("delete configmap %s failed: %v", GetMgmtdDeployName(mc.Name), err)
		return err
	}
	return nil
}

func (mc *MgmtdConfig) DeleteDeployIfExist() error {

	deployList := &appsv1.DeploymentList{}
	if err := mc.rclient.List(context.Background(), deployList, client.InNamespace(mc.Namespace), client.MatchingLabels{constant.ThreeFSMgmtdDeployKey: mc.Name}); err != nil {
		klog.Errorf("list deployment %s failed: %v", GetMgmtdDeployName(mc.Name), err)
		return err
	}

	for _, deploy := range deployList.Items {
		if err := mc.rclient.Delete(context.Background(), &deploy); err != nil {
			klog.Errorf("delete deployment %s failed: %v", deploy.Name, err)
			return err
		}
	}
	klog.Infof("delete mgmtd deploy success")
	return nil
}

func (mc *MgmtdConfig) CreateDeployIfNotExist() error {
	nodeList := &corev1.NodeList{}
	if err := mc.rclient.List(context.Background(), nodeList, client.MatchingLabels{constant.ThreeFSMgmtdNodeKey: "true"}); err != nil {
		klog.Errorf("list node failed: %v", err)
		return err
	}
	// add new deployment
	for _, node := range nodeList.Items {
		deploy := appsv1.Deployment{}
		deployName := fmt.Sprintf("%s-%s", GetMgmtdDeployName(mc.Name), utils.TranslatePlainNodeNameValid(node.Name))
		if err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: deployName, Namespace: mc.Namespace}, &deploy); err == nil {
			klog.Infof("deployment %s already exist", deployName)
			continue
		} else if !k8serror.IsNotFound(err) {
			klog.Errorf("get deployment %s failed: %v", deployName, err)
			return err
		}
		klog.Infof("deploy %+v", mc.Deploys)
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
	if err := mc.rclient.List(context.Background(), deployList, client.InNamespace(mc.Namespace), client.MatchingLabels{constant.ThreeFSMgmtdDeployKey: mc.Name}); err != nil {
		klog.Errorf("list deployment %s failed: %v", GetMgmtdDeployName(mc.Name), err)
	}
	for _, deploy := range deployList.Items {
		deployNodeName := deploy.Spec.Template.Spec.NodeSelector[constant.KubernetesHostnameKey]
		nodeObj := &corev1.Node{}
		if err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: deployNodeName, Namespace: mc.Namespace}, nodeObj); err != nil {
			klog.Errorf("get node %s failed: %v", deployNodeName, err)
			return err
		}
		if _, ok := nodeObj.Labels[constant.ThreeFSMgmtdNodeKey]; !ok {
			if err := mc.rclient.Delete(context.Background(), &deploy); err != nil {
				klog.Errorf("delete deployment %s failed: %v", deploy.Name, err)
				return err
			}
		}
	}

	return nil
}

func (mc *MgmtdConfig) WithDeployMeta(nodeName string) *MgmtdConfig {
	DsLabels := map[string]string{
		constant.ThreeFSMgmtdDeployKey: mc.Name,
	}
	if mc.Deploys[nodeName] == nil {
		klog.Infof("WithDeployMeta %s not exist", nodeName)
		mc.Deploys[nodeName] = native_resources.NewDeployConfig()
	}
	mc.Deploys[nodeName] = mc.Deploys[nodeName].WithDeployMeta(fmt.Sprintf("%s-%s", GetMgmtdDeployName(mc.Name), utils.TranslatePlainNodeNameValid(nodeName)), mc.Namespace, DsLabels)
	return mc
}

func (mc *MgmtdConfig) WithDeploySpec(nodeName string) *MgmtdConfig {
	dsLabels := map[string]string{
		constant.ThreeFSMgmtdDeployKey: mc.Name,
	}
	podLabels := map[string]string{
		constant.ThreeFSMgmtdDeployKey: mc.Name,
		constant.ThreeFSComponentLabel: "mgmtd",
		constant.ThreeFSPodLabel:       "true",
	}

	nodemaps := map[string]string{
		constant.KubernetesHostnameKey: nodeName,
	}
	hostNetwork := utils.GetUseHostNetworkEnv()
	mc.Deploys[nodeName] = mc.Deploys[nodeName].WithDeploySpec(dsLabels, podLabels, 1, hostNetwork, nodemaps, appsv1.RecreateDeploymentStrategyType)
	return mc
}

func (mc *MgmtdConfig) WithDeployVolumes(nodeName string) *MgmtdConfig {
	HostPathDirectoryOrCreate := corev1.HostPathDirectoryOrCreate
	volumes := []corev1.Volume{
		{
			Name: "log",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/opt/3fs/mgmtd/logs",
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

func (mc *MgmtdConfig) CheckResources() corev1.ResourceRequirements {
	if mc.Resources.Requests == nil {
		mc.Resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("400Mi"),
		}
	}
	if mc.Resources.Limits == nil {
		mc.Resources.Limits = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("10Gi"),
		}
	}
	hostNetwork := utils.GetUseHostNetworkEnv()
	if !hostNetwork {
		mc.Resources.Requests[constant.ErdmaResourceKey] = resource.MustParse("1")
		mc.Resources.Limits[constant.ErdmaResourceKey] = resource.MustParse("1")
	}
	return mc.Resources
}

func (mc *MgmtdConfig) WithDeployContainers(nodeName string) *MgmtdConfig {
	mgmtdImage := os.Getenv("MGMTD_IMAGE")
	content, _ := mc.FdbConfig.GetConfigContent()
	configName := GetMgmtdDeployName(mc.Name)
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
			Value: "mgmtd",
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
		WithContainer("mgmtd", mgmtdImage, envs, envFrom, nil, mc.CheckResources(), volumeMount, command)

	return mc
}
