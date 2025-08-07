package fdb

import (
	"bytes"
	"context"
	"fmt"
	v1 "github.com/aliyun/kvc-3fs-operator/api/v1"
	"github.com/aliyun/kvc-3fs-operator/internal/constant"
	"github.com/aliyun/kvc-3fs-operator/internal/native_resources"
	"github.com/aliyun/kvc-3fs-operator/internal/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/klog/v2"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sort"
	"strconv"
	"strings"
)

type FdbConfig struct {
	Port            int
	Name            string
	Namespace       string
	Nodes           []string
	StorageReplicas int
	ClusterSize     int
	ConfigContent   string // disabled
	Resources       corev1.ResourceRequirements
	DsConfig        *native_resources.DsConfig
	Deploys         map[string]*native_resources.DelpoyConfig
	rclient         client.Client
	restClient      rest.Interface
	restConfig      *rest.Config
	schema          *runtime.Scheme
}

func NewFdbConfig(name, namespace string, replica, clusterSize int, nodes []string, port int, resources corev1.ResourceRequirements, rclient client.Client, restClient rest.Interface, restConfig *rest.Config, schema *runtime.Scheme) *FdbConfig {

	deploys := make(map[string]*native_resources.DelpoyConfig)
	for _, node := range nodes {
		deploys[node] = native_resources.NewDeployConfig()
	}

	sort.Strings(nodes)
	return &FdbConfig{
		Name:            name,
		Namespace:       namespace,
		Port:            port,
		Nodes:           nodes,
		StorageReplicas: replica,
		ClusterSize:     clusterSize,
		Resources:       resources,
		DsConfig:        native_resources.NewDsConfig(),
		Deploys:         deploys,
		rclient:         rclient,
		restClient:      restClient,
		restConfig:      restConfig,
		schema:          schema,
	}
}

func GetFdbDeployName(name string) string {
	return fmt.Sprintf("%s-%s", name, "fdb")
}

func FilterFdbConfig(contents []string) string {
	if len(contents) == 0 {
		return ""
	}
	for _, content := range contents {
		if strings.Contains(content, "@") {
			return content
		}
	}
	return ""
}

func (fc *FdbConfig) GetRemoteConfigContentBak() (string, error) {
	podList := &corev1.PodList{}
	if err := fc.rclient.List(context.Background(), podList, client.InNamespace(fc.Namespace), client.MatchingLabels{constant.ThreeFSFdbDeployKey: fc.Name}); err != nil {
		klog.Errorf("list pod failed: %v", err)
		return "", err
	}

	if len(podList.Items) == 0 {
		klog.Errorf("fdb pod is empty")
		return "", fmt.Errorf("pod is empty")
	}

	execReq := fc.restClient.
		Post().
		Namespace(fc.Namespace).
		Resource("pods").
		Name(podList.Items[0].Name).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "fdb",
			Command:   []string{"bash", "-c", fmt.Sprintf("cat %s", constant.DefaultThreeFSFdbConfigPath)},
			Stdin:     true,
			Stdout:    true,
			Stderr:    true,
		}, runtime.NewParameterCodec(fc.schema))

	exec, err := remotecommand.NewSPDYExecutor(fc.restConfig, "POST", execReq.URL())
	if err != nil {
		klog.Errorf("error while creating remote command executor: %v", err)
		return "", fmt.Errorf("error while creating remote command executor: %v", err)
	}

	var stdout bytes.Buffer
	err = exec.Stream(remotecommand.StreamOptions{
		Stdin:  os.Stdin,
		Stdout: &stdout,
		Stderr: os.Stderr,
		Tty:    false,
	})
	if err != nil {
		klog.Errorf("error while streaming remote command: %v", err)
		return "", fmt.Errorf("error while streaming remote command: %v", err)
	}

	fileContents := strings.Split(string(stdout.Bytes()), "\n")
	klog.Infof("get file contents from remote: %+v", fileContents)
	if len(fileContents) > 0 {
		klog.Infof("get file contents from remote: %s", FilterFdbConfig(fileContents))
		return FilterFdbConfig(fileContents), nil
	}
	return "", fmt.Errorf("file is empty")
}

func FilterFdbNodes(rclient client.Client) ([]string, error) {
	nodeList := &corev1.NodeList{}
	if err := rclient.List(context.Background(), nodeList); err != nil {
		klog.Errorf("list node failed: %v", err)
		return nil, err
	}

	newNodes := make([]string, 0)
	for _, node := range nodeList.Items {
		if _, faultok := node.Labels[constant.ThreeFSFdbFaultNodeKey]; faultok {
			klog.Infof("node %s is fault node, skip from fdb node list", node.Name)
			continue
		}

		if _, ok := node.Labels[constant.ThreeFSFdbNodeKey]; ok {
			newNodes = append(newNodes, node.Name)
		}
	}
	return newNodes, nil
}

func (fc *FdbConfig) TagNodeLabel(vfsc *v1.ThreeFsCluster) error {

	newNodes, err := FilterFdbNodes(fc.rclient)
	if err != nil {
		return err
	}
	if len(newNodes) < fc.ClusterSize {
		klog.Errorf("fdb node is not enough, need %d, but got %d", fc.ClusterSize, len(newNodes))
		return fmt.Errorf("fdb node is not enough")
	}

	fc.Nodes = newNodes

	newObj := vfsc.DeepCopy()
	newObj.Status.NodesInfo.FdbNodes = newNodes
	klog.Infof("update vfsc fdb nodes list %s: %+v", vfsc.Name, newObj.Status.NodesInfo.FdbNodes)
	if err := fc.rclient.Status().Patch(context.Background(), newObj, client.MergeFrom(vfsc)); err != nil {
		klog.Errorf("update vfsc fdb nodes list %s failed: %v", vfsc.Name, err)
		return err
	}
	return nil
}

func (fc *FdbConfig) GetConfigContent() (string, error) {
	fdbConfigMap := corev1.ConfigMap{}
	if err := fc.rclient.Get(context.Background(), client.ObjectKey{Name: GetFdbDeployName(fc.Name), Namespace: fc.Namespace}, &fdbConfigMap); err != nil {
		klog.Errorf("get configmap %s failed: %v", GetFdbDeployName(fc.Name), err)
		return "", err
	}
	klog.Infof("get configmap %s content: %s", GetFdbDeployName(fc.Name), fdbConfigMap.Data["fdb.cluster"])
	return fdbConfigMap.Data["fdb.cluster"], nil
}

func (fc *FdbConfig) CreateFdbConfigIfNotExist(existingContent string) error {
	configMap := &corev1.ConfigMap{}
	if err := fc.rclient.Get(context.Background(), client.ObjectKey{Name: GetFdbDeployName(fc.Name), Namespace: fc.Namespace}, configMap); err == nil {
		klog.Infof("configmap %s already exist, try to update", GetFdbDeployName(fc.Name))
		if existingContent == "" {
			klog.Infof("fdb config is empty, skip update")
			return nil
		}
		if existingContent == configMap.Data["fdb.cluster"] {
			klog.Infof("fdb config is same, skip update")
			return nil
		}
		configMap.Data["fdb.cluster"] = existingContent
		if err := fc.rclient.Update(context.Background(), configMap); err != nil {
			klog.Errorf("update configmap %s failed: %v", GetFdbDeployName(fc.Name), err)
			return err
		}

		return nil
	} else if !k8serror.IsNotFound(err) {
		klog.Infof("configmap %s exist: %v", GetFdbDeployName(fc.Name), err)
	}

	nodeIps := make([]string, fc.ClusterSize)
	for idx, node := range fc.Nodes[:fc.ClusterSize] {
		pc := native_resources.NewNodeConfig(fc.rclient)
		nodeIp, err := pc.ParseNodeIp(node)
		if err != nil {
			klog.Errorf("get node %s ip failed: %v", node, err)
			return err
		}
		nodeIps[idx] = fmt.Sprintf("%s:%d", nodeIp, fc.Port)
	}

	content := fmt.Sprintf("%s:%s@%s", utils.GenerateUuidWithLen(10),
		utils.GenerateUuidWithLen(10), strings.Join(nodeIps, ","))

	fdbConfig := native_resources.NewConfigmapConfig(fc.rclient).
		WithMeta(GetFdbDeployName(fc.Name), fc.Namespace).
		WithData(map[string]string{
			"fdb.cluster": content,
		})
	if err := fc.rclient.Create(context.Background(), fdbConfig.ConfigMap); err != nil {
		klog.Errorf("update configmap %s failed: %v", GetFdbDeployName(fc.Name), err)
		return err
	}
	return nil
}

func (fc *FdbConfig) DeleteFdbConfigIfExist() error {
	configMap := &corev1.ConfigMap{}
	err := fc.rclient.Get(context.Background(), client.ObjectKey{Name: GetFdbDeployName(fc.Name), Namespace: fc.Namespace}, configMap)
	if err != nil {
		if k8serror.IsNotFound(err) {
			return nil
		}
		klog.Errorf("get configmap %s err: %+v", GetFdbDeployName(fc.Name), err)
		return err
	}

	if err := fc.rclient.Delete(context.Background(), configMap); err != nil {
		klog.Errorf("delete configmap %s failed: %v", GetFdbDeployName(fc.Name), err)
		return err
	}
	return nil
}

func (fc *FdbConfig) DeleteDeployIfExist() error {
	for _, node := range fc.Nodes {
		deploy := appsv1.Deployment{}
		deployName := fmt.Sprintf("%s-%s", GetFdbDeployName(fc.Name), utils.TranslatePlainNodeNameValid(node))
		if err := fc.rclient.Get(context.Background(), client.ObjectKey{Name: deployName, Namespace: fc.Namespace}, &deploy); err != nil {
			if k8serror.IsNotFound(err) {
				klog.Infof("deployment %s already deleted", deployName)
				continue
			}
			klog.Errorf("get deployment %s err: %+v", deployName, err)
			return err
		}

		if err := fc.rclient.Delete(context.Background(), &deploy); err != nil {
			klog.Errorf("delete deployment %s failed: %v", deployName, err)
			return err
		}
	}
	klog.Infof("delete fdb deploy success")
	return nil
}

func (fc *FdbConfig) CreateDeployIfNotExist() error {

	// add new deployment
	if fc.ClusterSize > len(fc.Nodes) {
		klog.Errorf("cluster size is more than node num, please check")
		return fmt.Errorf("cluster size is more than node num, please check")
	}

	if fc.ClusterSize < 2*fc.StorageReplicas-1 {
		klog.Errorf("cluster size is less than 2*storage-1 replicas, please check")
		return fmt.Errorf("cluster size is less than 2*storage-1 replicas, please check")
	}
	content, _ := fc.GetConfigContent()
	if content == "" {
		klog.Infof("fdb config is empty, wait")
		return fmt.Errorf("fdb config is empty, wait")
	}
	deployNum := fc.ClusterSize
	// make sure fdb deploy num is at least 2*fc.StorageReplicas-1(recommend)
	deployList := &appsv1.DeploymentList{}
	if err := fc.rclient.List(context.Background(), deployList, client.InNamespace(fc.Namespace), client.MatchingLabels{constant.ThreeFSFdbDeployKey: fc.Name}); err != nil {
		klog.Errorf("list deployment %s failed: %v", GetFdbDeployName(fc.Name), err)
	}
	existingDeploy := len(deployList.Items)
	for _, deploy := range deployList.Items {
		deployNodeName := deploy.Spec.Template.Spec.NodeSelector[constant.KubernetesHostnameKey]
		if !utils.StrListContains(fc.Nodes, deployNodeName) {
			existingDeploy--
		}
	}

	if existingDeploy == deployNum {
		klog.Infof("deployment num is satisfied, skip create")
		return nil
	}
	for _, node := range fc.Nodes {
		if existingDeploy == deployNum {
			klog.Infof("deployment num is satisfied, skip create")
			break
		}
		deploy := appsv1.Deployment{}
		deployName := fmt.Sprintf("%s-%s", GetFdbDeployName(fc.Name), utils.TranslatePlainNodeNameValid(node))
		if err := fc.rclient.Get(context.Background(), client.ObjectKey{Name: deployName, Namespace: fc.Namespace}, &deploy); err == nil {
			klog.Infof("deployment %s already exist", deployName)
			continue
		} else if !k8serror.IsNotFound(err) {
			klog.Errorf("get deployment %s failed: %v", deployName, err)
			return err
		}

		newdeploy := fc.WithDeployMeta(node).
			WithDeploySpec(node).
			WithDeployVolumes(node).
			WithDeployContainers(node, content).Deploys[node].Deployment
		if err := fc.rclient.Create(context.Background(), newdeploy); err != nil {
			klog.Errorf("create deployment %s failed: %v", deployName, err)
			return err
		}
		existingDeploy++
	}

	// delete deploy outside nodes
	if err := fc.rclient.List(context.Background(), deployList, client.InNamespace(fc.Namespace), client.MatchingLabels{constant.ThreeFSFdbDeployKey: fc.Name}); err != nil {
		klog.Errorf("list deployment %s failed: %v", GetFdbDeployName(fc.Name), err)
	}
	count := len(deployList.Items)
	for _, deploy := range deployList.Items {
		deployNodeName := deploy.Spec.Template.Spec.NodeSelector[constant.KubernetesHostnameKey]
		if !utils.StrListContains(fc.Nodes, deployNodeName) {
			if err := fc.rclient.Delete(context.Background(), &deploy); err != nil {
				klog.Errorf("delete deployment %s failed: %v", deploy.Name, err)
				return err
			}
			count--
		}
	}

	// delete deploy in nodes
	if count > fc.ClusterSize {
		for _, deploy := range deployList.Items {
			deployNodeName := deploy.Spec.Template.Spec.NodeSelector[constant.KubernetesHostnameKey]
			if utils.StrListContains(fc.Nodes, deployNodeName) {
				if err := fc.rclient.Delete(context.Background(), &deploy); err != nil {
					klog.Errorf("delete deployment %s failed: %v", deploy.Name, err)
					return err
				}
				count--
				if count == fc.ClusterSize {
					break
				}
			}
		}
	}

	return nil
}

func (fc *FdbConfig) WithDeployMeta(nodeName string) *FdbConfig {
	if fc.Deploys[nodeName] == nil {
		fc.Deploys[nodeName] = native_resources.NewDeployConfig()
	}
	DsLabels := map[string]string{
		constant.ThreeFSFdbDeployKey: fc.Name,
	}
	fc.Deploys[nodeName] = fc.Deploys[nodeName].WithDeployMeta(fmt.Sprintf("%s-%s", GetFdbDeployName(fc.Name), utils.TranslatePlainNodeNameValid(nodeName)), fc.Namespace, DsLabels)
	return fc
}

func (fc *FdbConfig) WithDeploySpec(nodeName string) *FdbConfig {
	dsLabels := map[string]string{
		constant.ThreeFSFdbDeployKey: fc.Name,
	}
	podLabels := map[string]string{
		constant.ThreeFSFdbDeployKey:   fc.Name,
		constant.ThreeFSComponentLabel: "fdb",
		constant.ThreeFSPodLabel:       "true",
	}

	nodemaps := map[string]string{
		constant.KubernetesHostnameKey: nodeName,
	}
	fc.Deploys[nodeName] = fc.Deploys[nodeName].WithDeploySpec(dsLabels, podLabels, 1, true, nodemaps, appsv1.RecreateDeploymentStrategyType)
	return fc
}

func (fc *FdbConfig) WithDeployVolumes(nodeName string) *FdbConfig {
	HostPathDirectoryOrCreate := corev1.HostPathDirectoryOrCreate
	volumes := []corev1.Volume{
		{
			Name: "log",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/opt/3fs/fdb/logs",
					Type: &HostPathDirectoryOrCreate,
				},
			},
		},
		{
			Name: "data",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/opt/3fs/fdb/data",
					Type: &HostPathDirectoryOrCreate,
				},
			},
		},
	}
	fc.Deploys[nodeName] = fc.Deploys[nodeName].WithVolumes(volumes)
	return fc
}

func (fc *FdbConfig) CheckResources() corev1.ResourceRequirements {
	if fc.Resources.Requests == nil {
		fc.Resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		}
	}
	if fc.Resources.Limits == nil {
		fc.Resources.Limits = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("5Gi"),
		}
	}
	return fc.Resources
}

func (fc *FdbConfig) WithDeployContainers(nodeName, content string) *FdbConfig {
	monitorImage := os.Getenv("FDB_IMAGE")

	ports := []corev1.ContainerPort{
		{
			Name:          "fdb",
			ContainerPort: int32(fc.Port),
		},
	}

	envs := []corev1.EnvVar{
		{
			Name:  constant.ENVFdbPort,
			Value: strconv.Itoa(fc.Port),
		},
		{
			Name:  constant.ENVFdbClusterFile,
			Value: constant.DefaultThreeFSFdbConfigPath,
		},
		{
			Name:  constant.ENVFdbClusterFileContent,
			Value: content,
		},
		{
			Name:  constant.ENVCoordinatorPort,
			Value: strconv.Itoa(fc.Port),
		},
		{
			Name:  constant.ENVProcessClass,
			Value: "unset",
		},
		{
			Name:  constant.ENVFdbNetworkMode,
			Value: "container",
		},
	}

	volumeMount := []corev1.VolumeMount{
		{
			Name:      "log",
			MountPath: "/var/log/foundationdb",
		},
		{
			Name:      "data",
			MountPath: "/var/lib/foundationdb/data",
		},
	}

	command := []string{
		"/tini",
		"-g", "--", "/entrypoint.sh",
	}
	fc.Deploys[nodeName] = fc.Deploys[nodeName].
		WithContainer("fdb", monitorImage, envs, nil, ports, fc.CheckResources(), volumeMount, command)

	return fc
}
