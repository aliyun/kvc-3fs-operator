package monitor

import (
	"context"
	"fmt"
	"github.com/aliyun/kvc-3fs-operator/internal/clickhouse"
	"github.com/aliyun/kvc-3fs-operator/internal/constant"
	"github.com/aliyun/kvc-3fs-operator/internal/native_resources"
	"github.com/aliyun/kvc-3fs-operator/internal/utils"
	"io"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
)

type MonitorConfig struct {
	Port         int
	Name         string
	Namespace    string
	Nodes        []string
	IsEcs        bool
	Resources    corev1.ResourceRequirements
	ChConfig     *clickhouse.ClickhouseConfig
	DeployConfig *native_resources.DelpoyConfig
	SvcConfig    *native_resources.ServiceConfig
	rclient      client.Client
}

func NewMonitorConfig(name, namespace string, nodes []string, isEcs bool, port int, resources corev1.ResourceRequirements, rclient client.Client, chConfig *clickhouse.ClickhouseConfig) *MonitorConfig {
	return &MonitorConfig{
		Name:         name,
		Namespace:    namespace,
		Nodes:        nodes,
		IsEcs:        isEcs,
		Port:         port,
		ChConfig:     chConfig,
		Resources:    resources,
		DeployConfig: native_resources.NewDeployConfig(),
		SvcConfig:    native_resources.NewServiceConfig(),
		rclient:      rclient,
	}
}

func GetMonitorDeployName(name string) string {
	return fmt.Sprintf("%s-%s", name, "monitor")
}

func (mc *MonitorConfig) TagNodeLabel() error {
	nodeList := &corev1.NodeList{}
	if err := mc.rclient.List(context.Background(), nodeList); err != nil {
		klog.Errorf("list node failed: %v", err)
		return err
	}
	for _, node := range nodeList.Items {
		node.Labels[constant.ThreeFSMonitorNodeKey] = "true"
		if err := mc.rclient.Update(context.Background(), &node); err != nil {
			klog.Errorf("update node %s failed: %v", node.Name, err)
			return err
		}
	}
	return nil
}

func (mc *MonitorConfig) ParseServiceIp() (string, error) {
	svc := &corev1.Service{}
	if err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: GetMonitorDeployName(mc.Name), Namespace: mc.Namespace}, svc); err != nil {
		klog.Errorf("get svc %s failed: %v", GetMonitorDeployName(mc.Name), err)
		return "", err
	}
	return svc.Spec.ClusterIP, nil
}

func (mc *MonitorConfig) UpdateMonitorConfig() error {
	configMap := &corev1.ConfigMap{}
	if err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: constant.DefaultThreeFSConfigMapName, Namespace: mc.Namespace}, configMap); err != nil {
		klog.Errorf("get configmap %s failed: %v", constant.DefaultThreeFSConfigMapName, err)
		return err
	}

	// already parsed
	if configMap.Labels[constant.ThreeFSMonitorConfigParseKey] == "true" {
		return nil
	}

	ip, err := mc.ChConfig.ParseServiceIp()
	if err != nil {
		klog.Errorf("get clickhouse service ip failed: %v", err)
		return err
	}
	replaceMaps := map[string]string{
		"clickhouse_host":   ip,
		"clickhouse_user":   mc.ChConfig.ClickhouseUser,
		"clickhouse_passwd": mc.ChConfig.ClickhousePassword,
		"clickhouse_port":   strconv.Itoa(mc.ChConfig.TcpPort),
	}
	configTemplate, err := utils.CreateTemplate(constant.ThreeFSMonitorMain, configMap.Data[constant.ThreeFSMonitorMain])
	if err != nil {
		klog.Errorf("create template failed: %v", err)
		return err
	}

	tmpf, err := utils.RenderTemplate(configTemplate, replaceMaps)
	if err != nil {
		klog.Errorf("render template failed: %v", err)
		return err
	}
	_, err = tmpf.Seek(0, io.SeekStart)
	content, err := io.ReadAll(tmpf)
	if err != nil {
		klog.Errorf("read tmp config file failed: %v", err)
		return err
	}
	tmpf.Close()
	os.Remove(tmpf.Name())
	parseLabels := map[string]string{
		constant.ThreeFSMonitorConfigParseKey: "true",
	}
	utils.MergeMapsInPlace(configMap.Labels, parseLabels)
	configMap.Data[constant.ThreeFSMonitorMain] = string(content)
	if err := mc.rclient.Update(context.Background(), configMap); err != nil {
		klog.Errorf("update configmap %s failed: %v", constant.DefaultThreeFSConfigMapName, err)
		return err
	}
	return nil
}

func (mc *MonitorConfig) buildMonitorService() *corev1.Service {
	svcLabels := map[string]string{
		constant.ThreeFSMonitorSvcKey: mc.Name,
	}

	ports := []corev1.ServicePort{
		{
			Name: "monitor",
			Port: int32(mc.Port),
		},
	}

	mc.SvcConfig.
		WithServiceMeta(GetMonitorDeployName(mc.Name), mc.Namespace).
		WithServiceSpec(svcLabels, ports, corev1.ServiceTypeClusterIP, "")

	return mc.SvcConfig.Service
}

func (mc *MonitorConfig) CreateServiceIfNotExist() error {
	svc := &corev1.Service{}
	err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: GetMonitorDeployName(mc.Name), Namespace: mc.Namespace}, svc)
	if err == nil {
		klog.Infof("service %s already exist", GetMonitorDeployName(mc.Name))
		return nil
	} else if !k8serror.IsNotFound(err) {
		klog.Errorf("get svc %s failed: %v", GetMonitorDeployName(mc.Name), err)
		return err
	}

	if err := mc.rclient.Create(context.Background(), mc.buildMonitorService()); err != nil {
		klog.Errorf("create svc %s failed: %v", GetMonitorDeployName(mc.Name), err)
		return err
	}
	return nil
}

func (mc *MonitorConfig) DeleteServiceIfExist() error {
	svc := &corev1.Service{}
	err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: GetMonitorDeployName(mc.Name), Namespace: mc.Namespace}, svc)
	if err != nil {
		if k8serror.IsNotFound(err) {
			return nil
		}
		klog.Errorf("get service %s err: %+v", GetMonitorDeployName(mc.Name), err)
		return err
	}

	if err := mc.rclient.Delete(context.Background(), svc); err != nil {
		klog.Errorf("delete svc %s failed: %v", GetMonitorDeployName(mc.Name), err)
		return err
	}
	return nil
}

func (mc *MonitorConfig) DeleteDeployIfExist() error {
	deploy := &appsv1.Deployment{}
	err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: GetMonitorDeployName(mc.Name), Namespace: mc.Namespace}, deploy)
	if err != nil {
		if k8serror.IsNotFound(err) {
			return nil
		}
		klog.Errorf("get deployment %s err: %+v", GetMonitorDeployName(mc.Name), err)
		return err
	}

	if err := mc.rclient.Delete(context.Background(), deploy); err != nil {
		klog.Errorf("delete deployment %s failed: %v", GetMonitorDeployName(mc.Name), err)
		return err
	}
	return nil
}

func (mc *MonitorConfig) CreateDeployIfNotExist() error {
	deploy := &appsv1.Deployment{}
	err := mc.rclient.Get(context.Background(), client.ObjectKey{Name: GetMonitorDeployName(mc.Name), Namespace: mc.Namespace}, deploy)
	if err == nil {
		klog.Infof("deployment %s already exist", GetMonitorDeployName(mc.Name))
		return nil
	} else if !k8serror.IsNotFound(err) {
		klog.Errorf("get deployment %s failed: %v", GetMonitorDeployName(mc.Name), err)
		return err
	}

	if err := mc.rclient.Create(context.Background(),
		mc.WithDeployMeta().
			WithDeploySpec().
			WithVolumes().
			WithContainers().DeployConfig.Deployment); err != nil {
		klog.Errorf("create deployment %s failed: %v", GetMonitorDeployName(mc.Name), err)
		return err
	}
	return nil
}

func (mc *MonitorConfig) WithDeployMeta() *MonitorConfig {
	deployLabels := map[string]string{
		constant.ThreeFSMonitorDeploymentKey: mc.Name,
	}
	mc.DeployConfig = mc.DeployConfig.WithDeployMeta(GetMonitorDeployName(mc.Name), mc.Namespace, deployLabels)
	return mc
}

func (mc *MonitorConfig) WithDeploySpec() *MonitorConfig {
	deployLabels := map[string]string{
		constant.ThreeFSMonitorDeploymentKey: mc.Name,
	}
	podLabels := map[string]string{
		constant.ThreeFSMonitorDeploymentKey: mc.Name,
		constant.ThreeFSMonitorSvcKey:        mc.Name,
		constant.ThreeFSComponentLabel:       "monitor",
		constant.ThreeFSPodLabel:             "true",
	}
	nodemaps := make(map[string]string)
	if mc.IsEcs {
		nodemaps[constant.ThreeFSStorageNodeKey] = "true"
	}

	replicaNum := int32(1)
	mc.DeployConfig = mc.DeployConfig.WithDeploySpec(deployLabels, podLabels, replicaNum, false, nodemaps, appsv1.RollingUpdateDeploymentStrategyType)
	return mc
}

func (mc *MonitorConfig) WithVolumes() *MonitorConfig {
	HostPathDirectoryOrCreate := corev1.HostPathDirectoryOrCreate
	volumes := []corev1.Volume{
		{
			Name: "log",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/opt/3fs/monitor/log",
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
	mc.DeployConfig = mc.DeployConfig.WithVolumes(volumes)
	return mc
}

func (mc *MonitorConfig) CheckResources() corev1.ResourceRequirements {
	if mc.Resources.Requests == nil {
		mc.Resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("400Mi"),
		}
	}
	if mc.Resources.Limits == nil {
		mc.Resources.Limits = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		}
	}
	return mc.Resources
}

func (mc *MonitorConfig) WithContainers() *MonitorConfig {
	monitorImage := os.Getenv("MONITOR_IMAGE")

	ports := []corev1.ContainerPort{
		{
			Name:          "monitor",
			ContainerPort: int32(mc.Port),
		},
	}

	var ip string
	if mc.IsEcs {
		ip = mc.ChConfig.ClickhouseHostname
	} else {
		ip, _ = mc.ChConfig.ParseServiceIp()
	}

	envs := []corev1.EnvVar{
		{
			Name:  constant.ENVClickhouseUserName,
			Value: mc.ChConfig.ClickhouseUser,
		},
		{
			Name:  constant.ENVClickhousePasswordName,
			Value: mc.ChConfig.ClickhousePassword,
		},
		{
			Name:  constant.ENVClickhouseTcpPort,
			Value: strconv.Itoa(mc.ChConfig.TcpPort),
		},
		{
			Name:  constant.ENVClickhouseHosTName,
			Value: ip,
		},
		{
			Name:  "COMPONENT",
			Value: "monitor",
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
	mc.DeployConfig = mc.DeployConfig.
		WithContainer("monitor", monitorImage, envs, nil, ports, mc.CheckResources(), volumeMount, command)

	return mc
}
