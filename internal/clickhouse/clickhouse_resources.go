package clickhouse

import (
	"context"
	"fmt"
	"github.com/aliyun/kvc-3fs-operator/internal/constant"
	"github.com/aliyun/kvc-3fs-operator/internal/native_resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
)

type ClickhouseConfig struct {
	Name               string   `json:"name"`
	Namespace          string   `json:"namespace"`
	Nodes              []string `json:"nodes"`
	TcpPort            int      `json:"tcp_port"`
	ClickhouseConfig   string   `json:"clickhouse_config"`
	ClickhouseHostname string   `json:"clickhouse_hostname"`
	ClickhouseUser     string   `json:"clickhouse_user"`
	ClickhousePassword string   `json:"clickhouse_password"`
	Resources          corev1.ResourceRequirements
	DeployConfig       *native_resources.DelpoyConfig
	SvcConfig          *native_resources.ServiceConfig
	rclient            client.Client
}

func NewClickhouseConfig(name, namespace string, nodes []string, clickhouseConfig, clickhouseUser, clickhouseHostname, clickhousePasswd string,
	tcpPort int, resources corev1.ResourceRequirements, rclient client.Client) *ClickhouseConfig {
	return &ClickhouseConfig{
		Name:               name,
		Namespace:          namespace,
		Nodes:              nodes,
		TcpPort:            tcpPort,
		ClickhouseConfig:   clickhouseConfig,
		ClickhouseUser:     clickhouseUser,
		ClickhouseHostname: clickhouseHostname,
		ClickhousePassword: clickhousePasswd,
		Resources:          resources,
		DeployConfig:       native_resources.NewDeployConfig(),
		SvcConfig:          native_resources.NewServiceConfig(),
		rclient:            rclient,
	}
}

func GetClickhouseDeployName(name string) string {
	return fmt.Sprintf("%s-%s", name, "clickhouse")
}

func (c *ClickhouseConfig) String() string {
	return "tcp_port=" + strconv.Itoa(c.TcpPort) + "&clickhouse_config=" + c.ClickhouseConfig + "&clickhouse_user=" + c.ClickhouseUser + "&clickhouse_password=" + c.ClickhousePassword
}

func (c *ClickhouseConfig) buildChService() *corev1.Service {
	svcLabels := map[string]string{
		constant.ThreeFSClickhouseSvcKey: c.Name,
	}

	ports := []corev1.ServicePort{
		{
			Name: "tcpport",
			Port: int32(c.TcpPort),
		},
	}

	c.SvcConfig.
		WithServiceMeta(GetClickhouseDeployName(c.Name), c.Namespace).
		WithServiceSpec(svcLabels, ports, corev1.ServiceTypeClusterIP, "")

	return c.SvcConfig.Service
}

func (c *ClickhouseConfig) ParseServiceIp() (string, error) {
	svc := &corev1.Service{}
	if err := c.rclient.Get(context.Background(), client.ObjectKey{Name: GetClickhouseDeployName(c.Name), Namespace: c.Namespace}, svc); err != nil {
		klog.Errorf("get svc %s failed: %v", GetClickhouseDeployName(c.Name), err)
		return "", err
	}
	return svc.Spec.ClusterIP, nil
}

func (c *ClickhouseConfig) CreateServiceIfNotExist() error {
	svc := &corev1.Service{}
	err := c.rclient.Get(context.Background(), client.ObjectKey{Name: GetClickhouseDeployName(c.Name), Namespace: c.Namespace}, svc)
	if err == nil {
		klog.Infof("service %s already exist", GetClickhouseDeployName(c.Name))
		return nil
	} else if !k8serror.IsNotFound(err) {
		klog.Errorf("get svc %s failed: %v", GetClickhouseDeployName(c.Name), err)
		return err
	}

	if err := c.rclient.Create(context.Background(), c.buildChService()); err != nil {
		klog.Errorf("create svc %s failed: %v", GetClickhouseDeployName(c.Name), err)
		return err
	}
	return nil
}

func (c *ClickhouseConfig) DeleteServiceIfExist() error {
	svc := &corev1.Service{}
	err := c.rclient.Get(context.Background(), client.ObjectKey{Name: GetClickhouseDeployName(c.Name), Namespace: c.Namespace}, svc)
	if err != nil {
		if k8serror.IsNotFound(err) {
			return nil
		}
		klog.Errorf("get service %s err: %+v", GetClickhouseDeployName(c.Name), err)
		return err
	}

	if err := c.rclient.Delete(context.Background(), svc); err != nil {
		klog.Errorf("delete svc %s failed: %v", GetClickhouseDeployName(c.Name), err)
		return err
	}
	return nil
}

func (c *ClickhouseConfig) DeleteDeployIfExist() error {
	deploy := &appsv1.Deployment{}
	err := c.rclient.Get(context.Background(), client.ObjectKey{Name: GetClickhouseDeployName(c.Name), Namespace: c.Namespace}, deploy)
	if err != nil {
		if k8serror.IsNotFound(err) {
			return nil
		}
		klog.Errorf("get deployment %s err: %+v", GetClickhouseDeployName(c.Name), err)
		return err
	}

	if err := c.rclient.Delete(context.Background(), deploy); err != nil {
		klog.Errorf("delete deployment %s failed: %v", GetClickhouseDeployName(c.Name), err)
		return err
	}
	return nil
}

func (c *ClickhouseConfig) CreateDeployIfNotExist() error {
	deploy := &appsv1.Deployment{}
	err := c.rclient.Get(context.Background(), client.ObjectKey{Name: GetClickhouseDeployName(c.Name), Namespace: c.Namespace}, deploy)
	if err == nil {
		klog.Infof("deployment %s already exist", GetClickhouseDeployName(c.Name))
		return nil
	} else if !k8serror.IsNotFound(err) {
		klog.Errorf("get deployment %s failed: %v", GetClickhouseDeployName(c.Name), err)
		return err
	}

	if err := c.rclient.Create(context.Background(), c.WithDeployMeta().WithDeploySpec().WithVolumes().WithContainers().DeployConfig.Deployment); err != nil {
		klog.Errorf("create deployment %s failed: %v", GetClickhouseDeployName(c.Name), err)
		return err
	}
	return nil
}

func (c *ClickhouseConfig) WithDeployMeta() *ClickhouseConfig {
	deployLabels := map[string]string{
		constant.ThreeFSClickhouseDeploymentKey: c.Name,
	}
	c.DeployConfig = c.DeployConfig.WithDeployMeta(GetClickhouseDeployName(c.Name), c.Namespace, deployLabels)
	return c
}

func (c *ClickhouseConfig) WithDeploySpec() *ClickhouseConfig {
	deployLabels := map[string]string{
		constant.ThreeFSClickhouseDeploymentKey: c.Name,
	}
	podLabels := map[string]string{
		constant.ThreeFSClickhouseDeploymentKey: c.Name,
		constant.ThreeFSClickhouseSvcKey:        c.Name,
		constant.ThreeFSComponentLabel:          "clickhouse",
		constant.ThreeFSPodLabel:                "true",
	}
	replicaNum := int32(1)

	nodemaps := map[string]string{
		constant.KubernetesHostnameKey: c.Nodes[0],
	}
	c.DeployConfig = c.DeployConfig.WithDeploySpec(deployLabels, podLabels, replicaNum, false, nodemaps, appsv1.RollingUpdateDeploymentStrategyType)
	return c
}

func (c *ClickhouseConfig) WithVolumes() *ClickhouseConfig {
	HostPathDirectoryOrCreate := corev1.HostPathDirectoryOrCreate
	volumes := []corev1.Volume{
		{
			Name: "data",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/opt/3fs/clickhouse/data",
					Type: &HostPathDirectoryOrCreate,
				},
			},
		},
		{
			Name: "log",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/opt/3fs/clickhouse/log",
					Type: &HostPathDirectoryOrCreate,
				},
			},
		},
	}
	c.DeployConfig = c.DeployConfig.WithVolumes(volumes)
	return c
}

func (c *ClickhouseConfig) CheckResources() corev1.ResourceRequirements {
	if c.Resources.Requests == nil {
		c.Resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1.5"),
			corev1.ResourceMemory: resource.MustParse("2Gi"),
		}
	}
	if c.Resources.Limits == nil {
		c.Resources.Limits = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("4"),
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		}
	}
	return c.Resources
}

func (c *ClickhouseConfig) WithContainers() *ClickhouseConfig {
	clickImage := os.Getenv("CLICKHOUSE_IMAGE")
	envs := []corev1.EnvVar{
		{
			Name:  constant.ENVClickhouseTcpPort,
			Value: strconv.Itoa(c.TcpPort),
		},
		{
			Name:  constant.ENVClickhouseConfigName,
			Value: c.ClickhouseConfig,
		},
		{
			Name:  constant.ENVClickhouseUserName,
			Value: c.ClickhouseUser,
		},
		{
			Name:  constant.ENVClickhousePasswordName,
			Value: c.ClickhousePassword,
		},
	}

	ports := []corev1.ContainerPort{
		{
			Name:          "tcpport",
			ContainerPort: int32(c.TcpPort),
		},
	}

	volumeMount := []corev1.VolumeMount{
		{
			Name:      "log",
			MountPath: "/var/log/clickhouse-server",
		},
		{
			Name:      "data",
			MountPath: "/var/lib/clickhouse",
		},
	}

	c.DeployConfig = c.DeployConfig.
		WithContainer("clickhouse", clickImage, envs, nil, ports, c.CheckResources(), volumeMount, nil)

	return c
}
