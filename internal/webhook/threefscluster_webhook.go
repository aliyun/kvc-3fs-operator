/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package webhook

import (
	"context"
	"fmt"
	"github.com/aliyun/kvc-3fs-operator/api/v1"
	"github.com/aliyun/kvc-3fs-operator/internal/constant"
	"github.com/aliyun/kvc-3fs-operator/internal/controller"
	"github.com/aliyun/kvc-3fs-operator/internal/fdb"
	"github.com/aliyun/kvc-3fs-operator/internal/native_resources"
	"github.com/aliyun/kvc-3fs-operator/internal/storage"
	"github.com/aliyun/kvc-3fs-operator/internal/utils"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"os"
	"path/filepath"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"strings"
)

type ThreeFsClusterValidator struct {
	Client client.Client
}

type ThreeFsClusterDefaulter struct {
	Client client.Client
}

// SetupWebhookWithManager will setup the manager to manage the webhooks
func SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&v1.ThreeFsCluster{}).
		WithValidator(&ThreeFsClusterValidator{Client: mgr.GetClient()}).
		Complete()
}

func SetupPodWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&corev1.Pod{}).
		WithDefaulter(&ThreeFsClusterDefaulter{Client: mgr.GetClient()}).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

// +kubebuilder:webhook:path=/mutate-threefs-aliyun-com-v1-threefscluster,mutating=true,failurePolicy=fail,sideEffects=None,groups=,resources=pods,verbs=create;update,versions=v1,name=mthreeFsCluster.kb.io,admissionReviewVersions=v1

var _ webhook.CustomDefaulter = &ThreeFsClusterDefaulter{}

func (r *ThreeFsClusterDefaulter) PatchSidecarContainer(pod *corev1.Pod, mountpath, name, namespace string) error {

	threeFsCluster := v1.ThreeFsCluster{}
	if err := r.Client.Get(context.Background(), client.ObjectKey{Name: name, Namespace: namespace}, &threeFsCluster); err != nil {
		return err
	}

	tokenConfig := corev1.ConfigMap{}
	if err := r.Client.Get(context.Background(), client.ObjectKey{Name: constant.DefaultTokenConfigName, Namespace: namespace}, &tokenConfig); err != nil {
		return err
	}

	containerConfig := native_resources.NewContainerConfig()
	fuseImage := os.Getenv("FUSE_IMAGE")

	mgmtdAddress := threeFsCluster.Status.MgmtdAddresses
	if mgmtdAddress == "" || !strings.Contains(mgmtdAddress, "RDMA") {
		return fmt.Errorf("get mgmtd address failed")
	}
	envs := []corev1.EnvVar{
		{
			Name:  "COMPONENT",
			Value: "fuse",
		},
		{
			Name:  "MOUNT_POINT",
			Value: "/3fs/stage",
		},
		{
			Name:  "MGMTD_SERVER_ADDRESS",
			Value: mgmtdAddress,
		},
		{
			Name:  "TOKEN",
			Value: tokenConfig.Data["token"],
		},
	}

	resources := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("15"),
			corev1.ResourceMemory: resource.MustParse("30Gi"),
		},
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("5"),
			corev1.ResourceMemory: resource.MustParse("5Gi"),
		},
	}
	if !utils.GetUseHostNetworkEnv() {
		resources.Limits[constant.ErdmaResourceKey] = resource.MustParse("1")
		resources.Requests[constant.ErdmaResourceKey] = resource.MustParse("1")
	}

	propagation := corev1.MountPropagationBidirectional
	volumeMount := []corev1.VolumeMount{
		{
			Name:      fmt.Sprintf("%s-dev", constant.DefaultSidecarPrefix),
			MountPath: "/dev",
		},
		{
			Name:      fmt.Sprintf("%s-log", constant.DefaultSidecarPrefix),
			MountPath: "/var/log/3fs",
		},
		{
			Name:             fmt.Sprintf("%s-fuse-volume", constant.DefaultSidecarPrefix),
			MountPath:        "/3fs/stage",
			MountPropagation: &propagation,
		},
	}

	comm := "/check_mount.sh /3fs/stage"
	size, ok := pod.Labels[constant.ThreeFSShmSize]
	if ok {
		comm = fmt.Sprintf("%s && mount -o remount,size=%s /dev/shm > /var/log/3fs/poststart.log", comm, size)
	}
	postStart := &corev1.LifecycleHandler{
		Exec: &corev1.ExecAction{
			Command: []string{"bash", "-c", comm},
		},
	}
	preStop := &corev1.LifecycleHandler{
		Exec: &corev1.ExecAction{
			Command: []string{"bash", "-c", "/check_umount.sh /3fs/stage > /var/log/3fs/prestop.log"},
		},
	}
	cc := containerConfig.
		WithContainer("threefs-sidecar", fuseImage, corev1.PullIfNotPresent, []string{"/setup.sh"}).
		WithContainerEnvs(envs, nil).
		WithContainerPrivileged(true).
		WithContainerResources(resources).
		WithContainerVolumeMounts(volumeMount).
		WithContainerLifeCycle(postStart, preStop)

	patchVm := corev1.VolumeMount{
		Name:             fmt.Sprintf("%s-fuse-volume", constant.DefaultSidecarPrefix),
		MountPath:        filepath.Join("/", mountpath),
		MountPropagation: &propagation,
	}

	privileged := true
	for idx, container := range pod.Spec.Containers {
		pod.Spec.Containers[idx].VolumeMounts = append(container.VolumeMounts, patchVm)
		pod.Spec.Containers[idx].SecurityContext = &corev1.SecurityContext{
			Privileged: &privileged,
		}
	}

	containers := append([]corev1.Container{*cc.Container}, pod.Spec.Containers...)
	pod.Spec.Containers = containers
	terminatingTimeout := int64(60)
	if pod.Spec.TerminationGracePeriodSeconds == nil {
		pod.Spec.TerminationGracePeriodSeconds = &terminatingTimeout
	} else {
		if *pod.Spec.TerminationGracePeriodSeconds < terminatingTimeout {
			pod.Spec.TerminationGracePeriodSeconds = &terminatingTimeout
		}
	}

	useShareIpc := true
	pod.Spec.ShareProcessNamespace = &useShareIpc
	volumes := []corev1.Volume{
		{
			Name: fmt.Sprintf("%s-dev", constant.DefaultSidecarPrefix),
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/dev",
				},
			},
		},
		{
			Name: fmt.Sprintf("%s-log", constant.DefaultSidecarPrefix),
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/opt/3fs/fuse/logs",
				},
			},
		},
		{
			Name: fmt.Sprintf("%s-fuse-volume", constant.DefaultSidecarPrefix),
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, volumes...)
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	pod.Labels[constant.ThreeFSPatched] = "true"
	return nil
}

// Default implements webhook.Defaulter so a webhook will be registered for the type
func (r *ThreeFsClusterDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return fmt.Errorf("expected a Pod object but got %T", obj)
	}
	mountpath, ok := pod.Labels[constant.ThreeFSMountLabel]
	threefsClusterName, ok2 := pod.Labels[constant.ThreeFSCrdLabel]
	threefsClusterNs, ok3 := pod.Labels[constant.ThreeFSCrdNsLabel]
	patched, ok4 := pod.Labels[constant.ThreeFSPatched]
	if ok4 {
		if patched == "true" {
			klog.Infof("pod %s has been patched, skip", pod.Name)
			return nil
		}
	}
	if !ok || !ok2 || !ok3 {
		klog.Infof("pod %s has no threefs mountpath/name/namespace label, skip", pod.Name)
		return nil
	}
	klog.Infof("pod %s has threefs mountpath label: %s, threefsClusterName: %s , ns:%s", pod.Name, mountpath, threefsClusterName, threefsClusterNs)
	return r.PatchSidecarContainer(pod, mountpath, threefsClusterName, threefsClusterNs)
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-threefs-aliyun-com-v1-threefscluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=threefs.aliyun.com.code.alibaba-inc.com,resources=threefsclusters,verbs=create;update,versions=v1,name=threefscluster.kb.io,admissionReviewVersions=v1

var _ webhook.CustomValidator = &ThreeFsClusterValidator{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (r *ThreeFsClusterValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	threefsCluster, ok := obj.(*v1.ThreeFsCluster)
	if !ok {
		return nil, fmt.Errorf("expected a ThreeFsCluster object but got %T", obj)
	}

	vfscList := &v1.ThreeFsClusterList{}
	if err := r.Client.List(context.Background(), vfscList); err != nil {
		return nil, err
	}
	if len(vfscList.Items) > 0 {
		return nil, fmt.Errorf("only support 1 threefsCluster, %s is exist", vfscList.Items[0].Name)
	}

	// check clickhouse
	if threefsCluster.Spec.Clickhouse.UseEcsClickhouse {
		if threefsCluster.Spec.Clickhouse.HostName == "" {
			return nil, fmt.Errorf("ecs clickhouse hostname is empty")
		}
	}

	// check tableid
	if threefsCluster.Spec.ChainTableId != "1" {
		return nil, fmt.Errorf("please set chaintableid to 1")
	}
	if threefsCluster.Spec.StripeSize <= 0 {
		return nil, fmt.Errorf("stripe size must be greater than 0")
	}

	// check storage node
	storageNodes, err := storage.FilterStorageNode(r.Client)
	if err != nil {
		return nil, err
	}
	targetNum := len(storageNodes) * len(threefsCluster.Spec.Storage.TargetPaths) * threefsCluster.Spec.Storage.TargetPerDisk
	if targetNum%threefsCluster.Spec.Storage.Replica != 0 {
		return nil, fmt.Errorf("threefsCluster %s args is not valid", threefsCluster.Name)
	}
	chainNum := targetNum / threefsCluster.Spec.Storage.Replica
	if chainNum < threefsCluster.Spec.StripeSize {
		return nil, fmt.Errorf("stripe size must be less or equal than chain num")
	}

	// check fdb
	fdbNodes, err := fdb.FilterFdbNodes(r.Client)
	if err != nil {
		return nil, err
	}
	if fdbNodes == nil || len(fdbNodes) == 0 {
		return nil, fmt.Errorf("fdb nodes pool is empty")
	}

	fdbThresReplica := 2
	if threefsCluster.Labels != nil && threefsCluster.Labels[constant.ThreeDebugMode] == "true" {
		fdbThresReplica = 1
	}
	if threefsCluster.Spec.Fdb.StorageReplicas < fdbThresReplica {
		return nil, fmt.Errorf("storage replicas must be greater than 1 for fault tolerance")
	}
	if threefsCluster.Spec.Fdb.StorageReplicas > (len(fdbNodes)+1)/2 {
		return nil, fmt.Errorf("n replica whith at least 2n-1 node is recommended")
	}
	if threefsCluster.Spec.Fdb.ClusterSize > len(fdbNodes) {
		return nil, fmt.Errorf("clustersize is larger then nodes pool")
	}
	nodeList := &corev1.NodeList{}
	if err := r.Client.List(context.Background(), nodeList); err != nil {
		return nil, err
	}
	nodeMaps := make(map[string]bool)
	for _, node := range nodeList.Items {
		nodeMaps[node.Name] = true
	}

	// check meta
	if threefsCluster.Spec.Meta.Replica > len(storageNodes) {
		return nil, fmt.Errorf("meta replica must be equal or less than storage nodes pool")
	}
	if threefsCluster.Spec.Meta.RdmaPort == threefsCluster.Spec.Storage.RdmaPort || threefsCluster.Spec.Meta.TcpPort == threefsCluster.Spec.Storage.TcpPort {
		return nil, fmt.Errorf("mgmtd port must be different from storage port")
	}

	// check mgmtd
	if threefsCluster.Spec.Mgmtd.Replica > len(storageNodes) {
		return nil, fmt.Errorf("mgmtd replica must be equal or less than storage nodes pool")
	}
	if threefsCluster.Spec.Mgmtd.RdmaPort == threefsCluster.Spec.Storage.RdmaPort || threefsCluster.Spec.Mgmtd.TcpPort == threefsCluster.Spec.Storage.TcpPort {
		return nil, fmt.Errorf("mgmtd port must be different from storage port")
	}

	// check storage
	if storageNodes == nil || len(storageNodes) == 0 {
		return nil, fmt.Errorf("storage nodes pool is empty")
	}
	if threefsCluster.Spec.Storage.Replica > len(storageNodes) {
		return nil, fmt.Errorf("storage replica must be equal or less than storage nodes pool")
	}

	// check fdb nodes are different from storage nodes
	if threefsCluster.Labels == nil && threefsCluster.Labels[constant.ThreeDebugMode] != "true" {
		for _, fdbNode := range fdbNodes {
			for _, storageNode := range storageNodes {
				if fdbNode == storageNode {
					return nil, fmt.Errorf("fdb node %s is same as storage node", fdbNode)
				}
			}
		}
	}

	return nil, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (r *ThreeFsClusterValidator) ValidateUpdate(ctx context.Context, oldObj runtime.Object, newObj runtime.Object) (admission.Warnings, error) {

	oldVfsc, ok := oldObj.(*v1.ThreeFsCluster)
	if !ok {
		return nil, fmt.Errorf("expected a ThreeFsCluster object but got %T", oldObj)
	}
	newVfsc, ok := newObj.(*v1.ThreeFsCluster)
	if !ok {
		return nil, fmt.Errorf("expected a ThreeFsCluster object but got %T", newObj)
	}

	if reflect.DeepEqual(oldVfsc.Spec, newVfsc.Spec) {
		klog.Infof("threefsCluster %s spec is not changed, skip", oldVfsc.Name)
		return nil, nil
	} else {
		klog.Infof("threefsCluster %s spec is changed, check", oldVfsc.Name)
	}

	// check in operator, no check here
	return nil, nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (r *ThreeFsClusterValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {

	threefsCluster, ok := obj.(*v1.ThreeFsCluster)
	if !ok {
		return nil, fmt.Errorf("expected a ThreeFsCluster object but got %T", obj)
	}

	// check if some pods are using threefsCluster
	podList := &corev1.PodList{}
	if err := r.Client.List(context.Background(), podList, client.InNamespace(threefsCluster.Namespace), client.MatchingLabels{constant.ThreeFSCrdLabel: threefsCluster.Name}); err != nil {
		if k8serror.IsNotFound(err) {
			return nil, nil
		}
	}
	if len(podList.Items) != 0 {
		return nil, fmt.Errorf("threefsCluster %s is still in use by %s, delete it first", threefsCluster.Name, podList.Items[0].Name)
	}

	// check if some vfsct are using threefsCluster
	if controller.IsProcessingTfsctExisted(r.Client) {
		return nil, fmt.Errorf("threefsCluster %s is still in use by threefs chaintable, delete processing vfsct first", threefsCluster.Name)
	}

	return nil, nil
}
