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
	"github.com/aliyun/kvc-3fs-operator/internal/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

type VcnsFsChaintableValidator struct {
	Client client.Client
}

// SetupChaintableWebhookWithManager will setup the manager to manage the webhooks
func SetupChaintableWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&v1.ThreeFsChainTable{}).
		WithValidator(&VcnsFsChaintableValidator{Client: mgr.GetClient()}).
		Complete()
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-threefs-aliyun-com-v1-threefsChanintable,mutating=false,failurePolicy=fail,sideEffects=None,groups=threefs.aliyun.com.code.alibaba-inc.com,resources=threefsChanintables,verbs=create;update;delete,versions=v1,name=threefsChanintable.kb.io,admissionReviewVersions=v1

var _ webhook.CustomValidator = &VcnsFsChaintableValidator{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (r *VcnsFsChaintableValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {

	vfsct, ok := obj.(*v1.ThreeFsChainTable)
	if !ok {
		return nil, fmt.Errorf("expected a ThreeFsChainTable object but got %T", obj)
	}

	vfsc := &v1.ThreeFsCluster{}
	if err := r.Client.Get(ctx, client.ObjectKey{Name: vfsct.Spec.ThreeFsClusterName, Namespace: vfsct.Spec.ThreeFsClusterNamespace}, vfsc); err != nil {
		return nil, fmt.Errorf("get threefsChanintable %s failed: %v", vfsct.Spec.ThreeFsClusterName, err)
	}

	if vfsc.Status.NodesInfo.StorageBackupNodes != nil && len(vfsc.Status.NodesInfo.StorageBackupNodes) > 0 {
		for _, node := range vfsct.Spec.NewNode {
			if !utils.StrListContains(vfsc.Status.NodesInfo.StorageBackupNodes, node) {
				return nil, fmt.Errorf("threefsChanintable job %s newNode %s is not in storage backup node list", vfsct.Name, node)
			}
		}
	}
	if vfsct.Spec.Type == constant.ThreeFSChainTableTypeCreate {
		addSize := 3
		// set addSize to 2 when test
		if vfsct.Labels != nil && vfsct.Labels[constant.ThreeDebugMode] == "true" {
			addSize = 2
		}
		if vfsct.Spec.NewNode == nil || len(vfsct.Spec.NewNode) < addSize || len(vfsct.Spec.NewNode) < vfsc.Spec.Storage.Replica {
			klog.Errorf("threefsChanintable job %s newNode is less then %d", vfsct.Name, addSize)
			return nil, fmt.Errorf("threefsChanintable job %s newNode is less then 3", vfsct.Name)
		}
		targetNum := len(vfsct.Spec.NewNode) * len(vfsc.Spec.Storage.TargetPaths) * vfsc.Spec.Storage.TargetPerDisk
		if targetNum%vfsc.Spec.Storage.Replica != 0 {
			return nil, fmt.Errorf("threefsChanintable job %s newNode number is not valid", vfsct.Name)
		}
		chainNum := targetNum / vfsc.Spec.Storage.Replica
		if chainNum < vfsc.Spec.StripeSize {
			return nil, fmt.Errorf("stripe size must be less or eqaul than chain num")
		}
	} else if vfsct.Spec.Type == constant.ThreeFSChainTableTypeDelete {
		if vfsct.Spec.OldNode == nil {
			klog.Errorf("threefsChanintable job %s oldNode is empty", vfsct.Name)
			return nil, fmt.Errorf("threefsChanintable job %s oldNode is empty", vfsct.Name)
		}
		return nil, fmt.Errorf("not support type %s", vfsct.Spec.Type)
	} else if vfsct.Spec.Type == constant.ThreeFSChainTableTypeReplace {
		if vfsct.Spec.OldNode == nil || vfsct.Spec.NewNode == nil || len(vfsct.Spec.OldNode) != 1 || len(vfsct.Spec.NewNode) != 1 {
			klog.Errorf("threefsChanintable job %s oldNode or newNode is empty or more then 1", vfsct.Name)
			return nil, fmt.Errorf("threefsChanintable job %s oldNode or newNode is empty or more then 1", vfsct.Name)
		}
	} else {
		klog.Errorf("threefsChanintable job %s type %s is invalid", vfsct.Name, vfsct.Spec.Type)
	}

	exsitingnodeMaps := make(map[string]bool)
	deployList := &appsv1.DeploymentList{}
	if err := r.Client.List(ctx, deployList, client.MatchingLabels{constant.ThreeFSStorageDeployKey: vfsc.Name}); err != nil {
		return nil, err
	}
	for _, deploy := range deployList.Items {
		exsitingnodeMaps[deploy.Spec.Template.Spec.NodeSelector[constant.KubernetesHostnameKey]] = true
	}

	// check new node validation
	newnodeMaps := make(map[string]bool)
	for _, node := range vfsct.Spec.NewNode {
		// check new node is already used in storage list
		if exsitingnodeMaps[node] {
			klog.Errorf("threefsChanintable job %s newNode %s is already used in storage list", vfsct.Name, node)
			return nil, fmt.Errorf("threefsChanintable job %s newNode %s is already used in storage list", vfsct.Name, node)
		}
		// check fault label
		nodeObj := corev1.Node{}
		if err := r.Client.Get(ctx, client.ObjectKey{Name: node}, &nodeObj); err != nil {
			return nil, err
		}
		if _, ok := nodeObj.Labels[constant.ThreeFSStorageFaultNodeKey]; ok {
			klog.Errorf("threefsChanintable job %s newNode %s is in storage fault node list", vfsct.Name, node)
			return nil, fmt.Errorf("threefsChanintable job %s newNode %s is in storage fault node list", vfsct.Name, node)
		}
		newnodeMaps[node] = true
	}

	// check old node validation
	for _, node := range vfsct.Spec.OldNode {
		if !exsitingnodeMaps[node] {
			klog.Errorf("threefsChanintable job %s oldNode %s is not in storage node list", vfsct.Name, node)
			return nil, fmt.Errorf("threefsChanintable job %s oldNode %s is not in storage node list", vfsct.Name, node)
		}
		if newnodeMaps[node] {
			klog.Errorf("threefsChanintable job %s oldNode %s is already used in newNode list", vfsct.Name, node)
			return nil, fmt.Errorf("threefsChanintable job %s oldNode %s is already used in newNode list", vfsct.Name, node)
		}
	}

	vfsctList := &v1.ThreeFsChainTableList{}
	if err := r.Client.List(ctx, vfsctList); err != nil {
		return nil, err
	}
	for _, item := range vfsctList.Items {
		if item.Status.Phase != constant.ThreeFSChainTableFinishedStatus {
			return nil, fmt.Errorf("ThreeFsChainTable %s is processing now, not allowed to be created new ThreeFsChainTable", item.Name)
		}
	}

	return nil, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (r *VcnsFsChaintableValidator) ValidateUpdate(ctx context.Context, oldObj runtime.Object, newObj runtime.Object) (admission.Warnings, error) {

	oldvfsct, ok := oldObj.(*v1.ThreeFsChainTable)
	if !ok {
		return nil, fmt.Errorf("expected a ThreeFsChainTable object but got %T", oldObj)
	}

	newvfsct, ok := newObj.(*v1.ThreeFsChainTable)
	if !ok {
		return nil, fmt.Errorf("expected a ThreeFsChainTable object but got %T", newObj)
	}

	if !reflect.DeepEqual(oldvfsct.Spec, newvfsct.Spec) {
		return nil, fmt.Errorf("threefsChanintable spec is not allowed to be updated")
	}

	return nil, nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (r *VcnsFsChaintableValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {

	vfsct, ok := obj.(*v1.ThreeFsChainTable)
	if !ok {
		return nil, fmt.Errorf("expected a ThreeFsChainTable object but got %T", obj)
	}

	ok = false
	if vfsct.Labels != nil {
		_, ok = vfsct.Labels[constant.ThreeDebugMode]
	}

	if !ok && vfsct.Status.Phase != constant.ThreeFSChainTableFinishedStatus {
		return nil, fmt.Errorf("threefsChanintable status before finished is not allowed to be deleted")
	}

	return nil, nil
}
