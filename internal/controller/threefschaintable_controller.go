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

package controller

import (
	"context"
	"fmt"
	threefsv1 "github.com/aliyun/kvc-3fs-operator/api/v1"
	clientcomm "github.com/aliyun/kvc-3fs-operator/internal/client"
	"github.com/aliyun/kvc-3fs-operator/internal/constant"
	"github.com/aliyun/kvc-3fs-operator/internal/storage"
	"github.com/aliyun/kvc-3fs-operator/internal/utils"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"path/filepath"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"strings"
)

// ThreeFsChainTableReconciler reconciles a ThreeFsChainTable object
type ThreeFsChainTableReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=threefs.aliyun.com.code.alibaba-inc.com,resources=threefschaintables,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=threefs.aliyun.com.code.alibaba-inc.com,resources=threefschaintables/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=threefs.aliyun.com.code.alibaba-inc.com,resources=threefschaintables/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ThreeFsChainTable object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/reconcile
func (r *ThreeFsChainTableReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {

	klog.Infof("Reconciling threefsChanintable %s", req.NamespacedName)
	threefsChanintable := &threefsv1.ThreeFsChainTable{}
	if err := r.Client.Get(ctx, req.NamespacedName, threefsChanintable); err != nil {
		if k8serror.IsNotFound(err) {
			klog.Infof("threefsChanintable %s has been deleted", req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if threefsChanintable.DeletionTimestamp != nil {
		if utils.StrListContains(threefsChanintable.GetFinalizers(), constant.ThreeFSFinalizer) && !controllerutil.RemoveFinalizer(threefsChanintable, constant.ThreeFSFinalizer) {
			return ctrl.Result{}, fmt.Errorf("remove finalizer %s failed", constant.ThreeFSFinalizer)
		}
		if err := r.Update(context.Background(), threefsChanintable); err != nil {
			klog.Errorf("remove threefsCluster %s finalizers failed, err: %+v", threefsChanintable.Name, err)
			return ctrl.Result{}, err
		}
		klog.Infof("delete threefsCluster %s related resources success", threefsChanintable.Name)
	}

	if threefsChanintable.Status.Phase == constant.ThreeFSChainTableFinishedStatus {
		klog.Infof("threefsChanintable job %s has finished, skip", req.NamespacedName)
		return ctrl.Result{}, nil
	}

	vfsc := threefsv1.ThreeFsCluster{}
	if err := r.Get(context.Background(), client.ObjectKey{Name: threefsChanintable.Spec.ThreeFsClusterName, Namespace: threefsChanintable.Spec.ThreeFsClusterNamespace}, &vfsc); err != nil {
		klog.Errorf("get ThreeFsCluster %s err: %+v", threefsChanintable.Spec.ThreeFsClusterName, err)
		return ctrl.Result{}, err
	}

	if vfsc.Status.Phase != constant.ThreeFSClusterReadyStatus {
		klog.Errorf("threefsChanintable job %s ThreeFsCluster is not ready, try later", req.NamespacedName)
		return ctrl.Result{}, fmt.Errorf("threefsChanintable job %s ThreeFsCluster is not ready", req.NamespacedName)
	}

	if !vfsc.Status.TagMgmtd || !strings.Contains(vfsc.Status.MgmtdAddresses, "RDMA") {
		klog.Errorf("threefsChanintable job %s ThreeFsCluster TagMgmtd is not set, try later", req.NamespacedName)
		return ctrl.Result{}, fmt.Errorf("threefsChanintable job %s ThreeFsCluster TagMgmtd is not set", req.NamespacedName)
	}
	adminCliConfig := clientcomm.NewAdminCli(vfsc.Status.MgmtdAddresses,
		filepath.Join(constant.DefaultConfigPath, constant.ThreeFSAdminCliMain))

	// for add/replace, need to check storage process status
	if threefsChanintable.Spec.NewNode != nil {
		for _, newnode := range threefsChanintable.Spec.NewNode {
			// update threefs cluster node spec for reconcile
			if !utils.StrListContains(vfsc.Status.NodesInfo.StorageNodes, newnode) {
				vfsc.Status.NodesInfo.StorageNodes = append(vfsc.Status.NodesInfo.StorageNodes, newnode)
				// if auto storage policy, need to remove from backup nodes
				if utils.StrListContains(vfsc.Status.NodesInfo.StorageBackupNodes, newnode) {
					vfsc.Status.NodesInfo.StorageBackupNodes = utils.StrListRemove(vfsc.Status.NodesInfo.StorageBackupNodes, newnode)
				}
				if err := r.Status().Update(context.Background(), &vfsc); err != nil {
					klog.Errorf("update ThreeFsCluster %s err: %+v", threefsChanintable.Spec.ThreeFsClusterName, err)
					return ctrl.Result{}, err
				}
			}

			storageEnvConfig := &corev1.ConfigMap{}
			if err := r.Get(context.Background(), client.ObjectKey{Name: storage.GetStorageDeployName(threefsChanintable.Spec.ThreeFsClusterName), Namespace: threefsChanintable.Spec.ThreeFsClusterNamespace}, storageEnvConfig); err != nil {
				klog.Errorf("get configmap %s err: %+v", storage.GetStorageDeployName(threefsChanintable.Spec.ThreeFsClusterName), err)
				return ctrl.Result{}, err
			}

			parseNodeName := strings.ReplaceAll(newnode, "-", "_")
			parseNodeName = strings.ReplaceAll(parseNodeName, ".", "_")
			if _, ok := storageEnvConfig.Data[parseNodeName]; !ok {
				klog.Errorf("threefsChanintable job %s newNode is not exist in env", req.NamespacedName)
				return ctrl.Result{}, fmt.Errorf("threefsChanintable job %s newNode is not exist in env", req.NamespacedName)
			}
			klog.Infof("threefsChanintable job %s newNode is exist in env", req.NamespacedName)

			// tag for new node
			if err := TagStorageNode(newnode, r.Client); err != nil {
				klog.Errorf("threefsChanintable job %s newNode is not ready", req.NamespacedName)
				return ctrl.Result{}, err
			}

			if !CheckComponentStatus(adminCliConfig, "STORAGE", newnode, true, r.Client) {
				klog.Errorf("threefsChanintable job %s newNode %s is not ready", req.NamespacedName, newnode)
				return ctrl.Result{}, fmt.Errorf("threefsChanintable job %s newnode is not ready", req.NamespacedName)
			}
		}
	}

	if threefsChanintable.Status.Phase == "" {
		if err := r.UpdatePhase(constant.ThreeFSChainTableProcessingStatus, threefsChanintable); err != nil {
			return ctrl.Result{}, err
		}
	}

	if !utils.StrListContains(threefsChanintable.GetFinalizers(), constant.ThreeFSFinalizer) {
		controllerutil.AddFinalizer(threefsChanintable, constant.ThreeFSFinalizer)
		if err := r.Update(context.Background(), threefsChanintable); err != nil {
			klog.Errorf("add threefsChanintable %s finalizers failed, err: %+v", threefsChanintable.Name, err)
			return ctrl.Result{}, err
		}
	}

	if threefsChanintable.DeletionTimestamp != nil {
		if utils.StrListContains(threefsChanintable.GetFinalizers(), constant.ThreeFSFinalizer) && !controllerutil.RemoveFinalizer(threefsChanintable, constant.ThreeFSFinalizer) {
			return ctrl.Result{}, fmt.Errorf("remove finalizer %s failed", constant.ThreeFSFinalizer)
		}
		if err := r.Update(context.Background(), threefsChanintable); err != nil {
			klog.Errorf("remove threefsCluster %s finalizers failed, err: %+v", threefsChanintable.Name, err)
			return ctrl.Result{}, err
		}
		klog.Infof("delete threefsCluster %s related resources success", threefsChanintable.Name)
	} else {
		if threefsChanintable.Status.Phase == constant.ThreeFSChainTableFinishedStatus {
			klog.Infof("threefsChanintable job %s has finished, skip", req.NamespacedName)
			return ctrl.Result{}, nil
		}

		tokenConfig := corev1.ConfigMap{}
		if err := r.Client.Get(context.Background(), client.ObjectKey{Name: constant.DefaultTokenConfigName, Namespace: threefsChanintable.Spec.ThreeFsClusterNamespace}, &tokenConfig); err != nil {
			return ctrl.Result{}, err
		}

		if threefsChanintable.Spec.Type == constant.ThreeFSChainTableTypeReplace {
			if !threefsChanintable.Status.Executed {
				oldNodeObj := &corev1.Node{}
				if err := r.Client.Get(context.Background(), client.ObjectKey{Name: threefsChanintable.Spec.OldNode[0]}, oldNodeObj); err != nil {
					klog.Errorf("get node %s err: %+v", threefsChanintable.Spec.OldNode[0], err)
					// no return
				} else {
					if _, ok := oldNodeObj.Labels[constant.ThreeFSStorageFaultNodeKey]; !ok {
						oldNodeObj.Labels[constant.ThreeFSStorageFaultNodeKey] = "true"
						if err = r.Client.Update(context.Background(), oldNodeObj); err != nil {
							klog.Errorf("update node %s with storage fault label err: %+v", threefsChanintable.Spec.OldNode[0], err)
							return ctrl.Result{}, err
						}
					}
				}

				// get chains with node name
				chains, err := GetChainTablesWithNode(adminCliConfig, threefsChanintable.Spec.OldNode[0])
				if err != nil {
					klog.Errorf("get chain table failed: %v", err)
					return ctrl.Result{}, err
				}
				if len(chains) == 0 && threefsChanintable.Status.ProcessChainIds != nil {
					klog.Infof("chain table is empty, get chain table with status chain id list")
					chains, err = r.GetChainTablesWithChainIdTargetId(adminCliConfig, threefsChanintable.Status.ProcessChainIds)
					if err != nil {
						klog.Errorf("get chain table with chain id failed: %v", err)
						return ctrl.Result{}, err
					}
				}

				// make sure processing chain updated success
				var chainids []string
				if threefsChanintable.Status.ProcessChainIds == nil {
					chainids, err = r.HandleProcessChains(adminCliConfig, chains, threefsChanintable.Spec.OldNode[0])
					if err != nil {
						klog.Errorf("handle process chains failed: %v", err)
						return ctrl.Result{}, err
					}
					if err := r.UpdateProcessChains(chainids, threefsChanintable); err != nil {
						klog.Errorf("update process chains failed: %v", err)
						return ctrl.Result{}, err
					}
					klog.Infof("update process chains success")
				}

				if chainids == nil {
					chainids = threefsChanintable.Status.ProcessChainIds
				}

				// check chain table related to old node
				if threefsChanintable.Spec.Force {
					klog.Infof("force tag detected, offline target related to old node first")
					if err := r.OfflineTargetRelatedNode(adminCliConfig, chains, threefsChanintable.Spec.OldNode[0], tokenConfig.Data["token"]); err != nil {
						klog.Errorf("offline target related to old node failed, err: %+v", err)
						return ctrl.Result{}, err
					}
				}

				err = r.CheckChainWithoutStatusRelatedNode(adminCliConfig, chains, "SERVING-UPTODATE", threefsChanintable.Spec.OldNode[0])
				if err != nil {
					klog.Errorf("check chain table related to old node failed")
					return ctrl.Result{}, fmt.Errorf("check chain table related to old node failed")
				}
				klog.Infof("check chain table related to old node success, all chain table related to old node is not SERVING-UPTODATE")

				// delete target related to old node
				if err = r.DeleteTargetRelatedNode(adminCliConfig, chains, threefsChanintable.Spec.OldNode[0], tokenConfig.Data["token"]); err != nil {
					klog.Errorf("delete target related to old node failed")
					return ctrl.Result{}, err
				}
				klog.Infof("delete target related to old node %s success", threefsChanintable.Spec.OldNode)

				// create target related to new node
				tmpFilePath, err := r.CreateTargetTmpFile(adminCliConfig, chainids, threefsChanintable.Spec.OldNode[0], threefsChanintable.Spec.NewNode[0])
				if err != nil {
					klog.Errorf("create target related to new node failed")
					return ctrl.Result{}, err
				}
				klog.Infof("create target batch file related to new node %s success, tmp file path: %s", threefsChanintable.Spec.NewNode, tmpFilePath)

				if err = adminCliConfig.CreateTarget(tokenConfig.Data["token"], tmpFilePath); err != nil {
					klog.Errorf("create target related to new node failed")
					return ctrl.Result{}, err
				}
				klog.Infof("create target related to new node %s success", threefsChanintable.Spec.NewNode)

				// add target related to new node
				if err = r.AddTargetRelatedNode(adminCliConfig, chainids, threefsChanintable.Spec.OldNode[0], threefsChanintable.Spec.NewNode[0], tokenConfig.Data["token"]); err != nil {
					klog.Errorf("add target related to new node failed")
					return ctrl.Result{}, err
				}
				klog.Infof("add target related to new node %s success", threefsChanintable.Spec.NewNode)

				// tag crd
				if err := r.UpdateExecTag(true, threefsChanintable); err != nil {
					klog.Errorf("update threefsChanintable %s executed failed, err: %+v", threefsChanintable.Name, err)
					return ctrl.Result{}, err
				}
				klog.Infof("threefsChanintable job %s executed yet", threefsChanintable.GetName())
			}

			chains, err := r.GetChainTablesWithChainIdTargetId(adminCliConfig, threefsChanintable.Status.ProcessChainIds)
			if err != nil {
				klog.Errorf("get chain table with chain id failed: %v", err)
			}
			num := r.CheckChainTargetWithNodeStatus(adminCliConfig, chains, threefsChanintable.Spec.NewNode[0], "SERVING-UPTODATE")
			threefsChanintable.Status.Process = fmt.Sprintf("%d/%d", num, len(chains))
			if num == len(chains) {
				threefsChanintable.Status.Phase = constant.ThreeFSChainTableProcessedStatus

				// delete old storage node tag after processed
				if err := r.Get(context.Background(), client.ObjectKey{Name: threefsChanintable.Spec.ThreeFsClusterName, Namespace: threefsChanintable.Spec.ThreeFsClusterNamespace}, &vfsc); err != nil {
					klog.Errorf("get ThreeFsCluster %s err: %+v", threefsChanintable.Spec.ThreeFsClusterName, err)
					return ctrl.Result{}, err
				}

				if utils.StrListContains(vfsc.Status.NodesInfo.StorageNodes, threefsChanintable.Spec.OldNode[0]) {
					vfsc.Status.NodesInfo.StorageNodes = utils.StrListRemove(vfsc.Status.NodesInfo.StorageNodes, threefsChanintable.Spec.OldNode[0])
					if err := r.Status().Update(context.Background(), &vfsc); err != nil {
						klog.Errorf("update old node in ThreeFsCluster %s err: %+v", threefsChanintable.Spec.ThreeFsClusterName, err)
						return ctrl.Result{}, err
					}
					klog.Infof("remove old node in ThreeFsCluster %s success", threefsChanintable.Spec.ThreeFsClusterName)
				} else {
					// unregister old node
					if CheckComponentExist(adminCliConfig, "STORAGE", threefsChanintable.Spec.OldNode[0]) {
						if !CheckComponentStatus(adminCliConfig, "STORAGE", threefsChanintable.Spec.OldNode[0], true, r.Client) {
							klog.Infof("threefsChanintable job %s oldNode %s is offline now", req.NamespacedName, threefsChanintable.Spec.OldNode[0])
							nodeid := ParseNodeIdWihtPlainName(adminCliConfig, threefsChanintable.Spec.OldNode[0], "STORAGE")
							if nodeid == "" {
								klog.Errorf("parse node id failed, err: %+v", err)
								return ctrl.Result{}, err
							}
							if err := adminCliConfig.UnregisterNode(nodeid, "STORAGE"); err != nil {
								klog.Errorf("unregister old node %s failed, err: %+v", threefsChanintable.Spec.OldNode[0], err)
								return ctrl.Result{}, err
							}
						} else {
							klog.Infof("threefsChanintable job %s oldNode %s is online now, wait", req.NamespacedName, threefsChanintable.Spec.OldNode[0])
							return ctrl.Result{}, nil
						}
					} else {
						threefsChanintable.Status.Phase = constant.ThreeFSChainTableFinishedStatus
					}
				}
			}
			klog.Infof("threefsChaintable job %s process is %s", threefsChanintable.GetName(), threefsChanintable.Status.Process)
			if err := r.Client.Status().Update(context.Background(), threefsChanintable); err != nil {
				klog.Errorf("update threefsChanintable %s status failed, err: %+v", threefsChanintable.Name, err)
				return ctrl.Result{}, err
			}
		} else if threefsChanintable.Spec.Type == constant.ThreeFSChainTableTypeCreate {
			if !threefsChanintable.Status.Executed {
				startIdx, err := ParseStartNodeId(r.Client, threefsChanintable.Spec.NewNode, vfsc)
				if err != nil {
					klog.Errorf("parse start node id failed, err: %+v", err)
					return ctrl.Result{}, err
				}
				if err := clientcomm.CreateDataPlacementRule(threefsChanintable.Spec.NewNode, &vfsc, startIdx); err != nil {
					r.Recorder.Event(threefsChanintable, "Warning", "CreateDataPlacementRuleFailed", err.Error())
					return ctrl.Result{}, err
				}

				// modify generated chain table chainid with exsiting chain
				maps, err := ParseMaxChainIdForEachDisk(adminCliConfig)
				if err != nil {
					return ctrl.Result{}, err
				}
				newTargetPath, newChainPath, newChainTablePath, err := UpdateChainIdWithExistingChain("/output/create_target_cmd.txt", "/output/generated_chains.csv", "/output/generated_chain_table.csv", maps)
				if err != nil {
					return ctrl.Result{}, err
				}

				if threefsChanintable.Status.ProcessChainIds == nil {
					chainsIdList, err := ParseChainTableFromFile(newChainPath)
					if err != nil || chainsIdList == nil || len(chainsIdList) == 0 {
						klog.Errorf("parse chain table from file failed, err: %+v", err)
						return ctrl.Result{}, err
					}
					if err := r.UpdateProcessChains(chainsIdList, threefsChanintable); err != nil {
						klog.Errorf("update process chains failed: %v", err)
						return ctrl.Result{}, err
					}
					klog.Infof("update process chains success")
				}

				// TODO modify generated chain list to avoid conflict with existing chain
				// chainid rule: chain_id = (chain_id_prefix * 1_000 + (disk_index+1)) * 1_00_000 + chain_index
				if err := adminCliConfig.CreateTarget(tokenConfig.Data["token"], newTargetPath); err != nil {
					return ctrl.Result{}, err
				}
				klog.Infof("create target related to new node %+v success", threefsChanintable.Spec.NewNode)

				if err := adminCliConfig.DumpChains(tokenConfig.Data["token"], "output/dump_chains.csv"); err != nil {
					return ctrl.Result{}, err
				}

				if err := utils.MergeCSVFiles(newChainPath, fmt.Sprintf("output/dump_chains.csv.%d", vfsc.Spec.Storage.Replica), "output/new_chains.csv"); err != nil {
					return ctrl.Result{}, err
				}
				if err := adminCliConfig.UploadChains(tokenConfig.Data["token"], "output/new_chains.csv"); err != nil {
					return ctrl.Result{}, err
				}
				klog.Infof("upload chains related to new node %+v success", threefsChanintable.Spec.NewNode)

				if err := adminCliConfig.DumpChainTable(tokenConfig.Data["token"], "output/dump_chain_table.csv"); err != nil {
					return ctrl.Result{}, err
				}
				if err := utils.MergeCSVFiles(newChainTablePath, "output/dump_chain_table.csv", "output/new_chaintables.csv"); err != nil {
					return ctrl.Result{}, err
				}
				if err := adminCliConfig.UploadChainTable(tokenConfig.Data["token"], "output/new_chaintables.csv"); err != nil {
					return ctrl.Result{}, err
				}
				klog.Infof("upload chain table related to new node %+v success", threefsChanintable.Spec.NewNode)

				// tag crd
				if err := r.UpdateExecTag(true, threefsChanintable); err != nil {
					klog.Errorf("update threefsChanintable %s executed failed, err: %+v", threefsChanintable.Name, err)
					return ctrl.Result{}, err
				}
				klog.Infof("threefsChanintable job %s executed yet", threefsChanintable.GetName())
			}

			// check status
			chains, err := r.GetChainTablesWithChainId(adminCliConfig, threefsChanintable.Status.ProcessChainIds)
			if err != nil {
				klog.Errorf("get chain table with chain id failed: %v", err)
			}
			num := r.CheckChainWithStatus(chains, "SERVING-UPTODATE")
			threefsChanintable.Status.Process = fmt.Sprintf("%d/%d", num, len(chains))
			if num == len(chains) {
				// set to finished directly
				threefsChanintable.Status.Phase = constant.ThreeFSChainTableFinishedStatus
			}
			klog.Infof("threefsChaintable job %s process is %s", threefsChanintable.GetName(), threefsChanintable.Status.Process)
			if err := r.Client.Status().Update(context.Background(), threefsChanintable); err != nil {
				klog.Errorf("update threefsChanintable %s status failed, err: %+v", threefsChanintable.Name, err)
				return ctrl.Result{}, err
			}
		}

	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ThreeFsChainTableReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&threefsv1.ThreeFsChainTable{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 5}).
		Complete(r)
}
