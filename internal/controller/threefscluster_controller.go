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
	"github.com/aliyun/kvc-3fs-operator/internal/clickhouse"
	clientcomm "github.com/aliyun/kvc-3fs-operator/internal/client"
	"github.com/aliyun/kvc-3fs-operator/internal/constant"
	"github.com/aliyun/kvc-3fs-operator/internal/fdb"
	"github.com/aliyun/kvc-3fs-operator/internal/meta"
	"github.com/aliyun/kvc-3fs-operator/internal/mgmtd"
	"github.com/aliyun/kvc-3fs-operator/internal/monitor"
	"github.com/aliyun/kvc-3fs-operator/internal/storage"
	"github.com/aliyun/kvc-3fs-operator/internal/utils"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"strings"
	"time"

	k8serror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	threefsv1 "github.com/aliyun/kvc-3fs-operator/api/v1"
)

// ThreeFsClusterReconciler reconciles a ThreeFsCluster object
type ThreeFsClusterReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Recorder   record.EventRecorder
	RESTClient rest.Interface
	RESTConfig *rest.Config
	Cache      cache.Cache
}

// +kubebuilder:rbac:groups=threefs.aliyun.com,resources=threefsclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=threefs.aliyun.com,resources=threefsclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=threefs.aliyun.com,resources=threefsclusters/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ThreeFsCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/reconcile
func (r *ThreeFsClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)

	klog.Infof("Reconciling ThreeFsCluster %s", req.NamespacedName)
	threeFsCluster := &threefsv1.ThreeFsCluster{}
	if err := r.Client.Get(ctx, req.NamespacedName, threeFsCluster); err != nil {
		if k8serror.IsNotFound(err) {
			klog.Infof("ThreeFsCluster %s has been deleted", req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if threeFsCluster.Status.Phase != constant.ThreeFSClusterDestroyStatus && !utils.StrListContains(threeFsCluster.GetFinalizers(), constant.ThreeFSFinalizer) {
		controllerutil.AddFinalizer(threeFsCluster, constant.ThreeFSFinalizer)
		if err := r.Update(context.Background(), threeFsCluster); err != nil {
			klog.Errorf("add ThreeFsCluster %s finalizers failed, err: %+v", threeFsCluster.Name, err)
			return ctrl.Result{}, err
		}
	}

	if threeFsCluster.Status.Phase == "" {
		if err := r.updateStatus(threeFsCluster, constant.ThreeFSClusterInitStatus); err != nil {
			klog.Errorf("update ThreeFsCluster %s status to %s failed, err: %+v", threeFsCluster.Name, constant.ThreeFSClusterInitStatus, err)
			return ctrl.Result{}, err
		}
	}

	// create related config
	chCongig := clickhouse.NewClickhouseConfig(threeFsCluster.Name, threeFsCluster.Namespace,
		threeFsCluster.Spec.Clickhouse.Nodes, constant.DefaultClickHouseConfigPath, threeFsCluster.Spec.Clickhouse.User,
		threeFsCluster.Spec.Clickhouse.HostName,
		threeFsCluster.Spec.Clickhouse.Password, threeFsCluster.Spec.Clickhouse.TCPPort,
		threeFsCluster.Spec.Clickhouse.Resources, r.Client)
	monConfig := monitor.NewMonitorConfig(threeFsCluster.Name, threeFsCluster.Namespace,
		threeFsCluster.Spec.Clickhouse.Nodes, threeFsCluster.Spec.Clickhouse.UseEcsClickhouse,
		threeFsCluster.Spec.Monitor.Port, threeFsCluster.Spec.Monitor.Resources, r.Client, chCongig)

	fdbConfig := fdb.NewFdbConfig(threeFsCluster.Name, threeFsCluster.Namespace,
		threeFsCluster.Spec.Fdb.StorageReplicas, threeFsCluster.Spec.Fdb.ClusterSize,
		threeFsCluster.Status.NodesInfo.FdbNodes, threeFsCluster.Spec.Fdb.Port, threeFsCluster.Spec.Fdb.Resources, r.Client,
		r.RESTClient, r.RESTConfig, r.Scheme)

	storageConfig := storage.NewStorageConfig(threeFsCluster.Name, threeFsCluster.Namespace,
		threeFsCluster.Status.NodesInfo.StorageNodes, "", threeFsCluster.Spec.Storage.RdmaPort,
		threeFsCluster.Spec.Storage.TcpPort,
		threeFsCluster.Spec.Storage.TargetPaths, threeFsCluster.Spec.Storage.Resources, r.Client)

	if threeFsCluster.DeletionTimestamp == nil {
		// check fdb node label and change fdb nodes
		if err := fdbConfig.TagNodeLabel(threeFsCluster); err != nil {
			if strings.Contains(err.Error(), "fdb node is not enough") {
				r.Recorder.Event(threeFsCluster, "Warning", "TagNodeLabelFailed", "tag fdb node number is not enough")
			}
			return ctrl.Result{}, err
		}

		// check storage node label and change storage nodes
		if err := storageConfig.TagNodeLabel(threeFsCluster); err != nil {
			if strings.Contains(err.Error(), "storage node is not enough") {
				r.Recorder.Event(threeFsCluster, "Warning", "TagNodeLabelFailed", "tag storage node number is not enough")
			}
			return ctrl.Result{}, err
		}

		if len(threeFsCluster.Status.NodesInfo.StorageNodes) < threeFsCluster.Spec.Mgmtd.Replica || len(threeFsCluster.Status.NodesInfo.StorageNodes) < threeFsCluster.Spec.Meta.Replica {
			return ctrl.Result{}, fmt.Errorf("storage node is not enough for mgmtd/meta replica")
		}
	}

	mgmtdConfig := mgmtd.NewMgmtdConfig(threeFsCluster.Name, threeFsCluster.Namespace,
		threeFsCluster.Status.NodesInfo.StorageNodes, threeFsCluster.Spec.Mgmtd.RdmaPort, threeFsCluster.Spec.Mgmtd.TcpPort,
		threeFsCluster.Spec.Mgmtd.Replica, threeFsCluster.Spec.Fdb.Resources, fdbConfig, r.Client)
	// client related config
	fdbcliConfig := clientcomm.NewFdbCliConfig(constant.DefaultThreeFSFdbConfigPath,
		threeFsCluster.Spec.Fdb.StorageReplicas, threeFsCluster.Spec.Fdb.CoordinatorNum, r.RESTClient)

	var mgmtdAddresses string
	if threeFsCluster.DeletionTimestamp == nil {
		// tag mgmtd first for mgmtd server address
		var mgmtdNodes []string
		if !threeFsCluster.Status.TagMgmtd {
			mgmtdNodes = mgmtdConfig.Nodes[:mgmtdConfig.Replica]
			useHostNetwork := utils.GetUseHostNetworkEnv()
			if useHostNetwork {
				mgmtdAddresses = r.ParseMgmtdAddresses(threeFsCluster.Name, threeFsCluster.Namespace, mgmtdNodes)
			} else {
				mgmtdAddresses = ParseMgmtdAddressesFromHeadlessSvc(threeFsCluster.Name, threeFsCluster.Namespace, mgmtdConfig.RdmaPort)
			}

			klog.Infof("mgmtdAddresses is: %v", mgmtdAddresses)
			newObj := threeFsCluster.DeepCopy()
			newObj.Status.TagMgmtd = true
			newObj.Status.MgmtdAddresses = mgmtdAddresses
			if err := r.Client.Status().Patch(context.Background(), newObj, client.MergeFrom(threeFsCluster)); err != nil {
				klog.Errorf("update ThreeFsCluster %s TagMgmtd status failed, err: %+v", threeFsCluster.Name, err)
				return ctrl.Result{}, err
			}
			klog.Infof("record tag mgmtd node in vfsc status success")
			if err := mgmtdConfig.TagNodeLabel(mgmtdNodes); err != nil {
				return ctrl.Result{}, err
			}
			klog.Infof("tag mgmtd node success")
		} else {
			mgmtdAddresses = threeFsCluster.Status.MgmtdAddresses
		}
		klog.Infof("mgmtdAddresses is: %v", mgmtdAddresses)
	}

	metaConfig := meta.NewMetaConfig(threeFsCluster.Name, threeFsCluster.Namespace,
		threeFsCluster.Status.NodesInfo.StorageNodes, mgmtdAddresses,
		threeFsCluster.Spec.Meta.RdmaPort, threeFsCluster.Spec.Meta.TcpPort, threeFsCluster.Spec.Meta.Replica,
		threeFsCluster.Spec.Meta.Resources, fdbConfig, r.Client)
	storageConfig.MgmtdAddresses = mgmtdAddresses
	adminCliConfig := clientcomm.NewAdminCli(mgmtdAddresses,
		filepath.Join(constant.DefaultConfigPath, constant.ThreeFSAdminCliMain))

	if threeFsCluster.DeletionTimestamp != nil {
		klog.Infof("ThreeFsCluster %s is being deleted", threeFsCluster.Name)
		if err := r.updateStatus(threeFsCluster, constant.ThreeFSClusterDestroyStatus); err != nil {
			klog.Errorf("update ThreeFsCluster %s status to %s failed, err: %+v", threeFsCluster.Name, constant.ThreeFSClusterDestroyStatus, err)
		}

		if err := storageConfig.DeleteDeployIfExist(); err != nil {
			return ctrl.Result{}, err
		}
		if err := storageConfig.DeleteStorageConfigIfExist(); err != nil {
			return ctrl.Result{}, err
		}
		klog.Infof("delete storage component success")

		if err := metaConfig.DeleteDeployIfExist(); err != nil {
			return ctrl.Result{}, err
		}
		if err := metaConfig.DeleteMetaConfigIfExist(); err != nil {
			return ctrl.Result{}, err
		}
		klog.Infof("delete meta component success")

		if err := mgmtdConfig.DeleteServiceIfExist(); err != nil {
			return ctrl.Result{}, err
		}
		if err := mgmtdConfig.DeleteDeployIfExist(); err != nil {
			return ctrl.Result{}, err
		}
		if err := mgmtdConfig.DeleteMgmtdConfigIfExist(); err != nil {
			return ctrl.Result{}, err
		}
		klog.Infof("delete mgmtd component success")

		if err := fdbConfig.DeleteDeployIfExist(); err != nil {
			return ctrl.Result{}, err
		}
		if err := fdbConfig.DeleteFdbConfigIfExist(); err != nil {
			return ctrl.Result{}, err
		}
		klog.Infof("delete fdb component success")

		if err := monConfig.DeleteServiceIfExist(); err != nil {
			return ctrl.Result{}, err
		}
		if err := monConfig.DeleteDeployIfExist(); err != nil {
			return ctrl.Result{}, err
		}
		klog.Infof("delete monitor component success")

		if err := chCongig.DeleteServiceIfExist(); err != nil {
			return ctrl.Result{}, err
		}
		if err := chCongig.DeleteDeployIfExist(); err != nil {
			return ctrl.Result{}, err
		}
		klog.Infof("delete clickhouse component success")

		if err := r.deleteTokenConfig(threeFsCluster); err != nil {
			return ctrl.Result{}, err
		}
		klog.Infof("delete token config success")

		if err := r.unTagNode(threeFsCluster); err != nil {
			return ctrl.Result{}, err
		}
		klog.Infof("untag node success")

		if utils.StrListContains(threeFsCluster.GetFinalizers(), constant.ThreeFSFinalizer) && !controllerutil.RemoveFinalizer(threeFsCluster, constant.ThreeFSFinalizer) {
			return ctrl.Result{}, fmt.Errorf("remove finalizer %s failed", constant.ThreeFSFinalizer)
		}
		if err := r.Update(context.Background(), threeFsCluster); err != nil {
			klog.Errorf("remove threeFsCluster %s finalizers failed, err: %+v", threeFsCluster.Name, err)
			return ctrl.Result{}, err
		}
		klog.Infof("delete threeFsCluster %s related resources success", threeFsCluster.Name)
	} else {
		// record latest image version
		if err := r.RecordImageversion(threeFsCluster); err != nil {
			return ctrl.Result{}, err
		}

		if !threeFsCluster.Spec.Clickhouse.UseEcsClickhouse {
			// check clickhouse deploy & service
			if err := chCongig.CreateDeployIfNotExist(); err != nil {
				return ctrl.Result{}, err
			}
			if err := chCongig.CreateServiceIfNotExist(); err != nil {
				return ctrl.Result{}, err
			}
			klog.Infof("clickhouse deploy & service created")
		}

		var ip string
		if threeFsCluster.Spec.Clickhouse.UseEcsClickhouse {
			ip = threeFsCluster.Spec.Clickhouse.HostName
		} else {
			var err error
			ip, err = chCongig.ParseServiceIp()
			if err != nil {
				klog.Errorf("get clickhouse service ip failed: %v", err)
				return ctrl.Result{}, err
			}
		}

		if !clientcomm.CheckClickhouseReady(threeFsCluster.Spec.Clickhouse.TCPPort, ip, chCongig.ClickhouseUser, chCongig.ClickhousePassword) {
			klog.Infof("clickhouse not ready, requeue after 10s")
			// healthy before
			if threeFsCluster.Status.ConfigStatus["clickhouse"] == constant.ThreeComponentReadyStatus {
				r.Recorder.Event(threeFsCluster, "Warning", "ClickhouseNotReady", "clickhouse not ready")
			}
			return ctrl.Result{RequeueAfter: time.Second * 10}, nil
		}
		if err := r.updateConfigtStatus(threeFsCluster, "clickhouse", constant.ThreeComponentReadyStatus); err != nil {
			klog.Errorf("update ThreeFsCluster %s status failed, err: %+v", threeFsCluster.Name, err)
		}
		klog.Infof("clickhouse is ready")

		if err := clientcomm.ExecuteSql(threeFsCluster.Spec.Clickhouse.TCPPort, ip, chCongig.ClickhouseUser, chCongig.ClickhousePassword, constant.DefaultClickhouseSqlPath); err != nil {
			klog.Errorf("execute clickhouse sql failed, requeue after 10s")
			return ctrl.Result{RequeueAfter: time.Second * 10}, nil
		}
		klog.Infof("create clickhouse db success")

		if err := monConfig.CreateDeployIfNotExist(); err != nil {
			return ctrl.Result{}, err
		}
		if err := monConfig.CreateServiceIfNotExist(); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.updateConfigtStatus(threeFsCluster, "monitor", constant.ThreeComponentReadyStatus); err != nil {
			klog.Errorf("update ThreeFsCluster %s status failed, err: %+v", threeFsCluster.Name, err)
		}
		klog.Infof("monitor deploy & service created")

		// check fdb configmap & deploy
		content, _ := fdbcliConfig.GetRemoteConfigContent()
		if err := fdbConfig.CreateFdbConfigIfNotExist(content); err != nil {
			return ctrl.Result{}, err
		}

		if err := fdbConfig.CreateDeployIfNotExist(); err != nil {
			return ctrl.Result{}, err
		}

		klog.Infof("fdb configmap & deploy created")

		if err := r.RenderMainConfigPhase1(monConfig, fdbConfig, mgmtdConfig); err != nil {
			return ctrl.Result{}, err
		}

		// check fdb cluster status
		if threeFsCluster.Status.ConfigStatus["fdb"] == constant.ThreeComponentReadyStatus {
			// for fault tolerance, no return
			if _, _, err := fdbcliConfig.ConfigureCoordinator(); err != nil {
				klog.Errorf("auto coordinator failed, err: %+v", err)
			}
		} else {
			if err := fdbcliConfig.InitFdbCluster(); err != nil {
				klog.Errorf("init/check fdb cluster failed, err: %+v", err)
				return ctrl.Result{}, err
			} else {
				if err := r.updateConfigtStatus(threeFsCluster, "fdb", constant.ThreeComponentReadyStatus); err != nil {
					klog.Errorf("update ThreeFsCluster %s status failed, err: %+v", threeFsCluster.Name, err)
					return ctrl.Result{}, err
				}
			}
		}

		details, err := fdbcliConfig.ParseStatusOutput()
		if err != nil {
			klog.Errorf("get fdb cluster details failed, err: %+v", err)
			return ctrl.Result{}, err
		}

		if details.Cluster.Data.State.Name != "healthy" {
			klog.Infof("fdb cluster not fully replicated healthy(%s), requeue after 10s", details.Cluster.Data.State.Description)
			return ctrl.Result{RequeueAfter: time.Second * 10}, nil
		}

		// update fdb crd status
		if err := r.UpdateClusterFdbStatus(threeFsCluster, details, r.Client); err != nil {
			klog.Errorf("update ThreeFsCluster %s status failed, err: %+v", threeFsCluster.Name, err)
			return ctrl.Result{}, err
		}
		_ = r.Get(context.Background(), client.ObjectKey{Name: threeFsCluster.Name, Namespace: threeFsCluster.Namespace}, threeFsCluster)
		klog.Infof("update fdb cluster status success")

		// update meta/mgmtd/storage crd status
		if strings.Contains(mgmtdAddresses, "RDMA") {
			if err := r.UpdateClusterStatus(adminCliConfig, threeFsCluster, r.Client); err != nil {
				klog.Errorf("update ThreeFsCluster %s status failed, err: %+v", threeFsCluster.Name, err)
			}
		}
		_ = r.Get(context.Background(), client.ObjectKey{Name: threeFsCluster.Name, Namespace: threeFsCluster.Namespace}, threeFsCluster)

		// check mgmtd configmap & deploy
		if threeFsCluster.Status.ConfigStatus["mgmtd"] != constant.ThreeComponentReadyStatus {
			if err := adminCliConfig.InitCluster(threeFsCluster.Spec.ChainTableId, threeFsCluster.Spec.StripeSize, threeFsCluster.Spec.ChunkSize); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.updateConfigtStatus(threeFsCluster, "mgmtd", constant.ThreeComponentReadyStatus); err != nil {
				klog.Errorf("update ThreeFsCluster %s status failed, err: %+v", threeFsCluster.Name, err)
			}
			klog.Infof("threefs cluster init success")
		}

		if err := mgmtdConfig.CreateMgmtdEnvConfigIfNotExist(); err != nil {
			return ctrl.Result{}, err
		}
		if !utils.GetUseHostNetworkEnv() {
			if err := mgmtdConfig.CreateServiceIfNotExist(); err != nil {
				return ctrl.Result{}, err
			}
		}
		if err := mgmtdConfig.CreateDeployIfNotExist(); err != nil {
			return ctrl.Result{}, err
		}

		if !utils.GetUseHostNetworkEnv() && !strings.Contains(mgmtdAddresses, "RDMA") {
			// flush mgmtd address
			mgmtdAddresses, err = r.ParseMgmtdAddressesInPodNet(threeFsCluster.Name, threeFsCluster.Namespace)
			if err != nil {
				if strings.Contains(err.Error(), "pod list length") || strings.Contains(err.Error(), "mgmtd pod ip is empty yet") {
					klog.Infof("mgmtd pod is not ready yet, requeue after 10s")
					return ctrl.Result{RequeueAfter: time.Second * 10}, nil
				}
				return ctrl.Result{}, err
			}
			newObj := threeFsCluster.DeepCopy()
			newObj.Status.MgmtdAddresses = mgmtdAddresses
			if err := r.Client.Status().Patch(context.Background(), newObj, client.MergeFrom(threeFsCluster)); err != nil {
				klog.Errorf("update ThreeFsCluster %s flushed mgmtdAddresses status failed, err: %+v", threeFsCluster.Name, err)
				return ctrl.Result{}, err
			}
			metaConfig.MgmtdAddresses = mgmtdAddresses
			storageConfig.MgmtdAddresses = mgmtdAddresses
			adminCliConfig.MgmtdServerAddresses = mgmtdAddresses
			klog.Infof("flushed mgmtd address: %s", mgmtdAddresses)
		}

		if !CheckComponentStatus(adminCliConfig, "MGMTD", "", false, r.Client) {
			klog.Infof("mgmtd not ready yet, requeue after 10s")
			return ctrl.Result{RequeueAfter: time.Second * 10}, nil
		}
		klog.Infof("mgmtd configmap & deploy created")

		if err := r.RenderMainConfigPhase2(monConfig, metaConfig, storageConfig, mgmtdAddresses); err != nil {
			return ctrl.Result{}, err
		}

		// check meta configmap & deploy
		if threeFsCluster.Status.ConfigStatus["meta"] != constant.ThreeComponentReadyStatus {
			klog.Infof("threeFsCluster %s meta not init, try to upload main config", threeFsCluster.Name)
			if err := adminCliConfig.UploadMainConfig("META", filepath.Join(constant.DefaultConfigPath, constant.ThreeFSMetaMain)); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.updateConfigtStatus(threeFsCluster, "meta", constant.ThreeComponentReadyStatus); err != nil {
				klog.Errorf("update ThreeFsCluster %s status failed, err: %+v", threeFsCluster.Name, err)
			}
			klog.Infof("threeFsCluster %s meta config uploaded", threeFsCluster.Name)
		}
		if err := metaConfig.TagNodeLabel(); err != nil {
			if strings.Contains(err.Error(), "tag meta node number is not enough") {
				r.Recorder.Event(threeFsCluster, "Warning", "TagNodeLabelFailed", err.Error())
			}
			return ctrl.Result{}, err
		}

		if !utils.GetUseHostNetworkEnv() {
			ips, err := utils.ResolveDNS(GetSvcDnsName(mgmtd.GetMgmtdDeployName(threeFsCluster.Name), threeFsCluster.Namespace))
			if err != nil || len(ips) == 0 {
				klog.Errorf("resolve mgmtd headless svc dns failed, wait after 10s")
				return ctrl.Result{RequeueAfter: time.Second * 10}, nil
			}
		}

		if err := metaConfig.CreateMetaEnvConfigIfNotExist(); err != nil {
			return ctrl.Result{}, err
		}
		if err := metaConfig.CreateDeployIfNotExist(); err != nil {
			return ctrl.Result{}, err
		}
		if !CheckComponentStatus(adminCliConfig, "META", "", false, r.Client) {
			klog.Infof("meta not ready yet, requeue after 10s")
			return ctrl.Result{RequeueAfter: time.Second * 10}, nil
		}
		klog.Infof("meta configmap & deploy created")

		// check storage configmap & deploy
		if threeFsCluster.Status.ConfigStatus["storage"] != constant.ThreeComponentReadyStatus {
			klog.Infof("threeFsCluster %s storage not init, try to upload main config", threeFsCluster.Name)
			if err := adminCliConfig.UploadMainConfig("STORAGE", filepath.Join(constant.DefaultConfigPath, constant.ThreeFSStorageMain)); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.updateConfigtStatus(threeFsCluster, "storage", constant.ThreeComponentReadyStatus); err != nil {
				klog.Errorf("update ThreeFsCluster %s status failed, err: %+v", threeFsCluster.Name, err)
			}
			klog.Infof("threeFsCluster %s storage config uploaded", threeFsCluster.Name)
		}

		if err := storageConfig.CreateStorageEnvConfigIfNotExist(); err != nil {
			return ctrl.Result{}, err
		}
		if err := storageConfig.CreateDeployIfNotExist(); err != nil {
			klog.Errorf("create storage deploy failed, err: %+v", err)
			return ctrl.Result{}, err
		}

		for _, storageNode := range threeFsCluster.Spec.Storage.Nodes {
			if !CheckComponentExist(adminCliConfig, "STORAGE", storageNode) {
				klog.Infof("storage %s not connected yet, requeue after 20s", storageNode)
				return ctrl.Result{RequeueAfter: time.Second * 20}, nil
			}
		}
		klog.Infof("all storage node connected to mgmtd")
		if !CheckComponentStatus(adminCliConfig, "STORAGE", "", true, r.Client) && threeFsCluster.Status.Phase == constant.ThreeFSClusterInitStatus {
			klog.Infof("storage not ready yet, requeue after 10s")
			return ctrl.Result{RequeueAfter: time.Second * 10}, nil
		}
		klog.Infof("storage configmap & deploy created")

		if threeFsCluster.Status.Phase == constant.ThreeFSClusterInitStatus {
			if err := r.updateStatus(threeFsCluster, constant.ThreeFSClusterDataPlacingStatus); err != nil {
				klog.Errorf("update ThreeFsCluster %s status to %s failed, err: %+v", threeFsCluster.Name, constant.ThreeFSClusterDataPlacingStatus, err)
				return ctrl.Result{}, err
			}
		}

		if threeFsCluster.Status.Phase == constant.ThreeFSClusterDataPlacingStatus {
			// create token configmap
			token, err := r.UserAdd(threeFsCluster, adminCliConfig)
			if err != nil {
				return ctrl.Result{}, err
			}

			if err := clientcomm.CreateDataPlacementRule(threeFsCluster.Status.NodesInfo.StorageNodes, threeFsCluster, constant.ThreeFSStorageStartNodeId); err != nil {
				r.Recorder.Event(threeFsCluster, "Warning", "CreateDataPlacementRuleFailed", err.Error())
				return ctrl.Result{}, err
			}

			if err := adminCliConfig.CreateTarget(token, "output/create_target_cmd.txt"); err != nil {
				return ctrl.Result{}, err
			}
			if err := adminCliConfig.UploadChains(token, "output/generated_chains.csv"); err != nil {
				return ctrl.Result{}, err
			}
			if err := adminCliConfig.UploadChainTable(token, "output/generated_chain_table.csv"); err != nil {
				return ctrl.Result{}, err
			}

			klog.Infof("threeFsCluster %s data placed", threeFsCluster.Name)
			if err := r.updateStatus(threeFsCluster, constant.ThreeFSClusterReadyStatus); err != nil {
				klog.Errorf("update ThreeFsCluster %s status to %s failed, err: %+v", threeFsCluster.Name, constant.ThreeFSClusterReadyStatus, err)
				return ctrl.Result{}, err
			}
		}

		if threeFsCluster.Status.Phase == constant.ThreeFSClusterReadyStatus {
			if err := UpdateTargetStatus(adminCliConfig, threeFsCluster, r.Client); err != nil {
				klog.Errorf("update ThreeFsCluster %s unhealthy target failed, err: %+v", threeFsCluster.Name, err)
				return ctrl.Result{}, err
			}
			klog.Infof("threeFsCluster %s is ready", threeFsCluster.Name)
		}

		if threeFsCluster.Status.ConfigStatus["fuse"] != constant.ThreeComponentReadyStatus {
			if err := adminCliConfig.UploadMainConfig("FUSE", filepath.Join(constant.DefaultConfigPath, constant.ThreeFSFuseMain)); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.updateConfigtStatus(threeFsCluster, "fuse", constant.ThreeComponentReadyStatus); err != nil {
				klog.Errorf("update ThreeFsCluster %s status failed, err: %+v", threeFsCluster.Name, err)
				return ctrl.Result{}, err
			}
			klog.Infof("threeFsCluster %s fuse config uploaded", threeFsCluster.Name)
		}

		// check images upgrade, only for threefs component now
		if threeFsCluster.Labels != nil && threeFsCluster.Labels[constant.ThreeFSRollingUpdateLabel] == "true" {
			klog.Infof("threeFsCluster %s rolling update enabled, check images upgrade", threeFsCluster.Name)
			if err := r.HandleImageUpgrade(adminCliConfig, threeFsCluster); err != nil {
				klog.Errorf("handle image upgrade failed, err: %+v", err)
				return ctrl.Result{}, err
			}
		}

		// check storage/target status for replace
		if threeFsCluster.Labels != nil && threeFsCluster.Labels[constant.ThreeFSAutoReplaceLabel] == "true" {
			klog.Infof("threeFsCluster %s auto replace enabled, check fault storage", threeFsCluster.Name)
			_ = r.Get(context.Background(), client.ObjectKey{Name: threeFsCluster.Name, Namespace: threeFsCluster.Namespace}, threeFsCluster)
			if err := r.HandleFaultStorage(threeFsCluster); err != nil {
				klog.Errorf("handle fault storage failed, err: %+v", err)
				return ctrl.Result{}, err
			}
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ThreeFsClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&threefsv1.ThreeFsCluster{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 5}).
		Complete(r)
}
