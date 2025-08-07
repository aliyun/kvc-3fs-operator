package constant

import "time"

const (
	// ResyncInterval is sync interval of the watcher
	ResyncInterval = 30 * time.Second
)

const (
	ThreeFSClusterEmptyStatus       = ""
	ThreeFSClusterInitStatus        = "Init"
	ThreeFSClusterDataPlacingStatus = "DataPlacing"
	ThreeFSClusterReadyStatus       = "Ready"
	ThreeFSClusterDestroyStatus     = "Destroy"

	ThreeComponentReadyStatus = "Processed"

	ThreeFSChainTableProcessingStatus = "Processing"
	ThreeFSChainTableProcessedStatus  = "Processed"
	ThreeFSChainTableFinishedStatus   = "Finished"
)

const (
	ThreeFSChainTableTypeCreate  = "NodeCreate"
	ThreeFSChainTableTypeDelete  = "NodeDelete"
	ThreeFSChainTableTypeReplace = "NodeReplace"
)

const (
	ThreeFSMgmtdStartNodeId   = 1
	ThreeFSMetaStartNodeId    = 101
	ThreeFSStorageStartNodeId = 10001

	ThreeFSTargetIDPrefix = 1
	ThreeFSChainIDPrefix  = 9
)

const (
	KubernetesHostnameKey = "kubernetes.io/hostname"

	ThreeDebugMode   = "threefs.aliyun.com/debug-mode"
	ThreeFSFinalizer = "threefs.aliyun.com/threefs-finalizer"

	ThreeFSClickhouseDeploymentKey = "threefs.aliyun.com/clickhouse-deploy"
	ThreeFSClickhouseSvcKey        = "threefs.aliyun.com/clickhouse-svc"

	ThreeFSMonitorConfigParseKey = "threefs.aliyun.com/monitor-parse"
	ThreeFSMonitorNodeKey        = "threefs.aliyun.com/monitor-node"
	ThreeFSMonitorDeploymentKey  = "threefs.aliyun.com/monitor-deploy"
	ThreeFSMonitorSvcKey         = "threefs.aliyun.com/monitor-svc"

	ThreeFSFdbNodeKey      = "threefs.aliyun.com/fdb-node"
	ThreeFSFdbFaultNodeKey = "threefs.aliyun.com/fdb-fault-node"
	ThreeFSFdbDaemonsetKey = "threefs.aliyun.com/fdb-daemonset"
	ThreeFSFdbDeployKey    = "threefs.aliyun.com/fdb-deploy"

	ThreeFSMgmtdPrimaryNodeKey = "threefs.aliyun.com/mgmtd-primary-node"
	ThreeFSMgmtdNodeKey        = "threefs.aliyun.com/mgmtd-node"
	ThreeFSMgmtdDaemonsetKey   = "threefs.aliyun.com/mgmtd-daemonset"
	ThreeFSMgmtdDeployKey      = "threefs.aliyun.com/mgmtd-deploy"

	ThreeFSMetaNodeKey      = "threefs.aliyun.com/meta-node"
	ThreeFSMetaDaemonsetKey = "threefs.aliyun.com/meta-daemonset"
	ThreeFSMetaDeployKey    = "threefs.aliyun.com/meta-deploy"

	ThreeFSStorageNodeKey      = "threefs.aliyun.com/storage-node"
	ThreeFSStorageDaemonsetKey = "threefs.aliyun.com/storage-daemonset"
	ThreeFSStorageDeployKey    = "threefs.aliyun.com/storage-deploy"
	ThreeFSStorageFaultNodeKey = "threefs.aliyun.com/storage-fault-node"

	ThreeFSSidecarLabel = "threefs.aliyun.com/sidecar"
	ThreeFSMountLabel   = "threefs.aliyun.com/mountpath"
	ThreeFSCrdLabel     = "threefs.aliyun.com/threefscluster"
	ThreeFSCrdNsLabel   = "threefs.aliyun.com/threefscluster-ns"
	ThreeFSPatched      = "threefs.aliyun.com/webhook-patched"
	ThreeFSShmSize      = "threefs.aliyun.com/shm-size"

	ThreeFSPodLabel       = "threefs.aliyun.com/threefs"
	ThreeFSComponentLabel = "threefs.aliyun.com/component"

	ThreeFSAutoReplaceLabel   = "threefs.aliyun.com/storage-auto-replace"
	ThreeFSRollingUpdateLabel = "threefs.aliyun.com/rolling-update"
)

const (
	ThreeFSAdminCliMain = "admin_cli.toml"
	ThreeFSMonitorMain  = "monitor_collector_main.toml"

	ThreeFSMetaMain     = "meta_main.toml"
	ThreeFSMetaTempMain = "meta_main_temp.toml"
	ThreeFSMetaLauncher = "meta_main_laucher.toml"
	ThreeFSMetaApp      = "meta_main_app.toml"

	ThreeFSMgmtdMain     = "mgmtd_main.toml"
	ThreeFSMgmtdTempMain = "mgmtd_main_temp.toml"
	ThreeFSMgmtdLauncher = "mgmtd_main_laucher.toml"
	ThreeFSMgmtdApp      = "mgmtd_main_app.toml"

	ThreeFSStorageMain     = "storage_main.toml"
	ThreeFSStorageTempMain = "storage_main_temp.toml"
	ThreeFSStorageLauncher = "storage_main_laucher.toml"
	ThreeFSStorageApp      = "storage_main_app.toml"

	ThreeFSFuseMain     = "hf3fs_fuse_main.toml"
	ThreeFSFuseTempMain = "hf3fs_fuse_main_temp.toml"
)

const (
	DefaultConfigPath = "/opt/3fs/etc"

	DefaultClickhouseDeployName  = "threefs-clickhouse-deploy"
	DefaultClickhouseServiceName = "threefs-clickhouse-svc"
	DefaultClickHouseConfigPath  = "/etc/clickhouse-server/config.xml"
	DefaultClickhouseSqlPath     = "/opt/3fs/etc/clickhouse.sql"
	DefaultThreeFSConfigMapName  = "threefs-config"

	DefaultThreeFSFdbConfigName = "threefs-fdb-config"
	DefaultThreeFSFdbConfigPath = "/opt/3fs/etc/fdb.cluster"

	DefaultTokenConfigName = "threefs-token-config"

	DefaultSidecarPrefix = "threefs-sidecar"

	DefaultThreeFSMutateWebhookName   = "threefs-mutating-webhook"
	DefaultThreeFSValidateWebhookName = "threefs-validation-webhook"

	ErdmaResourceKey = "aliyun/erdma"
)

const (
	ENVClickhouseTcpPort      = "TCP_PORT"
	ENVClickhouseConfigName   = "CLICKHOUSE_CONFIG"
	ENVClickhouseUserName     = "CLICKHOUSE_USER"
	ENVClickhousePasswordName = "CLICKHOUSE_PASSWORD"
	ENVClickhouseHosTName     = "CLICKHOUSE_HOST"

	ENVFdbPort               = "FDB_PORT"
	ENVFdbClusterFile        = "FDB_CLUSTER_FILE"
	ENVFdbClusterFileContent = "FDB_CLUSTER_FILE_CONTENTS"
	ENVCoordinatorPort       = "FDB_COORDINATOR_PORT"
	ENVProcessClass          = "FDB_PROCESS_CLASS"
	ENVFdbNetworkMode        = "FDB_NETWORKING_MODE"

	ENVUseHostnetwork = "USE_HOSTNETWORK"
	ENVFaultDuration  = "FAULT_DURATION"
	ENVEnableTrace    = "ENABLE_TRACE"
)

const (
	TimeLayout = "2006-01-02 15:04:05"
)
