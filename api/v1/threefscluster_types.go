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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

type FdbSpec struct {
	Nodes           []string                    `json:"nodes,omitempty"`
	ConfigureNew    bool                        `json:"configureNew,omitempty"`
	ClusterSize     int                         `json:"clusterSize,omitempty"`
	StorageReplicas int                         `json:"storageReplicas,omitempty"`
	CoordinatorNum  int                         `json:"coordinatorNum,omitempty"`
	Port            int                         `json:"port,omitempty"`
	Resources       corev1.ResourceRequirements `json:"resources,omitempty"`
}

type ClickhouseSpec struct {
	Nodes            []string                    `json:"nodes,omitempty"`
	Db               string                      `json:"db,omitempty"`
	User             string                      `json:"user"`
	HostName         string                      `json:"hostName,omitempty"`
	Password         string                      `json:"password"`
	TCPPort          int                         `json:"tcpPort"`
	UseEcsClickhouse bool                        `json:"useEcsClickhouse,omitempty"`
	Resources        corev1.ResourceRequirements `json:"resources,omitempty"`
}

type MonitorSpec struct {
	Port      int                         `json:"port"`
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

type MgmtdSpec struct {
	Nodes     []string                    `json:"nodes,omitempty"`
	Replica   int                         `json:"replica"`
	RdmaPort  int                         `json:"rdmaPort"`
	TcpPort   int                         `json:"tcpPort"`
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

type MetaSpec struct {
	Nodes     []string                    `json:"nodes,omitempty"`
	Replica   int                         `json:"replica"`
	RdmaPort  int                         `json:"rdmaPort"`
	TcpPort   int                         `json:"tcpPort"`
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

type StorageSpec struct {
	Nodes         []string                    `json:"nodes,omitempty"`
	BackupNodes   []string                    `json:"backupNodes,omitempty"`
	RdmaPort      int                         `json:"rdmaPort"`
	TcpPort       int                         `json:"tcpPort"`
	TargetPaths   []string                    `json:"targetPaths"`
	Replica       int                         `json:"replica"`
	TargetPerDisk int                         `json:"targetPerDisk"`
	Resources     corev1.ResourceRequirements `json:"resources,omitempty"`
}

// ThreeFsClusterSpec defines the desired state of ThreeFsCluster
type ThreeFsClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Foo is an example field of ThreeFsCluster. Edit threeFsCluster_types.go to remove/update
	StripeSize   int            `json:"stripeSize"`
	ChainTableId string         `json:"chainTableId"`
	ChunkSize    int            `json:"chunkSize"`
	Fdb          FdbSpec        `json:"fdb"`
	Clickhouse   ClickhouseSpec `json:"clickhouse"`
	Monitor      MonitorSpec    `json:"monitor"`
	Mgmtd        MgmtdSpec      `json:"mgmtd"`
	Meta         MetaSpec       `json:"meta"`
	Storage      StorageSpec    `json:"storage"`
}

type ClusterStatus struct {
	Name             string `json:"name"`
	Status           string `json:"status"`
	Msg              string `json:"msg,omitempty"`
	LastHeatBeatTime string `json:"lastHeatBeatTime,omitempty"`
}

type FdbClusterStatus struct {
	Name          string `json:"name"`
	IsCoordinator bool   `json:"isCoordinator"`
	IsReachable   bool   `json:"isReachable"`
	IsDeleted     bool   `json:"isDeleted,omitempty"`
	Status        string `json:"status"`
	Msg           string `json:"msg,omitempty"`
	FaultTime     string `json:"faultTime,omitempty"`
}

type TargetStatus struct {
	TargetId    string `json:"targetId"`
	Status      string `json:"status"`
	OfflineTime string `json:"offlineTime,omitempty"`
}

type UpgradeInfo struct {
	ImageVersion   map[string]string `json:"imageVersion,omitempty"`
	UpgradeProcess map[string]string `json:"upgradeProcess,omitempty"`
	Finished       bool              `json:"finished,omitempty"`
}

type NodesInfo struct {
	StorageNodes       []string `json:"storageNodes,omitempty"`
	StorageBackupNodes []string `json:"storageBackupNodes,omitempty"`
	FdbNodes           []string `json:"fdbNodes,omitempty"`
}

// ThreeFsClusterStatus defines the observed state of ThreeFsCluster
type ThreeFsClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	Phase                 string                              `json:"phase,omitempty"`
	TagMgmtd              bool                                `json:"tagMgmtd,omitempty"`
	MgmtdAddresses        string                              `json:"mgmtdAddresses,omitempty"`
	ConfigStatus          map[string]string                   `json:"configStatus,omitempty"`
	FdbStatus             map[string]FdbClusterStatus         `json:"fdbStatus,omitempty"`
	ClusterStatus         map[string]map[string]ClusterStatus `json:"clusterStatus,omitempty"`
	UnhealthyTargetStatus map[string][]TargetStatus           `json:"unhealthyTargetStatus,omitempty"`
	NodesInfo             NodesInfo                           `json:"nodesInfo,omitempty"`
	UpgradeInfo           UpgradeInfo                         `json:"upgradeInfo,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:path=threefsclusters,shortName=tfsc
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.phase`,description="Three Fs Cluster Status"

// ThreeFsCluster is the Schema for the threeFsClusters API
type ThreeFsCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ThreeFsClusterSpec   `json:"spec,omitempty"`
	Status ThreeFsClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ThreeFsClusterList contains a list of ThreeFsCluster
type ThreeFsClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ThreeFsCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ThreeFsCluster{}, &ThreeFsClusterList{})
}
