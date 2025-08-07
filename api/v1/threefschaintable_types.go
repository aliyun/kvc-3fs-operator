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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ThreeFsChainTableSpec defines the desired state of ThreeFsChainTable
type ThreeFsChainTableSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	ThreeFsClusterName      string   `json:"threeFsClusterName"`
	ThreeFsClusterNamespace string   `json:"threeFsClusterNamespace"`
	NewNode                 []string `json:"newNode,omitempty"`
	OldNode                 []string `json:"oldNode,omitempty"`
	Type                    string   `json:"type"`
	Force                   bool     `json:"force,omitempty"`
}

// ThreeFsChainTableStatus defines the observed state of ThreeFsChainTable
type ThreeFsChainTableStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	Phase           string   `json:"phase,omitempty"`
	Process         string   `json:"process,omitempty"`
	ProcessChainIds []string `json:"processChainIds,omitempty"`
	Executed        bool     `json:"executed,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=threefschaintables,shortName=tfsct
// +kubebuilder:printcolumn:name="Process",type=string,JSONPath=`.status.process`,description="ThreeFs chain table process"
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.phase`,description="ThreeFs chain table status"

// ThreeFsChainTable is the Schema for the Threefschaintables API
type ThreeFsChainTable struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ThreeFsChainTableSpec   `json:"spec,omitempty"`
	Status ThreeFsChainTableStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ThreeFsChainTableList contains a list of ThreeFsChainTable
type ThreeFsChainTableList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ThreeFsChainTable `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ThreeFsChainTable{}, &ThreeFsChainTableList{})
}

func NewThreeFsChainTable(name, namespace string) *ThreeFsChainTable {
	return &ThreeFsChainTable{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: ThreeFsChainTableSpec{},
	}
}

func (v *ThreeFsChainTable) WithNewNode(newNodes []string) *ThreeFsChainTable {
	v.Spec.NewNode = newNodes
	return v
}

func (v *ThreeFsChainTable) WithOldNode(oldNodes []string) *ThreeFsChainTable {
	v.Spec.OldNode = oldNodes
	return v
}

func (v *ThreeFsChainTable) WithType(t string) *ThreeFsChainTable {
	v.Spec.Type = t
	return v
}

func (v *ThreeFsChainTable) WithForce(force bool) *ThreeFsChainTable {
	v.Spec.Force = force
	return v
}

func (v *ThreeFsChainTable) WithThreeFsCluster(name, namespace string) *ThreeFsChainTable {
	v.Spec.ThreeFsClusterName = name
	v.Spec.ThreeFsClusterNamespace = namespace
	return v
}

func (v *ThreeFsChainTable) WithLabels(labels map[string]string) *ThreeFsChainTable {
	v.Labels = labels
	return v
}
