package controller

import (
	"context"
	"fmt"
	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/api/v1beta2"
	threefsv1 "github.com/aliyun/kvc-3fs-operator/api/v1"
	clientcomm "github.com/aliyun/kvc-3fs-operator/internal/client"
	"github.com/aliyun/kvc-3fs-operator/internal/constant"
	"github.com/aliyun/kvc-3fs-operator/internal/fdb"
	"github.com/aliyun/kvc-3fs-operator/internal/meta"
	"github.com/aliyun/kvc-3fs-operator/internal/mgmtd"
	"github.com/aliyun/kvc-3fs-operator/internal/monitor"
	"github.com/aliyun/kvc-3fs-operator/internal/native_resources"
	"github.com/aliyun/kvc-3fs-operator/internal/storage"
	"github.com/aliyun/kvc-3fs-operator/internal/utils"
	"github.com/pkg/errors"
	"io"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	"os"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
	"strings"
	"text/template"
	"time"
)

func (r *ThreeFsClusterReconciler) updateStatus(threeFsCluster *threefsv1.ThreeFsCluster, status string) error {
	localCache := threefsv1.ThreeFsCluster{}
	_ = r.Get(context.Background(), client.ObjectKey{Name: threeFsCluster.Name, Namespace: threeFsCluster.Namespace}, &localCache)
	modifiedObj := localCache.DeepCopy()
	if modifiedObj.Status.Phase == status {
		return nil
	}
	modifiedObj.Status.Phase = status
	return r.Client.Status().Patch(context.Background(), modifiedObj, client.MergeFrom(&localCache))
}

func (r *ThreeFsClusterReconciler) updateConfigtStatus(threeFsCluster *threefsv1.ThreeFsCluster, key, status string) error {
	localCache := threefsv1.ThreeFsCluster{}
	_ = r.Get(context.Background(), client.ObjectKey{Name: threeFsCluster.Name, Namespace: threeFsCluster.Namespace}, &localCache)

	modifiedObj := localCache.DeepCopy()
	if modifiedObj.Status.ConfigStatus == nil {
		modifiedObj.Status.ConfigStatus = make(map[string]string)
	}
	if modifiedObj.Status.ConfigStatus[key] == status {
		return nil
	}
	modifiedObj.Status.ConfigStatus[key] = status
	return r.Client.Status().Patch(context.Background(), modifiedObj, client.MergeFrom(&localCache))
}

func (r *ThreeFsClusterReconciler) addLabels(threeFsCluster *threefsv1.ThreeFsCluster, labels map[string]string) error {
	for k, v := range labels {
		threeFsCluster.Labels[k] = v
	}
	return r.Update(context.Background(), threeFsCluster)
}

func (r *ThreeFsClusterReconciler) ParseMgmtdAddressesBak(name, ns string) string {
	nodeList := &corev1.NodeList{}
	if err := r.Client.List(context.Background(), nodeList, client.MatchingLabels{constant.ThreeFSMgmtdNodeKey: "true"}); err != nil {
		klog.Errorf("list node failed: %v", err)
		return ""
	}

	tfsc := threefsv1.ThreeFsCluster{}
	if err := r.Client.Get(context.Background(), client.ObjectKey{Name: name, Namespace: ns}, &tfsc); err != nil {
		klog.Errorf("get threeFsCluster %s failed: %v", name, err)
		return ""
	}

	nodesList := make([]string, 0)
	for _, node := range nodeList.Items {
		ip, err := native_resources.NewNodeConfig(r.Client).ParseNodeIp(node.Name)
		if err != nil {
			klog.Errorf("get node %s ip failed: %v", node, err)
		}
		nodesList = append(nodesList, fmt.Sprintf(`"RDMA://%s:%d"`, ip, tfsc.Spec.Mgmtd.RdmaPort))
	}
	return fmt.Sprintf("[%s]", strings.Join(nodesList, ", "))
}

func (r *ThreeFsClusterReconciler) ParseMgmtdAddresses(name, ns string, nodes []string) string {
	tfsc := threefsv1.ThreeFsCluster{}
	if err := r.Client.Get(context.Background(), client.ObjectKey{Name: name, Namespace: ns}, &tfsc); err != nil {
		klog.Errorf("get threeFsCluster %s failed: %v", name, err)
		return ""
	}

	nodesList := make([]string, 0)
	for _, node := range nodes {
		ip, err := native_resources.NewNodeConfig(r.Client).ParseNodeIp(node)
		if err != nil {
			klog.Errorf("get node %s ip failed: %v", node, err)
		}
		nodesList = append(nodesList, fmt.Sprintf(`"RDMA://%s:%d"`, ip, tfsc.Spec.Mgmtd.RdmaPort))
	}
	return fmt.Sprintf("[%s]", strings.Join(nodesList, ", "))
}

func (r *ThreeFsClusterReconciler) ParseMgmtdAddressesInPodNet(name, ns string) (string, error) {
	tfsc := threefsv1.ThreeFsCluster{}
	if err := r.Client.Get(context.Background(), client.ObjectKey{Name: name, Namespace: ns}, &tfsc); err != nil {
		klog.Errorf("get threeFsCluster %s failed: %v", name, err)
		return "", err
	}

	podList := corev1.PodList{}
	if err := r.Client.List(context.Background(), &podList, client.MatchingLabels{constant.ThreeFSComponentLabel: "mgmtd"}); err != nil {
		klog.Errorf("list pod failed: %v", err)
		return "", err
	}
	mgmtdNodesList := strings.Split(tfsc.Status.MgmtdAddresses, "|")
	if len(podList.Items) != len(mgmtdNodesList) {
		return "", fmt.Errorf("pod list length not equal mgmtd nodes length")
	}
	nodesList := make([]string, 0)
	for _, pod := range podList.Items {
		if pod.Status.PodIP == "" {
			return "", fmt.Errorf("mgmtd pod ip is empty yet", pod.Name)
		}
		nodesList = append(nodesList, fmt.Sprintf(`"RDMA://%s:%d"`, pod.Status.PodIP, tfsc.Spec.Mgmtd.RdmaPort))
	}

	return fmt.Sprintf("[%s]", strings.Join(nodesList, ", ")), nil
}

func (r *ThreeFsClusterReconciler) ParseTargetPaths(storage *storage.StorageConfig) string {
	targetPathList := make([]string, 0)
	for _, targetpath := range storage.TargetPaths {
		targetPathList = append(targetPathList, fmt.Sprintf(`"%s"`, targetpath))
	}
	return fmt.Sprintf("[%s]", strings.Join(targetPathList, ", "))
}

func (r *ThreeFsClusterReconciler) RenderMainConfigPhase1(monConfig *monitor.MonitorConfig, fdbConfig *fdb.FdbConfig, mgmtdConfig *mgmtd.MgmtdConfig) error {
	if err := r.RenderFdbConfig(fdbConfig); err != nil {
		return err
	}
	if err := r.RenderMgmtdMainConfig(monConfig, mgmtdConfig); err != nil {
		return err
	}
	return nil
}

func (r *ThreeFsClusterReconciler) RenderMainConfigPhase2(monConfig *monitor.MonitorConfig,
	metaConfig *meta.MetaConfig, storageConfig *storage.StorageConfig, mgmtdAddresses string) error {
	if err := r.RenderMetaMainConfig(monConfig, mgmtdAddresses, metaConfig); err != nil {
		return err
	}
	if err := r.RenderStorageMainConfig(monConfig, mgmtdAddresses, storageConfig); err != nil {
		return err
	}
	if err := r.RenderFuseMainConfig(monConfig, mgmtdAddresses); err != nil {
		return err
	}
	return nil
}

func (r *ThreeFsClusterReconciler) RenderFdbConfig(fdbconfig *fdb.FdbConfig) error {
	content, err := fdbconfig.GetConfigContent()
	if err != nil {
		klog.Errorf("get fdb config content failed: %v", err)
		return err
	}
	os.RemoveAll(constant.DefaultThreeFSFdbConfigPath)
	file, err := os.OpenFile(constant.DefaultThreeFSFdbConfigPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(content)
	defer file.Sync()
	return err
}

func (r *ThreeFsClusterReconciler) RenderMgmtdMainConfig(monConfig *monitor.MonitorConfig, mgmtdConfig *mgmtd.MgmtdConfig) error {
	mgmtdTempConfigPath := filepath.Join(constant.DefaultConfigPath, constant.ThreeFSMgmtdTempMain)
	mgmtdConfigPath := filepath.Join(constant.DefaultConfigPath, constant.ThreeFSMgmtdMain)
	os.RemoveAll(mgmtdConfigPath)
	if _, err := os.Stat(mgmtdConfigPath); os.IsNotExist(err) {
		mcfile, err := os.OpenFile(mgmtdConfigPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer mcfile.Close()

		tmpl, err := template.New(constant.ThreeFSMgmtdTempMain).ParseFiles(mgmtdTempConfigPath)
		if err != nil {
			return err
		}

		ip, err := monConfig.ParseServiceIp()
		if err != nil {
			klog.Errorf("get monitor service ip failed: %v", err)
			return err
		}
		maps := map[string]string{
			"remote_ip": fmt.Sprintf("%s:%d", ip, monConfig.Port),
			"rdma_port": strconv.Itoa(mgmtdConfig.RdmaPort),
			"tcp_port":  strconv.Itoa(mgmtdConfig.TcpPort),
		}
		mcfile.Seek(0, io.SeekStart)
		return tmpl.Execute(mcfile, maps)
	}
	return nil
}

func (r *ThreeFsClusterReconciler) RenderMetaMainConfig(monConfig *monitor.MonitorConfig, mgmtdAddresses string, metaConfig *meta.MetaConfig) error {
	metaTempConfigPath := filepath.Join(constant.DefaultConfigPath, constant.ThreeFSMetaTempMain)
	metaConfigPath := filepath.Join(constant.DefaultConfigPath, constant.ThreeFSMetaMain)

	os.RemoveAll(metaConfigPath)
	if _, err := os.Stat(metaConfigPath); os.IsNotExist(err) {
		mcfile, err := os.OpenFile(metaConfigPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer mcfile.Close()

		tmpl, err := template.New(constant.ThreeFSMetaTempMain).ParseFiles(metaTempConfigPath)
		if err != nil {
			return err
		}

		ip, err := monConfig.ParseServiceIp()
		if err != nil {
			klog.Errorf("get monitor service ip failed: %v", err)
			return err
		}
		maps := map[string]string{
			"remote_ip":              fmt.Sprintf("%s:%d", ip, monConfig.Port),
			"mgmtd_server_addresses": mgmtdAddresses,
			"rdma_port":              strconv.Itoa(metaConfig.RdmaPort),
			"tcp_port":               strconv.Itoa(metaConfig.TcpPort),
			"enable_trace":           strconv.FormatBool(utils.GetEnableTraceEnv()),
		}
		mcfile.Seek(0, io.SeekStart)
		return tmpl.Execute(mcfile, maps)
	}
	return nil
}

func (r *ThreeFsClusterReconciler) RenderStorageMainConfig(monConfig *monitor.MonitorConfig, mgmtdAddresses string, storageConfig *storage.StorageConfig) error {
	storageTempConfigPath := filepath.Join(constant.DefaultConfigPath, constant.ThreeFSStorageTempMain)
	storageConfigPath := filepath.Join(constant.DefaultConfigPath, constant.ThreeFSStorageMain)

	os.RemoveAll(storageConfigPath)
	if _, err := os.Stat(storageConfigPath); os.IsNotExist(err) {
		mcfile, err := os.OpenFile(storageConfigPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer mcfile.Close()

		tmpl, err := template.New(constant.ThreeFSStorageTempMain).ParseFiles(storageTempConfigPath)
		if err != nil {
			return err
		}

		ip, err := monConfig.ParseServiceIp()
		if err != nil {
			klog.Errorf("get monitor service ip failed: %v", err)
			return err
		}
		maps := map[string]string{
			"remote_ip":              fmt.Sprintf("%s:%d", ip, monConfig.Port),
			"mgmtd_server_addresses": mgmtdAddresses,
			"target_paths":           r.ParseTargetPaths(storageConfig),
			"rdma_port":              strconv.Itoa(storageConfig.RdmaPort),
			"tcp_port":               strconv.Itoa(storageConfig.TcpPort),
			"enable_trace":           strconv.FormatBool(utils.GetEnableTraceEnv()),
		}
		mcfile.Seek(0, io.SeekStart)
		return tmpl.Execute(mcfile, maps)
	}
	return nil
}

func (r *ThreeFsClusterReconciler) RenderFuseMainConfig(monConfig *monitor.MonitorConfig, mgmtdAddresses string) error {
	fuseTempConfigPath := filepath.Join(constant.DefaultConfigPath, constant.ThreeFSFuseTempMain)
	fuseConfigPath := filepath.Join(constant.DefaultConfigPath, constant.ThreeFSFuseMain)

	os.RemoveAll(fuseConfigPath)
	if _, err := os.Stat(fuseConfigPath); os.IsNotExist(err) {
		mcfile, err := os.OpenFile(fuseConfigPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer mcfile.Close()

		tmpl, err := template.New(constant.ThreeFSFuseTempMain).ParseFiles(fuseTempConfigPath)
		if err != nil {
			return err
		}

		ip, err := monConfig.ParseServiceIp()
		if err != nil {
			klog.Errorf("get monitor service ip failed: %v", err)
			return err
		}
		maps := map[string]string{
			"remote_ip":              fmt.Sprintf("%s:%d", ip, monConfig.Port),
			"mgmtd_server_addresses": mgmtdAddresses,
		}
		mcfile.Seek(0, io.SeekStart)
		return tmpl.Execute(mcfile, maps)
	}
	return nil
}

func (r *ThreeFsClusterReconciler) UserAdd(threeFsCluster *threefsv1.ThreeFsCluster, adminCliConfig *clientcomm.AdminCliConfig) (string, error) {
	tmpCfm := corev1.ConfigMap{}
	var token string
	err := r.Get(context.Background(), client.ObjectKey{Name: constant.DefaultTokenConfigName, Namespace: threeFsCluster.Namespace}, &tmpCfm)
	if err == nil {
		klog.Infof("configmap %s already exist", constant.DefaultTokenConfigName)
		token = tmpCfm.Data["token"]
		return token, nil
	} else if !k8serror.IsNotFound(err) {
		klog.Errorf("get configmap %s failed: %v", constant.DefaultTokenConfigName, err)
		return token, nil
	}

	info, err := adminCliConfig.UserAdd()
	if err != nil {
		return token, nil
	}
	for _, line := range strings.Split(info, "\n") {
		if !strings.HasPrefix(line, "Token") {
			continue
		}
		parts := strings.Split(strings.TrimSpace(strings.TrimPrefix(line, "Token")), "(")
		if len(parts) != 2 {
			break
		}
		token = parts[0]
	}
	if token == "" {
		return token, fmt.Errorf("parse token failed")
	}
	tokenConfig := native_resources.NewConfigmapConfig(r.Client)
	tokencfm := tokenConfig.WithMeta(constant.DefaultTokenConfigName, threeFsCluster.Namespace).
		WithData(map[string]string{
			"token": token,
		})
	return token, r.Create(context.Background(), tokencfm.ConfigMap)
}

type NodeInfo struct {
	Id             string
	Type           string
	Status         string
	Hostname       string
	Pid            string
	Tags           string
	LastHeartbeat  string
	ConfigVersion  string
	ReleaseVersion string
}

func ParseNodeTable(data string) ([]NodeInfo, error) {
	lines := strings.Split(data, "\n")
	if len(lines) < 2 {
		return nil, errors.New("no data rows found")
	}

	var nodes []NodeInfo

	for i, line := range lines {
		if i == 0 {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}

		node := NodeInfo{
			Id:       fields[0],
			Type:     fields[1],
			Status:   fields[2],
			Hostname: fields[3],
			Pid:      fields[4],
			Tags:     fields[5],
		}
		if len(fields) == 9 {
			node.LastHeartbeat = fields[6]
			node.ConfigVersion = fields[7]
			node.ReleaseVersion = fields[8]
		} else {
			node.LastHeartbeat = fmt.Sprintf("%s %s", fields[6], fields[7])
			node.ConfigVersion = fields[8]
			node.ReleaseVersion = fields[9]
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func TagMgmtdPrimaryLabel(nodeName string, rclient client.Client) error {
	node := &corev1.Node{}
	if err := rclient.Get(context.Background(), client.ObjectKey{Name: nodeName}, node); err != nil {
		klog.Errorf("get node %s failed: %v", nodeName, err)
		return err
	}
	node.Labels[constant.ThreeFSMgmtdPrimaryNodeKey] = "true"
	if err := rclient.Update(context.Background(), node); err != nil {
		klog.Errorf("update node %s failed: %v", nodeName, err)
		return err
	}
	return nil
}

func GetPlainNodeNameFromAdmincli(parsedNodeName string, rclient client.Client) string {
	nodeList := &corev1.NodeList{}
	if err := rclient.List(context.Background(), nodeList); err != nil {
		return ""
	}
	for _, node := range nodeList.Items {
		tmpName := strings.ReplaceAll(node.Name, "-", "_")
		tmpName = strings.ReplaceAll(tmpName, ".", "_")
		if tmpName == parsedNodeName {
			return node.Name
		}
	}
	return ""
}

func (r *ThreeFsClusterReconciler) UpdateClusterFdbStatus(tfsc *threefsv1.ThreeFsCluster, details *fdbv1beta2.FoundationDBStatus, rclient client.Client) error {
	newObj := tfsc.DeepCopy()

	if newObj.Status.FdbStatus == nil {
		newObj.Status.FdbStatus = make(map[string]threefsv1.FdbClusterStatus)
	}

	coordinatorsMaps := make(map[string]bool)
	node2processMaps := make(map[string]string)
	machine2IpMaps := make(map[string]string)
	nodeList := &corev1.NodeList{}
	if err := rclient.List(context.Background(), nodeList); err != nil {
		klog.Errorf("list node failed: %v", err)
		return err
	}
	for _, node := range nodeList.Items {
		ip, _ := native_resources.NewNodeConfig(rclient).ParseNodeIp(node.Name)
		machine2IpMaps[node.Name] = ip
	}
	// klog.Infof("machine2IpMaps: %+v", machine2IpMaps)
	for _, process := range details.Cluster.Processes {
		node2processMaps[process.Locality["machineid"]] = process.Address.String()
	}
	// klog.Infof("node2processMaps: %+v", node2processMaps)
	for _, coordinator := range details.Client.Coordinators.Coordinators {
		coordinatorsMaps[strings.Split(coordinator.Address.String(), ":")[0]] = coordinator.Reachable
	}
	// klog.Infof("coordinatorsMaps: %+v", coordinatorsMaps)

	for _, node := range nodeList.Items {
		// if node is process
		_, isProcess := node2processMaps[node.Name]
		// if node is coordinator
		reachable, iscoordinators := coordinatorsMaps[machine2IpMaps[node.Name]]

		// no such process
		notExist := false
		if !isProcess && !iscoordinators {
			notExist = true
		}

		statusStr := "Healthy"
		faultTime := ""
		if iscoordinators && !reachable {
			statusStr = "UnHealthy"
			faultTime = time.Now().Format(constant.TimeLayout)
		}

		tmpStatus := threefsv1.FdbClusterStatus{
			Name:          node.Name,
			IsCoordinator: iscoordinators,
			IsReachable:   reachable,
			IsDeleted:     false,
			Status:        statusStr,
			Msg:           "",
			FaultTime:     faultTime,
		}

		if _, ok := newObj.Status.FdbStatus[node.Name]; !ok {
			// not exist before, not exist now, skip
			if notExist {
				continue
			}
			// not exist before, exist now
			newObj.Status.FdbStatus[node.Name] = tmpStatus
		} else {
			// exist before, not exist now, fault
			if notExist {
				if newObj.Status.FdbStatus[node.Name].FaultTime != "" {
					tmpStatus = newObj.Status.FdbStatus[node.Name]
				} else {
					// first time check: process down and not coordinators
					tmpStatus.Status = "UnHealthy"
					tmpStatus.FaultTime = time.Now().Format(constant.TimeLayout)
				}
				newObj.Status.FdbStatus[node.Name] = tmpStatus
				continue
			}
			// exist before, exist now
			if newObj.Status.FdbStatus[node.Name].IsReachable == reachable &&
				newObj.Status.FdbStatus[node.Name].IsCoordinator == iscoordinators &&
				newObj.Status.FdbStatus[node.Name].Status == statusStr {
				// no change here, use old fault time
				continue
			} else {
				// change,use new status
				newObj.Status.FdbStatus[node.Name] = tmpStatus
			}
		}
	}

	// check for fdb process, but not coordinators
	if tfsc.Status.FdbStatus != nil {
		for key, val := range tfsc.Status.FdbStatus {
			if _, ok := newObj.Status.FdbStatus[key]; !ok {
				// new fault
				if val.FaultTime == "" {
					val.FaultTime = time.Now().Format(constant.TimeLayout)
				}
				// existing fault
				newObj.Status.FdbStatus[key] = val
			}
		}
	}

	klog.Infof("try to update ThreeFsCluster %s fdb status to %+v", tfsc.Name, newObj.Status.FdbStatus)
	// check fault
	klog.Infof("try to check fdb fault time duration")
	for k, v := range newObj.Status.FdbStatus {
		if v.FaultTime == "" {
			continue
		}
		startTime, err := time.Parse(constant.TimeLayout, v.FaultTime)
		if err != nil {
			klog.Errorf("parse fault time failed: %v", err)
			return err
		}
		faultTime := utils.GetFaultDurationEnv()
		if time.Now().Sub(startTime) > time.Duration(int64(faultTime))*time.Minute && !v.IsDeleted {
			klog.Infof("node %s fault time duration is %s, try to label node", k, time.Now().Sub(startTime))
			r.Recorder.Event(tfsc, corev1.EventTypeWarning, "FdbFault", fmt.Sprintf("node %s fault time duration is %s, tag fdb fault label", k, time.Now().Sub(startTime)))
			nodeObj := &corev1.Node{}
			if err := rclient.Get(context.Background(), client.ObjectKey{Name: k}, nodeObj); err != nil {
				klog.Errorf("get node %s failed: %v", k, err)
			} else {
				nodeObj.Labels[constant.ThreeFSFdbFaultNodeKey] = "true"
				if err := rclient.Update(context.Background(), nodeObj); err != nil {
					klog.Errorf("update node %s with fdb fault label failed: %v", k, err)
					return err
				}
			}
			// TODO
			v.IsDeleted = true
			newObj.Status.FdbStatus[k] = v
		}
	}

	return rclient.Status().Patch(context.Background(), newObj, client.MergeFrom(tfsc))
}

func GetNodeNameFromParsedName(parsedName string, rclient client.Client) string {
	nodeList := &corev1.NodeList{}
	if err := rclient.List(context.Background(), nodeList); err != nil {
		klog.Errorf("list node failed: %v", err)
		return ""
	}
	for _, node := range nodeList.Items {
		if utils.TranslatePlainNodeName3fs(node.Name) == parsedName {
			return node.Name
		}
	}
	return ""
}

func SelectOneNodeWithLabelKey(nodeName, labelkey string, rclient client.Client) string {
	nodeList := &corev1.NodeList{}
	if err := rclient.List(context.Background(), nodeList, client.MatchingLabels{constant.ThreeFSStorageNodeKey: "true"}); err != nil {
		klog.Errorf("list node failed: %v", err)
		return ""
	}
	for _, node := range nodeList.Items {
		if node.Name == nodeName {
			// skip fault node
			continue
		}
		if _, ok := node.Labels[labelkey]; ok {
			// skip node with label
			continue
		}
		node.Labels[labelkey] = "true"
		if err := rclient.Update(context.Background(), &node); err != nil {
			klog.Errorf("update node %s with label failed: %v", node.Name, err)
			return ""
		}
		return node.Name
	}
	return ""
}

func (r *ThreeFsClusterReconciler) UpdateClusterStatus(adminCli *clientcomm.AdminCliConfig, tfsc *threefsv1.ThreeFsCluster, rclient client.Client) error {
	klog.Infof("UpdateClusterStatus: try to update ThreeFsCluster %s status", tfsc.Name)
	output, err := adminCli.ListNodes()
	if err != nil {
		klog.Infof("list nodes failed: %v", err)
		return err
	}
	nodes, err := ParseNodeTable(output)
	if err != nil {
		klog.Infof("parse node table failed: %v", err)
		return err
	}
	status := make(map[string]map[string]threefsv1.ClusterStatus)
	for _, node := range nodes {
		if _, ok := status[node.Type]; !ok {
			status[node.Type] = make(map[string]threefsv1.ClusterStatus)
		}
		tmpStatus := threefsv1.ClusterStatus{
			Name:             node.Hostname,
			Status:           node.Status,
			LastHeatBeatTime: node.LastHeartbeat,
		}

		// check if primary mgmtd is up-to-date
		if node.Status == "PRIMARY_MGMTD" {
			if !strings.Contains(node.ConfigVersion, "UPTODATE") {
				return fmt.Errorf("primary mgmtd is not up-to-date, wait")
			} else {
				status[node.Type][node.Hostname] = tmpStatus
				continue
			}
		}
		if node.Status == "HEARTBEAT_CONNECTING" {
			return fmt.Errorf("node %s, type: %s is heartbeat connecting, wait", node.Id, node.Type)
		}
		// no mgmtd primary here
		if (node.Type == "META" || node.Type == "MGMTD") && node.Status != "HEARTBEAT_CONNECTED" {
			tagFault := false
			if node.LastHeartbeat == "N/A" {
				tagFault = true
				klog.Errorf("node %s last heartbeat is N/A, tag fault", node.Id)
			} else {
				lastHeartBeat, err := time.Parse(constant.TimeLayout, node.LastHeartbeat)
				if err != nil {
					klog.Errorf("parse last heartbeat %s failed: %v", node.LastHeartbeat, err)
					return err
				}
				faultTime := utils.GetFaultDurationEnv()
				if time.Now().Sub(lastHeartBeat) > time.Duration(int64(faultTime))*time.Minute {
					tagFault = true
					klog.Errorf("node %s last heartbeat is %s, according to time duration, tag fault", node.Id, node.LastHeartbeat)
					r.Recorder.Event(tfsc, corev1.EventTypeWarning, "MetaFault", fmt.Sprintf("node %s last heartbeat is %s, try to move to another node", node.Id, node.LastHeartbeat))
				}
			}
			if tagFault {
				nodeName := GetNodeNameFromParsedName(node.Hostname, rclient)
				klog.Infof("node(%s) %s is fault, try to remove node label", node.Type, nodeName)
				nodeObj := &corev1.Node{}
				if err := rclient.Get(context.Background(), client.ObjectKey{Name: nodeName}, nodeObj); err != nil {
					klog.Errorf("get node %s failed: %v", nodeName, err)
					return err
				}

				if node.Type == "MGMTD" && !utils.GetUseHostNetworkEnv() {
					// TODO mgmtd fault handle
					if _, ok := nodeObj.Labels[constant.ThreeFSMgmtdNodeKey]; ok {
						delete(nodeObj.Labels, constant.ThreeFSMgmtdNodeKey)
						if err := rclient.Update(context.Background(), nodeObj); err != nil {
							klog.Errorf("remove node %s with mgmtd node label failed: %v", nodeName, err)
							return err
						}
						klog.Infof("remove node %s with mgmtd node label success", nodeName)
						newNodeName := SelectOneNodeWithLabelKey(nodeName, constant.ThreeFSMgmtdNodeKey, rclient)
						if newNodeName == "" {
							klog.Errorf("select one node with label %s failed", constant.ThreeFSMgmtdNodeKey)
							return err
						}
						klog.Infof("select one node with label %s success, new node is %s", constant.ThreeFSMgmtdNodeKey, newNodeName)
						adminCli.UnregisterNode(node.Id, node.Type)
					}
				} else if node.Type == "META" {
					if _, ok := nodeObj.Labels[constant.ThreeFSMetaNodeKey]; ok {
						delete(nodeObj.Labels, constant.ThreeFSMetaNodeKey)
						if err := rclient.Update(context.Background(), nodeObj); err != nil {
							klog.Errorf("remove node %s with meta node label failed: %v", nodeName, err)
							return err
						}
						klog.Infof("remove node %s with meta node label success", nodeName)
						newNodeName := SelectOneNodeWithLabelKey(nodeName, constant.ThreeFSMetaNodeKey, rclient)
						if newNodeName == "" {
							klog.Errorf("select one node with label %s failed", constant.ThreeFSMetaNodeKey)
							return err
						}
						klog.Infof("select one node with label %s success, new node is %s", constant.ThreeFSMetaNodeKey, newNodeName)
					}
					adminCli.UnregisterNode(node.Id, node.Type)
				}
			}
		}
		status[node.Type][node.Hostname] = tmpStatus
	}

	newObj := tfsc.DeepCopy()
	newObj.Status.ClusterStatus = status
	return rclient.Status().Patch(context.Background(), newObj, client.MergeFrom(tfsc))
}

func CheckComponentExist(adminCli *clientcomm.AdminCliConfig, component, nodeName string) bool {
	parsedNodename := utils.TranslatePlainNodeName3fs(nodeName)
	output, err := adminCli.ListNodes()
	if err != nil {
		klog.Infof("list nodes failed: %v", err)
		return false
	}
	nodes, err := ParseNodeTable(output)
	if err != nil {
		klog.Infof("parse node table failed: %v", err)
		return false
	}
	for _, node := range nodes {
		if node.Type == component && parsedNodename == node.Hostname {
			return true
		}
	}
	return false
}

func CheckComponentStatus(adminCli *clientcomm.AdminCliConfig, component, nodeName string, all bool, rclient client.Client) bool {
	var parsedNodeName string
	if len(nodeName) != 0 {
		parsedNodeName = strings.ReplaceAll(nodeName, "-", "_")
		parsedNodeName = strings.ReplaceAll(parsedNodeName, ".", "_")
		klog.Infof("parsed node name is %s", parsedNodeName)
	}
	output, err := adminCli.ListNodes()
	if err != nil {
		klog.Infof("list nodes failed: %v", err)
		return false
	}
	nodes, err := ParseNodeTable(output)
	if err != nil {
		klog.Infof("parse node table failed: %v", err)
		return false
	}

	// if this node+type existed
	tag := false
	for _, node := range nodes {
		if len(parsedNodeName) > 0 && node.Hostname != parsedNodeName {
			continue
		}
		if node.Type != component {
			continue
		}
		tag = true
		//if node.Status == "PRIMARY_MGMTD" {
		//	if err := TagMgmtdPrimaryLabel(GetPlainNodeNameFromAdmincli(node.Hostname, rclient), rclient); err != nil {
		//		klog.Errorf("tag node %s with mgmgt primary label failed: %v", node.Id, err)
		//		return false
		//	}
		//}
		if node.Status != "HEARTBEAT_CONNECTED" && node.Status != "PRIMARY_MGMTD" {
			klog.Errorf("node %s status is %s, ConfigVersion is %s", node.Id, node.Status, node.ConfigVersion)
			if all || len(parsedNodeName) > 0 {
				return false
			}
		} else {
			if !strings.Contains(node.ConfigVersion, "UPTODATE") {
				klog.Errorf("node %s ConfigVersion is %s", node.Id, node.ConfigVersion)
				if all || len(parsedNodeName) > 0 {
					return false
				}
			}
			if node.LastHeartbeat != "N/A" {
				lastHeartBeat, _ := time.Parse(constant.TimeLayout, node.LastHeartbeat)
				if time.Now().Sub(lastHeartBeat) > 10*time.Second {
					klog.Errorf("node %s last heartbeat is %s, more than 10s", node.Id, node.LastHeartbeat)
					if all || len(parsedNodeName) > 0 {
						return false
					}
				}
			}
		}
	}
	// all same type component is HEARTBEAT_CONNECTED and ConfigVersion is UPTODATE, return true
	return tag
}

func (r *ThreeFsClusterReconciler) deleteTokenConfig(threeFsCluster *threefsv1.ThreeFsCluster) error {
	tmpCfm := corev1.ConfigMap{}
	err := r.Get(context.Background(), client.ObjectKey{Name: constant.DefaultTokenConfigName, Namespace: threeFsCluster.Namespace}, &tmpCfm)
	if err != nil {
		if k8serror.IsNotFound(err) {
			return nil
		}
		klog.Errorf("get configmap %s failed: %v", constant.DefaultTokenConfigName, err)
		return err
	}
	return r.Client.Delete(context.Background(), &tmpCfm)
}

func (r *ThreeFsClusterReconciler) unTagNode(threeFsCluster *threefsv1.ThreeFsCluster) error {
	nodeList := &corev1.NodeList{}
	if err := r.List(context.Background(), nodeList); err != nil {
		klog.Errorf("list node failed: %v", err)
		return err
	}
	for _, node := range nodeList.Items {
		if _, ok := node.Labels[constant.ThreeFSMetaNodeKey]; ok {
			delete(node.Labels, constant.ThreeFSMetaNodeKey)
		}
		if _, ok := node.Labels[constant.ThreeFSMgmtdNodeKey]; ok {
			delete(node.Labels, constant.ThreeFSMgmtdNodeKey)
		}
		if _, ok := node.Labels[constant.ThreeFSMgmtdPrimaryNodeKey]; ok {
			delete(node.Labels, constant.ThreeFSMgmtdPrimaryNodeKey)
		}
		if err := r.Client.Update(context.Background(), &node); err != nil {
			klog.Errorf("update node %s failed: %v", node.Name, err)
			return err
		}
	}
	return nil
}

func UpdateTargetStatus(admincliConfig *clientcomm.AdminCliConfig, threeFsCluster *threefsv1.ThreeFsCluster, rclient client.Client) error {
	output, err := admincliConfig.ListNodes()
	if err != nil {
		klog.Errorf("list nodes failed: %v", err)
		return err
	}
	nodes, err := ParseNodeTable(output)
	if err != nil {
		klog.Errorf("parse node table failed: %v", err)
		return err
	}
	id2NodeNameMaps := make(map[string]string)
	unhealthTargets := make(map[string][]threefsv1.TargetStatus)
	for _, node := range nodes {
		if node.Type == "STORAGE" {
			id2NodeNameMaps[node.Id] = node.Hostname
			unhealthTargets[node.Hostname] = make([]threefsv1.TargetStatus, 0)
		}
	}

	output2, err := admincliConfig.ListTargets()
	if err != nil {
		klog.Errorf("list targets failed: %v", err)
		return err
	}

	targets, _ := ParseTargets(output2)
	if len(targets) == 0 {
		klog.Infof("targets is empty now")
		return nil
	}

	oldUnhealthyTargets := map[string]threefsv1.TargetStatus{}
	for _, targetStatus := range threeFsCluster.Status.UnhealthyTargetStatus {
		for _, target := range targetStatus {
			oldUnhealthyTargets[target.TargetId] = target
		}
	}

	for _, target := range targets {
		nodeid := target.TargetId[2:7]
		nodeName := id2NodeNameMaps[nodeid]
		if target.State != "UPTODATE" {
			targetObj := threefsv1.TargetStatus{
				TargetId: target.TargetId,
				Status:   target.State,
			}

			if strings.Contains(target.State, "OFFLINE") {
				oldTarget, ok := oldUnhealthyTargets[target.TargetId]
				// no change on status, use old time
				if ok && oldTarget.Status == target.State {
					targetObj.OfflineTime = oldTarget.OfflineTime
					if oldTarget.OfflineTime == "" {
						targetObj.OfflineTime = time.Now().Format(constant.TimeLayout)
					}
				} else {
					// not offline before, record time
					targetObj.OfflineTime = time.Now().Format(constant.TimeLayout)
				}
			}
			unhealthTargets[nodeName] = append(unhealthTargets[nodeName], targetObj)
		}
	}

	//klog.Warningf("unhealthTargets is %+v", unhealthTargets)
	newObj := threeFsCluster.DeepCopy()
	newObj.Status.UnhealthyTargetStatus = unhealthTargets
	return rclient.Status().Patch(context.Background(), newObj, client.MergeFrom(threeFsCluster))
}

func GetSvcDnsName(name, ns string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", name, ns)
}

func ParseMgmtdAddressesFromHeadlessSvc(name, ns string, port int) string {
	return fmt.Sprintf(`["RDMA://%s:%d"]`, GetSvcDnsName(mgmtd.GetMgmtdDeployName(name), ns), port)
}

func IsProcessingTfsctExisted(rclient client.Client) bool {
	tfsctList := &threefsv1.ThreeFsChainTableList{}
	if err := rclient.List(context.Background(), tfsctList); err != nil {
		klog.Errorf("list tfsct failed: %v", err)
		return false
	}
	for _, tfsct := range tfsctList.Items {
		if tfsct.Status.Phase == constant.ThreeFSChainTableFinishedStatus {
			continue
		}
		return true
	}
	return false
}

func CheckStorageBackup(threeFsCluster *threefsv1.ThreeFsCluster) bool {
	if threeFsCluster.Status.NodesInfo.StorageBackupNodes == nil || len(threeFsCluster.Status.NodesInfo.StorageBackupNodes) == 0 {
		return false
	}
	return true
}

func (r *ThreeFsClusterReconciler) CreateTfsct(tfsctName, tfscName, namespace, plainNewName, plainOldName string, tfsctLabels map[string]string) error {
	tfsctObj := threefsv1.NewThreeFsChainTable(tfsctName, namespace).
		WithThreeFsCluster(tfscName, namespace).
		WithNewNode([]string{plainNewName}).
		WithOldNode([]string{plainOldName}).
		WithType(constant.ThreeFSChainTableTypeReplace).
		WithForce(true).
		WithLabels(tfsctLabels)

	return r.Client.Create(context.Background(), tfsctObj)
}

func (r *ThreeFsClusterReconciler) HandleFaultStorage(threeFsCluster *threefsv1.ThreeFsCluster) error {
	// check storage status, if not heartbeat_connected for xx min, create tfsct crd for replace
	faultTime := utils.GetFaultDurationEnv()
	for _, storageNode := range threeFsCluster.Status.ClusterStatus["STORAGE"] {
		startTime, err := time.Parse(constant.TimeLayout, storageNode.LastHeatBeatTime)
		if err != nil {
			klog.Errorf("parse fault time failed: %v", err)
			return err
		}
		if storageNode.Status != "HEARTBEAT_CONNECTED" && time.Now().Sub(startTime) > time.Duration(int64(faultTime))*time.Minute {
			if !IsProcessingTfsctExisted(r.Client) {
				if !CheckStorageBackup(threeFsCluster) {
					klog.Errorf("storage %s status is not healthy, but storage backup nodes is empty", storageNode.Name)
					r.Recorder.Event(threeFsCluster, corev1.EventTypeWarning, "StorageNotHealthy", "storage not healthy")
				}

				plainOldName := ParsePlainNameWithNodeId(r.Client, storageNode.Name)
				plainNewName := threeFsCluster.Status.NodesInfo.StorageBackupNodes[0]
				tfsctName := fmt.Sprintf("tfsct-replace-%s", utils.GenerateUuidWithLen(6))
				klog.Infof("create tfsct %s for replace(newNode: %s, oldName:%s)", tfsctName, plainNewName, plainOldName)
				tfsctLabels := map[string]string{
					constant.ThreeFSAutoReplaceLabel: "true",
				}
				tfsctObj := threefsv1.NewThreeFsChainTable(tfsctName, threeFsCluster.Namespace).
					WithThreeFsCluster(threeFsCluster.Name, threeFsCluster.Namespace).
					WithNewNode([]string{plainNewName}).
					WithOldNode([]string{plainOldName}).
					WithType(constant.ThreeFSChainTableTypeReplace).
					WithForce(true).
					WithLabels(tfsctLabels)

				if err := r.Client.Create(context.Background(), tfsctObj); err != nil {
					klog.Errorf("create tfsct %s failed: %v", tfsctName, err)
					return err
				}
				return nil
			}
		}
	}

	// check storage target status
	oldTfscObj := threeFsCluster.DeepCopy()
	for node, targets := range threeFsCluster.Status.UnhealthyTargetStatus {
		if len(targets) == 0 {
			continue
		}
		klog.Infof("node %s has unhealthy targets, check fault duration and auto replace", node)
		tagFault := false
		for idx, target := range targets {
			if strings.Contains(target.Status, "OFFLINE") {
				if target.OfflineTime != "" {
					startTime, err := time.Parse(constant.TimeLayout, target.OfflineTime)
					if err != nil {
						klog.Errorf("parse fault time failed: %v", err)
						return err
					}
					if time.Now().Sub(startTime) > time.Duration(int64(faultTime))*time.Minute {
						tagFault = true
					}
				}
			}
			targets[idx] = target
			// detect one target, break here
			if tagFault {
				break
			}
		}
		threeFsCluster.Status.UnhealthyTargetStatus[node] = targets
		if err := r.Status().Patch(context.Background(), threeFsCluster, client.MergeFrom(oldTfscObj)); err != nil {
			klog.Errorf("patch threeFsCluster %s failed: %v", threeFsCluster.Name, err)
			return err
		}
		if tagFault {
			if !CheckStorageBackup(oldTfscObj) {
				klog.Errorf("storage %s status is not healthy, but storage backup nodes is empty", node)
				r.Recorder.Event(threeFsCluster, corev1.EventTypeWarning, "StorageNotHealthy", "storage not healthy")
				return fmt.Errorf("storage %s status is not healthy, but storage backup nodes is empty", node)
			}

			plainOldName := ParsePlainNameWithNodeId(r.Client, node)
			plainNewName := threeFsCluster.Status.NodesInfo.StorageBackupNodes[0]
			tfsctName := fmt.Sprintf("tfsct-replace-%s", utils.GenerateUuidWithLen(6))
			klog.Infof("create tfsct %s for replace(newNode: %s, oldName:%s)", tfsctName, plainNewName, plainOldName)
			tfsctLabels := map[string]string{
				constant.ThreeFSAutoReplaceLabel: "true",
			}
			if err := r.CreateTfsct(tfsctName, threeFsCluster.Name, threeFsCluster.Namespace, plainNewName, plainOldName, tfsctLabels); err != nil {
				klog.Errorf("create tfsct %s failed: %v", tfsctName, err)
				return err
			}
			// detect one target, break here
			break
		}
	}
	return nil
}

func (r *ThreeFsClusterReconciler) RecordImageversion(tfsc *threefsv1.ThreeFsCluster) error {
	monitorImage := os.Getenv("MONITOR_IMAGE")
	mgmtdImage := os.Getenv("MGMTD_IMAGE")
	metaImage := os.Getenv("META_IMAGE")
	storageImage := os.Getenv("STORAGE_IMAGE")

	oldTfsc := tfsc.DeepCopy()
	if tfsc.Status.UpgradeInfo.ImageVersion == nil {
		tfsc.Status.UpgradeInfo.ImageVersion = make(map[string]string)
		tfsc.Status.UpgradeInfo.UpgradeProcess = make(map[string]string)
	}

	tag := false
	if tfsc.Status.UpgradeInfo.ImageVersion["monitor"] != monitorImage ||
		tfsc.Status.UpgradeInfo.ImageVersion["mgmtd"] != mgmtdImage ||
		tfsc.Status.UpgradeInfo.ImageVersion["meta"] != metaImage ||
		tfsc.Status.UpgradeInfo.ImageVersion["storage"] != storageImage {
		tag = true
	}
	tfsc.Status.UpgradeInfo.ImageVersion["monitor"] = monitorImage
	tfsc.Status.UpgradeInfo.ImageVersion["mgmtd"] = mgmtdImage
	tfsc.Status.UpgradeInfo.ImageVersion["meta"] = metaImage
	tfsc.Status.UpgradeInfo.ImageVersion["storage"] = storageImage
	if tag {
		tfsc.Status.UpgradeInfo.UpgradeProcess = make(map[string]string)
		tfsc.Status.UpgradeInfo.Finished = false
	}

	if err := r.Status().Patch(context.Background(), tfsc, client.MergeFrom(oldTfsc)); err != nil {
		klog.Errorf("patch threeFsCluster %s failed: %v", tfsc.Name, err)
		return err
	}
	return nil
}

func CheckTargetStatus(adminCliConfig *clientcomm.AdminCliConfig, nodeName string) bool {
	targets, err := GetTargetsWithNode(adminCliConfig, nodeName)
	if err != nil {
		klog.Errorf("get target failed: %v", err)
		return false
	}
	for _, target := range targets {
		if target.State != "UPTODATE" {
			return false
		}
	}
	return true
}

func (r *ThreeFsClusterReconciler) HandleImageUpgrade(adminCliConfig *clientcomm.AdminCliConfig, tfsc *threefsv1.ThreeFsCluster) error {
	componentsMaps := map[string]string{
		"monitor": constant.ThreeFSMonitorDeploymentKey,
		"mgmtd":   constant.ThreeFSMgmtdDeployKey,
		"meta":    constant.ThreeFSMetaDeployKey,
		"storage": constant.ThreeFSStorageDeployKey,
	}

	for component, labelKey := range componentsMaps {
		deployList := &appsv1.DeploymentList{}
		if err := r.Client.List(context.Background(), deployList, client.MatchingLabels{labelKey: tfsc.Name}); err != nil {
			klog.Errorf("list deployment failed: %v", err)
			return err
		}

		usingImageList := make([]string, len(deployList.Items))
		for i, deploy := range deployList.Items {
			usingImageList[i] = deploy.Spec.Template.Spec.Containers[0].Image
		}
		if utils.StrListAllContains(usingImageList, tfsc.Status.UpgradeInfo.ImageVersion[component]) {
			// all images are up-to-date, no need to check
			continue
		}

		// check new image version
		tag := false
		for _, deploy := range deployList.Items {
			if deploy.Spec.Template.Spec.Containers[0].Image == tfsc.Status.UpgradeInfo.ImageVersion[component] {
				// check pod status first
				if deploy.Status.AvailableReplicas != deploy.Status.Replicas {
					tag = true
					break
				}
				deployNodeName := deploy.Spec.Template.Spec.NodeSelector[constant.KubernetesHostnameKey]
				threefsNodeName := utils.TranslatePlainNodeName3fs(deployNodeName)
				if component == "meta" || component == "mgmtd" {
					if !CheckComponentStatus(adminCliConfig, strings.ToUpper(component), threefsNodeName, false, r.Client) {
						klog.Infof("%s deploy on node %s is not ready yet, wait", component, deployNodeName)
						tag = true
						break
					}
				} else if component == "storage" {
					if !CheckComponentStatus(adminCliConfig, strings.ToUpper(component), threefsNodeName, false, r.Client) || !CheckTargetStatus(adminCliConfig, deployNodeName) {
						klog.Infof("storage deploy on node %s is not ready yet, wait", deployNodeName)
						tag = true
						break
					}
				}
			}
		}
		if tag {
			break
		}

		// check old image version
		for _, deploy := range deployList.Items {
			if deploy.Spec.Template.Spec.Containers[0].Image != tfsc.Status.UpgradeInfo.ImageVersion[component] {
				klog.Infof("%s image version is not up-to-date, usingImage: %s, newImageVersion: %s", component, deploy.Spec.Template.Spec.Containers[0].Image, tfsc.Status.UpgradeInfo.ImageVersion[component])
				deploy.Spec.Template.Spec.Containers[0].Image = tfsc.Status.UpgradeInfo.ImageVersion[component]
				if err := r.Client.Update(context.Background(), &deploy); err != nil {
					klog.Errorf("update deployment failed: %v", err)
					return err
				}
				// one by one
				tag = true
				// sleep here in order to specify component heartbeat
				time.Sleep(2 * time.Minute)
				break
			}
		}
		if tag {
			break
		}
	}

	return nil
}
