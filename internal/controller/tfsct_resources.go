package controller

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	threefsv1 "github.com/aliyun/kvc-3fs-operator/api/v1"
	clientcomm "github.com/aliyun/kvc-3fs-operator/internal/client"
	"github.com/aliyun/kvc-3fs-operator/internal/constant"
	"github.com/aliyun/kvc-3fs-operator/internal/storage"
	"github.com/aliyun/kvc-3fs-operator/internal/utils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"os"
	"regexp"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
	"strings"
)

func TagStorageNode(nodeName string, rclient client.Client) error {
	node := &corev1.Node{}
	if err := rclient.Get(context.Background(), client.ObjectKey{Name: nodeName}, node); err != nil {
		klog.Errorf("get node %s err: %+v", nodeName, err)
		return err
	}
	if _, ok := node.Labels[constant.ThreeFSStorageNodeKey]; ok {
		return nil
	}
	node.Labels[constant.ThreeFSStorageNodeKey] = "true"
	if err := rclient.Update(context.Background(), node); err != nil {
		klog.Errorf("update node %s with storage label failed: %v", node.Name, err)
		return err
	}
	return nil
}

type Chain struct {
	ChainId        string
	ReferencedBy   string
	ChainVersion   string
	Status         string
	PreferredOrder string
	TargetNum      int
	Targets        []Target
	Key            string
}

type Target struct {
	TargetId string
	State    string
}

func ParseChain(line string) (*Chain, error) {
	fields := strings.Fields(line)

	chain := &Chain{
		ChainId:        fields[0],
		ReferencedBy:   fields[1],
		ChainVersion:   fields[2],
		Status:         fields[3],
		PreferredOrder: fields[4],
	}

	chain.TargetNum = len(fields[5:])
	chain.Targets = make([]Target, chain.TargetNum)
	targetRegex := regexp.MustCompile(`^(\d+)\((\S+-\S+)\)$`)
	for i, targetField := range fields[5:] {
		matches := targetRegex.FindStringSubmatch(targetField)
		if len(matches) != 3 {
			return nil, fmt.Errorf("invalid target format: %s", targetField)
		}
		chain.Targets[i] = Target{
			TargetId: matches[1],
			State:    matches[2],
		}
	}

	return chain, nil
}

func ParseChainTable(output string) ([]Chain, error) {
	lines := strings.Split(string(output), "\n")
	chains := make([]Chain, 0)

	for i, line := range lines {
		if i == 0 || line == "" {
			continue
		}
		chain, err := ParseChain(line)
		if err != nil {
			fmt.Printf("parse line(%d) '%s' failed: %v\n", i, line, err)
			continue
		}
		chains = append(chains, *chain)
	}
	return chains, nil
}

func ParseTarget(line string) (*Target, error) {
	fields := strings.Fields(line)
	target := &Target{
		TargetId: fields[0],
		State:    fields[4],
	}
	return target, nil
}

func ParseTargets(output string) ([]Target, error) {
	lines := strings.Split(output, "\n")
	targets := make([]Target, 0)

	for i, line := range lines {
		if i == 0 || line == "" {
			continue
		}
		target, _ := ParseTarget(line)
		targets = append(targets, *target)
	}
	return targets, nil
}

func ParseNodeIdFromNodeName(adminCli *clientcomm.AdminCliConfig, nodeType, nodeName string) (int, error) {
	parsedNodeName := strings.ReplaceAll(nodeName, "-", "_")
	parsedNodeName = strings.ReplaceAll(parsedNodeName, ".", "_")
	output, err := adminCli.ListNodes()
	if err != nil {
		klog.Infof("list nodes failed: %v", err)
		return -1, err
	}
	nodes, err := ParseNodeTable(output)
	if err != nil {
		klog.Infof("parse node table failed: %v", err)
		return -1, err
	}
	for _, node := range nodes {
		if node.Type == nodeType && node.Hostname == parsedNodeName {
			return strconv.Atoi(node.Id)
		}
	}
	return -1, fmt.Errorf("node %s not found", parsedNodeName)
}

func (r *ThreeFsChainTableReconciler) AllocateNodeId(adminCli *clientcomm.AdminCliConfig, nodeType string) (int, error) {
	output, err := adminCli.ListNodes()
	if err != nil {
		klog.Infof("list nodes failed: %v", err)
		return -1, err
	}
	nodes, err := ParseNodeTable(output)
	if err != nil {
		klog.Infof("parse node table failed: %v", err)
		return -1, err
	}
	maxNodeId := constant.ThreeFSStorageStartNodeId
	for _, node := range nodes {
		if node.Type == nodeType {
			atoi, _ := strconv.Atoi(node.Id)
			if atoi > maxNodeId {
				maxNodeId = atoi
			}
		}
	}
	return maxNodeId + 1, nil
}

func GetChainTablesWithNode(adminCli *clientcomm.AdminCliConfig, nodeName string) ([]Chain, error) {
	output, err := adminCli.ListChains()
	if err != nil {
		klog.Infof("list nodes failed: %v", err)
		return nil, err
	}

	chains, err := ParseChainTable(output)
	if err != nil {
		klog.Infof("parse chain table failed: %v", err)
		return nil, err
	}

	nodeId, err := ParseNodeIdFromNodeName(adminCli, "STORAGE", nodeName)
	if err != nil {
		klog.Errorf("parse node id failed: %v", err)
		return nil, err
	}

	fileteredChains := make([]Chain, 0)
	for _, chain := range chains {
		for _, target := range chain.Targets {
			chainNodeId := target.TargetId[2:7]
			if chainNodeId == strconv.Itoa(nodeId) {
				fileteredChains = append(fileteredChains, chain)
			}
		}
	}

	return fileteredChains, nil
}

func GetTargetsWithNode(adminCli *clientcomm.AdminCliConfig, nodeName string) ([]Target, error) {
	output, err := adminCli.ListTargets()
	if err != nil {
		klog.Infof("list nodes failed: %v", err)
		return nil, err
	}

	targets, err := ParseTargets(output)
	if err != nil {
		klog.Infof("parse targets failed: %v", err)
		return nil, err
	}

	nodeId, err := ParseNodeIdFromNodeName(adminCli, "STORAGE", nodeName)
	if err != nil {
		klog.Errorf("parse node id failed: %v", err)
		return nil, err
	}

	fileteredTargets := make([]Target, 0)
	for _, target := range targets {
		targetNodeId := target.TargetId[2:7]
		if targetNodeId == strconv.Itoa(nodeId) {
			fileteredTargets = append(fileteredTargets, target)
		}
	}

	return fileteredTargets, nil
}

func (r *ThreeFsChainTableReconciler) GetChainTablesWithChainIdTargetId(adminCli *clientcomm.AdminCliConfig, chainids []string) ([]Chain, error) {
	output, err := adminCli.ListChains()
	if err != nil {
		klog.Infof("list nodes failed: %v", err)
		return nil, err
	}

	chains, err := ParseChainTable(output)
	if err != nil {
		klog.Infof("parse chain table failed: %v", err)
		return nil, err
	}

	chainidMaps := make(map[string]string)
	for _, key := range chainids {
		splits := strings.Split(key, "@")
		chainidMaps[splits[0]] = splits[1]
	}

	fileteredChains := make([]Chain, 0)
	for _, chain := range chains {
		if _, ok := chainidMaps[chain.ChainId]; ok {
			chain.Key = chainidMaps[chain.ChainId]
			fileteredChains = append(fileteredChains, chain)
		}
	}

	return fileteredChains, nil
}

func (r *ThreeFsChainTableReconciler) GetChainTablesWithChainId(adminCli *clientcomm.AdminCliConfig, chainids []string) ([]Chain, error) {
	output, err := adminCli.ListChains()
	if err != nil {
		klog.Infof("list nodes failed: %v", err)
		return nil, err
	}

	chains, err := ParseChainTable(output)
	if err != nil {
		klog.Infof("parse chain table failed: %v", err)
		return nil, err
	}

	chainidMaps := make(map[string]bool)
	for _, key := range chainids {
		chainidMaps[key] = true
	}

	fileteredChains := make([]Chain, 0)
	for _, chain := range chains {
		if _, ok := chainidMaps[chain.ChainId]; ok {
			fileteredChains = append(fileteredChains, chain)
		}
	}

	return fileteredChains, nil
}

func (r *ThreeFsChainTableReconciler) HandleProcessChains(adminCli *clientcomm.AdminCliConfig, chains []Chain, nodeName string) ([]string, error) {

	nodeId, err := ParseNodeIdFromNodeName(adminCli, "STORAGE", nodeName)
	if err != nil {
		klog.Errorf("parse node id failed: %v", err)
		return nil, err
	}

	chainids := make([]string, len(chains))
	for idx, chain := range chains {
		for _, target := range chain.Targets {
			chainNodeId := target.TargetId[2:7]
			if chainNodeId == strconv.Itoa(nodeId) {
				chainids[idx] = fmt.Sprintf("%s@%s", chain.ChainId, target.TargetId)
			}
		}
	}

	return chainids, nil
}

func (r *ThreeFsChainTableReconciler) UpdateProcessChains(chainids []string, chaintable *threefsv1.ThreeFsChainTable) error {

	originalObj := chaintable.DeepCopy()
	modififedObj := chaintable.DeepCopy()
	modififedObj.Status.ProcessChainIds = chainids
	if err := r.Client.Status().Patch(context.Background(), modififedObj, client.MergeFrom(originalObj)); err != nil {
		klog.Errorf("patch chain table status with processing chains failed: %v", err)
		return err
	}

	return nil
}

func (r *ThreeFsChainTableReconciler) UpdateExecTag(execute bool, chaintable *threefsv1.ThreeFsChainTable) error {

	originalObj := chaintable.DeepCopy()
	modififedObj := chaintable.DeepCopy()
	modififedObj.Status.Executed = execute
	if err := r.Client.Status().Patch(context.Background(), modififedObj, client.MergeFrom(originalObj)); err != nil {
		klog.Errorf("patch chain table status with execute tag failed: %v", err)
		return err
	}

	return nil
}

func (r *ThreeFsChainTableReconciler) UpdatePhase(phase string, chaintable *threefsv1.ThreeFsChainTable) error {

	originalObj := chaintable.DeepCopy()
	modififedObj := chaintable.DeepCopy()
	modififedObj.Status.Phase = phase
	if err := r.Client.Status().Patch(context.Background(), modififedObj, client.MergeFrom(originalObj)); err != nil {
		klog.Errorf("patch chain table status with phase failed: %v", err)
		return err
	}

	return nil
}

func (r *ThreeFsChainTableReconciler) CheckChainWithoutStatus(chains []Chain, status string) error {
	for _, chain := range chains {
		for _, target := range chain.Targets {
			if target.State == status {
				klog.Infof("chain %s target %s state is %s", chain.ChainId, target.TargetId, target.State)
				return fmt.Errorf("chain %s target %s state is %s", chain.ChainId, target.TargetId, target.State)
			}
		}
	}

	klog.Infof("chains is %s", status)
	return nil
}

func (r *ThreeFsChainTableReconciler) CheckChainWithoutStatusRelatedNode(adminCli *clientcomm.AdminCliConfig, chains []Chain, status, nodeName string) error {
	nodeId, err := ParseNodeIdFromNodeName(adminCli, "STORAGE", nodeName)
	if err != nil {
		klog.Errorf("parse node id failed: %v", err)
		return nil
	}
	for _, chain := range chains {
		for _, target := range chain.Targets {
			chainNodeId := target.TargetId[2:7]
			if strconv.Itoa(nodeId) != chainNodeId {
				continue
			}
			if target.State == status {
				klog.Infof("chain %s target %s state is %s", chain.ChainId, target.TargetId, target.State)
				return fmt.Errorf("chain %s target %s state is %s", chain.ChainId, target.TargetId, target.State)
			}
		}
	}

	klog.Infof("chains is %s", status)
	return nil
}

func (r *ThreeFsChainTableReconciler) CheckChainTargetWithNodeStatus(adminCli *clientcomm.AdminCliConfig, chains []Chain, nodeName, status string) int {

	nodeId, err := ParseNodeIdFromNodeName(adminCli, "STORAGE", nodeName)
	if err != nil {
		klog.Errorf("parse node id failed: %v", err)
		return 0
	}

	count := 0
	for _, chain := range chains {
		for _, target := range chain.Targets {
			if target.TargetId[2:7] != strconv.Itoa(nodeId) {
				continue
			}
			if target.State != status {
				count++
				klog.Infof("chain %s target %s state is %s", chain.ChainId, target.TargetId, target.State)
			}
		}
	}

	if count == 0 {
		return len(chains) - count
	}
	klog.Infof("chains is not all %s", status)
	return len(chains) - count
}

func (r *ThreeFsChainTableReconciler) CheckChainWithStatus(chains []Chain, status string) int {

	count := 0
	for _, chain := range chains {
		for _, target := range chain.Targets {
			if target.State != status {
				count++
				klog.Infof("chain %s target %s state is %s", chain.ChainId, target.TargetId, target.State)
				break
			}
		}
	}

	if count == 0 {
		return len(chains)
	}
	klog.Infof("chains is not all %s", status)
	return len(chains) - count
}

func (r *ThreeFsChainTableReconciler) OfflineTargetRelatedNode(adminCli *clientcomm.AdminCliConfig, chains []Chain, nodeName, token string) error {
	nodeId, err := ParseNodeIdFromNodeName(adminCli, "STORAGE", nodeName)
	if err != nil {
		klog.Errorf("parse node id failed: %v", err)
		return nil
	}
	for _, chain := range chains {
		for _, target := range chain.Targets {
			chainNodeId := target.TargetId[2:7]
			if chainNodeId == strconv.Itoa(nodeId) {
				if !strings.Contains(target.State, "OFFLINE") {
					if _, err := adminCli.OfflineTarget(token, strconv.Itoa(nodeId), target.TargetId); err != nil {
						klog.Errorf("offline target: nodeid(%d) target(%s) failed: %v", nodeId, target.TargetId, err)
						return err
					}
				}
				klog.Infof("offline target node %d target %s success", nodeId, target.TargetId)
			}
		}
	}
	klog.Infof("chains related to node(%s,%d) offlined", nodeName, nodeId)
	return nil
}

func (r *ThreeFsChainTableReconciler) DeleteTargetRelatedNode(adminCli *clientcomm.AdminCliConfig, chains []Chain, nodeName, token string) error {
	nodeId, err := ParseNodeIdFromNodeName(adminCli, "STORAGE", nodeName)
	if err != nil {
		klog.Errorf("parse node id failed: %v", err)
		return nil
	}
	for _, chain := range chains {
		for _, target := range chain.Targets {
			chainNodeId := target.TargetId[2:7]
			if chainNodeId == strconv.Itoa(nodeId) {
				if _, err := adminCli.UpdateChain(token, "remove", chain.ChainId, target.TargetId); err != nil {
					klog.Errorf("delete chain %s target %s failed: %v", chain.ChainId, target.TargetId, err)
					return err
				}
				klog.Infof("delete chain %s target %s success", chain.ChainId, target.TargetId)
			}
		}
	}
	klog.Infof("chains related to node(%s,%d) removed", nodeName, nodeId)
	return nil
}

func (r *ThreeFsChainTableReconciler) AddTargetRelatedNode(adminCli *clientcomm.AdminCliConfig, chainids []string, oldnodeName, newnodeName, token string) error {

	oldnodeId, err := ParseNodeIdFromNodeName(adminCli, "STORAGE", oldnodeName)
	if err != nil {
		klog.Errorf("parse old node id failed: %v", err)
		return nil
	}

	nodeId, err := ParseNodeIdFromNodeName(adminCli, "STORAGE", newnodeName)
	if err != nil {
		klog.Errorf("parse new node id failed: %v", err)
		return nil
	}

	for _, chain := range chainids {
		splits := strings.Split(chain, "@")
		chainId := splits[0]
		targetId := splits[1]
		newTargetId := strings.Replace(targetId, strconv.Itoa(oldnodeId), strconv.Itoa(nodeId), 1)
		if _, err := adminCli.UpdateChain(token, "add", chainId, newTargetId); err != nil {
			klog.Errorf("add chain %s target %s failed: %v", chainId, targetId, err)
			return err
		}
		klog.Infof("add chain %s target %s success", chainId, targetId)
	}

	klog.Infof("chains related to node(%s,%d) all added", newnodeName, nodeId)
	return nil
}

func (r *ThreeFsChainTableReconciler) CreateTargetTmpFile(adminCli *clientcomm.AdminCliConfig, chainids []string, oldNodeName, newNodeName string) (string, error) {

	oldnodeId, err := ParseNodeIdFromNodeName(adminCli, "STORAGE", oldNodeName)
	if err != nil {
		klog.Errorf("parse old node id failed: %v", err)
		return "", err
	}

	nodeId, err := ParseNodeIdFromNodeName(adminCli, "STORAGE", newNodeName)
	if err != nil {
		klog.Errorf("parse new node id failed: %v", err)
		return "", err
	}

	tmpFile, err := os.CreateTemp("/tmp", "create_target_cmd_*.txt")
	if err != nil {
		klog.Errorf("create tmp file failed: %v", err)
		return "", err
	}
	defer tmpFile.Close()

	writer := bufio.NewWriter(tmpFile)
	defer writer.Flush()
	for _, chain := range chainids {
		splits := strings.Split(chain, "@")
		chainId := splits[0]
		targetId := splits[1]
		diskIndex := targetId[7:10]
		diskIndexInt, err := strconv.Atoi(diskIndex)
		if err != nil {
			klog.Errorf("parse disk index failed: %v", err)
			return "", err
		}
		newTargetId := strings.Replace(targetId, strconv.Itoa(oldnodeId), strconv.Itoa(nodeId), 1)
		line := fmt.Sprintf("create-target --node-id %d --disk-index %d --target-id %s --chain-id %s  --use-new-chunk-engine\n", nodeId, diskIndexInt-1, newTargetId, chainId)
		klog.Infof("create-target line: %s", line)
		writer.WriteString(line)
	}

	return tmpFile.Name(), nil
}

func (r *ThreeFsChainTableReconciler) CreateTargetRelatedNode(adminCli *clientcomm.AdminCliConfig, oldchains []Chain, newnodeName, oldnodeName string) []Chain {
	newChains := make([]Chain, 0)
	oldnodeId, err := ParseNodeIdFromNodeName(adminCli, "STORAGE", oldnodeName)
	if err != nil {
		klog.Errorf("parse node id failed: %v", err)
		return nil
	}
	newnodeId, err := ParseNodeIdFromNodeName(adminCli, "STORAGE", newnodeName)
	if err != nil {
		klog.Errorf("parse node id failed: %v", err)
		return nil
	}
	for _, chain := range oldchains {
		for _, target := range chain.Targets {
			if target.TargetId[2:7] == oldnodeName {
				newChain := Chain{
					ChainId: chain.ChainId,
					Targets: make([]Target, 0),
				}
				newtargetid := strings.Replace(target.TargetId, strconv.Itoa(oldnodeId), strconv.Itoa(newnodeId), 1)
				newChain.Targets = append(newChain.Targets, Target{
					TargetId: newtargetid,
				})
				newChains = append(newChains, newChain)
			}
		}
	}

	return newChains
}

func ParseChainTableFromFile(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("can't open file %s: %+v", filePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read csv file failed: %+v", err)
	}

	if len(records) == 0 {
		return []string{}, nil
	}

	var result []string
	for _, row := range records[1:] {
		if len(row) == 0 {
			continue
		}
		result = append(result, row[0])
	}

	return result, nil
}

func ParseStartNodeId(rclient client.Client, nodeName []string, tfsc threefsv1.ThreeFsCluster) (int, error) {
	storageEnvConfig := &corev1.ConfigMap{}
	if err := rclient.Get(context.Background(), client.ObjectKey{Name: storage.GetStorageDeployName(tfsc.Name), Namespace: tfsc.Namespace}, storageEnvConfig); err != nil {
		klog.Errorf("get storage env config failed, err: %+v", err)
		return 0, err
	}

	startIdx := 10 * constant.ThreeFSStorageStartNodeId
	for _, key := range nodeName {
		parsedKey := strings.ReplaceAll(key, "-", "_")
		parsedKey = strings.ReplaceAll(parsedKey, ".", "_")
		val, ok := storageEnvConfig.Data[parsedKey]
		if !ok {
			return 0, fmt.Errorf("node %s not found", key)
		}
		nodeId, err := strconv.Atoi(val)
		if err != nil {
			return 0, fmt.Errorf("node %s node id %s is not a number", key, val)
		}
		if nodeId < startIdx {
			startIdx = nodeId
		}
	}
	return startIdx, nil
}

func ParseChainId(chainid string) (int, int, error) {
	atoi, err := strconv.Atoi(chainid)
	if err != nil {
		klog.Errorf("chainid %s is not a number", chainid)
		return -1, -1, err
	}
	chaindIdx := atoi % 100000
	diskIdx := (atoi / 100000) % 1000
	return chaindIdx, diskIdx, nil
}

func ParseMaxChainIdForEachDisk(adminCli *clientcomm.AdminCliConfig) (map[int]int, error) {
	maps := make(map[int]int)
	output, err := adminCli.ListChains()
	if err != nil {
		klog.Infof("list nodes failed: %v", err)
		return maps, err
	}

	chains, err := ParseChainTable(output)
	if err != nil {
		klog.Infof("parse chain table failed: %v", err)
		return maps, err
	}

	for _, chain := range chains {
		chainId := chain.ChainId
		chaindIdx, diskIdx, err := ParseChainId(chainId)
		if err != nil {
			klog.Errorf("parse chain id failed: %v", err)
			return maps, err
		}
		if _, ok := maps[diskIdx]; !ok {
			maps[diskIdx] = 0
		}
		if maps[diskIdx] < chaindIdx {
			maps[diskIdx] = chaindIdx
		}
	}
	return maps, nil
}

func UpdateChainFile(chainPath string, maps map[int]int) (string, error) {
	file, err := os.Open(chainPath)
	if err != nil {
		klog.Errorf("open file %s failed: %+v", chainPath, err)
		return "", err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		klog.Errorf("read csv file failed: %+v", err)
		return "", err
	}

	if len(records) < 2 {
		klog.Errorf("chain table is empty")
		return "", err
	}

	for i := 1; i < len(records); i++ {
		numStr := records[i][0]
		num, err := strconv.Atoi(numStr)
		if err != nil {
			klog.Errorf("parse chain id failed: %v", err)
			return "", err
		}
		_, diskIdx, err := ParseChainId(numStr)
		newNum := num + maps[diskIdx]
		records[i][0] = strconv.Itoa(newNum)
	}

	outFile, err := os.CreateTemp("/output", "chains_updated_*.csv")
	if err != nil {
		klog.Errorf("create file %s failed: %+v", outFile.Name(), err)
		return "", err
	}
	if err != nil {
		klog.Errorf("create file %s failed: %+v", outFile.Name(), err)
		return "", err
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	if err := writer.WriteAll(records); err != nil {
		klog.Errorf("write csv file failed: %+v", err)
		return "", err
	}

	klog.Infof("update chain file success, outputfile: %s", outFile.Name())
	return outFile.Name(), nil
}

func UpdateChainTableFile(chaintablePath string, maps map[int]int) (string, error) {
	file, err := os.Open(chaintablePath)
	if err != nil {
		klog.Errorf("open file %s failed: %+v", chaintablePath, err)
		return "", err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		klog.Errorf("read csv file failed: %+v", err)
		return "", err
	}

	if len(records) < 2 {
		klog.Errorf("chain table is empty")
		return "", err
	}

	for i := 1; i < len(records); i++ {
		numStr := records[i][0]
		num, err := strconv.Atoi(numStr)
		if err != nil {
			klog.Errorf("parse chain id failed: %v", err)
			return "", err
		}
		_, diskIdx, err := ParseChainId(numStr)
		newNum := num + maps[diskIdx]
		records[i][0] = strconv.Itoa(newNum)
	}

	outFile, err := os.CreateTemp("/output", "chain_table_updated_*.csv")
	if err != nil {
		klog.Errorf("create file %s failed: %+v", outFile.Name(), err)
		return "", err
	}
	if err != nil {
		klog.Errorf("create file %s failed: %+v", outFile.Name(), err)
		return "", err
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	if err := writer.WriteAll(records); err != nil {
		klog.Errorf("write csv file failed: %+v", err)
		return "", err
	}

	klog.Infof("update chain table file success, outputfile: %s", outFile.Name())
	return outFile.Name(), nil
}

func UpdateTargetFile(targetPath string, maps map[int]int) (string, error) {

	file, err := os.Open(targetPath)
	if err != nil {
		klog.Errorf("open file %s failed: %+v", targetPath, err)
		return "", err
	}
	defer file.Close()

	outFile, err := os.CreateTemp("/output", "target_updated_*.txt")
	if err != nil {
		klog.Errorf("create file %s failed: %+v", outFile.Name(), err)
		return "", err
	}
	defer outFile.Close()

	re := regexp.MustCompile(`--chain-id\s+(\d+)`)

	scanner := bufio.NewScanner(file)
	writer := bufio.NewWriter(outFile)

	for scanner.Scan() {
		line := scanner.Text()
		// find chain-id args
		matches := re.FindStringSubmatch(line)
		if len(matches) < 2 {
			_, _ = writer.WriteString(line + "\n")
			continue
		}

		oldChainID := matches[1]
		chainID, err := strconv.Atoi(oldChainID)
		if err != nil {
			klog.Errorf("parse chain-id err: %v", err)
			return "", err
		}

		_, diskIdx, err := ParseChainId(oldChainID)
		if err != nil {
			klog.Errorf("parse chain-id err: %v", err)
			return "", err
		}
		newChainIdx := chainID + maps[diskIdx]
		newLine := strings.Replace(line, oldChainID, strconv.Itoa(newChainIdx), 1)
		_, _ = writer.WriteString(newLine + "\n")
	}

	if err := writer.Flush(); err != nil {
		klog.Errorf("flush file failed: %+v", err)
		return "", err
	}

	klog.Infof("update target file success, outputfile: %s", outFile.Name())
	return outFile.Name(), nil
}

func UpdateChainIdWithExistingChain(targetPath, chainPath, ChaintablePath string, maps map[int]int) (string, string, string, error) {
	newtargetPath, err := UpdateTargetFile(targetPath, maps)
	if err != nil {
		return "", "", "", err
	}
	newchainPath, err := UpdateChainFile(chainPath, maps)
	if err != nil {
		return "", "", "", err
	}
	newchaintablePath, err := UpdateChainTableFile(ChaintablePath, maps)
	if err != nil {
		return "", "", "", err
	}
	return newtargetPath, newchainPath, newchaintablePath, nil
}

func ParseNodeIdWihtPlainName(adminCli *clientcomm.AdminCliConfig, nodeName, nodeType string) string {
	output, err := adminCli.ListNodes()
	if err != nil {
		klog.Infof("list nodes failed: %v", err)
		return ""
	}
	nodes, err := ParseNodeTable(output)
	if err != nil {
		klog.Infof("parse node table failed: %v", err)
		return ""
	}

	for _, node := range nodes {
		if node.Type != nodeType {
			continue
		}
		if node.Hostname == utils.TranslatePlainNodeName3fs(nodeName) {
			return node.Id
		}
	}
	return ""
}

func ParsePlainNameWithNodeId(rclient client.Client, nodeId string) string {

	nodeList := &corev1.NodeList{}
	if err := rclient.List(context.Background(), nodeList); err != nil {
		klog.Errorf("list node failed: %v", err)
		return ""
	}
	for _, node := range nodeList.Items {
		if nodeId == utils.TranslatePlainNodeName3fs(node.Name) {
			return node.Name
		}
	}
	return ""
}

func IsAutoStorageReplace(tfsct *threefsv1.ThreeFsChainTable) bool {
	if tfsct.Labels != nil && tfsct.Labels[constant.ThreeFSAutoReplaceLabel] == "true" {
		return true
	}
	return false
}
