package clientcomm

import (
	"context"
	"fmt"
	"github.com/aliyun/kvc-3fs-operator/internal/constant"
	"k8s.io/klog/v2"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type AdminCliConfig struct {
	MgmtdServerAddresses string `json:"mgmtd_server_addresses"`
	ConfigPath           string `json:"config_path"`
}

func NewAdminCli(addresses, configPath string) *AdminCliConfig {
	return &AdminCliConfig{
		MgmtdServerAddresses: addresses,
		ConfigPath:           configPath,
	}
}

func (ac *AdminCliConfig) InitCluster(chainTableId string, stripeSize, chunkSize int) error {
	command := CommandRunner{
		Command: "/admin_cli",
		Args: []string{
			"-cfg", ac.ConfigPath,
			"--",
			fmt.Sprintf("init-cluster --mgmtd %s %s %d %d", filepath.Join(constant.DefaultConfigPath, constant.ThreeFSMgmtdMain), chainTableId, chunkSize, stripeSize),
		},
		Timeout: 10 * time.Second,
	}
	output, errStr, err := command.Exec(context.Background())
	klog.Infof("init cluster output: %s", output)
	if strings.Contains(output, "Config for MGMTD existed") || strings.Contains(errStr, "Config for MGMTD existed") {
		return nil
	}
	return err
}

func (ac *AdminCliConfig) UploadMainConfig(componentType, configPath string) error {
	command := CommandRunner{
		Command: "/admin_cli",
		Args: []string{
			"-cfg", ac.ConfigPath,
			"--config.mgmtd_client.mgmtd_server_addresses", fmt.Sprintf("%s", ac.MgmtdServerAddresses),
			"--",
			fmt.Sprintf("set-config --type %s --file %s", componentType, configPath),
		},
		Timeout: 10 * time.Second,
	}
	output, _, err := command.Exec(context.Background())
	klog.Infof("upload main config output: %s", output)
	return err
}

func (ac *AdminCliConfig) UserAdd() (string, error) {
	command := CommandRunner{
		Command: "/admin_cli",
		Args: []string{
			"-cfg", ac.ConfigPath,
			"--config.mgmtd_client.mgmtd_server_addresses", fmt.Sprintf("%s", ac.MgmtdServerAddresses),
			"--",
			fmt.Sprintf("user-add --root --admin 0 root"),
		},
		Timeout: 10 * time.Second,
	}
	output, _, err := command.Exec(context.Background())
	klog.Infof("user-add output: %s", output)
	return output, err
}

func (ac *AdminCliConfig) UnregisterNode(nodeId, nodeType string) error {
	command := CommandRunner{
		Command: "/admin_cli",
		Args: []string{
			"-cfg", ac.ConfigPath,
			"--config.mgmtd_client.mgmtd_server_addresses", fmt.Sprintf("%s", ac.MgmtdServerAddresses),
			"--",
			fmt.Sprintf("unregister-node %s %s", nodeId, nodeType),
		},
		Timeout: 10 * time.Second,
	}
	output, _, err := command.Exec(context.Background())
	if strings.Contains(output, "error") {
		return fmt.Errorf("unregister node failed, err: %s", output)
	}
	return err
}

func (ac *AdminCliConfig) CreateTarget(token, filePath string) error {
	command := CommandRunner{
		Command: "bash",
		Args: []string{
			"-c",
			fmt.Sprintf("/admin_cli -cfg %s --config.mgmtd_client.mgmtd_server_addresses '%s' --config.user_info.token %s < %s", ac.ConfigPath, ac.MgmtdServerAddresses, token, filePath),
		},
		Timeout: 10 * time.Second,
	}
	output, _, err := command.Exec(context.Background())
	if strings.Contains(output, "error") {
		return fmt.Errorf("create target failed, err: %s", output)
	}
	return err
}

func (ac *AdminCliConfig) DumpChainTable(token, chaintablePath string) error {
	os.MkdirAll(filepath.Dir(chaintablePath), 0755)
	if _, err := os.Stat(chaintablePath); err == nil {
		if err := os.Remove(chaintablePath); err != nil {
			return fmt.Errorf("delete file: %+v", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat file err: %+v", err)
	}
	file, err := os.Create(chaintablePath)
	if err != nil {
		return fmt.Errorf("create file: %+v", err)
	}
	defer file.Close()

	command := CommandRunner{
		Command: "/admin_cli",
		Args: []string{
			"-cfg", ac.ConfigPath,
			"--config.mgmtd_client.mgmtd_server_addresses", fmt.Sprintf("%s", ac.MgmtdServerAddresses),
			"--config.user_info.token", token,
			"--",
			fmt.Sprintf("dump-chain-table 1 %s", chaintablePath),
		},
		Timeout: 10 * time.Second,
	}
	_, _, err = command.Exec(context.Background())
	return err
}

func (ac *AdminCliConfig) UploadChains(token, chainsPath string) error {
	command := CommandRunner{
		Command: "/admin_cli",
		Args: []string{
			"-cfg", ac.ConfigPath,
			"--config.mgmtd_client.mgmtd_server_addresses", fmt.Sprintf("%s", ac.MgmtdServerAddresses),
			"--config.user_info.token", token,
			"--",
			fmt.Sprintf("upload-chains %s", chainsPath),
		},
		Timeout: 10 * time.Second,
	}
	_, _, err := command.Exec(context.Background())
	return err
}

func (ac *AdminCliConfig) DumpChains(token, chainPath string) error {
	os.MkdirAll(filepath.Dir(chainPath), 0755)
	command := CommandRunner{
		Command: "/admin_cli",
		Args: []string{
			"-cfg", ac.ConfigPath,
			"--config.mgmtd_client.mgmtd_server_addresses", fmt.Sprintf("%s", ac.MgmtdServerAddresses),
			"--config.user_info.token", token,
			"--",
			fmt.Sprintf("dump-chains %s", chainPath),
		},
		Timeout: 10 * time.Second,
	}
	_, _, err := command.Exec(context.Background())
	return err
}

func (ac *AdminCliConfig) UploadChainTable(token, chaintablePath string) error {
	command := CommandRunner{
		Command: "/admin_cli",
		Args: []string{
			"-cfg", ac.ConfigPath,
			"--config.mgmtd_client.mgmtd_server_addresses", fmt.Sprintf("%s", ac.MgmtdServerAddresses),
			"--config.user_info.token", token,
			"--",
			fmt.Sprintf("upload-chain-table --desc stage 1 %s", chaintablePath),
		},
		Timeout: 10 * time.Second,
	}
	_, _, err := command.Exec(context.Background())
	return err
}

func (ac *AdminCliConfig) ListNodes() (string, error) {
	command := CommandRunner{
		Command: "/admin_cli",
		Args: []string{
			"-cfg", ac.ConfigPath,
			"--config.mgmtd_client.mgmtd_server_addresses", fmt.Sprintf("%s", ac.MgmtdServerAddresses),
			"--",
			"list-nodes",
		},
		Timeout: 10 * time.Second,
	}
	output, _, err := command.Exec(context.Background())
	if strings.Contains(output, "error") {
		return output, fmt.Errorf("list nodes failed, err: %s", output)
	}
	return output, err
}

func (ac *AdminCliConfig) ListTargets() (string, error) {
	command := CommandRunner{
		Command: "/admin_cli",
		Args: []string{
			"-cfg", ac.ConfigPath,
			"--config.mgmtd_client.mgmtd_server_addresses", fmt.Sprintf("%s", ac.MgmtdServerAddresses),
			"--",
			"list-targets",
		},
		Timeout: 10 * time.Second,
	}
	output, _, err := command.Exec(context.Background())
	//klog.Infof("list-targets output: %s", output)
	if strings.Contains(output, "error") {
		return output, fmt.Errorf("list targets failed, err: %s", output)
	}
	return output, err
}

func (ac *AdminCliConfig) ListChains() (string, error) {
	command := CommandRunner{
		Command: "/admin_cli",
		Args: []string{
			"-cfg", ac.ConfigPath,
			"--config.mgmtd_client.mgmtd_server_addresses", fmt.Sprintf("%s", ac.MgmtdServerAddresses),
			"--",
			"list-chains",
		},
		Timeout: 10 * time.Second,
	}
	output, _, err := command.Exec(context.Background())
	if strings.Contains(output, "error") {
		return output, fmt.Errorf("list chains failed, err: %s", output)
	}
	return output, err
}

func (ac *AdminCliConfig) UpdateChain(token, updateType, chainId, targetId string) (string, error) {
	command := CommandRunner{
		Command: "/admin_cli",
		Args: []string{
			"-cfg", ac.ConfigPath,
			"--config.mgmtd_client.mgmtd_server_addresses", fmt.Sprintf("%s", ac.MgmtdServerAddresses),
			"--config.user_info.token", token,
			"--",
			fmt.Sprintf("update-chain --mode %s %s %s", updateType, chainId, targetId),
		},
		Timeout: 10 * time.Second,
	}
	output, _, err := command.Exec(context.Background())
	klog.Infof("update-chain output: %s", output)
	if strings.Contains(output, "TargetExisted") {
		return output, nil
	}
	return output, err
}

func (ac *AdminCliConfig) OfflineTarget(token, nodeId, targetId string) (string, error) {
	command := CommandRunner{
		Command: "/admin_cli",
		Args: []string{
			"-cfg", ac.ConfigPath,
			"--config.mgmtd_client.mgmtd_server_addresses", fmt.Sprintf("%s", ac.MgmtdServerAddresses),
			"--config.user_info.token", token,
			"--",
			fmt.Sprintf("offline-target --node-id %s --target-id %s", nodeId, targetId),
		},
		Timeout: 10 * time.Second,
	}
	output, _, err := command.Exec(context.Background())
	klog.Infof("offline-target output: %s", output)
	if strings.Contains(output, "target is already offline") {
		return output, nil
	}
	if strings.Contains(output, "error") {
		return output, fmt.Errorf("offline target failed, err: %s", output)
	}
	return output, err
}
