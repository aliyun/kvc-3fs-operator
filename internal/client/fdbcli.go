package clientcomm

import (
	"context"
	"fmt"
	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/api/v1beta2"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"strings"
	"time"
)

type FdbcliConfig struct {
	ConfigPath     string         `json:"config_path"`
	ReplicaNum     int            `json:"replica_num"`
	CoordinatorNum int            `json:"coordinator_num"`
	RestClient     rest.Interface `json:"rest_client"`
}

func NewFdbCliConfig(configPath string, replicaNum, coordinatorNum int, restClient rest.Interface) *FdbcliConfig {
	return &FdbcliConfig{
		ConfigPath:     configPath,
		ReplicaNum:     replicaNum,
		CoordinatorNum: coordinatorNum,
		RestClient:     restClient,
	}
}

func (fc *FdbcliConfig) CreateNewDb() (string, string, error) {
	maps := map[int]string{
		1: "single",
		2: "double",
		3: "triple",
	}

	checkCommand := CommandRunner{
		Command: "fdbcli",
		Args: []string{
			"-C", fc.ConfigPath,
			"--exec",
			fmt.Sprintf("configure new %s ssd", maps[fc.ReplicaNum]),
		},
		Timeout: 30 * time.Second,
	}
	return checkCommand.Exec(context.Background())
}

func (fc *FdbcliConfig) ConfigureCoordinator() (string, string, error) {
	autoCommand := CommandRunner{
		Command: "fdbcli",
		Args: []string{
			"-C", fc.ConfigPath,
			"--exec",
			"coordinators auto",
		},
		Timeout: 10 * time.Second,
	}
	return autoCommand.Exec(context.Background())
}

func (fc *FdbcliConfig) CheckFdbCluster() (string, string, error) {
	checkCommand := CommandRunner{
		Command: "fdbcli",
		Args: []string{
			"-C", fc.ConfigPath,
			"--exec",
			"status minimal",
		},
		Timeout: 30 * time.Second,
	}
	return checkCommand.Exec(context.Background())
}

func (fc *FdbcliConfig) GetFdbDetails() (string, string, error) {
	checkOutput, _, _ := fc.CheckFdbCluster()
	if !strings.Contains(checkOutput, "The database is available") {
		return "", "", fmt.Errorf("fdb cluster is not available")
	}
	checkCommand := CommandRunner{
		Command: "fdbcli",
		Args: []string{
			"-C", fc.ConfigPath,
			"--exec",
			"status details",
		},
		Timeout: 30 * time.Second,
	}
	return checkCommand.Exec(context.Background())
}

func (fc *FdbcliConfig) GetFdbJson() (string, string, error) {
	checkOutput, _, _ := fc.CheckFdbCluster()
	if !strings.Contains(checkOutput, "The database is available") {
		return "", "", fmt.Errorf("fdb cluster is not available")
	}
	checkCommand := CommandRunner{
		Command: "fdbcli",
		Args: []string{
			"-C", fc.ConfigPath,
			"--exec",
			"status json",
		},
		Timeout: 30 * time.Second,
	}
	return checkCommand.Exec(context.Background())
}

func (fc *FdbcliConfig) GetRemoteConfigContent() (string, error) {
	checkOutput, _, _ := fc.CheckFdbCluster()
	if !strings.Contains(checkOutput, "The database is available") {
		return "", fmt.Errorf("fdb cluster is not available")
	}
	details, err := fc.ParseStatusOutput()
	if err != nil {
		klog.Errorf("parse status output failed, err: %+v", err)
		return "", err
	}
	return details.Cluster.ConnectionString, nil
}

func (fc *FdbcliConfig) ParseStatusOutput() (*fdbv1beta2.FoundationDBStatus, error) {

	rawStatus, _, err := fc.GetFdbJson()
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(rawStatus, "\r\nWARNING") {
		rawStatus = strings.TrimPrefix(
			rawStatus,
			"\r\nWARNING: Long delay (Ctrl-C to interrupt)\r\n",
		)
	}

	status := &fdbv1beta2.FoundationDBStatus{}
	err = json.Unmarshal([]byte(rawStatus), status)

	if err != nil {
		klog.Errorf("could not parse result of status json %w (unparseable JSON: %s)    ", err, rawStatus)
		return nil, fmt.Errorf(
			"could not parse result of status json %w (unparseable JSON: %s)	",
			err,
			rawStatus,
		)
	}

	return status, nil
}

func (fc *FdbcliConfig) InitFdbCluster() error {
	checkOutput, _, err := fc.CheckFdbCluster()
	if !strings.Contains(checkOutput, "The database is available") {
		if output, _, err := fc.CreateNewDb(); err != nil {
			if !strings.Contains(output, "Database already exists") {
				klog.Errorf("create new db failed, err: %+v", err)
				return err
			}
		}
	}

	time.Sleep(5 * time.Second)
	if _, _, err = fc.ConfigureCoordinator(); err != nil {
		klog.Errorf("auto coordinator failed, err: %+v", err)
		return err
	}
	return nil
}
