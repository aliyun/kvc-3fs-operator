package clientcomm

import (
	"context"
	"fmt"
	threefsv1 "github.com/aliyun/kvc-3fs-operator/api/v1"
	"github.com/aliyun/kvc-3fs-operator/internal/constant"
	"github.com/aliyun/kvc-3fs-operator/internal/utils"
	"os"
	"strconv"
	"time"
)

func CreateDataPlacementRule(nodes []string, threefsCluster *threefsv1.ThreeFsCluster, nodeidStart int) error {
	os.RemoveAll("/output")
	dataCommand := &CommandRunner{
		Command: "python3",
		Args: []string{
			"/opt/3fs/data_placement/data_placement.py",
			"-ql", "-relax", "-type", "CR",
			"--num_nodes", strconv.Itoa(len(nodes)),
			"--replication_factor", strconv.Itoa(threefsCluster.Spec.Storage.Replica),
			"--min_targets_per_disk", strconv.Itoa(threefsCluster.Spec.Storage.TargetPerDisk),
		},
		Timeout: 10 * time.Minute,
	}
	_, _, err := dataCommand.Exec(context.Background())
	if err != nil {
		return fmt.Errorf("run data_placement.py failed: %s", err)
	}
	var dataPlacementDir string
	matchfile, err := utils.GetPrefixFile("/output", "DataPlacementModel")
	if err != nil || matchfile == nil || len(matchfile) > 1 {
		return fmt.Errorf("get data_placement file failed: %+v", err)
	}
	dataPlacementDir = matchfile[0]

	genCommand := &CommandRunner{
		Command: "python3",
		Args: []string{
			"/opt/3fs/data_placement/gen_chain_table.py",
			"--chain_table_type", "CR",
			"--node_id_begin", strconv.Itoa(nodeidStart),
			"--node_id_end", strconv.Itoa(nodeidStart - 1 + len(nodes)),
			"--num_disks_per_node", strconv.Itoa(len(threefsCluster.Spec.Storage.TargetPaths)),
			"--num_targets_per_disk", strconv.Itoa(threefsCluster.Spec.Storage.TargetPerDisk),
			"--target_id_prefix", strconv.Itoa(constant.ThreeFSTargetIDPrefix),
			"--chain_id_prefix", strconv.Itoa(constant.ThreeFSChainIDPrefix),
			"--incidence_matrix_path", fmt.Sprintf("%s/incidence_matrix.pickle", dataPlacementDir),
		},
		Timeout: 60 * time.Second,
	}
	_, _, err = genCommand.Exec(context.Background())
	if err != nil {
		return fmt.Errorf("run gen_chain_table.py failed: %s", err)
	}
	return nil
}
