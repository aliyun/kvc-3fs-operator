package clientcomm

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
)

func TestAdminCliConfig_InitCluster(t *testing.T) {
	admincli := NewAdminCli("127.0.0.1:8080", "/opt/3fs/etc/admin_cli.toml")
	err := admincli.InitCluster("1", 1048576, 16)
	assert.Nil(t, err)
}

func TestNewAdminCli(t *testing.T) {
	targetRegex := regexp.MustCompile(`^(\d+)\((\S+-\S+)\)$`)
	matches := targetRegex.FindStringSubmatch("101000300319(SERVING-UPTODATE)")
	fmt.Printf("matches: %v", matches)
}
