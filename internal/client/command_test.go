package clientcomm

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCommandRunner_Exec(t *testing.T) {
	command := CommandRunner{
		Command: "ls",
		Args: []string{
			"/Users/wanna/GolandProjects/vcns-fs-control-plane/docker",
		},
		Timeout: 0,
	}
	stdout, stderr, err := command.Exec(context.Background())
	assert.Nil(t, err)
	fmt.Println(stdout, stderr)
}
