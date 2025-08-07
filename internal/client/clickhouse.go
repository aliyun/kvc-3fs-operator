package clientcomm

import (
	"context"
	"fmt"
	"time"
)

func CheckClickhouseReady(port int, svc, user, password string) bool {
	checkCommand := CommandRunner{
		Command: "bash",
		Args: []string{
			"-c",
			fmt.Sprintf("clickhouse-client -h %s --port %d --user %s --password \"%s\" --query \"SELECT 1\"", svc, port, user, password),
		},
		Timeout: 10 * time.Second,
	}
	_, _, err := checkCommand.Exec(context.Background())
	return err == nil
}

func ExecuteSql(port int, svc, user, passwd, sqlPath string) error {
	checkCommand := CommandRunner{
		Command: "bash",
		Args: []string{
			"-c",
			fmt.Sprintf("clickhouse-client -h %s --port %d --user %s --password '%s' -n < %s", svc, port, user, passwd, sqlPath),
		},
		Timeout: 10 * time.Second,
	}
	_, _, err := checkCommand.Exec(context.Background())
	return err
}
