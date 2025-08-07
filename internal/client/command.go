package clientcomm

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"k8s.io/klog/v2"
	"os/exec"
	"strings"
	"time"
)

type CommandRunner struct {
	Command string        `json:"command"`
	Args    []string      `json:"args"`
	Timeout time.Duration `json:"timeout"`
}

func (r *CommandRunner) Exec(ctx context.Context) (string, string, error) {

	outputErr := func(cmdStr, stdoutStr, strerrStr string, err error) (string, string, error) {
		klog.Errorf("exec command %s failed, stdout: %s, stderr: %s, err: %+v", cmdStr, stdoutStr, strerrStr, err)
		return stdoutStr, strerrStr, err
	}
	cmdStr := fmt.Sprintf("%s %s", r.Command, strings.Join(r.Args, " "))
	cmd := exec.Command(r.Command, r.Args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := r.runCtx(ctx, cmd)
	if err != nil {
		klog.Errorf("exec command %s failed, err: %+v", cmdStr, err)
		return outputErr(cmdStr, string(stdout.Bytes()), string(stderr.Bytes()), err)
	}

	// this command is too long
	if !strings.Contains(cmdStr, "status json") && !strings.Contains(cmdStr, "list-targets") {
		klog.Infof("Output of %s: %s", cmdStr, string(stdout.Bytes()))
	}

	return string(stdout.Bytes()), "", nil
}

func (r *CommandRunner) runCtx(ctx context.Context, cmd *exec.Cmd) error {

	startTime := time.Now()
	timeCtx, _ := context.WithTimeout(context.Background(), r.Timeout)
	maxExitTimeout := r.Timeout

	if err := cmd.Start(); err != nil {
		klog.Errorf("Failed to start command: %s", err)
		return err
	}

	done := make(chan error)
	go func() {
		done <- cmd.Wait()
		close(done)
	}()

	select {
	case <-timeCtx.Done():
		if err := cmd.Process.Kill(); err != nil {
			klog.Errorf("Failed to kill command: %s", err)
			return err
		}
		return errors.Errorf("wait process to exit timeout after %s", maxExitTimeout)
	case <-ctx.Done():
		d := time.Since(startTime)
		if err := cmd.Process.Kill(); err != nil {
			klog.Errorf("Failed to kill command: %s", err)
			return err
		}
		select {
		case <-done:
		case <-time.After(maxExitTimeout):
			klog.Warningf("Wait for command to exit timeout: %s", maxExitTimeout)
			return errors.Errorf("wait process to exit timeout after %s", maxExitTimeout)
		}
		if b, ok := cmd.Stdout.(*bytes.Buffer); ok {
			klog.Warningf("Process was killed after %v: %s %s\nstdout: %v\nstderr: %v",
				d.Round(100*time.Millisecond), cmd.Path, cmd.Args, b.String(), cmd.Stderr)
		} else {
			// Reduce time accuracy, avoid frequent log changes that affect logger rate limit
			klog.Warningf("Process was killed after %v: %s %s\nstdout: %v\nstderr: %v",
				d.Round(100*time.Millisecond), cmd.Path, cmd.Args, cmd.Stdout, cmd.Stderr)
		}
		return ctx.Err()
	case err := <-done:
		return err
	}
}
