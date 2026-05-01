//go:build !unix

package postgres

import "os/exec"

func configureCommandContextCancel(*exec.Cmd) {}
