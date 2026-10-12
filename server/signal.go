// Copyright 2012-2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !windows && !wasm

package server

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

var processName = "nats-server"

// SetProcessName allows to change the expected name of the process.
func SetProcessName(name string) {
	processName = name
}

// Signal Handling
func (s *Server) handleSignals() {
	if s.getOpts().NoSigs {
		return
	}
	c := make(chan os.Signal, 1)

	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGHUP)

	go func() {
		for {
			select {
			case sig := <-c:
				s.handleSignal(sig)
			case <-s.quitCh:
				return
			}
		}
	}()
}

func (s *Server) handleSignal(sig os.Signal) {
	s.Noticef("Trapped %q signal", sig)
	switch sig {
	case syscall.SIGINT:
		s.Shutdown()
		s.WaitForShutdown()
		os.Exit(0)
	case syscall.SIGTERM:
		// Shutdown unless graceful shutdown already in progress.
		s.mu.Lock()
		ldm := s.ldm
		s.mu.Unlock()

		if !ldm {
			s.Shutdown()
			s.WaitForShutdown()
			os.Exit(0)
		}
	case syscall.SIGUSR1:
		// File log re-open for rotating file logs.
		s.ReOpenLogFile()
	case syscall.SIGUSR2:
		s.startLameDuckMode()
	case syscall.SIGHUP:
		// Config reload.
		if err := s.Reload(); err != nil {
			s.Errorf("Failed to reload server configuration: %s", err)
		}
	}
}

// ProcessSignal sends the given signal command to the given process. If pidStr
// is empty, this will send the signal to the single running instance of
// nats-server. If multiple instances are running, pidStr can be a globular
// expression ending with '*'. This returns an error if the given process is
// not running or the command is invalid.
func ProcessSignal(command Command, pidExpr string) error {
	var (
		err    error
		errStr string
		pids   = make([]int, 1)
		pidStr = strings.TrimSuffix(pidExpr, "*")
		isGlob = strings.HasSuffix(pidExpr, "*")
	)

	// Validate input if given
	if pidStr != "" {
		if pids[0], err = strconv.Atoi(pidStr); err != nil {
			return fmt.Errorf("invalid pid: %s", pidStr)
		}
	}
	// Gather all PIDs unless the input is specific
	if pidStr == "" || isGlob {
		if pids, err = resolvePids(); err != nil {
			return err
		}
	}
	// Multiple instances are running and the input is not an expression
	if len(pids) > 1 && !isGlob {
		errStr = fmt.Sprintf("multiple %s processes running:", processName)
		for _, p := range pids {
			errStr += fmt.Sprintf("\n%d", p)
		}
		return errors.New(errStr)
	}
	// No instances are running
	if len(pids) == 0 {
		return fmt.Errorf("no %s processes running", processName)
	}

	var signum syscall.Signal
	if signum, err = CommandToSignal(command); err != nil {
		return err
	}

	for _, pid := range pids {
		if _pidStr := strconv.Itoa(pid); _pidStr != pidStr && pidStr != "" {
			if !isGlob || !strings.HasPrefix(_pidStr, pidStr) {
				continue
			}
		}
		if err = kill(pid, signum); err != nil {
			errStr += fmt.Sprintf("\nsignal %q %d: %s", command, pid, err)
		}
	}
	if errStr != "" {
		return errors.New(errStr)
	}
	return nil
}

// Translates a command to a signal number
func CommandToSignal(command Command) (syscall.Signal, error) {
	switch command {
	case CommandStop:
		return syscall.SIGKILL, nil
	case CommandQuit:
		return syscall.SIGINT, nil
	case CommandReopen:
		return syscall.SIGUSR1, nil
	case CommandReload:
		return syscall.SIGHUP, nil
	case commandLDMode:
		return syscall.SIGUSR2, nil
	case commandTerm:
		return syscall.SIGTERM, nil
	default:
		return 0, fmt.Errorf("unknown signal %q", command)
	}
}

// resolvePids returns the pids for all running nats-server processes.
func resolvePids() ([]int, error) {
	// If pgrep isn't available, this will just bail out and the user will be
	// required to specify a pid.
	output, err := pgrep()
	if err != nil {
		switch err.(type) {
		case *exec.ExitError:
			// ExitError indicates non-zero exit code, meaning no processes
			// found.
			break
		default:
			return nil, errors.New("unable to resolve pid, try providing one")
		}
	}
	var (
		myPid   = os.Getpid()
		pidStrs = strings.Split(string(output), "\n")
		pids    = make([]int, 0, len(pidStrs))
	)
	for _, pidStr := range pidStrs {
		if pidStr == "" {
			continue
		}
		pid, err := strconv.Atoi(pidStr)
		if err != nil {
			return nil, errors.New("unable to resolve pid, try providing one")
		}
		// Ignore the current process.
		if pid == myPid {
			continue
		}
		pids = append(pids, pid)
	}
	return pids, nil
}

var kill = func(pid int, signal syscall.Signal) error {
	return syscall.Kill(pid, signal)
}

var pgrep = func() ([]byte, error) {
	return exec.Command("pgrep", processName).Output()
}
