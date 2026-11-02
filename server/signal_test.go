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

//go:build !windows

package server

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats.go"
)

func TestSignalToReOpenLogFile(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "test.log")
	opts := &Options{
		Host:    "127.0.0.1",
		Port:    -1,
		NoSigs:  false,
		LogFile: logFile,
	}
	s := RunServer(opts)
	defer s.SetLogger(nil, false, false)
	defer s.Shutdown()

	// Set the file log
	fileLog := logger.NewFileLogger(s.opts.LogFile, s.opts.Logtime, s.opts.Debug, s.opts.Trace, true, logger.LogUTC(s.opts.LogtimeUTC))
	s.SetLogger(fileLog, false, false)

	// Add a trace
	expectedStr := "This is a Notice"
	s.Noticef(expectedStr)
	buf, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	if !strings.Contains(string(buf), expectedStr) {
		t.Fatalf("Expected log to contain %q, got %q", expectedStr, string(buf))
	}
	// Rename the file
	if err := os.Rename(logFile, logFile+".bak"); err != nil {
		t.Fatalf("Unable to rename file: %v", err)
	}
	// This should cause file to be reopened.
	syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
	// Wait a bit for action to be performed
	time.Sleep(500 * time.Millisecond)
	buf, err = os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	expectedStr = "File log re-opened"
	if !strings.Contains(string(buf), expectedStr) {
		t.Fatalf("Expected log to contain %q, got %q", expectedStr, string(buf))
	}
}

func TestSignalToReloadConfig(t *testing.T) {
	tmpl := `
		listen: 127.0.0.1:-1
		accounts: {
			A: { users: [ { user: %s, password: foo } ] }
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, "foo")))
	opts, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	opts.NoLog = true
	s := RunServer(opts)
	defer s.Shutdown()

	// Check that the reload time does not change when there are no changes.
	loaded := s.ConfigTime()
	time.Sleep(250 * time.Millisecond)
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	time.Sleep(250 * time.Millisecond)
	if reloaded := s.ConfigTime(); reloaded.Equal(loaded) {
		t.Fatalf("ConfigTime is incorrect.\nexpected reload time to change: %s\ngot: %s", loaded, reloaded)
	}

	// Repeat test to make sure that server services signals more than once...
	for i := 0; i < 2; i++ {
		loaded := s.ConfigTime()
		user := fmt.Sprintf("foo:%d", i)
		if err := os.WriteFile(conf, []byte(fmt.Sprintf(tmpl, user)), 0666); err != nil {
			t.Fatalf("Error creating config file: %v", err)
		}
		// Wait a bit to ensure ConfigTime changes.
		time.Sleep(5 * time.Millisecond)

		// This should cause config to be reloaded.
		syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
		// Wait a bit for action to be performed
		time.Sleep(500 * time.Millisecond)

		if reloaded := s.ConfigTime(); !reloaded.After(loaded) {
			t.Fatalf("ConfigTime is incorrect.\nexpected greater than: %s\ngot: %s", loaded, reloaded)
		}
	}
}

func TestProcessSignalNoProcesses(t *testing.T) {
	pgrepBefore := pgrep
	pgrep = func() ([]byte, error) {
		return nil, &exec.ExitError{}
	}
	defer func() {
		pgrep = pgrepBefore
	}()

	err := ProcessSignal(CommandStop, "")
	if err == nil {
		t.Fatal("Expected error")
	}
	expectedStr := "no nats-server processes running"
	if err.Error() != expectedStr {
		t.Fatalf("Error is incorrect.\nexpected: %s\ngot: %s", expectedStr, err.Error())
	}
}

func TestProcessSignalMultipleProcesses(t *testing.T) {
	pid := os.Getpid()
	pgrepBefore := pgrep
	pgrep = func() ([]byte, error) {
		return []byte(fmt.Sprintf("123\n456\n%d\n", pid)), nil
	}
	defer func() {
		pgrep = pgrepBefore
	}()

	err := ProcessSignal(CommandStop, "")
	if err == nil {
		t.Fatal("Expected error")
	}
	expectedStr := "multiple nats-server processes running:\n123\n456"
	if err.Error() != expectedStr {
		t.Fatalf("Error is incorrect.\nexpected: %s\ngot: %s", expectedStr, err.Error())
	}
}

func TestProcessSignalMultipleProcessesGlob(t *testing.T) {
	pid := os.Getpid()
	pgrepBefore := pgrep
	pgrep = func() ([]byte, error) {
		return []byte(fmt.Sprintf("123\n456\n%d\n", pid)), nil
	}
	defer func() {
		pgrep = pgrepBefore
	}()

	err := ProcessSignal(CommandStop, "*")
	if err == nil {
		t.Fatal("Expected error")
	}
	lines := strings.Split(err.Error(), "\n")
	require_Len(t, len(lines), 3)
	require_Equal(t, lines[0], "") // Empty line comes first
	require_True(t, strings.HasPrefix(lines[1], "signal \"stop\" 123:"))
	require_True(t, strings.HasPrefix(lines[2], "signal \"stop\" 456:"))
}

func TestProcessSignalMultipleProcessesGlobPartial(t *testing.T) {
	pid := os.Getpid()
	pgrepBefore := pgrep
	pgrep = func() ([]byte, error) {
		return []byte(fmt.Sprintf("123\n124\n456\n%d\n", pid)), nil
	}
	defer func() {
		pgrep = pgrepBefore
	}()

	err := ProcessSignal(CommandStop, "12*")
	if err == nil {
		t.Fatal("Expected error")
	}
	lines := strings.Split(err.Error(), "\n")
	require_Len(t, len(lines), 3)
	require_Equal(t, lines[0], "") // Empty line comes first
	require_True(t, strings.HasPrefix(lines[1], "signal \"stop\" 123:"))
	require_True(t, strings.HasPrefix(lines[2], "signal \"stop\" 124:"))
}

func TestProcessSignalPgrepError(t *testing.T) {
	pgrepBefore := pgrep
	pgrep = func() ([]byte, error) {
		return nil, errors.New("error")
	}
	defer func() {
		pgrep = pgrepBefore
	}()

	err := ProcessSignal(CommandStop, "")
	if err == nil {
		t.Fatal("Expected error")
	}
	expectedStr := "unable to resolve pid, try providing one"
	if err.Error() != expectedStr {
		t.Fatalf("Error is incorrect.\nexpected: %s\ngot: %s", expectedStr, err.Error())
	}
}

func TestProcessSignalPgrepMangled(t *testing.T) {
	pgrepBefore := pgrep
	pgrep = func() ([]byte, error) {
		return []byte("12x"), nil
	}
	defer func() {
		pgrep = pgrepBefore
	}()

	err := ProcessSignal(CommandStop, "")
	if err == nil {
		t.Fatal("Expected error")
	}
	expectedStr := "unable to resolve pid, try providing one"
	if err.Error() != expectedStr {
		t.Fatalf("Error is incorrect.\nexpected: %s\ngot: %s", expectedStr, err.Error())
	}
}

func TestProcessSignalResolveSingleProcess(t *testing.T) {
	pid := os.Getpid()
	pgrepBefore := pgrep
	pgrep = func() ([]byte, error) {
		return []byte(fmt.Sprintf("123\n%d\n", pid)), nil
	}
	defer func() {
		pgrep = pgrepBefore
	}()
	killBefore := kill
	called := false
	kill = func(pid int, signal syscall.Signal) error {
		called = true
		if pid != 123 {
			t.Fatalf("pid is incorrect.\nexpected: 123\ngot: %d", pid)
		}
		if signal != syscall.SIGKILL {
			t.Fatalf("signal is incorrect.\nexpected: killed\ngot: %v", signal)
		}
		return nil
	}
	defer func() {
		kill = killBefore
	}()

	if err := ProcessSignal(CommandStop, ""); err != nil {
		t.Fatalf("ProcessSignal failed: %v", err)
	}

	if !called {
		t.Fatal("Expected kill to be called")
	}
}

func TestProcessSignalInvalidCommand(t *testing.T) {
	err := ProcessSignal(Command("invalid"), "123")
	if err == nil {
		t.Fatal("Expected error")
	}
	expectedStr := "unknown signal \"invalid\""
	if err.Error() != expectedStr {
		t.Fatalf("Error is incorrect.\nexpected: %s\ngot: %s", expectedStr, err.Error())
	}
}

func TestProcessSignalInvalidPid(t *testing.T) {
	err := ProcessSignal(CommandStop, "abc")
	if err == nil {
		t.Fatal("Expected error")
	}
	expectedStr := "invalid pid: abc"
	if err.Error() != expectedStr {
		t.Fatalf("Error is incorrect.\nexpected: %s\ngot: %s", expectedStr, err.Error())
	}
}

func TestProcessSignalQuitProcess(t *testing.T) {
	killBefore := kill
	called := false
	kill = func(pid int, signal syscall.Signal) error {
		called = true
		if pid != 123 {
			t.Fatalf("pid is incorrect.\nexpected: 123\ngot: %d", pid)
		}
		if signal != syscall.SIGINT {
			t.Fatalf("signal is incorrect.\nexpected: interrupt\ngot: %v", signal)
		}
		return nil
	}
	defer func() {
		kill = killBefore
	}()

	if err := ProcessSignal(CommandQuit, "123"); err != nil {
		t.Fatalf("ProcessSignal failed: %v", err)
	}

	if !called {
		t.Fatal("Expected kill to be called")
	}
}

func TestProcessSignalTermProcess(t *testing.T) {
	killBefore := kill
	called := false
	kill = func(pid int, signal syscall.Signal) error {
		called = true
		if pid != 123 {
			t.Fatalf("pid is incorrect.\nexpected: 123\ngot: %d", pid)
		}
		if signal != syscall.SIGTERM {
			t.Fatalf("signal is incorrect.\nexpected: interrupt\ngot: %v", signal)
		}
		return nil
	}
	defer func() {
		kill = killBefore
	}()

	if err := ProcessSignal(commandTerm, "123"); err != nil {
		t.Fatalf("ProcessSignal failed: %v", err)
	}

	if !called {
		t.Fatal("Expected kill to be called")
	}
}

func TestProcessSignalReopenProcess(t *testing.T) {
	killBefore := kill
	called := false
	kill = func(pid int, signal syscall.Signal) error {
		called = true
		if pid != 123 {
			t.Fatalf("pid is incorrect.\nexpected: 123\ngot: %d", pid)
		}
		if signal != syscall.SIGUSR1 {
			t.Fatalf("signal is incorrect.\nexpected: user defined signal 1\ngot: %v", signal)
		}
		return nil
	}
	defer func() {
		kill = killBefore
	}()

	if err := ProcessSignal(CommandReopen, "123"); err != nil {
		t.Fatalf("ProcessSignal failed: %v", err)
	}

	if !called {
		t.Fatal("Expected kill to be called")
	}
}

func TestProcessSignalReloadProcess(t *testing.T) {
	killBefore := kill
	called := false
	kill = func(pid int, signal syscall.Signal) error {
		called = true
		if pid != 123 {
			t.Fatalf("pid is incorrect.\nexpected: 123\ngot: %d", pid)
		}
		if signal != syscall.SIGHUP {
			t.Fatalf("signal is incorrect.\nexpected: hangup\ngot: %v", signal)
		}
		return nil
	}
	defer func() {
		kill = killBefore
	}()

	if err := ProcessSignal(CommandReload, "123"); err != nil {
		t.Fatalf("ProcessSignal failed: %v", err)
	}

	if !called {
		t.Fatal("Expected kill to be called")
	}
}

func TestProcessSignalLameDuckMode(t *testing.T) {
	killBefore := kill
	called := false
	kill = func(pid int, signal syscall.Signal) error {
		called = true
		if pid != 123 {
			t.Fatalf("pid is incorrect.\nexpected: 123\ngot: %d", pid)
		}
		if signal != syscall.SIGUSR2 {
			t.Fatalf("signal is incorrect.\nexpected: sigusr2\ngot: %v", signal)
		}
		return nil
	}
	defer func() {
		kill = killBefore
	}()

	if err := ProcessSignal(commandLDMode, "123"); err != nil {
		t.Fatalf("ProcessSignal failed: %v", err)
	}

	if !called {
		t.Fatal("Expected kill to be called")
	}
}

func TestProcessSignalTermDuringLameDuckMode(t *testing.T) {
	opts := &Options{
		Host:                "127.0.0.1",
		Port:                -1,
		NoSigs:              true,
		NoLog:               true,
		LameDuckDuration:    2 * time.Second,
		LameDuckGracePeriod: 1 * time.Second,
	}
	s := RunServer(opts)
	defer s.Shutdown()

	// Create single NATS Connection which will cause the server
	// to delay the shutdown.
	doneCh := make(chan struct{})
	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port),
		nats.DisconnectHandler(func(*nats.Conn) {
			close(doneCh)
		}),
	)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Trigger lame duck based shutdown.
	go s.lameDuckMode()

	// Wait for client to be disconnected.
	select {
	case <-doneCh:
		break
	case <-time.After(3 * time.Second):
		t.Fatalf("Timed out waiting for client to disconnect")
	}

	// Termination signal should not cause server to shutdown
	// while in lame duck mode already.
	s.handleSignal(syscall.SIGTERM)

	// Wait for server shutdown due to lame duck shutdown.
	timeoutCh := make(chan error)
	timer := time.AfterFunc(3*time.Second, func() {
		timeoutCh <- errors.New("Timed out waiting for server shutdown")
	})
	for range time.NewTicker(1 * time.Millisecond).C {
		select {
		case err := <-timeoutCh:
			t.Fatal(err)
		default:
		}

		if !s.isRunning() {
			timer.Stop()
			break
		}
	}
}

func TestSignalTermRightAfterLameDuckSignal(t *testing.T) {
	if os.Getenv("IN_TEST") == "1" {
		s := RunServer(&Options{Host: "127.0.0.1", Port: -1, NoSigs: true, NoLog: true})
		defer s.Shutdown()
		// Same calls the signal loop makes, without lame duck mode the SIGTERM would exit.
		s.handleSignal(syscall.SIGUSR2)
		s.handleSignal(syscall.SIGTERM)
		require_True(t, s.isLameDuckMode())
		return
	}
	// Run as separate process, an exit from the SIGTERM must fail the test.
	cmd := exec.Command(os.Args[0], "-test.run=TestSignalTermRightAfterLameDuckSignal", "-test.paniconexit0")
	cmd.Env = append(os.Environ(), "IN_TEST=1")
	err := cmd.Run()
	require_NoError(t, err)
}

func TestSignalInterruptHasSuccessfulExit(t *testing.T) {
	if os.Getenv("IN_TEST") == "1" {
		s := RunServer(&Options{})
		defer s.Shutdown()
		require_NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGINT))
		s.WaitForShutdown()
		return
	}
	// To check for successful/0 exit code, need execute as separate process.
	cmd := exec.Command(os.Args[0], "-test.run=TestSignalInterruptHasSuccessfulExit")
	cmd.Env = append(os.Environ(), "IN_TEST=1")
	err := cmd.Run()
	require_NoError(t, err)
}

func TestSignalTermHasSuccessfulExit(t *testing.T) {
	if os.Getenv("IN_TEST") == "1" {
		s := RunServer(&Options{})
		defer s.Shutdown()
		require_NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGTERM))
		s.WaitForShutdown()
		return
	}
	// To check for successful/0 exit code, need execute as separate process.
	cmd := exec.Command(os.Args[0], "-test.run=TestSignalTermHasSuccessfulExit")
	cmd.Env = append(os.Environ(), "IN_TEST=1")
	err := cmd.Run()
	require_NoError(t, err)
}
