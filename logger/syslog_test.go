// Copyright 2012-2019 The NATS Authors
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

// +build !windows

package logger

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var serverFQN string

func TestSysLogger(t *testing.T) {
	ci := os.Getenv("GITLAB_CI")
	if ci != "" {
		t.SkipNow()
	}

	logger := NewSysLogger(false, false)

	if logger.debug {
		t.Fatalf("Expected %t, received %t\n", false, logger.debug)
	}

	if logger.trace {
		t.Fatalf("Expected %t, received %t\n", false, logger.trace)
	}
}

func TestSysLoggerWithDebugAndTrace(t *testing.T) {
	ci := os.Getenv("GITLAB_CI")
	if ci != "" {
		t.SkipNow()
	}

	logger := NewSysLogger(true, true)

	if !logger.debug {
		t.Fatalf("Expected %t, received %t\n", true, logger.debug)
	}

	if !logger.trace {
		t.Fatalf("Expected %t, received %t\n", true, logger.trace)
	}
}

func testTag(t *testing.T, exePath, expected string) {
	os.Args[0] = exePath
	if result := GetSysLoggerTag(); result != expected {
		t.Fatalf("Expected %s, received %s", expected, result)
	}
}

func restoreArg(orig string) {
	os.Args[0] = orig
}

func TestSysLoggerTagGen(t *testing.T) {
	origArg := os.Args[0]
	defer restoreArg(origArg)

	testTag(t, "nats-server", "nats-server")
	testTag(t, filepath.Join(".", "nats-server"), "nats-server")
	testTag(t, filepath.Join("home", "bin", "nats-server"), "nats-server")
	testTag(t, filepath.Join("..", "..", "nats-server"), "nats-server")
	testTag(t, "nats-server.service1", "nats-server.service1")
	testTag(t, "nats-server_service1", "nats-server_service1")
	testTag(t, "nats-server-service1", "nats-server-service1")
	testTag(t, "nats-server service1", "nats-server service1")
}

func TestSysLoggerTag(t *testing.T) {
	origArg := os.Args[0]
	defer restoreArg(origArg)

	os.Args[0] = "ServerLoggerTag"

	done := make(chan string)
	startServer(done)
	logger := NewRemoteSysLogger(serverFQN, true, true)
	logger.Noticef("foo")

	line := <-done
	data := strings.Split(line, "[")
	if len(data) != 2 {
		t.Fatalf("Unexpected syslog line %s\n", line)
	}

	if !strings.Contains(data[0], os.Args[0]) {
		t.Fatalf("Expected '%s', received '%s'\n", os.Args[0], data[0])
	}
}

func TestRemoteSysLogger(t *testing.T) {
	done := make(chan string)
	startServer(done)
	logger := NewRemoteSysLogger(serverFQN, true, true)

	if !logger.debug {
		t.Fatalf("Expected %t, received %t\n", true, logger.debug)
	}

	if !logger.trace {
		t.Fatalf("Expected %t, received %t\n", true, logger.trace)
	}
	logger.Noticef("foo")
	<-done
}

func TestRemoteSysLoggerNotice(t *testing.T) {
	done := make(chan string)
	startServer(done)
	logger := NewRemoteSysLogger(serverFQN, true, true)

	logger.Noticef("foo %s", "bar")
	expectSyslogOutput(t, <-done, "foo bar\n")
}

func TestRemoteSysLoggerDebug(t *testing.T) {
	done := make(chan string)
	startServer(done)
	logger := NewRemoteSysLogger(serverFQN, true, true)

	logger.Debugf("foo %s", "qux")
	expectSyslogOutput(t, <-done, "foo qux\n")
}

func TestRemoteSysLoggerDebugDisabled(t *testing.T) {
	done := make(chan string)
	startServer(done)
	logger := NewRemoteSysLogger(serverFQN, false, false)

	logger.Debugf("foo %s", "qux")
	rcvd := <-done
	if rcvd != "" {
		t.Fatalf("Unexpected syslog response %s\n", rcvd)
	}
}

func TestRemoteSysLoggerTrace(t *testing.T) {
	done := make(chan string)
	startServer(done)
	logger := NewRemoteSysLogger(serverFQN, true, true)

	logger.Tracef("foo %s", "qux")
	expectSyslogOutput(t, <-done, "foo qux\n")
}

func TestRemoteSysLoggerTraceDisabled(t *testing.T) {
	done := make(chan string)
	startServer(done)
	logger := NewRemoteSysLogger(serverFQN, true, false)

	logger.Tracef("foo %s", "qux")
	rcvd := <-done
	if rcvd != "" {
		t.Fatalf("Unexpected syslog response %s\n", rcvd)
	}
}

func TestGetNetworkAndAddrUDP(t *testing.T) {
	n, a := getNetworkAndAddr("udp://foo.com:1000")

	if n != "udp" {
		t.Fatalf("Unexpected network %s\n", n)
	}

	if a != "foo.com:1000" {
		t.Fatalf("Unexpected addr %s\n", a)
	}
}

func TestGetNetworkAndAddrTCP(t *testing.T) {
	n, a := getNetworkAndAddr("tcp://foo.com:1000")

	if n != "tcp" {
		t.Fatalf("Unexpected network %s\n", n)
	}

	if a != "foo.com:1000" {
		t.Fatalf("Unexpected addr %s\n", a)
	}
}

func TestGetNetworkAndAddrUnix(t *testing.T) {
	n, a := getNetworkAndAddr("unix:///foo.sock")

	if n != "unix" {
		t.Fatalf("Unexpected network %s\n", n)
	}

	if a != "/foo.sock" {
		t.Fatalf("Unexpected addr %s\n", a)
	}
}
func expectSyslogOutput(t *testing.T, line string, expected string) {
	data := strings.Split(line, "]: ")
	if len(data) != 2 {
		t.Fatalf("Unexpected syslog line %s\n", line)
	}

	if data[1] != expected {
		t.Fatalf("Expected '%s', received '%s'\n", expected, data[1])
	}
}

func runSyslog(c net.PacketConn, done chan<- string) {
	var buf [4096]byte
	var rcvd string
	for {
		n, _, err := c.ReadFrom(buf[:])
		if err != nil || n == 0 {
			break
		}
		rcvd += string(buf[:n])
	}
	done <- rcvd
}

func startServer(done chan<- string) {
	c, e := net.ListenPacket("udp", "127.0.0.1:0")
	if e != nil {
		log.Fatalf("net.ListenPacket failed udp :0 %v", e)
	}

	serverFQN = fmt.Sprintf("udp://%s", c.LocalAddr().String())
	c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	go runSyslog(c, done)
}
