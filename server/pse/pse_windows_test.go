// Copyright 2015-2025 The NATS Authors
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

//go:build windows

package pse

import (
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func checkValues(t *testing.T, pcpu, tPcpu float64, rss, tRss int64) {
	if pcpu != tPcpu {
		delta := int64(pcpu - tPcpu)
		if delta < 0 {
			delta = -delta
		}
		if delta > 30 { // 30%?
			t.Fatalf("CPUs did not match close enough: %f vs %f", pcpu, tPcpu)
		}
	}
	if rss != tRss {
		delta := rss - tRss
		if delta < 0 {
			delta = -delta
		}
		if delta > 1024*1024 { // 1MB
			t.Fatalf("RSSs did not match close enough: %d vs %d", rss, tRss)
		}
	}
}

func TestPSEmulationWin(t *testing.T) {
	var pcpu, tPcpu float64
	var rss, vss, tRss int64

	runtime.GC()

	if err := ProcUsage(&pcpu, &rss, &vss); err != nil {
		t.Fatalf("Error:  %v", err)
	}

	runtime.GC()

	imageName := getProcessImageName()
	// query the counters using typeperf
	out, err := exec.Command("typeperf.exe",
		fmt.Sprintf("\\Process(%s)\\%% Processor Time", imageName),
		fmt.Sprintf("\\Process(%s)\\Working Set - Private", imageName),
		fmt.Sprintf("\\Process(%s)\\Virtual Bytes", imageName),
		"-sc", "1").Output()
	if err != nil {
		t.Fatal("unable to run command", err)
	}

	// parse out results - refer to comments in procUsage for detail
	results := strings.Split(string(out), "\r\n")
	values := strings.Split(results[2], ",")

	// parse pcpu
	tPcpu, err = strconv.ParseFloat(strings.Trim(values[1], "\""), 64)
	if err != nil {
		t.Fatalf("Unable to parse percent cpu: %s", values[1])
	}

	// parse private bytes (rss)
	fval, err := strconv.ParseFloat(strings.Trim(values[2], "\""), 64)
	if err != nil {
		t.Fatalf("Unable to parse private bytes: %s", values[2])
	}
	tRss = int64(fval)

	checkValues(t, pcpu, tPcpu, rss, tRss)

	runtime.GC()

	// Again to test caching
	if err = ProcUsage(&pcpu, &rss, &vss); err != nil {
		t.Fatalf("Error:  %v", err)
	}
	checkValues(t, pcpu, tPcpu, rss, tRss)
}

func TestPSEmulationWinInitFailureClosesQuery(t *testing.T) {
	pcQueryLock.Lock()
	defer pcQueryLock.Unlock()

	savedHandle, savedInitial := pcHandle, initialSample
	savedPid, savedCPU, savedRss, savedVss := pidCounter, cpuCounter, rssCounter, vssCounter
	defer func() {
		pcHandle, initialSample = savedHandle, savedInitial
		pidCounter, cpuCounter, rssCounter, vssCounter = savedPid, savedCPU, savedRss, savedVss
	}()
	pcHandle = 0

	// No process matches this image name, so collecting the query data
	// fails with PDH_NO_DATA, the same error seen when the server's
	// account is not allowed to read performance counters.
	if err := initCounters("nats-pse-no-such-image"); err == nil {
		t.Fatal("Expected initCounters to fail")
	}
	if pcHandle != 0 {
		pdhCloseQuery(pcHandle)
		t.Fatal("Expected the query to be closed after initCounters failed")
	}
}
