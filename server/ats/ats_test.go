// Copyright 2025 The NATS Authors
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

package ats

import (
	"runtime"
	"testing"
	"time"
)

func TestNotRunningValue(t *testing.T) {
	// Set back to zero in case this test gets run multiple times via --count.
	utime.Store(0)
	at := AccessTime()
	if at == 0 {
		t.Fatal("Expected non-zero access time")
	}

	atn := AccessTime()
	if atn != at {
		t.Fatal("Did not expect updates to access time")
	}
}

func TestRegisterAndUnregister(t *testing.T) {
	ngrp := runtime.NumGoroutine()

	Register()
	if r := refs.Load(); r != 1 {
		t.Fatalf("Expected refs to be 1, got %d", r)
	}
	// Make sure we have a non-zero time.
	at := AccessTime()
	if at == 0 {
		t.Fatal("Expected non-zero access time")
	}
	// Make sure we are updating access time.
	time.Sleep(2 * TickInterval)
	atn := AccessTime()
	if atn == at {
		t.Fatal("Expected access time to be updated but it was not")
	}

	// Now unregister.
	Unregister()
	if r := refs.Load(); r != 0 {
		t.Fatalf("Expected refs to be 0, got %d", r)
	}

	at = AccessTime()
	time.Sleep(2 * TickInterval)
	atn = AccessTime()
	if atn != at {
		t.Fatal("Did not expect updates to access time")
	}

	// Check that we have no additional go routines running.
	ngra := runtime.NumGoroutine()
	if ngra != ngrp {
		t.Fatalf("Expected same number of go routines after removing all registered: %d vs %d", ngrp, ngra)
	}

	// Check that we spin back up on going from zero to one registered after spinning down.
	Register()
	defer Unregister()

	at = AccessTime()
	time.Sleep(2 * TickInterval)
	atn = AccessTime()
	if atn == at {
		t.Fatal("Expected access time to be updated but it was not")
	}
	ngra = runtime.NumGoroutine()
	if ngra != ngrp+1 {
		t.Fatalf("Expected to see additional go routine: %d vs %d", ngrp, ngra)
	}
}

func TestUnbalancedUnregister(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected function to panic, but it did not")
		}
	}()
	Unregister()
}
