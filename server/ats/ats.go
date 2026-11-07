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

// ats controls the go routines for the access time service.
// This allows more efficient unixnano operations for cache access times.
// We will have one per binary (usually per server).
package ats

import (
	"sync"
	"sync/atomic"
	"time"
)

// Update every 100ms for gathering access time in unix nano.
const TickInterval = 100 * time.Millisecond

var (
	// Our unix nano time.
	utime atomic.Int64
	// How may registered users do we have, controls lifetime of Go routine.
	refs atomic.Int64
	// Serializes worker startup and shutdown so a new worker cannot consume a
	// previous worker's stop signal.
	lifecycleMu sync.Mutex
	// To signal the shutdown of the Go routine.
	done     chan struct{}
	workerWG sync.WaitGroup
)

// Register usage. This will happen on filestore creation.
func Register() {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()

	if v := refs.Add(1); v == 1 {
		// This is the first to register (could also go up and down),
		// so spin up Go routine and grab initial time.
		utime.Store(time.Now().UnixNano())
		done = make(chan struct{})
		workerWG.Add(1)

		go func(done <-chan struct{}) {
			defer workerWG.Done()
			ticker := time.NewTicker(TickInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					utime.Store(time.Now().UnixNano())
				case <-done:
					return
				}
			}
		}(done)
	}
}

// Unregister usage. We will shutdown the go routine if no more registered users.
func Unregister() {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()

	if v := refs.Add(-1); v == 0 {
		close(done)
		workerWG.Wait()
		done = nil
	} else if v < 0 {
		refs.Store(0)
		panic("unbalanced unregister for access time state")
	}
}

// Will load the access time from an atomic.
// If no one has registered this will return 0 or stale data.
// It is the responsibility of the user to properly register and unregister.
func AccessTime() int64 {
	// Return last updated time.
	v := utime.Load()
	if v == 0 {
		// Always register a time, the worst case is a stale time.
		// On startup, we can register in parallel and could previously panic.
		v = time.Now().UnixNano()
		utime.Store(v)
	}
	return v
}
