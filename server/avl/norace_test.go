// Copyright 2023-2026 The NATS Authors
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

//go:build !race && !skip_no_race_tests && !skip_no_race_1_tests

package avl

import (
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

// Print Results: go test -v  --args --results
var printResults = flag.Bool("results", false, "Enable Results Logging")

// SequenceSet memory tests vs dmaps.
func TestNoRaceSeqSetSizeComparison(t *testing.T) {
	// Create 5M random entries (dupes possible but ok for this test) out of 8M range.
	num := 5_000_000
	max := 7_000_000

	seqs := make([]uint64, 0, num)
	for i := 0; i < num; i++ {
		n := uint64(rand.Int64N(int64(max + 1)))
		seqs = append(seqs, n)
	}

	runtime.GC()
	// Disable to get stable results.
	gcp := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(gcp)

	mem := runtime.MemStats{}
	runtime.ReadMemStats(&mem)
	inUseBefore := mem.HeapInuse

	dmap := make(map[uint64]struct{}, num)
	for _, n := range seqs {
		dmap[n] = struct{}{}
	}
	runtime.ReadMemStats(&mem)
	dmapUse := mem.HeapInuse - inUseBefore
	inUseBefore = mem.HeapInuse

	// Now do SequenceSet on same dataset.
	var sset SequenceSet
	for _, n := range seqs {
		sset.Insert(n)
	}

	runtime.ReadMemStats(&mem)
	seqSetUse := mem.HeapInuse - inUseBefore

	if seqSetUse > 2*1024*1024 {
		t.Fatalf("Expected SequenceSet size to be < 2M, got %v", friendlyBytes(seqSetUse))
	}
	if seqSetUse*50 > dmapUse {
		t.Fatalf("Expected SequenceSet to be at least 50x better then dmap approach: %v vs %v",
			friendlyBytes(seqSetUse),
			friendlyBytes(dmapUse),
		)
	}
}

func TestNoRaceSeqSetEncodeLarge(t *testing.T) {
	num := 2_500_000
	max := 5_000_000

	dmap := make(map[uint64]struct{}, num)
	var ss SequenceSet
	for i := 0; i < num; i++ {
		n := uint64(rand.Int64N(int64(max + 1)))
		ss.Insert(n)
		dmap[n] = struct{}{}
	}

	// Disable to get stable results.
	gcp := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(gcp)

	// In general should be about the same, but can see some variability.
	expected := time.Millisecond

	start := time.Now()
	b := ss.Encode(nil)

	if elapsed := time.Since(start); elapsed > expected {
		t.Fatalf("Expected encode of %d items with encoded size %v to take less than %v, got %v",
			num, friendlyBytes(len(b)), expected, elapsed)
	} else {
		logResults("Encode time for %d items was %v, encoded size is %v\n", num, elapsed, friendlyBytes(len(b)))
	}

	start = time.Now()
	ss2, _, err := Decode(b)
	require_NoError(t, err)
	if elapsed := time.Since(start); elapsed > expected {
		t.Fatalf("Expected decode to take less than %v, got %v", expected, elapsed)
	} else {
		logResults("Decode time is %v\n", elapsed)
	}
	require_True(t, ss.Nodes() == ss2.Nodes())
	require_True(t, ss.Size() == ss2.Size())
}

func TestNoRaceSeqSetRelativeSpeed(t *testing.T) {
	// Create 1M random entries (dupes possible but ok for this test) out of 3M range.
	num := 1_000_000
	max := 3_000_000

	seqs := make([]uint64, 0, num)
	for range num {
		n := uint64(rand.Int64N(int64(max + 1)))
		seqs = append(seqs, n)
	}

	measure := func() error {
		start := time.Now()
		// Now do SequenceSet on same dataset.
		var sset SequenceSet
		for _, n := range seqs {
			sset.Insert(n)
		}
		ssInsertElapsed := time.Since(start)
		logResults("Inserts SequenceSet: %v for %d items\n", ssInsertElapsed, num)

		start = time.Now()
		for _, n := range seqs {
			if ok := sset.Exists(n); !ok {
				t.Fatalf("Should exist")
			}
		}
		ssLookupElapsed := time.Since(start)
		logResults("Lookups: %v\n", ssLookupElapsed)

		// Now do a map.
		dmap := make(map[uint64]struct{})
		start = time.Now()
		for _, n := range seqs {
			dmap[n] = struct{}{}
		}
		mapInsertElapsed := time.Since(start)
		logResults("Inserts Map[uint64]: %v for %d items\n", mapInsertElapsed, num)

		start = time.Now()
		for _, n := range seqs {
			if _, ok := dmap[n]; !ok {
				t.Fatalf("Should exist")
			}
		}
		mapLookupElapsed := time.Since(start)
		logResults("Lookups: %v\n", mapLookupElapsed)

		// In general we are between 1.5 and 1.75 times slower atm then a straight map.
		// Let's test an upper bound of 2x for now.
		if mapInsertElapsed*2 <= ssInsertElapsed {
			return fmt.Errorf("Expected SequenceSet insert to be no more than 2x slower (%v vs %v)", mapInsertElapsed, ssInsertElapsed)
		}

		// Map lookups can be much faster on machines with large CPU caches, so allow up to 5x.
		if mapLookupElapsed*5 <= ssLookupElapsed {
			return fmt.Errorf("Expected SequenceSet lookups to be no more than 5x slower (%v vs %v)", mapLookupElapsed, ssLookupElapsed)
		}
		return nil
	}

	// Timings are sensitive to machine load, so allow a few attempts.
	var err error
	for range 5 {
		if err = measure(); err == nil {
			return
		}
	}
	t.Fatal(err)
}

// friendlyBytes returns a string with the given bytes int64
// represented as a size, such as 1KB, 10MB, etc...
func friendlyBytes[T int | uint64 | int64](bytes T) string {
	fbytes := float64(bytes)
	base := 1024
	pre := []string{"K", "M", "G", "T", "P", "E"}
	if fbytes < float64(base) {
		return fmt.Sprintf("%v B", fbytes)
	}
	exp := int(math.Log(fbytes) / math.Log(float64(base)))
	index := exp - 1
	return fmt.Sprintf("%.2f %sB", fbytes/math.Pow(float64(base), float64(exp)), pre[index])
}

func logResults(format string, args ...any) {
	if *printResults {
		fmt.Printf(format, args...)
	}
}
