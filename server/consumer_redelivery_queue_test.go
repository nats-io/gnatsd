// Copyright 2026 The NATS Authors
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

package server

import (
	"math/rand"
	"slices"
	"strconv"
	"testing"
)

func TestConsumerRedeliveryQueueOperations(t *testing.T) {
	var o consumer
	o.mu.Lock()
	defer o.mu.Unlock()
	require_Equal(t, o.getNextToRedeliver(), uint64(0))
	o.addToRedeliverQueue(10, 20, 30, 40)
	require_Equal(t, o.getNextToRedeliver(), uint64(10))
	require_False(t, o.onRedeliverQueue(10))
	// Exercise removal and append after the head has advanced.
	require_True(t, o.removeFromRedeliverQueue(30))
	require_False(t, o.removeFromRedeliverQueue(30))
	o.addToRedeliverQueue(50, 60)
	for _, seq := range []uint64{20, 40, 50, 60} {
		require_Equal(t, o.getNextToRedeliver(), seq)
		require_False(t, o.onRedeliverQueue(seq))
	}
	require_True(t, o.rdq == nil)
	require_Equal(t, o.rdqi.Size(), 0)
	require_False(t, o.hasRedeliveries())
	o.addToRedeliverQueue(70)
	require_Equal(t, o.getNextToRedeliver(), uint64(70))
	require_Equal(t, o.getNextToRedeliver(), uint64(0))
	require_True(t, o.rdq == nil)
	require_Equal(t, o.rdqi.Size(), 0)
}

func TestConsumerRedeliveryQueueInterleaved(t *testing.T) {
	var o consumer
	o.mu.Lock()
	defer o.mu.Unlock()
	rng := rand.New(rand.NewSource(42))
	var expected []uint64
	var next uint64
	for i := 0; i < 10000; i++ {
		switch rng.Intn(3) {
		case 0:
			next++
			o.addToRedeliverQueue(next)
			expected = append(expected, next)
		case 1:
			var want uint64
			if len(expected) > 0 {
				want, expected = expected[0], expected[1:]
			}
			require_Equal(t, o.getNextToRedeliver(), want)
		case 2:
			if len(expected) > 0 {
				index := rng.Intn(len(expected))
				require_True(t, o.removeFromRedeliverQueue(expected[index]))
				expected = slices.Delete(expected, index, index+1)
			}
		}
		require_True(t, slices.Equal(o.rdq, expected))
		require_Equal(t, o.rdqi.Size(), len(expected))
		for _, seq := range expected {
			require_True(t, o.onRedeliverQueue(seq))
		}
	}
}

func BenchmarkConsumerRedeliveryQueue(b *testing.B) {
	for _, size := range []int{1, 64, 4096, 1000000} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			var o consumer
			seqs := make([]uint64, size)
			for i := range seqs {
				seqs[i] = uint64(i) + 1
			}
			o.addToRedeliverQueue(seqs...)
			o.mu.Lock()
			defer o.mu.Unlock()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				seq := o.getNextToRedeliver()
				o.addToRedeliverQueue(seq)
			}
		})
	}
}
