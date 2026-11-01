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

package avl

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"testing"
)

func TestSeqSetBasics(t *testing.T) {
	var ss SequenceSet

	seqs := []uint64{22, 222, 2000, 2, 2, 4}
	for _, seq := range seqs {
		ss.Insert(seq)
		require_True(t, ss.Exists(seq))
	}

	require_True(t, ss.Nodes() == 1)
	require_True(t, ss.Size() == len(seqs)-1) // We have one dup in there.
	lh, rh := ss.Heights()
	require_True(t, lh == 0)
	require_True(t, rh == 0)
}

func TestSeqSetLeftLean(t *testing.T) {
	var ss SequenceSet

	for i := uint64(4 * numEntries); i > 0; i-- {
		ss.Insert(i)
	}

	require_True(t, ss.Nodes() == 5)
	require_True(t, ss.Size() == 4*numEntries)
	lh, rh := ss.Heights()
	require_True(t, lh == 2)
	require_True(t, rh == 1)
}

func TestSeqSetRightLean(t *testing.T) {
	var ss SequenceSet

	for i := uint64(0); i < uint64(4*numEntries); i++ {
		ss.Insert(i)
	}

	require_True(t, ss.Nodes() == 4)
	require_True(t, ss.Size() == 4*numEntries)
	lh, rh := ss.Heights()
	require_True(t, lh == 1)
	require_True(t, rh == 2)
}

func TestSeqSetCorrectness(t *testing.T) {
	// Generate 100k sequences across 500k range.
	num := 100_000
	max := 500_000

	set := make(map[uint64]struct{}, num)
	var ss SequenceSet
	for i := 0; i < num; i++ {
		n := uint64(rand.Int64N(int64(max + 1)))
		ss.Insert(n)
		set[n] = struct{}{}
	}

	for i := uint64(0); i <= uint64(max); i++ {
		_, exists := set[i]
		require_True(t, ss.Exists(i) == exists)
	}
}

func TestSeqSetRange(t *testing.T) {
	num := 2*numEntries + 22
	nums := make([]uint64, 0, num)
	for i := 0; i < num; i++ {
		nums = append(nums, uint64(i))
	}
	rand.Shuffle(len(nums), func(i, j int) { nums[i], nums[j] = nums[j], nums[i] })

	var ss SequenceSet
	for _, n := range nums {
		ss.Insert(n)
	}

	nums = nums[:0]
	ss.Range(func(n uint64) bool {
		nums = append(nums, n)
		return true
	})
	require_True(t, len(nums) == num)
	for i := uint64(0); i < uint64(num); i++ {
		require_True(t, nums[i] == i)
	}

	// Test truncating the range call.
	nums = nums[:0]
	ss.Range(func(n uint64) bool {
		if n >= 10 {
			return false
		}
		nums = append(nums, n)
		return true
	})
	require_True(t, len(nums) == 10)
	for i := uint64(0); i < 10; i++ {
		require_True(t, nums[i] == i)
	}
}

func TestSeqSetDelete(t *testing.T) {
	var ss SequenceSet

	// Simple single node.
	seqs := []uint64{22, 222, 2222, 2, 2, 4}
	for _, seq := range seqs {
		ss.Insert(seq)
	}

	for _, seq := range seqs {
		ss.Delete(seq)
		require_True(t, !ss.Exists(seq))
	}
	require_True(t, ss.root == nil)
}

func TestSeqSetInsertAndDeletePedantic(t *testing.T) {
	var ss SequenceSet

	num := 50*numEntries + 22
	nums := make([]uint64, 0, num)
	for i := 0; i < num; i++ {
		nums = append(nums, uint64(i))
	}
	rand.Shuffle(len(nums), func(i, j int) { nums[i], nums[j] = nums[j], nums[i] })

	// Make sure always balanced.
	testBalanced := func() {
		t.Helper()
		// Check heights.
		ss.root.nodeIter(func(n *node) {
			if n != nil && n.h != maxH(n)+1 {
				t.Fatalf("Node height is wrong: %+v", n)
			}
		})
		// Check balance factor.
		if bf := balanceF(ss.root); bf > 1 || bf < -1 {
			t.Fatalf("Unbalanced tree")
		}
	}

	for _, n := range nums {
		ss.Insert(n)
		testBalanced()
	}
	require_True(t, ss.root != nil)

	for _, n := range nums {
		ss.Delete(n)
		testBalanced()
		require_True(t, !ss.Exists(n))
		if ss.Size() > 0 {
			require_True(t, ss.root != nil)
		}
	}
	require_True(t, ss.root == nil)
}

func TestSeqSetMinMax(t *testing.T) {
	var ss SequenceSet

	// Simple single node.
	seqs := []uint64{22, 222, 2222, 2, 2, 4}
	for _, seq := range seqs {
		ss.Insert(seq)
	}

	min, max := ss.MinMax()
	require_True(t, min == 2 && max == 2222)

	// Multi-node
	ss.Empty()

	num := 22*numEntries + 22
	nums := make([]uint64, 0, num)
	for i := 0; i < num; i++ {
		nums = append(nums, uint64(i))
	}
	rand.Shuffle(len(nums), func(i, j int) { nums[i], nums[j] = nums[j], nums[i] })
	for _, n := range nums {
		ss.Insert(n)
	}

	min, max = ss.MinMax()
	require_True(t, min == 0 && max == uint64(num-1))
}

func TestSeqSetClone(t *testing.T) {
	// Generate 100k sequences across 500k range.
	num := 100_000
	max := 500_000

	var ss SequenceSet
	for i := 0; i < num; i++ {
		ss.Insert(uint64(rand.Int64N(int64(max + 1))))
	}

	ssc := ss.Clone()
	require_True(t, ss.Size() == ssc.Size())
	require_True(t, ss.Nodes() == ssc.Nodes())
}

func TestSeqSetEqual(t *testing.T) {
	var ss1, ss2, ss3 SequenceSet

	// Empty and nil sets are all equal.
	var nilSet *SequenceSet
	require_True(t, ss1.Equal(&ss2))
	require_True(t, ss1.Equal(nilSet))
	require_True(t, nilSet.Equal(&ss1))

	// Same state (first, last, size) but different contents.
	for _, seq := range []uint64{1, 3, 5} {
		ss1.Insert(seq)
	}
	for _, seq := range []uint64{1, 4, 5} {
		ss2.Insert(seq)
	}
	require_False(t, ss1.Equal(&ss2))
	require_False(t, ss2.Equal(&ss1))
	require_False(t, ss1.Equal(nilSet))
	require_False(t, nilSet.Equal(&ss1))

	// Identical contents, insertion order should not matter.
	for _, seq := range []uint64{5, 1, 3} {
		ss3.Insert(seq)
	}
	require_True(t, ss1.Equal(&ss3))
	require_True(t, ss3.Equal(&ss1))

	// Differing sizes.
	ss3.Insert(7)
	require_False(t, ss1.Equal(&ss3))
}

func TestSeqSetUnion(t *testing.T) {
	var ss1, ss2 SequenceSet

	seqs1 := []uint64{22, 222, 2222, 2, 2, 4}
	for _, seq := range seqs1 {
		ss1.Insert(seq)
	}

	seqs2 := []uint64{33, 333, 3333, 3, 33_333, 333_333}
	for _, seq := range seqs2 {
		ss2.Insert(seq)
	}

	ss := Union(&ss1, &ss2)
	require_True(t, ss.Size() == 11)

	seqs := append(seqs1, seqs2...)
	for _, n := range seqs {
		require_True(t, ss.Exists(n))
	}
}

func TestSeqSetFirst(t *testing.T) {
	var ss SequenceSet

	seqs := []uint64{22, 222, 2222, 222_222}
	for _, seq := range seqs {
		// Normal case where we pick first/base.
		ss.Insert(seq)
		require_True(t, ss.root.base == (seq/numEntries)*numEntries)
		ss.Empty()
		// Where we set the minimum start value.
		ss.SetInitialMin(seq)
		ss.Insert(seq)
		require_True(t, ss.root.base == seq)
		ss.Empty()
	}
}

// Test that we can union with nodes vs individual sequence insertion.
func TestSeqSetDistinctUnion(t *testing.T) {
	// Distinct sets.
	var ss1 SequenceSet
	seqs1 := []uint64{1, 10, 100, 200}
	for _, seq := range seqs1 {
		ss1.Insert(seq)
	}

	var ss2 SequenceSet
	seqs2 := []uint64{5000, 6100, 6200, 6222}
	for _, seq := range seqs2 {
		ss2.Insert(seq)
	}

	ss := ss1.Clone()
	allSeqs := append(seqs1, seqs2...)

	ss.Union(&ss2)
	require_True(t, ss.Size() == len(allSeqs))
	for _, seq := range allSeqs {
		require_True(t, ss.Exists(seq))
	}
}

func TestSeqSetDecodeV1(t *testing.T) {
	// Encoding from v1 which was 64 buckets.
	seqs := []uint64{22, 222, 2222, 222_222, 2_222_222}
	encStr := `
FgEDAAAABQAAAABgAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAADgIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAA==
`

	enc, err := base64.StdEncoding.DecodeString(encStr)
	require_NoError(t, err)

	ss, _, err := Decode(enc)
	require_NoError(t, err)

	require_True(t, ss.Size() == len(seqs))
	for _, seq := range seqs {
		require_True(t, ss.Exists(seq))
	}
}

func TestSeqSetDecodeNodeCountOverflow(t *testing.T) {
	// A replicated stream state carries an avl-encoded delete map decoded from
	// a peer. The node count is read as a uint32 into an int; on 32-bit builds
	// a value above MaxInt32 turns negative (and a smaller value can overflow
	// the nn*perNode multiply), defeating the length check so the decode then
	// panics in make([]node, nn) or reads past the buffer. Both encodings must
	// reject such counts on every architecture instead.
	le := binary.LittleEndian
	for _, ver := range []uint8{1, 2} {
		buf := make([]byte, minLen)
		buf[0], buf[1] = magic, ver
		le.PutUint32(buf[2:], 0xFFFFFFFF) // node count, negative as a 32-bit int
		if _, _, err := Decode(buf); err != ErrBadEncoding {
			t.Fatalf("v%d: expected ErrBadEncoding for oversized node count, got %v", ver, err)
		}

		// Positive on 32-bit but the multiply still overflows for a short buffer.
		le.PutUint32(buf[2:], 0x00800000)
		if _, _, err := Decode(buf); err != ErrBadEncoding {
			t.Fatalf("v%d: expected ErrBadEncoding for overflowing node count, got %v", ver, err)
		}
	}

	// A well-formed single-node v2 buffer must still decode.
	valid := make([]byte, minLen+(numBuckets+1)*8+2)
	valid[0], valid[1] = magic, version
	le.PutUint32(valid[2:], 1)
	if _, _, err := Decode(valid); err != nil {
		t.Fatalf("valid single-node buffer should decode, got %v", err)
	}
}

func TestSeqSetInsertExistingNodeFastPath(t *testing.T) {
	var ss SequenceSet

	// Build a multi-node tree with only even sequences set.
	for i := uint64(0); i < 8*numEntries; i += 2 {
		ss.Insert(i)
	}
	nodes, size := ss.Nodes(), ss.Size()

	// Inserting into existing nodes must not change the tree structure.
	for i := uint64(1); i < 8*numEntries; i += 2 {
		ss.Insert(i)
		require_True(t, ss.Exists(i))
	}
	require_True(t, ss.Nodes() == nodes)
	require_True(t, ss.Size() == size+4*numEntries)

	// Duplicate inserts must not change the size.
	for i := uint64(0); i < 8*numEntries; i++ {
		ss.Insert(i)
	}
	require_True(t, ss.Size() == 8*numEntries)

	// Heights and balance must still be intact.
	ss.root.nodeIter(func(n *node) {
		if n.h != maxH(n)+1 {
			t.Fatalf("Node height is wrong: %+v", n)
		}
	})
	if bf := balanceF(ss.root); bf > 1 || bf < -1 {
		t.Fatalf("Unbalanced tree")
	}
}

func TestSeqSetInsertUnalignedBase(t *testing.T) {
	var ss SequenceSet

	// SetInitialMin can create a node whose base is not aligned to numEntries.
	require_NoError(t, ss.SetInitialMin(1000))
	for _, seq := range []uint64{1000, 1001, 1000 + numEntries - 1} {
		ss.Insert(seq)
		require_True(t, ss.Exists(seq))
	}
	require_True(t, ss.Nodes() == 1)
	require_True(t, ss.Size() == 3)

	// Outside the node's range still creates a new node.
	ss.Insert(1000 + numEntries)
	require_True(t, ss.Exists(1000+numEntries))
	require_True(t, ss.Nodes() == 2)
	require_True(t, ss.Size() == 4)
}

func TestSeqSetDeleteNotPresent(t *testing.T) {
	var ss SequenceSet

	seqs := []uint64{22, 222, 2222, 222_222}
	for _, seq := range seqs {
		ss.Insert(seq)
	}
	nodes, size := ss.Nodes(), ss.Size()

	// Deleting sequences that are not set, both inside and outside
	// existing node ranges, must be a no-op and report false.
	for _, seq := range []uint64{0, 23, 2223, 5000, 100_000, 999_999} {
		require_True(t, !ss.Delete(seq))
	}
	require_True(t, ss.Nodes() == nodes)
	require_True(t, ss.Size() == size)
	for _, seq := range seqs {
		require_True(t, ss.Exists(seq))
	}

	// Real deletes still work and empty the set.
	for _, seq := range seqs {
		require_True(t, ss.Delete(seq))
	}
	require_True(t, ss.root == nil)
}

func TestSeqSetRangeSparse(t *testing.T) {
	// Cover bucket boundaries, node boundaries and large gaps.
	seqs := []uint64{0, 1, 63, 64, 127, 128, 2047, 2048, 4095, 4096, 100_000, 1_000_000}
	var ss SequenceSet
	for _, seq := range seqs {
		ss.Insert(seq)
	}

	var got []uint64
	ss.Range(func(n uint64) bool {
		got = append(got, n)
		return true
	})
	require_True(t, len(got) == len(seqs))
	for i, seq := range seqs {
		require_True(t, got[i] == seq)
	}

	// Terminating mid-bucket should stop the iteration immediately.
	got = got[:0]
	ss.Range(func(n uint64) bool {
		got = append(got, n)
		return n != 64
	})
	require_True(t, len(got) == 4)
	require_True(t, got[3] == 64)
}

func BenchmarkSeqSetInsertDense(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var ss SequenceSet
		for s := uint64(0); s < 200_000; s++ {
			ss.Insert(s)
		}
	}
}

func BenchmarkSeqSetInsertSparse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var ss SequenceSet
		for s := uint64(0); s < 200_000; s++ {
			ss.Insert(s * 7)
		}
	}
}

func BenchmarkSeqSetRangeDense(b *testing.B) {
	var ss SequenceSet
	for s := uint64(0); s < 200_000; s++ {
		ss.Insert(s)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var sum uint64
		ss.Range(func(n uint64) bool { sum += n; return true })
	}
}

func BenchmarkSeqSetRangeSparse(b *testing.B) {
	var ss SequenceSet
	for s := uint64(0); s < 10_000; s++ {
		ss.Insert(s * 211) // ~10 bits per 2048-entry node
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var sum uint64
		ss.Range(func(n uint64) bool { sum += n; return true })
	}
}

func BenchmarkSeqSetDeleteNotPresent(b *testing.B) {
	var ss SequenceSet
	for s := uint64(0); s < 200_000; s += 2 {
		ss.Insert(s)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for s := uint64(1); s < 200_000; s += 2 {
			ss.Delete(s)
		}
	}
}

// Compare finding a span's inclusive end with one SpanEnd call versus Exists
// calls through the first missing sequence. Vary tree size and alignment since
// both affect the crossover; each measured operation uses the same warm lookup.
func BenchmarkSeqSetSpanEnd(b *testing.B) {
	for _, nodes := range []uint64{8, 1024} {
		for _, offset := range []uint64{0, 61, numEntries - 3} {
			for _, span := range []uint64{1, 2, 3, 4, 5, 6, 7, 8, 16, 32, 64, 256, 2048, 4096} {
				b.Run(fmt.Sprintf("Nodes=%d/Offset=%d/Span=%d", nodes, offset, span), func(b *testing.B) {
					var ss SequenceSet
					// Keep all background nodes populated even when the span's
					// first missing sequence coincides with a background bit.
					for i := uint64(0); i < nodes; i++ {
						ss.Insert(i * numEntries)
						ss.Insert(i*numEntries + numEntries - 1)
					}
					start := (nodes/2)*numEntries + offset
					for seq := start; seq < start+span; seq++ {
						ss.Insert(seq)
					}
					ss.Delete(start + span)
					want := start + span - 1

					b.Run("SpanEnd", func(b *testing.B) {
						b.ReportAllocs()
						var end uint64
						var found bool
						for b.Loop() {
							end, found = ss.SpanEnd(start)
						}
						if !found || end != want {
							b.Fatalf("SpanEnd returned (%d, %v), want (%d, true)", end, found, want)
						}
					})
					b.Run("Exists", func(b *testing.B) {
						b.ReportAllocs()
						var end uint64
						var found bool
						for b.Loop() {
							seq := start
							for ss.Exists(seq) {
								seq++
							}
							found = seq > start
							end = seq - 1
						}
						if !found || end != want {
							b.Fatalf("Exists loop returned (%d, %v), want (%d, true)", end, found, want)
						}
					})
				})
			}
		}
	}
}

func require_NoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("require no error, but got: %v", err)
	}
}

func require_True(t *testing.T, b bool) {
	t.Helper()
	if !b {
		t.Fatalf("require true")
	}
}

func require_False(t *testing.T, b bool) {
	t.Helper()
	if b {
		t.Fatalf("require false")
	}
}
