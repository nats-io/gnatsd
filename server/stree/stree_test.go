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

package stree

import (
	crand "crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server/gsl"
)

// Print Results: go test -v  --args --results
// For some benchmarks.
var runResults = flag.Bool("results", false, "Enable Results Tests")

func TestSubjectTreeBasics(t *testing.T) {
	st := NewSubjectTree[int]()
	require_Equal(t, st.Size(), 0)
	// Single leaf
	old, updated := st.Insert(b("foo.bar.baz"), 22)
	require_True(t, old == nil)
	require_False(t, updated)
	require_Equal(t, st.Size(), 1)
	// Find shouldn't work with a wildcard.
	_, found := st.Find(b("foo.bar.*"))
	require_False(t, found)
	// But it should with a literal. Find with single leaf.
	v, found := st.Find(b("foo.bar.baz"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	// Update single leaf
	old, updated = st.Insert(b("foo.bar.baz"), 33)
	require_True(t, old != nil)
	require_Equal(t, *old, 22)
	require_True(t, updated)
	require_Equal(t, st.Size(), 1)
	// Split the tree
	old, updated = st.Insert(b("foo.bar"), 22)
	require_True(t, old == nil)
	require_False(t, updated)
	require_Equal(t, st.Size(), 2)
	// Now we have node4 -> leaf*2
	v, found = st.Find(b("foo.bar"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	// Make sure we can still retrieve the original after the split.
	v, found = st.Find(b("foo.bar.baz"))
	require_True(t, found)
	require_Equal(t, *v, 33)
}

func TestSubjectTreeNodeGrow(t *testing.T) {
	st := NewSubjectTree[int]()
	for i := 0; i < 4; i++ {
		subj := b(fmt.Sprintf("foo.bar.%c", 'A'+i))
		old, updated := st.Insert(subj, 22)
		require_True(t, old == nil)
		require_False(t, updated)
	}
	// We have filled a node4.
	_, ok := st.root.(*node4)
	require_True(t, ok)
	// This one will trigger us to grow.
	old, updated := st.Insert(b("foo.bar.E"), 22)
	require_True(t, old == nil)
	require_False(t, updated)
	_, ok = st.root.(*node10)
	require_True(t, ok)
	for i := 5; i < 10; i++ {
		subj := b(fmt.Sprintf("foo.bar.%c", 'A'+i))
		old, updated := st.Insert(subj, 22)
		require_True(t, old == nil)
		require_False(t, updated)
	}
	// This one will trigger us to grow.
	old, updated = st.Insert(b("foo.bar.K"), 22)
	require_True(t, old == nil)
	require_False(t, updated)
	// We have filled a node10.
	_, ok = st.root.(*node16)
	require_True(t, ok)
	for i := 11; i < 16; i++ {
		subj := b(fmt.Sprintf("foo.bar.%c", 'A'+i))
		old, updated := st.Insert(subj, 22)
		require_True(t, old == nil)
		require_False(t, updated)
	}
	// This one will trigger us to grow.
	old, updated = st.Insert(b("foo.bar.Q"), 22)
	require_True(t, old == nil)
	require_False(t, updated)
	_, ok = st.root.(*node48)
	require_True(t, ok)
	// Fill the node48.
	for i := 17; i < 48; i++ {
		subj := b(fmt.Sprintf("foo.bar.%c", 'A'+i))
		old, updated := st.Insert(subj, 22)
		require_True(t, old == nil)
		require_False(t, updated)
	}
	// This one will trigger us to grow.
	subj := b(fmt.Sprintf("foo.bar.%c", 'A'+49))
	old, updated = st.Insert(subj, 22)
	require_True(t, old == nil)
	require_False(t, updated)
	_, ok = st.root.(*node256)
	require_True(t, ok)
}

func TestSubjectTreeNodePrefixMismatch(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.A"), 11)
	st.Insert(b("foo.bar.B"), 22)
	st.Insert(b("foo.bar.C"), 33)
	// Grab current root. Split below will cause update.
	or := st.root
	// This one will force a split of the node
	st.Insert(b("foo.foo.A"), 44)
	require_True(t, or != st.root)
	// Now make sure we can retrieve correctly.
	v, found := st.Find(b("foo.bar.A"))
	require_True(t, found)
	require_Equal(t, *v, 11)
	v, found = st.Find(b("foo.bar.B"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	v, found = st.Find(b("foo.bar.C"))
	require_True(t, found)
	require_Equal(t, *v, 33)
	v, found = st.Find(b("foo.foo.A"))
	require_True(t, found)
	require_Equal(t, *v, 44)
}

func TestSubjectTreeNodeDelete(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.A"), 22)
	v, found := st.Delete(b("foo.bar.A"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	require_Equal(t, st.root, nil)
	v, found = st.Delete(b("foo.bar.A"))
	require_False(t, found)
	require_Equal(t, v, nil)
	v, found = st.Find(b("foo.foo.A"))
	require_False(t, found)
	require_Equal(t, v, nil)
	// Kick to a node4.
	st.Insert(b("foo.bar.A"), 11)
	st.Insert(b("foo.bar.B"), 22)
	st.Insert(b("foo.bar.C"), 33)
	// Make sure we can delete and that we shrink back to leaf.
	v, found = st.Delete(b("foo.bar.C"))
	require_True(t, found)
	require_Equal(t, *v, 33)
	v, found = st.Delete(b("foo.bar.B"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	// We should have shrunk here.
	require_True(t, st.root.isLeaf())
	v, found = st.Delete(b("foo.bar.A"))
	require_True(t, found)
	require_Equal(t, *v, 11)
	require_Equal(t, st.root, nil)
	// Now pop up to a node10 and make sure we can shrink back down.
	for i := 0; i < 5; i++ {
		subj := fmt.Sprintf("foo.bar.%c", 'A'+i)
		st.Insert(b(subj), 22)
	}
	_, ok := st.root.(*node10)
	require_True(t, ok)
	v, found = st.Delete(b("foo.bar.A"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	_, ok = st.root.(*node4)
	require_True(t, ok)
	// Now pop up to node16
	for i := 0; i < 11; i++ {
		subj := fmt.Sprintf("foo.bar.%c", 'A'+i)
		st.Insert(b(subj), 22)
	}
	_, ok = st.root.(*node16)
	require_True(t, ok)
	v, found = st.Delete(b("foo.bar.A"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	_, ok = st.root.(*node10)
	require_True(t, ok)
	v, found = st.Find(b("foo.bar.B"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	// Now pop up to node48
	st = NewSubjectTree[int]()
	for i := 0; i < 17; i++ {
		subj := fmt.Sprintf("foo.bar.%c", 'A'+i)
		st.Insert(b(subj), 22)
	}
	_, ok = st.root.(*node48)
	require_True(t, ok)
	v, found = st.Delete(b("foo.bar.A"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	_, ok = st.root.(*node16)
	require_True(t, ok)
	v, found = st.Find(b("foo.bar.B"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	// Now pop up to node256
	st = NewSubjectTree[int]()
	for i := 0; i < 49; i++ {
		subj := fmt.Sprintf("foo.bar.%c", 'A'+i)
		st.Insert(b(subj), 22)
	}
	_, ok = st.root.(*node256)
	require_True(t, ok)
	v, found = st.Delete(b("foo.bar.A"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	_, ok = st.root.(*node48)
	require_True(t, ok)
	v, found = st.Find(b("foo.bar.B"))
	require_True(t, found)
	require_Equal(t, *v, 22)
}

func TestSubjectTreeNodesAndPaths(t *testing.T) {
	st := NewSubjectTree[int]()
	check := func(subj string) {
		t.Helper()
		v, found := st.Find(b(subj))
		require_True(t, found)
		require_Equal(t, *v, 22)
	}
	st.Insert(b("foo.bar.A"), 22)
	st.Insert(b("foo.bar.B"), 22)
	st.Insert(b("foo.bar.C"), 22)
	st.Insert(b("foo.bar"), 22)
	check("foo.bar.A")
	check("foo.bar.B")
	check("foo.bar.C")
	check("foo.bar")
	// This will do several things in terms of shrinking and pruning,
	// want to make sure it gets prefix correct for new top node4.
	st.Delete(b("foo.bar"))
	check("foo.bar.A")
	check("foo.bar.B")
	check("foo.bar.C")
}

// Check that we are constructing a proper tree with complex insert patterns.
func TestSubjectTreeConstruction(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.A"), 1)
	st.Insert(b("foo.bar.B"), 2)
	st.Insert(b("foo.bar.C"), 3)
	st.Insert(b("foo.baz.A"), 11)
	st.Insert(b("foo.baz.B"), 22)
	st.Insert(b("foo.baz.C"), 33)
	st.Insert(b("foo.bar"), 42)

	checkNode := func(an *node, kind string, pors string, numChildren uint16) {
		t.Helper()
		require_True(t, an != nil)
		n := *an
		require_True(t, n != nil)
		require_Equal(t, n.kind(), kind)
		require_Equal(t, pors, string(n.path()))
		require_Equal(t, numChildren, n.numChildren())
	}

	checkNode(&st.root, "NODE4", "foo.ba", 2)
	nn := st.root.findChild('r')
	checkNode(nn, "NODE4", "r", 2)
	checkNode((*nn).findChild(noPivot), "LEAF", "", 0)
	rnn := (*nn).findChild('.')
	checkNode(rnn, "NODE4", ".", 3)
	checkNode((*rnn).findChild('A'), "LEAF", "A", 0)
	checkNode((*rnn).findChild('B'), "LEAF", "B", 0)
	checkNode((*rnn).findChild('C'), "LEAF", "C", 0)
	znn := st.root.findChild('z')
	checkNode(znn, "NODE4", "z.", 3)
	checkNode((*znn).findChild('A'), "LEAF", "A", 0)
	checkNode((*znn).findChild('B'), "LEAF", "B", 0)
	checkNode((*znn).findChild('C'), "LEAF", "C", 0)
	// Use st.Dump() if you want a tree print out.

	// Now delete "foo.bar" and make sure put ourselves back together properly.
	v, found := st.Delete(b("foo.bar"))
	require_True(t, found)
	require_Equal(t, *v, 42)

	checkNode(&st.root, "NODE4", "foo.ba", 2)
	nn = st.root.findChild('r')
	checkNode(nn, "NODE4", "r.", 3)
	checkNode((*nn).findChild('A'), "LEAF", "A", 0)
	checkNode((*nn).findChild('B'), "LEAF", "B", 0)
	checkNode((*nn).findChild('C'), "LEAF", "C", 0)
	znn = st.root.findChild('z')
	checkNode(znn, "NODE4", "z.", 3)
	checkNode((*znn).findChild('A'), "LEAF", "A", 0)
	checkNode((*znn).findChild('B'), "LEAF", "B", 0)
	checkNode((*znn).findChild('C'), "LEAF", "C", 0)
}

func match(t *testing.T, st *SubjectTree[int], filter string, expected int) {
	t.Helper()
	var matches []int
	st.Match(b(filter), func(_ []byte, v *int) {
		matches = append(matches, *v)
	})
	require_Equal(t, expected, len(matches))
}

func TestSubjectTreeMatchLeafOnly(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.baz.A"), 1)

	// Check all placements of pwc in token space.
	match(t, st, "foo.bar.*.A", 1)
	match(t, st, "foo.*.baz.A", 1)
	match(t, st, "foo.*.*.A", 1)
	match(t, st, "foo.*.*.*", 1)
	match(t, st, "*.*.*.*", 1)
	// Now check fwc.
	match(t, st, ">", 1)
	match(t, st, "foo.>", 1)
	match(t, st, "foo.*.>", 1)
	match(t, st, "foo.bar.>", 1)
	match(t, st, "foo.bar.*.>", 1)

	// Check partials so they do not trigger on leafs.
	match(t, st, "foo.bar.baz", 0)
}

func TestSubjectTreeMatchNodes(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.A"), 1)
	st.Insert(b("foo.bar.B"), 2)
	st.Insert(b("foo.bar.C"), 3)
	st.Insert(b("foo.baz.A"), 11)
	st.Insert(b("foo.baz.B"), 22)
	st.Insert(b("foo.baz.C"), 33)

	// Test literals.
	match(t, st, "foo.bar.A", 1)
	match(t, st, "foo.baz.A", 1)
	match(t, st, "foo.bar", 0)
	// Test internal pwc
	match(t, st, "foo.*.A", 2)
	// Test terminal pwc
	match(t, st, "foo.bar.*", 3)
	match(t, st, "foo.baz.*", 3)
	// Check fwc
	match(t, st, ">", 6)
	match(t, st, "foo.>", 6)
	match(t, st, "foo.bar.>", 3)
	match(t, st, "foo.baz.>", 3)
	// Make sure we do not have false positives on prefix matches.
	match(t, st, "foo.ba", 0)

	// Now add in "foo.bar" to make a more complex tree construction
	// and re-test.
	st.Insert(b("foo.bar"), 42)

	// Test literals.
	match(t, st, "foo.bar.A", 1)
	match(t, st, "foo.baz.A", 1)
	match(t, st, "foo.bar", 1)
	// Test internal pwc
	match(t, st, "foo.*.A", 2)
	// Test terminal pwc
	match(t, st, "foo.bar.*", 3)
	match(t, st, "foo.baz.*", 3)
	// Check fwc
	match(t, st, ">", 7)
	match(t, st, "foo.>", 7)
	match(t, st, "foo.bar.>", 3)
	match(t, st, "foo.baz.>", 3)
}

func matchUntilCount[T any](st *SubjectTree[T], filter string, stopAfter int) (int, bool) {
	var n int
	completed := st.MatchUntil(b(filter), func(_ []byte, _ *T) bool {
		n++
		return n < stopAfter
	})
	return n, completed
}

func TestSubjectTreeMatchUntil(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.A"), 1)
	st.Insert(b("foo.bar.B"), 2)
	st.Insert(b("foo.bar.C"), 3)
	st.Insert(b("foo.baz.A"), 11)
	st.Insert(b("foo.baz.B"), 22)
	st.Insert(b("foo.baz.C"), 33)
	st.Insert(b("foo.bar"), 42)

	// Ensure early stop terminates traversal.
	count, completed := matchUntilCount(st, "foo.>", 3)
	require_Equal(t, count, 3)
	require_False(t, completed)

	// Match completes
	count, completed = matchUntilCount(st, "foo.bar", 3)
	require_Equal(t, count, 1)
	require_True(t, completed)

	count, completed = matchUntilCount(st, "foo.baz.*", 4)
	require_Equal(t, count, 3)
	require_True(t, completed)
}

func TestSubjectTreeNoPrefix(t *testing.T) {
	st := NewSubjectTree[int]()
	for i := 0; i < 26; i++ {
		subj := b(fmt.Sprintf("%c", 'A'+i))
		old, updated := st.Insert(subj, 22)
		require_True(t, old == nil)
		require_False(t, updated)
	}
	n, ok := st.root.(*node48)
	require_True(t, ok)
	require_Equal(t, n.numChildren(), 26)
	v, found := st.Delete(b("B"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	require_Equal(t, n.numChildren(), 25)
	v, found = st.Delete(b("Z"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	require_Equal(t, n.numChildren(), 24)
}

func TestSubjectTreePartialTerminalWildcardBugMatch(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("STATE.GLOBAL.CELL1.7PDSGAALXNN000010.PROPERTY-A"), 5)
	st.Insert(b("STATE.GLOBAL.CELL1.7PDSGAALXNN000010.PROPERTY-B"), 1)
	st.Insert(b("STATE.GLOBAL.CELL1.7PDSGAALXNN000010.PROPERTY-C"), 2)
	match(t, st, "STATE.GLOBAL.CELL1.7PDSGAALXNN000010.*", 3)
}

func TestSubjectTreeMatchSubjectParam(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.A"), 1)
	st.Insert(b("foo.bar.B"), 2)
	st.Insert(b("foo.bar.C"), 3)
	st.Insert(b("foo.baz.A"), 11)
	st.Insert(b("foo.baz.B"), 22)
	st.Insert(b("foo.baz.C"), 33)
	st.Insert(b("foo.bar"), 42)

	checkValMap := map[string]int{
		"foo.bar.A": 1,
		"foo.bar.B": 2,
		"foo.bar.C": 3,
		"foo.baz.A": 11,
		"foo.baz.B": 22,
		"foo.baz.C": 33,
		"foo.bar":   42,
	}
	// Make sure we get a proper subject parameter and it matches our value properly.
	st.Match([]byte(">"), func(subject []byte, v *int) {
		if expected, ok := checkValMap[string(subject)]; !ok {
			t.Fatalf("Unexpected subject parameter: %q", subject)
		} else if expected != *v {
			t.Fatalf("Expected %q to have value of %d, but got %d", subject, expected, *v)
		}
	})
}

func TestSubjectTreeMatchRandomDoublePWC(t *testing.T) {
	st := NewSubjectTree[int]()
	for i := 1; i <= 10_000; i++ {
		subj := fmt.Sprintf("foo.%d.%d", rand.IntN(20)+1, i)
		st.Insert(b(subj), 42)
	}
	match(t, st, "foo.*.*", 10_000)

	// Check with pwc and short interior token.
	seen, verified := 0, 0
	st.Match(b("*.2.*"), func(_ []byte, _ *int) {
		seen++
	})
	// Now check via walk to make sure we are right.
	st.IterOrdered(func(subject []byte, v *int) bool {
		tokens := strings.Split(string(subject), ".")
		require_Equal(t, len(tokens), 3)
		if tokens[1] == "2" {
			verified++
		}
		return true
	})
	require_Equal(t, seen, verified)

	seen, verified = 0, 0
	st.Match(b("*.*.222"), func(_ []byte, _ *int) {
		seen++
	})
	st.IterOrdered(func(subject []byte, v *int) bool {
		tokens := strings.Split(string(subject), ".")
		require_Equal(t, len(tokens), 3)
		if tokens[2] == "222" {
			verified++
		}
		return true
	})
	require_Equal(t, seen, verified)
}

func TestSubjectTreeIterOrdered(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.A"), 1)
	st.Insert(b("foo.bar.B"), 2)
	st.Insert(b("foo.bar.C"), 3)
	st.Insert(b("foo.baz.A"), 11)
	st.Insert(b("foo.baz.B"), 22)
	st.Insert(b("foo.baz.C"), 33)
	st.Insert(b("foo.bar"), 42)

	checkValMap := map[string]int{
		"foo.bar.A": 1,
		"foo.bar.B": 2,
		"foo.bar.C": 3,
		"foo.baz.A": 11,
		"foo.baz.B": 22,
		"foo.baz.C": 33,
		"foo.bar":   42,
	}
	checkOrder := []string{
		"foo.bar",
		"foo.bar.A",
		"foo.bar.B",
		"foo.bar.C",
		"foo.baz.A",
		"foo.baz.B",
		"foo.baz.C",
	}
	var received int
	walk := func(subject []byte, v *int) bool {
		if expected := checkOrder[received]; expected != string(subject) {
			t.Fatalf("Expected %q for %d item returned, got %q", expected, received, subject)
		}
		received++
		require_True(t, v != nil)
		if expected := checkValMap[string(subject)]; expected != *v {
			t.Fatalf("Expected %q to have value of %d, but got %d", subject, expected, *v)
		}
		return true
	}
	// Kick in the iter.
	st.IterOrdered(walk)
	require_Equal(t, received, len(checkOrder))

	// Make sure we can terminate properly.
	received = 0
	st.IterOrdered(func(subject []byte, v *int) bool {
		received++
		return received != 4
	})
	require_Equal(t, received, 4)
}

func TestSubjectTreeIterFast(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.A"), 1)
	st.Insert(b("foo.bar.B"), 2)
	st.Insert(b("foo.bar.C"), 3)
	st.Insert(b("foo.baz.A"), 11)
	st.Insert(b("foo.baz.B"), 22)
	st.Insert(b("foo.baz.C"), 33)
	st.Insert(b("foo.bar"), 42)

	checkValMap := map[string]int{
		"foo.bar.A": 1,
		"foo.bar.B": 2,
		"foo.bar.C": 3,
		"foo.baz.A": 11,
		"foo.baz.B": 22,
		"foo.baz.C": 33,
		"foo.bar":   42,
	}
	var received int
	walk := func(subject []byte, v *int) bool {
		received++
		require_True(t, v != nil)
		if expected := checkValMap[string(subject)]; expected != *v {
			t.Fatalf("Expected %q to have value of %d, but got %d", subject, expected, *v)
		}
		return true
	}
	// Kick in the iter.
	st.IterFast(walk)
	require_Equal(t, received, len(checkValMap))

	// Make sure we can terminate properly.
	received = 0
	st.IterFast(func(subject []byte, v *int) bool {
		received++
		return received != 4
	})
	require_Equal(t, received, 4)
}

func TestSubjectTreeInsertSamePivotBug(t *testing.T) {
	testSubjects := [][]byte{
		[]byte("0d00.2abbb82c1d.6e16.fa7f85470e.3e46"),
		[]byte("534b12.3486c17249.4dde0666"),
		[]byte("6f26aabd.920ee3.d4d3.5ffc69f6"),
		[]byte("8850.ade3b74c31.aa533f77.9f59.a4bd8415.b3ed7b4111"),
		[]byte("5a75047dcb.5548e845b6.76024a34.14d5b3.80c426.51db871c3a"),
		[]byte("825fa8acfc.5331.00caf8bbbd.107c4b.c291.126d1d010e"),
	}
	st := NewSubjectTree[int]()
	for _, subj := range testSubjects {
		old, updated := st.Insert(subj, 22)
		require_True(t, old == nil)
		require_False(t, updated)
		if _, found := st.Find(subj); !found {
			t.Fatalf("Could not find subject %q which should be findable", subj)
		}
	}
}

func TestSubjectTreeMatchTsepSecondThenPartialPartBug(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.xxxxx.foo1234.zz"), 22)
	st.Insert(b("foo.yyy.foo123.zz"), 22)
	st.Insert(b("foo.yyybar789.zz"), 22)
	st.Insert(b("foo.yyy.foo12345.zz"), 22)
	st.Insert(b("foo.yyy.foo12345.yy"), 22)
	st.Insert(b("foo.yyy.foo123456789.zz"), 22)
	match(t, st, "foo.*.foo123456789.*", 1)
	match(t, st, "foo.*.*.zzz.foo.>", 0)
}

func TestSubjectTreeMatchMultipleWildcardBasic(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("A.B.C.D.0.G.H.I.0"), 22)
	st.Insert(b("A.B.C.D.1.G.H.I.0"), 22)
	match(t, st, "A.B.*.D.1.*.*.I.0", 1)
}

func TestSubjectTreeMatchInvalidWildcard(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.123"), 22)
	st.Insert(b("one.two.three.four.five"), 22)
	st.Insert(b("'*.123"), 22)
	st.Insert(b("bar"), 22)
	match(t, st, "invalid.>", 0)
	match(t, st, "foo.>.bar", 0)
	match(t, st, ">", 4)
	match(t, st, `'*.*`, 1)
	match(t, st, `'*.*.*'`, 0)
	// None of these should match.
	match(t, st, "`>`", 0)
	match(t, st, `">"`, 0)
	match(t, st, `'>'`, 0)
	match(t, st, `'*.>'`, 0)
	match(t, st, `'*.>.`, 0)
	match(t, st, "`invalid.>`", 0)
	match(t, st, `'*.*'`, 0)
}

func TestSubjectTreeMatchPartialWildcardIsLiteral(t *testing.T) {
	// Shared "a" prefix splits nodes right before the '*' or '>' in the filters.
	st := NewSubjectTree[int]()
	st.Insert(b("KV.a"), 1)
	st.Insert(b("KV.apple"), 2)
	st.Insert(b("KV.avocado"), 3)
	st.Insert(b("KV.banana"), 4)
	match(t, st, "KV.a*", 0)
	match(t, st, "KV.a>", 0)
	match(t, st, "KV.ap*", 0)
	match(t, st, "KV.*", 4)
	match(t, st, "KV.>", 4)

	// Subjects with a wildcard char inside a token are valid literals, and match exactly.
	st.Insert(b("KV.a*"), 5)
	st.Insert(b("KV.a*x"), 6)
	st.Insert(b("KV.a>"), 7)
	st.Insert(b("KV.a>x"), 8)
	match(t, st, "KV.a*", 1)
	match(t, st, "KV.a>", 1)
	match(t, st, "KV.a*x", 1)
	match(t, st, "*.a*", 1)
	match(t, st, "KV.*", 8)
	match(t, st, "KV.>", 8)

	// A trimmed '*' can also reach a leaf directly.
	st = NewSubjectTree[int]()
	st.Insert(b("KV.apple"), 1)
	st.Insert(b("KV.avocado"), 2)
	st.Insert(b("KV.a*x"), 3)
	match(t, st, "KV.a*", 0)
	match(t, st, "KV.a*x", 1)
}

func TestSubjectTreeRandomTrackEntries(t *testing.T) {
	st := NewSubjectTree[int]()
	smap := make(map[string]struct{}, 1000)

	// Make sure all added items can be found.
	check := func() {
		t.Helper()
		for subj := range smap {
			if _, found := st.Find(b(subj)); !found {
				t.Fatalf("Could not find subject %q which should be findable", subj)
			}
		}
	}

	buf := make([]byte, 10)
	for i := 0; i < 1000; i++ {
		var sb strings.Builder
		// 1-6 tokens.
		numTokens := rand.IntN(6) + 1
		for i := 0; i < numTokens; i++ {
			tlen := rand.IntN(4) + 2
			tok := buf[:tlen]
			crand.Read(tok)
			sb.WriteString(hex.EncodeToString(tok))
			if i != numTokens-1 {
				sb.WriteString(".")
			}
		}
		subj := sb.String()
		// Avoid dupes since will cause check to fail after we delete messages.
		if _, ok := smap[subj]; ok {
			continue
		}
		smap[subj] = struct{}{}
		old, updated := st.Insert(b(subj), 22)
		require_True(t, old == nil)
		require_False(t, updated)
		require_Equal(t, st.Size(), len(smap))
		check()
	}
}

// Needs to be longer then internal node prefix, which currently is 24.
func TestSubjectTreeLongTokens(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("a1.aaaaaaaaaaaaaaaaaaaaaa0"), 1)
	st.Insert(b("a2.0"), 2)
	st.Insert(b("a1.aaaaaaaaaaaaaaaaaaaaaa1"), 3)
	st.Insert(b("a2.1"), 4)
	// Simulate purge of a2.>
	// This required to show bug.
	st.Delete(b("a2.0"))
	st.Delete(b("a2.1"))
	require_Equal(t, st.Size(), 2)
	v, found := st.Find(b("a1.aaaaaaaaaaaaaaaaaaaaaa0"))
	require_True(t, found)
	require_Equal(t, *v, 1)
	v, found = st.Find(b("a1.aaaaaaaaaaaaaaaaaaaaaa1"))
	require_True(t, found)
	require_Equal(t, *v, 3)
}

func b(s string) []byte {
	return []byte(s)
}

func TestSubjectTreeMatchAllPerf(t *testing.T) {
	if !*runResults {
		t.Skip()
	}
	st := NewSubjectTree[int]()

	for i := 0; i < 1_000_000; i++ {
		subj := fmt.Sprintf("subj.%d.%d", rand.IntN(100)+1, i)
		st.Insert(b(subj), 22)
	}

	for _, f := range [][]byte{
		[]byte(">"),
		[]byte("subj.>"),
		[]byte("subj.*.*"),
		[]byte("*.*.*"),
		[]byte("subj.1.*"),
		[]byte("subj.1.>"),
		[]byte("subj.*.1"),
		[]byte("*.*.1"),
	} {
		start := time.Now()
		count := 0
		st.Match(f, func(_ []byte, _ *int) {
			count++
		})
		t.Logf("Match %q took %s and matched %d entries", f, time.Since(start), count)
	}
}

func TestSubjectTreeIterPerf(t *testing.T) {
	if !*runResults {
		t.Skip()
	}
	st := NewSubjectTree[int]()

	for i := 0; i < 1_000_000; i++ {
		subj := fmt.Sprintf("subj.%d.%d", rand.IntN(100)+1, i)
		st.Insert(b(subj), 22)
	}

	start := time.Now()
	count := 0
	st.IterOrdered(func(_ []byte, _ *int) bool {
		count++
		return true
	})
	t.Logf("Iter took %s and matched %d entries", time.Since(start), count)
}

func TestSubjectTreeMemoryPerf(t *testing.T) {
	if !*runResults {
		t.Skip()
	}

	const entries = 250_000
	subjects := make([][]byte, entries)
	for i := range subjects {
		subjects[i] = []byte("account." + strconv.Itoa(i%1000) + ".stream." + strconv.Itoa(i))
	}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	start := time.Now()
	st := NewSubjectTree[int]()
	for i, subject := range subjects {
		st.Insert(subject, i)
	}
	buildTime := time.Since(start)

	runtime.GC()
	runtime.KeepAlive(st)
	runtime.ReadMemStats(&after)

	const lookupRounds = 8
	start = time.Now()
	found := 0
	for range lookupRounds {
		for _, subject := range subjects {
			if _, ok := st.Find(subject); ok {
				found++
			}
		}
	}
	findTime := time.Since(start)

	const traversalRounds = 20
	start = time.Now()
	iterated := 0
	for range traversalRounds {
		st.IterFast(func(_ []byte, _ *int) bool {
			iterated++
			return true
		})
	}
	iterTime := time.Since(start)

	filter := []byte("account.*.stream.>")
	start = time.Now()
	matched := 0
	for range traversalRounds {
		st.Match(filter, func(_ []byte, _ *int) {
			matched++
		})
	}
	matchTime := time.Since(start)

	iterAllocs := testing.AllocsPerRun(20, func() {
		st.IterFast(func(_ []byte, _ *int) bool { return true })
	})
	matchAllocs := testing.AllocsPerRun(20, func() {
		st.Match(filter, func(_ []byte, _ *int) {})
	})

	t.Logf("Build took %s for %d entries, allocated %d bytes total and retained %d heap bytes across %d allocations",
		buildTime, st.Size(), after.TotalAlloc-before.TotalAlloc,
		after.HeapAlloc-before.HeapAlloc, after.Mallocs-before.Mallocs)
	t.Logf("Find took %s for %d lookups and found %d entries",
		findTime, entries*lookupRounds, found)
	t.Logf("IterFast took %s for %d entries with %.0f allocations per traversal",
		iterTime, iterated, iterAllocs)
	t.Logf("Match took %s for %d entries with %.0f allocations per traversal",
		matchTime, matched, matchAllocs)

	runtime.KeepAlive(subjects)
	runtime.KeepAlive(st)
}

func TestSubjectTreeNode48(t *testing.T) {
	var a, b, c leaf[int]
	var n node48

	n.addChild('A', &a)
	require_Equal(t, n.key['A'], 1)
	require_True(t, n.child[0] != nil)
	require_Equal(t, n.child[0].(*leaf[int]), &a)
	require_Equal(t, len(n.children()), 1)

	child := n.findChild('A')
	require_True(t, child != nil)
	require_Equal(t, (*child).(*leaf[int]), &a)

	n.addChild('B', &b)
	require_Equal(t, n.key['B'], 2)
	require_True(t, n.child[1] != nil)
	require_Equal(t, n.child[1].(*leaf[int]), &b)
	require_Equal(t, len(n.children()), 2)

	child = n.findChild('B')
	require_True(t, child != nil)
	require_Equal(t, (*child).(*leaf[int]), &b)

	n.addChild('C', &c)
	require_Equal(t, n.key['C'], 3)
	require_True(t, n.child[2] != nil)
	require_Equal(t, n.child[2].(*leaf[int]), &c)
	require_Equal(t, len(n.children()), 3)

	child = n.findChild('C')
	require_True(t, child != nil)
	require_Equal(t, (*child).(*leaf[int]), &c)

	n.deleteChild('A')
	require_Equal(t, len(n.children()), 2)
	require_Equal(t, n.key['A'], 0) // Now deleted
	require_Equal(t, n.key['B'], 2) // Untouched
	require_Equal(t, n.key['C'], 1) // Where A was

	child = n.findChild('A')
	require_Equal(t, child, nil)
	require_True(t, n.child[0] != nil)
	require_Equal(t, n.child[0].(*leaf[int]), &c)

	child = n.findChild('B')
	require_True(t, child != nil)
	require_Equal(t, (*child).(*leaf[int]), &b)
	require_True(t, n.child[1] != nil)
	require_Equal(t, n.child[1].(*leaf[int]), &b)

	child = n.findChild('C')
	require_True(t, child != nil)
	require_Equal(t, (*child).(*leaf[int]), &c)
	require_True(t, n.child[2] == nil)

	var gotB, gotC bool
	var iterations int
	n.iter(func(n node) bool {
		iterations++
		if gb, ok := n.(*leaf[int]); ok && &b == gb {
			gotB = true
		}
		if gc, ok := n.(*leaf[int]); ok && &c == gc {
			gotC = true
		}
		return true
	})
	require_Equal(t, iterations, 2)
	require_True(t, gotB)
	require_True(t, gotC)

	// Check for off-by-one on byte 255 as found by staticcheck, see
	// https://github.com/nats-io/nats-server/pull/5826.
	n.addChild(255, &c)
	require_Equal(t, n.key[255], 3)
	grown := n.grow().(*node256)
	require_True(t, grown.findChild(255) != nil)
	shrunk := n.shrink().(*node16)
	require_True(t, shrunk.findChild(255) != nil)
}

func TestSubjectTreeMatchNoCallbackDupe(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.A"), 1)
	st.Insert(b("foo.bar.B"), 1)
	st.Insert(b("foo.bar.C"), 1)
	st.Insert(b("foo.bar.>"), 1)

	for _, f := range [][]byte{
		[]byte(">"),
		[]byte("foo.>"),
		[]byte("foo.bar.>"),
	} {
		seen := map[string]struct{}{}
		st.Match(f, func(bsubj []byte, _ *int) {
			subj := string(bsubj)
			if _, ok := seen[subj]; ok {
				t.Logf("Match callback was called twice for %q", subj)
			}
			seen[subj] = struct{}{}
		})
	}
}

func TestSubjectTreeNilNoPanic(t *testing.T) {
	var st *SubjectTree[int]
	st.Match([]byte("foo"), func(_ []byte, _ *int) {})
	_, found := st.Find([]byte("foo"))
	require_False(t, found)
	_, found = st.Delete([]byte("foo"))
	require_False(t, found)
	_, found = st.Insert([]byte("foo"), 22)
	require_False(t, found)
}

// This bug requires the trailing suffix contain repeating nulls \x00
// and the second subject be longer with more nulls.
func TestSubjectTreeInsertLongerLeafSuffixWithTrailingNulls(t *testing.T) {
	st := NewSubjectTree[int]()
	subj := []byte("foo.bar.baz_")
	// add in 10 nulls.
	for i := 0; i < 10; i++ {
		subj = append(subj, 0)
	}

	st.Insert(subj, 1)
	// add in 10 more nulls.
	subj2 := subj
	for i := 0; i < 10; i++ {
		subj2 = append(subj, 0)
	}
	st.Insert(subj2, 2)

	// Make sure we can look them up.
	v, found := st.Find(subj)
	require_True(t, found)
	require_Equal(t, *v, 1)
	v, found = st.Find(subj2)
	require_True(t, found)
	require_Equal(t, *v, 2)
}

// Make sure the system does not insert any subject with the noPivot (DEL) in it.
func TestSubjectTreeInsertWithNoPivot(t *testing.T) {
	st := NewSubjectTree[int]()
	subj := []byte("foo.bar.baz.")
	subj = append(subj, noPivot)
	old, updated := st.Insert(subj, 22)
	require_True(t, old == nil)
	require_False(t, updated)
	require_Equal(t, st.Size(), 0)
}

// Make sure we don't panic when checking for fwc.
func TestSubjectTreeMatchHasFWCNoPanic(t *testing.T) {
	defer func() {
		p := recover()
		require_True(t, p == nil)
	}()
	st := NewSubjectTree[int]()
	subj := []byte("foo")
	st.Insert(subj, 1)
	st.Match([]byte("."), func(subject []byte, val *int) {})
}

func TestSubjectTreeLazyIntersect(t *testing.T) {
	st1 := NewSubjectTree[int]()
	st2 := NewSubjectTree[int]()

	// Should cause an intersection.
	st1.Insert([]byte("foo.bar"), 1)
	st2.Insert([]byte("foo.bar"), 1)

	// Should cause an intersection.
	st1.Insert([]byte("foo.bar.baz.qux"), 1)
	st2.Insert([]byte("foo.bar.baz.qux"), 1)

	// Should not cause any intersections.
	st1.Insert([]byte("bar"), 1)
	st2.Insert([]byte("baz"), 1)
	st1.Insert([]byte("a.b.c"), 1)
	st2.Insert([]byte("a.b.d"), 1)
	st1.Insert([]byte("a.b.ee"), 1)
	st2.Insert([]byte("a.b.e"), 1)
	st1.Insert([]byte("bb.c.d"), 1)
	st2.Insert([]byte("b.c.d"), 1)
	st2.Insert([]byte("foo.bar.baz.qux.alice"), 1)
	st2.Insert([]byte("foo.bar.baz.qux.bob"), 1)

	intersected := map[string]int{}
	LazyIntersect(st1, st2, func(key []byte, val1, val2 *int) {
		intersected[string(key)]++
	})
	require_Equal(t, len(intersected), 2)
	require_Equal(t, intersected["foo.bar"], 1)
	require_Equal(t, intersected["foo.bar.baz.qux"], 1)
}

func TestSubjectTreeGSLIntersection(t *testing.T) {
	st := NewSubjectTree[struct{}]()
	st.Insert([]byte("one.two.three.four"), struct{}{})
	st.Insert([]byte("one.two.three.five"), struct{}{})
	st.Insert([]byte("one.two.six"), struct{}{})
	st.Insert([]byte("one.two.seven"), struct{}{})
	st.Insert([]byte("eight.nine"), struct{}{})
	st.Insert([]byte("stream.A"), struct{}{})
	st.Insert([]byte("stream.A.child"), struct{}{})

	require_NoDuplicates := func(t *testing.T, got map[string]int) {
		t.Helper()
		for _, c := range got {
			require_Equal(t, c, 1)
		}
	}

	t.Run("Literals", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("one.two.six", 11))
		require_NoError(t, sl.Insert("eight.nine", 22))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 2)
		require_NoDuplicates(t, got)
	})

	t.Run("PWC", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("one.two.*.*", 11))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 2)
		require_NoDuplicates(t, got)
	})

	t.Run("PWCOverlapping", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("one.two.*.four", 11))
		require_NoError(t, sl.Insert("one.two.*.*", 22))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 2)
		require_NoDuplicates(t, got)
	})

	t.Run("PWCAll", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("*.*", 11))
		require_NoError(t, sl.Insert("*.*.*", 22))
		require_NoError(t, sl.Insert("*.*.*.*", 33))
		require_True(t, sl.HasInterest("foo.bar"))
		require_True(t, sl.HasInterest("foo.bar.baz"))
		require_True(t, sl.HasInterest("foo.bar.baz.qux"))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 7)
		require_NoDuplicates(t, got)
	})

	t.Run("FWC", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("one.>", 11))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 4)
		require_NoDuplicates(t, got)
	})

	t.Run("FWCOverlapping", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("one.two.three.four", 11))
		require_NoError(t, sl.Insert("one.>", 22))
		require_NoError(t, sl.Insert(">", 33))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 7)
		require_NoDuplicates(t, got)
	})

	t.Run("FWCExtended", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("stream.A.>", 11))
		require_NoError(t, sl.Insert("stream.A", 22))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 2)
		require_NoDuplicates(t, got)
	})

	t.Run("PWCExtended", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("stream.*.child", 11))
		require_NoError(t, sl.Insert("stream.A", 22))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 2)
		require_NoDuplicates(t, got)
	})

	t.Run("PWCExtendedAggressive", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("stream.A.child", 11))
		require_NoError(t, sl.Insert("*.A.child", 22))
		require_NoError(t, sl.Insert("stream.*.child", 22))
		require_NoError(t, sl.Insert("stream.A.*", 22))
		require_NoError(t, sl.Insert("stream.*.*", 22))
		require_NoError(t, sl.Insert("*.A.*", 22))
		require_NoError(t, sl.Insert("*.*.child", 22))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 1)
		require_NoDuplicates(t, got)
	})

	t.Run("PWCExtendedAggressive2", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("stream.A.child", 11))
		require_NoError(t, sl.Insert("*.A.child", 22))
		require_NoError(t, sl.Insert("stream.A.*", 22))
		require_NoError(t, sl.Insert("*.A.*", 22))
		require_NoError(t, sl.Insert("*.*.child", 22))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 1)
		require_NoDuplicates(t, got)
	})

	t.Run("PWCExtendedAggressive3", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("stream.A.child", 11))
		require_NoError(t, sl.Insert("stream.*.*", 22))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 1)
		require_NoDuplicates(t, got)
	})

	t.Run("PWCExtendedAggressive4", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("stream.A.child", 11))
		require_NoError(t, sl.Insert("stream.*.>", 22))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 1)
		require_NoDuplicates(t, got)
	})

	t.Run("PWCExtendedAggressive5", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("stream.A.child", 11))
		require_NoError(t, sl.Insert("*.*", 22))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 3)
		require_NoDuplicates(t, got)
	})

	t.Run("PWCExtendedAggressive6", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("stream.A.child", 11))
		require_NoError(t, sl.Insert("stream.*", 22))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 2)
		require_NoDuplicates(t, got)
	})

	t.Run("PWCExtendedAggressive7", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("stream.A", 11))
		require_NoError(t, sl.Insert("*.*.*", 22))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 4)
		require_NoDuplicates(t, got)
	})

	t.Run("FWCAll", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert(">", 11))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 7)
		require_NoDuplicates(t, got)
	})

	t.Run("StopsWhenCallbackReturnsFalse", func(t *testing.T) {
		var got int
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert(">", 11))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got++
			return false
		})
		require_Equal(t, got, 1)
	})

	t.Run("NoMatch", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("one", 11))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 0)
	})

	t.Run("NoMatches", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("one", 11))
		require_NoError(t, sl.Insert("eight", 22))
		require_NoError(t, sl.Insert("ten", 33))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 0)
	})

	t.Run("NoMatchPartial", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("stream.A.not-child", 11))
		require_NoError(t, sl.Insert("stream.A.child.>", 22))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 0)
		require_NoDuplicates(t, got)
	})

	t.Run("PWCDoesntHideLiteral", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("one.*.six", 11))
		require_NoError(t, sl.Insert("one.two.seven", 22))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 2)
		require_NoDuplicates(t, got)
	})

	t.Run("PWCDoesntHideLiteral2", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("one.*.*.four", 11))
		require_NoError(t, sl.Insert("one.*.*.five", 22))
		require_NoError(t, sl.Insert("one.*.three", 33))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 2)
		require_NoDuplicates(t, got)
	})

	t.Run("DisjointWildcardPositions", func(t *testing.T) {
		// Test case documenting bitmask approach limitation:
		// When patterns have wildcards at different positions (not strict subsets),
		// the bitmask considers earlier-position wildcards as "less specific"
		// which can cause some matches to be skipped.
		// e.g., *.two.three.four (wildcard at pos 0) vs one.two.*.* (wildcards at pos 2,3)
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("one.two.*.*", 33))
		require_NoError(t, sl.Insert("*.two.three.four", 55))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		// Due to bitmask comparison, one.two.*.* may be skipped in favor of *.two.three.four
		// This results in only one.two.three.four being matched, missing one.two.three.five
		require_Len(t, len(got), 2)
		require_NoDuplicates(t, got)
	})

	t.Run("LiteralMatchWithOverlappingPWCs", func(t *testing.T) {
		got := map[string]int{}
		sl := gsl.NewSublist[int]()
		require_NoError(t, sl.Insert("one.*.four.five", 11))
		require_NoError(t, sl.Insert("one.two.three.four", 22))
		IntersectGSL(st, sl, func(subj []byte, entry *struct{}) bool {
			got[string(subj)]++
			return true
		})
		require_Len(t, len(got), 1)
		require_NoDuplicates(t, got)
	})
}

func TestSubjectTreeDeleteShortSubjectNoPanic(t *testing.T) {
	defer func() {
		p := recover()
		require_True(t, p == nil)
	}()

	st := NewSubjectTree[int]()

	st.Insert(b("foo.bar.baz"), 1)
	st.Insert(b("foo.bar.qux"), 2)

	v, found := st.Delete(b("foo.bar"))
	require_False(t, found)
	require_Equal(t, v, nil)
	v, found = st.Find(b("foo.bar.baz"))
	require_True(t, found)
	require_Equal(t, *v, 1)
	v, found = st.Find(b("foo.bar.qux"))
	require_True(t, found)
	require_Equal(t, *v, 2)
}

func TestSubjectTreeEmpty(t *testing.T) {
	// Test Empty on nil tree
	var st *SubjectTree[int]
	st2 := st.Empty()
	require_True(t, st2 != nil)
	require_Equal(t, st2.Size(), 0)

	// Test Empty on new tree
	st = NewSubjectTree[int]()
	require_Equal(t, st.Size(), 0)
	st2 = st.Empty()
	require_True(t, st2 == st) // Should return same instance
	require_Equal(t, st2.Size(), 0)

	// Test Empty on tree with data
	st.Insert(b("foo.bar"), 1)
	st.Insert(b("foo.baz"), 2)
	st.Insert(b("bar.baz"), 3)
	require_Equal(t, st.Size(), 3)

	// Empty should clear everything
	st2 = st.Empty()
	require_True(t, st2 == st) // Should return same instance
	require_Equal(t, st.Size(), 0)
	require_True(t, st.root == nil)

	// Verify we can't find old entries
	_, found := st.Find(b("foo.bar"))
	require_False(t, found)
	_, found = st.Find(b("foo.baz"))
	require_False(t, found)
	_, found = st.Find(b("bar.baz"))
	require_False(t, found)

	// Verify we can insert new entries after Empty
	old, updated := st.Insert(b("new.entry"), 42)
	require_True(t, old == nil)
	require_False(t, updated)
	require_Equal(t, st.Size(), 1)

	v, found := st.Find(b("new.entry"))
	require_True(t, found)
	require_Equal(t, *v, 42)
}

func TestSubjectTreeLazyIntersectComprehensive(t *testing.T) {
	// Test with nil trees
	var st1 *SubjectTree[int]
	var st2 *SubjectTree[string]
	count := 0
	LazyIntersect(st1, st2, func(key []byte, v1 *int, v2 *string) {
		count++
	})
	require_Equal(t, count, 0)

	// Test with one nil tree
	st1 = NewSubjectTree[int]()
	st1.Insert(b("foo"), 1)
	LazyIntersect(st1, st2, func(key []byte, v1 *int, v2 *string) {
		count++
	})
	require_Equal(t, count, 0)

	// Test with empty trees
	st2 = NewSubjectTree[string]()
	LazyIntersect(st1, st2, func(key []byte, v1 *int, v2 *string) {
		count++
	})
	require_Equal(t, count, 0)

	// Test with different value types
	st1 = NewSubjectTree[int]()
	st2 = NewSubjectTree[string]()

	// Add some intersecting keys
	st1.Insert(b("foo.bar"), 42)
	st2.Insert(b("foo.bar"), "hello")
	st1.Insert(b("baz.qux"), 100)
	st2.Insert(b("baz.qux"), "world")

	// Add non-intersecting keys
	st1.Insert(b("only.in.st1"), 1)
	st2.Insert(b("only.in.st2"), "two")

	results := make(map[string]struct {
		v1 int
		v2 string
	})

	LazyIntersect(st1, st2, func(key []byte, v1 *int, v2 *string) {
		results[string(key)] = struct {
			v1 int
			v2 string
		}{*v1, *v2}
	})

	require_Equal(t, len(results), 2)
	require_Equal(t, results["foo.bar"].v1, 42)
	require_Equal(t, results["foo.bar"].v2, "hello")
	require_Equal(t, results["baz.qux"].v1, 100)
	require_Equal(t, results["baz.qux"].v2, "world")

	// Test that it iterates over smaller tree
	// Create a large tree and a small tree
	large := NewSubjectTree[int]()
	small := NewSubjectTree[int]()

	// Large tree has many entries
	for i := 0; i < 100; i++ {
		large.Insert([]byte(fmt.Sprintf("large.%d", i)), i)
	}
	// Small tree has few entries with some overlap
	small.Insert(b("large.5"), 500)
	small.Insert(b("large.10"), 1000)
	small.Insert(b("large.50"), 5000)
	small.Insert(b("small.only"), 999)

	intersectCount := 0
	LazyIntersect(large, small, func(key []byte, v1 *int, v2 *int) {
		intersectCount++
		// Verify we get the correct values
		switch string(key) {
		case "large.5":
			require_Equal(t, *v1, 5)
			require_Equal(t, *v2, 500)
		case "large.10":
			require_Equal(t, *v1, 10)
			require_Equal(t, *v2, 1000)
		case "large.50":
			require_Equal(t, *v1, 50)
			require_Equal(t, *v2, 5000)
		default:
			t.Fatalf("Unexpected key: %s", key)
		}
	})
	require_Equal(t, intersectCount, 3)

	// Test with complex subjects (multiple levels)
	st3 := NewSubjectTree[int]()
	st4 := NewSubjectTree[int]()

	// Deep nesting
	st3.Insert(b("a.b.c.d.e.f.g"), 1)
	st4.Insert(b("a.b.c.d.e.f.g"), 2)

	// Partial matches (should not intersect)
	st3.Insert(b("a.b.c.d"), 3)
	st4.Insert(b("a.b.c.d.e"), 4)

	// Same prefix different suffix
	st3.Insert(b("prefix.suffix1"), 5)
	st4.Insert(b("prefix.suffix2"), 6)

	intersections := 0
	LazyIntersect(st3, st4, func(key []byte, v1 *int, v2 *int) {
		intersections++
		require_Equal(t, string(key), "a.b.c.d.e.f.g")
		require_Equal(t, *v1, 1)
		require_Equal(t, *v2, 2)
	})
	require_Equal(t, intersections, 1)
}

func TestNode256Operations(t *testing.T) {
	// Test node256 creation and basic operations
	n := newNode256(b("prefix"))
	require_False(t, n.isFull()) // node256 is never full

	// Test findChild when child doesn't exist
	child := n.findChild('a')
	require_True(t, child == nil)

	// Add a child and find it
	leaf := newLeaf(b("suffix"), 42)
	n.addChild('a', leaf)
	child = n.findChild('a')
	require_True(t, child != nil)
	require_Equal(t, n.size, uint16(1))

	// Test iter function
	iterCount := 0
	n.iter(func(node) bool {
		iterCount++
		return true
	})
	require_Equal(t, iterCount, 1)

	// Test iter with early termination
	n.addChild('b', newLeaf(b("suffix2"), 43))
	n.addChild('c', newLeaf(b("suffix3"), 44))
	iterCount = 0
	n.iter(func(node) bool {
		iterCount++
		return false // Stop after first
	})
	require_Equal(t, iterCount, 1)

	// Test children() method
	children := n.children()
	require_Equal(t, len(children), 256)

	// Test that grow() panics
	defer func() {
		if r := recover(); r != nil {
			require_Equal(t, r, "grow can not be called on node256")
		} else {
			t.Fatal("grow() should panic on node256")
		}
	}()
	n.grow()
}

func TestNode256Shrink(t *testing.T) {
	// To get a node256, we need to go through the progression:
	// node4 -> node10 -> node16 -> node48 -> node256
	// We need at least 49 children to get to node256

	// Create nodes directly to test node256 shrinking
	n256 := newNode256(b("prefix"))

	// Add 49 children
	for i := 0; i < 49; i++ {
		n256.addChild(byte(i), newLeaf([]byte{byte(i)}, i))
	}
	require_Equal(t, n256.size, uint16(49))

	// Shrink should not happen yet (> 48 children)
	shrunk := n256.shrink()
	require_True(t, shrunk == nil)

	// Delete one to get to 48 children
	n256.deleteChild(0)
	require_Equal(t, n256.size, uint16(48))

	// Now shrink should return a node48
	shrunk = n256.shrink()
	require_True(t, shrunk != nil)
	_, isNode48 := shrunk.(*node48)
	require_True(t, isNode48)

	// Verify the shrunk node has all remaining children
	for i := 1; i < 49; i++ {
		child := shrunk.findChild(byte(i))
		require_True(t, child != nil)
	}
}

func TestLeafPanicMethods(t *testing.T) {
	leaf := newLeaf(b("test"), 42)

	// Test setPrefix panic
	t.Run("setPrefix", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				require_Equal(t, r, "setPrefix called on leaf")
			} else {
				t.Fatal("setPrefix should panic on leaf")
			}
		}()
		leaf.setPrefix(b("prefix"))
	})

	// Test addChild panic
	t.Run("addChild", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				require_Equal(t, r, "addChild called on leaf")
			} else {
				t.Fatal("addChild should panic on leaf")
			}
		}()
		leaf.addChild('a', nil)
	})

	// Test findChild panic
	t.Run("findChild", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				require_Equal(t, r, "findChild called on leaf")
			} else {
				t.Fatal("findChild should panic on leaf")
			}
		}()
		leaf.findChild('a')
	})

	// Test grow panic
	t.Run("grow", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				require_Equal(t, r, "grow called on leaf")
			} else {
				t.Fatal("grow should panic on leaf")
			}
		}()
		leaf.grow()
	})

	// Test deleteChild panic
	t.Run("deleteChild", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				require_Equal(t, r, "deleteChild called on leaf")
			} else {
				t.Fatal("deleteChild should panic on leaf")
			}
		}()
		leaf.deleteChild('a')
	})

	// Test shrink panic
	t.Run("shrink", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				require_Equal(t, r, "shrink called on leaf")
			} else {
				t.Fatal("shrink should panic on leaf")
			}
		}()
		leaf.shrink()
	})

	// Test other leaf methods that should work
	require_True(t, leaf.isFull())
	require_True(t, leaf.base() == nil)
	require_Equal(t, leaf.numChildren(), uint16(0))
	require_True(t, leaf.children() == nil)

	// Test iter (should do nothing)
	called := false
	leaf.iter(func(n node) bool {
		called = true
		return true
	})
	require_False(t, called)
}

func TestSizeOnNilTree(t *testing.T) {
	var st *SubjectTree[int]
	require_Equal(t, st.Size(), 0)
}

func TestFindEdgeCases(t *testing.T) {
	st := NewSubjectTree[int]()

	// Test Find with empty subject at root level
	st.Insert(b("foo.bar.baz"), 1)
	st.Insert(b("foo"), 2)

	// This should create a tree structure, now test finding with edge cases
	v, found := st.Find(b(""))
	require_False(t, found)
	require_True(t, v == nil)
}

func TestNodeIterMethods(t *testing.T) {
	// Test node4 iter
	n4 := newNode4(b("prefix"))
	n4.addChild('a', newLeaf(b("1"), 1))
	n4.addChild('b', newLeaf(b("2"), 2))

	count := 0
	n4.iter(func(n node) bool {
		count++
		return true
	})
	require_Equal(t, count, 2)

	// Test early termination
	count = 0
	n4.iter(func(n node) bool {
		count++
		return false
	})
	require_Equal(t, count, 1)

	// Test node10 iter
	n10 := newNode10(b("prefix"))
	for i := 0; i < 5; i++ {
		n10.addChild(byte('a'+i), newLeaf([]byte{byte('0' + i)}, i))
	}

	count = 0
	n10.iter(func(n node) bool {
		count++
		return true
	})
	require_Equal(t, count, 5)

	// Test node16 iter
	n16 := newNode16(b("prefix"))
	for i := 0; i < 8; i++ {
		n16.addChild(byte('a'+i), newLeaf([]byte{byte('0' + i)}, i))
	}

	count = 0
	n16.iter(func(n node) bool {
		count++
		return true
	})
	require_Equal(t, count, 8)
}

func TestIterOrderedAndIterFastNilRoot(t *testing.T) {
	// Test IterOrdered with nil root
	st := NewSubjectTree[int]()
	count := 0
	st.IterOrdered(func(subject []byte, val *int) bool {
		count++
		return true
	})
	require_Equal(t, count, 0)

	// Test IterFast with nil root
	count = 0
	st.IterFast(func(subject []byte, val *int) bool {
		count++
		return true
	})
	require_Equal(t, count, 0)
}

func TestNodeAddChildPanic(t *testing.T) {
	// Test node4 addChild panic when full
	n4 := newNode4(b("prefix"))
	n4.addChild('a', newLeaf(b("1"), 1))
	n4.addChild('b', newLeaf(b("2"), 2))
	n4.addChild('c', newLeaf(b("3"), 3))
	n4.addChild('d', newLeaf(b("4"), 4))

	defer func() {
		if r := recover(); r != nil {
			require_Equal(t, r, "node4 full!")
		} else {
			t.Fatal("addChild should panic when node4 is full")
		}
	}()
	n4.addChild('e', newLeaf(b("5"), 5))
}

func TestNodeAddChildPanicOthers(t *testing.T) {
	// Test node10 addChild panic when full
	t.Run("node10", func(t *testing.T) {
		n10 := newNode10(b("prefix"))
		for i := 0; i < 10; i++ {
			n10.addChild(byte('a'+i), newLeaf([]byte{byte('0' + i)}, i))
		}

		defer func() {
			if r := recover(); r != nil {
				require_Equal(t, r, "node10 full!")
			} else {
				t.Fatal("addChild should panic when node10 is full")
			}
		}()
		n10.addChild('k', newLeaf(b("11"), 11))
	})

	// Test node16 addChild panic when full
	t.Run("node16", func(t *testing.T) {
		n16 := newNode16(b("prefix"))
		for i := 0; i < 16; i++ {
			n16.addChild(byte(i), newLeaf([]byte{byte(i)}, i))
		}

		defer func() {
			if r := recover(); r != nil {
				require_Equal(t, r, "node16 full!")
			} else {
				t.Fatal("addChild should panic when node16 is full")
			}
		}()
		n16.addChild(16, newLeaf(b("16"), 16))
	})

	// Test node48 addChild panic when full
	t.Run("node48", func(t *testing.T) {
		n48 := newNode48(b("prefix"))
		for i := 0; i < 48; i++ {
			n48.addChild(byte(i), newLeaf([]byte{byte(i)}, i))
		}

		defer func() {
			if r := recover(); r != nil {
				require_Equal(t, r, "node48 full!")
			} else {
				t.Fatal("addChild should panic when node48 is full")
			}
		}()
		n48.addChild(48, newLeaf(b("48"), 48))
	})
}

func TestNodeDeleteChildNotFound(t *testing.T) {
	// Test node10 deleteChild when child doesn't exist
	n10 := newNode10(b("prefix"))
	n10.addChild('a', newLeaf(b("1"), 1))
	n10.addChild('b', newLeaf(b("2"), 2))

	// Try to delete non-existent child
	n10.deleteChild('z')
	require_Equal(t, n10.size, uint16(2)) // Size should remain unchanged

	// Test node16 deleteChild when child doesn't exist
	n16 := newNode16(b("prefix"))
	n16.addChild('a', newLeaf(b("1"), 1))
	n16.addChild('b', newLeaf(b("2"), 2))

	n16.deleteChild('z')
	require_Equal(t, n16.size, uint16(2))

	// Test node48 deleteChild when child doesn't exist
	n48 := newNode48(b("prefix"))
	n48.addChild(0, newLeaf(b("1"), 1))
	n48.addChild(1, newLeaf(b("2"), 2))

	n48.deleteChild(255)
	require_Equal(t, n48.size, uint16(2))
}

func TestNodeShrinkNotNeeded(t *testing.T) {
	// Test node10 shrink when not needed (has more than 4 children)
	n10 := newNode10(b("prefix"))
	for i := 0; i < 5; i++ {
		n10.addChild(byte('a'+i), newLeaf([]byte{byte('0' + i)}, i))
	}

	shrunk := n10.shrink()
	require_True(t, shrunk == nil) // Should not shrink

	// Test node16 shrink when not needed (has more than 10 children)
	n16 := newNode16(b("prefix"))
	for i := 0; i < 11; i++ {
		n16.addChild(byte(i), newLeaf([]byte{byte(i)}, i))
	}

	shrunk = n16.shrink()
	require_True(t, shrunk == nil) // Should not shrink
}

func TestNode48IterEarlyTermination(t *testing.T) {
	n48 := newNode48(b("prefix"))
	for i := 0; i < 10; i++ {
		n48.addChild(byte(i), newLeaf([]byte{byte(i)}, i))
	}

	count := 0
	n48.iter(func(n node) bool {
		count++
		return false // Stop immediately
	})
	require_Equal(t, count, 1)
}

func TestNode10And16IterEarlyTermination(t *testing.T) {
	// Test node10 early termination
	n10 := newNode10(b("prefix"))
	for i := 0; i < 5; i++ {
		n10.addChild(byte('a'+i), newLeaf([]byte{byte('0' + i)}, i))
	}

	count := 0
	n10.iter(func(n node) bool {
		count++
		return count < 2 // Stop after 2
	})
	require_Equal(t, count, 2)

	// Test node16 early termination
	n16 := newNode16(b("prefix"))
	for i := 0; i < 8; i++ {
		n16.addChild(byte(i), newLeaf([]byte{byte(i)}, i))
	}

	count = 0
	n16.iter(func(n node) bool {
		count++
		return count < 3 // Stop after 3
	})
	require_Equal(t, count, 3)
}

func TestMatchPartsEdgeCases(t *testing.T) {
	// Test the edge case in matchParts that's not covered
	// This is the case where we have a part that needs to be copied and modified

	// Create a complex filter that will trigger the edge case
	filter := b("foo.*.bar.>")
	parts := genParts(filter, nil)

	// Test with a fragment that will cause partial matching
	frag := b("foo.test")
	remaining, matched := matchParts(parts, string(frag))
	require_True(t, matched)
	require_True(t, len(remaining) > 0)
}

func TestInsertEdgeCases(t *testing.T) {
	st := NewSubjectTree[int]()

	// Test inserting with noPivot byte (should fail)
	old, updated := st.Insert([]byte("foo\x7fbar"), 1)
	require_True(t, old == nil)
	require_False(t, updated)
	require_Equal(t, st.Size(), 0) // Should not insert

	// Test the edge case where we need to split with same pivot
	st = NewSubjectTree[int]()
	// This case tests subjects that cause the same pivot after split
	// Both subjects share prefix "a" and have same pivot "." after split
	st.Insert(b("a.b"), 1)
	// Now insert one that will cause the split with same pivot
	st.Insert(b("a.c"), 2)

	require_Equal(t, st.Size(), 2)
}

func TestDeleteEdgeCases(t *testing.T) {
	st := NewSubjectTree[int]()

	// Test delete on empty tree
	val, deleted := st.Delete(b("foo"))
	require_False(t, deleted)
	require_True(t, val == nil)

	// Test delete with empty subject
	st.Insert(b("foo"), 1)
	val, deleted = st.Delete(b(""))
	require_False(t, deleted)
	require_True(t, val == nil)

	// Test delete with subject shorter than prefix
	st = NewSubjectTree[int]()
	st.Insert(b("verylongprefix.suffix"), 1)
	st.Insert(b("verylongprefix.suffix2"), 2)
	val, deleted = st.Delete(b("very"))
	require_False(t, deleted)
	require_True(t, val == nil)
}

func TestMatchEdgeCases(t *testing.T) {
	st := NewSubjectTree[int]()

	// Test match with nil callback
	st.Insert(b("foo.bar"), 1)
	st.Match(b("foo.*"), nil) // Should not panic

	// Test match with empty filter
	count := 0
	st.Match(b(""), func(subject []byte, val *int) {
		count++
	})
	require_Equal(t, count, 0)
}

func TestIterEdgeCases(t *testing.T) {
	st := NewSubjectTree[int]()

	// Add multiple subjects to create a complex tree
	st.Insert(b("a.b.c"), 1)
	st.Insert(b("a.b.d"), 2)
	st.Insert(b("a.c.d"), 3)
	st.Insert(b("b.c.d"), 4)

	// Test iter with early termination at different points
	count := 0
	st.iter(st.root, nil, false, func(subject []byte, val *int) bool {
		count++
		return count < 2
	})
	require_Equal(t, count, 2)
}

func TestLeafIter(t *testing.T) {
	// Test that leaf iter does nothing (it's a no-op)
	leaf := newLeaf(b("test"), 42)
	called := false

	// Call iter with a function that would set called to true
	leaf.iter(func(n node) bool {
		called = true
		return true
	})
	require_False(t, called) // Should never be called since leaf.iter is a no-op

	// Call iter again with a function that returns false
	leaf.iter(func(n node) bool {
		called = true
		return false
	})
	require_False(t, called) // Still should never be called

	// Verify the leaf itself is not affected
	require_True(t, leaf.match(b("test")))
	require_Equal(t, leaf.value, 42)

	// Also test through the node interface to ensure coverage
	var n node = leaf
	called = false
	n.iter(func(child node) bool {
		called = true
		return true
	})
	require_False(t, called) // Still should never be called
}

func TestDeleteChildEdgeCasesMore(t *testing.T) {
	// Test the edge case in node10 deleteChild where we don't swap (last element)
	n10 := newNode10(b("prefix"))
	n10.addChild('a', newLeaf(b("1"), 1))
	n10.addChild('b', newLeaf(b("2"), 2))
	n10.addChild('c', newLeaf(b("3"), 3))

	// Delete the last child
	n10.deleteChild('c')
	require_Equal(t, n10.size, uint16(2))

	// Test the edge case in node16 deleteChild where we don't swap (last element)
	n16 := newNode16(b("prefix"))
	n16.addChild('a', newLeaf(b("1"), 1))
	n16.addChild('b', newLeaf(b("2"), 2))
	n16.addChild('c', newLeaf(b("3"), 3))

	// Delete the last child
	n16.deleteChild('c')
	require_Equal(t, n16.size, uint16(2))
}

func TestMatchPartsMoreEdgeCases(t *testing.T) {
	// Test the remaining 2.6% of matchParts
	// Case where frag is empty
	parts := genParts(b("foo.*"), nil)
	remaining, matched := matchParts(parts, "")
	require_True(t, matched)
	require_Equal(t, len(remaining), len(parts))
}

func TestInsertComplexEdgeCases(t *testing.T) {
	st := NewSubjectTree[int]()

	// Test the recursive insert case with same pivot
	// This requires a very specific setup
	// First, create a tree structure that will trigger the recursive path
	st.Insert(b("a"), 1)
	st.Insert(b("aa"), 2) // This will create a split

	// Now insert something that has the same pivot after split
	st.Insert(b("aaa"), 3) // This should trigger the recursive insert path

	require_Equal(t, st.Size(), 3)

	// Verify all values can be found
	v, found := st.Find(b("a"))
	require_True(t, found)
	require_Equal(t, *v, 1)

	v, found = st.Find(b("aa"))
	require_True(t, found)
	require_Equal(t, *v, 2)

	v, found = st.Find(b("aaa"))
	require_True(t, found)
	require_Equal(t, *v, 3)
}

func TestDeleteNilNodePointer(t *testing.T) {
	st := NewSubjectTree[int]()
	// Test delete with nil node
	var n node
	val, deleted := st.delete(&n, b("foo"), 0)
	require_False(t, deleted)
	require_True(t, val == nil)
}

func TestMatchComplexEdgeCases(t *testing.T) {
	st := NewSubjectTree[int]()

	// Build a complex tree to test the 2.2% uncovered in match
	st.Insert(b("foo.bar.baz"), 1)
	st.Insert(b("foo.bar.qux"), 2)
	st.Insert(b("foo.baz.bar"), 3)
	st.Insert(b("bar.foo.baz"), 4)

	// Test with terminal fwc but no remaining parts
	count := 0
	st.Match(b("foo.bar.>"), func(subject []byte, val *int) {
		count++
	})
	require_Equal(t, count, 2)
}

func TestIterComplexTree(t *testing.T) {
	st := NewSubjectTree[int]()

	// Build a deeper tree to test the remaining iter cases
	for i := 0; i < 20; i++ {
		st.Insert([]byte(fmt.Sprintf("level1.level2.level3.item%d", i)), i)
	}

	// This should create multiple node types and test more paths
	count := 0
	st.IterOrdered(func(subject []byte, val *int) bool {
		count++
		return true
	})
	require_Equal(t, count, 20)
}
