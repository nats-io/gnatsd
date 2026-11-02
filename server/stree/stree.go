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
	"bytes"
	"slices"
	"strings"
	"unsafe"

	"github.com/nats-io/nats-server/v2/server/gsl"
)

// SubjectTree is an adaptive radix trie (ART) for storing subject information on literal subjects.
// Will use dynamic nodes, path compression and lazy expansion.
// The reason this exists is to not only save some memory in our filestore but to greatly optimize matching
// a wildcard subject to certain members, e.g. consumer NumPending calculations.
type SubjectTree[T any] struct {
	root node
	size int
}

// NewSubjectTree creates a new SubjectTree with values T.
func NewSubjectTree[T any]() *SubjectTree[T] {
	return &SubjectTree[T]{}
}

// Size returns the number of elements stored.
func (t *SubjectTree[T]) Size() int {
	if t == nil {
		return 0
	}
	return t.size
}

// Will empty out the tree, or if tree is nil create a new one.
func (t *SubjectTree[T]) Empty() *SubjectTree[T] {
	if t == nil {
		return NewSubjectTree[T]()
	}
	t.root, t.size = nil, 0
	return t
}

// Insert a value into the tree. Will return if the value was updated and if so the old value.
func (t *SubjectTree[T]) Insert(subject []byte, value T) (*T, bool) {
	if t == nil {
		return nil, false
	}

	// Make sure we never insert anything with a noPivot byte.
	if bytes.IndexByte(subject, noPivot) >= 0 {
		return nil, false
	}

	old, updated := t.insert(&t.root, subject, value, 0)
	if !updated {
		t.size++
	}
	return old, updated
}

// Find will find the value and return it or false if it was not found.
func (t *SubjectTree[T]) Find(subject []byte) (*T, bool) {
	if t == nil {
		return nil, false
	}

	var si int
	for n := t.root; n != nil; {
		if n.isLeaf() {
			if ln := n.(*leaf[T]); ln.match(subject[si:]) {
				return &ln.value, true
			}
			return nil, false
		}
		// We are a node type here, grab meta portion.
		if bn := n.base(); len(bn.prefix) > 0 {
			end := min(si+len(bn.prefix), len(subject))
			if string(subject[si:end]) != bn.prefix {
				return nil, false
			}
			// Increment our subject index.
			si += len(bn.prefix)
		}
		if an := n.findChild(pivot(subject, si)); an != nil {
			n = *an
		} else {
			return nil, false
		}
	}
	return nil, false
}

// Delete will delete the item and return its value, or not found if it did not exist.
func (t *SubjectTree[T]) Delete(subject []byte) (*T, bool) {
	if t == nil {
		return nil, false
	}

	val, deleted := t.delete(&t.root, subject, 0)
	if deleted {
		t.size--
	}
	return val, deleted
}

// Match will match against a subject that can have wildcards and invoke the callback func for each matched value.
func (t *SubjectTree[T]) Match(filter []byte, cb func(subject []byte, val *T)) {
	if t == nil || t.root == nil || len(filter) == 0 || cb == nil {
		return
	}
	// We need to break this up into chunks based on wildcards, either pwc '*' or fwc '>'.
	var raw [32][]byte
	parts := genParts(filter, raw[:0])
	var _pre [256]byte
	t.match(t.root, parts, _pre[:0], func(subject []byte, val *T) bool {
		cb(subject, val)
		return true
	})
}

// MatchUntil will match against a subject that can have wildcards and invoke
// the callback func for each matched value.
// Returning false from the callback will stop matching immediately.
// Returns true if matching ran to completion, false if callback stopped it early.
func (t *SubjectTree[T]) MatchUntil(filter []byte, cb func(subject []byte, val *T) bool) bool {
	if t == nil || t.root == nil || len(filter) == 0 || cb == nil {
		return true
	}
	// We need to break this up into chunks based on wildcards, either pwc '*' or fwc '>'.
	var raw [32][]byte
	parts := genParts(filter, raw[:0])
	var _pre [256]byte
	return t.match(t.root, parts, _pre[:0], cb)
}

// IterOrdered will walk all entries in the SubjectTree lexicographically. The callback can return false to terminate the walk.
func (t *SubjectTree[T]) IterOrdered(cb func(subject []byte, val *T) bool) {
	if t == nil || t.root == nil {
		return
	}
	var _pre [256]byte
	t.iter(t.root, _pre[:0], true, cb)
}

// IterFast will walk all entries in the SubjectTree with no guarantees of ordering. The callback can return false to terminate the walk.
func (t *SubjectTree[T]) IterFast(cb func(subject []byte, val *T) bool) {
	if t == nil || t.root == nil {
		return
	}
	var _pre [256]byte
	t.iter(t.root, _pre[:0], false, cb)
}

// Internal methods

// Internal call to insert that can be recursive.
func (t *SubjectTree[T]) insert(np *node, subject []byte, value T, si int) (*T, bool) {
	n := *np
	if n == nil {
		*np = newLeaf(subject, value)
		return nil, false
	}
	if n.isLeaf() {
		ln := n.(*leaf[T])
		if ln.match(subject[si:]) {
			// Replace with new value.
			old := ln.value
			ln.value = value
			return &old, true
		}
		// Here we need to split this leaf.
		cpi := commonPrefixLen(stringToBytes(ln.suffix), subject[si:])
		nn := newNode4(subject[si : si+cpi])
		ln.setSuffix(stringToBytes(ln.suffix[cpi:]))
		si += cpi
		// Make sure we have different pivot, normally this will be the case unless we have overflowing prefixes.
		if p := pivot(stringToBytes(ln.suffix), 0); cpi > 0 && si < len(subject) && p == subject[si] {
			// We need to split the original leaf. Recursively call into insert.
			t.insert(np, subject, value, si)
			// Now add the update version of *np as a child to the new node4.
			nn.addChild(p, *np)
		} else {
			// Can just add this new leaf as a sibling.
			nl := newLeaf(subject[si:], value)
			nn.addChild(pivot(stringToBytes(nl.suffix), 0), nl)
			// Add back original.
			nn.addChild(pivot(stringToBytes(ln.suffix), 0), ln)
		}
		*np = nn
		return nil, false
	}

	// Non-leaf nodes.
	bn := n.base()
	if len(bn.prefix) > 0 {
		cpi := commonPrefixLen(stringToBytes(bn.prefix), subject[si:])
		if pli := len(bn.prefix); cpi >= pli {
			// Move past this node. We look for an existing child node to recurse into.
			// If one does not exist we can create a new leaf node.
			si += pli
			if nn := n.findChild(pivot(subject, si)); nn != nil {
				return t.insert(nn, subject, value, si)
			}
			if n.isFull() {
				n = n.grow()
				*np = n
			}
			n.addChild(pivot(subject, si), newLeaf(subject[si:], value))
			return nil, false
		} else {
			// We did not match the prefix completely here.
			// Calculate new prefix for this node.
			prefix := subject[si : si+cpi]
			si += len(prefix)
			// We will insert a new node4 and attach our current node below after adjusting prefix.
			nn := newNode4(prefix)
			// Shift the prefix for our original node.
			n.setPrefix(stringToBytes(bn.prefix[cpi:]))
			nn.addChild(pivot(stringToBytes(bn.prefix), 0), n)
			// Add in our new leaf.
			nn.addChild(pivot(subject[si:], 0), newLeaf(subject[si:], value))
			// Update our node reference.
			*np = nn
		}
	} else {
		if nn := n.findChild(pivot(subject, si)); nn != nil {
			return t.insert(nn, subject, value, si)
		}
		// No prefix and no matched child, so add in new leafnode as needed.
		if n.isFull() {
			n = n.grow()
			*np = n
		}
		n.addChild(pivot(subject, si), newLeaf(subject[si:], value))
	}

	return nil, false
}

// internal function to recursively find the leaf to delete. Will do compaction if the item is found and removed.
func (t *SubjectTree[T]) delete(np *node, subject []byte, si int) (*T, bool) {
	if t == nil || np == nil || *np == nil || len(subject) == 0 {
		return nil, false
	}
	n := *np
	if n.isLeaf() {
		ln := n.(*leaf[T])
		if ln.match(subject[si:]) {
			*np = nil
			return &ln.value, true
		}
		return nil, false
	}
	// Not a leaf node.
	if bn := n.base(); len(bn.prefix) > 0 {
		// subject could be shorter and would panic on bad index into subject slice.
		if len(subject) < si+len(bn.prefix) {
			return nil, false
		}
		if string(subject[si:si+len(bn.prefix)]) != bn.prefix {
			return nil, false
		}
		// Increment our subject index.
		si += len(bn.prefix)
	}
	p := pivot(subject, si)
	nna := n.findChild(p)
	if nna == nil {
		return nil, false
	}
	nn := *nna
	if nn.isLeaf() {
		ln := nn.(*leaf[T])
		if ln.match(subject[si:]) {
			n.deleteChild(p)

			if sn := n.shrink(); sn != nil {
				bn := n.base()
				pre := bn.prefix
				// Need to fix up prefixes/suffixes.
				if sn.isLeaf() {
					ln := sn.(*leaf[T])
					ln.suffix = pre + ln.suffix
				} else {
					// We are a node here, we need to add in the old prefix.
					if len(pre) > 0 {
						bsn := sn.base()
						bsn.prefix = pre + bsn.prefix
					}
				}
				*np = sn
			}

			return &ln.value, true
		}
		return nil, false
	}
	return t.delete(nna, subject, si)
}

// Internal function which can be called recursively to match all leaf nodes to a given filter subject which
// once here has been decomposed to parts. These parts only care about wildcards, both pwc and fwc.
// Returns false if the callback requested to stop matching.
func (t *SubjectTree[T]) match(n node, parts [][]byte, pre []byte, cb func(subject []byte, val *T) bool) bool {
	// Capture if we are sitting on a terminal fwc.
	var hasFWC bool
	if lp := len(parts); lp > 0 && isFWCPart(parts[lp-1]) {
		hasFWC = true
	}

	for n != nil {
		nparts, matched := matchParts(parts, n.path())
		// Check if we did not match.
		if !matched {
			return true
		}
		// We have matched here. If we are a leaf and have exhausted all parts or he have a FWC fire callback.
		if n.isLeaf() {
			if len(nparts) == 0 || (hasFWC && len(nparts) == 1) {
				ln := n.(*leaf[T])
				if !cb(append(pre, ln.suffix...), &ln.value) {
					return false
				}
			}
			return true
		}
		// We have normal nodes here.
		// We need to append our prefix
		bn := n.base()
		if len(bn.prefix) > 0 {
			// Note that this append may reallocate, but it doesn't modify "pre" at the "match" callsite.
			pre = append(pre, bn.prefix...)
		}

		// Check our remaining parts.
		if len(nparts) == 0 && !hasFWC {
			// We are a node with no parts left and we are not looking at a fwc.
			// We could have a leafnode with no suffix which would be a match.
			// We could also have a terminal pwc. Check for those here.
			var hasTermPWC bool
			if lp := len(parts); lp > 0 && isPWCPart(parts[lp-1]) {
				// If we are sitting on a terminal pwc, put the pwc back and continue.
				nparts = parts[len(parts)-1:]
				hasTermPWC = true
			}
			for _, cn := range n.children() {
				if cn == nil {
					continue
				}
				if cn.isLeaf() {
					ln := cn.(*leaf[T])
					if len(ln.suffix) == 0 {
						if !cb(append(pre, ln.suffix...), &ln.value) {
							return false
						}
					} else if hasTermPWC && strings.IndexByte(ln.suffix, tsep) < 0 {
						if !cb(append(pre, ln.suffix...), &ln.value) {
							return false
						}
					}
				} else if hasTermPWC {
					// We have terminal pwc so call into match again with the child node.
					if !t.match(cn, nparts, pre, cb) {
						return false
					}
				}
			}
			// Return regardless.
			return true
		}
		// If we are sitting on a terminal fwc, put back and continue.
		if hasFWC && len(nparts) == 0 {
			nparts = parts[len(parts)-1:]
		}

		// Here we are a node type with a partial match.
		// Check if the first part is a wildcard.
		fp := nparts[0]
		p := pivot(fp, 0)
		// Check if we have a pwc/fwc part here. This will cause us to iterate.
		if isPWCPart(fp) || isFWCPart(fp) {
			// We need to iterate over all children here for the current node
			// to see if we match further down.
			for _, cn := range n.children() {
				if cn != nil {
					if !t.match(cn, nparts, pre, cb) {
						return false
					}
				}
			}
			return true
		}
		// Here we have normal traversal, so find the next child.
		nn := n.findChild(p)
		if nn == nil {
			return true
		}
		n, parts = *nn, nparts
	}
	return true
}

// Internal iter function to walk nodes in lexicographical order.
func (t *SubjectTree[T]) iter(n node, pre []byte, ordered bool, cb func(subject []byte, val *T) bool) bool {
	if n.isLeaf() {
		ln := n.(*leaf[T])
		return cb(append(pre, ln.suffix...), &ln.value)
	}
	// We are normal node here.
	bn := n.base()
	// Note that this append may reallocate, but it doesn't modify "pre" at the "iter" callsite.
	pre = append(pre, bn.prefix...)
	// Not everything requires lexicographical sorting, so support a fast path for iterating in
	// whatever order the stree has things stored instead.
	if !ordered {
		for _, cn := range n.children() {
			if cn == nil {
				continue
			}
			if !t.iter(cn, pre, false, cb) {
				return false
			}
		}
		return true
	}
	// Collect nodes since unsorted.
	var _nodes [256]node
	nodes := _nodes[:0]
	for _, cn := range n.children() {
		if cn != nil {
			nodes = append(nodes, cn)
		}
	}
	// Now sort.
	slices.SortStableFunc(nodes, func(a, b node) int { return strings.Compare(a.path(), b.path()) })
	// Now walk the nodes in order and call into next iter.
	for i := range nodes {
		if !t.iter(nodes[i], pre, true, cb) {
			return false
		}
	}
	return true
}

// LazyIntersect iterates the smaller of the two provided subject trees and
// looks for matching entries in the other. It is lazy in that it does not
// aggressively optimize against repeated walks, but is considerably faster
// in most cases than intersecting against a potentially large sublist.
func LazyIntersect[TL, TR any](tl *SubjectTree[TL], tr *SubjectTree[TR], cb func([]byte, *TL, *TR)) {
	if tl == nil || tr == nil || tl.root == nil || tr.root == nil {
		return
	}
	// Iterate over the smaller tree to reduce the number of rounds.
	if tl.Size() <= tr.Size() {
		tl.IterFast(func(key []byte, v1 *TL) bool {
			if v2, ok := tr.Find(key); ok {
				cb(key, v1, v2)
			}
			return true
		})
	} else {
		tr.IterFast(func(key []byte, v2 *TR) bool {
			if v1, ok := tl.Find(key); ok {
				cb(key, v1, v2)
			}
			return true
		})
	}
}

// IntersectGSL will match all items in the given subject tree that
// have interest expressed in the given sublist. The callback will only be called
// once for each subject, regardless of overlapping subscriptions in the sublist.
func IntersectGSL[T any, SL comparable](t *SubjectTree[T], sl *gsl.GenericSublist[SL], cb func(subject []byte, val *T) bool) {
	if t == nil || t.root == nil || sl == nil {
		return
	}
	var _pre [256]byte
	_intersectGSL(t.root, _pre[:0], sl, cb)
}

func _intersectGSL[T any, SL comparable](n node, pre []byte, sl *gsl.GenericSublist[SL], cb func(subject []byte, val *T) bool) bool {
	if n.isLeaf() {
		ln := n.(*leaf[T])
		subj := append(pre, ln.suffix...)
		if sl.HasInterest(bytesToString(subj)) {
			return cb(subj, &ln.value)
		}
		return true
	}
	bn := n.base()
	pre = append(pre, bn.prefix...)
	for _, cn := range n.children() {
		if cn == nil {
			continue
		}
		subj := append(pre, cn.path()...)
		if !hasInterestForTokens(sl, subj, len(pre)) {
			continue
		}
		if !_intersectGSL(cn, pre, sl, cb) {
			return false
		}
	}
	return true
}

// The subject tree can return partial tokens so we need to check starting interest
// only from whole tokens when we encounter a tsep.
func hasInterestForTokens[SL comparable](sl *gsl.GenericSublist[SL], subj []byte, since int) bool {
	for i := since; i < len(subj); i++ {
		if subj[i] == tsep {
			if !sl.HasInterestStartingIn(bytesToString(subj[:i])) {
				return false
			}
		}
	}
	return true
}

// Note this will avoid a copy of the data used for the string, but it will also reference the existing slice's data pointer.
// So this should be used sparingly when we know the encompassing byte slice's lifetime is the same.
func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	p := unsafe.SliceData(b)
	return unsafe.String(p, len(b))
}

// Note this will avoid a copy of the string data, but the returned slice must
// only be used for reading since strings are immutable.
func stringToBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	p := unsafe.StringData(s)
	return unsafe.Slice(p, len(s))
}
