// Copyright 2016-2026 The NATS Authors
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
	"bytes"
	"errors"
	"iter"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
)

// Sublist is a routing mechanism to handle subject distribution and
// provides a facility to match subjects from published messages to
// interested subscribers. Subscribers can have wildcard subjects to
// match multiple published subjects.

// Common byte variables for wildcards and token separator.
const (
	pwc   = '*'
	pwcs  = "*"
	fwc   = '>'
	fwcs  = ">"
	tsep  = "."
	btsep = '.'
)

// Sublist related errors
var (
	ErrInvalidSubject    = errors.New("sublist: invalid subject")
	ErrNotFound          = errors.New("sublist: no matches found")
	ErrNilChan           = errors.New("sublist: nil channel")
	ErrAlreadyRegistered = errors.New("sublist: notification already registered")
)

const (
	// cacheMax is used to bound limit the frontend cache
	slCacheMax = 1024
	// If we run a sweeper we will drain to this count.
	slCacheSweep = 256
	// plistMin is our lower bounds to create a fast plist for Match.
	plistMin = 256
)

// SublistResult is a result structure better optimized for queue subs.
type SublistResult struct {
	psubs []*subscription
	qsubs [][]*subscription // don't make this a map, too expensive to iterate
	qwn   []int             // number of weighted remote qsubs at the front of each queue group
}

// SublistFilter is a function that is used to decide if a subscription should
// be returned by the Filter() call.
type SublistFilter func(sub *subscription) bool

// A Sublist stores and efficiently retrieves subscriptions.
type Sublist struct {
	sync.RWMutex
	genid     uint64
	matches   uint64
	cacheHits uint64
	inserts   uint64
	removes   uint64
	root      *level
	cache     map[string]*SublistResult
	ccSweep   int32
	notify    *notifyMaps
	count     uint32
}

// notifyMaps holds maps of arrays of channels for notifications
// on a change of interest.
type notifyMaps struct {
	insert map[string][]chan<- bool
	remove map[string][]chan<- bool
}

// A node contains subscriptions and a pointer to the next level.
type node struct {
	next  *level
	psubs map[*subscription]struct{}
	qsubs map[string]map[*subscription]struct{}
	plist []*subscription
}

// A level represents a group of nodes and special pointers to
// wildcard nodes.
type level struct {
	nodes    map[string]*node
	pwc, fwc *node
}

// Create a new default node.
func newNode() *node {
	return &node{psubs: make(map[*subscription]struct{})}
}

// Create a new default level.
func newLevel() *level {
	return &level{nodes: make(map[string]*node)}
}

// In general caching is recommended however in some extreme cases where
// interest changes are high, suppressing the cache can help.
// https://github.com/nats-io/nats-server/issues/941
// FIXME(dlc) - should be more dynamic at some point based on cache thrashing.

// NewSublist will create a default sublist with caching enabled per the flag.
func NewSublist(enableCache bool) *Sublist {
	if enableCache {
		return &Sublist{root: newLevel(), cache: make(map[string]*SublistResult)}
	}
	return &Sublist{root: newLevel()}
}

// NewSublistForServer will create a default sublist with caching enabled determined
// by the server options.
func NewSublistForServer(srv *Server) *Sublist {
	if srv == nil {
		return NewSublistNoCache() // Probably just unit tests.
	}
	if opts := srv.getOpts(); opts != nil {
		return NewSublist(!opts.NoSublistCache)
	}
	return NewSublistNoCache()
}

// NewSublistWithCache will create a default sublist with caching enabled.
func NewSublistWithCache() *Sublist {
	return NewSublist(true)
}

// NewSublistNoCache will create a default sublist with caching disabled.
func NewSublistNoCache() *Sublist {
	return NewSublist(false)
}

// CacheEnabled returns whether or not caching is enabled for this sublist.
func (s *Sublist) CacheEnabled() bool {
	s.RLock()
	enabled := s.cache != nil
	s.RUnlock()
	return enabled
}

// RegisterNotification will register for notifications when interest for the given
// subject changes. The subject must be a literal publish type subject.
// The notification is true for when the first interest for a subject is inserted,
// and false when all interest in the subject is removed. Note that this interest
// needs to be exact and that wildcards will not trigger the notifications. The sublist
// will not block when trying to send the notification. Its up to the caller to make
// sure the channel send will not block.
func (s *Sublist) RegisterNotification(subject string, notify chan<- bool) error {
	return s.registerNotification(subject, _EMPTY_, notify)
}

func (s *Sublist) RegisterQueueNotification(subject, queue string, notify chan<- bool) error {
	return s.registerNotification(subject, queue, notify)
}

func (s *Sublist) registerNotification(subject, queue string, notify chan<- bool) error {
	if subjectHasWildcard(subject) {
		return ErrInvalidSubject
	}
	if notify == nil {
		return ErrNilChan
	}

	var hasInterest bool
	// Match under the lock so a concurrent insert or remove can't be missed.
	s.Lock()
	r := s.matchNoLock(subject)

	if len(r.psubs)+len(r.qsubs) > 0 {
		if queue == _EMPTY_ {
			for _, sub := range r.psubs {
				if string(sub.subject) == subject {
					hasInterest = true
					break
				}
			}
		} else {
			for _, qsub := range r.qsubs {
				qs := qsub[0]
				if string(qs.subject) == subject && string(qs.queue) == queue {
					hasInterest = true
					break
				}
			}
		}
	}

	key := keyFromSubjectAndQueue(subject, queue)
	var err error

	if s.notify == nil {
		s.notify = &notifyMaps{
			insert: make(map[string][]chan<- bool),
			remove: make(map[string][]chan<- bool),
		}
	}
	// Check which list to add us to.
	if hasInterest {
		err = s.addRemoveNotify(key, notify)
	} else {
		err = s.addInsertNotify(key, notify)
	}
	s.Unlock()

	if err == nil {
		sendNotification(notify, hasInterest)
	}
	return err
}

// Lock should be held.
func chkAndRemove(key string, notify chan<- bool, ms map[string][]chan<- bool) bool {
	chs := ms[key]
	for i, ch := range chs {
		if ch == notify {
			chs[i] = chs[len(chs)-1]
			chs = chs[:len(chs)-1]
			if len(chs) == 0 {
				delete(ms, key)
			}
			return true
		}
	}
	return false
}

func (s *Sublist) ClearNotification(subject string, notify chan<- bool) bool {
	return s.clearNotification(subject, _EMPTY_, notify)
}

func (s *Sublist) ClearQueueNotification(subject, queue string, notify chan<- bool) bool {
	return s.clearNotification(subject, queue, notify)
}

func (s *Sublist) clearNotification(subject, queue string, notify chan<- bool) bool {
	s.Lock()
	if s.notify == nil {
		s.Unlock()
		return false
	}
	key := keyFromSubjectAndQueue(subject, queue)
	// Check both, start with remove.
	didRemove := chkAndRemove(key, notify, s.notify.remove)
	didRemove = didRemove || chkAndRemove(key, notify, s.notify.insert)
	// Check if everything is gone
	if len(s.notify.remove)+len(s.notify.insert) == 0 {
		s.notify = nil
	}
	s.Unlock()
	return didRemove
}

func sendNotification(ch chan<- bool, hasInterest bool) {
	select {
	case ch <- hasInterest:
	default:
	}
}

// Add a new channel for notification in insert map.
// Write lock should be held.
func (s *Sublist) addInsertNotify(subject string, notify chan<- bool) error {
	return s.addNotify(s.notify.insert, subject, notify)
}

// Add a new channel for notification in removal map.
// Write lock should be held.
func (s *Sublist) addRemoveNotify(subject string, notify chan<- bool) error {
	return s.addNotify(s.notify.remove, subject, notify)
}

// Add a new channel for notification.
// Write lock should be held.
func (s *Sublist) addNotify(m map[string][]chan<- bool, subject string, notify chan<- bool) error {
	chs := m[subject]
	if len(chs) > 0 {
		// Check to see if this chan is already registered.
		for _, ch := range chs {
			if ch == notify {
				return ErrAlreadyRegistered
			}
		}
	}

	m[subject] = append(chs, notify)
	return nil
}

// To generate a key from subject and queue. We just add spc.
func keyFromSubjectAndQueue(subject, queue string) string {
	if len(queue) == 0 {
		return subject
	}
	var sb strings.Builder
	sb.WriteString(subject)
	sb.WriteString(" ")
	sb.WriteString(queue)
	return sb.String()
}

// chkForInsertNotification will check to see if we need to notify on this subject.
// Write lock should be held.
func (s *Sublist) chkForInsertNotification(subject, queue string) {
	key := keyFromSubjectAndQueue(subject, queue)

	// All notify subjects are also literal so just do a hash lookup here.
	if chs := s.notify.insert[key]; len(chs) > 0 {
		for _, ch := range chs {
			sendNotification(ch, true)
		}
		// Move from the insert map to the remove map.
		s.notify.remove[key] = append(s.notify.remove[key], chs...)
		delete(s.notify.insert, key)
	}
}

// chkForRemoveNotification will check to see if we need to notify on this subject.
// Write lock should be held.
func (s *Sublist) chkForRemoveNotification(subject, queue string) {
	key := keyFromSubjectAndQueue(subject, queue)
	if chs := s.notify.remove[key]; len(chs) > 0 {
		// We need to always check that we have no interest anymore.
		var hasInterest bool
		r := s.matchNoLock(subject)

		if len(r.psubs)+len(r.qsubs) > 0 {
			if queue == _EMPTY_ {
				for _, sub := range r.psubs {
					if string(sub.subject) == subject {
						hasInterest = true
						break
					}
				}
			} else {
				for _, qsub := range r.qsubs {
					qs := qsub[0]
					if string(qs.subject) == subject && string(qs.queue) == queue {
						hasInterest = true
						break
					}
				}
			}
		}
		if !hasInterest {
			for _, ch := range chs {
				sendNotification(ch, false)
			}
			// Move from the remove map to the insert map.
			s.notify.insert[key] = append(s.notify.insert[key], chs...)
			delete(s.notify.remove, key)
		}
	}
}

// Insert adds a subscription into the sublist
func (s *Sublist) Insert(sub *subscription) error {
	// copy the subject since we hold this and this might be part of a large byte slice.
	subject := string(sub.subject)

	s.Lock()

	var sfwc, haswc, isnew bool
	var n *node
	l := s.root

	for t := range strings.SplitSeq(subject, tsep) {
		lt := len(t)
		if lt == 0 || sfwc {
			s.Unlock()
			return ErrInvalidSubject
		}

		if lt > 1 {
			n = l.nodes[t]
		} else {
			switch t[0] {
			case pwc:
				n = l.pwc
				haswc = true
			case fwc:
				n = l.fwc
				haswc, sfwc = true, true
			default:
				n = l.nodes[t]
			}
		}
		if n == nil {
			n = newNode()
			if lt > 1 {
				l.nodes[t] = n
			} else {
				switch t[0] {
				case pwc:
					l.pwc = n
				case fwc:
					l.fwc = n
				default:
					l.nodes[t] = n
				}
			}
		}
		if n.next == nil {
			n.next = newLevel()
		}
		l = n.next
	}
	if sub.queue == nil {
		n.psubs[sub] = struct{}{}
		isnew = len(n.psubs) == 1
		if n.plist != nil {
			n.plist = append(n.plist, sub)
		} else if len(n.psubs) > plistMin {
			n.plist = make([]*subscription, 0, len(n.psubs))
			// Populate
			for psub := range n.psubs {
				n.plist = append(n.plist, psub)
			}
		}
	} else {
		if n.qsubs == nil {
			n.qsubs = make(map[string]map[*subscription]struct{})
		}
		qname := string(sub.queue)
		// This is a queue subscription
		subs, ok := n.qsubs[qname]
		if !ok {
			subs = make(map[*subscription]struct{})
			n.qsubs[qname] = subs
			isnew = true
		}
		subs[sub] = struct{}{}
	}

	s.count++
	s.inserts++

	s.addToCache(subject, sub)
	atomic.AddUint64(&s.genid, 1)

	if s.notify != nil && isnew && !haswc && len(s.notify.insert) > 0 {
		s.chkForInsertNotification(subject, string(sub.queue))
	}
	s.Unlock()

	return nil
}

// Deep copy
func copyResult(r *SublistResult) *SublistResult {
	nr := &SublistResult{}
	nr.psubs = append([]*subscription(nil), r.psubs...)
	for _, qr := range r.qsubs {
		nqr := append([]*subscription(nil), qr...)
		nr.qsubs = append(nr.qsubs, nqr)
	}
	nr.qwn = append([]int(nil), r.qwn...)
	return nr
}

// Adds a new sub to an existing result.
func (r *SublistResult) addSubToResult(sub *subscription) *SublistResult {
	// Copy since others may have a reference.
	nr := copyResult(r)
	if sub.queue == nil {
		nr.psubs = append(nr.psubs, sub)
	} else {
		i := findQSlot(sub.queue, nr.qsubs)
		if i < 0 {
			i = len(nr.qsubs)
			nr.qsubs = append(nr.qsubs, nil)
		}
		nr.addQSub(i, sub)
	}
	return nr
}

// Adds a queue sub to queue group i, keeping weighted remote qsubs at the front.
func (r *SublistResult) addQSub(i int, sub *subscription) {
	r.qsubs[i] = append(r.qsubs[i], sub)
	if qsubWeight(sub) == 1 {
		return
	}
	for len(r.qwn) <= i {
		r.qwn = append(r.qwn, 0)
	}
	qs, n := r.qsubs[i], r.qwn[i]
	qs[n], qs[len(qs)-1] = qs[len(qs)-1], qs[n]
	r.qwn[i]++
}

// addToCache will add the new entry to the existing cache
// entries if needed. Assumes write lock is held.
// Assumes write lock is held.
func (s *Sublist) addToCache(subject string, sub *subscription) {
	if s.cache == nil {
		return
	}
	// If literal we can direct match.
	if subjectIsLiteral(subject) {
		if r := s.cache[subject]; r != nil {
			s.cache[subject] = r.addSubToResult(sub)
		}
		return
	}
	for key, r := range s.cache {
		if matchLiteral(key, subject) {
			s.cache[key] = r.addSubToResult(sub)
		}
	}
}

// removeFromCache will remove the sub from any active cache entries.
// Assumes write lock is held.
func (s *Sublist) removeFromCache(subject string) {
	if s.cache == nil {
		return
	}
	// If literal we can direct match.
	if subjectIsLiteral(subject) {
		delete(s.cache, subject)
		return
	}
	// Wildcard here.
	for key := range s.cache {
		if matchLiteral(key, subject) {
			delete(s.cache, key)
		}
	}
}

// a place holder for an empty result.
var emptyResult = &SublistResult{}

// Match will match all entries to the literal subject.
// It will return a set of results for both normal and queue subscribers.
func (s *Sublist) Match(subject string) *SublistResult {
	return s.match(subject, true, false)
}

// MatchBytes will match all entries to the literal subject.
// It will return a set of results for both normal and queue subscribers.
func (s *Sublist) MatchBytes(subject []byte) *SublistResult {
	return s.match(bytesToString(subject), true, true)
}

// HasInterest will return whether or not there is any interest in the subject.
// In cases where more detail is not required, this may be faster than Match.
func (s *Sublist) HasInterest(subject string) bool {
	return s.hasInterest(subject, true, nil, nil)
}

// NumInterest will return the number of subs/qsubs interested in the subject.
// In cases where more detail is not required, this may be faster than Match.
func (s *Sublist) NumInterest(subject string) (np, nq int) {
	s.hasInterest(subject, true, &np, &nq)
	return
}

func (s *Sublist) matchNoLock(subject string) *SublistResult {
	return s.match(subject, false, false)
}

func (s *Sublist) match(subject string, doLock bool, doCopyOnCache bool) *SublistResult {
	atomic.AddUint64(&s.matches, 1)

	// Check cache first.
	if doLock {
		s.RLock()
	}
	cacheEnabled := s.cache != nil
	r, ok := s.cache[subject]
	if doLock {
		s.RUnlock()
	}
	if ok {
		atomic.AddUint64(&s.cacheHits, 1)
		return r
	}

	tsa := [32]string{}
	tokens := tsa[:0]
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			if i-start == 0 {
				return emptyResult
			}
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	if start >= len(subject) {
		return emptyResult
	}
	tokens = append(tokens, subject[start:])

	// FIXME(dlc) - Make shared pool between sublist and client readLoop?
	result := &SublistResult{}

	// Get result from the main structure and place into the shared cache.
	// Hold the read lock to avoid race between match and store.
	var n int

	if doLock {
		if cacheEnabled {
			s.Lock()
		} else {
			s.RLock()
		}
	}

	matchLevel(s.root, tokens, result)
	// Check for empty result.
	if len(result.psubs) == 0 && len(result.qsubs) == 0 {
		result = emptyResult
	}
	if cacheEnabled {
		if doCopyOnCache {
			subject = copyString(subject)
		}
		s.cache[subject] = result
		n = len(s.cache)
	}
	if doLock {
		if cacheEnabled {
			s.Unlock()
		} else {
			s.RUnlock()
		}
	}

	// Reduce the cache count if we have exceeded our set maximum.
	if cacheEnabled && n > slCacheMax && atomic.CompareAndSwapInt32(&s.ccSweep, 0, 1) {
		go s.reduceCacheCount()
	}

	return result
}

func (s *Sublist) hasInterest(subject string, doLock bool, np, nq *int) bool {
	// Check cache first.
	if doLock {
		s.RLock()
	}
	var matched, ok bool
	if s.cache != nil {
		var r *SublistResult
		if r, ok = s.cache[subject]; ok {
			if np != nil && nq != nil {
				*np += len(r.psubs)
				for _, qsub := range r.qsubs {
					*nq += len(qsub)
				}
			}
			matched = len(r.psubs)+len(r.qsubs) > 0
		}
	}
	if doLock {
		s.RUnlock()
	}
	if ok {
		atomic.AddUint64(&s.cacheHits, 1)
		return matched
	}

	tsa := [32]string{}
	tokens := tsa[:0]
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			if i-start == 0 {
				return false
			}
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	if start >= len(subject) {
		return false
	}
	tokens = append(tokens, subject[start:])

	if doLock {
		s.RLock()
		defer s.RUnlock()
	}
	return matchLevelForAny(s.root, tokens, np, nq)
}

// Remove entries in the cache until we are under the maximum.
// TODO(dlc) this could be smarter now that its not inline.
func (s *Sublist) reduceCacheCount() {
	defer atomic.StoreInt32(&s.ccSweep, 0)
	// If we are over the cache limit randomly drop until under the limit.
	s.Lock()
	for key := range s.cache {
		delete(s.cache, key)
		if len(s.cache) <= slCacheSweep {
			break
		}
	}
	s.Unlock()
}

// Helper function for weighted remote qsubs.
func isRemoteQSub(sub *subscription) bool {
	return sub != nil && sub.queue != nil && sub.client != nil && (sub.client.kind == ROUTER || sub.client.kind == LEAF)
}

// Returns the selection weight of a queue sub.
// Remote queue subs represent qw members on the remote side.
func qsubWeight(sub *subscription) uint64 {
	if isRemoteQSub(sub) {
		if qw := atomic.LoadInt32(&sub.qw); qw > 1 {
			return uint64(qw)
		}
	}
	return 1
}

// Picks a random index into qsubs, weighted by qsubWeight.
// The first nw subs are weighted remote qsubs, the rest have weight 1.
func weightedQSubIndex(qsubs []*subscription, nw int) int {
	// All weights are 1, so pick uniformly.
	if nw == 0 {
		return int(rand.Uint64N(uint64(len(qsubs))))
	}
	total := uint64(len(qsubs) - nw)
	for _, sub := range qsubs[:nw] {
		total += qsubWeight(sub)
	}
	n := rand.Uint64N(total)
	for i, sub := range qsubs[:nw] {
		w := qsubWeight(sub)
		if n < w {
			return i
		}
		n -= w
	}
	// Past the weighted prefix every sub has weight 1.
	// Clamp in case a weight was lowered concurrently.
	return min(nw+int(n), len(qsubs)-1)
}

// UpdateRemoteQSub should be called when we update the weight of an existing
// remote queue sub.
func (s *Sublist) UpdateRemoteQSub(sub *subscription) {
	// We could search to make sure we find it, but probably not worth
	// it unless we are thrashing the cache. Just remove from our L2 and update
	// the genid so L1 will be flushed.
	s.Lock()
	s.removeFromCache(string(sub.subject))
	atomic.AddUint64(&s.genid, 1)
	s.Unlock()
}

// This will add in a node's results to the total results.
func addNodeToResults(n *node, results *SublistResult) {
	// Normal subscriptions
	if n.plist != nil {
		results.psubs = append(results.psubs, n.plist...)
	} else {
		for psub := range n.psubs {
			results.psubs = append(results.psubs, psub)
		}
	}
	// Queue subscriptions
	for qname, qr := range n.qsubs {
		if len(qr) == 0 {
			continue
		}
		// Need to find matching list in results
		var i int
		if i = findQSlot([]byte(qname), results.qsubs); i < 0 {
			i = len(results.qsubs)
			nqsub := make([]*subscription, 0, len(qr))
			results.qsubs = append(results.qsubs, nqsub)
		}
		// Remote queue subs are added once, their weight is applied on delivery.
		for sub := range qr {
			results.addQSub(i, sub)
		}
	}
}

// We do not use a map here since we want iteration to be past when
// processing publishes in L1 on client. So we need to walk sequentially
// for now. Keep an eye on this in case we start getting large number of
// different queue subscribers for the same subject.
func findQSlot(queue []byte, qsl [][]*subscription) int {
	if queue == nil {
		return -1
	}
	for i, qr := range qsl {
		if len(qr) > 0 && bytes.Equal(queue, qr[0].queue) {
			return i
		}
	}
	return -1
}

// matchLevel is used to recursively descend into the trie.
func matchLevel(l *level, toks []string, results *SublistResult) {
	var pwc, n *node
	for i, t := range toks {
		if l == nil {
			return
		}
		if l.fwc != nil {
			addNodeToResults(l.fwc, results)
		}
		if pwc = l.pwc; pwc != nil {
			matchLevel(pwc.next, toks[i+1:], results)
		}
		n = l.nodes[t]
		if n != nil {
			l = n.next
		} else {
			l = nil
		}
	}
	if n != nil {
		addNodeToResults(n, results)
	}
	if pwc != nil {
		addNodeToResults(pwc, results)
	}
}

func matchLevelForAny(l *level, toks []string, np, nq *int) bool {
	var pwc, n *node
	for i, t := range toks {
		if l == nil {
			return false
		}
		if l.fwc != nil {
			if np != nil && nq != nil {
				*np += len(l.fwc.psubs)
				for _, qsub := range l.fwc.qsubs {
					*nq += len(qsub)
				}
			}
			return true
		}
		if pwc = l.pwc; pwc != nil {
			if match := matchLevelForAny(pwc.next, toks[i+1:], np, nq); match {
				return true
			}
		}
		n = l.nodes[t]
		if n != nil {
			l = n.next
		} else {
			l = nil
		}
	}
	if n != nil {
		if np != nil && nq != nil {
			*np += len(n.psubs)
			for _, qsub := range n.qsubs {
				*nq += len(qsub)
			}
		}
		if len(n.plist) > 0 || len(n.psubs) > 0 || len(n.qsubs) > 0 {
			return true
		}
	}
	if pwc != nil {
		if np != nil && nq != nil {
			*np += len(pwc.psubs)
			for _, qsub := range pwc.qsubs {
				*nq += len(qsub)
			}
		}
		return len(pwc.plist) > 0 || len(pwc.psubs) > 0 || len(pwc.qsubs) > 0
	}
	return false
}

// lnt is used to track descent into levels for a removal for pruning.
type lnt struct {
	l *level
	n *node
	t string
}

// Raw low level remove, can do batches with lock held outside.
func (s *Sublist) remove(sub *subscription, shouldLock bool, doCacheUpdates bool) error {
	subject := string(sub.subject)

	if shouldLock {
		s.Lock()
		defer s.Unlock()
	}

	var sfwc, haswc bool
	var n *node
	l := s.root

	// Track levels for pruning
	var lnts [32]lnt
	levels := lnts[:0]

	for t := range strings.SplitSeq(subject, tsep) {
		lt := len(t)
		if lt == 0 || sfwc {
			return ErrInvalidSubject
		}
		if l == nil {
			return ErrNotFound
		}
		if lt > 1 {
			n = l.nodes[t]
		} else {
			switch t[0] {
			case pwc:
				n = l.pwc
				haswc = true
			case fwc:
				n = l.fwc
				haswc, sfwc = true, true
			default:
				n = l.nodes[t]
			}
		}
		if n != nil {
			levels = append(levels, lnt{l, n, t})
			l = n.next
		} else {
			l = nil
		}
	}
	removed, last := s.removeFromNode(n, sub)
	if !removed {
		return ErrNotFound
	}

	s.count--
	s.removes++

	for i := len(levels) - 1; i >= 0; i-- {
		l, n, t := levels[i].l, levels[i].n, levels[i].t
		if n.isEmpty() {
			l.pruneNode(n, t)
		}
	}
	if doCacheUpdates {
		s.removeFromCache(subject)
		atomic.AddUint64(&s.genid, 1)
	}

	if s.notify != nil && last && !haswc && len(s.notify.remove) > 0 {
		s.chkForRemoveNotification(subject, string(sub.queue))
	}

	return nil
}

// Remove will remove a subscription.
func (s *Sublist) Remove(sub *subscription) error {
	return s.remove(sub, true, true)
}

// RemoveBatch will remove a list of subscriptions.
func (s *Sublist) RemoveBatch(subs []*subscription) error {
	if len(subs) == 0 {
		return nil
	}

	s.Lock()
	defer s.Unlock()

	// TODO(dlc) - We could try to be smarter here for a client going away but the account
	// has a large number of subscriptions compared to this client. Quick and dirty testing
	// though said just disabling all the time best for now.

	// Turn off our cache if enabled.
	wasEnabled := s.cache != nil
	s.cache = nil
	// We will try to remove all subscriptions but will report the first that caused
	// an error. In other words, we don't bail out at the first error which would
	// possibly leave a bunch of subscriptions that could have been removed.
	var err error
	for _, sub := range subs {
		if lerr := s.remove(sub, false, false); lerr != nil && err == nil {
			err = lerr
		}
	}
	// Turn caching back on here.
	atomic.AddUint64(&s.genid, 1)
	if wasEnabled {
		s.cache = make(map[string]*SublistResult)
	}
	return err
}

// pruneNode is used to prune an empty node from the tree.
func (l *level) pruneNode(n *node, t string) {
	if n == nil {
		return
	}
	if n == l.fwc {
		l.fwc = nil
	} else if n == l.pwc {
		l.pwc = nil
	} else {
		delete(l.nodes, t)
	}
}

// isEmpty will test if the node has any entries. Used
// in pruning.
func (n *node) isEmpty() bool {
	if len(n.psubs) == 0 && len(n.qsubs) == 0 {
		if n.next == nil || n.next.numNodes() == 0 {
			return true
		}
	}
	return false
}

// Return the number of nodes for the given level.
func (l *level) numNodes() int {
	if l == nil {
		return 0
	}
	num := len(l.nodes)
	if l.pwc != nil {
		num++
	}
	if l.fwc != nil {
		num++
	}
	return num
}

// Remove the sub for the given node.
func (s *Sublist) removeFromNode(n *node, sub *subscription) (found, last bool) {
	if n == nil {
		return false, true
	}
	if sub.queue == nil {
		_, found = n.psubs[sub]
		delete(n.psubs, sub)
		if found && n.plist != nil {
			// This will brute force remove the plist to perform
			// correct behavior. Will get re-populated on a call
			// to Match as needed.
			n.plist = nil
		}
		return found, len(n.psubs) == 0
	}

	// We have a queue group subscription here
	qsub := n.qsubs[string(sub.queue)]
	_, found = qsub[sub]
	delete(qsub, sub)
	if len(qsub) == 0 {
		// This is the last queue subscription interest when len(qsub) == 0, not
		// when n.qsubs is empty.
		last = true
		delete(n.qsubs, string(sub.queue))
	}
	return found, last
}

// Count returns the number of subscriptions.
func (s *Sublist) Count() uint32 {
	s.RLock()
	defer s.RUnlock()
	return s.count
}

// CacheCount returns the number of result sets in the cache.
func (s *Sublist) CacheCount() int {
	s.RLock()
	cc := len(s.cache)
	s.RUnlock()
	return cc
}

// SublistStats are public stats for the sublist
type SublistStats struct {
	NumSubs      uint32  `json:"num_subscriptions"`
	NumCache     uint32  `json:"num_cache"`
	NumInserts   uint64  `json:"num_inserts"`
	NumRemoves   uint64  `json:"num_removes"`
	NumMatches   uint64  `json:"num_matches"`
	CacheHitRate float64 `json:"cache_hit_rate"`
	MaxFanout    uint32  `json:"max_fanout"`
	AvgFanout    float64 `json:"avg_fanout"`
	totFanout    int
	cacheCnt     int
	cacheHits    uint64
}

func (s *SublistStats) add(stat *SublistStats) {
	s.NumSubs += stat.NumSubs
	s.NumCache += stat.NumCache
	s.NumInserts += stat.NumInserts
	s.NumRemoves += stat.NumRemoves
	s.NumMatches += stat.NumMatches
	s.cacheHits += stat.cacheHits
	if s.MaxFanout < stat.MaxFanout {
		s.MaxFanout = stat.MaxFanout
	}

	// ignore slStats.AvgFanout, collect the values
	// it's based on instead
	s.totFanout += stat.totFanout
	s.cacheCnt += stat.cacheCnt
	if s.totFanout > 0 {
		s.AvgFanout = float64(s.totFanout) / float64(s.cacheCnt)
	}
	if s.NumMatches > 0 {
		s.CacheHitRate = float64(s.cacheHits) / float64(s.NumMatches)
	}
}

// Stats will return a stats structure for the current state.
func (s *Sublist) Stats() *SublistStats {
	st := &SublistStats{}

	s.RLock()
	cache := s.cache
	cc := len(s.cache)
	st.NumSubs = s.count
	st.NumInserts = s.inserts
	st.NumRemoves = s.removes
	s.RUnlock()

	st.NumCache = uint32(cc)
	st.NumMatches = atomic.LoadUint64(&s.matches)
	st.cacheHits = atomic.LoadUint64(&s.cacheHits)
	if st.NumMatches > 0 {
		st.CacheHitRate = float64(st.cacheHits) / float64(st.NumMatches)
	}

	// whip through cache for fanout stats, this can be off if cache is full and doing evictions.
	// If this is called frequently, which it should not be, this could hurt performance.
	if cache != nil {
		tot, max, clen := 0, 0, 0
		s.RLock()
		for _, r := range s.cache {
			clen++
			l := len(r.psubs) + len(r.qsubs)
			tot += l
			if l > max {
				max = l
			}
		}
		s.RUnlock()
		st.totFanout = tot
		st.cacheCnt = clen
		st.MaxFanout = uint32(max)
		if tot > 0 {
			st.AvgFanout = float64(tot) / float64(clen)
		}
	}
	return st
}

// numLevels will return the maximum number of levels
// contained in the Sublist tree.
func (s *Sublist) numLevels() int {
	return visitLevel(s.root, 0)
}

// visitLevel is used to descend the Sublist tree structure
// recursively.
func visitLevel(l *level, depth int) int {
	if l == nil || l.numNodes() == 0 {
		return depth
	}

	depth++
	maxDepth := depth

	for _, n := range l.nodes {
		if n == nil {
			continue
		}
		newDepth := visitLevel(n.next, depth)
		if newDepth > maxDepth {
			maxDepth = newDepth
		}
	}
	if l.pwc != nil {
		pwcDepth := visitLevel(l.pwc.next, depth)
		if pwcDepth > maxDepth {
			maxDepth = pwcDepth
		}
	}
	if l.fwc != nil {
		fwcDepth := visitLevel(l.fwc.next, depth)
		if fwcDepth > maxDepth {
			maxDepth = fwcDepth
		}
	}
	return maxDepth
}

// Determine if a subject has any wildcard tokens.
func subjectHasWildcard(subject string) bool {
	// This one exits earlier then !subjectIsLiteral(subject)
	for i, c := range subject {
		if c == pwc || c == fwc {
			if (i == 0 || subject[i-1] == btsep) &&
				(i+1 == len(subject) || subject[i+1] == btsep) {
				return true
			}
		}
	}
	return false
}

// Determine if the subject has any wildcards. Fast version, does not check for
// valid subject. Used in caching layer.
func subjectIsLiteral(subject string) bool {
	for i, c := range subject {
		if c == pwc || c == fwc {
			if (i == 0 || subject[i-1] == btsep) &&
				(i+1 == len(subject) || subject[i+1] == btsep) {
				return false
			}
		}
	}
	return true
}

// IsValidPublishSubject returns true if a subject is valid and a literal, false otherwise
func IsValidPublishSubject(subject string) bool {
	return IsValidSubject(subject) && subjectIsLiteral(subject)
}

// IsValidSubject returns true if a subject is valid, false otherwise
func IsValidSubject(subject string) bool {
	return isValidSubject(subject, false)
}

func isValidSubject(subject string, checkRunes bool) bool {
	if subject == _EMPTY_ {
		return false
	}
	if checkRunes {
		// Check if we have embedded nulls.
		if bytes.IndexByte(stringToBytes(subject), 0) >= 0 {
			return false
		}
		// Since casting to a string will always produce valid UTF-8, we need to look for replacement runes.
		// This signals something is off or corrupt.
		for _, r := range subject {
			if r == utf8.RuneError {
				return false
			}
		}
	}
	sfwc := false
	for t := range strings.SplitSeq(subject, tsep) {
		length := len(t)
		if length == 0 || sfwc {
			return false
		}
		if length > 1 {
			if strings.ContainsAny(t, "\t\n\f\r ") {
				return false
			}
			continue
		}
		switch t[0] {
		case fwc:
			sfwc = true
		case ' ', '\t', '\n', '\r', '\f':
			return false
		}
	}
	return true
}

// IsValidLiteralSubject returns true if a subject is valid and literal (no wildcards), false otherwise
func IsValidLiteralSubject(subject string) bool {
	return isValidLiteralSubject(strings.SplitSeq(subject, tsep))
}

// isValidLiteralSubject returns true if the tokens are valid and literal (no wildcards), false otherwise
func isValidLiteralSubject(tokens iter.Seq[string]) bool {
	for t := range tokens {
		if len(t) == 0 {
			return false
		}
		if len(t) > 1 {
			continue
		}
		switch t[0] {
		case pwc, fwc:
			return false
		}
	}
	return true
}

// ValidateMapping returns nil error if the subject is a valid subject mapping destination subject
func ValidateMapping(src string, dest string) error {
	if dest == _EMPTY_ {
		return nil
	}
	sfwc := false
	for t := range strings.SplitSeq(dest, tsep) {
		length := len(t)
		if length == 0 || sfwc {
			return &mappingDestinationErr{t, ErrInvalidMappingDestinationSubject}
		}

		// if it looks like it contains a mapping function, it should be a valid mapping function
		if isMappingFunctionToken(t) {
			// Account scoped functions must have been resolved before the mapping is validated.
			// Better error in case some (future) code path builds a transform without expanding it first.
			if isAccountMappingFunction(t) {
				return &mappingDestinationErr{t, ErrMappingFunctionNotAvailable}
			}
			if !partitionMappingFunctionRegEx.MatchString(t) &&
				!wildcardMappingFunctionRegEx.MatchString(t) &&
				!splitFromLeftMappingFunctionRegEx.MatchString(t) &&
				!splitFromRightMappingFunctionRegEx.MatchString(t) &&
				!sliceFromLeftMappingFunctionRegEx.MatchString(t) &&
				!sliceFromRightMappingFunctionRegEx.MatchString(t) &&
				!splitMappingFunctionRegEx.MatchString(t) &&
				!leftMappingFunctionRegEx.MatchString(t) &&
				!rightMappingFunctionRegEx.MatchString(t) &&
				!randomMappingFunctionRegEx.MatchString(t) {
				return &mappingDestinationErr{t, ErrUnknownMappingDestinationFunction}
			} else {
				continue
			}
		}

		if length == 1 && t[0] == fwc {
			sfwc = true
		} else if strings.ContainsAny(t, "\t\n\f\r ") {
			return ErrInvalidMappingDestinationSubject
		}
	}

	// Finally, verify that the transform can actually be created from the source and destination
	_, err := NewSubjectTransform(src, dest)
	return err
}

// Will check tokens and report back if the have any partial or full wildcards.
func analyzeTokens(tokens []string) (hasPWC, hasFWC bool) {
	for _, t := range tokens {
		if lt := len(t); lt == 0 || lt > 1 {
			continue
		}
		switch t[0] {
		case pwc:
			hasPWC = true
		case fwc:
			hasFWC = true
		}
	}
	return
}

// Check on a token basis if they could match.
func tokensCanMatch(t1, t2 string) bool {
	if len(t1) == 0 || len(t2) == 0 {
		return false
	}
	t1c, t2c := t1[0], t2[0]
	if t1c == pwc || t2c == pwc || t1c == fwc || t2c == fwc {
		return true
	}
	return t1 == t2
}

// SubjectsCollide will determine if two subjects could both match a single literal subject.
func SubjectsCollide(subj1, subj2 string) bool {
	if subj1 == subj2 {
		return true
	}
	tsa, tsb := [32]string{}, [32]string{}
	toks1 := tokenizeSubjectIntoSlice(tsa[:0], subj1)
	toks2 := tokenizeSubjectIntoSlice(tsb[:0], subj2)
	pwc1, fwc1 := analyzeTokens(toks1)
	pwc2, fwc2 := analyzeTokens(toks2)
	// if both literal just string compare.
	l1, l2 := !(pwc1 || fwc1), !(pwc2 || fwc2)
	if l1 && l2 {
		return subj1 == subj2
	}
	// So one or both have wildcards. If one is literal than we can do subset matching.
	if l1 && !l2 {
		return isSubsetMatchTokenized(toks1, toks2)
	} else if l2 && !l1 {
		return isSubsetMatchTokenized(toks2, toks1)
	}
	// Both have wildcards.
	// If they only have partials then the lengths must match.
	if !fwc1 && !fwc2 && len(toks1) != len(toks2) {
		return false
	}
	if lt1, lt2 := len(toks1), len(toks2); lt1 != lt2 {
		// If the shorter one only has partials then these will not collide.
		if lt1 < lt2 && !fwc1 || lt2 < lt1 && !fwc2 {
			return false
		}
	}

	stop := len(toks1)
	if len(toks2) < stop {
		stop = len(toks2)
	}

	// We look for reasons to say no.
	for i := 0; i < stop; i++ {
		t1, t2 := toks1[i], toks2[i]
		if !tokensCanMatch(t1, t2) {
			return false
		}
	}

	return true
}

// Returns number of tokens in the subject.
func numTokens(subject string) int {
	var numTokens int
	if len(subject) == 0 {
		return 0
	}
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			numTokens++
		}
	}
	return numTokens + 1
}

// Fast way to return an indexed token.
// This is one based, so first token is TokenAt(subject, 1)
func tokenAt(subject string, index uint8) string {
	ti, start := uint8(1), 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			if ti == index {
				return subject[start:i]
			}
			start = i + 1
			ti++
		}
	}
	if ti == index {
		return subject[start:]
	}
	return _EMPTY_
}

// use similar to append. meaning, the updated slice will be returned
func tokenizeSubjectIntoSlice(tts []string, subject string) []string {
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			tts = append(tts, subject[start:i])
			start = i + 1
		}
	}
	tts = append(tts, subject[start:])
	return tts
}

// SubjectMatchesFilter returns true if the subject matches the provided
// filter or false otherwise.
func SubjectMatchesFilter(subject, filter string) bool {
	return subjectIsSubsetMatch(subject, filter)
}

// Calls into the function isSubsetMatch()
func subjectIsSubsetMatch(subject, test string) bool {
	tsa := [32]string{}
	tts := tokenizeSubjectIntoSlice(tsa[:0], subject)
	return isSubsetMatch(tts, test)
}

// This will test a subject as an array of tokens against a test subject
// Calls into the function isSubsetMatchTokenized
func isSubsetMatch(tokens []string, test string) bool {
	tsa := [32]string{}
	tts := tokenizeSubjectIntoSlice(tsa[:0], test)
	return isSubsetMatchTokenized(tokens, tts)
}

// This will test a subject as an array of tokens against a test subject (also encoded as array of tokens)
// and determine if the tokens are matched. Both test subject and tokens
// may contain wildcards. So foo.* is a subset match of [">", "*.*", "foo.*"],
// but not of foo.bar, etc.
func isSubsetMatchTokenized(tokens, test []string) bool {
	// Walk the target tokens
	for i, t2 := range test {
		if i >= len(tokens) {
			return false
		}
		l := len(t2)
		if l == 0 {
			return false
		}
		if t2[0] == fwc && l == 1 {
			return true
		}
		t1 := tokens[i]

		l = len(t1)
		if l == 0 || t1[0] == fwc && l == 1 {
			return false
		}

		if t1[0] == pwc && len(t1) == 1 {
			m := t2[0] == pwc && len(t2) == 1
			if !m {
				return false
			}
			if i >= len(test) {
				return true
			}
			continue
		}
		if t2[0] != pwc && strings.Compare(t1, t2) != 0 {
			return false
		}
	}
	return len(tokens) == len(test)
}

// matchLiteral is used to test literal subjects, those that do not have any
// wildcards, with a target subject. This is used in the cache layer.
func matchLiteral(literal, subject string) bool {
	li := 0
	ll := len(literal)
	ls := len(subject)
	for i := 0; i < ls; i++ {
		if li >= ll {
			return false
		}
		// This function has been optimized for speed.
		// For instance, do not set b:=subject[i] here since
		// we may bump `i` in this loop to avoid `continue` or
		// skipping common test in a particular test.
		// Run Benchmark_SublistMatchLiteral before making any change.
		switch subject[i] {
		case pwc:
			// NOTE: This is not testing validity of a subject, instead ensures
			// that wildcards are treated as such if they follow some basic rules,
			// namely that they are a token on their own.
			if i == 0 || subject[i-1] == btsep {
				if i == ls-1 {
					// There is no more token in the subject after this wildcard.
					// Skip token in literal and expect to not find a separator.
					for {
						// End of literal, this is a match.
						if li >= ll {
							return true
						}
						// Presence of separator, this can't be a match.
						if literal[li] == btsep {
							return false
						}
						li++
					}
				} else if subject[i+1] == btsep {
					// There is another token in the subject after this wildcard.
					// Skip token in literal and expect to get a separator.
					for {
						// We found the end of the literal before finding a separator,
						// this can't be a match.
						if li >= ll {
							return false
						}
						if literal[li] == btsep {
							break
						}
						li++
					}
					// Bump `i` since we know there is a `.` following, we are
					// safe. The common test below is going to check `.` with `.`
					// which is good. A `continue` here is too costly.
					i++
				}
			}
		case fwc:
			// For `>` to be a wildcard, it means being the only or last character
			// in the string preceded by a `.`
			if (i == 0 || subject[i-1] == btsep) && i == ls-1 {
				return true
			}
		}
		if subject[i] != literal[li] {
			return false
		}
		li++
	}
	// Make sure we have processed all of the literal's chars..
	return li >= ll
}

func addLocalSub(sub *subscription, subs *[]*subscription, includeLeafHubs bool) {
	if sub != nil && sub.client != nil {
		kind := sub.client.kind
		if kind == CLIENT || kind == SYSTEM || kind == JETSTREAM || kind == ACCOUNT ||
			(includeLeafHubs && sub.client.isHubLeafNode() /* implied kind==LEAF */) {
			*subs = append(*subs, sub)
		}
	}
}

func (s *Sublist) addNodeToSubs(n *node, subs *[]*subscription, includeLeafHubs bool) {
	// Normal subscriptions
	if n.plist != nil {
		for _, sub := range n.plist {
			addLocalSub(sub, subs, includeLeafHubs)
		}
	} else {
		for sub := range n.psubs {
			addLocalSub(sub, subs, includeLeafHubs)
		}
	}
	// Queue subscriptions
	for _, qr := range n.qsubs {
		for sub := range qr {
			addLocalSub(sub, subs, includeLeafHubs)
		}
	}
}

func (s *Sublist) collectLocalSubs(l *level, subs *[]*subscription, includeLeafHubs bool) {
	for _, n := range l.nodes {
		s.addNodeToSubs(n, subs, includeLeafHubs)
		s.collectLocalSubs(n.next, subs, includeLeafHubs)
	}
	if l.pwc != nil {
		s.addNodeToSubs(l.pwc, subs, includeLeafHubs)
		s.collectLocalSubs(l.pwc.next, subs, includeLeafHubs)
	}
	if l.fwc != nil {
		s.addNodeToSubs(l.fwc, subs, includeLeafHubs)
		s.collectLocalSubs(l.fwc.next, subs, includeLeafHubs)
	}
}

// Return all local client subscriptions. Use the supplied slice.
func (s *Sublist) localSubs(subs *[]*subscription, includeLeafHubs bool) {
	s.RLock()
	s.collectLocalSubs(s.root, subs, includeLeafHubs)
	s.RUnlock()
}

// All is used to collect all subscriptions.
func (s *Sublist) All(subs *[]*subscription) {
	s.RLock()
	s.collectAllSubs(s.root, subs, nil)
	s.RUnlock()
}

func (s *Sublist) Filter(subs *[]*subscription, filter SublistFilter) {
	s.RLock()
	s.collectAllSubs(s.root, subs, filter)
	s.RUnlock()
}

func (s *Sublist) addAllNodeToSubs(n *node, subs *[]*subscription, filter SublistFilter) {
	// Normal subscriptions
	if n.plist != nil {
		if filter == nil {
			*subs = append(*subs, n.plist...)
		} else {
			for _, sub := range n.plist {
				if filter(sub) {
					*subs = append(*subs, sub)
				}
			}
		}
	} else {
		for sub := range n.psubs {
			if filter != nil && !filter(sub) {
				continue
			}
			*subs = append(*subs, sub)
		}
	}
	// Queue subscriptions
	for _, qr := range n.qsubs {
		for sub := range qr {
			if filter != nil && !filter(sub) {
				continue
			}
			*subs = append(*subs, sub)
		}
	}
}

func (s *Sublist) collectAllSubs(l *level, subs *[]*subscription, filter SublistFilter) {
	for _, n := range l.nodes {
		s.addAllNodeToSubs(n, subs, filter)
		s.collectAllSubs(n.next, subs, filter)
	}
	if l.pwc != nil {
		s.addAllNodeToSubs(l.pwc, subs, filter)
		s.collectAllSubs(l.pwc.next, subs, filter)
	}
	if l.fwc != nil {
		s.addAllNodeToSubs(l.fwc, subs, filter)
		s.collectAllSubs(l.fwc.next, subs, filter)
	}
}

// For a given subject (which may contain wildcards), this call returns all
// subscriptions that would match that subject. For instance, suppose that
// the sublist contains: foo.bar, foo.bar.baz and foo.baz, ReverseMatch("foo.*")
// would return foo.bar and foo.baz.
// This is used in situations where the sublist is likely to contain only
// literals and one wants to get all the subjects that would have been a match
// to a subscription on `subject`.
func (s *Sublist) ReverseMatch(subject string) *SublistResult {
	tsa := [32]string{}
	tokens := tsa[:0]
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	tokens = append(tokens, subject[start:])

	result := &SublistResult{}

	s.RLock()
	reverseMatchLevel(s.root, tokens, nil, result)
	// Check for empty result.
	if len(result.psubs) == 0 && len(result.qsubs) == 0 {
		result = emptyResult
	}
	s.RUnlock()

	return result
}

func reverseMatchLevel(l *level, toks []string, n *node, results *SublistResult) {
	if l == nil {
		return
	}
	for i, t := range toks {
		if len(t) == 1 {
			if t[0] == fwc {
				getAllNodes(l, results)
				return
			} else if t[0] == pwc {
				for _, n := range l.nodes {
					reverseMatchLevel(n.next, toks[i+1:], n, results)
				}
				if l.pwc != nil {
					reverseMatchLevel(l.pwc.next, toks[i+1:], n, results)
				}
				if l.fwc != nil {
					getAllNodes(l, results)
				}
				return
			}
		}
		// If the sub tree has a fwc at this position, match as well.
		if l.fwc != nil {
			getAllNodes(l, results)
			return
		} else if l.pwc != nil {
			reverseMatchLevel(l.pwc.next, toks[i+1:], n, results)
		}
		n = l.nodes[t]
		if n == nil {
			break
		}
		l = n.next
	}
	if n != nil {
		addNodeToResults(n, results)
	}
}

func getAllNodes(l *level, results *SublistResult) {
	if l == nil {
		return
	}
	if l.pwc != nil {
		addNodeToResults(l.pwc, results)
	}
	if l.fwc != nil {
		addNodeToResults(l.fwc, results)
	}
	for _, n := range l.nodes {
		addNodeToResults(n, results)
		getAllNodes(n.next, results)
	}
}
