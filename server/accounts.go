// Copyright 2018-2026 The NATS Authors
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
	"cmp"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"math/rand/v2"
	"net/http"
	"net/textproto"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

// For backwards compatibility with NATS < 2.0, users who are not explicitly defined into an
// account will be grouped in the default global account.
const globalAccountName = DEFAULT_GLOBAL_ACCOUNT

const defaultMaxSubLimitReportThreshold = int64(2 * time.Second)

var maxSubLimitReportThreshold = defaultMaxSubLimitReportThreshold

// Account are subject namespace definitions. By default no messages are shared between accounts.
// You can share via Exports and Imports of Streams and Services.
type Account struct {
	// Total stats for the account.
	stats struct {
		sync.Mutex
		stats       // Totals
		gw    stats // Gateways
		rt    stats // Routes
		ln    stats // Leafnodes
	}

	gwReplyMapping
	Name         string
	Nkey         string
	Issuer       string
	claimJWT     string
	updated      time.Time
	mu           sync.RWMutex
	smu          sync.Mutex // serializes route interest updates
	cmu          sync.Mutex // serializes claim updates
	sl           *Sublist
	ic           *client
	sq           *sendq
	isid         uint64
	etmr         *time.Timer
	ctmr         *time.Timer
	strack       map[string]sconns
	nrclients    int32
	sysclients   int32
	nleafs       int32
	nrleafs      int32
	clients      map[*client]struct{}
	rm           map[string]int64
	lws          map[string]int32 // per key, last queueWeight(rm[key]) sent to routes; used to dedup sends
	usersRevoked map[string]int64
	mappings     []*mapping
	hasMapped    atomic.Bool
	lmu          sync.RWMutex
	lleafs       []*client
	leafClusters map[string]uint64
	imports      importMap
	exports      exportMap
	js           *jsAccount
	jsLimits     map[string]JetStreamAccountLimits
	nrgAccount   string
	limits
	expired      atomic.Bool
	incomplete   bool
	signingKeys  map[string]jwt.Scope
	extAuth      *jwt.ExternalAuthorization
	srv          *Server // server this account is registered with (possibly nil)
	lds          string  // loop detection subject for leaf nodes
	siReply      []byte  // service reply prefix, will form wildcard subscription.
	eventIds     *nuid.NUID
	eventIdsMu   sync.Mutex
	defaultPerms *Permissions
	tags         jwt.TagList
	nameTag      string
	lastLimErr   int64
	routePoolIdx int
	// If the trace destination is specified and a message with a traceParentHdr
	// is received, and has the least significant bit of the last token set to 1,
	// then if traceDestSampling is > 0 and < 100, a random value will be selected
	// and if it falls between 0 and that value, message tracing will be triggered.
	traceDest         string
	traceDestSampling int
	// Guarantee that only one goroutine can be running either checkJetStreamMigrate
	// or clearObserverState at a given time for this account to prevent interleaving.
	jscmMu sync.Mutex
}

const (
	accDedicatedRoute                = -1
	accTransitioningToDedicatedRoute = -2
)

// Account based limits.
type limits struct {
	mpay           int32
	msubs          int32
	mconns         int32
	mleafs         int32
	disallowBearer bool
}

// Used to track remote clients and leafnodes per remote server.
type sconns struct {
	conns int32
	leafs int32
}

// clampInt64ToInt32 safely converts an int64 limit to int32,
// clamping values to the [math.MinInt32, math.MaxInt32] range.
func clampInt64ToInt32(v int64) int32 {
	return int32(max(math.MinInt32, min(math.MaxInt32, v)))
}

// Import stream mapping struct
type streamImport struct {
	acc     *Account
	from    string
	to      string
	tr      *subjectTransform
	rtr     *subjectTransform
	claim   *jwt.Import
	usePub  bool
	invalid bool
	// This is `allow_trace` and when true and message tracing is happening,
	// we will trace egresses past the account boundary, if `false`, we stop
	// at the account boundary.
	atrc bool
}

const ClientInfoHdr = "Nats-Request-Info"

// Import service mapping struct
type serviceImport struct {
	acc         *Account
	claim       *jwt.Import
	se          *serviceExport
	sid         []byte
	from        string
	to          string
	tr          *subjectTransform
	ts          int64
	rt          ServiceRespType
	latency     *serviceLatency
	m1          *ServiceLatency
	rc          *client
	mt          *msgTrace
	usePub      bool
	response    bool
	invalid     bool
	share       bool
	tracking    bool
	didDeliver  bool
	atrc        bool        // allow trace (got from service export)
	trackingHdr http.Header // header from request
}

// This is used to record when we create a mapping for implicit service
// imports. We use this to clean up entries that are not singletons when
// we detect that interest is no longer present. The key to the map will
// be the actual interest. We record the mapped subject and the account.
type serviceRespEntry struct {
	acc  *Account
	msub string
}

// rrIndexThreshold is the number of outstanding responses sharing a single
// reply subject above which we build an index for constant time removal.
// Reply subjects are normally unique per request, so the overwhelming majority
// of entries stay a one element list and never allocate the index.
const rrIndexThreshold = 16

// respEntries holds the outstanding response entries for one reply subject.
// The list is authoritative and unordered; idx, when present, maps a response
// subject to its position in the list. Once allocated the index is kept for as
// long as the reply subject has any entry left, since a subject that has been
// shared once tends to be shared again.
//
// This is held by value in importMap.rrMap, so mutating it means writing it
// back under its key. Add and remove entries through importMap.addRespEntry
// and importMap.removeRespEntry, which keep that write back in one place,
// rather than calling the methods below directly.
type respEntries struct {
	list []*serviceRespEntry
	idx  map[string]int
}

// add appends an entry, building or maintaining the index as needed.
func (re *respEntries) add(sre *serviceRespEntry) {
	re.list = append(re.list, sre)
	if re.idx != nil {
		re.idx[sre.msub] = len(re.list) - 1
		return
	}
	if len(re.list) > rrIndexThreshold {
		re.idx = make(map[string]int, len(re.list))
		for i, e := range re.list {
			re.idx[e.msub] = i
		}
	}
}

// remove drops the entry for msub, in constant time once indexed.
func (re *respEntries) remove(msub string) {
	i := -1
	if re.idx != nil {
		var ok bool
		if i, ok = re.idx[msub]; !ok {
			return
		}
		delete(re.idx, msub)
	} else {
		for j, e := range re.list {
			if e.msub == msub {
				i = j
				break
			}
		}
		if i < 0 {
			return
		}
	}
	// Swap with the last entry so removal does not shift the whole list.
	last := len(re.list) - 1
	if i != last {
		moved := re.list[last]
		re.list[i] = moved
		if re.idx != nil {
			re.idx[moved.msub] = i
		}
	}
	re.list[last] = nil
	re.list = re.list[:last]
}

// ServiceRespType represents the types of service request response types.
type ServiceRespType uint8

// Service response types. Defaults to a singleton.
const (
	Singleton ServiceRespType = iota
	Streamed
	Chunked
)

// String helper.
func (rt ServiceRespType) String() string {
	switch rt {
	case Singleton:
		return "Singleton"
	case Streamed:
		return "Streamed"
	case Chunked:
		return "Chunked"
	}
	return "Unknown ServiceResType"
}

// exportAuth holds configured approvals or boolean indicating an
// auth token is required for import.
type exportAuth struct {
	tokenReq    bool
	accountPos  uint
	approved    map[string]*Account
	actsRevoked map[string]int64
}

// streamExport
type streamExport struct {
	exportAuth
}

// serviceExport holds additional information for exported services.
type serviceExport struct {
	exportAuth
	acc        *Account
	respType   ServiceRespType
	latency    *serviceLatency
	rtmr       *time.Timer
	respThresh time.Duration
	// This is `allow_trace` and when true and message tracing is happening,
	// when processing a service import we will go through account boundary
	// and trace egresses on that other account. If `false`, we stop at the
	// account boundary.
	atrc bool
}

// Used to track service latency.
type serviceLatency struct {
	sampling int8 // percentage from 1-100 or 0 to indicate triggered by header
	subject  string
}

// exportMap tracks the exported streams and services.
type exportMap struct {
	streams   map[string]*streamExport
	services  map[string]*serviceExport
	responses map[string]*serviceImport
}

// importMap tracks the imported streams and services.
// For services we will also track the response mappings as well.
type importMap struct {
	streams  []*streamImport
	services map[string][]*serviceImport
	rrMap    map[string]respEntries
}

// addRespEntry records an outstanding response entry under reply.
// Lock should be held on the owning account.
func (im *importMap) addRespEntry(reply string, sre *serviceRespEntry) {
	if im.rrMap == nil {
		im.rrMap = make(map[string]respEntries)
	}
	re := im.rrMap[reply]
	re.add(sre)
	im.rrMap[reply] = re
}

// removeRespEntry drops the outstanding response entry for msub under reply,
// dropping the reply subject itself once its last entry goes.
// Lock should be held on the owning account.
func (im *importMap) removeRespEntry(reply, msub string) {
	re, ok := im.rrMap[reply]
	if !ok {
		return
	}
	re.remove(msub)
	if len(re.list) == 0 {
		delete(im.rrMap, reply)
	} else {
		im.rrMap[reply] = re
	}
}

// NewAccount creates a new unlimited account with the given name.
func NewAccount(name string) *Account {
	a := &Account{
		Name:     name,
		limits:   limits{-1, -1, -1, -1, false},
		eventIds: nuid.New(),
	}
	return a
}

func (a *Account) String() string {
	return a.Name
}

func (a *Account) setTraceDest(dest string) {
	a.mu.Lock()
	a.traceDest = dest
	a.mu.Unlock()
}

func (a *Account) getTraceDestAndSampling() (string, int) {
	a.mu.RLock()
	dest := a.traceDest
	sampling := a.traceDestSampling
	a.mu.RUnlock()
	return dest, sampling
}

// Used to create shallow copies of accounts for transfer
// from opts to real accounts in server struct.
// Account `na` write lock is expected to be held on entry
// while account `a` is the one from the Options struct
// being loaded/reloaded and do not need locking.
func (a *Account) shallowCopy(na *Account) {
	na.Nkey = a.Nkey
	na.Issuer = a.Issuer
	na.traceDest, na.traceDestSampling = a.traceDest, a.traceDestSampling
	na.nrgAccount = a.nrgAccount

	if a.imports.streams != nil {
		na.imports.streams = make([]*streamImport, 0, len(a.imports.streams))
		for _, v := range a.imports.streams {
			si := *v
			na.imports.streams = append(na.imports.streams, &si)
		}
	}
	if a.imports.services != nil {
		na.imports.services = make(map[string][]*serviceImport)
		for k, v := range a.imports.services {
			sis := make([]*serviceImport, 0, len(v))
			for _, si := range v {
				csi := *si
				sis = append(sis, &csi)
			}
			na.imports.services[k] = sis
		}
	}
	if a.exports.streams != nil {
		na.exports.streams = make(map[string]*streamExport)
		for k, v := range a.exports.streams {
			if v != nil {
				se := *v
				na.exports.streams[k] = &se
			} else {
				na.exports.streams[k] = nil
			}
		}
	}
	if a.exports.services != nil {
		na.exports.services = make(map[string]*serviceExport)
		for k, v := range a.exports.services {
			if v != nil {
				se := *v
				na.exports.services[k] = &se
			} else {
				na.exports.services[k] = nil
			}
		}
	}
	na.mappings = a.mappings
	na.hasMapped.Store(len(na.mappings) > 0)

	// JetStream
	na.jsLimits = a.jsLimits
	// Server config account limits.
	na.limits = a.limits
}

// nextEventID uses its own lock for better concurrency.
func (a *Account) nextEventID() string {
	a.eventIdsMu.Lock()
	id := a.eventIds.Next()
	a.eventIdsMu.Unlock()
	return id
}

// Returns a slice of clients stored in the account, or nil if none is present.
// Lock is held on entry.
func (a *Account) getClientsLocked() []*client {
	if len(a.clients) == 0 {
		return nil
	}
	clients := make([]*client, 0, len(a.clients))
	for c := range a.clients {
		clients = append(clients, c)
	}
	return clients
}

// Returns a slice of clients stored in the account, or nil if none is present.
func (a *Account) getClients() []*client {
	a.mu.RLock()
	clients := a.getClientsLocked()
	a.mu.RUnlock()
	return clients
}

// Returns a slice of external (non-internal) clients stored in the account, or nil if none is present.
// Lock is held on entry.
func (a *Account) getExternalClientsLocked() []*client {
	if len(a.clients) == 0 {
		return nil
	}
	var clients []*client
	for c := range a.clients {
		if !isInternalClient(c.kind) {
			clients = append(clients, c)
		}
	}
	return clients
}

// Called to track a remote server and connections and leafnodes it
// has for this account.
func (a *Account) updateRemoteServer(m *AccountNumConns) []*client {
	a.mu.Lock()
	if a.strack == nil {
		a.strack = make(map[string]sconns)
	}
	// This does not depend on receiving all updates since each one is idempotent.
	// FIXME(dlc) - We should cleanup when these both go to zero.
	prev := a.strack[m.Server.ID]
	a.strack[m.Server.ID] = sconns{conns: int32(m.Conns), leafs: int32(m.LeafNodes)}
	a.nrclients += int32(m.Conns) - prev.conns
	a.nrleafs += int32(m.LeafNodes) - prev.leafs

	mtce := a.mconns != jwt.NoLimit && (len(a.clients)-int(a.sysclients)+int(a.nrclients) > int(a.mconns))
	// If we are over here some have snuck in and we need to rebalance.
	// All others will probably be doing the same thing but better to be
	// conservative and bit harsh here. Clients will reconnect if we over compensate.
	var clients []*client
	if mtce {
		clients = a.getExternalClientsLocked()

		// Sort in reverse chronological.
		slices.SortFunc(clients, func(i, j *client) int { return -i.start.Compare(j.start) })
		over := (len(a.clients) - int(a.sysclients) + int(a.nrclients)) - int(a.mconns)
		if over < len(clients) {
			clients = clients[:over]
		}
	}
	// Now check leafnodes.
	mtlce := a.mleafs != jwt.NoLimit && (a.nleafs+a.nrleafs > a.mleafs)
	if mtlce {
		// Take ones from the end.
		a.lmu.RLock()
		leafs := a.lleafs
		over := int(a.nleafs + a.nrleafs - a.mleafs)
		if over < len(leafs) {
			leafs = leafs[len(leafs)-over:]
		}
		clients = append(clients, leafs...)
		a.lmu.RUnlock()
	}
	a.mu.Unlock()

	// If we have exceeded our max clients this will be populated.
	return clients
}

// Removes tracking for a remote server that has shutdown.
func (a *Account) removeRemoteServer(sid string) {
	a.mu.Lock()
	if a.strack != nil {
		prev := a.strack[sid]
		delete(a.strack, sid)
		a.nrclients -= prev.conns
		a.nrleafs -= prev.leafs
	}
	a.mu.Unlock()
}

// When querying for subject interest this is the number of
// expected responses. We need to actually check that the entry
// has active connections.
func (a *Account) expectedRemoteResponses() (expected int32) {
	a.mu.RLock()
	for _, sc := range a.strack {
		if sc.conns > 0 || sc.leafs > 0 {
			expected++
		}
	}
	a.mu.RUnlock()
	return
}

// Clears eventing and tracking for this account.
func (a *Account) clearEventing() {
	a.mu.Lock()
	a.nrclients = 0
	// Now clear state
	clearTimer(&a.etmr)
	clearTimer(&a.ctmr)
	a.clients = nil
	a.strack = nil
	a.mu.Unlock()
}

// GetName will return the accounts name.
func (a *Account) GetName() string {
	if a == nil {
		return "n/a"
	}
	a.mu.RLock()
	name := a.Name
	a.mu.RUnlock()
	return name
}

// getNameTag will return the name tag or the account name if not set.
func (a *Account) getNameTag() string {
	if a == nil {
		return _EMPTY_
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.getNameTagLocked()
}

// getNameTagLocked will return the name tag or the account name if not set.
// Lock should be held.
func (a *Account) getNameTagLocked() string {
	if a == nil {
		return _EMPTY_
	}
	nameTag := a.nameTag
	if nameTag == _EMPTY_ {
		nameTag = a.Name
	}
	return nameTag
}

// NumConnections returns active number of clients for this account for
// all known servers.
func (a *Account) NumConnections() int {
	a.mu.RLock()
	nc := len(a.clients) - int(a.sysclients) + int(a.nrclients)
	a.mu.RUnlock()
	return nc
}

// NumRemoteConnections returns the number of client or leaf connections that
// are not on this server.
func (a *Account) NumRemoteConnections() int {
	a.mu.RLock()
	nc := int(a.nrclients + a.nrleafs)
	a.mu.RUnlock()
	return nc
}

// NumLocalConnections returns active number of clients for this account
// on this server.
func (a *Account) NumLocalConnections() int {
	a.mu.RLock()
	nlc := a.numLocalConnections()
	a.mu.RUnlock()
	return nlc
}

// Do not account for the system accounts.
func (a *Account) numLocalConnections() int {
	return len(a.clients) - int(a.sysclients) - int(a.nleafs)
}

// This is for extended local interest.
// Lock should not be held.
func (a *Account) numLocalAndLeafConnections() int {
	a.mu.RLock()
	nlc := len(a.clients) - int(a.sysclients)
	a.mu.RUnlock()
	return nlc
}

func (a *Account) numLocalLeafNodes() int {
	return int(a.nleafs)
}

// MaxTotalConnectionsReached returns if we have reached our limit for number of connections.
func (a *Account) MaxTotalConnectionsReached() bool {
	var mtce bool
	a.mu.RLock()
	if a.mconns != jwt.NoLimit {
		mtce = len(a.clients)-int(a.sysclients)+int(a.nrclients) >= int(a.mconns)
	}
	a.mu.RUnlock()
	return mtce
}

// maxTotalConnectionsExceeded returns true if the account has more client
// connections than its limit, counting connections already registered.
func (a *Account) maxTotalConnectionsExceeded() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.mconns != jwt.NoLimit && len(a.clients)-int(a.sysclients)+int(a.nrclients) > int(a.mconns)
}

// MaxActiveConnections return the set limit for the account system
// wide for total number of active connections.
func (a *Account) MaxActiveConnections() int {
	a.mu.RLock()
	mconns := int(a.mconns)
	a.mu.RUnlock()
	return mconns
}

// MaxTotalLeafNodesReached returns if we have reached our limit for number of leafnodes.
func (a *Account) MaxTotalLeafNodesReached() bool {
	a.mu.RLock()
	mtc := a.maxTotalLeafNodesReached()
	a.mu.RUnlock()
	return mtc
}

func (a *Account) maxTotalLeafNodesReached() bool {
	if a.mleafs != jwt.NoLimit {
		return a.nleafs+a.nrleafs >= a.mleafs
	}
	return false
}

// NumLeafNodes returns the active number of local and remote
// leaf node connections.
func (a *Account) NumLeafNodes() int {
	a.mu.RLock()
	nln := int(a.nleafs + a.nrleafs)
	a.mu.RUnlock()
	return nln
}

// NumRemoteLeafNodes returns the active number of remote
// leaf node connections.
func (a *Account) NumRemoteLeafNodes() int {
	a.mu.RLock()
	nrn := int(a.nrleafs)
	a.mu.RUnlock()
	return nrn
}

// MaxActiveLeafNodes return the set limit for the account system
// wide for total number of leavenode connections.
// NOTE: these are tracked separately.
func (a *Account) MaxActiveLeafNodes() int {
	a.mu.RLock()
	mleafs := int(a.mleafs)
	a.mu.RUnlock()
	return mleafs
}

// RoutedSubs returns how many subjects we would send across a route when first
// connected or expressing interest. Local client subs.
func (a *Account) RoutedSubs() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.rm)
}

// TotalSubs returns total number of Subscriptions for this account.
func (a *Account) TotalSubs() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.sl == nil {
		return 0
	}
	return int(a.sl.Count())
}

func (a *Account) shouldLogMaxSubErr() bool {
	if a == nil {
		return true
	}
	a.mu.RLock()
	last := a.lastLimErr
	a.mu.RUnlock()
	if now := time.Now().UnixNano(); now-last >= maxSubLimitReportThreshold {
		a.mu.Lock()
		a.lastLimErr = now
		a.mu.Unlock()
		return true
	}
	return false
}

// MapDest is for mapping published subjects for clients.
type MapDest struct {
	Subject string `json:"subject"`
	Weight  uint8  `json:"weight"`
	Cluster string `json:"cluster,omitempty"`
}

func NewMapDest(subject string, weight uint8) *MapDest {
	return &MapDest{subject, weight, _EMPTY_}
}

// destination is for internal representation for a weighted mapped destination.
type destination struct {
	tr     *subjectTransform
	weight uint8
}

// mapping is an internal entry for mapping subjects.
type mapping struct {
	src    string
	wc     bool
	dests  []*destination
	cdests map[string][]*destination
}

// AddMapping adds in a simple route mapping from src subject to dest subject
// for inbound client messages.
func (a *Account) AddMapping(src, dest string) error {
	return a.AddWeightedMappings(src, NewMapDest(dest, 100))
}

// AddWeightedMappings will add in a weighted mappings for the destinations.
func (a *Account) AddWeightedMappings(src string, dests ...*MapDest) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !IsValidSubject(src) {
		return ErrBadSubject
	}

	m := &mapping{src: src, wc: subjectHasWildcard(src), dests: make([]*destination, 0, len(dests)+1)}
	seen := make(map[string]struct{})

	tw := make(map[string]uint8)
	for _, d := range dests {
		if _, ok := seen[d.Subject]; ok {
			return fmt.Errorf("duplicate entry for %q", d.Subject)
		}
		seen[d.Subject] = struct{}{}
		if d.Weight > 100 {
			return fmt.Errorf("individual weights need to be <= 100")
		}
		tw[d.Cluster] += d.Weight
		if tw[d.Cluster] > 100 {
			return fmt.Errorf("total weight needs to be <= 100")
		}
		err := ValidateMapping(src, d.Subject)
		if err != nil {
			return err
		}
		tr, err := NewSubjectTransform(src, d.Subject)
		if err != nil {
			return err
		}
		if d.Cluster == _EMPTY_ {
			m.dests = append(m.dests, &destination{tr, d.Weight})
		} else {
			// We have a cluster scoped filter.
			if m.cdests == nil {
				m.cdests = make(map[string][]*destination)
			}
			ad := m.cdests[d.Cluster]
			ad = append(ad, &destination{tr, d.Weight})
			m.cdests[d.Cluster] = ad
		}
	}

	processDestinations := func(dests []*destination) ([]*destination, error) {
		var ltw uint8
		for _, d := range dests {
			ltw += d.weight
		}
		// Auto add in original at weight difference if all entries weight does not total to 100.
		// Iff the src was not already added in explicitly, meaning they want loss.
		_, haveSrc := seen[src]
		if ltw != 100 && !haveSrc {
			dest := src
			if m.wc {
				// We need to make the appropriate markers for the wildcards etc.
				dest = transformTokenize(dest)
			}
			tr, err := NewSubjectTransform(src, dest)
			if err != nil {
				return nil, err
			}
			aw := 100 - ltw
			if len(dests) == 0 {
				aw = 100
			}
			dests = append(dests, &destination{tr, aw})
		}
		slices.SortFunc(dests, func(i, j *destination) int { return cmp.Compare(i.weight, j.weight) })

		var lw uint8
		for _, d := range dests {
			d.weight += lw
			lw = d.weight
		}
		return dests, nil
	}

	var err error
	if m.dests, err = processDestinations(m.dests); err != nil {
		return err
	}

	// Option cluster scoped destinations
	for cluster, dests := range m.cdests {
		if dests, err = processDestinations(dests); err != nil {
			return err
		}
		m.cdests[cluster] = dests
	}

	// Replace an old one if it exists.
	for i, em := range a.mappings {
		if em.src == src {
			a.mappings[i] = m
			return nil
		}
	}
	// If we did not replace add to the end.
	a.mappings = append(a.mappings, m)
	a.hasMapped.Store(len(a.mappings) > 0)

	// If we have connected leafnodes make sure to update.
	if a.nleafs > 0 {
		// Need to release because lock ordering is client -> account
		a.mu.Unlock()
		// Now grab the leaf list lock. We can hold client lock under this one.
		a.lmu.RLock()
		for _, lc := range a.lleafs {
			lc.forceAddToSmap(src)
		}
		a.lmu.RUnlock()
		a.mu.Lock()
	}
	return nil
}

// RemoveMapping will remove an existing mapping.
func (a *Account) RemoveMapping(src string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i, m := range a.mappings {
		if m.src == src {
			// Swap last one into this spot. Its ok to change order.
			a.mappings[i] = a.mappings[len(a.mappings)-1]
			a.mappings[len(a.mappings)-1] = nil // gc
			a.mappings = a.mappings[:len(a.mappings)-1]
			a.hasMapped.Store(len(a.mappings) > 0)
			// If we have connected leafnodes make sure to update.
			if a.nleafs > 0 {
				// Need to release because lock ordering is client -> account
				a.mu.Unlock()
				// Now grab the leaf list lock. We can hold client lock under this one.
				a.lmu.RLock()
				for _, lc := range a.lleafs {
					lc.forceRemoveFromSmap(src)
				}
				a.lmu.RUnlock()
				a.mu.Lock()
			}
			return true
		}
	}
	return false
}

// Indicates we have mapping entries.
func (a *Account) hasMappings() bool {
	if a == nil {
		return false
	}
	return a.hasMapped.Load()
}

// This performs the logic to map to a new dest subject based on mappings.
// Should only be called from processInboundClientMsg or service import processing.
func (a *Account) selectMappedSubject(dest string) (string, bool) {
	if !a.hasMappings() {
		return dest, false
	}

	a.mu.RLock()
	// In case we have to tokenize for subset matching.
	tsa := [32]string{}
	tts := tsa[:0]

	var m *mapping
	for _, rm := range a.mappings {
		if !rm.wc && rm.src == dest {
			m = rm
			break
		} else {
			// tokenize and reuse for subset matching.
			if len(tts) == 0 {
				start := 0
				subject := dest
				for i := 0; i < len(subject); i++ {
					if subject[i] == btsep {
						tts = append(tts, subject[start:i])
						start = i + 1
					}
				}
				tts = append(tts, subject[start:])
			}
			if isSubsetMatch(tts, rm.src) {
				m = rm
				break
			}
		}
	}

	if m == nil {
		a.mu.RUnlock()
		return dest, false
	}

	// The selected destination for the mapping.
	var d *destination
	var ndest string

	dests := m.dests
	if len(m.cdests) > 0 {
		cn := a.srv.cachedClusterName()
		dests = m.cdests[cn]
		if dests == nil {
			// Fallback to main if we do not match the cluster.
			dests = m.dests
		}
	}

	// Optimize for single entry case.
	if len(dests) == 1 && dests[0].weight == 100 {
		d = dests[0]
	} else {
		w := uint8(rand.Uint32N(100))
		for _, rm := range dests {
			if w < rm.weight {
				d = rm
				break
			}
		}
	}

	if d != nil {
		if len(d.tr.dtokmftokindexesargs) == 0 {
			ndest = d.tr.dest
		} else {
			ndest = d.tr.TransformTokenizedSubject(tts)
		}
	}

	a.mu.RUnlock()
	return ndest, true
}

// SubscriptionInterest returns true if this account has a matching subscription
// for the given `subject`.
func (a *Account) SubscriptionInterest(subject string) bool {
	return a.Interest(subject) > 0
}

// Interest returns the number of subscriptions for a given subject that match.
func (a *Account) Interest(subject string) int {
	var nms int
	a.mu.RLock()
	if a.sl != nil {
		np, nq := a.sl.NumInterest(subject)
		nms = np + nq
	}
	a.mu.RUnlock()
	return nms
}

// addClient keeps our accounting of local active clients or leafnodes updated.
// Returns previous total.
func (a *Account) addClient(c *client) int {
	a.mu.Lock()
	n := len(a.clients)

	// Could come here earlier than the account is registered with the server.
	// Make sure we can still track clients.
	if a.clients == nil {
		a.clients = make(map[*client]struct{})
	}
	a.clients[c] = struct{}{}

	// If we did not add it, we are done
	if n == len(a.clients) {
		a.mu.Unlock()
		return n
	}
	if c.kind != CLIENT && c.kind != LEAF {
		a.sysclients++
	} else if c.kind == LEAF {
		a.nleafs++
	}
	isGlobal := a.Name == globalAccountName
	a.mu.Unlock()

	// If we added a new leaf use the list lock and add it to the list.
	if c.kind == LEAF {
		a.lmu.Lock()
		a.lleafs = append(a.lleafs, c)
		a.lmu.Unlock()
	}

	if !isGlobal && c != nil && c.srv != nil {
		c.srv.accConnsUpdate(a)
	}

	return n
}

// For registering clusters for remote leafnodes.
// We only register as the hub.
func (a *Account) registerLeafNodeCluster(cluster string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.leafClusters == nil {
		a.leafClusters = make(map[string]uint64)
	}
	a.leafClusters[cluster]++
}

// Check to see if we already have this cluster registered.
func (a *Account) hasLeafNodeCluster(cluster string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.leafClusters[cluster] > 0
}

// Check to see if this cluster is isolated, meaning the only one.
// Read Lock should be held.
func (a *Account) isLeafNodeClusterIsolated(cluster string) bool {
	if cluster == _EMPTY_ {
		return false
	}
	if len(a.leafClusters) > 1 {
		return false
	}
	return a.leafClusters[cluster] == uint64(a.nleafs)
}

// Helper function to remove leaf nodes. If number of leafnodes gets large
// this may need to be optimized out of linear search but believe number
// of active leafnodes per account scope to be small and therefore cache friendly.
// Lock should not be held on general account lock.
func (a *Account) removeLeafNode(c *client) {
	// Make sure we hold the list lock as well.
	a.lmu.Lock()
	defer a.lmu.Unlock()

	ll := len(a.lleafs)
	for i, l := range a.lleafs {
		if l == c {
			a.lleafs[i] = a.lleafs[ll-1]
			a.lleafs[ll-1] = nil
			if ll == 1 {
				a.lleafs = nil
			} else {
				a.lleafs = a.lleafs[:ll-1]
			}
			return
		}
	}
}

// removeClient keeps our accounting of local active clients updated.
func (a *Account) removeClient(c *client) int {
	a.mu.Lock()
	n := len(a.clients)
	delete(a.clients, c)
	// If we did not actually remove it, we are done.
	if n == len(a.clients) {
		a.mu.Unlock()
		return n
	}
	if c.kind != CLIENT && c.kind != LEAF {
		a.sysclients--
	} else if c.kind == LEAF {
		a.nleafs--
		// Need to do cluster accounting here.
		// Do cluster accounting if we are a hub.
		if c.isHubLeafNode() {
			cluster := c.remoteCluster()
			if count := a.leafClusters[cluster]; count > 1 {
				a.leafClusters[cluster]--
			} else if count == 1 {
				delete(a.leafClusters, cluster)
			}
		}
	}
	isGlobal := a.Name == globalAccountName
	a.mu.Unlock()

	if c.kind == LEAF {
		a.removeLeafNode(c)
	}

	if !isGlobal && c != nil && c.srv != nil {
		c.srv.accConnsUpdate(a)
	}

	return n
}

func setExportAuth(ea *exportAuth, subject string, accounts []*Account, accountPos uint) error {
	if accountPos > 0 {
		token := strings.Split(subject, tsep)
		if len(token) < int(accountPos) || token[accountPos-1] != "*" {
			return ErrInvalidSubject
		}
	}
	ea.accountPos = accountPos
	// empty means auth required but will be import token.
	if accounts == nil {
		return nil
	}
	if len(accounts) == 0 {
		ea.tokenReq = true
		return nil
	}
	if ea.approved == nil {
		ea.approved = make(map[string]*Account, len(accounts))
	}
	for _, acc := range accounts {
		ea.approved[acc.Name] = acc
	}
	return nil
}

// AddServiceExport will configure the account with the defined export.
func (a *Account) AddServiceExport(subject string, accounts []*Account) error {
	return a.addServiceExportWithResponseAndAccountPos(subject, Singleton, accounts, 0)
}

// AddServiceExport will configure the account with the defined export.
func (a *Account) addServiceExportWithAccountPos(subject string, accounts []*Account, accountPos uint) error {
	return a.addServiceExportWithResponseAndAccountPos(subject, Singleton, accounts, accountPos)
}

// AddServiceExportWithResponse will configure the account with the defined export and response type.
func (a *Account) AddServiceExportWithResponse(subject string, respType ServiceRespType, accounts []*Account) error {
	return a.addServiceExportWithResponseAndAccountPos(subject, respType, accounts, 0)
}

// AddServiceExportWithresponse will configure the account with the defined export and response type.
func (a *Account) addServiceExportWithResponseAndAccountPos(
	subject string, respType ServiceRespType, accounts []*Account, accountPos uint,
) error {
	if a == nil {
		return ErrMissingAccount
	}

	a.mu.Lock()
	if a.exports.services == nil {
		a.exports.services = make(map[string]*serviceExport)
	}

	se := a.exports.services[subject]
	// Always  create a service export
	if se == nil {
		se = &serviceExport{}
	}

	if respType != Singleton {
		se.respType = respType
	}

	if accounts != nil || accountPos > 0 {
		if err := setExportAuth(&se.exportAuth, subject, accounts, accountPos); err != nil {
			a.mu.Unlock()
			return err
		}
	}
	lrt := a.lowestServiceExportResponseTime()
	se.acc = a
	se.respThresh = DEFAULT_SERVICE_EXPORT_RESPONSE_THRESHOLD
	a.exports.services[subject] = se

	var clients []*client
	nlrt := a.lowestServiceExportResponseTime()
	if nlrt != lrt && len(a.clients) > 0 {
		clients = a.getClientsLocked()
	}
	// Need to release because lock ordering is client -> Account
	a.mu.Unlock()
	if len(clients) > 0 {
		updateAllClientsServiceExportResponseTime(clients, nlrt)
	}
	return nil
}

// TrackServiceExport will enable latency tracking of the named service.
// Results will be published in this account to the given results subject.
func (a *Account) TrackServiceExport(service, results string) error {
	return a.TrackServiceExportWithSampling(service, results, DEFAULT_SERVICE_LATENCY_SAMPLING)
}

// TrackServiceExportWithSampling will enable latency tracking of the named service for the given
// sampling rate (1-100). Results will be published in this account to the given results subject.
func (a *Account) TrackServiceExportWithSampling(service, results string, sampling int) error {
	if a == nil {
		return ErrMissingAccount
	}

	if sampling != 0 { // 0 means triggered by header
		if sampling < 1 || sampling > 100 {
			return ErrBadSampling
		}
	}
	if !IsValidPublishSubject(results) {
		return ErrBadPublishSubject
	}
	// Don't loop back on outselves.
	if a.IsExportService(results) {
		return ErrBadPublishSubject
	}

	if a.srv != nil && !a.srv.EventsEnabled() {
		return ErrNoSysAccount
	}

	a.mu.Lock()
	if a.exports.services == nil {
		a.mu.Unlock()
		return ErrMissingService
	}
	ea, ok := a.exports.services[service]
	if !ok {
		a.mu.Unlock()
		return ErrMissingService
	}
	if ea == nil {
		ea = &serviceExport{}
		a.exports.services[service] = ea
	} else if ea.respType != Singleton {
		a.mu.Unlock()
		return ErrBadServiceType
	}
	ea.latency = &serviceLatency{
		sampling: int8(sampling),
		subject:  results,
	}
	s := a.srv
	a.mu.Unlock()

	if s == nil {
		return nil
	}

	// Now track down the imports and add in latency as needed to enable.
	s.accounts.Range(func(k, v any) bool {
		acc := v.(*Account)
		acc.mu.Lock()
		for _, ims := range acc.imports.services {
			for _, im := range ims {
				if im != nil && im.acc.Name == a.Name && subjectIsSubsetMatch(im.to, service) {
					im.latency = ea.latency
				}
			}
		}
		acc.mu.Unlock()
		return true
	})

	return nil
}

// UnTrackServiceExport will disable latency tracking of the named service.
func (a *Account) UnTrackServiceExport(service string) {
	if a == nil || (a.srv != nil && !a.srv.EventsEnabled()) {
		return
	}

	a.mu.Lock()
	if a.exports.services == nil {
		a.mu.Unlock()
		return
	}
	ea, ok := a.exports.services[service]
	if !ok || ea == nil || ea.latency == nil {
		a.mu.Unlock()
		return
	}
	// We have latency here.
	ea.latency = nil
	s := a.srv
	a.mu.Unlock()

	if s == nil {
		return
	}

	// Now track down the imports and clean them up.
	s.accounts.Range(func(k, v any) bool {
		acc := v.(*Account)
		acc.mu.Lock()
		for _, ims := range acc.imports.services {
			for _, im := range ims {
				if im != nil && im.acc.Name == a.Name {
					if subjectIsSubsetMatch(im.to, service) {
						im.latency, im.m1 = nil, nil
					}
				}
			}
		}
		acc.mu.Unlock()
		return true
	})
}

// IsExportService will indicate if this service exists. Will check wildcard scenarios.
func (a *Account) IsExportService(service string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	_, ok := a.exports.services[service]
	if ok {
		return true
	}
	tokens := strings.Split(service, tsep)
	for subj := range a.exports.services {
		if isSubsetMatch(tokens, subj) {
			return true
		}
	}
	return false
}

// IsExportServiceTracking will indicate if given publish subject is an export service with tracking enabled.
func (a *Account) IsExportServiceTracking(service string) bool {
	a.mu.RLock()
	ea, ok := a.exports.services[service]
	if ok && ea == nil {
		a.mu.RUnlock()
		return false
	}
	if ok && ea != nil && ea.latency != nil {
		a.mu.RUnlock()
		return true
	}
	// FIXME(dlc) - Might want to cache this is in the hot path checking for latency tracking.
	tokens := strings.Split(service, tsep)
	for subj, ea := range a.exports.services {
		if isSubsetMatch(tokens, subj) && ea != nil && ea.latency != nil {
			a.mu.RUnlock()
			return true
		}
	}
	a.mu.RUnlock()
	return false
}

// ServiceLatency is the JSON message sent out in response to latency tracking for
// an accounts exported services. Additional client info is available in requestor
// and responder. Note that for a requestor, the only information shared by default
// is the RTT used to calculate the total latency. The requestor's account can
// designate to share the additional information in the service import.
type ServiceLatency struct {
	TypedEvent
	Status         int           `json:"status"`
	Error          string        `json:"description,omitempty"`
	Requestor      *ClientInfo   `json:"requestor,omitempty"`
	Responder      *ClientInfo   `json:"responder,omitempty"`
	RequestHeader  http.Header   `json:"header,omitempty"` // only contains header(s) triggering the measurement
	RequestStart   time.Time     `json:"start"`
	ServiceLatency time.Duration `json:"service"`
	SystemLatency  time.Duration `json:"system"`
	TotalLatency   time.Duration `json:"total"`
}

// ServiceLatencyType is the NATS Event Type for ServiceLatency
const ServiceLatencyType = "io.nats.server.metric.v1.service_latency"

// NATSTotalTime is a helper function that totals the NATS latencies.
func (m1 *ServiceLatency) NATSTotalTime() time.Duration {
	return m1.Requestor.RTT + m1.Responder.RTT + m1.SystemLatency
}

// Merge function to merge m1 and m2 (requestor and responder) measurements
// when there are two samples. This happens when the requestor and responder
// are on different servers.
//
// m2 ServiceLatency is correct, so use that.
// m1 TotalLatency is correct, so use that.
// Will use those to back into NATS latency.
func (m1 *ServiceLatency) merge(m2 *ServiceLatency) {
	rtt := time.Duration(0)
	if m2.Responder != nil {
		rtt = m2.Responder.RTT
	}
	m1.SystemLatency = m1.ServiceLatency - (m2.ServiceLatency + rtt)
	m1.ServiceLatency = m2.ServiceLatency
	m1.Responder = m2.Responder
	sanitizeLatencyMetric(m1)
}

// sanitizeLatencyMetric adjusts latency metric values that could go
// negative in some edge conditions since we estimate client RTT
// for both requestor and responder.
// These numbers are never meant to be negative, it just could be
// how we back into the values based on estimated RTT.
func sanitizeLatencyMetric(sl *ServiceLatency) {
	if sl.ServiceLatency < 0 {
		sl.ServiceLatency = 0
	}
	if sl.SystemLatency < 0 {
		sl.SystemLatency = 0
	}
}

// Used for transporting remote latency measurements.
type remoteLatency struct {
	Account    string         `json:"account"`
	ReqId      string         `json:"req_id"`
	M2         ServiceLatency `json:"m2"`
	respThresh time.Duration
}

// sendLatencyResult will send a latency result and clear the si of the requestor(rc).
func (a *Account) sendLatencyResult(si *serviceImport, sl *ServiceLatency) {
	sl.Type = ServiceLatencyType
	sl.ID = a.nextEventID()
	sl.Time = time.Now().UTC()
	a.mu.Lock()
	lsubj := si.latency.subject
	si.rc = nil
	a.mu.Unlock()

	a.srv.sendInternalAccountMsg(a, lsubj, sl)
}

// Used to send a bad request metric when we do not have a reply subject
func (a *Account) sendBadRequestTrackingLatency(si *serviceImport, requestor *client, header http.Header) {
	sl := &ServiceLatency{
		Status:    400,
		Error:     "Bad Request",
		Requestor: requestor.getClientInfo(si.share),
	}
	sl.RequestHeader = header
	sl.RequestStart = time.Now().Add(-sl.Requestor.RTT).UTC()
	a.sendLatencyResult(si, sl)
}

// Used to send a latency result when the requestor interest was lost before the
// response could be delivered.
func (a *Account) sendReplyInterestLostTrackLatency(si *serviceImport) {
	sl := &ServiceLatency{
		Status: 408,
		Error:  "Request Timeout",
	}
	a.mu.RLock()
	rc, share, ts := si.rc, si.share, si.ts
	sl.RequestHeader = si.trackingHdr
	a.mu.RUnlock()
	if rc != nil {
		sl.Requestor = rc.getClientInfo(share)
	}
	var reqRTT time.Duration
	if sl.Requestor != nil {
		reqRTT = sl.Requestor.RTT
	}
	sl.RequestStart = time.Unix(0, ts-int64(reqRTT)).UTC()
	a.sendLatencyResult(si, sl)
}

func (a *Account) sendBackendErrorTrackingLatency(si *serviceImport, reason rsiReason) {
	sl := &ServiceLatency{}
	a.mu.RLock()
	rc, share, ts := si.rc, si.share, si.ts
	sl.RequestHeader = si.trackingHdr
	a.mu.RUnlock()
	if rc != nil {
		sl.Requestor = rc.getClientInfo(share)
	}
	var reqRTT time.Duration
	if sl.Requestor != nil {
		reqRTT = sl.Requestor.RTT
	}
	sl.RequestStart = time.Unix(0, ts-int64(reqRTT)).UTC()
	if reason == rsiNoDelivery {
		sl.Status = 503
		sl.Error = "Service Unavailable"
	} else if reason == rsiTimeout {
		sl.Status = 504
		sl.Error = "Service Timeout"
	}
	a.sendLatencyResult(si, sl)
}

// sendTrackingLatency will send out the appropriate tracking information for the
// service request/response latency. This is called when the requestor's server has
// received the response.
// TODO(dlc) - holding locks for RTTs may be too much long term. Should revisit.
func (a *Account) sendTrackingLatency(si *serviceImport, responder *client) bool {
	a.mu.RLock()
	rc, share, siTs := si.rc, si.share, si.ts
	a.mu.RUnlock()
	if rc == nil {
		return true
	}

	ts := time.Now()
	serviceRTT := time.Duration(ts.UnixNano() - siTs)
	requestor := rc

	sl := &ServiceLatency{
		Status:    200,
		Requestor: requestor.getClientInfo(share),
		Responder: responder.getClientInfo(true),
	}
	var respRTT, reqRTT time.Duration
	if sl.Responder != nil {
		respRTT = sl.Responder.RTT
	}
	if sl.Requestor != nil {
		reqRTT = sl.Requestor.RTT
	}
	sl.RequestStart = time.Unix(0, siTs-int64(reqRTT)).UTC()
	sl.ServiceLatency = serviceRTT - respRTT
	sl.TotalLatency = reqRTT + serviceRTT
	if respRTT > 0 {
		sl.SystemLatency = time.Since(ts)
		sl.TotalLatency += sl.SystemLatency
	}
	sl.RequestHeader = si.trackingHdr
	sanitizeLatencyMetric(sl)

	sl.Type = ServiceLatencyType
	sl.ID = a.nextEventID()
	sl.Time = time.Now().UTC()

	// If we are expecting a remote measurement, store our sl here.
	// We need to account for the race between this and us receiving the
	// remote measurement.
	// FIXME(dlc) - We need to clean these up but this should happen
	// already with the auto-expire logic.
	if responder != nil && responder.kind != CLIENT {
		si.acc.mu.Lock()
		if si.m1 != nil {
			m1, m2 := sl, si.m1
			m1.merge(m2)
			si.acc.mu.Unlock()
			a.srv.sendInternalAccountMsg(a, si.latency.subject, m1)
			a.mu.Lock()
			si.rc = nil
			a.mu.Unlock()
			return true
		}
		si.m1 = sl
		si.acc.mu.Unlock()
		return false
	} else {
		a.srv.sendInternalAccountMsg(a, si.latency.subject, sl)
		a.mu.Lock()
		si.rc = nil
		a.mu.Unlock()
	}
	return true
}

// This will check to make sure our response lower threshold is set
// properly in any clients doing rrTracking.
func updateAllClientsServiceExportResponseTime(clients []*client, lrt time.Duration) {
	for _, c := range clients {
		c.mu.Lock()
		if c.rrTracking != nil && lrt != c.rrTracking.lrt {
			c.rrTracking.lrt = lrt
			if c.rrTracking.ptmr.Stop() {
				c.rrTracking.ptmr.Reset(lrt)
			}
		}
		c.mu.Unlock()
	}
}

// Will select the lowest respThresh from all service exports.
// Read lock should be held.
func (a *Account) lowestServiceExportResponseTime() time.Duration {
	// Lowest we will allow is 5 minutes. Its an upper bound for this function.
	lrt := 5 * time.Minute
	for _, se := range a.exports.services {
		if se.respThresh < lrt {
			lrt = se.respThresh
		}
	}
	return lrt
}

// AddServiceImportWithClaim will add in the service import via the jwt claim.
func (a *Account) AddServiceImportWithClaim(destination *Account, from, to string, imClaim *jwt.Import) error {
	return a.addServiceImportWithClaim(destination, from, to, imClaim, false)
}

// addServiceImportWithClaim will add in the service import via the jwt claim.
// It will also skip the authorization check in cases where internal is true
func (a *Account) addServiceImportWithClaim(destination *Account, from, to string, imClaim *jwt.Import, internal bool) error {
	if destination == nil {
		return ErrMissingAccount
	}
	// Empty means use from.
	if to == _EMPTY_ {
		to = from
	}
	if !IsValidSubject(from) || !IsValidSubject(to) {
		return ErrInvalidSubject
	}

	// First check to see if the account has authorized us to route to the "to" subject.
	if !internal && !destination.checkServiceImportAuthorized(a, to, imClaim) {
		return ErrServiceImportAuthorization
	}

	// Check if this introduces a cycle before proceeding.
	// From will be the mapped subject.
	// If the 'to' has a wildcard make sure we pre-transform the 'from' before we check for cycles, e.g. '$1'
	fromT := from
	if subjectHasWildcard(to) {
		fromT, _ = transformUntokenize(from)
	}
	if err := a.serviceImportFormsCycle(destination, fromT); err != nil {
		return err
	}

	_, err := a.addServiceImport(destination, from, to, imClaim)

	return err
}

const MaxAccountCycleSearchDepth = 1024

func (a *Account) serviceImportFormsCycle(dest *Account, from string) error {
	return dest.checkServiceImportsForCycles(from, map[string]bool{a.Name: true})
}

func (a *Account) checkServiceImportsForCycles(from string, visited map[string]bool) error {
	if len(visited) >= MaxAccountCycleSearchDepth {
		return ErrCycleSearchDepth
	}
	a.mu.RLock()
	for _, sis := range a.imports.services {
		for _, si := range sis {
			if SubjectsCollide(from, si.to) {
				a.mu.RUnlock()
				if visited[si.acc.Name] {
					return ErrImportFormsCycle
				}
				// Push ourselves and check si.acc
				visited[a.Name] = true
				// Make a copy to not overwrite the passed value.
				f := from
				if subjectIsSubsetMatch(si.from, f) {
					f = si.from
				}
				if err := si.acc.checkServiceImportsForCycles(f, visited); err != nil {
					return err
				}
				a.mu.RLock()
			}
		}
	}
	a.mu.RUnlock()
	return nil
}

func (a *Account) streamImportFormsCycle(dest *Account, to string) error {
	return dest.checkStreamImportsForCycles(to, map[string]bool{a.Name: true})
}

// Lock should be held.
func (a *Account) hasServiceExportMatching(to string) bool {
	for subj := range a.exports.services {
		if subjectIsSubsetMatch(to, subj) {
			return true
		}
	}
	return false
}

// Lock should be held.
func (a *Account) hasStreamExportMatching(to string) bool {
	for subj := range a.exports.streams {
		if subjectIsSubsetMatch(to, subj) {
			return true
		}
	}
	return false
}

func (a *Account) checkStreamImportsForCycles(to string, visited map[string]bool) error {
	if len(visited) >= MaxAccountCycleSearchDepth {
		return ErrCycleSearchDepth
	}

	a.mu.RLock()

	if !a.hasStreamExportMatching(to) {
		a.mu.RUnlock()
		return nil
	}

	for _, si := range a.imports.streams {
		if SubjectsCollide(to, si.to) {
			a.mu.RUnlock()
			if visited[si.acc.Name] {
				return ErrImportFormsCycle
			}
			// Push ourselves and check si.acc
			visited[a.Name] = true
			// Make a copy to not overwrite the passed value.
			t := to
			if subjectIsSubsetMatch(si.to, t) {
				t = si.to
			}
			if err := si.acc.checkStreamImportsForCycles(t, visited); err != nil {
				return err
			}
			a.mu.RLock()
		}
	}
	a.mu.RUnlock()
	return nil
}

// SetServiceImportSharing will allow sharing of information about requests with the export account.
// Used for service latency tracking at the moment.
func (a *Account) SetServiceImportSharing(destination *Account, to string, allow bool) error {
	return a.setServiceImportSharing(destination, to, true, allow)
}

// setServiceImportSharing will allow sharing of information about requests with the export account.
func (a *Account) setServiceImportSharing(destination *Account, to string, check, allow bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if check && a.isClaimAccount() {
		return fmt.Errorf("claim based accounts can not be updated directly")
	}
	// We can't use getServiceImportForAccountLocked() here since we are looking
	// for the service import with the si.to == to, which may not be the key
	// for the service import in the map.
	for _, sis := range a.imports.services {
		for _, si := range sis {
			if si.acc.Name == destination.Name && si.to == to {
				si.share = allow
				return nil
			}
		}
	}
	return fmt.Errorf("service import not found")
}

// AddServiceImport will add a route to an account to send published messages / requests
// to the destination account. From is the local subject to map, To is the
// subject that will appear on the destination account. Destination will need
// to have an import rule to allow access via addService.
func (a *Account) AddServiceImport(destination *Account, from, to string) error {
	return a.AddServiceImportWithClaim(destination, from, to, nil)
}

// NumPendingReverseResponses returns the number of response mappings we have for all outstanding
// requests for service imports.
func (a *Account) NumPendingReverseResponses() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.imports.rrMap)
}

// NumPendingAllResponses return the number of all responses outstanding for service exports.
func (a *Account) NumPendingAllResponses() int {
	return a.NumPendingResponses(_EMPTY_)
}

// NumPendingResponses returns the number of responses outstanding for service exports
// on this account. An empty filter string returns all responses regardless of which export.
// If you specify the filter we will only return ones that are for that export.
// NOTE this is only for what this server is tracking.
func (a *Account) NumPendingResponses(filter string) int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if filter == _EMPTY_ {
		return len(a.exports.responses)
	}
	se := a.getServiceExport(filter)
	if se == nil {
		return 0
	}
	var nre int
	for _, si := range a.exports.responses {
		if si.se == se {
			nre++
		}
	}
	return nre
}

// NumServiceImports returns the number of service imports we have configured.
func (a *Account) NumServiceImports() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.imports.services)
}

// Reason why we are removing this response serviceImport.
type rsiReason int

const (
	rsiOk = rsiReason(iota)
	rsiNoDelivery
	rsiTimeout
)

// removeRespServiceImport removes a response si mapping and the reverse entries for interest detection.
func (a *Account) removeRespServiceImport(si *serviceImport, reason rsiReason) {
	if si == nil {
		return
	}

	a.mu.Lock()
	c := a.ic
	delete(a.exports.responses, si.from)
	dest, to, tracking, rc, didDeliver := si.acc, si.to, si.tracking, si.rc, si.didDeliver
	a.mu.Unlock()

	// If we have a sid make sure to unsub.
	if len(si.sid) > 0 && c != nil {
		c.processUnsub(si.sid)
	}

	if tracking && rc != nil && !didDeliver {
		a.sendBackendErrorTrackingLatency(si, reason)
	}

	dest.checkForReverseEntry(to, si, false)
}

func (a *Account) getServiceImportForAccountLocked(dstAccName, subject string) *serviceImport {
	sis, ok := a.imports.services[subject]
	if !ok {
		return nil
	}
	if len(sis) == 1 && sis[0].acc.Name == dstAccName {
		return sis[0]
	}
	for _, si := range sis {
		if si.acc.Name == dstAccName {
			return si
		}
	}
	return nil
}

// removeServiceImport will remove the route by subject.
func (a *Account) removeServiceImport(dstAccName, subject string) {
	a.mu.Lock()
	sis, ok := a.imports.services[subject]
	if !ok {
		a.mu.Unlock()
		return
	}
	var si *serviceImport
	if len(sis) == 1 {
		si = sis[0]
		if si.acc.Name != dstAccName {
			si = nil
		} else {
			delete(a.imports.services, subject)
		}
	} else {
		for i, esi := range sis {
			if esi.acc.Name == dstAccName {
				si = esi
				last := len(sis) - 1
				if i != last {
					sis[i] = sis[last]
				}
				sis = sis[:last]
				a.imports.services[subject] = sis
				break
			}
		}
	}
	if si == nil {
		a.mu.Unlock()
		return
	}
	var sid []byte
	c := a.ic
	if c != nil && si.sid != nil {
		sid = si.sid
	}
	a.mu.Unlock()

	if sid != nil {
		c.processUnsub(sid)
	}
}

// This tracks responses to service requests mappings. This is used for cleanup.
func (a *Account) addReverseRespMapEntry(acc *Account, reply, from string) {
	a.mu.Lock()
	a.imports.addRespEntry(reply, &serviceRespEntry{acc, from})
	a.mu.Unlock()
}

// checkForReverseEntries is for when we are trying to match reverse entries to a wildcard.
// This will be called from checkForReverseEntry when the reply arg is a wildcard subject.
// This will usually be called in a go routine since we need to walk all the entries.
func (a *Account) checkForReverseEntries(reply string, checkInterest, recursed bool) {
	if subjectIsLiteral(reply) {
		a._checkForReverseEntry(reply, nil, checkInterest, recursed)
		return
	}

	a.mu.RLock()
	if len(a.imports.rrMap) == 0 {
		a.mu.RUnlock()
		return
	}

	var _rs [64]string
	rs := _rs[:0]
	if n := len(a.imports.rrMap); n > cap(rs) {
		rs = make([]string, 0, n)
	}

	for k := range a.imports.rrMap {
		rs = append(rs, k)
	}
	a.mu.RUnlock()

	tsa := [32]string{}
	tts := tokenizeSubjectIntoSlice(tsa[:0], reply)

	rsa := [32]string{}
	for _, r := range rs {
		rts := tokenizeSubjectIntoSlice(rsa[:0], r)
		//  isSubsetMatchTokenized is heavy so make sure we do this without the lock.
		if isSubsetMatchTokenized(rts, tts) {
			a._checkForReverseEntry(r, nil, checkInterest, recursed)
		}
	}
}

// This checks for any response map entries. If you specify an si we will only match and
// clean up for that one, otherwise we remove them all.
func (a *Account) checkForReverseEntry(reply string, si *serviceImport, checkInterest bool) {
	a._checkForReverseEntry(reply, si, checkInterest, false)
}

// Callers should use checkForReverseEntry instead. This function exists to help prevent
// infinite recursion.
func (a *Account) _checkForReverseEntry(reply string, si *serviceImport, checkInterest, recursed bool) {
	a.mu.RLock()
	if len(a.imports.rrMap) == 0 {
		a.mu.RUnlock()
		return
	}

	if subjectHasWildcard(reply) {
		if recursed {
			// If we have reached this condition then it is because the reverse entries also
			// contain wildcards (that shouldn't happen but a client *could* provide an inbox
			// prefix that is illegal because it ends in a wildcard character), at which point
			// we will end up with infinite recursion between this func and checkForReverseEntries.
			// To avoid a stack overflow panic, we'll give up instead.
			a.mu.RUnlock()
			return
		}

		doInline := len(a.imports.rrMap) <= 64
		a.mu.RUnlock()

		if doInline {
			a.checkForReverseEntries(reply, checkInterest, true)
		} else {
			go a.checkForReverseEntries(reply, checkInterest, true)
		}
		return
	}

	if _, ok := a.imports.rrMap[reply]; !ok {
		a.mu.RUnlock()
		return
	}

	// If we are here we have an entry we should check.
	// If requested we will first check if there is any
	// interest for this subject for the entire account.
	// If there is we can not delete any entries yet.
	// Note that if we are here reply has to be a literal subject.
	if checkInterest {
		// If interest still exists we can not clean these up yet.
		if a.sl.HasInterest(reply) {
			a.mu.RUnlock()
			return
		}
	}
	a.mu.RUnlock()

	// Delete the appropriate entries here based on optional si.
	a.mu.Lock()
	// We need a new lookup here because we have released the lock.
	var sres []*serviceRespEntry
	if si == nil {
		sres = a.imports.rrMap[reply].list
		delete(a.imports.rrMap, reply)
	} else {
		// Constant time once indexed, instead of a linear scan across every
		// outstanding response that happens to share this reply subject.
		// This also keys the write back off reply rather than si.to. Every
		// caller reaches here with si.to equal to reply, so behavior is
		// unchanged, but the lookup and the write back can no longer drift
		// apart if that ever stops holding.
		a.imports.removeRespEntry(reply, si.from)
	}
	a.mu.Unlock()

	// If we are here we no longer have interest and we have
	// response entries that we should clean up.
	if si == nil {
		// sres is now known to have been removed from a.imports.rrMap, so we
		// can safely (data race wise) iterate through.
		for _, sre := range sres {
			acc := sre.acc
			var trackingCleanup bool
			var rsi *serviceImport
			acc.mu.Lock()
			c := acc.ic
			if rsi = acc.exports.responses[sre.msub]; rsi != nil && !rsi.didDeliver {
				delete(acc.exports.responses, rsi.from)
				trackingCleanup = rsi.tracking && rsi.rc != nil
			}
			acc.mu.Unlock()
			// If we are doing explicit subs for all responses (e.g. bound to leafnode)
			// we will have a non-empty sid here.
			if rsi != nil && len(rsi.sid) > 0 && c != nil {
				c.processUnsub(rsi.sid)
			}
			if trackingCleanup {
				acc.sendReplyInterestLostTrackLatency(rsi)
			}
		}
	}
}

// Checks to see if a potential service import subject is already overshadowed.
func (a *Account) serviceImportShadowed(from string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.imports.services[from] != nil {
		return true
	}
	// We did not find a direct match, so check individually.
	for subj := range a.imports.services {
		if subjectIsSubsetMatch(from, subj) {
			return true
		}
	}
	return false
}

// Internal check to see if a service import exists.
func (a *Account) serviceImportExists(dstAccName, from string) bool {
	a.mu.RLock()
	dup := a.getServiceImportForAccountLocked(dstAccName, from)
	a.mu.RUnlock()
	return dup != nil
}

// Add a service import.
// This does no checks and should only be called by the msg processing code.
// Use AddServiceImport from above if responding to user input or config changes, etc.
func (a *Account) addServiceImport(dest *Account, from, to string, claim *jwt.Import) (*serviceImport, error) {
	rt := Singleton
	var lat *serviceLatency

	if dest == nil {
		return nil, ErrMissingAccount
	}

	var atrc bool
	dest.mu.RLock()
	se := dest.getServiceExport(to)
	if se != nil {
		rt = se.respType
		lat = se.latency
		atrc = se.atrc
	}
	destAccName := dest.Name
	dest.mu.RUnlock()

	a.mu.Lock()
	if a.imports.services == nil {
		a.imports.services = make(map[string][]*serviceImport)
	} else if dup := a.getServiceImportForAccountLocked(destAccName, from); dup != nil {
		a.mu.Unlock()
		return nil, fmt.Errorf("duplicate service import subject %q, previously used in import for account %q, subject %q",
			from, dup.acc.Name, dup.to)
	}

	if to == _EMPTY_ {
		to = from
	}
	// Check to see if we have a wildcard
	var (
		usePub bool
		tr     *subjectTransform
		err    error
	)

	if subjectHasWildcard(to) {
		// If to and from match, then we use the published subject.
		if to == from {
			usePub = true
		} else {
			to, _ = transformUntokenize(to)
			// Create a transform. Do so in reverse such that $ symbols only exist in to
			if tr, err = NewSubjectTransformStrict(to, transformTokenize(from)); err != nil {
				a.mu.Unlock()
				return nil, fmt.Errorf("failed to create mapping transform for service import subject from %q to %q: %v",
					from, to, err)
			} else {
				// un-tokenize and reverse transform so we get the transform needed
				from, _ = transformUntokenize(from)
				tr = tr.reverse()
			}
		}
	}
	var share bool
	if claim != nil {
		share = claim.Share
	}
	si := &serviceImport{dest, claim, se, nil, from, to, tr, 0, rt, lat, nil, nil, nil, usePub, false, false, share, false, false, atrc, nil}
	sis := a.imports.services[from]
	sis = append(sis, si)
	a.imports.services[from] = sis
	a.mu.Unlock()

	if err := a.addServiceImportSub(si); err != nil {
		a.removeServiceImport(destAccName, si.from)
		return nil, err
	}
	return si, nil
}

// Returns the internal client, will create one if not present.
// Lock should be held.
func (a *Account) internalClient() *client {
	if a.ic == nil && a.srv != nil {
		a.ic = a.srv.createInternalAccountClient()
		a.ic.acc = a
	}
	return a.ic
}

// Internal account scoped subscriptions.
func (a *Account) subscribeInternal(subject string, cb msgHandler) (*subscription, error) {
	return a.subscribeInternalEx(subject, cb, false)
}

// Unsubscribe from an internal account subscription.
func (a *Account) unsubscribeInternal(sub *subscription) {
	if ic := a.internalClient(); ic != nil {
		ic.processUnsub(sub.sid)
	}
}

// Creates internal subscription for service import responses.
func (a *Account) subscribeServiceImportResponse(subject string) (*subscription, error) {
	return a.subscribeInternalEx(subject, a.processServiceImportResponse, true)
}

func (a *Account) subscribeInternalEx(subject string, cb msgHandler, ri bool) (*subscription, error) {
	a.mu.Lock()
	a.isid++
	c, sid := a.internalClient(), strconv.FormatUint(a.isid, 10)
	a.mu.Unlock()

	// This will happen in parsing when the account has not been properly setup.
	if c == nil {
		return nil, fmt.Errorf("no internal account client")
	}

	return c.processSubEx([]byte(subject), nil, []byte(sid), cb, false, false, ri)
}

// This will add an account subscription that matches the "from" from a service import entry.
func (a *Account) addServiceImportSub(si *serviceImport) error {
	a.mu.Lock()
	c := a.internalClient()
	// This will happen in parsing when the account has not been properly setup.
	if c == nil {
		a.mu.Unlock()
		return nil
	}
	if si.sid != nil {
		a.mu.Unlock()
		return fmt.Errorf("duplicate call to create subscription for service import")
	}
	a.isid++
	sid := strconv.FormatUint(a.isid, 10)
	si.sid = []byte(sid)
	subject := si.from
	a.mu.Unlock()

	cb := func(sub *subscription, c *client, acc *Account, subject, reply string, msg []byte) {
		c.pa.delivered = c.processServiceImport(si, acc, msg)
	}
	sub, err := c.processSubEx([]byte(subject), nil, []byte(sid), cb, true, true, false)
	if err != nil {
		return err
	}
	// Leafnodes introduce a new way to introduce messages into the system. Therefore forward import subscription
	// This is similar to what initLeafNodeSmapAndSendSubs does
	// TODO we need to consider performing this update as we get client subscriptions.
	//      This behavior would result in subscription propagation only where actually used.
	a.updateLeafNodes(sub, 1)
	return nil
}

// Remove all the subscriptions associated with service imports.
func (a *Account) removeAllServiceImportSubs() {
	a.mu.Lock()
	var sids [][]byte
	for _, sis := range a.imports.services {
		for _, si := range sis {
			if si.sid != nil {
				sids = append(sids, si.sid)
				si.sid = nil
			}
		}
	}
	c := a.ic
	a.ic = nil
	a.mu.Unlock()

	if c == nil {
		return
	}
	for _, sid := range sids {
		c.processUnsub(sid)
	}
	c.closeConnection(InternalClient)
}

// Add in subscriptions for all registered service imports.
func (a *Account) addAllServiceImportSubs() {
	var sis [32]*serviceImport
	serviceImports := sis[:0]
	a.mu.RLock()
	for _, sis := range a.imports.services {
		serviceImports = append(serviceImports, sis...)
	}
	a.mu.RUnlock()
	for _, si := range serviceImports {
		a.addServiceImportSub(si)
	}
}

var (
	// header where all information is encoded in one value.
	trcUber = textproto.CanonicalMIMEHeaderKey("Uber-Trace-Id")
	trcCtx  = textproto.CanonicalMIMEHeaderKey("Traceparent")
	trcB3   = textproto.CanonicalMIMEHeaderKey("B3")
	// openzipkin header to check
	trcB3Sm = textproto.CanonicalMIMEHeaderKey("X-B3-Sampled")
	trcB3Id = textproto.CanonicalMIMEHeaderKey("X-B3-TraceId")
	// additional header needed to include when present
	trcB3PSId        = textproto.CanonicalMIMEHeaderKey("X-B3-ParentSpanId")
	trcB3SId         = textproto.CanonicalMIMEHeaderKey("X-B3-SpanId")
	trcCtxSt         = textproto.CanonicalMIMEHeaderKey("Tracestate")
	trcUberCtxPrefix = textproto.CanonicalMIMEHeaderKey("Uberctx-")
)

func newB3Header(h http.Header) http.Header {
	retHdr := http.Header{}
	if v, ok := h[trcB3Sm]; ok {
		retHdr[trcB3Sm] = v
	}
	if v, ok := h[trcB3Id]; ok {
		retHdr[trcB3Id] = v
	}
	if v, ok := h[trcB3PSId]; ok {
		retHdr[trcB3PSId] = v
	}
	if v, ok := h[trcB3SId]; ok {
		retHdr[trcB3SId] = v
	}
	return retHdr
}

func newUberHeader(h http.Header, tId []string) http.Header {
	retHdr := http.Header{trcUber: tId}
	for k, v := range h {
		if strings.HasPrefix(k, trcUberCtxPrefix) {
			retHdr[k] = v
		}
	}
	return retHdr
}

func newTraceCtxHeader(h http.Header, tId []string) http.Header {
	retHdr := http.Header{trcCtx: tId}
	if v, ok := h[trcCtxSt]; ok {
		retHdr[trcCtxSt] = v
	}
	return retHdr
}

// Helper to determine when to sample. When header has a value, sampling is driven by header
func shouldSample(l *serviceLatency, c *client) (bool, http.Header) {
	if l == nil {
		return false, nil
	}
	if l.sampling < 0 {
		return false, nil
	}
	if l.sampling >= 100 {
		return true, nil
	}
	if l.sampling > 0 && rand.Int32N(100) <= int32(l.sampling) {
		return true, nil
	}
	h := c.parseState.getHeader()
	if len(h) == 0 {
		return false, nil
	}
	if tId := h[trcUber]; len(tId) != 0 {
		// sample 479fefe9525eddb:5adb976bfc1f95c1:479fefe9525eddb:1
		tk := strings.Split(tId[0], ":")
		if len(tk) == 4 && len(tk[3]) > 0 && len(tk[3]) <= 2 {
			dst := [2]byte{}
			src := [2]byte{'0', tk[3][0]}
			if len(tk[3]) == 2 {
				src[1] = tk[3][1]
			}
			if _, err := hex.Decode(dst[:], src[:]); err == nil && dst[0]&1 == 1 {
				return true, newUberHeader(h, tId)
			}
		}
		return false, nil
	} else if sampled := h[trcB3Sm]; len(sampled) != 0 && sampled[0] == "1" {
		return true, newB3Header(h) // allowed
	} else if len(sampled) != 0 && sampled[0] == "0" {
		return false, nil // denied
	} else if _, ok := h[trcB3Id]; ok {
		// sample 80f198ee56343ba864fe8b2a57d3eff7
		// presence (with X-B3-Sampled not being 0) means sampling left to recipient
		return true, newB3Header(h)
	} else if b3 := h[trcB3]; len(b3) != 0 {
		// sample 80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-1-05e3ac9a4f6e3b90
		// sample 0
		tk := strings.Split(b3[0], "-")
		if len(tk) > 2 && tk[2] == "0" {
			return false, nil // denied
		} else if len(tk) == 1 && tk[0] == "0" {
			return false, nil // denied
		}
		return true, http.Header{trcB3: b3} // sampling allowed or left to recipient of header
	} else if tId := h[trcCtx]; len(tId) != 0 {
		var sample bool
		// sample 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
		tk := strings.Split(tId[0], "-")
		if len(tk) == 4 && len([]byte(tk[3])) == 2 {
			if hexVal, err := strconv.ParseInt(tk[3], 16, 8); err == nil {
				sample = hexVal&0x1 == 0x1
			}
		}
		if sample {
			return true, newTraceCtxHeader(h, tId)
		} else {
			return false, nil
		}
	}
	return false, nil
}

// Used to mimic client like replies.
const (
	replyPrefix    = "_R_."
	replyPrefixLen = len(replyPrefix)
	baseServerLen  = 10
	replyLen       = 6
	minReplyLen    = 15
	digits         = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	base           = 62
)

// This is where all service export responses are handled.
func (a *Account) processServiceImportResponse(sub *subscription, c *client, _ *Account, subject, reply string, msg []byte) {
	a.mu.RLock()
	if a.expired.Load() || len(a.exports.responses) == 0 {
		a.mu.RUnlock()
		return
	}
	si := a.exports.responses[subject]

	if si == nil || si.invalid {
		a.mu.RUnlock()
		return
	}
	a.mu.RUnlock()

	// Send for normal processing.
	c.processServiceImport(si, a, msg)
}

// Will create the response prefix for fast generation of responses.
// A wildcard subscription may be used handle interest graph propagation
// for all service replies, unless we are bound to a leafnode.
// Lock should be held.
func (a *Account) createRespWildcard() {
	b := [baseServerLen]byte{'_', 'R', '_', '.'}
	rn := rand.Uint64()
	for i, l := replyPrefixLen, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	a.siReply = append(b[:], '.')
}

// Test whether this is a tracked reply.
func isTrackedReply(reply []byte) bool {
	lreply := len(reply) - 1
	return lreply > 3 && reply[lreply-1] == '.' && reply[lreply] == 'T'
}

// Generate a new service reply from the wildcard prefix.
// FIXME(dlc) - probably do not have to use rand here. about 25ns per.
func (a *Account) newServiceReply(tracking bool) []byte {
	a.mu.Lock()
	s := a.srv
	rn := rand.Uint64()

	// Check if we need to create the reply here.
	var createdSiReply bool
	if a.siReply == nil {
		a.createRespWildcard()
		createdSiReply = true
	}
	replyPre := a.siReply
	a.mu.Unlock()

	// If we created the siReply and we are not bound to a leafnode
	// we need to do the wildcard subscription.
	if createdSiReply {
		a.subscribeServiceImportResponse(string(append(replyPre, '>')))
	}

	var b [replyLen]byte
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	// Make sure to copy.
	reply := make([]byte, 0, len(replyPre)+len(b))
	reply = append(reply, replyPre...)
	reply = append(reply, b[:]...)

	if tracking {
		if shash := s.Node(); shash != _EMPTY_ {
			// Add in our tracking identifier. This allows the metrics to get back to only
			// this server without needless SUBS/UNSUBS.
			reply = append(reply, '.')
			reply = append(reply, shash...)
			reply = append(reply, '.', 'T')

		}
	}
	return reply
}

// Checks if a serviceImport was created to map responses.
func (si *serviceImport) isRespServiceImport() bool {
	return si != nil && si.response
}

// Sets the response threshold timer for a service export.
// Account lock should be held
func (se *serviceExport) setResponseThresholdTimer() {
	if se.rtmr != nil {
		return // Already set
	}
	se.rtmr = time.AfterFunc(se.respThresh, se.checkExpiredResponses)
}

// Account lock should be held
func (se *serviceExport) clearResponseThresholdTimer() bool {
	if se.rtmr == nil {
		return true
	}
	stopped := se.rtmr.Stop()
	se.rtmr = nil
	return stopped
}

// checkExpiredResponses will check for any pending responses that need to
// be cleaned up.
func (se *serviceExport) checkExpiredResponses() {
	acc := se.acc
	if acc == nil {
		se.clearResponseThresholdTimer()
		return
	}

	var expired []*serviceImport
	mints := time.Now().UnixNano() - int64(se.respThresh)

	// TODO(dlc) - Should we release lock while doing this? Or only do these in batches?
	// Should we break this up for responses only from this service export?
	// Responses live on acc directly for fast inbound processsing for the _R_ wildcard.
	// We could do another indirection at this level but just to get to the service export?
	var totalResponses int
	acc.mu.RLock()
	for _, si := range acc.exports.responses {
		if si.se == se {
			totalResponses++
			if si.ts <= mints {
				expired = append(expired, si)
			}
		}
	}
	acc.mu.RUnlock()

	for _, si := range expired {
		acc.removeRespServiceImport(si, rsiTimeout)
	}

	// Pull out expired to determine if we have any left for timer.
	totalResponses -= len(expired)

	// Redo timer as needed.
	acc.mu.Lock()
	if totalResponses > 0 && se.rtmr != nil {
		se.rtmr.Stop()
		se.rtmr.Reset(se.respThresh)
	} else {
		se.clearResponseThresholdTimer()
	}
	acc.mu.Unlock()
}

// ServiceExportResponseThreshold returns the current threshold.
func (a *Account) ServiceExportResponseThreshold(export string) (time.Duration, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	se := a.getServiceExport(export)
	if se == nil {
		return 0, fmt.Errorf("no export defined for %q", export)
	}
	return se.respThresh, nil
}

// SetServiceExportResponseThreshold sets the maximum time the system will a response to be delivered
// from a service export responder.
func (a *Account) SetServiceExportResponseThreshold(export string, maxTime time.Duration) error {
	a.mu.Lock()
	if a.isClaimAccount() {
		a.mu.Unlock()
		return fmt.Errorf("claim based accounts can not be updated directly")
	}
	lrt := a.lowestServiceExportResponseTime()
	se := a.getServiceExport(export)
	if se == nil {
		a.mu.Unlock()
		return fmt.Errorf("no export defined for %q", export)
	}
	se.respThresh = maxTime

	var clients []*client
	nlrt := a.lowestServiceExportResponseTime()
	if nlrt != lrt && len(a.clients) > 0 {
		clients = a.getClientsLocked()
	}
	// Need to release because lock ordering is client -> Account
	a.mu.Unlock()
	if len(clients) > 0 {
		updateAllClientsServiceExportResponseTime(clients, nlrt)
	}
	return nil
}

func (a *Account) SetServiceExportAllowTrace(export string, allowTrace bool) error {
	a.mu.Lock()
	se := a.getServiceExport(export)
	if se == nil {
		a.mu.Unlock()
		return fmt.Errorf("no export defined for %q", export)
	}
	se.atrc = allowTrace
	a.mu.Unlock()
	return nil
}

// This is for internal service import responses.
func (a *Account) addRespServiceImport(dest *Account, to string, osi *serviceImport, tracking bool, header http.Header, mt *msgTrace) *serviceImport {
	nrr := string(osi.acc.newServiceReply(tracking))

	dest.mu.Lock()
	osiSe, osiLat, osiRT, osiShare := osi.se, osi.latency, osi.rt, osi.share
	dest.mu.Unlock()

	a.mu.Lock()

	// dest is the requestor's account. a is the service responder with the export.
	// Marked as internal here, that is how we distinguish.
	si := &serviceImport{dest, nil, osiSe, nil, nrr, to, nil, 0, osiRT, nil, nil, nil, mt, false, true, false, osiShare, false, false, false, nil}

	if a.exports.responses == nil {
		a.exports.responses = make(map[string]*serviceImport)
	}
	a.exports.responses[nrr] = si

	// Always grab time and make sure response threshold timer is running.
	si.ts = time.Now().UnixNano()
	if osiSe != nil {
		osiSe.setResponseThresholdTimer()
	}

	if osiRT == Singleton && tracking {
		si.latency = osiLat
		si.tracking = true
		si.trackingHdr = header
	}
	a.mu.Unlock()

	// We do add in the reverse map such that we can detect loss of interest and do proper
	// cleanup of this si as interest goes away.
	dest.addReverseRespMapEntry(a, to, nrr)

	return si
}

// AddStreamImportWithClaim will add in the stream import from a specific account with optional token.
func (a *Account) AddStreamImportWithClaim(account *Account, from, prefix string, imClaim *jwt.Import) error {
	return a.addStreamImportWithClaim(account, from, prefix, false, imClaim)
}

func (a *Account) addStreamImportWithClaim(account *Account, from, prefix string, allowTrace bool, imClaim *jwt.Import) error {
	if account == nil {
		return ErrMissingAccount
	}

	// First check to see if the account has authorized export of the subject.
	if !account.checkStreamImportAuthorized(a, from, imClaim) {
		return ErrStreamImportAuthorization
	}

	// Check prefix if it exists and make sure its a literal.
	// Append token separator if not already present.
	if prefix != _EMPTY_ {
		// Make sure there are no wildcards here, this prefix needs to be a literal
		// since it will be prepended to a publish subject.
		if !subjectIsLiteral(prefix) {
			return ErrStreamImportBadPrefix
		}
		if prefix[len(prefix)-1] != btsep {
			prefix = prefix + string(btsep)
		}
	}

	return a.addMappedStreamImportWithClaim(account, from, prefix+from, allowTrace, imClaim)
}

// AddMappedStreamImport helper for AddMappedStreamImportWithClaim
func (a *Account) AddMappedStreamImport(account *Account, from, to string) error {
	return a.AddMappedStreamImportWithClaim(account, from, to, nil)
}

// AddMappedStreamImportWithClaim will add in the stream import from a specific account with optional token.
func (a *Account) AddMappedStreamImportWithClaim(account *Account, from, to string, imClaim *jwt.Import) error {
	return a.addMappedStreamImportWithClaim(account, from, to, false, imClaim)
}

func (a *Account) addMappedStreamImportWithClaim(account *Account, from, to string, allowTrace bool, imClaim *jwt.Import) error {
	if account == nil {
		return ErrMissingAccount
	}

	// First check to see if the account has authorized export of the subject.
	if !account.checkStreamImportAuthorized(a, from, imClaim) {
		return ErrStreamImportAuthorization
	}

	if to == _EMPTY_ {
		to = from
	}

	// Check if this forms a cycle.
	if err := a.streamImportFormsCycle(account, to); err != nil {
		return err
	}

	if err := a.streamImportFormsCycle(account, from); err != nil {
		return err
	}

	var (
		usePub bool
		tr     *subjectTransform
		err    error
	)
	if subjectHasWildcard(from) {
		if to == from {
			usePub = true
		} else {
			// Create a transform
			if tr, err = NewSubjectTransformStrict(from, transformTokenize(to)); err != nil {
				return fmt.Errorf("failed to create mapping transform for stream import subject from %q to %q: %v",
					from, to, err)
			}
			to, _ = transformUntokenize(to)
		}
	}

	a.mu.Lock()
	if a.isStreamImportDuplicate(account, from) {
		a.mu.Unlock()
		return ErrStreamImportDuplicate
	}
	if imClaim != nil {
		allowTrace = imClaim.AllowTrace
	}
	a.imports.streams = append(a.imports.streams, &streamImport{account, from, to, tr, nil, imClaim, usePub, false, allowTrace})
	a.mu.Unlock()
	return nil
}

// isStreamImportDuplicate checks for duplicate.
// Lock should be held.
func (a *Account) isStreamImportDuplicate(acc *Account, from string) bool {
	for _, si := range a.imports.streams {
		if si.acc == acc && si.from == from {
			return true
		}
	}
	return false
}

// AddStreamImport will add in the stream import from a specific account.
func (a *Account) AddStreamImport(account *Account, from, prefix string) error {
	return a.addStreamImportWithClaim(account, from, prefix, false, nil)
}

// IsPublicExport is a placeholder to denote a public export.
var IsPublicExport = []*Account(nil)

// AddStreamExport will add an export to the account. If accounts is nil
// it will signify a public export, meaning anyone can import.
func (a *Account) AddStreamExport(subject string, accounts []*Account) error {
	return a.addStreamExportWithAccountPos(subject, accounts, 0)
}

// AddStreamExport will add an export to the account. If accounts is nil
// it will signify a public export, meaning anyone can import.
// if accountPos is > 0, all imports will be granted where the following holds:
// strings.Split(subject, tsep)[accountPos] == account id will be granted.
func (a *Account) addStreamExportWithAccountPos(subject string, accounts []*Account, accountPos uint) error {
	if a == nil {
		return ErrMissingAccount
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.exports.streams == nil {
		a.exports.streams = make(map[string]*streamExport)
	}
	ea := a.exports.streams[subject]
	if accounts != nil || accountPos > 0 {
		if ea == nil {
			ea = &streamExport{}
		}
		if err := setExportAuth(&ea.exportAuth, subject, accounts, accountPos); err != nil {
			return err
		}
	}
	a.exports.streams[subject] = ea
	return nil
}

// Check if another account is authorized to import from us.
func (a *Account) checkStreamImportAuthorized(account *Account, subject string, imClaim *jwt.Import) bool {
	// Find the subject in the exports list.
	a.mu.RLock()
	auth := a.checkStreamImportAuthorizedNoLock(account, subject, imClaim)
	a.mu.RUnlock()
	return auth
}

func (a *Account) checkStreamImportAuthorizedNoLock(account *Account, subject string, imClaim *jwt.Import) bool {
	if a.exports.streams == nil || !IsValidSubject(subject) {
		return false
	}
	return a.checkStreamExportApproved(account, subject, imClaim)
}

func (a *Account) checkAuth(ea *exportAuth, account *Account, imClaim *jwt.Import, tokens []string) bool {
	// if ea is nil or ea.approved is nil, that denotes a public export
	if ea == nil || (len(ea.approved) == 0 && !ea.tokenReq && ea.accountPos == 0) {
		return true
	}
	// Check if the export is protected and enforces presence of importing account identity
	if ea.accountPos > 0 {
		return ea.accountPos <= uint(len(tokens)) && tokens[ea.accountPos-1] == account.Name
	}
	// Check if token required
	if ea.tokenReq {
		return a.checkActivation(account, imClaim, ea, true)
	}
	if ea.approved == nil {
		return false
	}
	// If we have a matching account we are authorized
	_, ok := ea.approved[account.Name]
	return ok
}

func (a *Account) checkStreamExportApproved(account *Account, subject string, imClaim *jwt.Import) bool {
	// Check direct match of subject first
	ea, ok := a.exports.streams[subject]
	if ok {
		// if ea is nil or eq.approved is nil, that denotes a public export
		if ea == nil {
			return true
		}
		return a.checkAuth(&ea.exportAuth, account, imClaim, nil)
	}

	// ok if we are here we did not match directly so we need to test each one.
	// The import subject arg has to take precedence, meaning the export
	// has to be a true subset of the import claim. We already checked for
	// exact matches above.
	tokens := strings.Split(subject, tsep)
	for subj, ea := range a.exports.streams {
		if isSubsetMatch(tokens, subj) {
			if ea == nil {
				return true
			}
			return a.checkAuth(&ea.exportAuth, account, imClaim, tokens)
		}
	}
	return false
}

func (a *Account) checkServiceExportApproved(account *Account, subject string, imClaim *jwt.Import) bool {
	// Check direct match of subject first
	se, ok := a.exports.services[subject]
	if ok {
		// if se is nil or eq.approved is nil, that denotes a public export
		if se == nil {
			return true
		}
		return a.checkAuth(&se.exportAuth, account, imClaim, nil)
	}
	// ok if we are here we did not match directly so we need to test each one.
	// The import subject arg has to take precedence, meaning the export
	// has to be a true subset of the import claim. We already checked for
	// exact matches above.
	tokens := strings.Split(subject, tsep)
	for subj, se := range a.exports.services {
		if isSubsetMatch(tokens, subj) {
			if se == nil {
				return true
			}
			return a.checkAuth(&se.exportAuth, account, imClaim, tokens)
		}
	}
	return false
}

// Helper function to get a serviceExport.
// Lock should be held on entry.
func (a *Account) getServiceExport(subj string) *serviceExport {
	se, ok := a.exports.services[subj]
	// The export probably has a wildcard, so lookup that up.
	if !ok {
		se = a.getWildcardServiceExport(subj)
	}
	return se
}

// This helper is used when trying to match a serviceExport record that is
// represented by a wildcard.
// Lock should be held on entry.
func (a *Account) getWildcardServiceExport(from string) *serviceExport {
	tokens := strings.Split(from, tsep)
	for subj, se := range a.exports.services {
		if isSubsetMatch(tokens, subj) {
			return se
		}
	}
	return nil
}

// These are import stream specific versions for when an activation expires.
func (a *Account) streamActivationExpired(exportAcc *Account, subject string) {
	a.mu.RLock()
	if a.expired.Load() || a.imports.streams == nil {
		a.mu.RUnlock()
		return
	}
	var si *streamImport
	for _, im := range a.imports.streams {
		if im.acc == exportAcc && im.from == subject {
			si = im
			break
		}
	}

	if si == nil || si.invalid {
		a.mu.RUnlock()
		return
	}
	a.mu.RUnlock()

	if si.acc.checkActivation(a, si.claim, nil, false) {
		// The token has been updated most likely and we are good to go.
		return
	}

	a.mu.Lock()
	si.invalid = true
	clients := a.getClientsLocked()
	awcsti := map[string]struct{}{a.Name: {}}
	a.mu.Unlock()
	for _, c := range clients {
		c.processSubsOnConfigReload(awcsti)
	}
}

// These are import service specific versions for when an activation expires.
func (a *Account) serviceActivationExpired(dstAcc *Account, subject string) {
	a.mu.RLock()
	if a.expired.Load() || a.imports.services == nil {
		a.mu.RUnlock()
		return
	}
	si := a.getServiceImportForAccountLocked(dstAcc.Name, subject)
	if si == nil || si.invalid {
		a.mu.RUnlock()
		return
	}
	a.mu.RUnlock()

	if si.acc.checkActivation(a, si.claim, nil, false) {
		// The token has been updated most likely and we are good to go.
		return
	}

	a.mu.Lock()
	si.invalid = true
	a.mu.Unlock()
}

// Fires for expired activation tokens. We could track this with timers etc.
// Instead we just re-analyze where we are and if we need to act.
func (a *Account) activationExpired(exportAcc *Account, subject string, kind jwt.ExportType) {
	switch kind {
	case jwt.Stream:
		a.streamActivationExpired(exportAcc, subject)
	case jwt.Service:
		a.serviceActivationExpired(exportAcc, subject)
	}
}

func isRevoked(revocations map[string]int64, subject string, issuedAt int64) bool {
	if len(revocations) == 0 {
		return false
	}
	if t, ok := revocations[subject]; !ok || t < issuedAt {
		if t, ok := revocations[jwt.All]; !ok || t < issuedAt {
			return false
		}
	}
	return true
}

// checkActivation will check the activation token for validity.
// ea may only be nil in cases where revocation may not be checked, say triggered by expiration timer.
func (a *Account) checkActivation(importAcc *Account, claim *jwt.Import, ea *exportAuth, expTimer bool) bool {
	if claim == nil || claim.Token == _EMPTY_ {
		return false
	}
	// Create a quick clone so we can inline Token JWT.
	clone := *claim

	vr := jwt.CreateValidationResults()
	clone.Validate(importAcc.Name, vr)
	if vr.IsBlocking(true) {
		return false
	}
	act, err := jwt.DecodeActivationClaims(clone.Token)
	if err != nil {
		return false
	}
	if !a.isIssuerClaimTrusted(act) {
		return false
	}
	vr = jwt.CreateValidationResults()
	act.Validate(vr)
	if vr.IsBlocking(true) {
		return false
	}
	if act.Expires != 0 {
		tn := time.Now().Unix()
		if act.Expires <= tn {
			return false
		}
		if expTimer {
			expiresAt := time.Duration(act.Expires - tn)
			time.AfterFunc(expiresAt*time.Second, func() {
				importAcc.activationExpired(a, string(act.ImportSubject), claim.Type)
			})
		}
	}
	if ea == nil {
		return true
	}
	// Check for token revocation..
	return !isRevoked(ea.actsRevoked, act.Subject, act.IssuedAt)
}

// Returns true if the activation claim is trusted. That is the issuer matches
// the account or is an entry in the signing keys.
func (a *Account) isIssuerClaimTrusted(claims *jwt.ActivationClaims) bool {
	// if no issuer account, issuer is the account
	if claims.IssuerAccount == _EMPTY_ {
		return true
	}
	// If the IssuerAccount is not us, then this is considered an error.
	if a.Name != claims.IssuerAccount {
		if a.srv != nil {
			a.srv.Errorf("Invalid issuer account %q in activation claim (subject: %q - type: %q) for account %q",
				claims.IssuerAccount, claims.Activation.ImportSubject, claims.Activation.ImportType, a.Name)
		}
		return false
	}
	_, ok := a.hasIssuerNoLock(claims.Issuer)
	return ok
}

// Returns true if `a` and `b` stream imports are the same. Note that the
// check is done with the account's name, not the pointer. This is used
// during config reload where we are comparing current and new config
// in which pointers are different.
// Acquires `a` read lock, but `b` is assumed to not be accessed
// by anyone but the caller (`b` is not registered anywhere).
func (a *Account) checkStreamImportsEqual(b *Account) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if len(a.imports.streams) != len(b.imports.streams) {
		return false
	}
	// Load the b imports into a map index by what we are looking for.
	bm := make(map[string]*streamImport, len(b.imports.streams))
	for _, bim := range b.imports.streams {
		bm[bim.acc.Name+bim.from+bim.to] = bim
	}
	for _, aim := range a.imports.streams {
		if bim, ok := bm[aim.acc.Name+aim.from+aim.to]; !ok {
			return false
		} else if aim.atrc != bim.atrc {
			return false
		}
	}
	return true
}

// Returns true if `a` and `b` stream exports are the same.
// Acquires `a` read lock, but `b` is assumed to not be accessed
// by anyone but the caller (`b` is not registered anywhere).
func (a *Account) checkStreamExportsEqual(b *Account) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.exports.streams) != len(b.exports.streams) {
		return false
	}
	for subj, aea := range a.exports.streams {
		bea, ok := b.exports.streams[subj]
		if !ok {
			return false
		}
		if !isStreamExportEqual(aea, bea) {
			return false
		}
	}
	return true
}

func isStreamExportEqual(a, b *streamExport) bool {
	if a == nil && b == nil {
		return true
	}
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	return isExportAuthEqual(&a.exportAuth, &b.exportAuth)
}

// Returns true if `a` and `b` service exports are the same.
// Acquires `a` read lock, but `b` is assumed to not be accessed
// by anyone but the caller (`b` is not registered anywhere).
func (a *Account) checkServiceExportsEqual(b *Account) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.exports.services) != len(b.exports.services) {
		return false
	}
	for subj, aea := range a.exports.services {
		bea, ok := b.exports.services[subj]
		if !ok {
			return false
		}
		if !isServiceExportEqual(aea, bea) {
			return false
		}
	}
	return true
}

func isServiceExportEqual(a, b *serviceExport) bool {
	if a == nil && b == nil {
		return true
	}
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	if !isExportAuthEqual(&a.exportAuth, &b.exportAuth) {
		return false
	}
	if a.acc.Name != b.acc.Name {
		return false
	}
	if a.respType != b.respType {
		return false
	}
	if a.latency != nil || b.latency != nil {
		if (a.latency != nil && b.latency == nil) || (a.latency == nil && b.latency != nil) {
			return false
		}
		if a.latency.sampling != b.latency.sampling {
			return false
		}
		if a.latency.subject != b.latency.subject {
			return false
		}
	}
	if a.atrc != b.atrc {
		return false
	}
	return true
}

// Returns true if `a` and `b` exportAuth structures are equal.
// Both `a` and `b` are guaranteed to be non-nil.
// Locking is handled by the caller.
func isExportAuthEqual(a, b *exportAuth) bool {
	if a.tokenReq != b.tokenReq {
		return false
	}
	if a.accountPos != b.accountPos {
		return false
	}
	if len(a.approved) != len(b.approved) {
		return false
	}
	for ak, av := range a.approved {
		if bv, ok := b.approved[ak]; !ok || av.Name != bv.Name {
			return false
		}
	}
	if len(a.actsRevoked) != len(b.actsRevoked) {
		return false
	}
	for ak, av := range a.actsRevoked {
		if bv, ok := b.actsRevoked[ak]; !ok || av != bv {
			return false
		}
	}
	return true
}

// Check if another account is authorized to route requests to this service.
func (a *Account) checkServiceImportAuthorized(account *Account, subject string, imClaim *jwt.Import) bool {
	a.mu.RLock()
	authorized := a.checkServiceImportAuthorizedNoLock(account, subject, imClaim)
	a.mu.RUnlock()
	return authorized
}

// Check if another account is authorized to route requests to this service.
func (a *Account) checkServiceImportAuthorizedNoLock(account *Account, subject string, imClaim *jwt.Import) bool {
	// Find the subject in the services list.
	if a.exports.services == nil {
		return false
	}
	return a.checkServiceExportApproved(account, subject, imClaim)
}

// IsExpired returns expiration status.
func (a *Account) IsExpired() bool {
	return a.expired.Load()
}

// Called when an account has expired.
func (a *Account) expiredTimeout() {
	// Mark expired first.
	a.expired.Store(true)

	// Collect the clients and expire them.
	cs := a.getClients()
	for _, c := range cs {
		if !isInternalClient(c.kind) {
			c.accountAuthExpired()
		}
	}
}

// Sets the expiration timer for an account JWT that has it set.
func (a *Account) setExpirationTimer(d time.Duration) {
	a.etmr = time.AfterFunc(d, a.expiredTimeout)
}

// Lock should be held
func (a *Account) clearExpirationTimer() bool {
	if a.etmr == nil {
		return true
	}
	stopped := a.etmr.Stop()
	a.etmr = nil
	return stopped
}

// checkUserRevoked will check if a user has been revoked.
func (a *Account) checkUserRevoked(nkey string, issuedAt int64) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return isRevoked(a.usersRevoked, nkey, issuedAt)
}

// failBearer will return if bearer token are allowed (false) or disallowed (true)
func (a *Account) failBearer() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.disallowBearer
}

// Check expiration and set the proper state as needed.
func (a *Account) checkExpiration(claims *jwt.ClaimsData) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.clearExpirationTimer()
	if claims.Expires == 0 {
		a.expired.Store(false)
		return
	}
	tn := time.Now().Unix()
	if claims.Expires <= tn {
		a.expired.Store(true)
		return
	}
	expiresAt := time.Duration(claims.Expires - tn)
	a.setExpirationTimer(expiresAt * time.Second)
	a.expired.Store(false)
}

// hasIssuer returns true if the issuer matches the account
// If the issuer is a scoped signing key, the scope will be returned as well
// issuer or it is a signing key for the account.
func (a *Account) hasIssuer(issuer string) (jwt.Scope, bool) {
	a.mu.RLock()
	scope, ok := a.hasIssuerNoLock(issuer)
	a.mu.RUnlock()
	return scope, ok
}

// hasIssuerNoLock is the unlocked version of hasIssuer
func (a *Account) hasIssuerNoLock(issuer string) (jwt.Scope, bool) {
	scope, ok := a.signingKeys[issuer]
	return scope, ok
}

// Returns the loop detection subject used for leafnodes
func (a *Account) getLDSubject() string {
	a.mu.RLock()
	lds := a.lds
	a.mu.RUnlock()
	return lds
}

// Placeholder for signaling token auth required.
var tokenAuthReq = []*Account{}

func authAccounts(tokenReq bool) []*Account {
	if tokenReq {
		return tokenAuthReq
	}
	return nil
}

// SetAccountResolver will assign the account resolver.
func (s *Server) SetAccountResolver(ar AccountResolver) {
	s.mu.Lock()
	s.accResolver = ar
	s.mu.Unlock()
}

// AccountResolver returns the registered account resolver.
func (s *Server) AccountResolver() AccountResolver {
	s.mu.RLock()
	ar := s.accResolver
	s.mu.RUnlock()
	return ar
}

// isClaimAccount returns if this account is backed by a JWT claim.
// Lock should be held.
func (a *Account) isClaimAccount() bool {
	return a.claimJWT != _EMPTY_
}

// UpdateAccountClaims will update an existing account with new claims.
// This will replace any exports or imports previously defined.
// Lock MUST NOT be held upon entry.
func (s *Server) UpdateAccountClaims(a *Account, ac *jwt.AccountClaims) {
	s.updateAccountClaimsWithRefresh(a, ac, true)
}

func (a *Account) traceLabel() string {
	if a == nil {
		return _EMPTY_
	}
	if a.nameTag != _EMPTY_ {
		return fmt.Sprintf("%s/%s", a.Name, a.nameTag)
	}
	return a.Name
}

// Check if an account has external auth set.
// Operator/Account Resolver only.
func (a *Account) hasExternalAuth() bool {
	if a == nil {
		return false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.extAuth != nil
}

// Determine if this is an external auth user.
func (a *Account) isExternalAuthUser(userID string) bool {
	if a == nil {
		return false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.extAuth != nil && slices.Contains(a.extAuth.AuthUsers, userID)
}

// Return the external authorization xkey if external authorization is enabled and the xkey is set.
// Operator/Account Resolver only.
func (a *Account) externalAuthXKey() string {
	if a == nil {
		return _EMPTY_
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.extAuth != nil && a.extAuth.XKey != _EMPTY_ {
		return a.extAuth.XKey
	}
	return _EMPTY_
}

// Check if an account switch for external authorization is allowed.
func (a *Account) isAllowedAcount(acc string) bool {
	if a == nil {
		return false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.extAuth != nil {
		// if we have a single allowed account, and we have a wildcard
		// we accept it
		if len(a.extAuth.AllowedAccounts) == 1 &&
			a.extAuth.AllowedAccounts[0] == jwt.AnyAccount {
			return true
		}
		// otherwise must match exactly
		for _, a := range a.extAuth.AllowedAccounts {
			if a == acc {
				return true
			}
		}
	}
	return false
}

// updateAccountClaimsWithRefresh will update an existing account with new claims.
// If refreshImportingAccounts is true it will also update incomplete dependent accounts
// This will replace any exports or imports previously defined.
// Lock MUST NOT be held upon entry.
func (s *Server) updateAccountClaimsWithRefresh(a *Account, ac *jwt.AccountClaims, refreshImportingAccounts bool) {
	if a == nil {
		return
	}
	// Rebuilding the exports below empties them, so must not overlap with the
	// checks at the end that mark imports of other accounts invalid.
	a.cmu.Lock()
	s.Debugf("Updating account claims: %s/%s", a.Name, ac.Name)
	a.checkExpiration(ac.Claims())

	a.mu.Lock()
	// Clone to update, only select certain fields.
	old := &Account{
		Name:         a.Name,
		exports:      a.exports,
		limits:       a.limits,
		signingKeys:  a.signingKeys,
		defaultPerms: a.defaultPerms.clone(),
	}

	// overwrite claim meta data
	a.nameTag = ac.Name
	a.tags = ac.Tags

	// Grab trace label under lock.
	tl := a.traceLabel()

	var td string
	var tds int
	if ac.Trace != nil {
		// Update trace destination and sampling
		td, tds = string(ac.Trace.Destination), ac.Trace.Sampling
		if !IsValidPublishSubject(td) {
			td, tds = _EMPTY_, 0
		} else if tds <= 0 || tds > 100 {
			tds = 100
		}
	}
	a.traceDest, a.traceDestSampling = td, tds

	// Check for external authorization.
	if ac.HasExternalAuthorization() {
		a.extAuth = &jwt.ExternalAuthorization{}
		a.extAuth.AuthUsers.Add(ac.Authorization.AuthUsers...)
		a.extAuth.AllowedAccounts.Add(ac.Authorization.AllowedAccounts...)
		a.extAuth.XKey = ac.Authorization.XKey
	} else {
		a.extAuth = nil
	}

	// Reset exports and imports here.

	// Exports is creating a whole new map.
	a.exports = exportMap{}

	// Imports are checked unlocked in processInbound, so we can't change out the struct here. Need to process inline.
	if a.imports.streams != nil {
		old.imports.streams = a.imports.streams
		a.imports.streams = nil
	}
	if a.imports.services != nil {
		old.imports.services = make(map[string][]*serviceImport, len(a.imports.services))
		for k, v := range a.imports.services {
			sis := append([]*serviceImport(nil), v...)
			old.imports.services[k] = sis
			delete(a.imports.services, k)
		}
	}

	alteredScope := map[string]struct{}{}

	// update account signing keys
	a.signingKeys = nil
	_, strict := s.strictSigningKeyUsage[a.Issuer]
	if len(ac.SigningKeys) > 0 || !strict {
		a.signingKeys = make(map[string]jwt.Scope)
	}
	signersChanged := false
	for k, scope := range ac.SigningKeys {
		a.signingKeys[k] = scope
	}
	if !strict {
		a.signingKeys[a.Name] = nil
	}
	if len(a.signingKeys) != len(old.signingKeys) {
		signersChanged = true
	}
	for k, scope := range a.signingKeys {
		if oldScope, ok := old.signingKeys[k]; !ok {
			signersChanged = true
		} else if !reflect.DeepEqual(scope, oldScope) {
			signersChanged = true
			alteredScope[k] = struct{}{}
		}
	}
	// collect mappings that need to be removed
	removeList := []string{}
	for _, m := range a.mappings {
		if _, ok := ac.Mappings[jwt.Subject(m.src)]; !ok {
			removeList = append(removeList, m.src)
		}
	}
	a.mu.Unlock()

	mappingFailed := false
	for sub, wm := range ac.Mappings {
		mappings := make([]*MapDest, len(wm))
		for i, m := range wm {
			mappings[i] = &MapDest{
				Subject: string(m.Subject),
				Weight:  m.GetWeight(),
				Cluster: m.Cluster,
			}
		}
		// This will overwrite existing entries
		if err := a.AddWeightedMappings(string(sub), mappings...); err != nil {
			// Do not remove existing mappings when a replacement fails to install;
			// otherwise a rejected dest can leave the account with no mapping.
			s.Errorf("Error adding subject mapping %q for account [%s]: %v", sub, tl, err)
			mappingFailed = true
		}
	}
	// remove mappings only after all replacements installed cleanly
	if !mappingFailed {
		for _, rmMapping := range removeList {
			a.RemoveMapping(rmMapping)
		}
	}

	// Re-register system exports/imports.
	if a == s.SystemAccount() {
		s.addSystemAccountExports(a)
	} else {
		s.registerSystemImports(a)
	}

	jsEnabled := s.JetStreamEnabled()

	streamTokenExpirationChanged := false
	serviceTokenExpirationChanged := false

	for _, e := range ac.Exports {
		switch e.Type {
		case jwt.Stream:
			s.Debugf("Adding stream export %q for %s", e.Subject, tl)
			if err := a.addStreamExportWithAccountPos(
				string(e.Subject), authAccounts(e.TokenReq), e.AccountTokenPosition,
			); err != nil {
				s.Debugf("Error adding stream export to account [%s]: %v", tl, err.Error())
			}
		case jwt.Service:
			s.Debugf("Adding service export %q for %s", e.Subject, tl)
			rt := Singleton
			switch e.ResponseType {
			case jwt.ResponseTypeStream:
				rt = Streamed
			case jwt.ResponseTypeChunked:
				rt = Chunked
			}
			if err := a.addServiceExportWithResponseAndAccountPos(
				string(e.Subject), rt, authAccounts(e.TokenReq), e.AccountTokenPosition,
			); err != nil {
				s.Debugf("Error adding service export to account [%s]: %v", tl, err)
				continue
			}
			sub := string(e.Subject)
			if e.Latency != nil {
				if err := a.TrackServiceExportWithSampling(sub, string(e.Latency.Results), int(e.Latency.Sampling)); err != nil {
					hdrNote := _EMPTY_
					if e.Latency.Sampling == jwt.Headers {
						hdrNote = " (using headers)"
					}
					s.Debugf("Error adding latency tracking%s for service export to account [%s]: %v", hdrNote, tl, err)
				}
			}
			if e.ResponseThreshold != 0 {
				// Response threshold was set in options.
				if err := a.SetServiceExportResponseThreshold(sub, e.ResponseThreshold); err != nil {
					s.Debugf("Error adding service export response threshold for [%s]: %v", tl, err)
				}
			}
			if err := a.SetServiceExportAllowTrace(sub, e.AllowTrace); err != nil {
				s.Debugf("Error adding allow_trace for %q: %v", sub, err)
			}
		}

		var revocationChanged *bool
		var ea *exportAuth

		a.mu.Lock()
		switch e.Type {
		case jwt.Stream:
			revocationChanged = &streamTokenExpirationChanged
			if se, ok := a.exports.streams[string(e.Subject)]; ok && se != nil {
				ea = &se.exportAuth
			}
		case jwt.Service:
			revocationChanged = &serviceTokenExpirationChanged
			if se, ok := a.exports.services[string(e.Subject)]; ok && se != nil {
				ea = &se.exportAuth
			}
		}
		if ea != nil {
			oldRevocations := ea.actsRevoked
			if len(e.Revocations) == 0 {
				// remove all, no need to evaluate existing imports
				ea.actsRevoked = nil
			} else if len(oldRevocations) == 0 {
				// add all, existing imports need to be re evaluated
				ea.actsRevoked = e.Revocations
				*revocationChanged = true
			} else {
				ea.actsRevoked = e.Revocations
				// diff, existing imports need to be conditionally re evaluated, depending on:
				// if a key was added, or it's timestamp increased
				for k, t := range e.Revocations {
					if tOld, ok := oldRevocations[k]; !ok || tOld < t {
						*revocationChanged = true
					}
				}
			}
		}
		a.mu.Unlock()
	}
	// Resolving the imports below can update this same account again.
	a.cmu.Unlock()

	var incompleteImports []*jwt.Import
	for _, i := range ac.Imports {
		acc, err := s.lookupAccount(i.Account)
		if acc == nil || err != nil {
			s.Errorf("Can't locate account [%s] for import of [%v] %s (err=%v)", i.Account, i.Subject, i.Type, err)
			incompleteImports = append(incompleteImports, i)
			continue
		}
		// Capture trace labels.
		acc.mu.RLock()
		atl := acc.traceLabel()
		acc.mu.RUnlock()
		// Grab from and to
		from, to := string(i.Subject), i.GetTo()
		switch i.Type {
		case jwt.Stream:
			if i.LocalSubject != _EMPTY_ {
				// set local subject implies to is empty
				to = string(i.LocalSubject)
				s.Debugf("Adding stream import %s:%q for %s:%q", atl, from, tl, to)
				err = a.AddMappedStreamImportWithClaim(acc, from, to, i)
			} else {
				s.Debugf("Adding stream import %s:%q for %s:%q", atl, from, tl, to)
				err = a.AddStreamImportWithClaim(acc, from, to, i)
			}
			if err != nil {
				s.Errorf("Error adding stream import %q from account %s to account %s: %v", i.Subject, atl, tl, err)
				incompleteImports = append(incompleteImports, i)
			}
		case jwt.Service:
			if i.LocalSubject != _EMPTY_ {
				from = string(i.LocalSubject)
				to = string(i.Subject)
			}
			s.Debugf("Adding service import %s:%q for %s:%q", atl, from, tl, to)
			if err := a.AddServiceImportWithClaim(acc, from, to, i); err != nil {
				s.Errorf("Error adding service import %q from account %s to account %s: %v", i.Subject, atl, tl, err)
				incompleteImports = append(incompleteImports, i)
			}
		}
	}
	a.cmu.Lock()

	// Now let's apply any needed changes from import/export changes.
	if !a.checkStreamImportsEqual(old) {
		awcsti := map[string]struct{}{a.Name: {}}
		for _, c := range a.getClients() {
			c.processSubsOnConfigReload(awcsti)
		}
	}
	// Now check if stream exports have changed.
	if !a.checkStreamExportsEqual(old) || signersChanged || streamTokenExpirationChanged {
		clients := map[*client]struct{}{}
		// We need to check all accounts that have an import claim from this account.
		awcsti := map[string]struct{}{}

		// We must only allow one goroutine to go through here, otherwise we could deadlock
		// due to locking two accounts in succession.
		s.mu.Lock()
		s.accounts.Range(func(k, v any) bool {
			acc := v.(*Account)
			// Move to the next if this account is actually account "a".
			if acc.Name == a.Name {
				return true
			}
			acc.mu.Lock()
			for _, im := range acc.imports.streams {
				if im != nil && im.acc.Name == a.Name {
					// Check for if we are still authorized for an import.
					im.invalid = !a.checkStreamImportAuthorized(acc, im.from, im.claim)
					awcsti[acc.Name] = struct{}{}
					for c := range acc.clients {
						clients[c] = struct{}{}
					}
				}
			}
			acc.mu.Unlock()
			return true
		})
		s.mu.Unlock()
		// Now walk clients.
		for c := range clients {
			c.processSubsOnConfigReload(awcsti)
		}
	}
	// Now check if service exports have changed.
	if !a.checkServiceExportsEqual(old) || signersChanged || serviceTokenExpirationChanged {
		// We must only allow one goroutine to go through here, otherwise we could deadlock
		// due to locking two accounts in succession.
		s.mu.Lock()
		s.accounts.Range(func(k, v any) bool {
			acc := v.(*Account)
			// Move to the next if this account is actually account "a".
			if acc.Name == a.Name {
				return true
			}
			acc.mu.Lock()
			for _, sis := range acc.imports.services {
				for _, si := range sis {
					if si != nil && si.acc.Name == a.Name {
						// Check for if we are still authorized for an import.
						si.invalid = !a.checkServiceImportAuthorized(acc, si.to, si.claim)
						// Make sure we should still be tracking latency and if we
						// are allowed to trace.
						if !si.response {
							a.mu.RLock()
							if se := a.getServiceExport(si.to); se != nil {
								if si.latency != nil {
									si.latency = se.latency
								}
								// Update allow trace.
								si.atrc = se.atrc
							}
							a.mu.RUnlock()
						}
					}
				}
			}
			acc.mu.Unlock()
			return true
		})
		s.mu.Unlock()
	}

	// Now make sure we shutdown the old service import subscriptions.
	var sids [][]byte
	a.mu.RLock()
	c := a.ic
	if c != nil {
		for _, sis := range old.imports.services {
			for _, si := range sis {
				if si.sid != nil {
					sids = append(sids, si.sid)
				}
			}
		}
	}
	a.mu.RUnlock()
	for _, sid := range sids {
		c.processUnsub(sid)
	}

	// Now do limits if they are present.
	a.mu.Lock()
	a.msubs = clampInt64ToInt32(ac.Limits.Subs)
	a.mpay = clampInt64ToInt32(ac.Limits.Payload)
	a.mconns = clampInt64ToInt32(ac.Limits.Conn)
	a.mleafs = clampInt64ToInt32(ac.Limits.LeafNodeConn)
	a.disallowBearer = ac.Limits.DisallowBearer
	// Check for any revocations
	if len(ac.Revocations) > 0 {
		// We will always replace whatever we had with most current, so no
		// need to look at what we have.
		a.usersRevoked = make(map[string]int64, len(ac.Revocations))
		for pk, t := range ac.Revocations {
			a.usersRevoked[pk] = t
		}
	} else {
		a.usersRevoked = nil
	}
	a.defaultPerms = buildPermissionsFromJwt(&ac.DefaultPermissions)
	a.incomplete = len(incompleteImports) != 0
	for _, i := range incompleteImports {
		s.incompleteAccExporterMap.Store(i.Account, struct{}{})
	}
	if a.srv == nil {
		a.srv = s
	}

	if ac.Limits.IsJSEnabled() {
		toUnlimited := func(value int64) int64 {
			if value > 0 {
				return value
			}
			return -1
		}
		if ac.Limits.JetStreamLimits.DiskStorage != 0 || ac.Limits.JetStreamLimits.MemoryStorage != 0 {
			// JetStreamAccountLimits and jwt.JetStreamLimits use same value for unlimited
			a.jsLimits = map[string]JetStreamAccountLimits{
				_EMPTY_: {
					MaxMemory:            ac.Limits.JetStreamLimits.MemoryStorage,
					MaxStore:             ac.Limits.JetStreamLimits.DiskStorage,
					MaxStreams:           int(ac.Limits.JetStreamLimits.Streams),
					MaxConsumers:         int(ac.Limits.JetStreamLimits.Consumer),
					MemoryMaxStreamBytes: toUnlimited(ac.Limits.JetStreamLimits.MemoryMaxStreamBytes),
					StoreMaxStreamBytes:  toUnlimited(ac.Limits.JetStreamLimits.DiskMaxStreamBytes),
					MaxBytesRequired:     ac.Limits.JetStreamLimits.MaxBytesRequired,
					MaxAckPending:        int(toUnlimited(ac.Limits.JetStreamLimits.MaxAckPending)),
				},
			}
		} else {
			a.jsLimits = map[string]JetStreamAccountLimits{}
			for t, l := range ac.Limits.JetStreamTieredLimits {
				a.jsLimits[t] = JetStreamAccountLimits{
					MaxMemory:            l.MemoryStorage,
					MaxStore:             l.DiskStorage,
					MaxStreams:           int(l.Streams),
					MaxConsumers:         int(l.Consumer),
					MemoryMaxStreamBytes: toUnlimited(l.MemoryMaxStreamBytes),
					StoreMaxStreamBytes:  toUnlimited(l.DiskMaxStreamBytes),
					MaxBytesRequired:     l.MaxBytesRequired,
					MaxAckPending:        int(toUnlimited(l.MaxAckPending)),
				}
			}
		}
	} else if a.jsLimits != nil {
		// covers failed update followed by disable
		a.jsLimits = nil
	}

	defaultPerms := a.defaultPerms
	defaultPermsChanged := !reflect.DeepEqual(old.defaultPerms, defaultPerms)
	a.updated = time.Now()
	clients := a.getClientsLocked()
	ajs := a.js
	hasJsLimits := a.jsLimits != nil
	a.mu.Unlock()

	// Sort in chronological order so that most recent connections over the limit are pruned.
	if a.MaxTotalConnectionsReached() {
		slices.SortFunc(clients, func(i, j *client) int { return i.start.Compare(j.start) })
	}

	// If JetStream is enabled for this server we will call into configJetStream for the account
	// regardless of enabled or disabled. It handles both cases.
	if jsEnabled {
		if err := s.configJetStream(a, nil); err != nil {
			s.Errorf("Error configuring jetstream for account [%s]: %v", tl, err.Error())
			a.mu.Lock()
			// Absent reload of js server cfg, this is going to be broken until js is disabled
			a.incomplete = true
			a.mu.Unlock()
		} else {
			a.mu.Lock()
			// Refresh reference, we've just enabled JetStream, so it would have been nil before.
			ajs = a.js
			a.mu.Unlock()
		}
	} else if hasJsLimits {
		// We do not have JS enabled for this server, but the account has it enabled so setup
		// our imports properly. This allows this server to proxy JS traffic correctly.
		s.checkJetStreamExports()
		a.enableAllJetStreamServiceImportsAndMappings()
	}

	if ajs != nil {
		// Check whether the account NRG status changed. If it has then we need to notify the
		// Raft groups running on the system so that they can move their subs if needed.
		a.mu.Lock()
		previous := a.nrgAccount
		switch ac.ClusterTraffic {
		case "system", _EMPTY_:
			a.nrgAccount = _EMPTY_
		case "owner":
			a.nrgAccount = a.Name
		default:
			s.Errorf("Account claim for %q has invalid value %q for cluster traffic account", a.Name, ac.ClusterTraffic)
		}
		changed := a.nrgAccount != previous
		a.mu.Unlock()
		if changed {
			s.updateNRGAccountStatus()
		}
	}

	// client list is in chronological order (older cids at the beginning of the list).
	count := 0
	for _, c := range clients {
		a.mu.RLock()
		exceeded := a.mconns != jwt.NoLimit && count >= int(a.mconns)
		a.mu.RUnlock()
		// Only kick non-internal clients.
		if !isInternalClient(c.kind) {
			if exceeded {
				c.maxAccountConnExceeded()
				continue
			}
			count++
		}
		c.mu.Lock()
		c.applyAccountLimits()
		// if we have an nkey user we are a callout user - save
		// the issuedAt, and nkey user id to honor revocations
		var nkeyUserID string
		var issuedAt int64
		if c.user != nil {
			issuedAt = c.user.Issued
			nkeyUserID = c.user.Nkey
		}
		theJWT := c.opts.JWT
		c.mu.Unlock()
		if defaultPermsChanged && c.updateDefaultPermissions(defaultPerms) && defaultPerms != nil {
			c.processSubsOnConfigReload(nil)
		}
		// Check for being revoked here. We use ac one to avoid the account lock.
		if (ac.Revocations != nil || ac.Limits.DisallowBearer) && theJWT != _EMPTY_ {
			if juc, err := jwt.DecodeUserClaims(theJWT); err != nil {
				c.Debugf("User JWT not valid: %v", err)
				c.authViolation()
				continue
			} else if juc.BearerToken && ac.Limits.DisallowBearer {
				c.Debugf("Bearer User JWT not allowed")
				c.authViolation()
				continue
			} else if ok := ac.IsClaimRevoked(juc); ok {
				c.sendErrAndDebug("User Authentication Revoked")
				c.closeConnection(Revocation)
				continue
			}
		}

		// if we extracted nkeyUserID and issuedAt we are a callout type
		// calloutIAT should only be set if we are in callout scenario as
		// the user JWT is _NOT_ associated with the client for callouts,
		// so we rely on the calloutIAT to know when the JWT was issued
		// revocations simply state that JWT issued before or by that date
		// are not valid
		if ac.Revocations != nil && nkeyUserID != _EMPTY_ && issuedAt > 0 {
			seconds, ok := ac.Revocations[jwt.All]
			if ok && seconds >= issuedAt {
				c.sendErrAndDebug("User Authentication Revoked")
				c.closeConnection(Revocation)
				continue
			}
			seconds, ok = ac.Revocations[nkeyUserID]
			if ok && seconds >= issuedAt {
				c.sendErrAndDebug("User Authentication Revoked")
				c.closeConnection(Revocation)
				continue
			}
		}
	}

	// Check if the signing keys changed, might have to evict
	if signersChanged {
		for _, c := range clients {
			c.mu.Lock()
			if c.user == nil {
				c.mu.Unlock()
				continue
			}
			sk := c.user.SigningKey
			c.mu.Unlock()
			if sk == _EMPTY_ {
				continue
			}
			if _, ok := alteredScope[sk]; ok {
				c.closeConnection(AuthenticationViolation)
			} else if _, ok := a.hasIssuer(sk); !ok {
				c.closeConnection(AuthenticationViolation)
			}
		}
	}

	// Updating other accounts below takes their lock, so release ours first.
	a.cmu.Unlock()

	if _, ok := s.incompleteAccExporterMap.Load(old.Name); ok && refreshImportingAccounts {
		s.incompleteAccExporterMap.Delete(old.Name)
		s.accounts.Range(func(key, value any) bool {
			acc := value.(*Account)
			acc.mu.RLock()
			incomplete := acc.incomplete
			name := acc.Name
			label := acc.traceLabel()
			// Must use jwt in account or risk failing on fetch
			// This jwt may not be the same that caused exportingAcc to be in incompleteAccExporterMap
			claimJWT := acc.claimJWT
			acc.mu.RUnlock()
			if incomplete && name != old.Name {
				if accClaims, _, err := s.verifyAccountClaims(claimJWT); err == nil {
					// Since claimJWT has not changed, acc can become complete
					// but it won't alter incomplete for it's dependents accounts.
					s.updateAccountClaimsWithRefresh(acc, accClaims, false)
					// old.Name was deleted before ranging over accounts
					// If it exists again, UpdateAccountClaims set it for failed imports of acc.
					// So there was one import of acc that imported this account and failed again.
					// Since this account just got updated, the import itself may be in error. So trace that.
					if _, ok := s.incompleteAccExporterMap.Load(old.Name); ok {
						s.incompleteAccExporterMap.Delete(old.Name)
						s.Errorf("Account %s has issues importing account %s", label, old.Name)
					}
				}
			}
			return true
		})
	}
}

// Helper to build an internal account structure from a jwt.AccountClaims.
// Lock MUST NOT be held upon entry.
func (s *Server) buildInternalAccount(ac *jwt.AccountClaims) *Account {
	acc := NewAccount(ac.Subject)
	acc.Issuer = ac.Issuer
	// Set this here since we are placing in s.tmpAccounts below and may be
	// referenced by an route RS+, etc.
	s.setAccountSublist(acc)

	// We don't want to register an account that is in the process of
	// being built, however, to solve circular import dependencies, we
	// need to store it here.
	if v, loaded := s.tmpAccounts.LoadOrStore(ac.Subject, acc); loaded {
		return v.(*Account)
	}

	// Update based on claims.
	s.UpdateAccountClaims(acc, ac)

	return acc
}

// Helper to build Permissions from jwt.Permissions
// or return nil if none were specified
func buildPermissionsFromJwt(uc *jwt.Permissions) *Permissions {
	if uc == nil {
		return nil
	}
	var p *Permissions
	if len(uc.Pub.Allow) > 0 || len(uc.Pub.Deny) > 0 {
		p = &Permissions{}
		p.Publish = &SubjectPermission{}
		p.Publish.Allow = uc.Pub.Allow
		p.Publish.Deny = uc.Pub.Deny
	}
	if len(uc.Sub.Allow) > 0 || len(uc.Sub.Deny) > 0 {
		if p == nil {
			p = &Permissions{}
		}
		p.Subscribe = &SubjectPermission{}
		p.Subscribe.Allow = uc.Sub.Allow
		p.Subscribe.Deny = uc.Sub.Deny
	}
	if uc.Resp != nil {
		if p == nil {
			p = &Permissions{}
		}
		p.Response = &ResponsePermission{
			MaxMsgs: uc.Resp.MaxMsgs,
			Expires: uc.Resp.Expires,
		}
		validateResponsePermissions(p)
	}
	return p
}

// Helper to build internal NKeyUser.
func buildInternalNkeyUser(uc *jwt.UserClaims, acts map[string]struct{}, acc *Account) *NkeyUser {
	nu := &NkeyUser{Nkey: uc.Subject, Account: acc, AllowedConnectionTypes: acts, Issued: uc.IssuedAt}
	if uc.IssuerAccount != _EMPTY_ {
		nu.SigningKey = uc.Issuer
	}

	// Now check for permissions.
	p := buildPermissionsFromJwt(&uc.Permissions)
	if p == nil {
		nu.defaultPerms = true
		acc.mu.RLock()
		if acc.defaultPerms != nil {
			p = acc.defaultPerms.clone()
		}
		acc.mu.RUnlock()
	}
	nu.Permissions = p
	return nu
}

func fetchAccount(res AccountResolver, name string) (string, error) {
	if !nkeys.IsValidPublicAccountKey(name) {
		return _EMPTY_, fmt.Errorf("will only fetch valid account keys")
	}
	return res.Fetch(copyString(name))
}

// AccountResolver interface. This is to fetch Account JWTs by public nkeys
type AccountResolver interface {
	Fetch(name string) (string, error)
	Store(name, jwt string) error
	IsReadOnly() bool
	Start(server *Server) error
	IsTrackingUpdate() bool
	Reload() error
	Close()
}

// Default implementations of IsReadOnly/Start so only need to be written when changed
type resolverDefaultsOpsImpl struct{}

func (*resolverDefaultsOpsImpl) IsReadOnly() bool {
	return true
}

func (*resolverDefaultsOpsImpl) IsTrackingUpdate() bool {
	return false
}

func (*resolverDefaultsOpsImpl) Start(*Server) error {
	return nil
}

func (*resolverDefaultsOpsImpl) Reload() error {
	return nil
}

func (*resolverDefaultsOpsImpl) Close() {
}

func (*resolverDefaultsOpsImpl) Store(_, _ string) error {
	return fmt.Errorf("store operation not supported for URL Resolver")
}

// MemAccResolver is a memory only resolver.
// Mostly for testing.
type MemAccResolver struct {
	sm sync.Map
	resolverDefaultsOpsImpl
}

// Fetch will fetch the account jwt claims from the internal sync.Map.
func (m *MemAccResolver) Fetch(name string) (string, error) {
	if j, ok := m.sm.Load(name); ok {
		return j.(string), nil
	}
	return _EMPTY_, ErrMissingAccount
}

// Store will store the account jwt claims in the internal sync.Map.
func (m *MemAccResolver) Store(name, jwt string) error {
	m.sm.Store(name, jwt)
	return nil
}

func (m *MemAccResolver) IsReadOnly() bool {
	return false
}

// URLAccResolver implements an http fetcher.
type URLAccResolver struct {
	url string
	c   *http.Client
	resolverDefaultsOpsImpl
}

// NewURLAccResolver returns a new resolver for the given base URL.
func NewURLAccResolver(url string) (*URLAccResolver, error) {
	if !strings.HasSuffix(url, "/") {
		url += "/"
	}
	// FIXME(dlc) - Make timeout and others configurable.
	// We create our own transport to amortize TLS.
	tr := &http.Transport{
		MaxIdleConns:    10,
		IdleConnTimeout: 30 * time.Second,
	}
	ur := &URLAccResolver{
		url: url,
		c:   &http.Client{Timeout: DEFAULT_ACCOUNT_FETCH_TIMEOUT, Transport: tr},
	}
	return ur, nil
}

// Fetch will fetch the account jwt claims from the base url, appending the
// account name onto the end.
func (ur *URLAccResolver) Fetch(name string) (string, error) {
	url := ur.url + name
	resp, err := ur.c.Get(url)
	if err != nil {
		return _EMPTY_, fmt.Errorf("could not fetch <%q>: %v", redactURLString(url), err)
	} else if resp == nil {
		return _EMPTY_, fmt.Errorf("could not fetch <%q>: no response", redactURLString(url))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return _EMPTY_, fmt.Errorf("could not fetch <%q>: %v", redactURLString(url), resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return _EMPTY_, err
	}
	return string(body), nil
}

// Resolver based on nats for synchronization and backing directory for storage.
type DirAccResolver struct {
	*DirJWTStore
	*Server
	syncInterval time.Duration
	fetchTimeout time.Duration
}

func (dr *DirAccResolver) IsTrackingUpdate() bool {
	return true
}

func (dr *DirAccResolver) Reload() error {
	return dr.DirJWTStore.Reload()
}

// ServerAPIClaimUpdateResponse is the response to $SYS.REQ.ACCOUNT.<id>.CLAIMS.UPDATE and $SYS.REQ.CLAIMS.UPDATE
type ServerAPIClaimUpdateResponse struct {
	Server *ServerInfo        `json:"server"`
	Data   *ClaimUpdateStatus `json:"data,omitempty"`
	Error  *ClaimUpdateError  `json:"error,omitempty"`
}

type ClaimUpdateError struct {
	Account     string `json:"account,omitempty"`
	Code        int    `json:"code"`
	Description string `json:"description,omitempty"`
}

type ClaimUpdateStatus struct {
	Account string `json:"account,omitempty"`
	Code    int    `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

func respondToUpdate(s *Server, respSubj string, acc string, message string, err error) {
	if err == nil {
		if acc == _EMPTY_ {
			s.Debugf("%s", message)
		} else {
			s.Debugf("%s - %s", message, acc)
		}
	} else {
		if acc == _EMPTY_ {
			s.Errorf("%s - %s", message, err)
		} else {
			s.Errorf("%s - %s - %s", message, acc, err)
		}
	}
	if respSubj == _EMPTY_ {
		return
	}

	response := ServerAPIClaimUpdateResponse{
		Server: &ServerInfo{},
	}

	if err == nil {
		response.Data = &ClaimUpdateStatus{
			Account: acc,
			Code:    http.StatusOK,
			Message: message,
		}
	} else {
		response.Error = &ClaimUpdateError{
			Account:     acc,
			Code:        http.StatusInternalServerError,
			Description: fmt.Sprintf("%s - %v", message, err),
		}
	}

	s.sendInternalMsgLocked(respSubj, _EMPTY_, response.Server, response)
}

func handleListRequest(store *DirJWTStore, s *Server, reply string) {
	if reply == _EMPTY_ {
		return
	}
	accIds := make([]string, 0, 1024)
	if err := store.PackWalk(1, func(partialPackMsg string) {
		if tk := strings.Split(partialPackMsg, "|"); len(tk) == 2 {
			accIds = append(accIds, tk[0])
		}
	}); err != nil {
		// let them timeout
		s.Errorf("list request error: %v", err)
	} else {
		s.Debugf("list request responded with %d account ids", len(accIds))
		server := &ServerInfo{}
		response := map[string]any{"server": server, "data": accIds}
		s.sendInternalMsgLocked(reply, _EMPTY_, server, response)
	}
}

func handleDeleteRequest(store *DirJWTStore, s *Server, msg []byte, reply string) {
	var accIds []any
	var subj, sysAccName string
	if sysAcc := s.SystemAccount(); sysAcc != nil {
		sysAccName = sysAcc.GetName()
	}
	// Only operator and operator signing key are allowed to delete
	gk, err := jwt.DecodeGeneric(string(msg))
	if err == nil {
		subj = gk.Subject
		if store.deleteType == NoDelete {
			err = fmt.Errorf("delete must be enabled in server config")
		} else if subj != gk.Issuer {
			err = fmt.Errorf("not self signed")
		} else if _, ok := store.operator[gk.Issuer]; !ok {
			err = fmt.Errorf("not trusted")
		} else if list, ok := gk.Data["accounts"]; !ok {
			err = fmt.Errorf("malformed request")
		} else if accIds, ok = list.([]any); !ok {
			err = fmt.Errorf("malformed request")
		} else {
			for _, entry := range accIds {
				if acc, ok := entry.(string); !ok ||
					acc == _EMPTY_ || !nkeys.IsValidPublicAccountKey(acc) {
					err = fmt.Errorf("malformed request")
					break
				} else if acc == sysAccName {
					err = fmt.Errorf("not allowed to delete system account")
					break
				}
			}
		}
	}
	if err != nil {
		respondToUpdate(s, reply, _EMPTY_, fmt.Sprintf("delete accounts request by %s failed", subj), err)
		return
	}
	errs := []string{}
	passCnt := 0
	for _, acc := range accIds {
		if err := store.delete(acc.(string)); err != nil {
			errs = append(errs, err.Error())
		} else {
			passCnt++
		}
	}
	if len(errs) == 0 {
		respondToUpdate(s, reply, _EMPTY_, fmt.Sprintf("deleted %d accounts", passCnt), nil)
	} else {
		respondToUpdate(s, reply, _EMPTY_, fmt.Sprintf("deleted %d accounts, failed for %d", passCnt, len(errs)),
			errors.New(strings.Join(errs, "\n")))
	}
}

func getOperatorKeys(s *Server) (string, map[string]struct{}, bool, error) {
	var op string
	var strict bool
	keys := make(map[string]struct{})
	if opts := s.getOpts(); opts != nil && len(opts.TrustedOperators) > 0 {
		op = opts.TrustedOperators[0].Subject
		strict = opts.TrustedOperators[0].StrictSigningKeyUsage
		if !strict {
			keys[opts.TrustedOperators[0].Subject] = struct{}{}
		}
		for _, key := range opts.TrustedOperators[0].SigningKeys {
			keys[key] = struct{}{}
		}
	}
	if len(keys) == 0 {
		return _EMPTY_, nil, false, fmt.Errorf("no operator key found")
	}
	return op, keys, strict, nil
}

func claimValidate(claim *jwt.AccountClaims) error {
	vr := &jwt.ValidationResults{}
	claim.Validate(vr)
	if vr.IsBlocking(false) {
		return fmt.Errorf("validation errors: %v", vr.Errors())
	}
	// Align with Account.AddWeightedMappings so JWT pushes that would be
	// silently discarded at install time are rejected up front instead.
	if err := validateAccountClaimMappings(claim); err != nil {
		return err
	}
	return nil
}

// validateAccountClaimMappings applies the same destination checks used by
// Account.AddWeightedMappings (duplicates, weight totals, ValidateMapping,
// NewSubjectTransform). jwt.Mapping.Validate is intentionally weaker.
func validateAccountClaimMappings(claim *jwt.AccountClaims) error {
	if claim == nil {
		return nil
	}
	for src, wms := range claim.Mappings {
		if !IsValidSubject(string(src)) {
			return fmt.Errorf("mapping %q: %w", src, ErrBadSubject)
		}
		seen := make(map[string]struct{})
		tw := make(map[string]uint8)
		for _, m := range wms {
			dest := string(m.Subject)
			if _, ok := seen[dest]; ok {
				return fmt.Errorf("mapping %q: duplicate entry for %q", src, dest)
			}
			seen[dest] = struct{}{}
			weight := m.GetWeight()
			if weight > 100 {
				return fmt.Errorf("mapping %q: individual weights need to be <= 100", src)
			}
			tw[m.Cluster] += weight
			if tw[m.Cluster] > 100 {
				return fmt.Errorf("mapping %q: total weight needs to be <= 100", src)
			}
			if err := ValidateMapping(string(src), dest); err != nil {
				return fmt.Errorf("mapping %q -> %q: %v", src, dest, err)
			}
			if _, err := NewSubjectTransform(string(src), dest); err != nil {
				return fmt.Errorf("mapping %q -> %q: %v", src, dest, err)
			}
		}
	}
	return nil
}

func removeCb(s *Server, pubKey string) {
	v, ok := s.accounts.Load(pubKey)
	if !ok {
		return
	}
	a := v.(*Account)
	s.Debugf("Disable account %s due to remove", pubKey)
	a.mu.Lock()
	// lock out new clients
	a.msubs = 0
	a.mpay = 0
	a.mconns = 0
	a.mleafs = 0
	a.updated = time.Now()
	jsa := a.js
	a.mu.Unlock()
	// set the account to be expired and disconnect clients
	a.expiredTimeout()
	// For JS, we need also to disable it.
	if js := s.getJetStream(); js != nil && jsa != nil {
		js.disableJetStream(jsa)
		// Remove JetStream state in memory, this will be reset
		// on the changed callback from the account in case it is
		// enabled again.
		a.mu.Lock()
		a.js = nil
		a.mu.Unlock()
	}
	// We also need to remove all ServerImport subscriptions
	a.removeAllServiceImportSubs()
	a.mu.Lock()
	a.clearExpirationTimer()
	a.mu.Unlock()
}

func (dr *DirAccResolver) Start(s *Server) error {
	op, opKeys, strict, err := getOperatorKeys(s)
	if err != nil {
		return err
	}
	dr.Lock()
	defer dr.Unlock()
	dr.Server = s
	dr.operator = opKeys
	dr.DirJWTStore.changed = func(pubKey string) {
		if v, ok := s.accounts.Load(pubKey); ok {
			if theJwt, err := dr.LoadAcc(pubKey); err != nil {
				s.Errorf("DirResolver - Update got error on load: %v", err)
			} else {
				acc := v.(*Account)
				if err = s.updateAccountWithClaimJWT(acc, theJwt); err != nil {
					s.Errorf("DirResolver - Update for account %q resulted in error %v", pubKey, err)
				} else {
					if _, jsa, err := acc.checkForJetStream(); err != nil {
						if !IsNatsErr(err, JSNotEnabledForAccountErr) {
							s.Warnf("DirResolver - Error checking for JetStream support for account %q: %v", pubKey, err)
						}
					} else if jsa == nil {
						if err = s.configJetStream(acc, nil); err != nil {
							s.Errorf("DirResolver - Error configuring JetStream for account %q: %v", pubKey, err)
						}
					}
				}
			}
		}
	}
	dr.DirJWTStore.deleted = func(pubKey string) {
		removeCb(s, pubKey)
	}
	packRespIb := s.newRespInbox()
	for _, reqSub := range []string{accUpdateEventSubjOld, accUpdateEventSubjNew} {
		// subscribe to account jwt update requests
		if _, err := s.sysSubscribe(fmt.Sprintf(reqSub, "*"), func(_ *subscription, _ *client, _ *Account, subj, resp string, msg []byte) {
			var pubKey string
			tk := strings.Split(subj, tsep)
			if len(tk) == accUpdateTokensNew {
				pubKey = tk[accReqAccIndex]
			} else if len(tk) == accUpdateTokensOld {
				pubKey = tk[accUpdateAccIdxOld]
			} else {
				s.Debugf("DirResolver - jwt update skipped due to bad subject %q", subj)
				return
			}
			if claim, err := jwt.DecodeAccountClaims(string(msg)); err != nil {
				respondToUpdate(s, resp, "n/a", "jwt update resulted in error", err)
			} else if err := claimValidate(claim); err != nil {
				respondToUpdate(s, resp, claim.Subject, "jwt validation failed", err)
			} else if claim.Subject != pubKey {
				err := errors.New("subject does not match jwt content")
				respondToUpdate(s, resp, pubKey, "jwt update resulted in error", err)
			} else if claim.Issuer == op && strict {
				err := errors.New("operator requires issuer to be a signing key")
				respondToUpdate(s, resp, pubKey, "jwt update resulted in error", err)
			} else if err := dr.save(pubKey, string(msg)); err != nil {
				respondToUpdate(s, resp, pubKey, "jwt update resulted in error", err)
			} else {
				respondToUpdate(s, resp, pubKey, "jwt updated", nil)
			}
		}); err != nil {
			return fmt.Errorf("error setting up update handling: %v", err)
		}
	}
	if _, err := s.sysSubscribe(accClaimsReqSubj, func(_ *subscription, c *client, _ *Account, _, resp string, msg []byte) {
		// As this is a raw message, we need to extract payload and only decode claims from it,
		// in case request is sent with headers.
		_, msg = c.msgParts(msg)
		if claim, err := jwt.DecodeAccountClaims(string(msg)); err != nil {
			respondToUpdate(s, resp, "n/a", "jwt update resulted in error", err)
		} else if claim.Issuer == op && strict {
			err := errors.New("operator requires issuer to be a signing key")
			respondToUpdate(s, resp, claim.Subject, "jwt update resulted in error", err)
		} else if err := claimValidate(claim); err != nil {
			respondToUpdate(s, resp, claim.Subject, "jwt validation failed", err)
		} else if err := dr.save(claim.Subject, string(msg)); err != nil {
			respondToUpdate(s, resp, claim.Subject, "jwt update resulted in error", err)
		} else {
			respondToUpdate(s, resp, claim.Subject, "jwt updated", nil)
		}
	}); err != nil {
		return fmt.Errorf("error setting up update handling: %v", err)
	}
	// respond to lookups with our version
	if _, err := s.sysSubscribe(fmt.Sprintf(accLookupReqSubj, "*"), func(_ *subscription, _ *client, _ *Account, subj, reply string, msg []byte) {
		if reply == _EMPTY_ {
			return
		}
		tk := strings.Split(subj, tsep)
		if len(tk) != accLookupReqTokens {
			return
		}
		accName := tk[accReqAccIndex]
		if theJWT, err := dr.DirJWTStore.LoadAcc(accName); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				s.Debugf("DirResolver - Could not find account %q", accName)
				// Reply with empty response to signal absence of JWT to others.
				s.sendInternalMsgLocked(reply, _EMPTY_, nil, nil)
			} else {
				s.Errorf("DirResolver - Error looking up account %q: %v", accName, err)
			}
		} else {
			s.sendInternalMsgLocked(reply, _EMPTY_, nil, []byte(theJWT))
		}
	}); err != nil {
		return fmt.Errorf("error setting up lookup request handling: %v", err)
	}
	// respond to pack requests with one or more pack messages
	// an empty message signifies the end of the response responder.
	if _, err := s.sysSubscribeQ(accPackReqSubj, "responder", func(_ *subscription, _ *client, _ *Account, _, reply string, theirHash []byte) {
		if reply == _EMPTY_ {
			return
		}
		ourHash := dr.DirJWTStore.Hash()
		if bytes.Equal(theirHash, ourHash[:]) {
			s.sendInternalMsgLocked(reply, _EMPTY_, nil, []byte{})
			s.Debugf("DirResolver - Pack request matches hash %x", ourHash[:])
		} else if err := dr.DirJWTStore.PackWalk(1, func(partialPackMsg string) {
			s.sendInternalMsgLocked(reply, _EMPTY_, nil, []byte(partialPackMsg))
		}); err != nil {
			// let them timeout
			s.Errorf("DirResolver - Pack request error: %v", err)
		} else {
			s.Debugf("DirResolver - Pack request hash %x - finished responding with hash %x", theirHash, ourHash)
			s.sendInternalMsgLocked(reply, _EMPTY_, nil, []byte{})
		}
	}); err != nil {
		return fmt.Errorf("error setting up pack request handling: %v", err)
	}
	// respond to list requests with one message containing all account ids
	if _, err := s.sysSubscribe(accListReqSubj, func(_ *subscription, _ *client, _ *Account, _, reply string, _ []byte) {
		handleListRequest(dr.DirJWTStore, s, reply)
	}); err != nil {
		return fmt.Errorf("error setting up list request handling: %v", err)
	}
	if _, err := s.sysSubscribe(accDeleteReqSubj, func(_ *subscription, c *client, _ *Account, _, reply string, msg []byte) {
		// As this is a raw message, we need to extract payload and only decode claims from it,
		// in case request is sent with headers.
		_, msg = c.msgParts(msg)
		handleDeleteRequest(dr.DirJWTStore, s, msg, reply)
	}); err != nil {
		return fmt.Errorf("error setting up delete request handling: %v", err)
	}
	// embed pack responses into store
	if _, err := s.sysSubscribe(packRespIb, func(_ *subscription, _ *client, _ *Account, _, _ string, msg []byte) {
		hash := dr.DirJWTStore.Hash()
		if len(msg) == 0 { // end of response stream
			s.Debugf("DirResolver - Merging finished and resulting in: %x", dr.DirJWTStore.Hash())
			return
		} else if err := dr.DirJWTStore.Merge(string(msg)); err != nil {
			s.Errorf("DirResolver - Merging resulted in error: %v", err)
		} else {
			s.Debugf("DirResolver - Merging succeeded and changed %x to %x", hash, dr.DirJWTStore.Hash())
		}
	}); err != nil {
		return fmt.Errorf("error setting up pack response handling: %v", err)
	}
	// periodically send out pack message
	quit := s.quitCh
	s.startGoRoutine(func() {
		defer s.grWG.Done()
		ticker := time.NewTicker(dr.syncInterval)
		for {
			select {
			case <-quit:
				ticker.Stop()
				return
			case <-ticker.C:
			}
			ourHash := dr.DirJWTStore.Hash()
			s.Debugf("DirResolver - Checking store state: %x", ourHash)
			s.sendInternalMsgLocked(accPackReqSubj, packRespIb, nil, ourHash[:])
		}
	})
	s.Noticef("Managing all jwt in exclusive directory %s", dr.directory)
	return nil
}

func (dr *DirAccResolver) Fetch(name string) (string, error) {
	if theJWT, err := dr.LoadAcc(name); theJWT != _EMPTY_ {
		return theJWT, nil
	} else {
		dr.Lock()
		srv := dr.Server
		to := dr.fetchTimeout
		dr.Unlock()
		if srv == nil {
			return _EMPTY_, err
		}
		return srv.fetch(dr, name, to) // lookup from other server
	}
}

func (dr *DirAccResolver) Store(name, jwt string) error {
	return dr.saveIfNewer(name, jwt)
}

type DirResOption func(s *DirAccResolver) error

// limits the amount of time spent waiting for an account fetch to complete
func FetchTimeout(to time.Duration) DirResOption {
	return func(r *DirAccResolver) error {
		if to <= time.Duration(0) {
			return fmt.Errorf("Fetch timeout %v is too smal", to)
		}
		r.fetchTimeout = to
		return nil
	}
}

func (dr *DirAccResolver) apply(opts ...DirResOption) error {
	for _, o := range opts {
		if err := o(dr); err != nil {
			return err
		}
	}
	return nil
}

func NewDirAccResolver(path string, limit int64, syncInterval time.Duration, delete deleteType, opts ...DirResOption) (*DirAccResolver, error) {
	if limit == 0 {
		limit = math.MaxInt64
	}
	if syncInterval <= 0 {
		syncInterval = time.Minute
	}
	store, err := NewExpiringDirJWTStore(path, false, true, delete, 0, limit, false, 0, nil)
	if err != nil {
		return nil, err
	}

	res := &DirAccResolver{store, nil, syncInterval, DEFAULT_ACCOUNT_FETCH_TIMEOUT}
	if err := res.apply(opts...); err != nil {
		return nil, err
	}
	return res, nil
}

// Caching resolver using nats for lookups and making use of a directory for storage
type CacheDirAccResolver struct {
	DirAccResolver
	ttl time.Duration
}

func (s *Server) fetch(res AccountResolver, name string, timeout time.Duration) (string, error) {
	if s == nil {
		return _EMPTY_, ErrNoAccountResolver
	}
	respC := make(chan []byte, 1)
	accountLookupRequest := fmt.Sprintf(accLookupReqSubj, name)
	s.mu.Lock()
	if s.sys == nil || s.sys.replies == nil {
		s.mu.Unlock()
		return _EMPTY_, fmt.Errorf("eventing shut down")
	}
	// Resolver will wait for detected active servers to reply
	// before serving an error in case there weren't any found.
	expectedServers := len(s.sys.servers)
	replySubj := s.newRespInbox()
	replies := s.sys.replies

	// Store our handler.
	replies[replySubj] = func(sub *subscription, _ *client, _ *Account, subject, _ string, msg []byte) {
		var clone []byte
		isEmpty := len(msg) == 0
		if !isEmpty {
			clone = make([]byte, len(msg))
			copy(clone, msg)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		expectedServers--
		// Skip empty responses until getting all the available servers.
		if isEmpty && expectedServers > 0 {
			return
		}
		// Use the first valid response if there is still interest or
		// one of the empty responses to signal that it was not found.
		if _, ok := replies[replySubj]; ok {
			select {
			case respC <- clone:
			default:
			}
		}
	}
	s.sendInternalMsg(accountLookupRequest, replySubj, nil, []byte{})
	quit := s.quitCh
	s.mu.Unlock()
	var err error
	var theJWT string
	select {
	case <-quit:
		err = errors.New("fetching jwt failed due to shutdown")
	case <-time.After(timeout):
		err = errors.New("fetching jwt timed out")
	case m := <-respC:
		if len(m) == 0 {
			err = errors.New("account jwt not found")
		} else if err = res.Store(name, string(m)); err == nil {
			theJWT = string(m)
		}
	}
	s.mu.Lock()
	delete(replies, replySubj)
	s.mu.Unlock()
	close(respC)
	return theJWT, err
}

func NewCacheDirAccResolver(path string, limit int64, ttl time.Duration, opts ...DirResOption) (*CacheDirAccResolver, error) {
	if limit <= 0 {
		limit = 1_000
	}
	store, err := NewExpiringDirJWTStore(path, false, true, HardDelete, 0, limit, true, ttl, nil)
	if err != nil {
		return nil, err
	}
	res := &CacheDirAccResolver{DirAccResolver{store, nil, 0, DEFAULT_ACCOUNT_FETCH_TIMEOUT}, ttl}
	if err := res.apply(opts...); err != nil {
		return nil, err
	}
	return res, nil
}

func (dr *CacheDirAccResolver) Start(s *Server) error {
	op, opKeys, strict, err := getOperatorKeys(s)
	if err != nil {
		return err
	}
	dr.Lock()
	defer dr.Unlock()
	dr.Server = s
	dr.operator = opKeys
	dr.DirJWTStore.changed = func(pubKey string) {
		if v, ok := s.accounts.Load(pubKey); !ok {
		} else if theJwt, err := dr.LoadAcc(pubKey); err != nil {
			s.Errorf("DirResolver - Update got error on load: %v", err)
		} else if err := s.updateAccountWithClaimJWT(v.(*Account), theJwt); err != nil {
			s.Errorf("DirResolver - Update resulted in error %v", err)
		}
	}
	dr.DirJWTStore.deleted = func(pubKey string) {
		removeCb(s, pubKey)
	}
	for _, reqSub := range []string{accUpdateEventSubjOld, accUpdateEventSubjNew} {
		// subscribe to account jwt update requests
		if _, err := s.sysSubscribe(fmt.Sprintf(reqSub, "*"), func(_ *subscription, _ *client, _ *Account, subj, resp string, msg []byte) {
			var pubKey string
			tk := strings.Split(subj, tsep)
			if len(tk) == accUpdateTokensNew {
				pubKey = tk[accReqAccIndex]
			} else if len(tk) == accUpdateTokensOld {
				pubKey = tk[accUpdateAccIdxOld]
			} else {
				s.Debugf("DirResolver - jwt update cache skipped due to bad subject %q", subj)
				return
			}
			if claim, err := jwt.DecodeAccountClaims(string(msg)); err != nil {
				respondToUpdate(s, resp, pubKey, "jwt update cache resulted in error", err)
			} else if claim.Subject != pubKey {
				err := errors.New("subject does not match jwt content")
				respondToUpdate(s, resp, pubKey, "jwt update cache resulted in error", err)
			} else if claim.Issuer == op && strict {
				err := errors.New("operator requires issuer to be a signing key")
				respondToUpdate(s, resp, pubKey, "jwt update cache resulted in error", err)
			} else if _, ok := s.accounts.Load(pubKey); !ok {
				respondToUpdate(s, resp, pubKey, "jwt update cache skipped", nil)
			} else if err := claimValidate(claim); err != nil {
				respondToUpdate(s, resp, claim.Subject, "jwt update cache validation failed", err)
			} else if err := dr.save(pubKey, string(msg)); err != nil {
				respondToUpdate(s, resp, pubKey, "jwt update cache resulted in error", err)
			} else {
				respondToUpdate(s, resp, pubKey, "jwt updated cache", nil)
			}
		}); err != nil {
			return fmt.Errorf("error setting up update handling: %v", err)
		}
	}
	if _, err := s.sysSubscribe(accClaimsReqSubj, func(_ *subscription, c *client, _ *Account, _, resp string, msg []byte) {
		// As this is a raw message, we need to extract payload and only decode claims from it,
		// in case request is sent with headers.
		_, msg = c.msgParts(msg)
		if claim, err := jwt.DecodeAccountClaims(string(msg)); err != nil {
			respondToUpdate(s, resp, "n/a", "jwt update cache resulted in error", err)
		} else if claim.Issuer == op && strict {
			err := errors.New("operator requires issuer to be a signing key")
			respondToUpdate(s, resp, claim.Subject, "jwt update cache resulted in error", err)
		} else if _, ok := s.accounts.Load(claim.Subject); !ok {
			respondToUpdate(s, resp, claim.Subject, "jwt update cache skipped", nil)
		} else if err := claimValidate(claim); err != nil {
			respondToUpdate(s, resp, claim.Subject, "jwt update cache validation failed", err)
		} else if err := dr.save(claim.Subject, string(msg)); err != nil {
			respondToUpdate(s, resp, claim.Subject, "jwt update cache resulted in error", err)
		} else {
			respondToUpdate(s, resp, claim.Subject, "jwt updated cache", nil)
		}
	}); err != nil {
		return fmt.Errorf("error setting up update handling: %v", err)
	}
	// respond to list requests with one message containing all account ids
	if _, err := s.sysSubscribe(accListReqSubj, func(_ *subscription, _ *client, _ *Account, _, reply string, _ []byte) {
		handleListRequest(dr.DirJWTStore, s, reply)
	}); err != nil {
		return fmt.Errorf("error setting up list request handling: %v", err)
	}
	if _, err := s.sysSubscribe(accDeleteReqSubj, func(_ *subscription, c *client, _ *Account, _, reply string, msg []byte) {
		// As this is a raw message, we need to extract payload and only decode claims from it,
		// in case request is sent with headers.
		_, msg = c.msgParts(msg)
		handleDeleteRequest(dr.DirJWTStore, s, msg, reply)
	}); err != nil {
		return fmt.Errorf("error setting up list request handling: %v", err)
	}
	s.Noticef("Managing some jwt in exclusive directory %s", dr.directory)
	return nil
}

func (dr *CacheDirAccResolver) Reload() error {
	return dr.DirAccResolver.Reload()
}
