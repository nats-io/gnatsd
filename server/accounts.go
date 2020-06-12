// Copyright 2018-2020 The NATS Authors
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
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nuid"
)

// For backwards compatibility with NATS < 2.0, users who are not explicitly defined into an
// account will be grouped in the default global account.
const globalAccountName = DEFAULT_GLOBAL_ACCOUNT

// Account are subject namespace definitions. By default no messages are shared between accounts.
// You can share via Exports and Imports of Streams and Services.
type Account struct {
	Name         string
	Nkey         string
	Issuer       string
	claimJWT     string
	updated      time.Time
	mu           sync.RWMutex
	sqmu         sync.Mutex
	sl           *Sublist
	ic           *client
	isid         uint64
	etmr         *time.Timer
	ctmr         *time.Timer
	strack       map[string]sconns
	nrclients    int32
	sysclients   int32
	nleafs       int32
	nrleafs      int32
	clients      map[*client]struct{}
	rm           map[string]int32
	lqws         map[string]int32
	usersRevoked map[string]int64
	actsRevoked  map[string]int64
	lleafs       []*client
	imports      importMap
	exports      exportMap
	js           *jsAccount
	jsLimits     *JetStreamAccountLimits
	limits
	expired      bool
	signingKeys  []string
	srv          *Server // server this account is registered with (possibly nil)
	lds          string  // loop detection subject for leaf nodes
	siReply      []byte  // service reply prefix, will form wildcard subscription.
	prand        *rand.Rand
	eventIds     *nuid.NUID
	eventIdsMu   sync.Mutex
	defaultPerms *Permissions
}

// Account based limits.
type limits struct {
	mpay   int32
	msubs  int32
	mconns int32
	mleafs int32
}

// Used to track remote clients and leafnodes per remote server.
type sconns struct {
	conns int32
	leafs int32
}

// Import stream mapping struct
type streamImport struct {
	acc     *Account
	from    string
	prefix  string
	claim   *jwt.Import
	invalid bool
}

// Import service mapping struct
type serviceImport struct {
	acc      *Account
	claim    *jwt.Import
	se       *serviceExport
	sub      *subscription
	from     string
	to       string
	exsub    string
	ts       int64
	rt       ServiceRespType
	latency  *serviceLatency
	m1       *ServiceLatency
	rc       *client
	hasWC    bool
	response bool
	invalid  bool
	tracking bool
	share    bool
}

// This is used to record when we create a mapping for implicit service
// imports. We use this to clean up entries that are not singletons when
// we detect that interest is no longer present. The key to the map will
// be the actual interest. We record the mapped subject and the account.
type serviceRespEntry struct {
	acc  *Account
	msub string
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
	tokenReq bool
	approved map[string]*Account
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
}

// Used to track service latency.
type serviceLatency struct {
	sampling int8
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
	services map[string]*serviceImport
	rrMap    map[string][]*serviceRespEntry
}

// NewAccount creates a new unlimited account with the given name.
func NewAccount(name string) *Account {
	a := &Account{
		Name:     name,
		limits:   limits{-1, -1, -1, -1},
		eventIds: nuid.New(),
	}

	return a
}

// Used to create shallow copies of accounts for transfer
// from opts to real accounts in server struct.
func (a *Account) shallowCopy() *Account {
	na := NewAccount(a.Name)
	na.Nkey = a.Nkey
	na.Issuer = a.Issuer

	if a.imports.streams != nil {
		na.imports.streams = make([]*streamImport, 0, len(a.imports.streams))
		for _, v := range a.imports.streams {
			si := *v
			na.imports.streams = append(na.imports.streams, &si)
		}
	}
	if a.imports.services != nil {
		na.imports.services = make(map[string]*serviceImport)
		for k, v := range a.imports.services {
			si := *v
			na.imports.services[k] = &si
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
	na.jsLimits = a.jsLimits

	return na
}

// nextEventID uses its own lock for better concurrency.
func (a *Account) nextEventID() string {
	a.eventIdsMu.Lock()
	id := a.eventIds.Next()
	a.eventIdsMu.Unlock()
	return id
}

// Called to track a remote server and connections and leafnodes it
// has for this account.
func (a *Account) updateRemoteServer(m *AccountNumConns) {
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
		clients = make([]*client, 0, len(a.clients))
		for c := range a.clients {
			clients = append(clients, c)
		}
		sort.Slice(clients, func(i, j int) bool {
			return clients[i].start.After(clients[j].start)
		})
		over := (len(a.clients) - int(a.sysclients) + int(a.nrclients)) - int(a.mconns)
		if over < len(clients) {
			clients = clients[:over]
		}
	}
	// Now check leafnodes.
	mtlce := a.mleafs != jwt.NoLimit && (a.nleafs+a.nrleafs > a.mleafs)
	if mtlce {
		// Take ones from the end.
		leafs := a.lleafs
		over := int(a.nleafs + a.nrleafs - a.mleafs)
		if over < len(leafs) {
			leafs = leafs[len(leafs)-over:]
		}
		clients = append(clients, leafs...)
	}
	a.mu.Unlock()

	// If we have exceeded our max clients this will be populated.
	for _, c := range clients {
		c.maxAccountConnExceeded()
	}
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

// NumConnections returns active number of clients for this account for
// all known servers.
func (a *Account) NumConnections() int {
	a.mu.RLock()
	nc := len(a.clients) + int(a.nrclients)
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
	return int(a.sl.Count())
}

// SubscriptionInterest returns true if this account has a matching subscription
// for the given `subject`. Works only for literal subjects.
// TODO: Add support for wildcards
func (a *Account) SubscriptionInterest(subject string) bool {
	var interest bool
	a.mu.RLock()
	if a.sl != nil {
		if res := a.sl.Match(subject); len(res.psubs)+len(res.qsubs) > 0 {
			interest = true
		}
	}
	a.mu.RUnlock()
	return interest
}

// addClient keeps our accounting of local active clients or leafnodes updated.
// Returns previous total.
func (a *Account) addClient(c *client) int {
	a.mu.Lock()
	n := len(a.clients)
	if a.clients != nil {
		a.clients[c] = struct{}{}
	}
	added := n != len(a.clients)
	if added {
		if c.kind == SYSTEM {
			a.sysclients++
		} else if c.kind == LEAF {
			a.nleafs++
			a.lleafs = append(a.lleafs, c)
		}
	}
	a.mu.Unlock()

	if c != nil && c.srv != nil && added {
		c.srv.accConnsUpdate(a)
	}

	return n
}

// Helper function to remove leaf nodes. If number of leafnodes gets large
// this may need to be optimized out of linear search but believe number
// of active leafnodes per account scope to be small and therefore cache friendly.
// Lock should be held on account.
func (a *Account) removeLeafNode(c *client) {
	ll := len(a.lleafs)
	for i, l := range a.lleafs {
		if l == c {
			a.lleafs[i] = a.lleafs[ll-1]
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
	removed := n != len(a.clients)
	if removed {
		if c.kind == SYSTEM {
			a.sysclients--
		} else if c.kind == LEAF {
			a.nleafs--
			a.removeLeafNode(c)
		}
	}
	jwt := a.claimJWT
	name := a.Name
	a.mu.Unlock()

	if c != nil && c.srv != nil && removed {
		c.srv.mu.Lock()
		var cleanupChan chan string
		doRemove := a != c.srv.gacc
		if n <= 1 && doRemove && jwt != "" && c.srv.sys != nil &&
			a != c.srv.sys.account && c.srv.accCleanupChan != nil {
			cleanupChan = c.srv.accCleanupChan
		}
		c.srv.mu.Unlock()
		if doRemove {
			c.srv.accConnsUpdate(a)
			if cleanupChan != nil {
				cleanupChan <- name
			}
		}
	}
	return n
}

func (a *Account) randomClient() *client {
	if a.ic != nil {
		return a.ic
	}
	var c *client
	for c = range a.clients {
		break
	}
	return c
}

// AddServiceExport will configure the account with the defined export.
func (a *Account) AddServiceExport(subject string, accounts []*Account) error {
	return a.AddServiceExportWithResponse(subject, Singleton, accounts)
}

// AddServiceExportWithResponse will configure the account with the defined export and response type.
func (a *Account) AddServiceExportWithResponse(subject string, respType ServiceRespType, accounts []*Account) error {
	if a == nil {
		return ErrMissingAccount
	}
	a.mu.Lock()
	defer a.mu.Unlock()

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

	if accounts != nil {
		// empty means auth required but will be import token.
		if len(accounts) == 0 {
			se.tokenReq = true
		} else {
			if se.approved == nil {
				se.approved = make(map[string]*Account, len(accounts))
			}
			for _, acc := range accounts {
				se.approved[acc.Name] = acc
			}
		}
	}
	lrt := a.lowestServiceExportResponseTime()
	se.acc = a
	se.respThresh = DEFAULT_SERVICE_EXPORT_RESPONSE_THRESHOLD
	a.exports.services[subject] = se
	if nlrt := a.lowestServiceExportResponseTime(); nlrt != lrt {
		a.updateAllClientsServiceExportResponseTime(nlrt)
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

	if sampling < 1 || sampling > 100 {
		return ErrBadSampling
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
	s.accounts.Range(func(k, v interface{}) bool {
		acc := v.(*Account)
		acc.mu.Lock()
		for _, im := range acc.imports.services {
			if im != nil && im.acc.Name == a.Name && subjectIsSubsetMatch(im.to, service) {
				im.latency = ea.latency
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
	if a == nil || a.exports.services == nil {
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
	s.accounts.Range(func(k, v interface{}) bool {
		acc := v.(*Account)
		acc.mu.Lock()
		for _, im := range acc.imports.services {
			if im != nil && im.acc.Name == a.Name {
				if subjectIsSubsetMatch(im.to, service) {
					im.latency, im.m1 = nil, nil
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
	Requestor      LatencyClient `json:"requestor,omitempty"`
	Responder      LatencyClient `json:"responder,omitempty"`
	RequestStart   time.Time     `json:"start"`
	ServiceLatency time.Duration `json:"service"`
	SystemLatency  time.Duration `json:"system"`
	TotalLatency   time.Duration `json:"total"`
}

// ServiceLatencyType is the NATS Event Type for ServiceLatency
const ServiceLatencyType = "io.nats.server.metric.v1.service_latency"

// LatencyClient is the JSON message structure assigned to requestors and responders.
// Note that for a requestor, the only information shared by default is the RTT used
// to calculate the total latency. The requestor's account can designate to share
// the additional information in the service import.
type LatencyClient struct {
	Account string        `json:"acc"`
	RTT     time.Duration `json:"rtt"`
	Start   time.Time     `json:"start,omitempty"`
	User    string        `json:"user,omitempty"`
	Name    string        `json:"name,omitempty"`
	Lang    string        `json:"lang,omitempty"`
	Version string        `json:"ver,omitempty"`
	IP      string        `json:"ip,omitempty"`
	CID     uint64        `json:"cid,omitempty"`
	Server  string        `json:"server,omitempty"`
}

// NATSTotalTime is a helper function that totals the NATS latencies.
func (nl *ServiceLatency) NATSTotalTime() time.Duration {
	return nl.Requestor.RTT + nl.Responder.RTT + nl.SystemLatency
}

// Merge function to merge m1 and m2 (requestor and responder) measurements
// when there are two samples. This happens when the requestor and responder
// are on different servers.
//
// m2 ServiceLatency is correct, so use that.
// m1 TotalLatency is correct, so use that.
// Will use those to back into NATS latency.
func (m1 *ServiceLatency) merge(m2 *ServiceLatency) {
	m1.SystemLatency = m1.ServiceLatency - (m2.ServiceLatency + m2.Responder.RTT)
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
func (a *Account) sendBadRequestTrackingLatency(si *serviceImport, requestor *client) {
	sl := &ServiceLatency{
		Status:    400,
		Error:     "Bad Request",
		Requestor: requestor.getClientInfo(si.share),
	}
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
	rc := si.rc
	share := si.share
	ts := si.ts
	a.mu.RUnlock()
	if rc != nil {
		sl.Requestor = rc.getClientInfo(share)
	}
	sl.RequestStart = time.Unix(0, ts-int64(sl.Requestor.RTT)).UTC()
	a.sendLatencyResult(si, sl)
}

func (a *Account) sendBackendErrorTrackingLatency(si *serviceImport, reason rsiReason) {
	sl := &ServiceLatency{}
	a.mu.RLock()
	rc := si.rc
	share := si.share
	ts := si.ts
	a.mu.RUnlock()
	if rc != nil {
		sl.Requestor = rc.getClientInfo(share)
	}
	sl.RequestStart = time.Unix(0, ts-int64(sl.Requestor.RTT)).UTC()
	if reason == rsiNoDelivery {
		sl.Status = 503
		sl.Error = "Service Unavailable"
	} else if reason == rsiTimeout {
		sl.Status = 504
		sl.Error = "Service Timeout"
	}
	a.sendLatencyResult(si, sl)
}

// sendTrackingMessage will send out the appropriate tracking information for the
// service request/response latency. This is called when the requestor's server has
// received the response.
// TODO(dlc) - holding locks for RTTs may be too much long term. Should revisit.
func (a *Account) sendTrackingLatency(si *serviceImport, responder *client) bool {
	if si.rc == nil {
		return true
	}

	ts := time.Now()
	serviceRTT := time.Duration(ts.UnixNano() - si.ts)
	requestor := si.rc

	sl := &ServiceLatency{
		Status:    200,
		Requestor: requestor.getClientInfo(si.share),
		Responder: responder.getClientInfo(true),
	}
	sl.RequestStart = time.Unix(0, si.ts-int64(sl.Requestor.RTT)).UTC()
	sl.ServiceLatency = serviceRTT - sl.Responder.RTT
	sl.TotalLatency = sl.Requestor.RTT + serviceRTT
	if sl.Responder.RTT > 0 {
		sl.SystemLatency = time.Since(ts)
		sl.TotalLatency += sl.SystemLatency
	}
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
// Lock should be held.
func (a *Account) updateAllClientsServiceExportResponseTime(lrt time.Duration) {
	for c := range a.clients {
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
	lrt := time.Duration(5 * time.Minute)
	for _, se := range a.exports.services {
		if se.respThresh < lrt {
			lrt = se.respThresh
		}
	}
	return lrt
}

// AddServiceImportWithClaim will add in the service import via the jwt claim.
func (a *Account) AddServiceImportWithClaim(destination *Account, from, to string, imClaim *jwt.Import) error {
	if destination == nil {
		return ErrMissingAccount
	}
	// Empty means use from. Also means we can use wildcards since we are not doing remapping.
	if !IsValidSubject(from) || (to != "" && (!IsValidLiteralSubject(from) || !IsValidLiteralSubject(to))) {
		return ErrInvalidSubject
	}

	// Empty means use from.
	if to == "" {
		to = from
	}
	// First check to see if the account has authorized us to route to the "to" subject.
	if !destination.checkServiceImportAuthorized(a, to, imClaim) {
		return ErrServiceImportAuthorization
	}

	_, err := a.addServiceImport(destination, from, to, imClaim)
	return err
}

// SetServiceImportSharing will allow sharing of information about requests with the export account.
// Used for service latency tracking at the moment.
func (a *Account) SetServiceImportSharing(destination *Account, to string, allow bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.isClaimAccount() {
		return fmt.Errorf("claim based accounts can not be updated directly")
	}
	for _, si := range a.imports.services {
		if si.acc == destination && si.to == to {
			si.share = allow
			return nil
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
	return a.NumPendingResponses("")
}

// NumResponsesPending returns the number of responses outstanding for service exports
// on this account. An empty filter string returns all responses regardless of which export.
// If you specify the filter we will only return ones that are for that export.
// NOTE this is only for what this server is tracking.
func (a *Account) NumPendingResponses(filter string) int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if filter == "" {
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
	delete(a.exports.responses, si.from)
	dest := si.acc
	to := si.to
	tracking := si.tracking
	rc := si.rc
	a.mu.Unlock()

	if tracking && rc != nil {
		a.sendBackendErrorTrackingLatency(si, reason)
	}

	dest.checkForReverseEntry(to, si, false)
}

// removeServiceImport will remove the route by subject.
func (a *Account) removeServiceImport(subject string) {
	a.mu.Lock()
	si, ok := a.imports.services[subject]
	delete(a.imports.services, subject)

	var sid []byte
	c := a.ic

	if ok && si != nil {
		if a.ic != nil && si.sub != nil && si.sub.sid != nil {
			sid = si.sub.sid
		}
	}
	a.mu.Unlock()

	if sid != nil {
		c.processUnsub(sid)
	}
}

// This tracks responses to service requests mappings. This is used for cleanup.
func (a *Account) addReverseRespMapEntry(acc *Account, reply, from string) {
	a.mu.Lock()
	if a.imports.rrMap == nil {
		a.imports.rrMap = make(map[string][]*serviceRespEntry)
	}
	sre := &serviceRespEntry{acc, from}
	sra := a.imports.rrMap[reply]
	a.imports.rrMap[reply] = append(sra, sre)
	a.mu.Unlock()
}

// checkForReverseEntries is for when we are trying to match reverse entries to a wildcard.
// This will be called from checkForReverseEntry when the reply arg is a wildcard subject.
// This will usually be called in a go routine since we need to walk all the entries.
func (a *Account) checkForReverseEntries(reply string, checkInterest bool) {
	a.mu.RLock()
	if len(a.imports.rrMap) == 0 {
		a.mu.RUnlock()
		return
	}

	if subjectIsLiteral(reply) {
		a.mu.RUnlock()
		a.checkForReverseEntry(reply, nil, checkInterest)
		return
	}

	var _rs [32]string
	rs := _rs[:0]

	for k := range a.imports.rrMap {
		if subjectIsSubsetMatch(k, reply) {
			rs = append(rs, k)
		}
	}
	a.mu.RUnlock()

	for _, reply := range rs {
		a.checkForReverseEntry(reply, nil, checkInterest)
	}
}

// This checks for any response map entries. If you specify an si we will only match and
// clean up for that one, otherwise we remove them all.
func (a *Account) checkForReverseEntry(reply string, si *serviceImport, checkInterest bool) {
	a.mu.RLock()
	if len(a.imports.rrMap) == 0 {
		a.mu.RUnlock()
		return
	}

	if subjectHasWildcard(reply) {
		a.mu.RUnlock()
		go a.checkForReverseEntries(reply, checkInterest)
		return
	}

	sres := a.imports.rrMap[reply]
	if sres == nil {
		a.mu.RUnlock()
		return
	}

	// If we are here we have an entry we should check.
	// If requested we will first check if there is any
	// interest for this subject for the entire account.
	// If there is we can not delete any entries yet.
	// Note that if we are here reply has to be a literal subject.
	if checkInterest {
		rr := a.sl.Match(reply)
		// If interest still exists we can not clean these up yet.
		if len(rr.psubs)+len(rr.qsubs) > 0 {
			a.mu.RUnlock()
			return
		}
	}
	a.mu.RUnlock()

	// Delete the appropriate entries here based on optional si.
	a.mu.Lock()
	if si == nil {
		delete(a.imports.rrMap, reply)
	} else {
		// Find the one we are looking for..
		for i, sre := range sres {
			if sre.msub == si.from {
				sres = append(sres[:i], sres[i+1:]...)
				break
			}
		}
		if len(sres) > 0 {
			a.imports.rrMap[si.to] = sres
		} else {
			delete(a.imports.rrMap, si.to)
		}
	}
	a.mu.Unlock()

	// If we are here we no longer have interest and we have
	// response entries that we should clean up.
	if si == nil {
		for _, sre := range sres {
			acc := sre.acc
			var trackingCleanup bool
			var rsi *serviceImport
			acc.mu.Lock()
			if rsi = acc.exports.responses[sre.msub]; rsi != nil {
				delete(acc.exports.responses, rsi.from)
				trackingCleanup = rsi.tracking && rsi.rc != nil
			}
			acc.mu.Unlock()

			if trackingCleanup {
				acc.sendReplyInterestLostTrackLatency(rsi)
			}
		}
	}
}

// Internal check to see if a service import exists.
func (a *Account) serviceImportExists(dest *Account, from string) bool {
	a.mu.RLock()
	dup := a.imports.services[from]
	a.mu.RUnlock()
	return dup != nil
}

// Add a service import.
// This does no checks and should only be called by the msg processing code. Use
// AddServiceImport from above if responding to user input or config changes, etc.
func (a *Account) addServiceImport(dest *Account, from, to string, claim *jwt.Import) (*serviceImport, error) {
	rt := Singleton
	var lat *serviceLatency

	dest.mu.RLock()
	se := dest.getServiceExport(to)
	if se != nil {
		rt = se.respType
		lat = se.latency
	}
	dest.mu.RUnlock()

	a.mu.Lock()
	if a.imports.services == nil {
		a.imports.services = make(map[string]*serviceImport)
	} else if dup := a.imports.services[from]; dup != nil {
		a.mu.Unlock()
		return nil, fmt.Errorf("duplicate service import subject %q, previously used in import for account %q, subject %q",
			from, dup.acc.Name, dup.to)
	}

	if to == "" {
		to = from
	}
	hasWC := subjectHasWildcard(from)

	si := &serviceImport{dest, claim, se, nil, from, to, "", 0, rt, lat, nil, nil, hasWC, false, false, false, false}
	a.imports.services[from] = si
	a.mu.Unlock()

	if err := a.addServiceImportSub(si); err != nil {
		a.removeServiceImport(si.from)
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
	a.mu.Lock()
	c := a.internalClient()
	a.isid++
	sid := strconv.FormatUint(a.isid, 10)
	a.mu.Unlock()

	// This will happen in parsing when the account has not been properly setup.
	if c == nil {
		return nil, fmt.Errorf("no internal account client")
	}

	sub, err := c.processSub([]byte(subject+" "+sid), false)
	if err != nil {
		return nil, err
	}

	sub.icb = cb
	return sub, nil
}

// This will add an account subscription that matches the "from" from a service import entry.
func (a *Account) addServiceImportSub(si *serviceImport) error {
	a.mu.Lock()
	c := a.internalClient()
	sid := strconv.FormatUint(a.isid+1, 10)
	a.mu.Unlock()

	// This will happen in parsing when the account has not been properly setup.
	if c == nil {
		return nil
	}

	if si.sub != nil {
		return fmt.Errorf("duplicate call to create subscription for service import")
	}

	sub, err := c.processSub([]byte(si.from+" "+sid), true)
	if err != nil {
		return err
	}

	sub.icb = func(sub *subscription, c *client, subject, reply string, msg []byte) {
		c.processServiceImport(si, a, msg)
	}

	a.mu.Lock()
	a.isid++
	si.sub = sub
	a.mu.Unlock()

	return nil
}

// Remove all the subscriptions associated with service imports.
func (a *Account) removeAllServiceImportSubs() {
	a.mu.RLock()
	var sids [][]byte
	for _, si := range a.imports.services {
		if si.sub != nil && si.sub.sid != nil {
			sids = append(sids, si.sub.sid)
			si.sub = nil
		}
	}
	c := a.ic
	a.ic = nil
	a.mu.RUnlock()

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
	for _, si := range a.imports.services {
		a.addServiceImportSub(si)
	}
}

// Helper to determine when to sample.
func shouldSample(l *serviceLatency) bool {
	if l == nil || l.sampling <= 0 {
		return false
	}
	if l.sampling >= 100 {
		return true
	}
	return rand.Int31n(100) <= int32(l.sampling)
}

// Used to mimic client like replies.
const (
	replyPrefix    = "_R_."
	trackSuffix    = ".T"
	replyPrefixLen = len(replyPrefix)
	baseServerLen  = 10
	replyLen       = 6
	minReplyLen    = 15
	digits         = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	base           = 62
)

// This is where all service export responses are handled.
func (a *Account) processServiceImportResponse(sub *subscription, c *client, subject, reply string, msg []byte) {
	a.mu.RLock()
	if a.expired || len(a.exports.responses) == 0 {
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

// Will create a wildcard subscription to handle interest graph propagation for all
// service replies.
// Lock should not be held.
func (a *Account) createRespWildcard() []byte {
	a.mu.Lock()
	if a.prand == nil {
		a.prand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	var b = [baseServerLen]byte{'_', 'R', '_', '.'}
	rn := a.prand.Int63()
	for i, l := replyPrefixLen, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	a.siReply = append(b[:], '.')
	pre := a.siReply
	wcsub := append(a.siReply, '>')
	c := a.internalClient()
	a.isid += 1
	sid := strconv.FormatUint(a.isid, 10)
	a.mu.Unlock()

	// Create subscription and internal callback for all the wildcard response subjects.
	if sub, _ := c.processSub([]byte(string(wcsub)+" "+sid), false); sub != nil {
		sub.icb = a.processServiceImportResponse
	}

	return pre
}

// Test whether this is a tracked reply.
func isTrackedReply(reply []byte) bool {
	lreply := len(reply) - 1
	return lreply > 3 && reply[lreply-1] == '.' && reply[lreply] == 'T'
}

// Generate a new service reply from the wildcard prefix.
// FIXME(dlc) - probably do not have to use rand here. about 25ns per.
func (a *Account) newServiceReply(tracking bool) []byte {
	a.mu.RLock()
	replyPre := a.siReply
	s := a.srv
	a.mu.RUnlock()

	if replyPre == nil {
		replyPre = a.createRespWildcard()
	}

	var b [replyLen]byte
	rn := a.prand.Int63()
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	// Make sure to copy.
	reply := make([]byte, 0, len(replyPre)+len(b))
	reply = append(reply, replyPre...)
	reply = append(reply, b[:]...)

	if tracking && s.sys != nil {
		// Add in our tracking identifier. This allows the metrics to get back to only
		// this server without needless SUBS/UNSUBS.
		reply = append(reply, '.')
		reply = append(reply, s.sys.shash...)
		reply = append(reply, '.', 'T')
	}
	return reply
}

// Checks if a serviceImport was created to map responses.
func (si *serviceImport) isRespServiceImport() bool {
	return si != nil && si.response
}

// Sets the response theshold timer for a service export.
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
		acc.mu.Lock()
		se.clearResponseThresholdTimer()
		acc.mu.Unlock()
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
	defer a.mu.Unlock()
	if a.isClaimAccount() {
		return fmt.Errorf("claim based accounts can not be updated directly")
	}
	lrt := a.lowestServiceExportResponseTime()
	se := a.getServiceExport(export)
	if se == nil {
		return fmt.Errorf("no export defined for %q", export)
	}
	se.respThresh = maxTime
	if nlrt := a.lowestServiceExportResponseTime(); nlrt != lrt {
		a.updateAllClientsServiceExportResponseTime(nlrt)
	}
	return nil
}

// This is for internal service import responses.
func (a *Account) addRespServiceImport(dest *Account, to string, osi *serviceImport) *serviceImport {
	tracking := shouldSample(osi.latency)
	nrr := string(osi.acc.newServiceReply(tracking))

	a.mu.Lock()
	rt := osi.rt

	// dest is the requestor's account. a is the service responder with the export.
	// Marked as internal here, that is how we distinguish.
	si := &serviceImport{dest, nil, osi.se, nil, nrr, to, osi.to, 0, rt, nil, nil, nil, false, true, false, false, osi.share}

	if a.exports.responses == nil {
		a.exports.responses = make(map[string]*serviceImport)
	}
	a.exports.responses[nrr] = si

	// Always grab time and make sure response threshold timer is running.
	si.ts = time.Now().UnixNano()
	osi.se.setResponseThresholdTimer()

	if rt == Singleton && tracking {
		si.latency = osi.latency
		si.tracking = true
	}
	a.mu.Unlock()

	// We do not do individual subscriptions here like we do on configured imports.
	// We have an internal callback for all responses inbound to this account and
	// will process appropriately there. This does not pollute the sublist and the caches.

	// We do add in the reverse map such that we can detect loss of interest and do proper
	// cleanup of this si as interest goes away.
	dest.addReverseRespMapEntry(a, to, nrr)

	return si
}

// AddStreamImportWithClaim will add in the stream import from a specific account with optional token.
func (a *Account) AddStreamImportWithClaim(account *Account, from, prefix string, imClaim *jwt.Import) error {
	if account == nil {
		return ErrMissingAccount
	}

	// First check to see if the account has authorized export of the subject.
	if !account.checkStreamImportAuthorized(a, from, imClaim) {
		return ErrStreamImportAuthorization
	}

	// Check prefix if it exists and make sure its a literal.
	// Append token separator if not already present.
	if prefix != "" {
		// Make sure there are no wildcards here, this prefix needs to be a literal
		// since it will be prepended to a publish subject.
		if !subjectIsLiteral(prefix) {
			return ErrStreamImportBadPrefix
		}
		if prefix[len(prefix)-1] != btsep {
			prefix = prefix + string(btsep)
		}
	}
	a.mu.Lock()
	if a.isStreamImportDuplicate(account, from) {
		a.mu.Unlock()
		return ErrStreamImportDuplicate
	}
	a.imports.streams = append(a.imports.streams, &streamImport{account, from, prefix, imClaim, false})
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
	return a.AddStreamImportWithClaim(account, from, prefix, nil)
}

// IsPublicExport is a placeholder to denote a public export.
var IsPublicExport = []*Account(nil)

// AddStreamExport will add an export to the account. If accounts is nil
// it will signify a public export, meaning anyone can impoort.
func (a *Account) AddStreamExport(subject string, accounts []*Account) error {
	if a == nil {
		return ErrMissingAccount
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.exports.streams == nil {
		a.exports.streams = make(map[string]*streamExport)
	}
	ea := a.exports.streams[subject]
	if accounts != nil {
		if ea == nil {
			ea = &streamExport{}
		}
		// empty means auth required but will be import token.
		if len(accounts) == 0 {
			ea.tokenReq = true
		} else {
			if ea.approved == nil {
				ea.approved = make(map[string]*Account, len(accounts))
			}
			for _, acc := range accounts {
				ea.approved[acc.Name] = acc
			}
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

func (a *Account) checkAuth(ea *exportAuth, account *Account, imClaim *jwt.Import) bool {
	// if ea is nil or ea.approved is nil, that denotes a public export
	if ea == nil || (ea.approved == nil && !ea.tokenReq) {
		return true
	}
	// Check if token required
	if ea.tokenReq {
		return a.checkActivation(account, imClaim, true)
	}
	// If we have a matching account we are authorized
	_, ok := ea.approved[account.Name]
	return ok
}

func (a *Account) checkStreamExportApproved(account *Account, subject string, imClaim *jwt.Import) bool {
	// Check direct match of subject first
	ea, ok := a.exports.streams[subject]
	if ok {
		if ea == nil {
			return true
		}
		return a.checkAuth(&ea.exportAuth, account, imClaim)
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
			return a.checkAuth(&ea.exportAuth, account, imClaim)
		}
	}
	return false
}

func (a *Account) checkServiceExportApproved(account *Account, subject string, imClaim *jwt.Import) bool {
	// Check direct match of subject first
	se, ok := a.exports.services[subject]
	if ok {
		// if se is nil or eq.approved is nil, that denotes a public export
		if se == nil || (se.approved == nil && !se.tokenReq) {
			return true
		}
		// Check if token required
		if se.tokenReq {
			return a.checkActivation(account, imClaim, true)
		}
		// If we have a matching account we are authorized
		_, ok := se.approved[account.Name]
		return ok
	}
	// ok if we are here we did not match directly so we need to test each one.
	// The import subject arg has to take precedence, meaning the export
	// has to be a true subset of the import claim. We already checked for
	// exact matches above.
	tokens := strings.Split(subject, tsep)
	for subj, se := range a.exports.services {
		if isSubsetMatch(tokens, subj) {
			if se == nil || (se.approved == nil && !se.tokenReq) {
				return true
			}
			// Check if token required
			if se.tokenReq {
				return a.checkActivation(account, imClaim, true)
			}
			_, ok := se.approved[account.Name]
			return ok
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

// Will fetch the activation token for an import.
func fetchActivation(url string) string {
	// FIXME(dlc) - Make configurable.
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(url)
	if err != nil || resp == nil {
		return ""
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	return string(body)
}

// These are import stream specific versions for when an activation expires.
func (a *Account) streamActivationExpired(exportAcc *Account, subject string) {
	a.mu.RLock()
	if a.expired || a.imports.streams == nil {
		a.mu.RUnlock()
		return
	}
	var si *streamImport
	for _, si = range a.imports.streams {
		if si.acc == exportAcc && si.from == subject {
			break
		}
	}

	if si == nil || si.invalid {
		a.mu.RUnlock()
		return
	}
	a.mu.RUnlock()

	if si.acc.checkActivation(a, si.claim, false) {
		// The token has been updated most likely and we are good to go.
		return
	}

	a.mu.Lock()
	si.invalid = true
	clients := make([]*client, 0, len(a.clients))
	for c := range a.clients {
		clients = append(clients, c)
	}
	awcsti := map[string]struct{}{a.Name: {}}
	a.mu.Unlock()
	for _, c := range clients {
		c.processSubsOnConfigReload(awcsti)
	}
}

// These are import service specific versions for when an activation expires.
func (a *Account) serviceActivationExpired(subject string) {
	a.mu.RLock()
	if a.expired || a.imports.services == nil {
		a.mu.RUnlock()
		return
	}
	si := a.imports.services[subject]
	if si == nil || si.invalid {
		a.mu.RUnlock()
		return
	}
	a.mu.RUnlock()

	if si.acc.checkActivation(a, si.claim, false) {
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
		a.serviceActivationExpired(subject)
	}
}

// checkActivation will check the activation token for validity.
func (a *Account) checkActivation(importAcc *Account, claim *jwt.Import, expTimer bool) bool {
	if claim == nil || claim.Token == "" {
		return false
	}
	// Create a quick clone so we can inline Token JWT.
	clone := *claim

	// We grab the token from a URL by hand here since we need expiration etc.
	if url, err := url.Parse(clone.Token); err == nil && url.Scheme != "" {
		clone.Token = fetchActivation(url.String())
	}
	vr := jwt.CreateValidationResults()
	clone.Validate(a.Name, vr)
	if vr.IsBlocking(true) {
		return false
	}
	act, err := jwt.DecodeActivationClaims(clone.Token)
	if err != nil {
		return false
	}
	vr = jwt.CreateValidationResults()
	act.Validate(vr)
	if vr.IsBlocking(true) {
		return false
	}
	if !a.isIssuerClaimTrusted(act) {
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
	// Check for token revocation..
	if a.actsRevoked != nil {
		if t, ok := a.actsRevoked[act.Subject]; ok && t <= time.Now().Unix() {
			return false
		}
	}

	return true
}

// Returns true if the activation claim is trusted. That is the issuer matches
// the account or is an entry in the signing keys.
func (a *Account) isIssuerClaimTrusted(claims *jwt.ActivationClaims) bool {
	// if no issuer account, issuer is the account
	if claims.IssuerAccount == "" {
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
	return a.hasIssuerNoLock(claims.Issuer)
}

// Returns true if `a` and `b` stream imports are the same. Note that the
// check is done with the account's name, not the pointer. This is used
// during config reload where we are comparing current and new config
// in which pointers are different.
// No lock is acquired in this function, so it is assumed that the
// import maps are not changed while this executes.
func (a *Account) checkStreamImportsEqual(b *Account) bool {
	if len(a.imports.streams) != len(b.imports.streams) {
		return false
	}
	// Load the b imports into a map index by what we are looking for.
	bm := make(map[string]*streamImport, len(b.imports.streams))
	for _, bim := range b.imports.streams {
		bm[bim.acc.Name+bim.from+bim.prefix] = bim
	}
	for _, aim := range a.imports.streams {
		if _, ok := bm[aim.acc.Name+aim.from+aim.prefix]; !ok {
			return false
		}
	}
	return true
}

func (a *Account) checkStreamExportsEqual(b *Account) bool {
	if len(a.exports.streams) != len(b.exports.streams) {
		return false
	}
	for subj, aea := range a.exports.streams {
		bea, ok := b.exports.streams[subj]
		if !ok {
			return false
		}
		if !reflect.DeepEqual(aea, bea) {
			return false
		}
	}
	return true
}

func (a *Account) checkServiceExportsEqual(b *Account) bool {
	if len(a.exports.services) != len(b.exports.services) {
		return false
	}
	for subj, aea := range a.exports.services {
		bea, ok := b.exports.services[subj]
		if !ok {
			return false
		}
		if !reflect.DeepEqual(aea, bea) {
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
	a.mu.RLock()
	exp := a.expired
	a.mu.RUnlock()
	return exp
}

// Called when an account has expired.
func (a *Account) expiredTimeout() {
	// Mark expired first.
	a.mu.Lock()
	a.expired = true
	a.mu.Unlock()

	// Collect the clients and expire them.
	cs := make([]*client, 0, len(a.clients))
	a.mu.RLock()
	for c := range a.clients {
		cs = append(cs, c)
	}
	a.mu.RUnlock()

	for _, c := range cs {
		c.accountAuthExpired()
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
func (a *Account) checkUserRevoked(nkey string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.usersRevoked == nil {
		return false
	}
	if t, ok := a.usersRevoked[nkey]; !ok || t > time.Now().Unix() {
		return false
	}
	return true
}

// Check expiration and set the proper state as needed.
func (a *Account) checkExpiration(claims *jwt.ClaimsData) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.clearExpirationTimer()
	if claims.Expires == 0 {
		a.expired = false
		return
	}
	tn := time.Now().Unix()
	if claims.Expires <= tn {
		a.expired = true
		return
	}
	expiresAt := time.Duration(claims.Expires - tn)
	a.setExpirationTimer(expiresAt * time.Second)
	a.expired = false
}

// hasIssuer returns true if the issuer matches the account
// issuer or it is a signing key for the account.
func (a *Account) hasIssuer(issuer string) bool {
	a.mu.RLock()
	hi := a.hasIssuerNoLock(issuer)
	a.mu.RUnlock()
	return hi
}

// hasIssuerNoLock is the unlocked version of hasIssuer
func (a *Account) hasIssuerNoLock(issuer string) bool {
	// same issuer
	if a.Issuer == issuer {
		return true
	}
	for i := 0; i < len(a.signingKeys); i++ {
		if a.signingKeys[i] == issuer {
			return true
		}
	}
	return false
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
	s.mu.Lock()
	ar := s.accResolver
	s.mu.Unlock()
	return ar
}

// isClaimAccount returns if this account is backed by a JWT claim.
// Lock should be held.
func (a *Account) isClaimAccount() bool {
	return a.claimJWT != ""
}

// updateAccountClaims will update an existing account with new claims.
// This will replace any exports or imports previously defined.
// Lock MUST NOT be held upon entry.
func (s *Server) UpdateAccountClaims(a *Account, ac *jwt.AccountClaims) {
	if a == nil {
		return
	}
	s.Debugf("Updating account claims: %s", a.Name)
	a.checkExpiration(ac.Claims())

	a.mu.Lock()
	// Clone to update, only select certain fields.
	old := &Account{Name: a.Name, exports: a.exports, limits: a.limits, signingKeys: a.signingKeys}

	// Reset exports and imports here.

	// Exports is creating a whole new map.
	a.exports = exportMap{}

	// Imports are checked unlocked in processInbound, so we can't change out the struct here. Need to process inline.
	if a.imports.streams != nil {
		old.imports.streams = a.imports.streams
		a.imports.streams = nil
	}
	if a.imports.services != nil {
		old.imports.services = make(map[string]*serviceImport, len(a.imports.services))
	}
	for k, v := range a.imports.services {
		old.imports.services[k] = v
		delete(a.imports.services, k)
	}

	// Reset any notion of export revocations.
	a.actsRevoked = nil

	// update account signing keys
	a.signingKeys = nil
	signersChanged := false
	if len(ac.SigningKeys) > 0 {
		// insure copy the new keys and sort
		a.signingKeys = append(a.signingKeys, ac.SigningKeys...)
		sort.Strings(a.signingKeys)
	}
	if len(a.signingKeys) != len(old.signingKeys) {
		signersChanged = true
	} else {
		for i := 0; i < len(old.signingKeys); i++ {
			if a.signingKeys[i] != old.signingKeys[i] {
				signersChanged = true
				break
			}
		}
	}
	a.mu.Unlock()

	gatherClients := func() []*client {
		a.mu.RLock()
		clients := make([]*client, 0, len(a.clients))
		for c := range a.clients {
			clients = append(clients, c)
		}
		a.mu.RUnlock()
		return clients
	}

	for _, e := range ac.Exports {
		switch e.Type {
		case jwt.Stream:
			s.Debugf("Adding stream export %q for %s", e.Subject, a.Name)
			if err := a.AddStreamExport(string(e.Subject), authAccounts(e.TokenReq)); err != nil {
				s.Debugf("Error adding stream export to account [%s]: %v", a.Name, err.Error())
			}
		case jwt.Service:
			s.Debugf("Adding service export %q for %s", e.Subject, a.Name)
			rt := Singleton
			switch e.ResponseType {
			case jwt.ResponseTypeStream:
				rt = Streamed
			case jwt.ResponseTypeChunked:
				rt = Chunked
			}
			if err := a.AddServiceExportWithResponse(string(e.Subject), rt, authAccounts(e.TokenReq)); err != nil {
				s.Debugf("Error adding service export to account [%s]: %v", a.Name, err)
			}
			if e.Latency != nil {
				if err := a.TrackServiceExportWithSampling(string(e.Subject), string(e.Latency.Results), e.Latency.Sampling); err != nil {
					s.Debugf("Error adding latency tracking for service export to account [%s]: %v", a.Name, err)
				}
			}
		}
		// We will track these at the account level. Should not have any collisions.
		if e.Revocations != nil {
			a.mu.Lock()
			if a.actsRevoked == nil {
				a.actsRevoked = make(map[string]int64)
			}
			for k, t := range e.Revocations {
				a.actsRevoked[k] = t
			}
			a.mu.Unlock()
		}
	}
	for _, i := range ac.Imports {
		acc, err := s.lookupAccount(i.Account)
		if acc == nil || err != nil {
			s.Errorf("Can't locate account [%s] for import of [%v] %s (err=%v)", i.Account, i.Subject, i.Type, err)
			continue
		}
		switch i.Type {
		case jwt.Stream:
			s.Debugf("Adding stream import %s:%q for %s:%q", acc.Name, i.Subject, a.Name, i.To)
			if err := a.AddStreamImportWithClaim(acc, string(i.Subject), string(i.To), i); err != nil {
				s.Debugf("Error adding stream import to account [%s]: %v", a.Name, err.Error())
			}
		case jwt.Service:
			// FIXME(dlc) - need to add in respThresh here eventually.
			s.Debugf("Adding service import %s:%q for %s:%q", acc.Name, i.Subject, a.Name, i.To)
			if err := a.AddServiceImportWithClaim(acc, string(i.Subject), string(i.To), i); err != nil {
				s.Debugf("Error adding service import to account [%s]: %v", a.Name, err.Error())
			}
		}
	}
	// Now let's apply any needed changes from import/export changes.
	if !a.checkStreamImportsEqual(old) {
		awcsti := map[string]struct{}{a.Name: {}}
		for _, c := range gatherClients() {
			c.processSubsOnConfigReload(awcsti)
		}
	}
	// Now check if stream exports have changed.
	if !a.checkStreamExportsEqual(old) || signersChanged {
		clients := map[*client]struct{}{}
		// We need to check all accounts that have an import claim from this account.
		awcsti := map[string]struct{}{}
		s.accounts.Range(func(k, v interface{}) bool {
			acc := v.(*Account)
			// Move to the next if this account is actually account "a".
			if acc.Name == a.Name {
				return true
			}
			// TODO: checkStreamImportAuthorized() stack should not be trying
			// to lock "acc". If we find that to be needed, we will need to
			// rework this to ensure we don't lock acc.
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
		// Now walk clients.
		for c := range clients {
			c.processSubsOnConfigReload(awcsti)
		}
	}
	// Now check if service exports have changed.
	if !a.checkServiceExportsEqual(old) || signersChanged {
		s.accounts.Range(func(k, v interface{}) bool {
			acc := v.(*Account)
			// Move to the next if this account is actually account "a".
			if acc.Name == a.Name {
				return true
			}
			// TODO: checkServiceImportAuthorized() stack should not be trying
			// to lock "acc". If we find that to be needed, we will need to
			// rework this to ensure we don't lock acc.
			acc.mu.Lock()
			for _, si := range acc.imports.services {
				if si != nil && si.acc.Name == a.Name {
					// Check for if we are still authorized for an import.
					si.invalid = !a.checkServiceImportAuthorized(acc, si.to, si.claim)
					if si.latency != nil && !si.response {
						// Make sure we should still be tracking latency.
						if se := a.getServiceExport(si.to); se != nil {
							si.latency = se.latency
						}
					}
				}
			}
			acc.mu.Unlock()
			return true
		})
	}

	// Now make sure we shutdown the old service import subscriptions.
	var sids [][]byte
	a.mu.RLock()
	c := a.ic
	for _, si := range old.imports.services {
		if c != nil && si.sub != nil && si.sub.sid != nil {
			sids = append(sids, si.sub.sid)
		}
	}
	a.mu.RUnlock()
	for _, sid := range sids {
		c.processUnsub(sid)
	}

	// Now do limits if they are present.
	a.mu.Lock()
	a.msubs = int32(ac.Limits.Subs)
	a.mpay = int32(ac.Limits.Payload)
	a.mconns = int32(ac.Limits.Conn)
	a.mleafs = int32(ac.Limits.LeafNodeConn)
	// Check for any revocations
	if len(ac.Revocations) > 0 {
		// We will always replace whatever we had with most current, so no
		// need to look at what we have.
		a.usersRevoked = make(map[string]int64, len(ac.Revocations))
		for pk, t := range ac.Revocations {
			a.usersRevoked[pk] = t
		}
	}
	a.defaultPerms = buildPermissionsFromJwt(&ac.DefaultPermissions)
	a.mu.Unlock()

	clients := gatherClients()
	// Sort if we are over the limit.
	if a.MaxTotalConnectionsReached() {
		sort.Slice(clients, func(i, j int) bool {
			return clients[i].start.After(clients[j].start)
		})
	}
	now := time.Now().Unix()
	for i, c := range clients {
		a.mu.RLock()
		exceeded := a.mconns != jwt.NoLimit && i >= int(a.mconns)
		a.mu.RUnlock()
		if exceeded {
			c.maxAccountConnExceeded()
			continue
		}
		c.mu.Lock()
		c.applyAccountLimits()
		// Check for being revoked here. We use ac one to avoid
		// the account lock.
		var nkey string
		if c.user != nil {
			nkey = c.user.Nkey
		}
		c.mu.Unlock()

		// Check if we have been revoked.
		if ac.Revocations != nil {
			if t, ok := ac.Revocations[nkey]; ok && now >= t {
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
			sk := c.user.SigningKey
			c.mu.Unlock()
			if sk != "" && !a.hasIssuer(sk) {
				c.closeConnection(AuthenticationViolation)
			}
		}
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
	s.tmpAccounts.Store(ac.Subject, acc)
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
		if p == nil {
			p = &Permissions{}
		}
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
func buildInternalNkeyUser(uc *jwt.UserClaims, acc *Account) *NkeyUser {
	nu := &NkeyUser{Nkey: uc.Subject, Account: acc}
	if uc.IssuerAccount != "" {
		nu.SigningKey = uc.Issuer
	}

	// Now check for permissions.
	var p = buildPermissionsFromJwt(&uc.Permissions)
	if p == nil && acc.defaultPerms != nil {
		p = acc.defaultPerms.clone()
	}
	nu.Permissions = p
	return nu
}

// AccountResolver interface. This is to fetch Account JWTs by public nkeys
type AccountResolver interface {
	Fetch(name string) (string, error)
	Store(name, jwt string) error
}

// MemAccResolver is a memory only resolver.
// Mostly for testing.
type MemAccResolver struct {
	sm sync.Map
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

// URLAccResolver implements an http fetcher.
type URLAccResolver struct {
	url string
	c   *http.Client
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
		c:   &http.Client{Timeout: 2 * time.Second, Transport: tr},
	}
	return ur, nil
}

// Fetch will fetch the account jwt claims from the base url, appending the
// account name onto the end.
func (ur *URLAccResolver) Fetch(name string) (string, error) {
	url := ur.url + name
	resp, err := ur.c.Get(url)
	if err != nil {
		return _EMPTY_, fmt.Errorf("could not fetch <%q>: %v", url, err)
	} else if resp == nil {
		return _EMPTY_, fmt.Errorf("could not fetch <%q>: no response", url)
	} else if resp.StatusCode != http.StatusOK {
		return _EMPTY_, fmt.Errorf("could not fetch <%q>: %v", url, resp.Status)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return _EMPTY_, err
	}
	return string(body), nil
}

// Store is not implemented for URL Resolver.
func (ur *URLAccResolver) Store(name, jwt string) error {
	return fmt.Errorf("Store operation not supported for URL Resolver")
}
