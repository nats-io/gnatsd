// Copyright 2013-2026 The NATS Authors
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
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"maps"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server/pse"
)

// Connz represents detailed information on current client connections.
type Connz struct {
	ID       string      `json:"server_id"`
	Now      time.Time   `json:"now"`
	NumConns int         `json:"num_connections"`
	Total    int         `json:"total"`
	Offset   int         `json:"offset"`
	Limit    int         `json:"limit"`
	Conns    []*ConnInfo `json:"connections"`
}

// ConnzOptions are the options passed to Connz()
type ConnzOptions struct {
	// Sort indicates how the results will be sorted. Check SortOpt for possible values.
	// Only the sort by connection ID (ByCid) is ascending, all others are descending.
	Sort SortOpt `json:"sort"`

	// Username indicates if user names should be included in the results.
	Username bool `json:"auth"`

	// Subscriptions indicates if subscriptions should be included in the results.
	Subscriptions bool `json:"subscriptions"`

	// SubscriptionsDetail indicates if subscription details should be included in the results
	SubscriptionsDetail bool `json:"subscriptions_detail"`

	// Offset is used for pagination. Connz() only returns connections starting at this
	// offset from the global results.
	Offset int `json:"offset"`

	// Limit is the maximum number of connections that should be returned by Connz().
	Limit int `json:"limit"`

	// Filter for this explicit client connection.
	CID uint64 `json:"cid"`

	// Filter for this explicit client connection based on the MQTT client ID
	MQTTClient string `json:"mqtt_client"`

	// Filter by connection state.
	State ConnState `json:"state"`

	// The below options only apply if auth is true.

	// Filter by username.
	User string `json:"user"`

	// Filter by account.
	Account string `json:"acc"`

	// Filter by subject interest
	FilterSubject string `json:"filter_subject"`
}

// ConnState is for filtering states of connections. We will only have two, open and closed.
type ConnState int

const (
	// ConnOpen filters on open clients.
	ConnOpen = ConnState(iota)
	// ConnClosed filters on closed clients.
	ConnClosed
	// ConnAll returns all clients.
	ConnAll
)

// ConnInfo has detailed information on a per connection basis.
type ConnInfo struct {
	Cid            uint64         `json:"cid"`
	Kind           string         `json:"kind,omitempty"`
	Type           string         `json:"type,omitempty"`
	IP             string         `json:"ip"`
	Port           int            `json:"port"`
	Start          time.Time      `json:"start"`
	LastActivity   time.Time      `json:"last_activity"`
	Stop           *time.Time     `json:"stop,omitempty"`
	Reason         string         `json:"reason,omitempty"`
	RTT            string         `json:"rtt,omitempty"`
	Uptime         string         `json:"uptime"`
	Idle           string         `json:"idle"`
	Pending        int            `json:"pending_bytes"`
	InMsgs         int64          `json:"in_msgs"`
	OutMsgs        int64          `json:"out_msgs"`
	InBytes        int64          `json:"in_bytes"`
	OutBytes       int64          `json:"out_bytes"`
	Stalls         int64          `json:"stalls,omitempty"`
	NumSubs        uint32         `json:"subscriptions"`
	Name           string         `json:"name,omitempty"`
	Lang           string         `json:"lang,omitempty"`
	Version        string         `json:"version,omitempty"`
	TLSVersion     string         `json:"tls_version,omitempty"`
	TLSCipher      string         `json:"tls_cipher_suite,omitempty"`
	TLSPeerCerts   []*TLSPeerCert `json:"tls_peer_certs,omitempty"`
	TLSFirst       bool           `json:"tls_first,omitempty"`
	AuthorizedUser string         `json:"authorized_user,omitempty"`
	Account        string         `json:"account,omitempty"`
	Subs           []string       `json:"subscriptions_list,omitempty"`
	SubsDetail     []SubDetail    `json:"subscriptions_list_detail,omitempty"`
	JWT            string         `json:"jwt,omitempty"`
	IssuerKey      string         `json:"issuer_key,omitempty"`
	NameTag        string         `json:"name_tag,omitempty"`
	Tags           jwt.TagList    `json:"tags,omitempty"`
	MQTTClient     string         `json:"mqtt_client,omitempty"` // This is the MQTT client id
	Proxy          *ProxyInfo     `json:"proxy,omitempty"`

	// Internal
	rtt int64 // For fast sorting
}

// ProxyInfo represents the information about this proxied connection.
type ProxyInfo struct {
	Key string `json:"key"`
}

// TLSPeerCert contains basic information about a TLS peer certificate
type TLSPeerCert struct {
	Subject          string `json:"subject,omitempty"`
	SubjectPKISha256 string `json:"spki_sha256,omitempty"`
	CertSha256       string `json:"cert_sha256,omitempty"`
}

// DefaultConnListSize is the default size of the connection list.
const DefaultConnListSize = 1024

// DefaultSubListSize is the default size of the subscriptions list.
const DefaultSubListSize = 1024

const defaultStackBufSize = 10000

func newSubsDetailList(client *client) []SubDetail {
	subsDetail := make([]SubDetail, 0, len(client.subs))
	for _, sub := range client.subs {
		subsDetail = append(subsDetail, newClientSubDetail(sub))
	}
	return subsDetail
}

func newSubsList(client *client) []string {
	subs := make([]string, 0, len(client.subs))
	for _, sub := range client.subs {
		subs = append(subs, string(sub.subject))
	}
	return subs
}

func redactBearerJWT(userJWT string) string {
	if userJWT == _EMPTY_ {
		return _EMPTY_
	}
	uc, err := jwt.DecodeUserClaims(userJWT)
	if err == nil && uc != nil && uc.BearerToken {
		return _EMPTY_
	}
	return userJWT
}

// Connz returns a Connz struct containing information about connections.
func (s *Server) Connz(opts *ConnzOptions) (*Connz, error) {
	var (
		sortOpt = ByCid
		auth    bool
		subs    bool
		subsDet bool
		offset  int
		limit   = DefaultConnListSize
		cid     = uint64(0)
		state   = ConnOpen
		user    string
		acc     string
		a       *Account
		filter  string
		mqttCID string
	)

	if opts != nil {
		// If no sort option given or sort is by uptime, then sort by cid
		if opts.Sort != _EMPTY_ {
			sortOpt = opts.Sort
			if !sortOpt.IsValid() {
				return nil, fmt.Errorf("invalid sorting option: %s", sortOpt)
			}
		}

		// Auth specifics.
		auth = opts.Username
		user = opts.User
		acc = opts.Account
		mqttCID = opts.MQTTClient

		subs = opts.Subscriptions
		subsDet = opts.SubscriptionsDetail
		offset = opts.Offset
		if offset < 0 {
			offset = 0
		}
		limit = opts.Limit
		if limit <= 0 {
			limit = DefaultConnListSize
		}
		// state
		state = opts.State

		// ByStop only makes sense on closed connections
		if sortOpt == ByStop && state != ConnClosed {
			return nil, fmt.Errorf("sort by stop only valid on closed connections")
		}
		// ByReason is the same.
		if sortOpt == ByReason && state != ConnClosed {
			return nil, fmt.Errorf("sort by reason only valid on closed connections")
		}
		// If searching by CID
		if opts.CID > 0 {
			cid = opts.CID
			limit = 1
		}
		// If filtering by subject.
		if opts.FilterSubject != _EMPTY_ && opts.FilterSubject != fwcs {
			if acc == _EMPTY_ {
				return nil, fmt.Errorf("filter by subject only valid with account filtering")
			}
			filter = opts.FilterSubject
		}
	}

	c := &Connz{
		Offset: offset,
		Limit:  limit,
		Now:    time.Now().UTC(),
	}

	// Open clients
	var openClients []*client
	// Selected client
	var cidClient *client
	// Hold for closed clients if requested.
	var closedClients []*closedClient

	var clist map[uint64]*client

	if acc != _EMPTY_ {
		var err error
		a, err = s.lookupAccount(acc)
		if err != nil {
			return c, nil
		}
		a.mu.RLock()
		clist = make(map[uint64]*client, a.numLocalConnections())
		for c := range a.clients {
			if c.kind == CLIENT || c.kind == LEAF {
				clist[c.cid] = c
			}
		}
		a.mu.RUnlock()
	}

	// Snapshot server connection state.
	s.mu.RLock()
	// Default to all client unless filled in above.
	if clist == nil {
		clist = make(map[uint64]*client, len(s.clients)+len(s.leafs))
		maps.Copy(clist, s.clients)
		maps.Copy(clist, s.leafs)
	}

	// copy the server id for monitoring
	c.ID = s.info.ID
	// select client by CID
	if cid > 0 {
		cidClient = clist[cid]
	}
	s.mu.RUnlock()

	// Number of total clients. The resulting ConnInfo array
	// may be smaller if pagination is used.
	switch state {
	case ConnOpen:
		c.Total = len(clist)
	case ConnClosed:
		closedClients = s.closed.closedClients()
		c.Total = len(closedClients)
	case ConnAll:
		c.Total = len(clist)
		closedClients = s.closed.closedClients()
		c.Total += len(closedClients)
	}

	// We may need to filter these connections.
	if acc != _EMPTY_ && len(closedClients) > 0 {
		var ccc []*closedClient
		for _, cc := range closedClients {
			if cc.acc != acc {
				continue
			}
			ccc = append(ccc, cc)
		}
		c.Total -= (len(closedClients) - len(ccc))
		closedClients = ccc
	}

	totalClients := c.Total
	if cid > 0 { // Meaning we only want 1.
		totalClients = 1
	}
	if state == ConnOpen || state == ConnAll {
		openClients = make([]*client, 0, totalClients)
	}

	// Data structures for results.
	var conns []ConnInfo // Limits allocs for actual ConnInfos.
	var pconns ConnInfos

	switch state {
	case ConnOpen:
		conns = make([]ConnInfo, totalClients)
		pconns = make(ConnInfos, totalClients)
	case ConnClosed:
		pconns = make(ConnInfos, totalClients)
	case ConnAll:
		conns = make([]ConnInfo, cap(openClients))
		pconns = make(ConnInfos, totalClients)
	}

	// Search by individual CID.
	if cid > 0 {
		// Let's first check if user also selects on ConnOpen or ConnAll
		// and look for opened connections.
		if state == ConnOpen || state == ConnAll {
			if cidClient != nil {
				// The account filter was already applied by scoping clist,
				// but user and MQTT client ID filters still need to be applied.
				cidClient.mu.RLock()
				cAuthUser := cidClient.getRawAuthUser()
				cMQTTID := cidClient.getMQTTClientID()
				cidClient.mu.RUnlock()
				if (user == _EMPTY_ || cAuthUser == user) && (mqttCID == _EMPTY_ || cMQTTID == mqttCID) {
					openClients = append(openClients, cidClient)
					closedClients = nil
				}
			}
		}
		// If we did not find, and the user selected for ConnClosed or ConnAll,
		// look for closed connections.
		if len(openClients) == 0 && (state == ConnClosed || state == ConnAll) {
			copyClosed := closedClients
			closedClients = nil
			for _, cc := range copyClosed {
				if cc.Cid == cid {
					closedClients = []*closedClient{cc}
					break
				}
			}
		}
	} else {
		// Gather all open clients.
		if state == ConnOpen || state == ConnAll {
			for _, client := range clist {
				// Snapshot each under the client lock.
				client.mu.RLock()
				var cAccName string
				if client.acc != nil {
					cAccName = client.acc.Name
				}
				cAuthUser := client.getRawAuthUser()
				cMQTTID := client.getMQTTClientID()
				client.mu.RUnlock()

				// If we have an account specified we need to filter.
				if acc != _EMPTY_ && cAccName != acc {
					continue
				}
				// Do user filtering second
				if user != _EMPTY_ && cAuthUser != user {
					continue
				}
				// Do mqtt client ID filtering next
				if mqttCID != _EMPTY_ && cMQTTID != mqttCID {
					continue
				}
				openClients = append(openClients, client)
			}
		}
	}

	// Filter by subject now if needed. We do this outside of server lock.
	if filter != _EMPTY_ {
		var oc []*client
		for _, c := range openClients {
			c.mu.Lock()
			for _, sub := range c.subs {
				if SubjectsCollide(filter, string(sub.subject)) {
					oc = append(oc, c)
					break
				}
			}
			c.mu.Unlock()
			openClients = oc
		}
	}

	// Just return with empty array if nothing here.
	if len(openClients) == 0 && len(closedClients) == 0 {
		c.Conns = ConnInfos{}
		return c, nil
	}

	// Now whip through and generate ConnInfo entries
	// Open Clients
	i := 0
	for _, client := range openClients {
		client.mu.Lock()
		ci := &conns[i]
		ci.fill(client, client.nc, c.Now, auth)
		// Fill in subscription data if requested.
		if len(client.subs) > 0 {
			if subsDet {
				ci.SubsDetail = newSubsDetailList(client)
			} else if subs {
				ci.Subs = newSubsList(client)
			}
		}
		// Fill in user if auth requested.
		if auth {
			ci.AuthorizedUser = client.getRawAuthUser()
			if name := client.acc.GetName(); name != globalAccountName {
				ci.Account = name
			}
			ci.JWT = client.opts.JWT
			ci.IssuerKey = issuerForClient(client)
			ci.Tags = client.tags
			ci.NameTag = client.acc.getNameTag()
		}
		client.mu.Unlock()
		ci.JWT = redactBearerJWT(ci.JWT)
		pconns[i] = ci
		i++
	}
	// Closed Clients
	var needCopy bool
	if subs || subsDet || auth {
		needCopy = true
	}
	for _, cc := range closedClients {
		// If we have an account specified we need to filter.
		if acc != _EMPTY_ && cc.acc != acc {
			continue
		}
		// Do user filtering second
		if user != _EMPTY_ && cc.user != user {
			continue
		}
		// Do mqtt client ID filtering next
		if mqttCID != _EMPTY_ && cc.MQTTClient != mqttCID {
			continue
		}
		// Copy if needed for any changes to the ConnInfo
		if needCopy {
			cx := *cc
			cc = &cx
		}
		// Fill in subscription data if requested.
		if len(cc.subs) > 0 {
			if subsDet {
				cc.SubsDetail = cc.subs
			} else if subs {
				cc.Subs = make([]string, 0, len(cc.subs))
				for _, sub := range cc.subs {
					cc.Subs = append(cc.Subs, sub.Subject)
				}
			}
		}
		// Fill in user if auth requested.
		if auth {
			cc.AuthorizedUser = cc.user
			if cc.acc != _EMPTY_ && (cc.acc != globalAccountName) {
				cc.Account = cc.acc
				if acc, err := s.LookupAccount(cc.acc); err == nil {
					cc.NameTag = acc.getNameTag()
				}
			}
			cc.JWT = redactBearerJWT(cc.JWT)
		}
		pconns[i] = &cc.ConnInfo
		i++
	}

	// This will trip if we have filtered out client connections.
	if len(pconns) != i {
		pconns = pconns[:i]
		totalClients = i
	}

	switch sortOpt {
	case ByCid, ByStart:
		sort.Sort(SortByCid{pconns})
	case BySubs:
		sort.Sort(sort.Reverse(SortBySubs{pconns}))
	case ByPending:
		sort.Sort(sort.Reverse(SortByPending{pconns}))
	case ByOutMsgs:
		sort.Sort(sort.Reverse(SortByOutMsgs{pconns}))
	case ByInMsgs:
		sort.Sort(sort.Reverse(SortByInMsgs{pconns}))
	case ByOutBytes:
		sort.Sort(sort.Reverse(SortByOutBytes{pconns}))
	case ByInBytes:
		sort.Sort(sort.Reverse(SortByInBytes{pconns}))
	case ByLast:
		sort.Sort(sort.Reverse(SortByLast{pconns}))
	case ByIdle:
		sort.Sort(sort.Reverse(SortByIdle{pconns, c.Now}))
	case ByUptime:
		sort.Sort(SortByUptime{pconns, time.Now()})
	case ByStop:
		sort.Sort(sort.Reverse(SortByStop{pconns}))
	case ByReason:
		sort.Sort(SortByReason{pconns})
	case ByRTT:
		sort.Sort(sort.Reverse(SortByRTT{pconns}))
	}

	// Make sure these are sane.
	maxIndex := totalClients
	minoff := min(max(c.Offset, 0), maxIndex)
	maxoff := minoff + min(c.Limit, maxIndex-minoff)

	// Now pare down to the requested size.
	// TODO(dlc) - for very large number of connections we
	// could save the whole list in a hash, send hash on first
	// request and allow users to use has for subsequent pages.
	// Low TTL, say < 1sec.
	c.Conns = pconns[minoff:maxoff]
	c.NumConns = len(c.Conns)

	return c, nil
}

// Fills in the ConnInfo from the client.
// client should be locked.
func (ci *ConnInfo) fill(client *client, nc net.Conn, now time.Time, auth bool) {
	// For fast sort if required.
	rtt := client.getRTT()
	ci.rtt = int64(rtt)

	ci.Cid = client.cid
	ci.MQTTClient = client.getMQTTClientID()
	ci.Kind = client.kindString()
	ci.Type = client.clientTypeString()
	ci.Start = client.start
	ci.LastActivity = client.last
	ci.Uptime = myUptime(now.Sub(client.start))
	ci.Idle = myUptime(now.Sub(client.last))
	ci.RTT = rtt.String()
	ci.OutMsgs = client.outMsgs
	ci.OutBytes = client.outBytes
	ci.NumSubs = uint32(len(client.subs))
	ci.Pending = int(client.out.pb)
	ci.Name = client.opts.Name
	ci.Lang = client.opts.Lang
	ci.Version = client.opts.Version
	// inMsgs and inBytes are updated outside of the client's lock, so
	// we need to use atomic here.
	ci.InMsgs = atomic.LoadInt64(&client.inMsgs)
	ci.InBytes = atomic.LoadInt64(&client.inBytes)
	ci.Stalls = atomic.LoadInt64(&client.stalls)
	ci.Proxy = createProxyInfo(client)

	// If the connection is gone, too bad, we won't set TLSVersion and TLSCipher.
	// Exclude clients that are still doing handshake so we don't block in
	// ConnectionState().
	if client.flags.isSet(handshakeComplete) && nc != nil {
		if conn, ok := nc.(*tls.Conn); ok {
			cs := conn.ConnectionState()
			ci.TLSVersion = tlsVersion(cs.Version)
			ci.TLSCipher = tls.CipherSuiteName(cs.CipherSuite)
			if auth && len(cs.PeerCertificates) > 0 {
				ci.TLSPeerCerts = makePeerCerts(cs.PeerCertificates)
			}
			ci.TLSFirst = client.flags.isSet(didTLSFirst)
		}
	}

	if client.port != 0 {
		ci.Port = int(client.port)
		ci.IP = client.host
	}
}

// If this client came from a trusted proxy, this will return a ProxyInfo
// to be used in ConnInfo or LeafInfo.
//
// Client lock must be held on entry.
func createProxyInfo(c *client) *ProxyInfo {
	if c.proxyKey == _EMPTY_ {
		return nil
	}
	return &ProxyInfo{Key: c.proxyKey}
}

func makePeerCerts(pc []*x509.Certificate) []*TLSPeerCert {
	res := make([]*TLSPeerCert, len(pc))
	for i, c := range pc {
		tmp := sha256.Sum256(c.RawSubjectPublicKeyInfo)
		ssha := hex.EncodeToString(tmp[:])
		tmp = sha256.Sum256(c.Raw)
		csha := hex.EncodeToString(tmp[:])
		res[i] = &TLSPeerCert{Subject: c.Subject.String(), SubjectPKISha256: ssha, CertSha256: csha}
	}
	return res
}

// Assume lock is held
func (c *client) getRTT() time.Duration {
	if c.rtt == 0 {
		// If a real client, go ahead and send ping now to get a value
		// for RTT. For tests and telnet, or if client is closing, etc skip.
		if c.opts.Lang != _EMPTY_ {
			c.sendRTTPingLocked()
		}
		return 0
	}
	var rtt time.Duration
	if c.rtt > time.Microsecond && c.rtt < time.Millisecond {
		rtt = c.rtt.Truncate(time.Microsecond)
	} else {
		rtt = c.rtt.Truncate(time.Nanosecond)
	}
	return rtt
}

func decodeBool(w http.ResponseWriter, r *http.Request, param string) (bool, error) {
	str := r.URL.Query().Get(param)
	if str == _EMPTY_ {
		return false, nil
	}
	val, err := strconv.ParseBool(str)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("Error decoding boolean for '%s': %v", param, err)))
		return false, err
	}
	return val, nil
}

func decodeUint64(w http.ResponseWriter, r *http.Request, param string) (uint64, error) {
	str := r.URL.Query().Get(param)
	if str == _EMPTY_ {
		return 0, nil
	}
	val, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("Error decoding uint64 for '%s': %v", param, err)))
		return 0, err
	}
	return val, nil
}

func decodeInt(w http.ResponseWriter, r *http.Request, param string) (int, error) {
	str := r.URL.Query().Get(param)
	if str == _EMPTY_ {
		return 0, nil
	}
	val, err := strconv.Atoi(str)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("Error decoding int for '%s': %v", param, err)))
		return 0, err
	}
	return val, nil
}

func decodeState(w http.ResponseWriter, r *http.Request) (ConnState, error) {
	str := r.URL.Query().Get("state")
	if str == _EMPTY_ {
		return ConnOpen, nil
	}
	switch strings.ToLower(str) {
	case "open":
		return ConnOpen, nil
	case "closed":
		return ConnClosed, nil
	case "any", "all":
		return ConnAll, nil
	}
	// We do not understand intended state here.
	w.WriteHeader(http.StatusBadRequest)
	err := fmt.Errorf("Error decoding state for %s", str)
	w.Write([]byte(err.Error()))
	return 0, err
}

func decodeSubs(w http.ResponseWriter, r *http.Request) (subs bool, subsDet bool, err error) {
	subsDet = strings.ToLower(r.URL.Query().Get("subs")) == "detail"
	if !subsDet {
		subs, err = decodeBool(w, r, "subs")
	}
	return
}

// HandleConnz process HTTP requests for connection information.
func (s *Server) HandleConnz(w http.ResponseWriter, r *http.Request) {
	sortOpt := SortOpt(r.URL.Query().Get("sort"))
	auth, err := decodeBool(w, r, "auth")
	if err != nil {
		return
	}
	subs, subsDet, err := decodeSubs(w, r)
	if err != nil {
		return
	}
	offset, err := decodeInt(w, r, "offset")
	if err != nil {
		return
	}
	limit, err := decodeInt(w, r, "limit")
	if err != nil {
		return
	}
	cid, err := decodeUint64(w, r, "cid")
	if err != nil {
		return
	}
	state, err := decodeState(w, r)
	if err != nil {
		return
	}

	user := r.URL.Query().Get("user")
	acc := r.URL.Query().Get("acc")
	mqttCID := r.URL.Query().Get("mqtt_client")

	connzOpts := &ConnzOptions{
		Sort:                sortOpt,
		Username:            auth,
		Subscriptions:       subs,
		SubscriptionsDetail: subsDet,
		Offset:              offset,
		Limit:               limit,
		CID:                 cid,
		MQTTClient:          mqttCID,
		State:               state,
		User:                user,
		Account:             acc,
	}

	s.mu.Lock()
	s.httpReqStats[ConnzPath]++
	s.mu.Unlock()

	c, err := s.Connz(connzOpts)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		s.Errorf("Error marshaling response to /connz request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

// Routez represents detailed information on current client connections.
type Routez struct {
	ID        string             `json:"server_id"`
	Name      string             `json:"server_name"`
	Now       time.Time          `json:"now"`
	Import    *SubjectPermission `json:"import,omitempty"`
	Export    *SubjectPermission `json:"export,omitempty"`
	NumRoutes int                `json:"num_routes"`
	Routes    []*RouteInfo       `json:"routes"`
}

// RoutezOptions are options passed to Routez
type RoutezOptions struct {
	// Subscriptions indicates that Routez will return a route's subscriptions
	Subscriptions bool `json:"subscriptions"`
	// SubscriptionsDetail indicates if subscription details should be included in the results
	SubscriptionsDetail bool `json:"subscriptions_detail"`
}

// RouteInfo has detailed information on a per connection basis.
type RouteInfo struct {
	Rid          uint64             `json:"rid"`
	RemoteID     string             `json:"remote_id"`
	RemoteName   string             `json:"remote_name"`
	DidSolicit   bool               `json:"did_solicit"`
	IsConfigured bool               `json:"is_configured"`
	IP           string             `json:"ip"`
	Port         int                `json:"port"`
	Start        time.Time          `json:"start"`
	LastActivity time.Time          `json:"last_activity"`
	RTT          string             `json:"rtt,omitempty"`
	Uptime       string             `json:"uptime"`
	Idle         string             `json:"idle"`
	Import       *SubjectPermission `json:"import,omitempty"`
	Export       *SubjectPermission `json:"export,omitempty"`
	Pending      int                `json:"pending_size"`
	InMsgs       int64              `json:"in_msgs"`
	OutMsgs      int64              `json:"out_msgs"`
	InBytes      int64              `json:"in_bytes"`
	OutBytes     int64              `json:"out_bytes"`
	NumSubs      uint32             `json:"subscriptions"`
	Subs         []string           `json:"subscriptions_list,omitempty"`
	SubsDetail   []SubDetail        `json:"subscriptions_list_detail,omitempty"`
	Account      string             `json:"account,omitempty"`
	Compression  string             `json:"compression,omitempty"`
}

// Routez returns a Routez struct containing information about routes.
func (s *Server) Routez(routezOpts *RoutezOptions) (*Routez, error) {
	rs := &Routez{
		Now:    time.Now().UTC(),
		Routes: []*RouteInfo{},
	}

	if routezOpts == nil {
		routezOpts = &RoutezOptions{}
	}

	s.mu.Lock()
	rs.NumRoutes = s.numRoutes()

	// copy the server id for monitoring
	rs.ID = s.info.ID

	// Check for defined permissions for all connected routes.
	if perms := s.getOpts().Cluster.Permissions; perms != nil {
		rs.Import = perms.Import
		rs.Export = perms.Export
	}
	rs.Name = s.info.Name

	addRoute := func(r *client) {
		r.mu.Lock()
		ri := &RouteInfo{
			Rid:          r.cid,
			RemoteID:     r.route.remoteID,
			RemoteName:   r.route.remoteName,
			DidSolicit:   r.route.didSolicit,
			IsConfigured: r.route.routeType == Explicit,
			InMsgs:       atomic.LoadInt64(&r.inMsgs),
			OutMsgs:      r.outMsgs,
			InBytes:      atomic.LoadInt64(&r.inBytes),
			OutBytes:     r.outBytes,
			NumSubs:      uint32(len(r.subs)),
			Import:       r.opts.Import,
			Pending:      int(r.out.pb),
			Export:       r.opts.Export,
			RTT:          r.getRTT().String(),
			Start:        r.start,
			LastActivity: r.last,
			Uptime:       myUptime(rs.Now.Sub(r.start)),
			Idle:         myUptime(rs.Now.Sub(r.last)),
			Account:      string(r.route.accName),
			Compression:  r.route.compression,
		}

		if len(r.subs) > 0 {
			if routezOpts.SubscriptionsDetail {
				ri.SubsDetail = newSubsDetailList(r)
			} else if routezOpts.Subscriptions {
				ri.Subs = newSubsList(r)
			}
		}

		switch conn := r.nc.(type) {
		case *net.TCPConn, *tls.Conn:
			addr := conn.RemoteAddr().(*net.TCPAddr)
			ri.Port = addr.Port
			ri.IP = addr.IP.String()
		}
		r.mu.Unlock()
		rs.Routes = append(rs.Routes, ri)
	}

	// Walk the list
	s.forEachRoute(func(r *client) {
		addRoute(r)
	})
	s.mu.Unlock()
	return rs, nil
}

// HandleRoutez process HTTP requests for route information.
func (s *Server) HandleRoutez(w http.ResponseWriter, r *http.Request) {
	subs, subsDetail, err := decodeSubs(w, r)
	if err != nil {
		return
	}

	opts := RoutezOptions{Subscriptions: subs, SubscriptionsDetail: subsDetail}

	s.mu.Lock()
	s.httpReqStats[RoutezPath]++
	s.mu.Unlock()

	// As of now, no error is ever returned.
	rs, _ := s.Routez(&opts)
	b, err := json.MarshalIndent(rs, "", "  ")
	if err != nil {
		s.Errorf("Error marshaling response to /routez request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

// Subsz represents detail information on current connections.
type Subsz struct {
	ID  string    `json:"server_id"`
	Now time.Time `json:"now"`
	*SublistStats
	Total  int         `json:"total"`
	Offset int         `json:"offset"`
	Limit  int         `json:"limit"`
	Subs   []SubDetail `json:"subscriptions_list,omitempty"`
}

// SubszOptions are the options passed to Subsz.
// As of now, there are no options defined.
type SubszOptions struct {
	// Offset is used for pagination. Subsz() only returns connections starting at this
	// offset from the global results.
	Offset int `json:"offset"`

	// Limit is the maximum number of subscriptions that should be returned by Subsz().
	Limit int `json:"limit"`

	// Subscriptions indicates if subscription details should be included in the results.
	Subscriptions bool `json:"subscriptions"`

	// Filter based on this account name.
	Account string `json:"account,omitempty"`

	// Test the list against this subject. Needs to be literal since it signifies a publish subject.
	// We will only return subscriptions that would match if a message was sent to this subject.
	Test string `json:"test,omitempty"`
}

// SubDetail is for verbose information for subscriptions.
type SubDetail struct {
	Account    string `json:"account,omitempty"`
	AccountTag string `json:"account_tag,omitempty"`
	Subject    string `json:"subject"`
	Queue      string `json:"qgroup,omitempty"`
	Sid        string `json:"sid"`
	Msgs       int64  `json:"msgs"`
	Max        int64  `json:"max,omitempty"`
	Cid        uint64 `json:"cid"`
}

// Subscription client should be locked and guaranteed to be present.
func newSubDetail(sub *subscription) SubDetail {
	sd := newClientSubDetail(sub)
	sd.Account = sub.client.acc.GetName()
	sd.AccountTag = sub.client.acc.getNameTag()
	return sd
}

// For subs details under clients.
func newClientSubDetail(sub *subscription) SubDetail {
	return SubDetail{
		Subject: string(sub.subject),
		Queue:   string(sub.queue),
		Sid:     string(sub.sid),
		Msgs:    sub.nm,
		Max:     sub.max,
		Cid:     sub.client.cid,
	}
}

// Subsz returns a Subsz struct containing subjects statistics
func (s *Server) Subsz(opts *SubszOptions) (*Subsz, error) {
	var (
		subdetail bool
		test      bool
		offset    int
		testSub   string
		filterAcc string
		limit     = DefaultSubListSize
	)

	if opts != nil {
		subdetail = opts.Subscriptions
		offset = opts.Offset
		if offset < 0 {
			offset = 0
		}
		limit = opts.Limit
		if limit <= 0 {
			limit = DefaultSubListSize
		}
		if opts.Test != _EMPTY_ {
			testSub = opts.Test
			test = true
			if !IsValidLiteralSubject(testSub) {
				return nil, fmt.Errorf("invalid test subject, must be valid publish subject: %s", testSub)
			}
		}
		if opts.Account != _EMPTY_ {
			filterAcc = opts.Account
		}
	}

	slStats := &SublistStats{}

	// FIXME(dlc) - Make account aware.
	sz := &Subsz{
		ID:           s.info.ID,
		Now:          time.Now().UTC(),
		SublistStats: slStats,
		Total:        0,
		Offset:       offset,
		Limit:        limit,
		Subs:         nil,
	}

	if subdetail {
		var raw [4096]*subscription
		subs := raw[:0]
		s.accounts.Range(func(k, v any) bool {
			acc := v.(*Account)
			if filterAcc != _EMPTY_ && acc.GetName() != filterAcc {
				return true
			}
			slStats.add(acc.sl.Stats())
			acc.sl.localSubs(&subs, false)
			return true
		})

		details := make([]SubDetail, 0, len(subs))
		i := 0
		// TODO(dlc) - may be inefficient and could just do normal match when total subs is large and filtering.
		for _, sub := range subs {
			// Check for filter
			if test && !matchLiteral(testSub, string(sub.subject)) {
				continue
			}
			if sub.client == nil {
				continue
			}
			sub.client.mu.Lock()
			details = append(details, newSubDetail(sub))
			sub.client.mu.Unlock()
			i++
		}
		// Make sure these are sane.
		maxIndex := i
		minoff := min(max(sz.Offset, 0), maxIndex)
		maxoff := minoff + min(sz.Limit, maxIndex-minoff)
		sz.Subs = details[minoff:maxoff]
		sz.Total = len(details)
	} else {
		s.accounts.Range(func(k, v any) bool {
			acc := v.(*Account)
			if filterAcc != _EMPTY_ && acc.GetName() != filterAcc {
				return true
			}
			slStats.add(acc.sl.Stats())
			return true
		})
	}

	return sz, nil
}

// HandleSubsz processes HTTP requests for subjects stats.
func (s *Server) HandleSubsz(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.httpReqStats[SubszPath]++
	s.mu.Unlock()

	subs, err := decodeBool(w, r, "subs")
	if err != nil {
		return
	}
	offset, err := decodeInt(w, r, "offset")
	if err != nil {
		return
	}
	limit, err := decodeInt(w, r, "limit")
	if err != nil {
		return
	}
	testSub := r.URL.Query().Get("test")
	// Filtered account.
	filterAcc := r.URL.Query().Get("acc")

	subszOpts := &SubszOptions{
		Subscriptions: subs,
		Offset:        offset,
		Limit:         limit,
		Account:       filterAcc,
		Test:          testSub,
	}

	st, err := s.Subsz(subszOpts)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	var b []byte
	b, err = json.MarshalIndent(st, "", "  ")
	if err != nil {
		s.Errorf("Error marshaling response to /subscriptionsz request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

// HandleStacksz processes HTTP requests for getting stacks
func (s *Server) HandleStacksz(w http.ResponseWriter, r *http.Request) {
	// Do not get any lock here that would prevent getting the stacks
	// if we were to have a deadlock somewhere.
	var defaultBuf [defaultStackBufSize]byte
	size := defaultStackBufSize
	buf := defaultBuf[:size]
	n := 0
	for {
		n = runtime.Stack(buf, true)
		if n < size {
			break
		}
		size *= 2
		buf = make([]byte, size)
	}
	// Handle response
	ResponseHandler(w, r, buf[:n])
}

type IpqueueszStatusIPQ struct {
	Pending    int `json:"pending"`
	InProgress int `json:"in_progress,omitempty"`
}

type IpqueueszStatus map[string]IpqueueszStatusIPQ

func (s *Server) Ipqueuesz(opts *IpqueueszOptions) *IpqueueszStatus {
	all, qfilter := opts.All, opts.Filter
	queues := IpqueueszStatus{}
	s.ipQueues.Range(func(k, v any) bool {
		var pending, inProgress int
		name := k.(string)
		queue, ok := v.(interface {
			len() int
			inProgress() int64
		})
		if ok {
			pending = queue.len()
			inProgress = int(queue.inProgress())
		}
		if !all && (pending == 0 && inProgress == 0) {
			return true
		} else if qfilter != _EMPTY_ && !strings.Contains(name, qfilter) {
			return true
		}
		queues[name] = IpqueueszStatusIPQ{Pending: pending, InProgress: inProgress}
		return true
	})
	return &queues
}

func (s *Server) HandleIPQueuesz(w http.ResponseWriter, r *http.Request) {
	all, err := decodeBool(w, r, "all")
	if err != nil {
		return
	}
	qfilter := r.URL.Query().Get("queues")

	queues := s.Ipqueuesz(&IpqueueszOptions{
		All:    all,
		Filter: qfilter,
	})

	b, _ := json.MarshalIndent(queues, "", "   ")
	ResponseHandler(w, r, b)
}

// Varz will output server information on the monitoring port at /varz.
type Varz struct {
	ID                    string                 `json:"server_id"`                         // ID is the unique server ID generated at start
	Name                  string                 `json:"server_name"`                       // Name is the configured server name, equals ID when not set
	Version               string                 `json:"version"`                           // Version is the version of the running server
	Proto                 int                    `json:"proto"`                             // Proto is the protocol version this server supports
	GitCommit             string                 `json:"git_commit,omitempty"`              // GitCommit is the git repository commit hash that the build corresponds with
	GoVersion             string                 `json:"go"`                                // GoVersion is the version of Go used to build this binary
	Host                  string                 `json:"host"`                              // Host is the hostname the server runs on
	Port                  int                    `json:"port"`                              // Port is the port the server listens on for client connections
	AuthRequired          bool                   `json:"auth_required,omitempty"`           // AuthRequired indicates if users are required to authenticate to join the server
	TLSRequired           bool                   `json:"tls_required,omitempty"`            // TLSRequired indicates if connections must use TLS when connecting to this server
	TLSVerify             bool                   `json:"tls_verify,omitempty"`              // TLSVerify indicates if full TLS verification will be performed
	TLSOCSPPeerVerify     bool                   `json:"tls_ocsp_peer_verify,omitempty"`    // TLSOCSPPeerVerify indicates if the OCSP protocol will be used to verify peers
	IP                    string                 `json:"ip,omitempty"`                      // IP is the IP address the server listens on if set
	ClientConnectURLs     []string               `json:"connect_urls,omitempty"`            // ClientConnectURLs is the list of URLs NATS clients can use to connect to this server
	WSConnectURLs         []string               `json:"ws_connect_urls,omitempty"`         // WSConnectURLs is the list of URLs websocket clients can use to connect to this server
	MaxConn               int                    `json:"max_connections"`                   // MaxConn is the maximum amount of connections the server can accept
	MaxSubs               int                    `json:"max_subscriptions,omitempty"`       // MaxSubs is the maximum amount of subscriptions the server can manage
	PingInterval          time.Duration          `json:"ping_interval"`                     // PingInterval is the interval the server will send PING messages during periods of inactivity on a connection
	MaxPingsOut           int                    `json:"ping_max"`                          // MaxPingsOut is the number of unanswered PINGs after which the connection will be considered stale
	HTTPHost              string                 `json:"http_host"`                         // HTTPHost is the HTTP host monitoring connections are accepted on
	HTTPPort              int                    `json:"http_port"`                         // HTTPPort is the port monitoring connections are accepted on
	HTTPBasePath          string                 `json:"http_base_path"`                    // HTTPBasePath is the path prefix for access to monitor endpoints
	HTTPSPort             int                    `json:"https_port"`                        // HTTPSPort is the HTTPS host monitoring connections are accepted on`
	AuthTimeout           float64                `json:"auth_timeout"`                      // AuthTimeout is the amount of seconds connections have to complete authentication
	MaxControlLine        int32                  `json:"max_control_line"`                  // MaxControlLine is the amount of bytes a signal control message may be
	MaxPayload            int                    `json:"max_payload"`                       // MaxPayload is the maximum amount of bytes a message may have as payload
	MaxPending            int64                  `json:"max_pending"`                       // MaxPending is the maximum amount of unprocessed bytes a connection may have
	Cluster               ClusterOptsVarz        `json:"cluster,omitempty"`                 // Cluster is the Cluster state
	Gateway               GatewayOptsVarz        `json:"gateway,omitempty"`                 // Gateway is the Super Cluster state
	LeafNode              LeafNodeOptsVarz       `json:"leaf,omitempty"`                    // LeafNode is the Leafnode state
	MQTT                  MQTTOptsVarz           `json:"mqtt,omitempty"`                    // MQTT is the MQTT state
	Websocket             WebsocketOptsVarz      `json:"websocket,omitempty"`               // Websocket is the Websocket client state
	JetStream             JetStreamVarz          `json:"jetstream,omitempty"`               // JetStream is the JetStream state
	TLSTimeout            float64                `json:"tls_timeout"`                       // TLSTimeout is how long TLS operations have to complete
	WriteDeadline         time.Duration          `json:"write_deadline"`                    // WriteDeadline is the maximum time writes to sockets have to complete
	WriteTimeout          string                 `json:"write_timeout,omitempty"`           // WriteTimeout is the closure policy for write deadline errors
	Start                 time.Time              `json:"start"`                             // Start is time when the server was started
	Now                   time.Time              `json:"now"`                               // Now is the current time of the server
	Uptime                string                 `json:"uptime"`                            // Uptime is how long the server has been running
	Mem                   int64                  `json:"mem"`                               // Mem is the resident memory allocation
	Cores                 int                    `json:"cores"`                             // Cores is the number of cores the process has access to
	MaxProcs              int                    `json:"gomaxprocs"`                        // MaxProcs is the configured GOMAXPROCS value
	MemLimit              int64                  `json:"gomemlimit,omitempty"`              // MemLimit is the configured GOMEMLIMIT value
	CPU                   float64                `json:"cpu"`                               // CPU is the current total CPU usage
	Connections           int                    `json:"connections"`                       // Connections is the current connected connections
	TotalConnections      uint64                 `json:"total_connections"`                 // TotalConnections is the total connections the server have ever handled
	Routes                int                    `json:"routes"`                            // Routes is the number of connected route servers
	Remotes               int                    `json:"remotes"`                           // Remotes is the configured route remote endpoints
	Leafs                 int                    `json:"leafnodes"`                         // Leafs is the number connected leafnode clients
	InMsgs                int64                  `json:"in_msgs"`                           // InMsgs is the total number of messages this server received. This includes messages from the clients, routers, gateways and leaf nodes
	InBytes               int64                  `json:"in_bytes"`                          // InBytes is the total number of bytes this server received. This includes messages from the clients, routers, gateways and leaf nodes
	InClientMsgs          int64                  `json:"in_client_msgs"`                    // InClientMsgs is the number of messages this server received from the clients
	InClientBytes         int64                  `json:"in_client_bytes"`                   // InClientBytes is the number of bytes this server received from the clients
	OutMsgs               int64                  `json:"out_msgs"`                          // OutMsgs is the total number of message this server sent. This includes messages sent to the clients, routers, gateways and leaf nodes
	OutBytes              int64                  `json:"out_bytes"`                         // OutBytes is the total number of bytes this server sent. This includes messages sent to the clients, routers, gateways and leaf nodes
	OutClientMsgs         int64                  `json:"out_client_msgs"`                   // OutClientMsgs is the number of messages this server sent to the clients
	OutClientBytes        int64                  `json:"out_client_bytes"`                  // OutClientBytes is the number of bytes this server sent to the clients
	SlowConsumers         int64                  `json:"slow_consumers"`                    // SlowConsumers is the total count of clients that were disconnected since start due to being slow consumers
	StaleConnections      int64                  `json:"stale_connections"`                 // StaleConnections is the total count of stale connections that were detected
	StalledClients        int64                  `json:"stalled_clients"`                   // StalledClients is the total number of times that clients have been stalled.
	Subscriptions         uint32                 `json:"subscriptions"`                     // Subscriptions is the count of active subscriptions
	HTTPReqStats          map[string]uint64      `json:"http_req_stats"`                    // HTTPReqStats is the number of requests each HTTP endpoint received
	ConfigLoadTime        time.Time              `json:"config_load_time"`                  // ConfigLoadTime is the time the configuration was loaded or reloaded
	ConfigDigest          string                 `json:"config_digest"`                     // ConfigDigest is a calculated hash of the current configuration
	Tags                  jwt.TagList            `json:"tags,omitempty"`                    // Tags are the tags assigned to the server in configuration
	Metadata              map[string]string      `json:"metadata,omitempty"`                // Metadata is the metadata assigned to the server in configuration
	FeatureFlags          map[string]bool        `json:"feature_flags,omitempty"`           // FeatureFlags is the feature flags enabled/disabled in configuration
	TrustedOperatorsJwt   []string               `json:"trusted_operators_jwt,omitempty"`   // TrustedOperatorsJwt is the JWTs for all trusted operators
	TrustedOperatorsClaim []*jwt.OperatorClaims  `json:"trusted_operators_claim,omitempty"` // TrustedOperatorsClaim is the decoded claims for each trusted operator
	SystemAccount         string                 `json:"system_account,omitempty"`          // SystemAccount is the name of the System account
	PinnedAccountFail     uint64                 `json:"pinned_account_fails,omitempty"`    // PinnedAccountFail is how often user logon fails due to the issuer account not being pinned.
	OCSPResponseCache     *OCSPResponseCacheVarz `json:"ocsp_peer_cache,omitempty"`         // OCSPResponseCache is the state of the OCSP cache
	SlowConsumersStats    *SlowConsumersStats    `json:"slow_consumer_stats"`               // SlowConsumersStats are statistics about all detected Slow Consumer
	StaleConnectionStats  *StaleConnectionStats  `json:"stale_connection_stats,omitempty"`  // StaleConnectionStats are statistics about all detected Stale Connections
	DiskIOWaitStats       *DiskIOWaitStats       `json:"disk_io_wait_stats"`                // DiskIOWaitStats are statistics about disk I/O semaphore contention
	Proxies               *ProxiesOptsVarz       `json:"proxies,omitempty"`                 // Proxies hold information about network proxy devices
	TLSCertNotAfter       time.Time              `json:"tls_cert_not_after,omitzero"`       // TLSCertNotAfter is the expiration date of the TLS certificate of this server
}

// JetStreamVarz contains basic runtime information about jetstream
type JetStreamVarz struct {
	Config *JetStreamConfig `json:"config,omitempty"` // Config is the active JetStream configuration
	Stats  *JetStreamStats  `json:"stats,omitempty"`  // Stats is the statistics for the JetStream server
	Meta   *MetaClusterInfo `json:"meta,omitempty"`   // Meta is information about the JetStream metalayer
	Limits *JSLimitOpts     `json:"limits,omitempty"` // Limits are the configured JetStream limits
}

// ClusterOptsVarz contains monitoring cluster information
type ClusterOptsVarz struct {
	Name            string        `json:"name,omitempty"`              // Name is the configured cluster name
	Host            string        `json:"addr,omitempty"`              // Host is the host the cluster listens on for connections
	Port            int           `json:"cluster_port,omitempty"`      // Port is the port the cluster listens on for connections
	AuthTimeout     float64       `json:"auth_timeout,omitempty"`      // AuthTimeout is the time cluster connections have to complete authentication
	URLs            []string      `json:"urls,omitempty"`              // URLs is the list of cluster URLs
	TLSTimeout      float64       `json:"tls_timeout,omitempty"`       // TLSTimeout is how long TLS operations have to complete
	TLSRequired     bool          `json:"tls_required,omitempty"`      // TLSRequired indicates if TLS is required for connections
	TLSVerify       bool          `json:"tls_verify,omitempty"`        // TLSVerify indicates if full verification of TLS connections is performed
	PoolSize        int           `json:"pool_size,omitempty"`         // PoolSize is the configured route connection pool size
	WriteDeadline   time.Duration `json:"write_deadline,omitempty"`    // WriteDeadline is the maximum time writes to sockets have to complete
	WriteTimeout    string        `json:"write_timeout,omitempty"`     // WriteTimeout is the closure policy for write deadline errors
	TLSCertNotAfter time.Time     `json:"tls_cert_not_after,omitzero"` // TLSCertNotAfter is the expiration date of the TLS certificate
}

// GatewayOptsVarz contains monitoring gateway information
type GatewayOptsVarz struct {
	Name            string                  `json:"name,omitempty"`              // Name is the configured cluster name
	Host            string                  `json:"host,omitempty"`              // Host is the host the gateway listens on for connections
	Port            int                     `json:"port,omitempty"`              // Port is the post gateway connections listens on
	AuthTimeout     float64                 `json:"auth_timeout,omitempty"`      // AuthTimeout is the time cluster connections have to complete authentication
	TLSTimeout      float64                 `json:"tls_timeout,omitempty"`       // TLSTimeout is how long TLS operations have to complete
	TLSRequired     bool                    `json:"tls_required,omitempty"`      // TLSRequired indicates if TLS is required for connections
	TLSVerify       bool                    `json:"tls_verify,omitempty"`        // TLSVerify indicates if full verification of TLS connections is performed
	Advertise       string                  `json:"advertise,omitempty"`         // Advertise is the URL advertised to remote gateway clients
	ConnectRetries  int                     `json:"connect_retries,omitempty"`   // ConnectRetries is how many connection attempts the route will make
	Gateways        []RemoteGatewayOptsVarz `json:"gateways,omitempty"`          // Gateways is state of configured gateway remotes
	RejectUnknown   bool                    `json:"reject_unknown,omitempty"`    // RejectUnknown indicates if unknown cluster connections will be rejected
	WriteDeadline   time.Duration           `json:"write_deadline,omitempty"`    // WriteDeadline is the maximum time writes to sockets have to complete
	WriteTimeout    string                  `json:"write_timeout,omitempty"`     // WriteTimeout is the closure policy for write deadline errors
	TLSCertNotAfter time.Time               `json:"tls_cert_not_after,omitzero"` // TLSCertNotAfter is the expiration date of the TLS certificaet
}

// RemoteGatewayOptsVarz contains monitoring remote gateway information
type RemoteGatewayOptsVarz struct {
	Name       string   `json:"name"`                  // Name is the name of the remote gateway
	TLSTimeout float64  `json:"tls_timeout,omitempty"` // TLSTimeout is how long TLS operations have to complete
	URLs       []string `json:"urls,omitempty"`        // URLs is the list of Gateway URLs
}

// LeafNodeOptsVarz contains monitoring leaf node information
type LeafNodeOptsVarz struct {
	Host              string               `json:"host,omitempty"`                 // Host is the host the server listens on
	Port              int                  `json:"port,omitempty"`                 // Port is the port the server listens on
	AuthTimeout       float64              `json:"auth_timeout,omitempty"`         // AuthTimeout is the time Leafnode connections have to complete authentication
	TLSTimeout        float64              `json:"tls_timeout,omitempty"`          // TLSTimeout is how long TLS operations have to complete
	TLSRequired       bool                 `json:"tls_required,omitempty"`         // TLSRequired indicates if TLS is required for connections
	TLSVerify         bool                 `json:"tls_verify,omitempty"`           // TLSVerify indicates if full verification of TLS connections is performed
	Remotes           []RemoteLeafOptsVarz `json:"remotes,omitempty"`              // Remotes is state of configured Leafnode remotes
	TLSOCSPPeerVerify bool                 `json:"tls_ocsp_peer_verify,omitempty"` // TLSOCSPPeerVerify indicates if OCSP verification will be performed
	WriteDeadline     time.Duration        `json:"write_deadline,omitempty"`       // WriteDeadline is the maximum time writes to sockets have to complete
	WriteTimeout      string               `json:"write_timeout,omitempty"`        // WriteTimeout is the closure policy for write deadline errors
	TLSCertNotAfter   time.Time            `json:"tls_cert_not_after,omitzero"`    // TLSCertNotAfter is the expiration date of the TLS certificate
}

// DenyRules Contains lists of subjects not allowed to be imported/exported
type DenyRules struct {
	Exports []string `json:"exports,omitempty"` // Exports are denied exports
	Imports []string `json:"imports,omitempty"` // Imports are denied imports
}

// RemoteLeafOptsVarz contains monitoring remote leaf node information
type RemoteLeafOptsVarz struct {
	LocalAccount      string     `json:"local_account,omitempty"`        // LocalAccount is the local account this leaf is logged into
	TLSTimeout        float64    `json:"tls_timeout,omitempty"`          // TLSTimeout is how long TLS operations have to complete
	URLs              []string   `json:"urls,omitempty"`                 // URLs is the list of URLs for the remote Leafnode connection
	Deny              *DenyRules `json:"deny,omitempty"`                 // Deny is the configured import and exports that the Leafnode may not access
	TLSOCSPPeerVerify bool       `json:"tls_ocsp_peer_verify,omitempty"` // TLSOCSPPeerVerify indicates if OCSP verification will be done
}

// MQTTOptsVarz contains monitoring MQTT information
type MQTTOptsVarz struct {
	Host              string        `json:"host,omitempty"`                 // Host is the host the server listens on
	Port              int           `json:"port,omitempty"`                 // Port is the port the server listens on
	NoAuthUser        string        `json:"no_auth_user,omitempty"`         // NoAuthUser is the user that will be used for unauthenticated connections
	AuthTimeout       float64       `json:"auth_timeout,omitempty"`         // AuthTimeout is how long authentication has to complete
	TLSMap            bool          `json:"tls_map,omitempty"`              // TLSMap indicates if TLS Mapping is enabled
	TLSTimeout        float64       `json:"tls_timeout,omitempty"`          // TLSTimeout is how long TLS operations have to complete
	TLSPinnedCerts    []string      `json:"tls_pinned_certs,omitempty"`     // TLSPinnedCerts is the list of certificates pinned to this connection
	JsDomain          string        `json:"js_domain,omitempty"`            // JsDomain is the JetStream domain used for MQTT state
	AckWait           time.Duration `json:"ack_wait,omitempty"`             // AckWait is how long the internal JetStream state store will allow acks to complete
	MaxAckPending     uint16        `json:"max_ack_pending,omitempty"`      // MaxAckPending is how many outstanding acks the internal JetStream state store will allow
	TLSOCSPPeerVerify bool          `json:"tls_ocsp_peer_verify,omitempty"` // TLSOCSPPeerVerify indicates if OCSP verification will be done
	TLSCertNotAfter   time.Time     `json:"tls_cert_not_after,omitzero"`    // TLSCertNotAfter is the expiration date of the TLS certificate
}

// WebsocketOptsVarz contains monitoring websocket information
type WebsocketOptsVarz struct {
	Host              string        `json:"host,omitempty"`                 // Host is the host the server listens on
	Port              int           `json:"port,omitempty"`                 // Port is the port the server listens on
	Advertise         string        `json:"advertise,omitempty"`            // Advertise is the connection URL the server advertises
	NoAuthUser        string        `json:"no_auth_user,omitempty"`         // NoAuthUser is the user that will be used for unauthenticated connections
	JWTCookie         string        `json:"jwt_cookie,omitempty"`           // JWTCookie is the name of a cookie the server will read for the connection JWT
	HandshakeTimeout  time.Duration `json:"handshake_timeout,omitempty"`    // HandshakeTimeout is how long the connection has to complete the websocket setup
	AuthTimeout       float64       `json:"auth_timeout,omitempty"`         // AuthTimeout is how long authentication has to complete
	NoTLS             bool          `json:"no_tls,omitempty"`               // NoTLS indicates if TLS is disabled
	TLSMap            bool          `json:"tls_map,omitempty"`              // TLSMap indicates if TLS Mapping is enabled
	TLSPinnedCerts    []string      `json:"tls_pinned_certs,omitempty"`     // TLSPinnedCerts is the list of certificates pinned to this connection
	SameOrigin        bool          `json:"same_origin,omitempty"`          // SameOrigin indicates if same origin connections are allowed
	AllowedOrigins    []string      `json:"allowed_origins,omitempty"`      // AllowedOrigins list of configured trusted origins
	Compression       bool          `json:"compression,omitempty"`          // Compression indicates if compression is supported
	TLSOCSPPeerVerify bool          `json:"tls_ocsp_peer_verify,omitempty"` // TLSOCSPPeerVerify indicates if OCSP verification will be done
	TLSCertNotAfter   time.Time     `json:"tls_cert_not_after,omitzero"`    // TLSCertNotAfter is the expiration date of the TLS certificate
}

// OCSPResponseCacheVarz contains OCSP response cache information
type OCSPResponseCacheVarz struct {
	Type      string `json:"cache_type,omitempty"`               // Type is the kind of cache being used
	Hits      int64  `json:"cache_hits,omitempty"`               // Hits is how many times the cache was able to answer a request
	Misses    int64  `json:"cache_misses,omitempty"`             // Misses is how many times the cache failed to answer a request
	Responses int64  `json:"cached_responses,omitempty"`         // Responses is how many responses are currently stored in the cache
	Revokes   int64  `json:"cached_revoked_responses,omitempty"` // Revokes is how many of the stored cache entries are revokes
	Goods     int64  `json:"cached_good_responses,omitempty"`    // Goods is how many of the stored cache entries are good responses
	Unknowns  int64  `json:"cached_unknown_responses,omitempty"` // Unknowns  is how many of the stored cache entries are unknown responses
}

// ProxiesOptsVarz contains proxies information
type ProxiesOptsVarz struct {
	Trusted []*ProxyOptsVarz `json:"trusted,omitempty"` // Trusted holds a list of trusted proxies
}

// ProxyOptsVarz contains proxy information
type ProxyOptsVarz struct {
	Key string `json:"key"` // Key is the public key of the proxy
}

// VarzOptions are the options passed to Varz().
// Currently, there are no options defined.
type VarzOptions struct{}

// SlowConsumersStats contains information about the slow consumers from different type of connections.
type SlowConsumersStats struct {
	Clients  uint64 `json:"clients"`  // Clients is how many Clients were slow consumers
	Routes   uint64 `json:"routes"`   // Routes is how many Routes were slow consumers
	Gateways uint64 `json:"gateways"` // Gateways is how many Gateways were slow consumers
	Leafs    uint64 `json:"leafs"`    // Leafs is how many Leafnodes were slow consumers
}

// StaleConnectionStats contains information about the stale connections from different type of connections.
type StaleConnectionStats struct {
	Clients  uint64 `json:"clients"`  // Clients is how many Client connections became stale connections
	Routes   uint64 `json:"routes"`   // Routes is how many Route connections became stale connections
	Gateways uint64 `json:"gateways"` // Gateways is how many Gateway connections became stale connections
	Leafs    uint64 `json:"leafs"`    // Leafs is how many Leafnode connections became stale connections
}

// DiskIOWaitStats contains information about disk I/O semaphore contention.
type DiskIOWaitStats struct {
	Waiters     int64  `json:"waiters"`       // Waiters is the number of goroutines waiting on the dios
	Waits       uint64 `json:"waits"`         // Waits is the number of dios acquires that had to wait
	WaitTime    uint64 `json:"wait_time"`     // WaitTime is the cumulative time spent waiting for dios
	MaxWaitTime uint64 `json:"max_wait_time"` // MaxWaitTime is the longest observed wait
}

func myUptime(d time.Duration) string {
	// Just use total seconds for uptime, and display days / years
	tsecs := d / time.Second
	tmins := tsecs / 60
	thrs := tmins / 60
	tdays := thrs / 24
	tyrs := tdays / 365

	if tyrs > 0 {
		return fmt.Sprintf("%dy%dd%dh%dm%ds", tyrs, tdays%365, thrs%24, tmins%60, tsecs%60)
	}
	if tdays > 0 {
		return fmt.Sprintf("%dd%dh%dm%ds", tdays, thrs%24, tmins%60, tsecs%60)
	}
	if thrs > 0 {
		return fmt.Sprintf("%dh%dm%ds", thrs, tmins%60, tsecs%60)
	}
	if tmins > 0 {
		return fmt.Sprintf("%dm%ds", tmins, tsecs%60)
	}
	return fmt.Sprintf("%ds", tsecs)
}

func tlsCertNotAfter(config *tls.Config) time.Time {
	if config == nil || len(config.Certificates) == 0 {
		return time.Time{}
	}
	cert := config.Certificates[0]
	leaf := cert.Leaf
	if leaf == nil {
		var err error
		leaf, err = x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return time.Time{}
		}
	}
	return leaf.NotAfter
}

// HandleRoot will show basic info and links to others handlers.
func (s *Server) HandleRoot(w http.ResponseWriter, r *http.Request) {
	// This feels dumb to me, but is required: https://code.google.com/p/go/issues/detail?id=4799
	if r.URL.Path != s.httpBasePath {
		http.NotFound(w, r)
		return
	}
	s.mu.Lock()
	s.httpReqStats[RootPath]++
	s.mu.Unlock()

	// Calculate source url. If git set go directly to that tag, otherwise just main.
	var srcUrl string
	if gitCommit == _EMPTY_ {
		srcUrl = "https://github.com/nats-io/nats-server"
	} else if serverVersion != _EMPTY_ {
		srcUrl = fmt.Sprintf("https://github.com/nats-io/nats-server/tree/%s", serverVersion)
	} else {
		srcUrl = fmt.Sprintf("https://github.com/nats-io/nats-server/tree/%s", gitCommit)
	}

	fmt.Fprintf(w, `<html lang="en">
	<head>
	<link rel="shortcut icon" href="https://nats.io/favicon.ico">
	<style type="text/css">
		body { font-family: ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,"Noto Sans",sans-serif; font-size: 18; font-weight: light-bold; margin-left: 32px }
		a { display:block; margin-left: 7px; padding-bottom: 6px; color: rgb(72 72 92); text-decoration: none }
		a:hover { font-weight: 600; color: rgb(59 50 202) }
		a.help { display:inline; font-weight: 600; color: rgb(59 50 202); font-size: 20}
		a.last { padding-bottom: 16px }
		a.version { font-size: 14; font-weight: 400; width: 312px; text-align: right; margin-top: -2rem }
		a.version:hover { color: rgb(22 22 32) }
		.endpoint { font-size: 12px; color: #999; font-family: monospace; display: none }
		a:hover .endpoint { display: inline }

	</style>
	</head>
	<body>
   <svg xmlns="http://www.w3.org/2000/svg" role="img" width="325" height="110" viewBox="-4.14 -3.89 436.28 119.03"><style>.st1{fill:#fff}.st2{fill:#34a574}</style><path fill="#27aae1" d="M4.3 84.6h42.2L70.7 107V84.6H103v-80H4.3v80zm15.9-61.3h18.5l35.6 33.2V23.3h11.8v42.9H68.2L32 32.4v33.8H20.2V23.3z"/><path d="M32 32.4l36.2 33.8h17.9V23.3H74.3v33.2L38.7 23.3H20.2v42.9H32z" class="st1"/><path d="M159.8 30.7L147 49h25.6z" class="st2"/><path d="M111.3 84.6H210v-80h-98.7v80zm41-61.5H168l30.8 43.2h-14.1l-5.8-8.3h-38.1l-5.8 8.3h-13.5l30.8-43.2z" class="st2"/><path d="M140.8 57.9h38.1l5.8 8.3h14.1L168 23.1h-15.7l-30.8 43.2H135l5.8-8.4zm19-27.2L172.6 49H147l12.8-18.3z" class="st1"/><path fill="#375c93" d="M218.3 84.6H317v-80h-98.7v80zm15.5-61.3h66.7V33h-27.2v33.2h-12.2V33h-27.3v-9.7z"/><path d="M261.1 66.2h12.2V33h27.2v-9.7h-66.7V33h27.3z" class="st1"/><path fill="#8dc63f" d="M325.3 4.6v80H424v-80h-98.7zm76.5 56.7c-3.2 3.2-10.2 5.7-26.8 5.7-12.3 0-24.1-1.9-30.7-4.7v-10c6.3 2.8 20.1 5.5 30.7 5.5 9.3 0 15.8-.3 17.5-2.1.6-.6.7-1.3.7-2 0-.8-.2-1.3-.7-1.8-1-1-2.6-1.7-17.4-2.1-15.7-.4-23.4-2-27-5.6-1.7-1.7-2.6-4.4-2.6-7.5 0-3.3.6-6.2 3.3-8.9 3.6-3.6 10.7-5.3 25.1-5.3 10.8 0 21.6 1.7 27.3 4v10.1c-6.5-2.8-17.8-4.8-27.2-4.8-10.4 0-14.8.6-16.2 2-.5.5-.8 1.1-.8 1.9 0 .9.2 1.5.7 2 1.3 1.3 6.1 1.7 17.3 1.9 16.4.4 23.5 1.8 27 5.2 1.8 1.8 2.8 4.7 2.8 7.7.1 3.2-.6 6.4-3 8.8z"/><path d="M375.2 39.5c-11.2-.2-16-.6-17.3-1.9-.5-.5-.7-1.1-.7-2 0-.8.3-1.4.8-1.9 1.3-1.3 5.8-2 16.2-2 9.4 0 20.7 2 27.2 4.8v-10c-5.7-2.3-16.6-4-27.3-4-14.5 0-21.6 1.8-25.1 5.3-2.7 2.7-3.3 5.6-3.3 8.9 0 3.1 1 5.8 2.6 7.5 3.6 3.6 11.3 5.2 27 5.6 14.8.4 16.4 1.1 17.4 2.1.5.5.7 1 .7 1.8 0 .7-.1 1.3-.7 2-1.8 1.8-8.3 2.1-17.5 2.1-10.6 0-24.3-2.6-30.7-5.5v10.1c6.6 2.8 18.4 4.7 30.7 4.7 16.6 0 23.6-2.5 26.8-5.7 2.4-2.4 3.1-5.6 3.1-8.9 0-3.1-1-5.9-2.8-7.7-3.6-3.5-10.7-4.9-27.1-5.3z" class="st1"/></svg>

	<a href=%s class='version'>v%s</a>

	</div>
	<br/>
	<a href=.%s>General<span class="endpoint"> %s</span></a>
	<a href=.%s>JetStream<span class="endpoint"> %s</span></a>
	<a href=.%s>Connections<span class="endpoint"> %s</span></a>
	<a href=.%s>Accounts<span class="endpoint"> %s</span></a>
	<a href=.%s>Account Stats<span class="endpoint"> %s</span></a>
	<a href=.%s>Subscriptions<span class="endpoint"> %s</span></a>
	<a href=.%s>Routes<span class="endpoint"> %s</span></a>
	<a href=.%s>LeafNodes<span class="endpoint"> %s</span></a>
	<a href=.%s>Gateways<span class="endpoint"> %s</span></a>
	<a href=.%s>Raft Groups<span class="endpoint"> %s</span></a>
	<a href=.%s>Health Probe<span class="endpoint"> %s</span></a>
	<a href=.%s class=last>Expvar<span class="endpoint"> %s</span></a>
    <a href=https://docs.nats.io/running-a-nats-service/nats_admin/monitoring class="help">Help</a>
  </body>
</html>`,
		srcUrl,
		VERSION,
		s.basePath(VarzPath), VarzPath,
		s.basePath(JszPath), JszPath,
		s.basePath(ConnzPath), ConnzPath,
		s.basePath(AccountzPath), AccountzPath,
		s.basePath(AccountStatzPath), AccountStatzPath,
		s.basePath(SubszPath), SubszPath,
		s.basePath(RoutezPath), RoutezPath,
		s.basePath(LeafzPath), LeafzPath,
		s.basePath(GatewayzPath), GatewayzPath,
		s.basePath(RaftzPath), RaftzPath,
		s.basePath(HealthzPath), HealthzPath,
		s.basePath(ExpvarzPath), ExpvarzPath,
	)
}

func (s *Server) updateJszVarz(js *jetStream, v *JetStreamVarz, doConfig bool) {
	if doConfig {
		js.mu.RLock()
		// We want to snapshot the config since it will then be available outside
		// of the js lock. So make a copy first, then point to this copy.
		cfg := js.config
		v.Config = &cfg
		js.mu.RUnlock()
	}
	v.Stats = js.usageStats()
	v.Limits = &s.getOpts().JetStreamLimits
	if mg := js.getMetaGroup(); mg != nil {
		if ci := s.raftNodeToClusterInfo(mg); ci != nil {
			v.Meta = &MetaClusterInfo{
				Name:         ci.Name,
				Leader:       ci.Leader,
				Replicas:     ci.Replicas,
				Size:         mg.ClusterSize(),
				QuorumNeeded: mg.QuorumNeeded(),
				Rescue:       mg.InRescue(),
			}
			if ci.Leader != _EMPTY_ {
				v.Meta.Peer = getHash(ci.Leader)
			}
			if ipq := s.jsAPIRoutedReqs; ipq != nil {
				v.Meta.PendingRequests = ipq.len()
			}
			if ipq := s.jsAPIRoutedInfoReqs; ipq != nil {
				v.Meta.PendingInfos = ipq.len()
			}
			v.Meta.Pending = v.Meta.PendingRequests + v.Meta.PendingInfos
			v.Meta.Snapshot = s.metaClusterSnapshotStats(js, mg)
		}
	}
}

// Varz returns a Varz struct containing the server information.
func (s *Server) Varz(varzOpts *VarzOptions) (*Varz, error) {
	var rss, vss int64
	var pcpu float64

	// We want to do that outside of the lock.
	pse.ProcUsage(&pcpu, &rss, &vss)

	s.mu.RLock()
	// We need to create a new instance of Varz (with no reference
	// whatsoever to anything stored in the server) since the user
	// has access to the returned value.
	v := s.createVarz(pcpu, rss)
	s.mu.RUnlock()

	if js := s.getJetStream(); js != nil {
		s.updateJszVarz(js, &v.JetStream, true)
	}

	return v, nil
}

// Returns a Varz instance.
// Server lock is held on entry.
func (s *Server) createVarz(pcpu float64, rss int64) *Varz {
	info := s.info
	opts := s.getOpts()
	c := &opts.Cluster
	gw := &opts.Gateway
	ln := &opts.LeafNode
	mqtt := &opts.MQTT
	ws := &opts.Websocket
	clustTlsReq := c.TLSConfig != nil
	gatewayTlsReq := gw.TLSConfig != nil
	leafTlsReq := ln.TLSConfig != nil
	leafTlsVerify := leafTlsReq && ln.TLSConfig.ClientAuth == tls.RequireAndVerifyClientCert
	leafTlsOCSPPeerVerify := s.ocspPeerVerify && leafTlsReq && ln.tlsConfigOpts.OCSPPeerConfig != nil && ln.tlsConfigOpts.OCSPPeerConfig.Verify
	mqttTlsOCSPPeerVerify := s.ocspPeerVerify && mqtt.TLSConfig != nil && mqtt.tlsConfigOpts.OCSPPeerConfig != nil && mqtt.tlsConfigOpts.OCSPPeerConfig.Verify
	wsTlsOCSPPeerVerify := s.ocspPeerVerify && ws.TLSConfig != nil && ws.tlsConfigOpts.OCSPPeerConfig != nil && ws.tlsConfigOpts.OCSPPeerConfig.Verify
	varz := &Varz{
		ID:           info.ID,
		Version:      info.Version,
		Proto:        info.Proto,
		GitCommit:    info.GitCommit,
		GoVersion:    info.GoVersion,
		Name:         info.Name,
		Host:         info.Host,
		Port:         info.Port,
		IP:           info.IP,
		HTTPHost:     opts.HTTPHost,
		HTTPPort:     opts.HTTPPort,
		HTTPBasePath: opts.HTTPBasePath,
		HTTPSPort:    opts.HTTPSPort,
		Cluster: ClusterOptsVarz{
			Name:          info.Cluster,
			Host:          c.Host,
			Port:          c.Port,
			AuthTimeout:   c.AuthTimeout,
			TLSTimeout:    c.TLSTimeout,
			TLSRequired:   clustTlsReq,
			TLSVerify:     clustTlsReq,
			PoolSize:      opts.Cluster.PoolSize,
			WriteDeadline: opts.Cluster.WriteDeadline,
			WriteTimeout:  opts.Cluster.WriteTimeout.String(),
		},
		Gateway: GatewayOptsVarz{
			Name:           gw.Name,
			Host:           gw.Host,
			Port:           gw.Port,
			AuthTimeout:    gw.AuthTimeout,
			TLSTimeout:     gw.TLSTimeout,
			TLSRequired:    gatewayTlsReq,
			TLSVerify:      gatewayTlsReq,
			Advertise:      gw.Advertise,
			ConnectRetries: gw.ConnectRetries,
			Gateways:       []RemoteGatewayOptsVarz{},
			RejectUnknown:  gw.RejectUnknown,
			WriteDeadline:  opts.Cluster.WriteDeadline,
			WriteTimeout:   opts.Cluster.WriteTimeout.String(),
		},
		LeafNode: LeafNodeOptsVarz{
			Host:              ln.Host,
			Port:              ln.Port,
			AuthTimeout:       ln.AuthTimeout,
			TLSTimeout:        ln.TLSTimeout,
			TLSRequired:       leafTlsReq,
			TLSVerify:         leafTlsVerify,
			TLSOCSPPeerVerify: leafTlsOCSPPeerVerify,
			Remotes:           []RemoteLeafOptsVarz{},
			WriteDeadline:     opts.Cluster.WriteDeadline,
			WriteTimeout:      opts.Cluster.WriteTimeout.String(),
		},
		MQTT: MQTTOptsVarz{
			Host:              mqtt.Host,
			Port:              mqtt.Port,
			NoAuthUser:        mqtt.NoAuthUser,
			AuthTimeout:       mqtt.AuthTimeout,
			TLSMap:            mqtt.TLSMap,
			TLSTimeout:        mqtt.TLSTimeout,
			JsDomain:          mqtt.JsDomain,
			AckWait:           mqtt.AckWait,
			MaxAckPending:     mqtt.MaxAckPending,
			TLSOCSPPeerVerify: mqttTlsOCSPPeerVerify,
		},
		Websocket: WebsocketOptsVarz{
			Host:              ws.Host,
			Port:              ws.Port,
			Advertise:         ws.Advertise,
			NoAuthUser:        ws.NoAuthUser,
			JWTCookie:         ws.JWTCookie,
			AuthTimeout:       ws.AuthTimeout,
			NoTLS:             ws.NoTLS,
			TLSMap:            ws.TLSMap,
			SameOrigin:        ws.SameOrigin,
			AllowedOrigins:    copyStrings(ws.AllowedOrigins),
			Compression:       ws.Compression,
			HandshakeTimeout:  ws.HandshakeTimeout,
			TLSOCSPPeerVerify: wsTlsOCSPPeerVerify,
		},
		Start:                 s.start.UTC(),
		MaxSubs:               opts.MaxSubs,
		Cores:                 runtime.NumCPU(),
		MaxProcs:              runtime.GOMAXPROCS(0),
		TrustedOperatorsJwt:   opts.operatorJWT,
		TrustedOperatorsClaim: opts.TrustedOperators,
	}
	if mm := debug.SetMemoryLimit(-1); mm < math.MaxInt64 {
		varz.MemLimit = mm
	}
	// If this is a leaf without cluster, reset the cluster name (that is otherwise
	// set to the server name).
	if s.leafNoCluster {
		varz.Cluster.Name = _EMPTY_
	}
	if len(opts.Routes) > 0 {
		varz.Cluster.URLs = urlsToStrings(opts.Routes)
	}
	if l := len(gw.Gateways); l > 0 {
		rgwa := make([]RemoteGatewayOptsVarz, l)
		for i, r := range gw.Gateways {
			rgwa[i] = RemoteGatewayOptsVarz{
				Name:       r.Name,
				TLSTimeout: r.TLSTimeout,
			}
		}
		varz.Gateway.Gateways = rgwa
	}
	// Finish setting it up with fields that can be updated during
	// configuration reload and runtime.
	s.updateVarzConfigReloadableFields(varz)
	s.updateVarzRuntimeFields(varz, true, pcpu, rss)
	return varz
}

// Builds the list of remote leafnodes for Varz from the given options.
// Server lock is held on entry.
func (s *Server) varzLeafNodeRemotes(opts *Options) []RemoteLeafOptsVarz {
	l := len(opts.LeafNode.Remotes)
	if l == 0 {
		return nil
	}
	rlna := make([]RemoteLeafOptsVarz, l)
	for i, r := range opts.LeafNode.Remotes {
		var deny *DenyRules
		if len(r.DenyImports) > 0 || len(r.DenyExports) > 0 {
			deny = &DenyRules{
				Imports: r.DenyImports,
				Exports: r.DenyExports,
			}
		}
		remoteTlsOCSPPeerVerify := s.ocspPeerVerify && r.tlsConfigOpts != nil && r.tlsConfigOpts.OCSPPeerConfig != nil && r.tlsConfigOpts.OCSPPeerConfig.Verify

		rlna[i] = RemoteLeafOptsVarz{
			LocalAccount:      r.LocalAccount,
			URLs:              urlsToStrings(r.URLs),
			TLSTimeout:        r.TLSTimeout,
			Deny:              deny,
			TLSOCSPPeerVerify: remoteTlsOCSPPeerVerify,
		}
	}
	return rlna
}

func urlsToStrings(urls []*url.URL) []string {
	sURLs := make([]string, len(urls))
	for i, u := range urls {
		sURLs[i] = u.Host
	}
	return sURLs
}

// Invoked during configuration reload once options have possibly be changed
// and config load time has been set. If s.varz has not been initialized yet
// (because no pooling of /varz has been made), this function does nothing.
// Server lock is held on entry.
func (s *Server) updateVarzConfigReloadableFields(v *Varz) {
	if v == nil {
		return
	}
	opts := s.getOpts()
	info := &s.info
	v.AuthRequired = info.AuthRequired
	v.TLSRequired = info.TLSRequired
	v.TLSVerify = info.TLSVerify
	v.MaxConn = opts.MaxConn
	v.PingInterval = opts.PingInterval
	v.MaxPingsOut = opts.MaxPingsOut
	v.AuthTimeout = opts.AuthTimeout
	v.MaxControlLine = opts.MaxControlLine
	v.MaxPayload = int(opts.MaxPayload)
	v.MaxPending = opts.MaxPending
	v.TLSTimeout = opts.TLSTimeout
	v.WriteDeadline = opts.WriteDeadline
	v.WriteTimeout = opts.WriteTimeout.String()
	v.ConfigLoadTime = s.configTime.UTC()
	v.ConfigDigest = opts.configDigest
	v.Tags = opts.Tags
	v.Metadata = opts.Metadata
	v.FeatureFlags = opts.getMergedFeatureFlags()
	// Update route URLs if applicable
	if s.varzUpdateRouteURLs {
		v.Cluster.URLs = urlsToStrings(opts.Routes)
		s.varzUpdateRouteURLs = false
	}
	if s.sys != nil && s.sys.account != nil {
		v.SystemAccount = s.sys.account.GetName()
	}
	v.MQTT.TLSPinnedCerts = getPinnedCertsAsSlice(opts.MQTT.TLSPinnedCerts)
	v.Websocket.TLSPinnedCerts = getPinnedCertsAsSlice(opts.Websocket.TLSPinnedCerts)

	v.TLSOCSPPeerVerify = s.ocspPeerVerify && v.TLSRequired && s.opts.tlsConfigOpts != nil && s.opts.tlsConfigOpts.OCSPPeerConfig != nil && s.opts.tlsConfigOpts.OCSPPeerConfig.Verify

	v.TLSCertNotAfter = tlsCertNotAfter(opts.TLSConfig)
	v.Cluster.TLSCertNotAfter = tlsCertNotAfter(opts.Cluster.TLSConfig)
	v.Gateway.TLSCertNotAfter = tlsCertNotAfter(opts.Gateway.TLSConfig)
	v.LeafNode.TLSCertNotAfter = tlsCertNotAfter(opts.LeafNode.TLSConfig)
	v.LeafNode.Remotes = s.varzLeafNodeRemotes(opts)
	v.MQTT.TLSCertNotAfter = tlsCertNotAfter(opts.MQTT.TLSConfig)
	v.Websocket.TLSCertNotAfter = tlsCertNotAfter(opts.Websocket.TLSConfig)

	if opts.Proxies != nil {
		if v.Proxies == nil {
			v.Proxies = &ProxiesOptsVarz{}
		}
		trusted := make([]*ProxyOptsVarz, 0, len(opts.Proxies.Trusted))
		for _, t := range opts.Proxies.Trusted {
			trusted = append(trusted, &ProxyOptsVarz{Key: t.Key})
		}
		v.Proxies.Trusted = trusted
	} else {
		v.Proxies = nil
	}

	if cfg := v.JetStream.Config; cfg != nil {
		if opts.JetStreamMaxMemory > 0 {
			cfg.MaxMemory = opts.JetStreamMaxMemory
		}
		if opts.JetStreamMaxStore > 0 {
			cfg.MaxStore = opts.JetStreamMaxStore
		}
	}
}

func getPinnedCertsAsSlice(certs PinnedCertSet) []string {
	if len(certs) == 0 {
		return nil
	}
	res := make([]string, 0, len(certs))
	for cn := range certs {
		res = append(res, cn)
	}
	return res
}

// Updates the runtime Varz fields, that is, fields that change during
// runtime and that should be updated any time Varz() or polling of /varz
// is done.
// Server lock is held on entry.
func (s *Server) updateVarzRuntimeFields(v *Varz, forceUpdate bool, pcpu float64, rss int64) {
	v.Now = time.Now().UTC()
	v.Uptime = myUptime(time.Since(s.start))
	v.Mem = rss
	v.CPU = pcpu
	if l := len(s.info.ClientConnectURLs); l > 0 {
		v.ClientConnectURLs = append([]string(nil), s.info.ClientConnectURLs...)
	}
	if l := len(s.info.WSConnectURLs); l > 0 {
		v.WSConnectURLs = append([]string(nil), s.info.WSConnectURLs...)
	}
	v.Connections = len(s.clients)
	v.TotalConnections = s.totalClients
	v.Routes = s.numRoutes()
	v.Remotes = s.numRemotes()
	v.Leafs = len(s.leafs)
	v.InMsgs = atomic.LoadInt64(&s.inMsgs)
	v.InBytes = atomic.LoadInt64(&s.inBytes)
	v.OutMsgs = atomic.LoadInt64(&s.outMsgs)
	v.OutBytes = atomic.LoadInt64(&s.outBytes)
	v.InClientMsgs = atomic.LoadInt64(&s.inClientMsgs)
	v.InClientBytes = atomic.LoadInt64(&s.inClientBytes)
	v.OutClientMsgs = atomic.LoadInt64(&s.outClientMsgs)
	v.OutClientBytes = atomic.LoadInt64(&s.outClientBytes)
	v.SlowConsumers = atomic.LoadInt64(&s.slowConsumers)
	v.StalledClients = atomic.LoadInt64(&s.stalls)
	v.SlowConsumersStats = &SlowConsumersStats{
		Clients:  s.NumSlowConsumersClients(),
		Routes:   s.NumSlowConsumersRoutes(),
		Gateways: s.NumSlowConsumersGateways(),
		Leafs:    s.NumSlowConsumersLeafs(),
	}
	v.StaleConnections = atomic.LoadInt64(&s.staleConnections)
	v.StaleConnectionStats = &StaleConnectionStats{
		Clients:  s.NumStaleConnectionsClients(),
		Routes:   s.NumStaleConnectionsRoutes(),
		Gateways: s.NumStaleConnectionsGateways(),
		Leafs:    s.NumStaleConnectionsLeafs(),
	}
	v.PinnedAccountFail = atomic.LoadUint64(&s.pinnedAccFail)

	// Make sure to reset in case we are re-using.
	v.Subscriptions = 0
	s.accounts.Range(func(k, val any) bool {
		acc := val.(*Account)
		v.Subscriptions += acc.sl.Count()
		return true
	})

	v.HTTPReqStats = make(map[string]uint64, len(s.httpReqStats))
	for key, val := range s.httpReqStats {
		v.HTTPReqStats[key] = val
	}

	// Update Gateway remote urls if applicable
	gw := s.gateway
	gw.RLock()
	if gw.enabled {
		for i := 0; i < len(v.Gateway.Gateways); i++ {
			g := &v.Gateway.Gateways[i]
			rgw := gw.remotes[g.Name]
			if rgw != nil {
				rgw.RLock()
				// forceUpdate is needed if user calls Varz() programmatically,
				// since we need to create a new instance every time and the
				// gateway's varzUpdateURLs may have been set to false after
				// a web /varz inspection.
				if forceUpdate || rgw.varzUpdateURLs {
					// Make reuse of backend array
					g.URLs = g.URLs[:0]
					// rgw.urls is a map[string]*url.URL where the key is
					// already in the right format (host:port, without any
					// user info present).
					for u := range rgw.urls {
						g.URLs = append(g.URLs, u)
					}
					rgw.varzUpdateURLs = false
				}
				rgw.RUnlock()
			} else if g.Name == gw.name && len(gw.ownCfgURLs) > 0 {
				// This is a remote that correspond to this very same server.
				// We report the URLs that were configured (if any).
				// Since we don't support changes to the gateway configuration
				// at this time, we could do this only if g.URLs has not been already
				// set, but let's do it regardless in case we add support for
				// gateway config reload.
				g.URLs = g.URLs[:0]
				g.URLs = append(g.URLs, gw.ownCfgURLs...)
			}
		}
	}
	gw.RUnlock()

	if s.ocsprc != nil && s.ocsprc.Type() != "none" {
		stats := s.ocsprc.Stats()
		if stats != nil {
			v.OCSPResponseCache = &OCSPResponseCacheVarz{
				s.ocsprc.Type(),
				stats.Hits,
				stats.Misses,
				stats.Responses,
				stats.Revokes,
				stats.Goods,
				stats.Unknowns,
			}
		}
	}
	v.DiskIOWaitStats = diskIOWaitStats(s.dios)
}

func diskIOWaitStats(d *diskIOSemaphore) *DiskIOWaitStats {
	if d == nil {
		return &DiskIOWaitStats{}
	}
	return &DiskIOWaitStats{
		Waiters:     d.waiters.Load(),
		Waits:       d.waits.Load(),
		WaitTime:    d.waitNanos.Load(),
		MaxWaitTime: d.maxWaitNanos.Load(),
	}
}

// HandleVarz will process HTTP requests for server information.
func (s *Server) HandleVarz(w http.ResponseWriter, r *http.Request) {
	var rss, vss int64
	var pcpu float64

	// We want to do that outside of the lock.
	pse.ProcUsage(&pcpu, &rss, &vss)

	// In response to http requests, we want to minimize mem copies
	// so we use an object stored in the server. Creating/collecting
	// server metrics is done under server lock, but we don't want
	// to marshal under that lock. Still, we need to prevent concurrent
	// http requests to /varz to update s.varz while marshal is
	// happening, so we need a new lock that serialize those http
	// requests and include marshaling.
	s.varzMu.Lock()

	// Use server lock to create/update the server's varz object.
	s.mu.Lock()
	var created bool
	s.httpReqStats[VarzPath]++
	if s.varz == nil {
		s.varz = s.createVarz(pcpu, rss)
		created = true
	} else {
		s.updateVarzRuntimeFields(s.varz, false, pcpu, rss)
	}
	s.mu.Unlock()
	// Since locking is jetStream -> Server, need to update jetstream
	// varz outside of server lock.

	if js := s.getJetStream(); js != nil {
		var v JetStreamVarz
		// Work on stack variable
		s.updateJszVarz(js, &v, created)
		// Now update server's varz
		s.mu.RLock()
		sv := &s.varz.JetStream
		if created {
			sv.Config = v.Config
		}
		sv.Stats = v.Stats
		sv.Meta = v.Meta
		sv.Limits = v.Limits
		s.mu.RUnlock()
	}

	// Do the marshaling outside of server lock, but under varzMu lock.
	b, err := json.MarshalIndent(s.varz, "", "  ")
	s.varzMu.Unlock()

	if err != nil {
		s.Errorf("Error marshaling response to /varz request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

// GatewayzOptions are the options passed to Gatewayz()
type GatewayzOptions struct {
	// Name will output only remote gateways with this name
	Name string `json:"name"`

	// Accounts indicates if accounts with its interest should be included in the results.
	Accounts bool `json:"accounts"`

	// AccountName will limit the list of accounts to that account name (makes Accounts implicit)
	AccountName string `json:"account_name"`

	// AccountSubscriptions indicates if subscriptions should be included in the results.
	// Note: This is used only if `Accounts` or `AccountName` are specified.
	AccountSubscriptions bool `json:"subscriptions"`

	// AccountSubscriptionsDetail indicates if subscription details should be included in the results.
	// Note: This is used only if `Accounts` or `AccountName` are specified.
	AccountSubscriptionsDetail bool `json:"subscriptions_detail"`
}

// Gatewayz represents detailed information on Gateways
type Gatewayz struct {
	ID               string                       `json:"server_id"`
	Now              time.Time                    `json:"now"`
	Name             string                       `json:"name,omitempty"`
	Host             string                       `json:"host,omitempty"`
	Port             int                          `json:"port,omitempty"`
	OutboundGateways map[string]*RemoteGatewayz   `json:"outbound_gateways"`
	InboundGateways  map[string][]*RemoteGatewayz `json:"inbound_gateways"`
}

// RemoteGatewayz represents information about an outbound connection to a gateway
type RemoteGatewayz struct {
	IsConfigured bool               `json:"configured"`
	Connection   *ConnInfo          `json:"connection,omitempty"`
	Accounts     []*AccountGatewayz `json:"accounts,omitempty"`
}

// AccountGatewayz represents interest mode for this account
type AccountGatewayz struct {
	Name                  string      `json:"name"`
	InterestMode          string      `json:"interest_mode"`
	NoInterestCount       int         `json:"no_interest_count,omitempty"`
	InterestOnlyThreshold int         `json:"interest_only_threshold,omitempty"`
	TotalSubscriptions    int         `json:"num_subs,omitempty"`
	NumQueueSubscriptions int         `json:"num_queue_subs,omitempty"`
	Subs                  []string    `json:"subscriptions_list,omitempty"`
	SubsDetail            []SubDetail `json:"subscriptions_list_detail,omitempty"`
}

// Gatewayz returns a Gatewayz struct containing information about gateways.
func (s *Server) Gatewayz(opts *GatewayzOptions) (*Gatewayz, error) {
	srvID := s.ID()
	now := time.Now().UTC()
	gw := s.gateway
	gw.RLock()
	if !gw.enabled || gw.info == nil {
		gw.RUnlock()
		gwz := &Gatewayz{
			ID:               srvID,
			Now:              now,
			OutboundGateways: map[string]*RemoteGatewayz{},
			InboundGateways:  map[string][]*RemoteGatewayz{},
		}
		return gwz, nil
	}
	// Here gateways are enabled, so fill up more.
	gwz := &Gatewayz{
		ID:   srvID,
		Now:  now,
		Name: gw.name,
		Host: gw.info.Host,
		Port: gw.info.Port,
	}
	gw.RUnlock()

	gwz.OutboundGateways = s.createOutboundsRemoteGatewayz(opts, now)
	gwz.InboundGateways = s.createInboundsRemoteGatewayz(opts, now)

	return gwz, nil
}

// Based on give options struct, returns if there is a filtered
// Gateway Name and if we should do report Accounts.
// Note that if Accounts is false but AccountName is not empty,
// then Accounts is implicitly set to true.
func getMonitorGWOptions(opts *GatewayzOptions) (string, bool) {
	var name string
	var accs bool
	if opts != nil {
		if opts.Name != _EMPTY_ {
			name = opts.Name
		}
		accs = opts.Accounts
		if !accs && opts.AccountName != _EMPTY_ {
			accs = true
		}
	}
	return name, accs
}

// Returns a map of gateways outbound connections.
// Based on options, will include a single or all gateways,
// with no/single/or all accounts interest information.
func (s *Server) createOutboundsRemoteGatewayz(opts *GatewayzOptions, now time.Time) map[string]*RemoteGatewayz {
	targetGWName, doAccs := getMonitorGWOptions(opts)

	if targetGWName != _EMPTY_ {
		c := s.getOutboundGatewayConnection(targetGWName)
		if c == nil {
			return nil
		}
		outbounds := make(map[string]*RemoteGatewayz, 1)
		_, rgw := createOutboundRemoteGatewayz(c, opts, now, doAccs)
		outbounds[targetGWName] = rgw
		return outbounds
	}

	var connsa [16]*client
	var conns = connsa[:0]

	s.getOutboundGatewayConnections(&conns)

	outbounds := make(map[string]*RemoteGatewayz, len(conns))
	for _, c := range conns {
		name, rgw := createOutboundRemoteGatewayz(c, opts, now, doAccs)
		if rgw != nil {
			outbounds[name] = rgw
		}
	}
	return outbounds
}

// Returns a RemoteGatewayz for a given outbound gw connection
func createOutboundRemoteGatewayz(c *client, opts *GatewayzOptions, now time.Time, doAccs bool) (string, *RemoteGatewayz) {
	var name string
	var rgw *RemoteGatewayz

	c.mu.Lock()
	if c.gw != nil {
		rgw = &RemoteGatewayz{}
		if doAccs {
			rgw.Accounts = createOutboundAccountsGatewayz(opts, c.gw)
		}
		if c.gw.cfg != nil {
			rgw.IsConfigured = !c.gw.cfg.isImplicit()
		}
		rgw.Connection = &ConnInfo{}
		rgw.Connection.fill(c, c.nc, now, false)
		name = c.gw.name
	}
	c.mu.Unlock()

	return name, rgw
}

// Returns the list of accounts for this outbound gateway connection.
// Based on the options, it will be a single or all accounts for
// this outbound.
func createOutboundAccountsGatewayz(opts *GatewayzOptions, gw *gateway) []*AccountGatewayz {
	if gw.outsim == nil {
		return nil
	}

	var accName string
	if opts != nil {
		accName = opts.AccountName
	}
	if accName != _EMPTY_ {
		ei, ok := gw.outsim.Load(accName)
		if !ok {
			return nil
		}
		a := createAccountOutboundGatewayz(opts, accName, ei)
		return []*AccountGatewayz{a}
	}

	accs := make([]*AccountGatewayz, 0, 4)
	gw.outsim.Range(func(k, v any) bool {
		name := k.(string)
		a := createAccountOutboundGatewayz(opts, name, v)
		accs = append(accs, a)
		return true
	})
	return accs
}

// Returns an AccountGatewayz for this gateway outbound connection
func createAccountOutboundGatewayz(opts *GatewayzOptions, name string, ei any) *AccountGatewayz {
	a := &AccountGatewayz{
		Name:                  name,
		InterestOnlyThreshold: gatewayMaxRUnsubBeforeSwitch,
	}
	if ei != nil {
		e := ei.(*outsie)
		e.RLock()
		a.InterestMode = e.mode.String()
		a.NoInterestCount = len(e.ni)
		a.NumQueueSubscriptions = e.qsubs
		a.TotalSubscriptions = int(e.sl.Count())
		if opts.AccountSubscriptions || opts.AccountSubscriptionsDetail {
			var subsa [4096]*subscription
			subs := subsa[:0]
			e.sl.All(&subs)
			if opts.AccountSubscriptions {
				a.Subs = make([]string, 0, len(subs))
			} else {
				a.SubsDetail = make([]SubDetail, 0, len(subs))
			}
			for _, sub := range subs {
				if opts.AccountSubscriptions {
					a.Subs = append(a.Subs, string(sub.subject))
				} else {
					a.SubsDetail = append(a.SubsDetail, newClientSubDetail(sub))
				}
			}
		}
		e.RUnlock()
	} else {
		a.InterestMode = Optimistic.String()
	}
	return a
}

// Returns a map of gateways inbound connections.
// Each entry is an array of RemoteGatewayz since a given server
// may have more than one inbound from the same remote gateway.
// Based on options, will include a single or all gateways,
// with no/single/or all accounts interest information.
func (s *Server) createInboundsRemoteGatewayz(opts *GatewayzOptions, now time.Time) map[string][]*RemoteGatewayz {
	targetGWName, doAccs := getMonitorGWOptions(opts)

	var connsa [16]*client
	var conns = connsa[:0]
	s.getInboundGatewayConnections(&conns)

	m := make(map[string][]*RemoteGatewayz)
	for _, c := range conns {
		c.mu.Lock()
		if c.gw != nil && (targetGWName == _EMPTY_ || targetGWName == c.gw.name) {
			igws := m[c.gw.name]
			if igws == nil {
				igws = make([]*RemoteGatewayz, 0, 2)
			}
			rgw := &RemoteGatewayz{}
			if doAccs {
				rgw.Accounts = createInboundAccountsGatewayz(opts, c.gw)
			}
			rgw.Connection = &ConnInfo{}
			rgw.Connection.fill(c, c.nc, now, false)
			igws = append(igws, rgw)
			m[c.gw.name] = igws
		}
		c.mu.Unlock()
	}
	return m
}

// Returns the list of accounts for this inbound gateway connection.
// Based on the options, it will be a single or all accounts for
// this inbound.
func createInboundAccountsGatewayz(opts *GatewayzOptions, gw *gateway) []*AccountGatewayz {
	if gw.insim == nil {
		return nil
	}

	var accName string
	if opts != nil {
		accName = opts.AccountName
	}
	if accName != _EMPTY_ {
		e, ok := gw.insim[accName]
		if !ok {
			return nil
		}
		a := createInboundAccountGatewayz(accName, e)
		return []*AccountGatewayz{a}
	}

	accs := make([]*AccountGatewayz, 0, 4)
	for name, e := range gw.insim {
		a := createInboundAccountGatewayz(name, e)
		accs = append(accs, a)
	}
	return accs
}

// Returns an AccountGatewayz for this gateway inbound connection
func createInboundAccountGatewayz(name string, e *insie) *AccountGatewayz {
	a := &AccountGatewayz{
		Name:                  name,
		InterestOnlyThreshold: gatewayMaxRUnsubBeforeSwitch,
	}
	if e != nil {
		a.InterestMode = e.mode.String()
		a.NoInterestCount = len(e.ni)
	} else {
		a.InterestMode = Optimistic.String()
	}
	return a
}

// HandleGatewayz process HTTP requests for route information.
func (s *Server) HandleGatewayz(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.httpReqStats[GatewayzPath]++
	s.mu.Unlock()

	subs, subsDet, err := decodeSubs(w, r)
	if err != nil {
		return
	}
	accs, err := decodeBool(w, r, "accs")
	if err != nil {
		return
	}
	gwName := r.URL.Query().Get("gw_name")
	accName := r.URL.Query().Get("acc_name")
	if accName != _EMPTY_ {
		accs = true
	}

	opts := &GatewayzOptions{
		Name:                       gwName,
		Accounts:                   accs,
		AccountName:                accName,
		AccountSubscriptions:       subs,
		AccountSubscriptionsDetail: subsDet,
	}
	gw, err := s.Gatewayz(opts)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	b, err := json.MarshalIndent(gw, "", "  ")
	if err != nil {
		s.Errorf("Error marshaling response to /gatewayz request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

// Leafz represents detailed information on Leafnodes.
type Leafz struct {
	ID       string      `json:"server_id"`
	Now      time.Time   `json:"now"`
	NumLeafs int         `json:"leafnodes"`
	Leafs    []*LeafInfo `json:"leafs"`
}

// LeafzOptions are options passed to Leafz
type LeafzOptions struct {
	// Subscriptions indicates that Leafz will return a leafnode's subscriptions
	Subscriptions bool   `json:"subscriptions"`
	Account       string `json:"account"`
}

// LeafInfo has detailed information on each remote leafnode connection.
type LeafInfo struct {
	ID          uint64     `json:"id"`
	Name        string     `json:"name"`
	IsSpoke     bool       `json:"is_spoke"`
	IsIsolated  bool       `json:"is_isolated,omitempty"`
	Account     string     `json:"account"`
	IP          string     `json:"ip"`
	Port        int        `json:"port"`
	RTT         string     `json:"rtt,omitempty"`
	InMsgs      int64      `json:"in_msgs"`
	OutMsgs     int64      `json:"out_msgs"`
	InBytes     int64      `json:"in_bytes"`
	OutBytes    int64      `json:"out_bytes"`
	NumSubs     uint32     `json:"subscriptions"`
	Subs        []string   `json:"subscriptions_list,omitempty"`
	Compression string     `json:"compression,omitempty"`
	Websocket   bool       `json:"websocket,omitempty"`
	Proxy       *ProxyInfo `json:"proxy,omitempty"`
}

// Leafz returns a Leafz structure containing information about leafnodes.
func (s *Server) Leafz(opts *LeafzOptions) (*Leafz, error) {
	// Grab leafnodes
	var lconns []*client
	s.mu.Lock()
	if len(s.leafs) > 0 {
		lconns = make([]*client, 0, len(s.leafs))
		for _, ln := range s.leafs {
			if opts != nil && opts.Account != _EMPTY_ {
				ln.mu.Lock()
				ok := ln.acc.Name == opts.Account
				ln.mu.Unlock()
				if !ok {
					continue
				}
			}
			lconns = append(lconns, ln)
		}
	}
	s.mu.Unlock()

	leafnodes := make([]*LeafInfo, 0, len(lconns))

	if len(lconns) > 0 {
		for _, ln := range lconns {
			ln.mu.Lock()
			lni := &LeafInfo{
				ID:          ln.cid,
				Name:        ln.leaf.remoteServer,
				IsSpoke:     ln.isSpokeLeafNode(),
				IsIsolated:  ln.leaf.isolated,
				Account:     ln.acc.Name,
				IP:          ln.host,
				Port:        int(ln.port),
				RTT:         ln.getRTT().String(),
				InMsgs:      atomic.LoadInt64(&ln.inMsgs),
				OutMsgs:     ln.outMsgs,
				InBytes:     atomic.LoadInt64(&ln.inBytes),
				OutBytes:    ln.outBytes,
				NumSubs:     uint32(len(ln.subs)),
				Compression: ln.leaf.compression,
				Websocket:   ln.isWebsocket(),
				Proxy:       createProxyInfo(ln),
			}
			if opts != nil && opts.Subscriptions {
				lni.Subs = make([]string, 0, len(ln.subs))
				for _, sub := range ln.subs {
					lni.Subs = append(lni.Subs, string(sub.subject))
				}
			}
			ln.mu.Unlock()
			leafnodes = append(leafnodes, lni)
		}
	}

	return &Leafz{
		ID:       s.ID(),
		Now:      time.Now().UTC(),
		NumLeafs: len(leafnodes),
		Leafs:    leafnodes,
	}, nil
}

// HandleLeafz process HTTP requests for leafnode information.
func (s *Server) HandleLeafz(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.httpReqStats[LeafzPath]++
	s.mu.Unlock()

	subs, err := decodeBool(w, r, "subs")
	if err != nil {
		return
	}
	l, err := s.Leafz(&LeafzOptions{subs, r.URL.Query().Get("acc")})
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	b, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		s.Errorf("Error marshaling response to /leafz request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

// Leafz represents detailed information on Leafnodes.
type AccountStatz struct {
	ID       string         `json:"server_id"`
	Now      time.Time      `json:"now"`
	Accounts []*AccountStat `json:"account_statz"`
}

// AccountStatzOptions are options passed to account stats requests.
type AccountStatzOptions struct {
	Accounts      []string `json:"accounts"`
	IncludeUnused bool     `json:"include_unused"`
}

// Leafz returns a AccountStatz structure containing summary information about accounts.
func (s *Server) AccountStatz(opts *AccountStatzOptions) (*AccountStatz, error) {
	stz := &AccountStatz{
		ID:       s.ID(),
		Now:      time.Now().UTC(),
		Accounts: []*AccountStat{},
	}
	if opts == nil || len(opts.Accounts) == 0 {
		s.accounts.Range(func(key, a any) bool {
			acc := a.(*Account)
			acc.mu.RLock()
			if (opts != nil && opts.IncludeUnused) || acc.numLocalConnections() != 0 || acc.numLocalLeafNodes() != 0 {
				stz.Accounts = append(stz.Accounts, acc.statz())
			}
			acc.mu.RUnlock()
			return true
		})
	} else {
		for _, a := range opts.Accounts {
			if acc, ok := s.accounts.Load(a); ok {
				acc := acc.(*Account)
				acc.mu.RLock()
				if opts.IncludeUnused || acc.numLocalConnections() != 0 || acc.numLocalLeafNodes() != 0 {
					stz.Accounts = append(stz.Accounts, acc.statz())
				}
				acc.mu.RUnlock()
			}
		}
	}
	return stz, nil
}

// HandleAccountStatz process HTTP requests for statz information of all accounts.
func (s *Server) HandleAccountStatz(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.httpReqStats[AccountStatzPath]++
	s.mu.Unlock()

	unused, err := decodeBool(w, r, "unused")
	if err != nil {
		return
	}

	l, err := s.AccountStatz(&AccountStatzOptions{IncludeUnused: unused})
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	b, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		s.Errorf("Error marshaling response to %s request: %v", AccountStatzPath, err)
		return
	}

	// Handle response
	ResponseHandler(w, r, b)
}

// ResponseHandler handles responses for monitoring routes.
func ResponseHandler(w http.ResponseWriter, r *http.Request, data []byte) {
	handleResponse(http.StatusOK, w, r, data)
}

// handleResponse handles responses for monitoring routes with a specific HTTP status code.
func handleResponse(code int, w http.ResponseWriter, _ *http.Request, data []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(code)
	w.Write(data)
}

func (reason ClosedState) String() string {
	switch reason {
	case ClientClosed:
		return "Client Closed"
	case AuthenticationTimeout:
		return "Authentication Timeout"
	case AuthenticationViolation:
		return "Authentication Failure"
	case TLSHandshakeError:
		return "TLS Handshake Failure"
	case SlowConsumerPendingBytes:
		return "Slow Consumer (Pending Bytes)"
	case SlowConsumerWriteDeadline:
		return "Slow Consumer (Write Deadline)"
	case WriteError:
		return "Write Error"
	case ReadError:
		return "Read Error"
	case ParseError:
		return "Parse Error"
	case StaleConnection:
		return "Stale Connection"
	case ProtocolViolation:
		return "Protocol Violation"
	case BadClientProtocolVersion:
		return "Bad Client Protocol Version"
	case WrongPort:
		return "Incorrect Port"
	case MaxConnectionsExceeded:
		return "Maximum Connections Exceeded"
	case MaxAccountConnectionsExceeded:
		return "Maximum Account Connections Exceeded"
	case MaxPayloadExceeded:
		return "Maximum Message Payload Exceeded"
	case MaxControlLineExceeded:
		return "Maximum Control Line Exceeded"
	case MaxSubscriptionsExceeded:
		return "Maximum Subscriptions Exceeded"
	case DuplicateRoute:
		return "Duplicate Route"
	case RouteRemoved:
		return "Route Removed"
	case ServerShutdown:
		return "Server Shutdown"
	case AuthenticationExpired:
		return "Authentication Expired"
	case WrongGateway:
		return "Wrong Gateway"
	case MissingAccount:
		return "Missing Account"
	case Revocation:
		return "Credentials Revoked"
	case InternalClient:
		return "Internal Client"
	case MsgHeaderViolation:
		return "Message Header Violation"
	case NoRespondersRequiresHeaders:
		return "No Responders Requires Headers"
	case ClusterNameConflict:
		return "Cluster Name Conflict"
	case DuplicateRemoteLeafnodeConnection:
		return "Duplicate Remote LeafNode Connection"
	case DuplicateClientID:
		return "Duplicate Client ID"
	case DuplicateServerName:
		return "Duplicate Server Name"
	case MinimumVersionRequired:
		return "Minimum Version Required"
	case ClusterNamesIdentical:
		return "Cluster Names Identical"
	case Kicked:
		return "Kicked"
	case ProxyNotTrusted:
		return "Proxy Not Trusted"
	case ProxyRequired:
		return "Proxy Required"
	}

	return "Unknown State"
}

// AccountzOptions are options passed to Accountz
type AccountzOptions struct {
	// Account indicates that Accountz will return details for the account
	Account string `json:"account"`
}

func newExtServiceLatency(l *serviceLatency) *jwt.ServiceLatency {
	if l == nil {
		return nil
	}
	return &jwt.ServiceLatency{
		Sampling: jwt.SamplingRate(l.sampling),
		Results:  jwt.Subject(l.subject),
	}
}

type ExtImport struct {
	jwt.Import
	Invalid     bool                `json:"invalid"`
	Share       bool                `json:"share"`
	Tracking    bool                `json:"tracking"`
	TrackingHdr http.Header         `json:"tracking_header,omitempty"`
	Latency     *jwt.ServiceLatency `json:"latency,omitempty"`
	M1          *ServiceLatency     `json:"m1,omitempty"`
}

type ExtExport struct {
	jwt.Export
	ApprovedAccounts []string             `json:"approved_accounts,omitempty"`
	RevokedAct       map[string]time.Time `json:"revoked_activations,omitempty"`
}

type ExtVrIssues struct {
	Description string `json:"description"`
	Blocking    bool   `json:"blocking"`
	Time        bool   `json:"time_check"`
}

type ExtMap map[string][]*MapDest

type AccountInfo struct {
	AccountName string               `json:"account_name"`
	LastUpdate  time.Time            `json:"update_time,omitempty"`
	IsSystem    bool                 `json:"is_system,omitempty"`
	Expired     bool                 `json:"expired"`
	Complete    bool                 `json:"complete"`
	JetStream   bool                 `json:"jetstream_enabled"`
	LeafCnt     int                  `json:"leafnode_connections"`
	ClientCnt   int                  `json:"client_connections"`
	SubCnt      uint32               `json:"subscriptions"`
	Mappings    ExtMap               `json:"mappings,omitempty"`
	Exports     []ExtExport          `json:"exports,omitempty"`
	Imports     []ExtImport          `json:"imports,omitempty"`
	Jwt         string               `json:"jwt,omitempty"`
	IssuerKey   string               `json:"issuer_key,omitempty"`
	NameTag     string               `json:"name_tag,omitempty"`
	Tags        jwt.TagList          `json:"tags,omitempty"`
	Claim       *jwt.AccountClaims   `json:"decoded_jwt,omitempty"`
	Vr          []ExtVrIssues        `json:"validation_result_jwt,omitempty"`
	RevokedUser map[string]time.Time `json:"revoked_user,omitempty"`
	Sublist     *SublistStats        `json:"sublist_stats,omitempty"`
	Responses   map[string]ExtImport `json:"responses,omitempty"`
}

type Accountz struct {
	ID            string       `json:"server_id"`
	Now           time.Time    `json:"now"`
	SystemAccount string       `json:"system_account,omitempty"`
	Accounts      []string     `json:"accounts,omitempty"`
	Account       *AccountInfo `json:"account_detail,omitempty"`
}

// HandleAccountz process HTTP requests for account information.
func (s *Server) HandleAccountz(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.httpReqStats[AccountzPath]++
	s.mu.Unlock()
	if l, err := s.Accountz(&AccountzOptions{r.URL.Query().Get("acc")}); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
	} else if b, err := json.MarshalIndent(l, "", "  "); err != nil {
		s.Errorf("Error marshaling response to %s request: %v", AccountzPath, err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
	} else {
		ResponseHandler(w, r, b) // Handle response
	}
}

func (s *Server) Accountz(optz *AccountzOptions) (*Accountz, error) {
	a := &Accountz{
		ID:  s.ID(),
		Now: time.Now().UTC(),
	}
	if sacc := s.SystemAccount(); sacc != nil {
		a.SystemAccount = sacc.GetName()
	}
	if optz == nil || optz.Account == _EMPTY_ {
		a.Accounts = []string{}
		s.accounts.Range(func(key, value any) bool {
			a.Accounts = append(a.Accounts, key.(string))
			return true
		})
		return a, nil
	}
	aInfo, err := s.accountInfo(optz.Account)
	if err != nil {
		return nil, err
	}
	a.Account = aInfo
	return a, nil
}

func newExtImport(v *serviceImport) ExtImport {
	imp := ExtImport{
		Invalid: true,
		Import:  jwt.Import{Type: jwt.Service},
	}
	if v != nil {
		imp.Share = v.share
		imp.Tracking = v.tracking
		imp.Invalid = v.invalid
		imp.Import = jwt.Import{
			Subject: jwt.Subject(v.to),
			Account: v.acc.Name,
			Type:    jwt.Service,
			// Deprecated so we duplicate. Use LocalSubject.
			To:           jwt.Subject(v.from),
			LocalSubject: jwt.RenamingSubject(v.from),
		}
		imp.TrackingHdr = v.trackingHdr
		imp.Latency = newExtServiceLatency(v.latency)
		if v.m1 != nil {
			m1 := *v.m1
			imp.M1 = &m1
		}
	}
	return imp
}

func (s *Server) accountInfo(accName string) (*AccountInfo, error) {
	var a *Account
	if v, ok := s.accounts.Load(accName); !ok {
		return nil, fmt.Errorf("Account %s does not exist", accName)
	} else {
		a = v.(*Account)
	}
	isSys := a == s.SystemAccount()
	a.mu.RLock()
	defer a.mu.RUnlock()
	var vrIssues []ExtVrIssues
	claim, _ := jwt.DecodeAccountClaims(a.claimJWT) // ignore error
	if claim != nil {
		vr := jwt.ValidationResults{}
		claim.Validate(&vr)
		vrIssues = make([]ExtVrIssues, len(vr.Issues))
		for i, v := range vr.Issues {
			vrIssues[i] = ExtVrIssues{v.Description, v.Blocking, v.TimeCheck}
		}
	}
	collectRevocations := func(revocations map[string]int64) map[string]time.Time {
		l := len(revocations)
		if l == 0 {
			return nil
		}
		rev := make(map[string]time.Time, l)
		for k, v := range revocations {
			rev[k] = time.Unix(v, 0)
		}
		return rev
	}
	exports := []ExtExport{}
	for k, v := range a.exports.services {
		e := ExtExport{
			Export: jwt.Export{
				Subject: jwt.Subject(k),
				Type:    jwt.Service,
			},
			ApprovedAccounts: []string{},
		}
		if v != nil {
			e.Latency = newExtServiceLatency(v.latency)
			e.TokenReq = v.tokenReq
			e.ResponseType = jwt.ResponseType(v.respType.String())
			for name := range v.approved {
				e.ApprovedAccounts = append(e.ApprovedAccounts, name)
			}
			e.RevokedAct = collectRevocations(v.actsRevoked)
		}
		exports = append(exports, e)
	}
	for k, v := range a.exports.streams {
		e := ExtExport{
			Export: jwt.Export{
				Subject: jwt.Subject(k),
				Type:    jwt.Stream,
			},
			ApprovedAccounts: []string{},
		}
		if v != nil {
			e.TokenReq = v.tokenReq
			for name := range v.approved {
				e.ApprovedAccounts = append(e.ApprovedAccounts, name)
			}
			e.RevokedAct = collectRevocations(v.actsRevoked)
		}
		exports = append(exports, e)
	}
	imports := []ExtImport{}
	for _, v := range a.imports.streams {
		imp := ExtImport{
			Invalid: true,
			Import:  jwt.Import{Type: jwt.Stream},
		}
		if v != nil {
			imp.Invalid = v.invalid
			imp.Import = jwt.Import{
				Subject:      jwt.Subject(v.from),
				Account:      v.acc.Name,
				Type:         jwt.Stream,
				LocalSubject: jwt.RenamingSubject(v.to),
			}
		}
		imports = append(imports, imp)
	}
	for _, sis := range a.imports.services {
		for _, v := range sis {
			imports = append(imports, newExtImport(v))
		}
	}
	responses := map[string]ExtImport{}
	for k, v := range a.exports.responses {
		responses[k] = newExtImport(v)
	}
	mappings := ExtMap{}
	for _, m := range a.mappings {
		var dests []*MapDest
		var src string
		if m == nil {
			src = "nil"
			if _, ok := mappings[src]; ok { // only set if not present (keep orig in case nil is used)
				continue
			}
			dests = append(dests, &MapDest{})
		} else {
			src = m.src
			for _, d := range m.dests {
				dests = append(dests, &MapDest{d.tr.dest, d.weight, _EMPTY_})
			}
			for c, cd := range m.cdests {
				for _, d := range cd {
					dests = append(dests, &MapDest{d.tr.dest, d.weight, c})
				}
			}
		}
		mappings[src] = dests
	}
	return &AccountInfo{
		AccountName: accName,
		LastUpdate:  a.updated.UTC(),
		IsSystem:    isSys,
		Expired:     a.expired.Load(),
		Complete:    !a.incomplete,
		JetStream:   a.js != nil,
		LeafCnt:     a.numLocalLeafNodes(),
		ClientCnt:   a.numLocalConnections(),
		SubCnt:      a.sl.Count(),
		Mappings:    mappings,
		Exports:     exports,
		Imports:     imports,
		Jwt:         a.claimJWT,
		IssuerKey:   a.Issuer,
		NameTag:     a.getNameTagLocked(),
		Tags:        a.tags,
		Claim:       claim,
		Vr:          vrIssues,
		RevokedUser: collectRevocations(a.usersRevoked),
		Sublist:     a.sl.Stats(),
		Responses:   responses,
	}, nil
}

// JSzOptions are options passed to Jsz
type JSzOptions struct {
	Account          string `json:"account,omitempty"`
	Accounts         bool   `json:"accounts,omitempty"`
	Streams          bool   `json:"streams,omitempty"`
	Consumer         bool   `json:"consumer,omitempty"`
	DirectConsumer   bool   `json:"direct_consumer,omitempty"`
	Config           bool   `json:"config,omitempty"`
	LeaderOnly       bool   `json:"leader_only,omitempty"`
	Offset           int    `json:"offset,omitempty"`
	Limit            int    `json:"limit,omitempty"`
	RaftGroups       bool   `json:"raft,omitempty"`
	StreamLeaderOnly bool   `json:"stream_leader_only,omitempty"`
}

// HealthzOptions are options passed to Healthz
type HealthzOptions struct {
	// Deprecated: Use JSEnabledOnly instead
	JSEnabled     bool   `json:"js-enabled,omitempty"`
	JSEnabledOnly bool   `json:"js-enabled-only,omitempty"`
	JSServerOnly  bool   `json:"js-server-only,omitempty"`
	JSMetaOnly    bool   `json:"js-meta-only,omitempty"`
	LeafRemotes   bool   `json:"leaf-remotes,omitempty"`
	Account       string `json:"account,omitempty"`
	Stream        string `json:"stream,omitempty"`
	Consumer      string `json:"consumer,omitempty"`
	Details       bool   `json:"details,omitempty"`
}

// ProfilezOptions are options passed to Profilez
type ProfilezOptions struct {
	Name     string        `json:"name"`
	Debug    int           `json:"debug"`
	Duration time.Duration `json:"duration,omitempty"`
}

// IpqueueszOptions are options passed to Ipqueuesz
type IpqueueszOptions struct {
	All    bool   `json:"all"`
	Filter string `json:"filter"`
}

// RaftzOptions are options passed to Raftz
type RaftzOptions struct {
	AccountFilter string `json:"account"`
	GroupFilter   string `json:"group"`
}

// StreamDetail shows information about the stream state and its consumers.
type StreamDetail struct {
	Name               string              `json:"name"`
	Created            time.Time           `json:"created"`
	Cluster            *ClusterInfo        `json:"cluster,omitempty"`
	Config             *StreamConfig       `json:"config,omitempty"`
	State              StreamState         `json:"state,omitempty"`
	Consumer           []*ConsumerInfo     `json:"consumer_detail,omitempty"`
	DirectConsumer     []*ConsumerInfo     `json:"direct_consumer_detail,omitempty"`
	Mirror             *StreamSourceInfo   `json:"mirror,omitempty"`
	Sources            []*StreamSourceInfo `json:"sources,omitempty"`
	RaftGroup          string              `json:"stream_raft_group,omitempty"`
	ConsumerRaftGroups []*RaftGroupDetail  `json:"consumer_raft_groups,omitempty"`
}

// RaftGroupDetail shows information details about the Raft group.
type RaftGroupDetail struct {
	Name      string `json:"name"`
	RaftGroup string `json:"raft_group,omitempty"`
}

type AccountDetail struct {
	Name string `json:"name"`
	Id   string `json:"id"`
	JetStreamStats
	Streams []StreamDetail `json:"stream_detail,omitempty"`
}

// MetaSnapshotStats shows information about meta snapshots.
type MetaSnapshotStats struct {
	PendingEntries uint64        `json:"pending_entries"`         // PendingEntries is the count of pending entries in the meta layer
	PendingSize    uint64        `json:"pending_size"`            // PendingSize is the size in bytes of pending entries in the meta layer
	LastTime       time.Time     `json:"last_time,omitempty"`     // LastTime is when the last meta snapshot was taken
	LastDuration   time.Duration `json:"last_duration,omitempty"` // LastDuration is how long the last meta snapshot took
}

// metaClusterSnapshotStats returns snapshot statistics for the meta group.
func (s *Server) metaClusterSnapshotStats(js *jetStream, mg RaftNode) *MetaSnapshotStats {
	entries, bytes := mg.Size()
	snap := &MetaSnapshotStats{
		PendingEntries: entries,
		PendingSize:    bytes,
	}

	js.mu.RLock()
	cluster := js.cluster
	js.mu.RUnlock()

	if cluster != nil {
		timeNanos := atomic.LoadInt64(&cluster.lastMetaSnapTime)
		durationNanos := atomic.LoadInt64(&cluster.lastMetaSnapDuration)
		if timeNanos > 0 {
			snap.LastTime = time.Unix(0, timeNanos).UTC()
		}
		if durationNanos > 0 {
			snap.LastDuration = time.Duration(durationNanos)
		}
	}

	return snap
}

// MetaClusterInfo shows information about the meta group.
type MetaClusterInfo struct {
	Name            string             `json:"name,omitempty"`     // Name is the name of the cluster
	Leader          string             `json:"leader,omitempty"`   // Leader is the server name of the cluster leader
	Peer            string             `json:"peer,omitempty"`     // Peer is unique ID of the leader
	Replicas        []*PeerInfo        `json:"replicas,omitempty"` // Replicas is a list of known peers
	Size            int                `json:"cluster_size"`       // Size is the known size of the cluster
	QuorumNeeded    int                `json:"quorum_needed"`      // QuorumNeeded is this server's current effective quorum size
	Rescue          bool               `json:"rescue,omitempty"`   // Rescue indicates the quorum is unsafely lowered by an active rescue
	Pending         int                `json:"pending"`            // Pending is how many RAFT messages are not yet processed
	PendingRequests int                `json:"pending_requests"`   // PendingRequests is how many CRUD operations are queued for processing
	PendingInfos    int                `json:"pending_infos"`      // PendingInfos is how many info operations are queued for processing
	Snapshot        *MetaSnapshotStats `json:"snapshot"`           // Snapshot contains meta snapshot statistics
}

// JSInfo has detailed information on JetStream.
type JSInfo struct {
	JetStreamStats
	ID              string           `json:"server_id"`
	Now             time.Time        `json:"now"`
	Disabled        bool             `json:"disabled,omitempty"`
	Config          JetStreamConfig  `json:"config,omitempty"`
	Limits          *JSLimitOpts     `json:"limits,omitempty"`
	Streams         int              `json:"streams"`
	StreamsLeader   int              `json:"streams_leader,omitempty"`
	Consumers       int              `json:"consumers"`
	ConsumersLeader int              `json:"consumers_leader,omitempty"`
	Messages        uint64           `json:"messages"`
	Bytes           uint64           `json:"bytes"`
	Meta            *MetaClusterInfo `json:"meta_cluster,omitempty"`
	AccountDetails  []*AccountDetail `json:"account_details,omitempty"`
	Total           int              `json:"total"`
}

func (s *Server) accountDetail(jsa *jsAccount, optStreams, optConsumers, optDirectConsumers, optCfg, optRaft, optStreamLeader bool) *AccountDetail {
	jsa.mu.RLock()
	acc := jsa.account
	name := acc.GetName()
	id := name
	if acc.nameTag != _EMPTY_ {
		name = acc.nameTag
	}
	jsa.usageMu.RLock()
	totalMem, totalStore := jsa.storageTotals()
	detail := AccountDetail{
		Name: name,
		Id:   id,
		JetStreamStats: JetStreamStats{
			Memory: totalMem,
			Store:  totalStore,
			API: JetStreamAPIStats{
				Total:  jsa.apiTotal,
				Errors: jsa.apiErrors,
			},
		},
		Streams: make([]StreamDetail, 0, len(jsa.streams)),
	}
	if reserved, ok := jsa.limits[_EMPTY_]; ok {
		detail.JetStreamStats.ReservedMemory = uint64(reserved.MaxMemory)
		detail.JetStreamStats.ReservedStore = uint64(reserved.MaxStore)
	}
	jsa.usageMu.RUnlock()

	var streams []*stream
	if optStreams {
		for _, stream := range jsa.streams {
			streams = append(streams, stream)
		}
	}
	jsa.mu.RUnlock()

	if js := s.getJetStream(); js != nil && optStreams {
		for _, stream := range streams {
			rgroup := stream.raftGroup()
			ci := js.clusterInfo(rgroup)
			var cfg *StreamConfig
			if optCfg {
				// Report the config as requested, the stream can still be running at its origin.
				// Must be consistent with the desired state reported as part of the cluster info.
				c := js.targetStreamConfig(stream, stream.config())
				cfg = &c
			}
			// Skip if we are only looking for stream leaders.
			if optStreamLeader && ci != nil && ci.Leader != s.Name() {
				continue
			}
			sdet := StreamDetail{
				Name:    stream.name(),
				Created: stream.createdTime(),
				State:   stream.state(),
				Cluster: ci,
				Config:  cfg,
				Mirror:  stream.mirrorInfo(),
				Sources: stream.sourcesInfo(),
			}
			if optRaft && rgroup != nil {
				sdet.RaftGroup = rgroup.Name
				sdet.ConsumerRaftGroups = make([]*RaftGroupDetail, 0)
			}
			if optConsumers {
				for _, consumer := range stream.getPublicConsumers() {
					cInfo := consumer.info()
					if cInfo == nil {
						continue
					}
					if !optCfg {
						cInfo.Config = nil
					}
					sdet.Consumer = append(sdet.Consumer, cInfo)
					if optRaft {
						crgroup := consumer.raftGroup()
						if crgroup != nil {
							sdet.ConsumerRaftGroups = append(sdet.ConsumerRaftGroups,
								&RaftGroupDetail{cInfo.Name, crgroup.Name},
							)
						}
					}
				}
				if optDirectConsumers {
					for _, consumer := range stream.getDirectConsumers() {
						cInfo := consumer.info()
						if cInfo == nil {
							continue
						}
						if !optCfg {
							cInfo.Config = nil
						}
						sdet.DirectConsumer = append(sdet.DirectConsumer, cInfo)
					}
				}
			}
			detail.Streams = append(detail.Streams, sdet)
		}
	}
	return &detail
}

func (s *Server) JszAccount(opts *JSzOptions) (*AccountDetail, error) {
	js := s.getJetStream()
	if js == nil {
		return nil, fmt.Errorf("jetstream not enabled")
	}
	acc := opts.Account
	account, ok := s.accounts.Load(acc)
	if !ok {
		return nil, fmt.Errorf("account %q not found", acc)
	}
	js.mu.RLock()
	jsa, ok := js.accounts[account.(*Account).Name]
	js.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("account %q not jetstream enabled", acc)
	}
	return s.accountDetail(jsa, opts.Streams, opts.Consumer, opts.DirectConsumer, opts.Config, opts.RaftGroups, opts.StreamLeaderOnly), nil
}

// helper to get cluster info from node via dummy group
func (s *Server) raftNodeToClusterInfo(node RaftNode) *ClusterInfo {
	if node == nil {
		return nil
	}
	peers := node.Peers()
	peerList := make([]string, len(peers))
	for i, p := range peers {
		peerList[i] = p.ID
	}
	group := &raftGroup{
		Name:  _EMPTY_,
		Peers: peerList,
		node:  node,
	}
	return s.getJetStream().clusterInfo(group)
}

// Jsz returns a Jsz structure containing information about JetStream.
func (s *Server) Jsz(opts *JSzOptions) (*JSInfo, error) {
	// set option defaults
	if opts == nil {
		opts = &JSzOptions{}
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	if opts.Limit == 0 {
		opts.Limit = 1024
	}
	if opts.Consumer {
		opts.Streams = true
	}
	if opts.Streams && opts.Account == _EMPTY_ {
		opts.Accounts = true
	}

	jsi := &JSInfo{
		ID:  s.ID(),
		Now: time.Now().UTC(),
	}

	js := s.getJetStream()
	if js == nil || !js.isEnabled() {
		if opts.LeaderOnly {
			return nil, fmt.Errorf("%w: not leader", errSkipZreq)
		}

		jsi.Disabled = true
		return jsi, nil
	}

	jsi.Limits = &s.getOpts().JetStreamLimits

	js.mu.RLock()
	isLeader := js.cluster == nil || js.cluster.isLeader()
	js.mu.RUnlock()

	if opts.LeaderOnly && !isLeader {
		return nil, fmt.Errorf("%w: not leader", errSkipZreq)
	}

	var accounts []*jsAccount

	js.mu.RLock()
	jsi.Config = js.config
	for _, info := range js.accounts {
		accounts = append(accounts, info)
	}
	js.mu.RUnlock()

	jsi.Total = len(accounts)

	if mg := js.getMetaGroup(); mg != nil {
		if ci := s.raftNodeToClusterInfo(mg); ci != nil {
			jsi.Meta = &MetaClusterInfo{
				Name:         ci.Name,
				Leader:       ci.Leader,
				Replicas:     ci.Replicas,
				Size:         mg.ClusterSize(),
				QuorumNeeded: mg.QuorumNeeded(),
				Rescue:       mg.InRescue(),
			}
			if ci.Leader != _EMPTY_ {
				jsi.Meta.Peer = getHash(ci.Leader)
			}
			if ipq := s.jsAPIRoutedReqs; ipq != nil {
				jsi.Meta.PendingRequests = ipq.len()
			}
			if ipq := s.jsAPIRoutedInfoReqs; ipq != nil {
				jsi.Meta.PendingInfos = ipq.len()
			}
			jsi.Meta.Pending = jsi.Meta.PendingRequests + jsi.Meta.PendingInfos
			jsi.Meta.Snapshot = s.metaClusterSnapshotStats(js, mg)
		}
	}

	jsi.JetStreamStats = *js.usageStats()

	// If a specific account is requested, track the index.
	filterIdx := -1

	// Calculate the stats of all accounts and streams regardless of the filtering.
	for i, jsa := range accounts {
		if jsa.acc().GetName() == opts.Account {
			filterIdx = i
		}

		jsa.mu.RLock()
		streams := make([]*stream, 0, len(jsa.streams))
		for _, stream := range jsa.streams {
			streams = append(streams, stream)
		}
		jsa.mu.RUnlock()

		jsi.Streams += len(streams)
		for _, stream := range streams {
			streamState := stream.state()
			jsi.Messages += streamState.Msgs
			jsi.Bytes += streamState.Bytes
			jsi.Consumers += streamState.Consumers
			if opts.RaftGroups {
				if node := stream.raftNode(); node == nil || node.Leader() {
					jsi.StreamsLeader++
				}
				for _, consumer := range stream.getPublicConsumers() {
					if node := consumer.raftNode(); node == nil || node.Leader() {
						jsi.ConsumersLeader++
					}
				}
			}
		}
	}

	// Targeted account takes precedence.
	if filterIdx >= 0 {
		accounts = accounts[filterIdx : filterIdx+1]
	} else if opts.Accounts {

		if opts.Limit > 0 {
			// Sort by name for a consistent read (barring any concurrent changes)
			slices.SortFunc(accounts, func(i, j *jsAccount) int { return cmp.Compare(i.acc().Name, j.acc().Name) })

			// Offset larger than the number of accounts.
			offset := min(opts.Offset, len(accounts))
			accounts = accounts[offset:]

			limit := min(opts.Limit, len(accounts))
			accounts = accounts[:limit]
		}
	} else {
		accounts = nil
	}

	if len(accounts) > 0 {
		jsi.AccountDetails = make([]*AccountDetail, 0, len(accounts))

		for _, jsa := range accounts {
			detail := s.accountDetail(jsa, opts.Streams, opts.Consumer, opts.DirectConsumer, opts.Config, opts.RaftGroups, opts.StreamLeaderOnly)
			jsi.AccountDetails = append(jsi.AccountDetails, detail)
		}
	}

	return jsi, nil
}

// HandleJsz process HTTP requests for jetstream information.
func (s *Server) HandleJsz(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.httpReqStats[JszPath]++
	s.mu.Unlock()
	accounts, err := decodeBool(w, r, "accounts")
	if err != nil {
		return
	}
	streams, err := decodeBool(w, r, "streams")
	if err != nil {
		return
	}
	consumers, err := decodeBool(w, r, "consumers")
	if err != nil {
		return
	}
	directConsumers, err := decodeBool(w, r, "direct-consumers")
	if err != nil {
		return
	}
	config, err := decodeBool(w, r, "config")
	if err != nil {
		return
	}
	offset, err := decodeInt(w, r, "offset")
	if err != nil {
		return
	}
	limit, err := decodeInt(w, r, "limit")
	if err != nil {
		return
	}
	leader, err := decodeBool(w, r, "leader-only")
	if err != nil {
		return
	}
	rgroups, err := decodeBool(w, r, "raft")
	if err != nil {
		return
	}

	sleader, err := decodeBool(w, r, "stream-leader-only")
	if err != nil {
		return
	}

	l, err := s.Jsz(&JSzOptions{
		Account:          r.URL.Query().Get("acc"),
		Accounts:         accounts,
		Streams:          streams,
		Consumer:         consumers,
		DirectConsumer:   directConsumers,
		Config:           config,
		LeaderOnly:       leader,
		Offset:           offset,
		Limit:            limit,
		RaftGroups:       rgroups,
		StreamLeaderOnly: sleader,
	})
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	b, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		s.Errorf("Error marshaling response to /jsz request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

type HealthStatus struct {
	Status     string         `json:"status"`
	StatusCode int            `json:"status_code,omitempty"`
	Error      string         `json:"error,omitempty"`
	Errors     []HealthzError `json:"errors,omitempty"`
}

type HealthzError struct {
	Type     HealthZErrorType `json:"type"`
	Account  string           `json:"account,omitempty"`
	Stream   string           `json:"stream,omitempty"`
	Consumer string           `json:"consumer,omitempty"`
	Error    string           `json:"error,omitempty"`
}

type HealthZErrorType int

const (
	HealthzErrorConn HealthZErrorType = iota
	HealthzErrorBadRequest
	HealthzErrorJetStream
	HealthzErrorAccount
	HealthzErrorStream
	HealthzErrorConsumer
)

func (t HealthZErrorType) String() string {
	switch t {
	case HealthzErrorConn:
		return "CONNECTION"
	case HealthzErrorBadRequest:
		return "BAD_REQUEST"
	case HealthzErrorJetStream:
		return "JETSTREAM"
	case HealthzErrorAccount:
		return "ACCOUNT"
	case HealthzErrorStream:
		return "STREAM"
	case HealthzErrorConsumer:
		return "CONSUMER"
	default:
		return "unknown"
	}
}

func (t HealthZErrorType) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

func (t *HealthZErrorType) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case `"CONNECTION"`:
		*t = HealthzErrorConn
	case `"BAD_REQUEST"`:
		*t = HealthzErrorBadRequest
	case `"JETSTREAM"`:
		*t = HealthzErrorJetStream
	case `"ACCOUNT"`:
		*t = HealthzErrorAccount
	case `"STREAM"`:
		*t = HealthzErrorStream
	case `"CONSUMER"`:
		*t = HealthzErrorConsumer
	default:
		return fmt.Errorf("unknown healthz error type %q", data)
	}
	return nil
}

// https://datatracker.ietf.org/doc/html/draft-inadarei-api-health-check
func (s *Server) HandleHealthz(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.httpReqStats[HealthzPath]++
	s.mu.Unlock()

	jsEnabled, err := decodeBool(w, r, "js-enabled")
	if err != nil {
		return
	}
	if jsEnabled {
		s.Warnf("Healthcheck: js-enabled deprecated, use js-enabled-only instead")
	}
	jsEnabledOnly, err := decodeBool(w, r, "js-enabled-only")
	if err != nil {
		return
	}
	jsServerOnly, err := decodeBool(w, r, "js-server-only")
	if err != nil {
		return
	}
	jsMetaOnly, err := decodeBool(w, r, "js-meta-only")
	if err != nil {
		return
	}
	leafRemotes, err := decodeBool(w, r, "leaf-remotes")
	if err != nil {
		return
	}

	includeDetails, err := decodeBool(w, r, "details")
	if err != nil {
		return
	}

	hs := s.healthz(&HealthzOptions{
		JSEnabled:     jsEnabled,
		JSEnabledOnly: jsEnabledOnly,
		JSServerOnly:  jsServerOnly,
		JSMetaOnly:    jsMetaOnly,
		LeafRemotes:   leafRemotes,
		Account:       r.URL.Query().Get("account"),
		Stream:        r.URL.Query().Get("stream"),
		Consumer:      r.URL.Query().Get("consumer"),
		Details:       includeDetails,
	})

	code := hs.StatusCode
	if hs.Error != _EMPTY_ {
		s.Warnf("Healthcheck failed: %q", hs.Error)
	} else if len(hs.Errors) != 0 {
		s.Warnf("Healthcheck failed: %d errors", len(hs.Errors))
	}
	// Remove StatusCode from JSON representation when responding via HTTP
	// since this is already in the response.
	hs.StatusCode = 0
	b, err := json.Marshal(hs)
	if err != nil {
		s.Errorf("Error marshaling response to /healthz request: %v", err)
	}

	handleResponse(code, w, r, b)
}

// Generate health status.
func (s *Server) healthz(opts *HealthzOptions) *HealthStatus {
	var health = &HealthStatus{Status: "ok"}

	// set option defaults
	if opts == nil {
		opts = &HealthzOptions{}
	}
	details := opts.Details
	defer func() {
		// for response with details enabled, set status to either "error" or "ok"
		if details {
			if len(health.Errors) != 0 {
				health.Status = "error"
			} else {
				health.Status = "ok"
			}
		}
		// if no specific status code was set, set it based on the presence of errors
		if health.StatusCode == 0 {
			if health.Error != _EMPTY_ || len(health.Errors) != 0 {
				health.StatusCode = http.StatusServiceUnavailable
			} else {
				health.StatusCode = http.StatusOK
			}
		}
	}()

	if opts.Account == _EMPTY_ && opts.Stream != _EMPTY_ {
		health.StatusCode = http.StatusBadRequest
		if !details {
			health.Status = "error"
			health.Error = fmt.Sprintf("%q must not be empty when checking stream health", "account")
		} else {
			health.Errors = append(health.Errors, HealthzError{
				Type:  HealthzErrorBadRequest,
				Error: fmt.Sprintf("%q must not be empty when checking stream health", "account"),
			})
		}
		return health
	}

	if opts.Stream == _EMPTY_ && opts.Consumer != _EMPTY_ {
		health.StatusCode = http.StatusBadRequest
		if !details {
			health.Status = "error"
			health.Error = fmt.Sprintf("%q must not be empty when checking consumer health", "stream")
		} else {
			health.Errors = append(health.Errors, HealthzError{
				Type:  HealthzErrorBadRequest,
				Error: fmt.Sprintf("%q must not be empty when checking consumer health", "stream"),
			})
		}
		return health
	}

	if err := s.readyForConnections(time.Millisecond); err != nil {
		health.StatusCode = http.StatusInternalServerError
		health.Status = "error"
		if !details {
			health.Error = err.Error()
		} else {
			health.Errors = append(health.Errors, HealthzError{
				Type:  HealthzErrorConn,
				Error: err.Error(),
			})
		}
		return health
	}

	// If requested, make sure every enabled solicited leafnode remote is connected.
	if opts.LeafRemotes {
		if errs := s.disconnectedLeafRemotes(); len(errs) > 0 {
			health.StatusCode = http.StatusServiceUnavailable
			health.Status = "unavailable"
			if !details {
				health.Error = strings.Join(errs, "; ")
				return health
			}
			for _, e := range errs {
				health.Errors = append(health.Errors, HealthzError{Type: HealthzErrorConn, Error: e})
			}
			return health
		}
	}

	// If JSServerOnly is true, then do not check further accounts, streams and consumers.
	if opts.JSServerOnly {
		return health
	}

	sopts := s.getOpts()

	// If JS is not enabled in the config, we stop.
	if !sopts.JetStream {
		return health
	}

	// Access the Jetstream state to perform additional checks.
	js := s.getJetStream()
	const na = "unavailable"
	if !js.isEnabled() {
		health.StatusCode = http.StatusServiceUnavailable
		health.Status = na
		if !details {
			health.Error = NewJSNotEnabledError().Error()
		} else {
			health.Errors = append(health.Errors, HealthzError{
				Type:  HealthzErrorJetStream,
				Error: NewJSNotEnabledError().Error(),
			})
		}
		return health
	}
	// Only check if JS is enabled, skip meta and asset check.
	if opts.JSEnabledOnly || opts.JSEnabled {
		return health
	}

	// Clustered JetStream
	js.mu.RLock()
	cc := js.cluster
	js.mu.RUnlock()

	// Currently single server we make sure the streams were recovered.
	if cc == nil {
		sdir := js.config.StoreDir
		// Whip through account folders and pull each stream name.
		fis, _ := os.ReadDir(sdir)
		var accFound, streamFound, consumerFound bool
		for _, fi := range fis {
			if fi.Name() == snapStagingDir {
				continue
			}
			if opts.Account != _EMPTY_ {
				if fi.Name() != opts.Account {
					continue
				}
				accFound = true
			}
			acc, err := s.LookupAccount(fi.Name())
			// Expired accounts are not a JetStream health problem; skip them when
			// scanning all accounts. Still surface an error if this account was
			// explicitly requested — including the err==nil + IsExpired() case.
			expired := (err != nil && errors.Is(err, ErrAccountExpired)) || (err == nil && acc.IsExpired())
			if expired {
				if opts.Account == _EMPTY_ {
					continue
				}
				msg := fmt.Sprintf("JetStream account '%s' is expired", fi.Name())
				if !details {
					health.Status = na
					health.Error = msg
					return health
				}
				health.Errors = append(health.Errors, HealthzError{
					Type:    HealthzErrorAccount,
					Account: fi.Name(),
					Error:   msg,
				})
				// Return so later stream/consumer not-found checks do not
				// replace this with a misleading 404 for assets we skipped.
				return health
			}
			if err != nil {
				if !details {
					health.Status = na
					health.Error = fmt.Sprintf("JetStream account '%s' could not be resolved", fi.Name())
					return health
				}
				health.Errors = append(health.Errors, HealthzError{
					Type:    HealthzErrorAccount,
					Account: fi.Name(),
					Error:   fmt.Sprintf("JetStream account '%s' could not be resolved", fi.Name()),
				})
				continue
			}
			sfis, _ := os.ReadDir(filepath.Join(sdir, fi.Name(), "streams"))
			for _, sfi := range sfis {
				if opts.Stream != _EMPTY_ {
					if sfi.Name() != opts.Stream {
						continue
					}
					streamFound = true
				}
				stream := sfi.Name()
				s, err := acc.lookupStream(stream)
				if err != nil {
					if !details {
						health.Status = na
						health.Error = fmt.Sprintf("JetStream stream '%s > %s' could not be recovered", acc, stream)
						return health
					}
					health.Errors = append(health.Errors, HealthzError{
						Type:    HealthzErrorStream,
						Account: acc.Name,
						Stream:  stream,
						Error:   fmt.Sprintf("JetStream stream '%s > %s' could not be recovered", acc, stream),
					})
					continue
				}
				if streamWerr := s.getWriteErr(); streamWerr != nil {
					if !details {
						health.Status = na
						health.Error = fmt.Sprintf("JetStream stream '%s > %s' write error: %v", acc, stream, streamWerr)
						return health
					}
					health.Errors = append(health.Errors, HealthzError{
						Type:    HealthzErrorStream,
						Account: acc.Name,
						Stream:  stream,
						Error:   fmt.Sprintf("JetStream stream '%s > %s' write error: %v", acc, stream, streamWerr),
					})
					continue
				}
				if streamFound {
					// if consumer option is passed, verify that the consumer exists on stream
					if opts.Consumer != _EMPTY_ {
						if s.lookupConsumer(opts.Consumer) != nil {
							consumerFound = true
						}
					}
					break
				}
			}
			if accFound {
				break
			}
		}
		if opts.Account != _EMPTY_ && !accFound {
			health.StatusCode = http.StatusNotFound
			if !details {
				health.Status = na
				health.Error = fmt.Sprintf("JetStream account %q not found", opts.Account)
			} else {
				health.Errors = []HealthzError{
					{
						Type:    HealthzErrorAccount,
						Account: opts.Account,
						Error:   fmt.Sprintf("JetStream account %q not found", opts.Account),
					},
				}
			}
			return health
		}
		if opts.Stream != _EMPTY_ && !streamFound {
			health.StatusCode = http.StatusNotFound
			if !details {
				health.Status = na
				health.Error = fmt.Sprintf("JetStream stream %q not found on account %q", opts.Stream, opts.Account)
			} else {
				health.Errors = []HealthzError{
					{
						Type:    HealthzErrorStream,
						Account: opts.Account,
						Stream:  opts.Stream,
						Error:   fmt.Sprintf("JetStream stream %q not found on account %q", opts.Stream, opts.Account),
					},
				}
			}
			return health
		}
		if opts.Consumer != _EMPTY_ && !consumerFound {
			health.StatusCode = http.StatusNotFound
			if !details {
				health.Status = na
				health.Error = fmt.Sprintf("JetStream consumer %q not found for stream %q on account %q", opts.Consumer, opts.Stream, opts.Account)
			} else {
				health.Errors = []HealthzError{
					{
						Type:     HealthzErrorConsumer,
						Account:  opts.Account,
						Stream:   opts.Stream,
						Consumer: opts.Consumer,
						Error:    fmt.Sprintf("JetStream consumer %q not found for stream %q on account %q", opts.Consumer, opts.Stream, opts.Account),
					},
				}
			}
		}
		return health
	}

	// If we are here we want to check for any assets assigned to us.
	var meta RaftNode
	js.mu.RLock()
	meta = cc.meta
	js.mu.RUnlock()

	// Check meta layer health.
	var metaNoLeader, metaClosed, metaUnhealthy bool
	var metaWerr error
	if meta != nil {
		metaNoLeader = meta.GroupLeader() == _EMPTY_
		metaClosed = meta.State() == Closed
		metaUnhealthy = !meta.Healthy()
		metaWerr = meta.GetWriteErr()
	}
	// Surface an application-level meta write error (e.g. an undecodable meta entry)
	// in addition to the node-level write error above.
	metaApplyErr := js.getMetaWriteErr()
	metaRecovering := js.isMetaRecovering()
	if meta == nil || metaNoLeader || metaClosed || metaUnhealthy || metaWerr != nil || metaApplyErr != nil || metaRecovering {
		var desc string
		if metaWerr != nil {
			desc = fmt.Sprintf("JetStream meta layer write error: %v", metaWerr)
		} else if metaApplyErr != nil {
			desc = fmt.Sprintf("JetStream meta layer apply error: %v", metaApplyErr)
		} else if metaClosed {
			desc = "JetStream meta layer is not running"
		} else if meta != nil && metaRecovering {
			desc = "JetStream is still recovering meta layer"
		} else if meta == nil || metaNoLeader {
			desc = "JetStream has not established contact with a meta leader"
		} else {
			desc = "JetStream is not current with the meta leader"
		}
		if !details {
			health.Status = na
			health.Error = desc
		} else {
			health.Errors = []HealthzError{
				{
					Type:  HealthzErrorJetStream,
					Error: desc,
				},
			}
		}
		return health
	}

	// Skips doing full healthz and only checks the meta leader.
	if opts.JSMetaOnly {
		return health
	}

	// Range across all accounts, the streams assigned to them, and the consumers.
	// If they are assigned to this server check their status.
	ourID := meta.ID()

	// Copy the meta layer so we do not need to hold the js read lock for an extended period of time.
	var streams map[string]map[string]*streamAssignment
	js.mu.RLock()
	if opts.Account == _EMPTY_ {
		// Collect all relevant streams and consumers.
		streams = make(map[string]map[string]*streamAssignment, len(cc.streams))
		for acc, asa := range cc.streams {
			nasa := make(map[string]*streamAssignment)
			for stream, sa := range asa {
				// If we are a member and we are not being restored, select for check.
				if sa.Group.isMember(ourID) && sa.Restore == nil {
					csa := sa.copyGroup()
					csa.consumers = make(map[string]*consumerAssignment)
					for consumer, ca := range sa.consumers {
						if ca.Group.isMember(ourID) {
							// Use original here. Not a copy.
							csa.consumers[consumer] = ca
						}
					}
					nasa[stream] = csa
				}
			}
			streams[acc] = nasa
		}
	} else {
		streams = make(map[string]map[string]*streamAssignment, 1)
		asa, ok := cc.streams[opts.Account]
		if !ok {
			health.StatusCode = http.StatusNotFound
			if !details {
				health.Status = na
				health.Error = fmt.Sprintf("JetStream account %q not found", opts.Account)
			} else {
				health.Errors = []HealthzError{
					{
						Type:    HealthzErrorAccount,
						Account: opts.Account,
						Error:   fmt.Sprintf("JetStream account %q not found", opts.Account),
					},
				}
			}
			js.mu.RUnlock()
			return health
		}
		nasa := make(map[string]*streamAssignment)
		if opts.Stream != _EMPTY_ {
			sa, ok := asa[opts.Stream]
			if !ok || !sa.Group.isMember(ourID) {
				health.StatusCode = http.StatusNotFound
				if !details {
					health.Status = na
					health.Error = fmt.Sprintf("JetStream stream %q not found on account %q", opts.Stream, opts.Account)
				} else {
					health.Errors = []HealthzError{
						{
							Type:    HealthzErrorStream,
							Account: opts.Account,
							Stream:  opts.Stream,
							Error:   fmt.Sprintf("JetStream stream %q not found on account %q", opts.Stream, opts.Account),
						},
					}
				}
				js.mu.RUnlock()
				return health
			}
			csa := sa.copyGroup()
			csa.consumers = make(map[string]*consumerAssignment)
			var consumerFound bool
			for consumer, ca := range sa.consumers {
				if opts.Consumer != _EMPTY_ {
					if consumer != opts.Consumer || !ca.Group.isMember(ourID) {
						continue
					}
					consumerFound = true
				}
				// If we are a member and we are not being restored, select for check.
				if sa.Group.isMember(ourID) && sa.Restore == nil {
					csa.consumers[consumer] = ca
				}
				if consumerFound {
					break
				}
			}
			if opts.Consumer != _EMPTY_ && !consumerFound {
				health.StatusCode = http.StatusNotFound
				if !details {
					health.Status = na
					health.Error = fmt.Sprintf("JetStream consumer %q not found for stream %q on account %q", opts.Consumer, opts.Stream, opts.Account)
				} else {
					health.Errors = []HealthzError{
						{
							Type:     HealthzErrorConsumer,
							Account:  opts.Account,
							Stream:   opts.Stream,
							Consumer: opts.Consumer,
							Error:    fmt.Sprintf("JetStream consumer %q not found for stream %q on account %q", opts.Consumer, opts.Stream, opts.Account),
						},
					}
				}
				js.mu.RUnlock()
				return health
			}
			nasa[opts.Stream] = csa
		} else {
			for stream, sa := range asa {
				// If we are a member and we are not being restored, select for check.
				if sa.Group.isMember(ourID) && sa.Restore == nil {
					csa := sa.copyGroup()
					csa.consumers = make(map[string]*consumerAssignment)
					for consumer, ca := range sa.consumers {
						if ca.Group.isMember(ourID) {
							csa.consumers[consumer] = ca
						}
					}
					nasa[stream] = csa
				}
			}
		}
		streams[opts.Account] = nasa
	}
	js.mu.RUnlock()

	// Use our copy to traverse so we do not need to hold the js lock.
	for accName, asa := range streams {
		acc, err := s.LookupAccount(accName)
		// Expired accounts are not a JetStream health problem; skip them when
		// scanning all accounts. Still surface an error if this account was
		// explicitly requested — including the err==nil + IsExpired() case.
		expired := (err != nil && errors.Is(err, ErrAccountExpired)) || (err == nil && acc.IsExpired())
		if expired {
			if opts.Account == _EMPTY_ {
				continue
			}
			msg := fmt.Sprintf("JetStream account %q is expired", accName)
			if !details {
				health.Status = na
				health.Error = msg
				return health
			}
			health.Errors = append(health.Errors, HealthzError{
				Type:    HealthzErrorAccount,
				Account: accName,
				Error:   msg,
			})
			// Return so later health checks do not obscure the expired account.
			return health
		}
		if err != nil && len(asa) > 0 {
			if !details {
				health.Status = na
				health.Error = fmt.Sprintf("JetStream can not lookup account %q: %v", accName, err)
				return health
			}
			health.Errors = append(health.Errors, HealthzError{
				Type:    HealthzErrorAccount,
				Account: accName,
				Error:   fmt.Sprintf("JetStream can not lookup account %q: %v", accName, err),
			})
			continue
		}

		for stream, sa := range asa {
			if sa != nil && sa.unsupported != nil {
				continue
			}
			// Make sure we can look up
			if err := js.isStreamHealthy(acc, sa); err != nil {
				if !details {
					health.Status = na
					health.Error = fmt.Sprintf("JetStream stream '%s > %s' is not current: %s", accName, stream, err)
					return health
				}
				health.Errors = append(health.Errors, HealthzError{
					Type:    HealthzErrorStream,
					Account: accName,
					Stream:  stream,
					Error:   fmt.Sprintf("JetStream stream '%s > %s' is not current: %s", accName, stream, err),
				})
				continue
			}
			mset, _ := acc.lookupStream(stream)
			// Now check consumers.
			for consumer, ca := range sa.consumers {
				if err := js.isConsumerHealthy(mset, sa, consumer, ca); err != nil {
					if !details {
						health.Status = na
						health.Error = fmt.Sprintf("JetStream consumer '%s > %s > %s' is not current: %s", acc, stream, consumer, err)
						return health
					}
					health.Errors = append(health.Errors, HealthzError{
						Type:     HealthzErrorConsumer,
						Account:  accName,
						Stream:   stream,
						Consumer: consumer,
						Error:    fmt.Sprintf("JetStream consumer '%s > %s > %s' is not current: %s", acc, stream, consumer, err),
					})
				}
			}
		}
	}
	// Success.
	return health
}

// Healthz returns the health status of the server.
func (s *Server) Healthz(opts *HealthzOptions) *HealthStatus {
	return s.healthz(opts)
}

type ExpvarzStatus struct {
	Memstats json.RawMessage `json:"memstats"`
	Cmdline  json.RawMessage `json:"cmdline"`
}

func (s *Server) expvarz(_ *ExpvarzEventOptions) *ExpvarzStatus {
	var stat ExpvarzStatus

	const memStatsKey = "memstats"
	const cmdLineKey = "cmdline"

	expvar.Do(func(v expvar.KeyValue) {
		switch v.Key {
		case memStatsKey:
			stat.Memstats = json.RawMessage(v.Value.String())

		case cmdLineKey:
			stat.Cmdline = json.RawMessage(v.Value.String())
		}
	})

	return &stat
}

type ProfilezStatus struct {
	Profile []byte `json:"profile"`
	Error   string `json:"error"`
}

func (s *Server) profilez(opts *ProfilezOptions) *ProfilezStatus {
	var buffer bytes.Buffer
	switch opts.Name {
	case _EMPTY_:
		return &ProfilezStatus{
			Error: "Profile name not specified",
		}
	case "cpu":
		if opts.Duration <= 0 || opts.Duration > 15*time.Second {
			return &ProfilezStatus{
				Error: fmt.Sprintf("Duration %s should be between 0s and 15s", opts.Duration),
			}
		}
		if err := pprof.StartCPUProfile(&buffer); err != nil {
			return &ProfilezStatus{
				Error: fmt.Sprintf("Failed to start CPU profile: %s", err),
			}
		}
		time.Sleep(opts.Duration)
		pprof.StopCPUProfile()
	default:
		profile := pprof.Lookup(opts.Name)
		if profile == nil {
			return &ProfilezStatus{
				Error: fmt.Sprintf("Profile %q not found", opts.Name),
			}
		}
		if err := profile.WriteTo(&buffer, opts.Debug); err != nil {
			return &ProfilezStatus{
				Error: fmt.Sprintf("Profile %q error: %s", opts.Name, err),
			}
		}
	}
	return &ProfilezStatus{
		Profile: buffer.Bytes(),
	}
}

type RaftzGroup struct {
	ID            string                    `json:"id"`
	State         string                    `json:"state"`
	Size          int                       `json:"size"`
	QuorumNeeded  int                       `json:"quorum_needed"`
	Observer      bool                      `json:"observer,omitempty"`
	Paused        bool                      `json:"paused,omitempty"`
	Overrun       bool                      `json:"overrun,omitempty"`
	OverrunCount  uint64                    `json:"overrun_count,omitempty"`
	Committed     uint64                    `json:"committed"`
	Applied       uint64                    `json:"applied"`
	CatchingUp    bool                      `json:"catching_up,omitempty"`
	Leader        string                    `json:"leader,omitempty"`
	LeaderSince   *time.Time                `json:"leader_since,omitempty"`
	EverHadLeader bool                      `json:"ever_had_leader"`
	Term          uint64                    `json:"term"`
	Vote          string                    `json:"voted_for,omitempty"`
	PTerm         uint64                    `json:"pterm"`
	PIndex        uint64                    `json:"pindex"`
	SystemAcc     bool                      `json:"system_account"`
	TrafficAcc    string                    `json:"traffic_account"`
	IPQPropLen    int                       `json:"ipq_proposal_len"`
	IPQEntryLen   int                       `json:"ipq_entry_len"`
	IPQRespLen    int                       `json:"ipq_resp_len"`
	IPQApplyLen   int                       `json:"ipq_apply_len"`
	WAL           StreamState               `json:"wal"`
	WALError      error                     `json:"wal_error,omitempty"`
	Peers         map[string]RaftzGroupPeer `json:"peers"`
}

type RaftzGroupPeer struct {
	Name                string `json:"name"`
	Known               bool   `json:"known"`
	LastReplicatedIndex uint64 `json:"last_replicated_index,omitempty"`
	LastSeen            string `json:"last_seen,omitempty"`
}

type RaftzStatus map[string]map[string]RaftzGroup

func (s *Server) HandleRaftz(w http.ResponseWriter, r *http.Request) {
	if s.raftNodes == nil {
		w.WriteHeader(404)
		w.Write([]byte("No Raft nodes registered"))
		return
	}

	groups := s.Raftz(&RaftzOptions{
		AccountFilter: r.URL.Query().Get("acc"),
		GroupFilter:   r.URL.Query().Get("group"),
	})

	if groups == nil {
		w.WriteHeader(404)
		w.Write([]byte("No Raft nodes returned, check supplied filters"))
		return
	}

	b, _ := json.MarshalIndent(groups, "", "   ")
	ResponseHandler(w, r, b)
}

func (s *Server) Raftz(opts *RaftzOptions) *RaftzStatus {
	afilter, gfilter := opts.AccountFilter, opts.GroupFilter

	if afilter == _EMPTY_ {
		if sys := s.SystemAccount(); sys != nil {
			afilter = sys.Name
		} else {
			return nil
		}
	}

	groups := map[string]RaftNode{}
	infos := RaftzStatus{} // account -> group ID

	s.rnMu.RLock()
	if gfilter != _EMPTY_ {
		if rg, ok := s.raftNodes[gfilter]; ok && rg != nil {
			if n, ok := rg.(*raft); ok {
				if n.accName == afilter {
					groups[gfilter] = rg
				}
			}
		}
	} else {
		for name, rg := range s.raftNodes {
			if rg == nil {
				continue
			}
			if n, ok := rg.(*raft); ok {
				if n.accName != afilter {
					continue
				}
				groups[name] = rg
			}
		}
	}
	s.rnMu.RUnlock()

	for name, rg := range groups {
		n, ok := rg.(*raft)
		if n == nil || !ok {
			continue
		}
		if _, ok := infos[n.accName]; !ok {
			infos[n.accName] = map[string]RaftzGroup{}
		}
		// Only take the lock once, using the public RaftNode functions would
		// cause us to take and release the locks over and over again.
		n.RLock()
		info := RaftzGroup{
			ID:            n.id,
			State:         RaftState(n.state.Load()).String(),
			Size:          n.csz,
			QuorumNeeded:  n.qn,
			Observer:      n.observer,
			Paused:        n.paused,
			Overrun:       n.quorumPaused || n.isLeaderOverrun(),
			OverrunCount:  n.overrunCount,
			Committed:     n.commit,
			Applied:       n.applied,
			CatchingUp:    n.catchup != nil,
			Leader:        n.leader,
			LeaderSince:   n.leaderSince.Load(),
			EverHadLeader: n.pleader.Load(),
			Term:          n.term,
			Vote:          n.vote,
			PTerm:         n.pterm,
			PIndex:        n.pindex,
			SystemAcc:     n.IsSystemAccount(),
			TrafficAcc:    n.t.Account().GetName(),
			IPQPropLen:    n.prop.len(),
			IPQEntryLen:   n.entry.len(),
			IPQRespLen:    n.resp.len(),
			IPQApplyLen:   n.apply.len(),
			WALError:      n.werr,
			Peers:         map[string]RaftzGroupPeer{},
		}
		n.wal.FastState(&info.WAL)
		for id, p := range n.peers {
			if id == n.id {
				continue
			}
			kp := n.membChange == nil || n.membChange.peer != id
			peer := RaftzGroupPeer{
				Name:                s.serverNameForNode(id),
				Known:               kp,
				LastReplicatedIndex: p.li,
			}
			if !p.ts.IsZero() {
				peer.LastSeen = time.Since(p.ts).String()
			}
			info.Peers[id] = peer
		}
		n.RUnlock()
		infos[n.accName][name] = info
	}

	return &infos
}

// disconnectedLeafRemotes returns a description of each enabled solicited
// leafnode remote that does not currently have an established connection.
func (s *Server) disconnectedLeafRemotes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.leafRemoteCfgs) == 0 {
		return nil
	}
	connected := make(map[*leafNodeCfg]struct{}, len(s.leafs))
	for _, c := range s.leafs {
		c.mu.Lock()
		if c.leaf != nil && c.leaf.remote != nil {
			connected[c.leaf.remote] = struct{}{}
		}
		c.mu.Unlock()
	}
	var errs []string
	for cfg := range s.leafRemoteCfgs {
		if !cfg.stillValid() {
			continue
		}
		if _, ok := connected[cfg]; !ok {
			// Only report the account and redacted URLs. The remote name also includes
			// the credentials file path, which should not be exposed on this endpoint.
			acc := cfg.LocalAccount
			if acc == _EMPTY_ {
				acc = globalAccountName
			}
			errs = append(errs, fmt.Sprintf("leafnode remote for account %q (urls=%q) is not connected", acc, redactURLList(cfg.URLs)))
		}
	}
	sort.Strings(errs)
	return errs
}
