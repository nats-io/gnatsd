// Copyright 2013-2019 The NATS Authors
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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats-server/v2/server/pse"
)

// Snapshot this
var numCores int
var maxProcs int

func init() {
	numCores = runtime.NumCPU()
	maxProcs = runtime.GOMAXPROCS(0)
}

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

	// Filter by connection state.
	State ConnState `json:"state"`

	// The below options only apply if auth is true.

	// Filter by username.
	User string `json:"user"`

	// Filter by account.
	Account string `json:"acc"`
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
	Cid            uint64      `json:"cid"`
	IP             string      `json:"ip"`
	Port           int         `json:"port"`
	Start          time.Time   `json:"start"`
	LastActivity   time.Time   `json:"last_activity"`
	Stop           *time.Time  `json:"stop,omitempty"`
	Reason         string      `json:"reason,omitempty"`
	RTT            string      `json:"rtt,omitempty"`
	Uptime         string      `json:"uptime"`
	Idle           string      `json:"idle"`
	Pending        int         `json:"pending_bytes"`
	InMsgs         int64       `json:"in_msgs"`
	OutMsgs        int64       `json:"out_msgs"`
	InBytes        int64       `json:"in_bytes"`
	OutBytes       int64       `json:"out_bytes"`
	NumSubs        uint32      `json:"subscriptions"`
	Name           string      `json:"name,omitempty"`
	Lang           string      `json:"lang,omitempty"`
	Version        string      `json:"version,omitempty"`
	TLSVersion     string      `json:"tls_version,omitempty"`
	TLSCipher      string      `json:"tls_cipher_suite,omitempty"`
	AuthorizedUser string      `json:"authorized_user,omitempty"`
	Account        string      `json:"account,omitempty"`
	Subs           []string    `json:"subscriptions_list,omitempty"`
	SubsDetail     []SubDetail `json:"subscriptions_list_detail,omitempty"`
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
	)

	if opts != nil {
		// If no sort option given or sort is by uptime, then sort by cid
		if opts.Sort == "" {
			sortOpt = ByCid
		} else {
			sortOpt = opts.Sort
			if !sortOpt.IsValid() {
				return nil, fmt.Errorf("invalid sorting option: %s", sortOpt)
			}
		}

		// Auth specifics.
		auth = opts.Username
		if !auth && (user != "" || acc != "") {
			return nil, fmt.Errorf("filter by user or account only allowed with auth option")
		}
		user = opts.User
		acc = opts.Account

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
	}

	c := &Connz{
		Offset: offset,
		Limit:  limit,
		Now:    time.Now(),
	}

	// Open clients
	var openClients []*client
	// Hold for closed clients if requested.
	var closedClients []*closedClient

	// Walk the open client list with server lock held.
	s.mu.Lock()

	// copy the server id for monitoring
	c.ID = s.info.ID

	// Number of total clients. The resulting ConnInfo array
	// may be smaller if pagination is used.
	switch state {
	case ConnOpen:
		c.Total = len(s.clients)
	case ConnClosed:
		c.Total = s.closed.len()
		closedClients = s.closed.closedClients()
		c.Total = len(closedClients)
	case ConnAll:
		closedClients = s.closed.closedClients()
		c.Total = len(s.clients) + len(closedClients)
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
		if state == ConnClosed || state == ConnAll {
			copyClosed := closedClients
			closedClients = nil
			for _, cc := range copyClosed {
				if cc.Cid == cid {
					closedClients = []*closedClient{cc}
					break
				}
			}
		} else if state == ConnOpen || state == ConnAll {
			client := s.clients[cid]
			if client != nil {
				openClients = append(openClients, client)
			}
		}
	} else {
		// Gather all open clients.
		if state == ConnOpen || state == ConnAll {
			for _, client := range s.clients {
				// If we have an account specified we need to filter.
				if acc != "" && (client.acc == nil || client.acc.Name != acc) {
					continue
				}
				// Do user filtering second
				if user != "" && client.opts.Username != user {
					continue
				}
				openClients = append(openClients, client)
			}
		}
	}
	s.mu.Unlock()

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
		ci.fill(client, client.nc, c.Now)
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
			ci.AuthorizedUser = client.opts.Username
			// Add in account iff not the global account.
			if client.acc != nil && (client.acc.Name != globalAccountName) {
				ci.Account = client.acc.Name
			}
		}
		client.mu.Unlock()
		pconns[i] = ci
		i++
	}
	// Closed Clients
	var needCopy bool
	if subs || auth {
		needCopy = true
	}
	for _, cc := range closedClients {
		// If we have an account specified we need to filter.
		if acc != "" && cc.acc != acc {
			continue
		}
		// Do user filtering second
		if user != "" && cc.user != user {
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
			// Add in account iff not the global account.
			if cc.acc != "" && (cc.acc != globalAccountName) {
				cc.Account = cc.acc
			}
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
		sort.Sort(byCid{pconns})
	case BySubs:
		sort.Sort(sort.Reverse(bySubs{pconns}))
	case ByPending:
		sort.Sort(sort.Reverse(byPending{pconns}))
	case ByOutMsgs:
		sort.Sort(sort.Reverse(byOutMsgs{pconns}))
	case ByInMsgs:
		sort.Sort(sort.Reverse(byInMsgs{pconns}))
	case ByOutBytes:
		sort.Sort(sort.Reverse(byOutBytes{pconns}))
	case ByInBytes:
		sort.Sort(sort.Reverse(byInBytes{pconns}))
	case ByLast:
		sort.Sort(sort.Reverse(byLast{pconns}))
	case ByIdle:
		sort.Sort(sort.Reverse(byIdle{pconns}))
	case ByUptime:
		sort.Sort(byUptime{pconns, time.Now()})
	case ByStop:
		sort.Sort(sort.Reverse(byStop{pconns}))
	case ByReason:
		sort.Sort(byReason{pconns})
	}

	minoff := c.Offset
	maxoff := c.Offset + c.Limit

	maxIndex := totalClients

	// Make sure these are sane.
	if minoff > maxIndex {
		minoff = maxIndex
	}
	if maxoff > maxIndex {
		maxoff = maxIndex
	}

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
func (ci *ConnInfo) fill(client *client, nc net.Conn, now time.Time) {
	ci.Cid = client.cid
	ci.Start = client.start
	ci.LastActivity = client.last
	ci.Uptime = myUptime(now.Sub(client.start))
	ci.Idle = myUptime(now.Sub(client.last))
	ci.RTT = client.getRTT()
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

	// If the connection is gone, too bad, we won't set TLSVersion and TLSCipher.
	// Exclude clients that are still doing handshake so we don't block in
	// ConnectionState().
	if client.flags.isSet(handshakeComplete) && nc != nil {
		conn := nc.(*tls.Conn)
		cs := conn.ConnectionState()
		ci.TLSVersion = tlsVersion(cs.Version)
		ci.TLSCipher = tlsCipher(cs.CipherSuite)
	}

	if client.port != 0 {
		ci.Port = int(client.port)
		ci.IP = client.host
	}
}

// Assume lock is held
func (c *client) getRTT() string {
	if c.rtt == 0 {
		// If a real client, go ahead and send ping now to get a value
		// for RTT. For tests and telnet, or if client is closing, etc skip.
		if c.opts.Lang != "" {
			c.sendRTTPingLocked()
		}
		return ""
	}
	var rtt time.Duration
	if c.rtt > time.Microsecond && c.rtt < time.Millisecond {
		rtt = c.rtt.Truncate(time.Microsecond)
	} else {
		rtt = c.rtt.Truncate(time.Nanosecond)
	}
	return rtt.String()
}

func decodeBool(w http.ResponseWriter, r *http.Request, param string) (bool, error) {
	str := r.URL.Query().Get(param)
	if str == "" {
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
	if str == "" {
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
	if str == "" {
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
	if str == "" {
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

	connzOpts := &ConnzOptions{
		Sort:                sortOpt,
		Username:            auth,
		Subscriptions:       subs,
		SubscriptionsDetail: subsDet,
		Offset:              offset,
		Limit:               limit,
		CID:                 cid,
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
	DidSolicit   bool               `json:"did_solicit"`
	IsConfigured bool               `json:"is_configured"`
	IP           string             `json:"ip"`
	Port         int                `json:"port"`
	Import       *SubjectPermission `json:"import,omitempty"`
	Export       *SubjectPermission `json:"export,omitempty"`
	Pending      int                `json:"pending_size"`
	RTT          string             `json:"rtt,omitempty"`
	InMsgs       int64              `json:"in_msgs"`
	OutMsgs      int64              `json:"out_msgs"`
	InBytes      int64              `json:"in_bytes"`
	OutBytes     int64              `json:"out_bytes"`
	NumSubs      uint32             `json:"subscriptions"`
	Subs         []string           `json:"subscriptions_list,omitempty"`
	SubsDetail   []SubDetail        `json:"subscriptions_list_detail,omitempty"`
}

// Routez returns a Routez struct containing information about routes.
func (s *Server) Routez(routezOpts *RoutezOptions) (*Routez, error) {
	rs := &Routez{Routes: []*RouteInfo{}}
	rs.Now = time.Now()

	if routezOpts == nil {
		routezOpts = &RoutezOptions{}
	}

	s.mu.Lock()
	rs.NumRoutes = len(s.routes)

	// copy the server id for monitoring
	rs.ID = s.info.ID

	// Check for defined permissions for all connected routes.
	if perms := s.getOpts().Cluster.Permissions; perms != nil {
		rs.Import = perms.Import
		rs.Export = perms.Export
	}

	// Walk the list
	for _, r := range s.routes {
		r.mu.Lock()
		ri := &RouteInfo{
			Rid:          r.cid,
			RemoteID:     r.route.remoteID,
			DidSolicit:   r.route.didSolicit,
			IsConfigured: r.route.routeType == Explicit,
			InMsgs:       atomic.LoadInt64(&r.inMsgs),
			OutMsgs:      r.outMsgs,
			InBytes:      atomic.LoadInt64(&r.inBytes),
			OutBytes:     r.outBytes,
			NumSubs:      uint32(len(r.subs)),
			Import:       r.opts.Import,
			Export:       r.opts.Export,
			RTT:          r.getRTT(),
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
	Account string `json:"account,omitempty"`
	Subject string `json:"subject"`
	Queue   string `json:"qgroup,omitempty"`
	Sid     string `json:"sid"`
	Msgs    int64  `json:"msgs"`
	Max     int64  `json:"max,omitempty"`
	Cid     uint64 `json:"cid"`
}

// Subscription client should be locked and guaranteed to be present.
func newSubDetail(sub *subscription) SubDetail {
	sd := newClientSubDetail(sub)
	if sub.client.acc != nil {
		sd.Account = sub.client.acc.GetName()
	}
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
		limit     = DefaultSubListSize
		testSub   = ""
		filterAcc = ""
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
		if opts.Test != "" {
			testSub = opts.Test
			test = true
			if !IsValidLiteralSubject(testSub) {
				return nil, fmt.Errorf("invalid test subject, must be valid publish subject: %s", testSub)
			}
		}
		if opts.Account != "" {
			filterAcc = opts.Account
		}
	}

	slStats := &SublistStats{}

	// FIXME(dlc) - Make account aware.
	sz := &Subsz{s.info.ID, time.Now(), slStats, 0, offset, limit, nil}

	if subdetail {
		var raw [4096]*subscription
		subs := raw[:0]
		s.accounts.Range(func(k, v interface{}) bool {
			acc := v.(*Account)
			if filterAcc != "" && acc.GetName() != filterAcc {
				return true
			}
			slStats.add(acc.sl.Stats())
			acc.sl.localSubs(&subs)
			return true
		})

		details := make([]SubDetail, len(subs))
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
			details[i] = newSubDetail(sub)
			sub.client.mu.Unlock()
			i++
		}
		minoff := sz.Offset
		maxoff := sz.Offset + sz.Limit

		maxIndex := i

		// Make sure these are sane.
		if minoff > maxIndex {
			minoff = maxIndex
		}
		if maxoff > maxIndex {
			maxoff = maxIndex
		}
		sz.Subs = details[minoff:maxoff]
		sz.Total = len(sz.Subs)
	} else {
		s.accounts.Range(func(k, v interface{}) bool {
			acc := v.(*Account)
			if filterAcc != "" && acc.GetName() != filterAcc {
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

	if len(st.Subs) == 0 {
		b, err = json.MarshalIndent(st.SublistStats, "", "  ")
	} else {
		b, err = json.MarshalIndent(st, "", "  ")
	}
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

// Varz will output server information on the monitoring port at /varz.
type Varz struct {
	ID                string            `json:"server_id"`
	Name              string            `json:"server_name"`
	Version           string            `json:"version"`
	Proto             int               `json:"proto"`
	GitCommit         string            `json:"git_commit,omitempty"`
	GoVersion         string            `json:"go"`
	Host              string            `json:"host"`
	Port              int               `json:"port"`
	AuthRequired      bool              `json:"auth_required,omitempty"`
	TLSRequired       bool              `json:"tls_required,omitempty"`
	TLSVerify         bool              `json:"tls_verify,omitempty"`
	IP                string            `json:"ip,omitempty"`
	ClientConnectURLs []string          `json:"connect_urls,omitempty"`
	WSConnectURLs     []string          `json:"ws_connect_urls,omitempty"`
	MaxConn           int               `json:"max_connections"`
	MaxSubs           int               `json:"max_subscriptions,omitempty"`
	PingInterval      time.Duration     `json:"ping_interval"`
	MaxPingsOut       int               `json:"ping_max"`
	HTTPHost          string            `json:"http_host"`
	HTTPPort          int               `json:"http_port"`
	HTTPBasePath      string            `json:"http_base_path"`
	HTTPSPort         int               `json:"https_port"`
	AuthTimeout       float64           `json:"auth_timeout"`
	MaxControlLine    int32             `json:"max_control_line"`
	MaxPayload        int               `json:"max_payload"`
	MaxPending        int64             `json:"max_pending"`
	Cluster           ClusterOptsVarz   `json:"cluster,omitempty"`
	Gateway           GatewayOptsVarz   `json:"gateway,omitempty"`
	LeafNode          LeafNodeOptsVarz  `json:"leaf,omitempty"`
	JetStream         JetStreamVarz     `json:"jetstream,omitempty"`
	TLSTimeout        float64           `json:"tls_timeout"`
	WriteDeadline     time.Duration     `json:"write_deadline"`
	Start             time.Time         `json:"start"`
	Now               time.Time         `json:"now"`
	Uptime            string            `json:"uptime"`
	Mem               int64             `json:"mem"`
	Cores             int               `json:"cores"`
	MaxProcs          int               `json:"gomaxprocs"`
	CPU               float64           `json:"cpu"`
	Connections       int               `json:"connections"`
	TotalConnections  uint64            `json:"total_connections"`
	Routes            int               `json:"routes"`
	Remotes           int               `json:"remotes"`
	Leafs             int               `json:"leafnodes"`
	InMsgs            int64             `json:"in_msgs"`
	OutMsgs           int64             `json:"out_msgs"`
	InBytes           int64             `json:"in_bytes"`
	OutBytes          int64             `json:"out_bytes"`
	SlowConsumers     int64             `json:"slow_consumers"`
	Subscriptions     uint32            `json:"subscriptions"`
	HTTPReqStats      map[string]uint64 `json:"http_req_stats"`
	ConfigLoadTime    time.Time         `json:"config_load_time"`
}

// JetStreamVarz contains basic runtime information about jetstream
type JetStreamVarz struct {
	MaxMemory int64  `json:"max_memory,omitempty"`
	MaxStore  int64  `json:"max_store,omitempty"`
	StoreDir  string `json:"store_dir,omitempty"`
	Accounts  int    `json:"accounts,omitempty"`
}

// ClusterOptsVarz contains monitoring cluster information
type ClusterOptsVarz struct {
	Name        string   `json:"name,omitempty"`
	Host        string   `json:"addr,omitempty"`
	Port        int      `json:"cluster_port,omitempty"`
	AuthTimeout float64  `json:"auth_timeout,omitempty"`
	URLs        []string `json:"urls,omitempty"`
}

// GatewayOptsVarz contains monitoring gateway information
type GatewayOptsVarz struct {
	Name           string                  `json:"name,omitempty"`
	Host           string                  `json:"host,omitempty"`
	Port           int                     `json:"port,omitempty"`
	AuthTimeout    float64                 `json:"auth_timeout,omitempty"`
	TLSTimeout     float64                 `json:"tls_timeout,omitempty"`
	Advertise      string                  `json:"advertise,omitempty"`
	ConnectRetries int                     `json:"connect_retries,omitempty"`
	Gateways       []RemoteGatewayOptsVarz `json:"gateways,omitempty"`
	RejectUnknown  bool                    `json:"reject_unknown,omitempty"`
}

// RemoteGatewayOptsVarz contains monitoring remote gateway information
type RemoteGatewayOptsVarz struct {
	Name       string   `json:"name"`
	TLSTimeout float64  `json:"tls_timeout,omitempty"`
	URLs       []string `json:"urls,omitempty"`
}

// LeafNodeOptsVarz contains monitoring leaf node information
type LeafNodeOptsVarz struct {
	Host        string               `json:"host,omitempty"`
	Port        int                  `json:"port,omitempty"`
	AuthTimeout float64              `json:"auth_timeout,omitempty"`
	TLSTimeout  float64              `json:"tls_timeout,omitempty"`
	Remotes     []RemoteLeafOptsVarz `json:"remotes,omitempty"`
}

// RemoteLeafOptsVarz contains monitoring remote leaf node information
type RemoteLeafOptsVarz struct {
	LocalAccount string   `json:"local_account,omitempty"`
	TLSTimeout   float64  `json:"tls_timeout,omitempty"`
	URLs         []string `json:"urls,omitempty"`
}

// VarzOptions are the options passed to Varz().
// Currently, there are no options defined.
type VarzOptions struct{}

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
	fmt.Fprintf(w, `<html lang="en">
   <head>
    <link rel="shortcut icon" href="http://nats.io/img/favicon.ico">
    <style type="text/css">
      body { font-family: "Century Gothic", CenturyGothic, AppleGothic, sans-serif; font-size: 22; }
      a { margin-left: 32px; }
    </style>
  </head>
  <body>
    <img src="http://nats.io/img/logo.png" alt="NATS">
    <br/>
	<a href=.%s>varz</a><br/>
	<a href=.%s>connz</a><br/>
	<a href=.%s>routez</a><br/>
	<a href=.%s>gatewayz</a><br/>
	<a href=.%s>leafz</a><br/>
	<a href=.%s>subsz</a><br/>
    <br/>
    <a href=https://docs.nats.io/nats-server/configuration/monitoring.html>help</a>
  </body>
</html>`,
		s.basePath(VarzPath),
		s.basePath(ConnzPath),
		s.basePath(RoutezPath),
		s.basePath(GatewayzPath),
		s.basePath(LeafzPath),
		s.basePath(SubszPath),
	)
}

// Varz returns a Varz struct containing the server information.
func (s *Server) Varz(varzOpts *VarzOptions) (*Varz, error) {
	var rss, vss int64
	var pcpu float64

	// We want to do that outside of the lock.
	pse.ProcUsage(&pcpu, &rss, &vss)

	s.mu.Lock()
	// We need to create a new instance of Varz (with no reference
	// whatsoever to anything stored in the server) since the user
	// has access to the returned value.
	v := s.createVarz(pcpu, rss)
	s.mu.Unlock()

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
			Name:        info.Cluster,
			Host:        c.Host,
			Port:        c.Port,
			AuthTimeout: c.AuthTimeout,
		},
		Gateway: GatewayOptsVarz{
			Name:           gw.Name,
			Host:           gw.Host,
			Port:           gw.Port,
			AuthTimeout:    gw.AuthTimeout,
			TLSTimeout:     gw.TLSTimeout,
			Advertise:      gw.Advertise,
			ConnectRetries: gw.ConnectRetries,
			Gateways:       []RemoteGatewayOptsVarz{},
			RejectUnknown:  gw.RejectUnknown,
		},
		LeafNode: LeafNodeOptsVarz{
			Host:        ln.Host,
			Port:        ln.Port,
			AuthTimeout: ln.AuthTimeout,
			TLSTimeout:  ln.TLSTimeout,
			Remotes:     []RemoteLeafOptsVarz{},
		},
		Start:    s.start,
		MaxSubs:  opts.MaxSubs,
		Cores:    numCores,
		MaxProcs: maxProcs,
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
	if l := len(ln.Remotes); l > 0 {
		rlna := make([]RemoteLeafOptsVarz, l)
		for i, r := range ln.Remotes {
			rlna[i] = RemoteLeafOptsVarz{
				LocalAccount: r.LocalAccount,
				URLs:         urlsToStrings(r.URLs),
				TLSTimeout:   r.TLSTimeout,
			}
		}
		varz.LeafNode.Remotes = rlna
	}

	if s.js != nil {
		s.js.mu.RLock()
		varz.JetStream = JetStreamVarz{
			MaxMemory: s.js.config.MaxMemory,
			MaxStore:  s.js.config.MaxStore,
			StoreDir:  s.js.config.StoreDir,
		}
		s.js.mu.RUnlock()
	}

	// Finish setting it up with fields that can be updated during
	// configuration reload and runtime.
	s.updateVarzConfigReloadableFields(varz)
	s.updateVarzRuntimeFields(varz, true, pcpu, rss)
	return varz
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
	v.ConfigLoadTime = s.configTime
	// Update route URLs if applicable
	if s.varzUpdateRouteURLs {
		v.Cluster.URLs = urlsToStrings(opts.Routes)
		s.varzUpdateRouteURLs = false
	}
}

// Updates the runtime Varz fields, that is, fields that change during
// runtime and that should be updated any time Varz() or polling of /varz
// is done.
// Server lock is held on entry.
func (s *Server) updateVarzRuntimeFields(v *Varz, forceUpdate bool, pcpu float64, rss int64) {
	v.Now = time.Now()
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
	v.Routes = len(s.routes)
	v.Remotes = len(s.remotes)
	v.Leafs = len(s.leafs)
	v.InMsgs = atomic.LoadInt64(&s.inMsgs)
	v.InBytes = atomic.LoadInt64(&s.inBytes)
	v.OutMsgs = atomic.LoadInt64(&s.outMsgs)
	v.OutBytes = atomic.LoadInt64(&s.outBytes)
	v.SlowConsumers = atomic.LoadInt64(&s.slowConsumers)
	// FIXME(dlc) - make this multi-account aware.
	v.Subscriptions = s.gacc.sl.Count()
	v.HTTPReqStats = make(map[string]uint64, len(s.httpReqStats))
	for key, val := range s.httpReqStats {
		v.HTTPReqStats[key] = val
	}
	v.Cluster.Name = s.info.Cluster

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
			}
		}
	}
	gw.RUnlock()

	if s.js != nil {
		s.js.mu.RLock()
		v.JetStream.Accounts = len(s.js.accounts)
		s.js.mu.RUnlock()
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
	s.httpReqStats[VarzPath]++
	if s.varz == nil {
		s.varz = s.createVarz(pcpu, rss)
	} else {
		s.updateVarzRuntimeFields(s.varz, false, pcpu, rss)
	}
	s.mu.Unlock()

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
	Name                  string `json:"name"`
	InterestMode          string `json:"interest_mode"`
	NoInterestCount       int    `json:"no_interest_count,omitempty"`
	InterestOnlyThreshold int    `json:"interest_only_threshold,omitempty"`
	TotalSubscriptions    int    `json:"num_subs,omitempty"`
	NumQueueSubscriptions int    `json:"num_queue_subs,omitempty"`
}

// Gatewayz returns a Gatewayz struct containing information about gateways.
func (s *Server) Gatewayz(opts *GatewayzOptions) (*Gatewayz, error) {
	srvID := s.ID()
	now := time.Now()
	gw := s.gateway
	gw.RLock()
	if !gw.enabled {
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
		rgw.Connection.fill(c, c.nc, now)
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
		a := createAccountOutboundGatewayz(accName, ei)
		return []*AccountGatewayz{a}
	}

	accs := make([]*AccountGatewayz, 0, 4)
	gw.outsim.Range(func(k, v interface{}) bool {
		name := k.(string)
		a := createAccountOutboundGatewayz(name, v)
		accs = append(accs, a)
		return true
	})
	return accs
}

// Returns an AccountGatewayz for this gateway outbound connection
func createAccountOutboundGatewayz(name string, ei interface{}) *AccountGatewayz {
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
			rgw.Connection.fill(c, c.nc, now)
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
		Name:        gwName,
		Accounts:    accs,
		AccountName: accName,
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
	Subscriptions bool `json:"subscriptions"`
}

// LeafInfo has detailed information on each remote leafnode connection.
type LeafInfo struct {
	Account  string   `json:"account"`
	IP       string   `json:"ip"`
	Port     int      `json:"port"`
	RTT      string   `json:"rtt,omitempty"`
	InMsgs   int64    `json:"in_msgs"`
	OutMsgs  int64    `json:"out_msgs"`
	InBytes  int64    `json:"in_bytes"`
	OutBytes int64    `json:"out_bytes"`
	NumSubs  uint32   `json:"subscriptions"`
	Subs     []string `json:"subscriptions_list,omitempty"`
}

// Leafz returns a Leafz structure containing information about leafnodes.
func (s *Server) Leafz(opts *LeafzOptions) (*Leafz, error) {
	// Grab leafnodes
	var lconns []*client
	s.mu.Lock()
	if len(s.leafs) > 0 {
		lconns = make([]*client, 0, len(s.leafs))
		for _, ln := range s.leafs {
			lconns = append(lconns, ln)
		}
	}
	s.mu.Unlock()

	var leafnodes []*LeafInfo
	if len(lconns) > 0 {
		leafnodes = make([]*LeafInfo, 0, len(lconns))
		for _, ln := range lconns {
			ln.mu.Lock()
			lni := &LeafInfo{
				Account:  ln.acc.Name,
				IP:       ln.host,
				Port:     int(ln.port),
				RTT:      ln.getRTT(),
				InMsgs:   atomic.LoadInt64(&ln.inMsgs),
				OutMsgs:  ln.outMsgs,
				InBytes:  atomic.LoadInt64(&ln.inBytes),
				OutBytes: ln.outBytes,
				NumSubs:  uint32(len(ln.subs)),
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
		Now:      time.Now(),
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
	var opts *LeafzOptions
	if subs {
		opts = &LeafzOptions{Subscriptions: true}
	}

	l, err := s.Leafz(opts)
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

// ResponseHandler handles responses for monitoring routes
func ResponseHandler(w http.ResponseWriter, r *http.Request, data []byte) {
	// Get callback from request
	callback := r.URL.Query().Get("callback")
	// If callback is not empty then
	if callback != "" {
		// Response for JSONP
		w.Header().Set("Content-Type", "application/javascript")
		fmt.Fprintf(w, "%s(%s)", callback, data)
	} else {
		// Otherwise JSON
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	}
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
	}

	return "Unknown State"
}
