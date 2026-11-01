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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"net"
	"net/url"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/s2"
)

// RouteType designates the router type
type RouteType int

// Type of Route
const (
	// This route we learned from speaking to other routes.
	Implicit RouteType = iota
	// This route was explicitly configured.
	Explicit
)

// Include the space for the proto
var (
	aSubBytes   = []byte{'A', '+', ' '}
	aUnsubBytes = []byte{'A', '-', ' '}
	rSubBytes   = []byte{'R', 'S', '+', ' '}
	rUnsubBytes = []byte{'R', 'S', '-', ' '}
	lSubBytes   = []byte{'L', 'S', '+', ' '}
	lUnsubBytes = []byte{'L', 'S', '-', ' '}
)

const leafNoOriginCluster = "_"

type route struct {
	remoteID     string
	remoteName   string
	didSolicit   bool
	retry        bool
	lnoc         bool
	lnocu        bool
	ln           bool
	routeType    RouteType
	url          *url.URL
	authRequired bool
	tlsRequired  bool
	jetstream    bool
	connectURLs  []string
	wsConnURLs   []string
	gatewayURL   string
	leafnodeURL  string
	hash         string
	idHash       string
	// Location of the route in the slice: s.routes[remoteID][]*client.
	// Initialized to -1 on creation, as to indicate that it is not
	// added to the list.
	poolIdx int
	// If this is set, it means that the route is dedicated for this
	// account and the account name will not be included in protocols.
	accName []byte
	// This is set to true if this is a route connection to an old
	// server or a server that has pooling completely disabled.
	noPool bool
	// Selected compression mode, which may be different from the
	// server configured mode.
	compression string
	// Transient value used to set the Info.GossipMode when initiating
	// an implicit route and sending to the remote.
	gossipMode byte
	// This will be set in case of pooling so that a route can trigger
	// the creation of the next after receiving a PONG, ensuring
	// that authentication did not fail.
	startNewRoute *routeInfo
}

// This contains the information required to create a new route.
type routeInfo struct {
	url        *url.URL
	rtype      RouteType
	gossipMode byte
}

// Do not change the values/order since they are exchanged between servers.
const (
	gossipDefault = byte(iota)
	gossipDisabled
	gossipOverride
)

type connectInfo struct {
	Echo     bool   `json:"echo"`
	Verbose  bool   `json:"verbose"`
	Pedantic bool   `json:"pedantic"`
	User     string `json:"user,omitempty"`
	Pass     string `json:"pass,omitempty"`
	TLS      bool   `json:"tls_required"`
	Headers  bool   `json:"headers"`
	Name     string `json:"name"`
	Cluster  string `json:"cluster"`
	Dynamic  bool   `json:"cluster_dynamic,omitempty"`
	LNOC     bool   `json:"lnoc,omitempty"`
	LNOCU    bool   `json:"lnocu,omitempty"` // Support for LS- with origin cluster name
	LN       bool   `json:"ln,omitempty"`    // Support for LS+/LS- leaf interest using a sentinel origin cluster
	Gateway  string `json:"gateway,omitempty"`
}

// Route protocol constants
const (
	ConProto  = "CONNECT %s" + _CRLF_
	InfoProto = "INFO %s" + _CRLF_
)

const (
	// Warning when user configures cluster TLS insecure
	clusterTLSInsecureWarning = "TLS certificate chain and hostname of solicited routes will not be verified. DO NOT USE IN PRODUCTION!"

	// The default ping interval is set to 2 minutes, which is fine for client
	// connections, etc.. but for route compression, the CompressionS2Auto
	// mode uses RTT measurements (ping/pong) to decide which compression level
	// to use, we want the interval to not be that high.
	defaultRouteMaxPingInterval = 30 * time.Second
)

// Can be changed for tests
var (
	routeConnectDelay    = DEFAULT_ROUTE_CONNECT
	routeConnectMaxDelay = DEFAULT_ROUTE_CONNECT_MAX
	routeReconnectDelay  = DEFAULT_ROUTE_RECONNECT
	routeMaxPingInterval = defaultRouteMaxPingInterval
)

// removeReplySub is called when we trip the max on remoteReply subs.
func (c *client) removeReplySub(sub *subscription) {
	if sub == nil {
		return
	}
	// Lookup the account based on sub.sid.
	if i := bytes.Index(sub.sid, []byte(" ")); i > 0 {
		// First part of SID for route is account name.
		if v, ok := c.srv.accounts.Load(bytesToString(sub.sid[:i])); ok {
			(v.(*Account)).sl.Remove(sub)
		}
		c.mu.Lock()
		delete(c.subs, bytesToString(sub.sid))
		c.mu.Unlock()
	}
}

func (c *client) processAccountSub(arg []byte) error {
	if c.kind == GATEWAY {
		return c.processGatewayAccountSub(string(arg))
	}
	return nil
}

func (c *client) processAccountUnsub(arg []byte) {
	if c.kind == GATEWAY {
		c.processGatewayAccountUnsub(string(arg))
	}
}

// Process an inbound LMSG specification from the remote route. This means
// we have an origin cluster and we force header semantics.
func (c *client) processRoutedOriginClusterMsgArgs(arg []byte) error {
	// Unroll splitArgs to avoid runtime/heap issues
	args := c.argsa[:0]
	start := -1
	for i, b := range arg {
		switch b {
		case ' ', '\t', '\r', '\n':
			if start >= 0 {
				args = append(args, arg[start:i])
				start = -1
			}
		default:
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		args = append(args, arg[start:])
	}

	var an []byte
	if c.kind == ROUTER {
		if an = c.route.accName; len(an) > 0 && len(args) > 2 {
			args = append(args[:2], args[1:]...)
			args[1] = an
		}
	}
	c.pa.arg = arg
	switch len(args) {
	case 0, 1, 2, 3, 4:
		return fmt.Errorf("processRoutedOriginClusterMsgArgs Parse Error: '%s'", args)
	case 5:
		c.pa.reply = nil
		c.pa.queues = nil
		c.pa.hdb = args[3]
		c.pa.hdr = parseSize(args[3])
		c.pa.szb = args[4]
		c.pa.size = parseSize(args[4])
	case 6:
		c.pa.reply = args[3]
		c.pa.queues = nil
		c.pa.hdb = args[4]
		c.pa.hdr = parseSize(args[4])
		c.pa.szb = args[5]
		c.pa.size = parseSize(args[5])
	default:
		// args[2] is our reply indicator. Should be + or | normally.
		if len(args[3]) != 1 {
			return fmt.Errorf("processRoutedOriginClusterMsgArgs Bad or Missing Reply Indicator: '%s'", args[3])
		}
		switch args[3][0] {
		case '+':
			c.pa.reply = args[4]
		case '|':
			c.pa.reply = nil
		default:
			return fmt.Errorf("processRoutedOriginClusterMsgArgs Bad or Missing Reply Indicator: '%s'", args[3])
		}

		// Grab header size.
		c.pa.hdb = args[len(args)-2]
		c.pa.hdr = parseSize(c.pa.hdb)

		// Grab size.
		c.pa.szb = args[len(args)-1]
		c.pa.size = parseSize(c.pa.szb)

		// Grab queue names.
		if c.pa.reply != nil {
			c.pa.queues = args[5 : len(args)-2]
		} else {
			c.pa.queues = args[4 : len(args)-2]
		}
	}
	if c.pa.hdr < 0 {
		return fmt.Errorf("processRoutedOriginClusterMsgArgs Bad or Missing Header Size: '%s'", arg)
	}
	if c.pa.size < 0 {
		return fmt.Errorf("processRoutedOriginClusterMsgArgs Bad or Missing Size: '%s'", args)
	}
	if c.pa.hdr > c.pa.size {
		return fmt.Errorf("processRoutedOriginClusterMsgArgs Header Size larger then TotalSize: '%s'", arg)
	}

	// Common ones processed after check for arg length
	c.pa.origin = args[0]
	c.pa.account = args[1]
	c.pa.subject = args[2]
	if len(an) > 0 {
		c.pa.pacache = c.pa.subject
	} else {
		c.pa.pacache = arg[len(args[0])+1 : len(args[0])+len(args[1])+len(args[2])+2]
	}
	return nil
}

// Process an inbound HMSG specification from the remote route.
func (c *client) processRoutedHeaderMsgArgs(arg []byte) error {
	// Unroll splitArgs to avoid runtime/heap issues
	args := c.argsa[:0]
	var an []byte
	if c.kind == ROUTER {
		if an = c.route.accName; len(an) > 0 {
			args = append(args, an)
		}
	} else if c.kind == GATEWAY && c.gw.outbound {
		return fmt.Errorf("processRoutedHeaderMsgArgs received HMSG from gateway on outbound connection")
	}
	start := -1
	for i, b := range arg {
		switch b {
		case ' ', '\t', '\r', '\n':
			if start >= 0 {
				args = append(args, arg[start:i])
				start = -1
			}
		default:
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		args = append(args, arg[start:])
	}

	c.pa.arg = arg
	switch len(args) {
	case 0, 1, 2, 3:
		return fmt.Errorf("processRoutedHeaderMsgArgs Parse Error: '%s'", args)
	case 4:
		c.pa.reply = nil
		c.pa.queues = nil
		c.pa.hdb = args[2]
		c.pa.hdr = parseSize(args[2])
		c.pa.szb = args[3]
		c.pa.size = parseSize(args[3])
	case 5:
		c.pa.reply = args[2]
		c.pa.queues = nil
		c.pa.hdb = args[3]
		c.pa.hdr = parseSize(args[3])
		c.pa.szb = args[4]
		c.pa.size = parseSize(args[4])
	default:
		// args[2] is our reply indicator. Should be + or | normally.
		if len(args[2]) != 1 {
			return fmt.Errorf("processRoutedHeaderMsgArgs Bad or Missing Reply Indicator: '%s'", args[2])
		}
		switch args[2][0] {
		case '+':
			c.pa.reply = args[3]
		case '|':
			c.pa.reply = nil
		default:
			return fmt.Errorf("processRoutedHeaderMsgArgs Bad or Missing Reply Indicator: '%s'", args[2])
		}

		// Grab header size.
		c.pa.hdb = args[len(args)-2]
		c.pa.hdr = parseSize(c.pa.hdb)

		// Grab size.
		c.pa.szb = args[len(args)-1]
		c.pa.size = parseSize(c.pa.szb)

		// Grab queue names.
		if c.pa.reply != nil {
			c.pa.queues = args[4 : len(args)-2]
		} else {
			c.pa.queues = args[3 : len(args)-2]
		}
	}
	if c.pa.hdr < 0 {
		return fmt.Errorf("processRoutedHeaderMsgArgs Bad or Missing Header Size: '%s'", arg)
	}
	if c.pa.size < 0 {
		return fmt.Errorf("processRoutedHeaderMsgArgs Bad or Missing Size: '%s'", args)
	}
	if c.pa.hdr > c.pa.size {
		return fmt.Errorf("processRoutedHeaderMsgArgs Header Size larger then TotalSize: '%s'", arg)
	}

	// Common ones processed after check for arg length
	c.pa.account = args[0]
	c.pa.subject = args[1]
	if len(an) > 0 {
		c.pa.pacache = c.pa.subject
	} else {
		c.pa.pacache = arg[:len(args[0])+len(args[1])+1]
	}
	return nil
}

// Process an inbound RMSG or LMSG specification from the remote route.
func (c *client) processRoutedMsgArgs(arg []byte) error {
	// Unroll splitArgs to avoid runtime/heap issues
	args := c.argsa[:0]
	var an []byte
	if c.kind == ROUTER {
		if an = c.route.accName; len(an) > 0 {
			args = append(args, an)
		}
	} else if c.kind == GATEWAY && c.gw.outbound {
		return fmt.Errorf("processRoutedMsgArgs received RMSG from gateway on outbound connection")
	}
	start := -1
	for i, b := range arg {
		switch b {
		case ' ', '\t', '\r', '\n':
			if start >= 0 {
				args = append(args, arg[start:i])
				start = -1
			}
		default:
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		args = append(args, arg[start:])
	}

	c.pa.arg = arg
	switch len(args) {
	case 0, 1, 2:
		return fmt.Errorf("processRoutedMsgArgs Parse Error: '%s'", args)
	case 3:
		c.pa.reply = nil
		c.pa.queues = nil
		c.pa.szb = args[2]
		c.pa.size = parseSize(args[2])
	case 4:
		c.pa.reply = args[2]
		c.pa.queues = nil
		c.pa.szb = args[3]
		c.pa.size = parseSize(args[3])
	default:
		// args[2] is our reply indicator. Should be + or | normally.
		if len(args[2]) != 1 {
			return fmt.Errorf("processRoutedMsgArgs Bad or Missing Reply Indicator: '%s'", args[2])
		}
		switch args[2][0] {
		case '+':
			c.pa.reply = args[3]
		case '|':
			c.pa.reply = nil
		default:
			return fmt.Errorf("processRoutedMsgArgs Bad or Missing Reply Indicator: '%s'", args[2])
		}
		// Grab size.
		c.pa.szb = args[len(args)-1]
		c.pa.size = parseSize(c.pa.szb)

		// Grab queue names.
		if c.pa.reply != nil {
			c.pa.queues = args[4 : len(args)-1]
		} else {
			c.pa.queues = args[3 : len(args)-1]
		}
	}
	if c.pa.size < 0 {
		return fmt.Errorf("processRoutedMsgArgs Bad or Missing Size: '%s'", args)
	}

	// Common ones processed after check for arg length
	c.pa.account = args[0]
	c.pa.subject = args[1]
	if len(an) > 0 {
		c.pa.pacache = c.pa.subject
	} else {
		c.pa.pacache = arg[:len(args[0])+len(args[1])+1]
	}
	return nil
}

// processInboundRoutedMsg is called to process an inbound msg from a route.
func (c *client) processInboundRoutedMsg(msg []byte) {
	// Update statistics
	c.in.msgs++
	// The msg includes the CR_LF, so pull back out for accounting.
	size := len(msg) - LEN_CR_LF
	c.in.bytes += int32(size)

	if c.opts.Verbose {
		c.sendOK()
	}

	// Mostly under testing scenarios.
	if c.srv == nil {
		return
	}

	// If the subject (c.pa.subject) has the gateway prefix, this function will handle it.
	if c.handleGatewayReply(msg) {
		// We are done here.
		return
	}

	acc, r := c.getAccAndResultFromCache()
	if acc == nil {
		c.Debugf("Unknown account %q for routed message on subject: %q", c.pa.account, c.pa.subject)
		return
	}

	acc.stats.Lock()
	acc.stats.inMsgs++
	acc.stats.inBytes += int64(size)
	acc.stats.rt.inMsgs++
	acc.stats.rt.inBytes += int64(size)
	acc.stats.Unlock()

	// Check for no interest, short circuit if so.
	// This is the fanout scale.
	if len(r.psubs)+len(r.qsubs) > 0 {
		c.processMsgResults(acc, r, msg, nil, c.pa.subject, c.pa.reply, pmrNoFlag)
	}
}

// Lock should be held entering here.
func (c *client) sendRouteConnect(clusterName string, tlsRequired bool) error {
	var user, pass string
	if userInfo := c.route.url.User; userInfo != nil {
		user = userInfo.Username()
		pass, _ = userInfo.Password()
	}
	s := c.srv
	cinfo := connectInfo{
		Echo:     true,
		Verbose:  false,
		Pedantic: false,
		User:     user,
		Pass:     pass,
		TLS:      tlsRequired,
		Name:     s.info.ID,
		Headers:  s.supportsHeaders(),
		Cluster:  clusterName,
		Dynamic:  s.isClusterNameDynamic(),
		LNOC:     true,
		LN:       true,
	}

	b, err := json.Marshal(cinfo)
	if err != nil {
		c.Errorf("Error marshaling CONNECT to route: %v\n", err)
		return err
	}
	c.enqueueProto([]byte(fmt.Sprintf(ConProto, b)))
	return nil
}

// Returns a route pool index for this account based on the given pool size.
// If `poolSize` is smaller or equal to 1, the returned value will always
// be 0, regardless of the account name. If not, the returned value will
// be in the range [0..poolSize-1]. The value for a given account name
// is constant and same on all servers (given the same `poolSize` value).
func computeRoutePoolIdx(poolSize int, an string) int {
	if poolSize <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(an))
	sum32 := h.Sum32()
	return int((sum32 % uint32(poolSize)))
}

// Process the info message if we are a route.
func (c *client) processRouteInfo(info *Info) {

	supportsHeaders := c.srv.supportsHeaders()
	clusterName := c.srv.ClusterName()
	srvName := c.srv.Name()

	c.mu.Lock()
	// Connection can be closed at any time (by auth timeout, etc).
	// Does not make sense to continue here if connection is gone.
	if c.route == nil || c.isClosed() {
		c.mu.Unlock()
		return
	}

	s := c.srv

	// Detect route to self.
	if info.ID == s.info.ID {
		// Need to set this so that the close does the right thing
		c.route.remoteID = info.ID
		c.mu.Unlock()
		c.closeConnection(DuplicateRoute)
		return
	}
	if info.Cluster == leafNoOriginCluster {
		c.mu.Unlock()
		c.closeConnection(ClusterNameConflict)
		return
	}

	// Detect if we have a mismatch of cluster names.
	if info.Cluster != "" && info.Cluster != clusterName {
		c.mu.Unlock()
		// If we are dynamic we may update our cluster name.
		// Use other if remote is non dynamic or their name is "bigger"
		if s.isClusterNameDynamic() && (!info.Dynamic || (strings.Compare(clusterName, info.Cluster) < 0)) {
			s.setClusterName(info.Cluster)
			s.removeAllRoutesExcept(info.ID)
			c.mu.Lock()
		} else {
			c.closeConnection(ClusterNameConflict)
			return
		}
	}

	opts := s.getOpts()

	didSolicit := c.route.didSolicit

	// If this is an async INFO from an existing route...
	if c.flags.isSet(infoReceived) {
		remoteID := c.route.remoteID

		// Check if this is an INFO about adding a per-account route during
		// a configuration reload.
		if info.RouteAccReqID != _EMPTY_ {
			c.mu.Unlock()

			// If there is an account name, then the remote server is telling
			// us that this account will now have its dedicated route.
			if an := info.RouteAccount; an != _EMPTY_ {
				acc, err := s.LookupAccount(an)
				if err != nil {
					s.Errorf("Error looking up account %q: %v", an, err)
					return
				}
				s.mu.Lock()
				// If running without system account and adding a dedicated
				// route for an account for the first time, it could be that
				// the map is nil. If so, create it.
				if s.accRoutes == nil {
					s.accRoutes = make(map[string]map[string]*client)
				}
				if _, ok := s.accRoutes[an]; !ok {
					s.accRoutes[an] = make(map[string]*client)
				}
				acc.mu.Lock()
				sl := acc.sl
				rpi := acc.routePoolIdx
				// Make sure that the account was not already switched.
				if rpi >= 0 {
					s.setRouteInfo(acc)
					// Change the route pool index to indicate that this
					// account is actually transitioning. This will be used
					// to suppress possible remote subscription interest coming
					// in while the transition is happening.
					acc.routePoolIdx = accTransitioningToDedicatedRoute
				}
				acc.mu.Unlock()
				// Since v2.11.0, we support remotes with a different pool size
				// (for rolling upgrades), so we need to use the remote route
				// pool index (based on the remote configured pool size) since
				// the remote subscriptions will be attached to the route at
				// that index, not at our account's route pool index. But we
				// need to compute only if rpi is negative or the pool sizes
				// are different.
				if rpi <= 0 || info.RoutePoolSize != s.routesPoolSize {
					rpi = computeRoutePoolIdx(info.RoutePoolSize, an)
				}
				// Go over each remote's route at pool index `rpi` and remove
				// remote subs for this account.
				s.forEachRouteIdx(rpi, func(r *client) bool {
					r.mu.Lock()
					// Exclude routes to servers that don't support pooling.
					if !r.route.noPool {
						if subs := r.removeRemoteSubsForAcc(an); len(subs) > 0 {
							sl.RemoveBatch(subs)
						}
					}
					r.mu.Unlock()
					return true
				})
				// Respond to the remote by clearing the RouteAccount field.
				info.RouteAccount = _EMPTY_
				proto := generateInfoJSON(info)
				c.mu.Lock()
				c.enqueueProto(proto)
				c.mu.Unlock()
				s.mu.Unlock()
			} else {
				// If no account name is specified, this is a response from the
				// remote. Simply send to the communication channel, if the
				// request ID matches the current one.
				s.mu.Lock()
				if info.RouteAccReqID == s.accAddedReqID && s.accAddedCh != nil {
					select {
					case s.accAddedCh <- struct{}{}:
					default:
					}
				}
				s.mu.Unlock()
			}
			// In both cases, we are done here.
			return
		}

		// Check if this is an INFO for gateways...
		if info.Gateway != _EMPTY_ {
			c.mu.Unlock()
			// If this server has no gateway configured, report error and return.
			if !s.gateway.enabled {
				// FIXME: Should this be a Fatalf()?
				s.Errorf("Received information about gateway %q from %s, but gateway is not configured",
					info.Gateway, remoteID)
				return
			}
			s.processGatewayInfoFromRoute(info, remoteID)
			return
		}

		// We receive an INFO from a server that informs us about another server,
		// so the info.ID in the INFO protocol does not match the ID of this route.
		if remoteID != _EMPTY_ && remoteID != info.ID {
			// We want to know if the existing route supports pooling/pinned-account
			// or not when processing the implicit route.
			noPool := c.route.noPool
			c.mu.Unlock()

			// Process this implicit route. We will check that it is not an explicit
			// route and/or that it has not been connected already.
			s.processImplicitRoute(info, noPool)
			return
		}

		var connectURLs []string
		var wsConnectURLs []string
		var updateRoutePerms bool

		// If we are notified that the remote is going into LDM mode, capture route's connectURLs.
		if info.LameDuckMode {
			connectURLs = c.route.connectURLs
			wsConnectURLs = c.route.wsConnURLs
		} else {
			// Update only if we detect a difference
			updateRoutePerms = !reflect.DeepEqual(c.opts.Import, info.Import) || !reflect.DeepEqual(c.opts.Export, info.Export)
		}
		c.mu.Unlock()

		if updateRoutePerms {
			s.updateRemoteRoutePerms(c, info)
		}

		// If the remote is going into LDM and there are client connect URLs
		// associated with this route and we are allowed to advertise, remove
		// those URLs and update our clients.
		if (len(connectURLs) > 0 || len(wsConnectURLs) > 0) && !opts.Cluster.NoAdvertise {
			s.mu.Lock()
			s.removeConnectURLsAndSendINFOToClients(connectURLs, wsConnectURLs)
			s.mu.Unlock()
		}
		return
	}

	// Check if remote has same server name than this server.
	if !didSolicit && info.Name == srvName {
		c.mu.Unlock()
		// This is now an error and we close the connection. We need unique names for JetStream clustering.
		c.Errorf("Remote server has a duplicate name: %q", info.Name)
		c.closeConnection(DuplicateServerName)
		return
	}

	var sendDelayedInfo bool

	// First INFO, check if this server is configured for compression because
	// if that is the case, we need to negotiate it with the remote server.
	if needsCompression(opts.Cluster.Compression.Mode) {
		accName := bytesToString(c.route.accName)
		// If we did not yet negotiate...
		compNeg := c.flags.isSet(compressionNegotiated)
		if !compNeg {
			// Prevent from getting back here.
			c.flags.set(compressionNegotiated)
			// Release client lock since following function will need server lock.
			c.mu.Unlock()
			compress, err := s.negotiateRouteCompression(c, didSolicit, accName, info.Compression, opts)
			if err != nil {
				c.sendErrAndErr(err.Error())
				c.closeConnection(ProtocolViolation)
				return
			}
			if compress {
				// Done for now, will get back another INFO protocol...
				return
			}
			// No compression because one side does not want/can't, so proceed.
			c.mu.Lock()
			// Check that the connection did not close if the lock was released.
			if c.isClosed() {
				c.mu.Unlock()
				return
			}
		}
		// We can set the ping timer after we just negotiated compression above,
		// or for solicited routes if we already negotiated.
		if !compNeg || didSolicit {
			c.setFirstPingTimer()
		}
		// When compression is configured, we delay the initial INFO for any
		// solicited route. So we need to send the delayed INFO simply based
		// on the didSolicit boolean.
		sendDelayedInfo = didSolicit
	} else {
		// Coming from an old server, the Compression field would be the empty
		// string. For servers that are configured with CompressionNotSupported,
		// this makes them behave as old servers.
		if info.Compression == _EMPTY_ || opts.Cluster.Compression.Mode == CompressionNotSupported {
			c.route.compression = CompressionNotSupported
		} else {
			c.route.compression = CompressionOff
		}
		// When compression is not configured, we delay the initial INFO only
		// for solicited pooled routes, so use the same check that we did when
		// we decided to delay in createRoute().
		sendDelayedInfo = didSolicit && routeShouldDelayInfo(bytesToString(c.route.accName), opts)
	}

	// Mark that the INFO protocol has been received, so we can detect updates.
	c.flags.set(infoReceived)

	// Get the route's proto version. It will be used to check if the connection
	// supports certain features, such as message tracing.
	c.opts.Protocol = info.Proto

	// Headers
	c.headers = supportsHeaders && info.Headers

	// Copy over important information.
	c.route.remoteID = info.ID
	c.route.authRequired = info.AuthRequired
	c.route.tlsRequired = info.TLSRequired
	c.route.gatewayURL = info.GatewayURL
	c.route.remoteName = info.Name
	c.route.ln = info.LN
	c.route.lnoc = info.LNOC || c.route.ln
	c.route.lnocu = info.LNOCU || c.route.ln
	c.route.jetstream = info.JetStream

	// When sent through route INFO, if the field is set, it should be of size 1.
	if len(info.LeafNodeURLs) == 1 {
		c.route.leafnodeURL = info.LeafNodeURLs[0]
	}
	// Compute the hash of this route based on remote server name
	c.route.hash = getHash(info.Name)
	// Same with remote server ID (used for GW mapped replies routing).
	// Use getGWHash since we don't use the same hash len for that
	// for backward compatibility.
	c.route.idHash = string(getGWHash(info.ID))

	// Copy over permissions as well.
	c.opts.Import = info.Import
	c.opts.Export = info.Export

	// If we do not know this route's URL, construct one on the fly
	// from the information provided.
	if c.route.url == nil {
		// Add in the URL from host and port
		hp := net.JoinHostPort(info.Host, strconv.Itoa(info.Port))
		url, err := url.Parse(fmt.Sprintf("nats-route://%s/", hp))
		if err != nil {
			c.Errorf("Error parsing URL from INFO: %v\n", err)
			c.mu.Unlock()
			c.closeConnection(ParseError)
			return
		}
		c.route.url = url
	}
	// The incoming INFO from the route will have IP set
	// if it has Cluster.Advertise. In that case, use that
	// otherwise construct it from the remote TCP address.
	if info.IP == _EMPTY_ {
		// Need to get the remote IP address.
		switch conn := c.nc.(type) {
		case *net.TCPConn, *tls.Conn:
			addr := conn.RemoteAddr().(*net.TCPAddr)
			info.IP = fmt.Sprintf("nats-route://%s/", net.JoinHostPort(addr.IP.String(),
				strconv.Itoa(info.Port)))
		default:
			info.IP = c.route.url.String()
		}
	}
	// For accounts that are configured to have their own route:
	// If this is a solicited route, we already have c.route.accName set in createRoute.
	// For non solicited route (the accept side), we will set the account name that
	// is present in the INFO protocol.
	if didSolicit && len(c.route.accName) > 0 {
		// Set it in the info.RouteAccount so that addRoute can use that
		// and we properly gossip that this is a route for an account.
		info.RouteAccount = string(c.route.accName)
	} else if !didSolicit && info.RouteAccount != _EMPTY_ {
		c.route.accName = []byte(info.RouteAccount)
	}
	accName := string(c.route.accName)

	// Capture the noGossip value and reset it here.
	gossipMode := c.route.gossipMode
	c.route.gossipMode = 0

	// Check to see if we have this remote already registered.
	// This can happen when both servers have routes to each other.
	c.mu.Unlock()

	if added := s.addRoute(c, didSolicit, sendDelayedInfo, gossipMode, info, accName); added {
		if accName != _EMPTY_ {
			c.Debugf("Registering remote route %q for account %q", info.ID, accName)
		} else {
			c.Debugf("Registering remote route %q", info.ID)
		}
	} else {
		c.Debugf("Detected duplicate remote route %q", info.ID)
		c.closeConnection(DuplicateRoute)
	}
}

func (s *Server) negotiateRouteCompression(c *client, didSolicit bool, accName, infoCompression string, opts *Options) (bool, error) {
	// Negotiate the appropriate compression mode (or no compression)
	cm, err := selectCompressionMode(opts.Cluster.Compression.Mode, infoCompression)
	if err != nil {
		return false, err
	}
	c.mu.Lock()
	// For "auto" mode, set the initial compression mode based on RTT
	if cm == CompressionS2Auto {
		if c.rttStart.IsZero() {
			c.rtt = computeRTT(c.start)
		}
		cm = selectS2AutoModeBasedOnRTT(c.rtt, opts.Cluster.Compression.RTTThresholds)
	}
	// Keep track of the negotiated compression mode.
	c.route.compression = cm
	c.mu.Unlock()

	// If we end-up doing compression...
	if needsCompression(cm) {
		// Generate an INFO with the chosen compression mode.
		s.mu.Lock()
		infoProto := s.generateRouteInitialInfoJSON(accName, cm, 0, gossipDefault)
		s.mu.Unlock()

		// If we solicited, then send this INFO protocol BEFORE switching
		// to compression writer. However, if we did not, we send it after.
		c.mu.Lock()
		if didSolicit {
			c.enqueueProto(infoProto)
			// Make sure it is completely flushed (the pending bytes goes to
			// 0) before proceeding.
			for c.out.pb > 0 && !c.isClosed() {
				c.flushOutbound()
			}
		}
		// This is to notify the readLoop that it should switch to a
		// (de)compression reader.
		c.in.flags.set(switchToCompression)
		// Create the compress writer before queueing the INFO protocol for
		// a route that did not solicit. It will make sure that that proto
		// is sent with compression on.
		c.out.cw = s2.NewWriter(nil, s2WriterOptions(cm)...)
		if !didSolicit {
			c.enqueueProto(infoProto)
		}
		// We can now set the ping timer.
		c.setFirstPingTimer()
		c.mu.Unlock()
		return true, nil
	}
	return false, nil
}

// Possibly sends local subscriptions interest to this route
// based on changes in the remote's Export permissions.
func (s *Server) updateRemoteRoutePerms(c *client, info *Info) {
	c.mu.Lock()
	// Interested only on Export permissions for the remote server.
	// Create "fake" clients that we will use to check permissions
	// using the old permissions...
	oldPerms := &RoutePermissions{Export: c.opts.Export}
	oldPermsTester := &client{}
	oldPermsTester.setRoutePermissions(oldPerms)
	// and the new ones.
	newPerms := &RoutePermissions{Export: info.Export}
	newPermsTester := &client{}
	newPermsTester.setRoutePermissions(newPerms)

	c.opts.Import = info.Import
	c.opts.Export = info.Export

	routeAcc, poolIdx, noPool := bytesToString(c.route.accName), c.route.poolIdx, c.route.noPool
	c.mu.Unlock()

	var (
		_localSubs [4096]*subscription
		_allSubs   [4096]*subscription
		allSubs    = _allSubs[:0]
	)

	s.accounts.Range(func(_, v any) bool {
		acc := v.(*Account)
		acc.mu.RLock()
		accName, sl, accPoolIdx := acc.Name, acc.sl, acc.routePoolIdx
		acc.mu.RUnlock()

		// Do this only for accounts handled by this route
		if (accPoolIdx >= 0 && accPoolIdx == poolIdx) || (routeAcc == accName) || noPool {
			localSubs := _localSubs[:0]
			sl.localSubs(&localSubs, false)
			if len(localSubs) > 0 {
				allSubs = append(allSubs, localSubs...)
			}
		}
		return true
	})

	if len(allSubs) == 0 {
		return
	}

	c.mu.Lock()
	c.sendRouteSubProtos(allSubs, false, func(sub *subscription) bool {
		subj := string(sub.subject)
		// If the remote can now export but could not before, and this server can import this
		// subject, then send SUB protocol.
		if newPermsTester.canExport(subj) && !oldPermsTester.canExport(subj) && c.canImport(subj) {
			return true
		}
		return false
	})
	c.mu.Unlock()
}

// sendAsyncInfoToClients sends an INFO protocol to all
// connected clients that accept async INFO updates.
// The server lock is held on entry.
func (s *Server) sendAsyncInfoToClients(regCli, wsCli bool) {
	// If there are no clients supporting async INFO protocols, we are done.
	// Also don't send if we are shutting down...
	if s.cproto == 0 || s.isShuttingDown() {
		return
	}
	info := s.copyInfo()

	for _, c := range s.clients {
		c.mu.Lock()
		// Here, we are going to send only to the clients that are fully
		// registered (server has received CONNECT and first PING). For
		// clients that are not at this stage, this will happen in the
		// processing of the first PING (see client.processPing)
		if ((regCli && !c.isWebsocket()) || (wsCli && c.isWebsocket())) &&
			c.opts.Protocol >= ClientProtoInfo &&
			c.flags.isSet(firstPongSent) {
			// sendInfo takes care of checking if the connection is still
			// valid or not, so don't duplicate tests here.
			c.enqueueProto(c.generateClientInfoJSON(info, true))
		}
		c.mu.Unlock()
	}
}

// This will process implicit route information received from another server.
// We will check to see if we have configured or are already connected,
// and if so we will ignore. Otherwise we will attempt to connect.
func (s *Server) processImplicitRoute(info *Info, routeNoPool bool) {
	remoteID := info.ID

	s.mu.Lock()
	defer s.mu.Unlock()

	// Don't connect to ourself
	if remoteID == s.info.ID {
		return
	}

	// Snapshot server options.
	opts := s.getOpts()

	// Check if this route already exists
	if accName := info.RouteAccount; accName != _EMPTY_ {
		// If we don't support pooling/pinned account, bail.
		if opts.Cluster.PoolSize <= 0 {
			return
		}
		if remotes, ok := s.accRoutes[accName]; ok {
			if r := remotes[remoteID]; r != nil {
				return
			}
		}
	} else if _, exists := s.routes[remoteID]; exists {
		return
	}
	// Check if we have this route as a configured route
	if s.hasThisRouteConfigured(info) {
		return
	}

	// Initiate the connection, using info.IP instead of info.URL here...
	r, err := url.Parse(info.IP)
	if err != nil {
		s.Errorf("Error parsing URL from INFO: %v\n", err)
		return
	}

	if info.AuthRequired {
		r.User = url.UserPassword(opts.Cluster.Username, opts.Cluster.Password)
	}
	s.startGoRoutine(func() { s.connectToRoute(r, Implicit, true, info.GossipMode, info.RouteAccount) })
	// If we are processing an implicit route from a route that does not
	// support pooling/pinned-accounts, we won't receive an INFO for each of
	// the pinned-accounts that we would normally receive. In that case, just
	// initiate routes for all our configured pinned accounts.
	if routeNoPool && info.RouteAccount == _EMPTY_ && len(opts.Cluster.PinnedAccounts) > 0 {
		// Copy since we are going to pass as closure to a go routine.
		rURL := r
		for _, an := range opts.Cluster.PinnedAccounts {
			accName := an
			s.startGoRoutine(func() { s.connectToRoute(rURL, Implicit, true, info.GossipMode, accName) })
		}
	}
}

// hasThisRouteConfigured returns true if info.Host:info.Port is present
// in the server's opts.Routes, false otherwise.
// Server lock is assumed to be held by caller.
func (s *Server) hasThisRouteConfigured(info *Info) bool {
	routes := s.getOpts().Routes
	if len(routes) == 0 {
		return false
	}
	// This could possibly be a 0.0.0.0 host so we will also construct a second
	// url with the host section of the `info.IP` (if present).
	sPort := strconv.Itoa(info.Port)
	urlOne := strings.ToLower(net.JoinHostPort(info.Host, sPort))
	var urlTwo string
	if info.IP != _EMPTY_ {
		if u, _ := url.Parse(info.IP); u != nil {
			urlTwo = strings.ToLower(net.JoinHostPort(u.Hostname(), sPort))
			// Ignore if same than the first
			if urlTwo == urlOne {
				urlTwo = _EMPTY_
			}
		}
	}
	for _, ri := range routes {
		rHost := strings.ToLower(ri.Host)
		if rHost == urlOne {
			return true
		}
		if urlTwo != _EMPTY_ && rHost == urlTwo {
			return true
		}
	}
	return false
}

// forwardNewRouteInfoToKnownServers possibly sends the INFO protocol of the
// new route to all routes known by this server. In turn, each server will
// contact this new route.
// Server lock held on entry.
func (s *Server) forwardNewRouteInfoToKnownServers(info *Info, rtype RouteType, didSolicit bool, localGossipMode byte) {
	// Determine if this connection is resulting from a gossip notification.
	fromGossip := didSolicit && rtype == Implicit
	// If from gossip (but we are not overriding it) or if the remote disabled gossip, bail out.
	if (fromGossip && localGossipMode != gossipOverride) || info.GossipMode == gossipDisabled {
		return
	}

	// Note: nonce is not used in routes.
	// That being said, the info we get is the initial INFO which
	// contains a nonce, but we now forward this to existing routes,
	// so clear it now.
	info.Nonce = _EMPTY_

	var (
		infoGMDefault  []byte
		infoGMDisabled []byte
		infoGMOverride []byte
	)

	generateJSON := func(gm byte) []byte {
		info.GossipMode = gm
		b, _ := json.Marshal(info)
		return []byte(fmt.Sprintf(InfoProto, b))
	}

	getJSON := func(r *client) []byte {
		if (!didSolicit && r.route.routeType == Explicit) || (didSolicit && rtype == Explicit) {
			if infoGMOverride == nil {
				infoGMOverride = generateJSON(gossipOverride)
			}
			return infoGMOverride
		} else if !didSolicit {
			if infoGMDisabled == nil {
				infoGMDisabled = generateJSON(gossipDisabled)
			}
			return infoGMDisabled
		}
		if infoGMDefault == nil {
			infoGMDefault = generateJSON(0)
		}
		return infoGMDefault
	}

	var accRemotes map[string]*client
	pinnedAccount := info.RouteAccount != _EMPTY_
	// If this is for a pinned account, we will try to send the gossip
	// through our pinned account routes, but fall back to the other
	// routes in case we don't have one for a given remote.
	if pinnedAccount {
		var ok bool
		if accRemotes, ok = s.accRoutes[info.RouteAccount]; ok {
			for remoteID, r := range accRemotes {
				if r == nil {
					continue
				}
				r.mu.Lock()
				// Do not send to a remote that does not support pooling/pinned-accounts.
				if remoteID != info.ID && !r.route.noPool {
					r.enqueueProto(getJSON(r))
				}
				r.mu.Unlock()
			}
		}
	}

	s.forEachRemote(func(r *client) {
		r.mu.Lock()
		remoteID := r.route.remoteID
		if pinnedAccount {
			if _, processed := accRemotes[remoteID]; processed {
				r.mu.Unlock()
				return
			}
		}
		// If this is a new route for a given account, do not send to a server
		// that does not support pooling/pinned-accounts.
		if remoteID != info.ID && (!pinnedAccount || !r.route.noPool) {
			r.enqueueProto(getJSON(r))
		}
		r.mu.Unlock()
	})
}

// canImport is whether or not we will send a SUB for interest to the other side.
// This is for ROUTER connections only.
// Lock is held on entry.
func (c *client) canImport(subject string) bool {
	// Use pubAllowed() since this checks Publish permissions which
	// is what Import maps to.
	return c.pubAllowedFullCheck(subject, false, true)
}

// canExport is whether or not we will accept a SUB from the remote for a given subject.
// This is for ROUTER connections only.
// Lock is held on entry
func (c *client) canExport(subject string) bool {
	// Use canSubscribe() since this checks Subscribe permissions which
	// is what Export maps to.
	return c.canSubscribe(subject)
}

// Initialize or reset cluster's permissions.
// This is for ROUTER connections only.
// Client lock is held on entry
func (c *client) setRoutePermissions(perms *RoutePermissions) {
	// Reset if some were set
	if perms == nil {
		c.perms = nil
		c.mperms = nil
		return
	}
	// Convert route permissions to user permissions.
	// The Import permission is mapped to Publish
	// and Export permission is mapped to Subscribe.
	// For meaning of Import/Export, see canImport and canExport.
	p := &Permissions{
		Publish:   perms.Import,
		Subscribe: perms.Export,
	}
	c.setPermissions(p)
}

// Type used to hold a list of subs on a per account basis.
type asubs struct {
	acc  *Account
	subs []*subscription
}

// Returns the account name from the subscription's key.
// This is invoked knowing that the key contains an account name, so for a sub
// that is not from a pinned-account route.
// The `keyHasSubType` boolean indicates that the key starts with the indicator
// for leaf or regular routed subscriptions. No-origin leaf subscriptions always have the
// indicator so they remain distinct from RS+ even without LNOCU.
func getAccNameFromRoutedSubKey(sub *subscription, key string, keyHasSubType bool) string {
	fields := strings.Fields(key)
	var accIdx int
	if keyHasSubType || (sub.leaf && len(sub.origin) == 0) {
		// Start after the sub type indicator.
		accIdx = 1
		// But if there is an origin, bump its index.
		if len(sub.origin) > 0 || (sub.leaf && len(fields) > 1 && fields[1] == leafNoOriginCluster) {
			accIdx = 2
		}
	}
	return fields[accIdx]
}

// Returns if the route is dedicated to an account, its name, and a boolean
// that indicates if this route uses the routed subscription indicator at
// the beginning of the subscription key.
// Lock held on entry.
func (c *client) getRoutedSubKeyInfo() (bool, string, bool) {
	var accName string
	if an := c.route.accName; len(an) > 0 {
		accName = string(an)
	}
	return accName != _EMPTY_, accName, c.route.lnocu
}

// removeRemoteSubs will walk the subs and remove them from the appropriate account.
func (c *client) removeRemoteSubs() {
	// We need to gather these on a per account basis.
	// FIXME(dlc) - We should be smarter about this..
	as := map[string]*asubs{}
	c.mu.Lock()
	srv := c.srv
	subs := c.subs
	c.subs = nil
	pa, accountName, hasSubType := c.getRoutedSubKeyInfo()
	c.mu.Unlock()

	for key, sub := range subs {
		c.mu.Lock()
		sub.max = 0
		c.mu.Unlock()
		// If not a pinned-account route, we need to find the account
		// name from the sub's key.
		if !pa {
			accountName = getAccNameFromRoutedSubKey(sub, key, hasSubType)
		}
		ase := as[accountName]
		if ase == nil {
			if v, ok := srv.accounts.Load(accountName); ok {
				ase = &asubs{acc: v.(*Account), subs: []*subscription{sub}}
				as[accountName] = ase
			} else {
				continue
			}
		} else {
			ase.subs = append(ase.subs, sub)
		}
		delta := int32(1)
		if len(sub.queue) > 0 {
			delta = sub.qw
		}
		if srv.gateway.enabled {
			srv.gatewayUpdateSubInterest(accountName, sub, -delta)
		}
		ase.acc.updateLeafNodes(sub, -delta)
	}

	// Now remove the subs by batch for each account sublist.
	for _, ase := range as {
		c.Debugf("Removing %d subscriptions for account %q", len(ase.subs), ase.acc.Name)
		ase.acc.mu.Lock()
		ase.acc.sl.RemoveBatch(ase.subs)
		ase.acc.mu.Unlock()
	}
}

// Removes (and returns) the subscriptions from this route's subscriptions map
// that belong to the given account.
// Lock is held on entry
func (c *client) removeRemoteSubsForAcc(name string) []*subscription {
	var subs []*subscription
	_, _, hasSubType := c.getRoutedSubKeyInfo()
	for key, sub := range c.subs {
		an := getAccNameFromRoutedSubKey(sub, key, hasSubType)
		if an == name {
			sub.max = 0
			subs = append(subs, sub)
			delete(c.subs, key)
		}
	}
	return subs
}

func (c *client) parseUnsubProto(arg []byte, accInProto, hasOrigin bool) ([]byte, string, []byte, []byte, error) {
	// Indicate any activity, so pub and sub or unsubs.
	c.in.subs++

	args := splitArg(arg)

	var (
		origin      []byte
		accountName string
		queue       []byte
		subjIdx     int
	)
	// If `hasOrigin` is true, then it means this is a LS- with origin in proto.
	if hasOrigin {
		// We would not be here if there was not at least 1 field.
		origin = args[0]
		subjIdx = 1
	}
	// If there is an account in the protocol, bump the subject index.
	if accInProto {
		subjIdx++
	}

	switch len(args) {
	case subjIdx + 1:
	case subjIdx + 2:
		queue = args[subjIdx+1]
	default:
		return nil, _EMPTY_, nil, nil, fmt.Errorf("parse error: '%s'", arg)
	}
	if accInProto {
		// If there is an account in the protocol, it is before the subject.
		accountName = string(args[subjIdx-1])
	}
	return origin, accountName, args[subjIdx], queue, nil
}

// Indicates no more interest in the given account/subject for the remote side.
func (c *client) processRemoteUnsub(arg []byte, leafUnsub bool) (err error) {
	srv := c.srv
	if srv == nil {
		return nil
	}

	var accountName string
	// Assume the account will be in the protocol.
	accInProto := true

	c.mu.Lock()
	originSupport := c.route.lnocu
	lnSupport := c.route.ln
	if c.route != nil && len(c.route.accName) > 0 {
		accountName, accInProto = string(c.route.accName), false
	}
	c.mu.Unlock()

	hasOrigin := leafUnsub && originSupport
	origin, accNameFromProto, subject, _, err := c.parseUnsubProto(arg, accInProto, hasOrigin)
	if err != nil {
		return fmt.Errorf("processRemoteUnsub %s", err.Error())
	}
	noOrigin := lnSupport && bytesToString(origin) == leafNoOriginCluster
	if accInProto {
		accountName = accNameFromProto
	}
	// Lookup the account
	var acc *Account
	if v, ok := srv.accounts.Load(accountName); ok {
		acc = v.(*Account)
	} else {
		c.Debugf("Unknown account %q for subject %q", accountName, subject)
		return nil
	}

	c.mu.Lock()
	if c.isClosed() {
		c.mu.Unlock()
		return nil
	}

	_keya := [128]byte{}
	_key := _keya[:0]

	var key string
	if !originSupport && !noOrigin {
		// If it is an LS- or RS-, we use the protocol as-is as the key.
		key = bytesToString(arg)
	} else {
		// We need to prefix with the sub type.
		if leafUnsub {
			_key = append(_key, keyRoutedLeafSubByte)
		} else {
			_key = append(_key, keyRoutedSubByte)
		}
		_key = append(_key, ' ')
		_key = append(_key, arg...)
		key = bytesToString(_key)
	}
	delta := int32(1)
	sub, ok := c.subs[key]
	if ok {
		delete(c.subs, key)
		acc.sl.Remove(sub)
		if len(sub.queue) > 0 {
			delta = sub.qw
		}
	}
	c.mu.Unlock()

	// Update gateways and leaf nodes only if the subscription was found.
	if ok {
		if srv.gateway.enabled {
			srv.gatewayUpdateSubInterest(accountName, sub, -delta)
		}

		// Now check on leafnode updates.
		acc.updateLeafNodes(sub, -delta)
	}

	if c.opts.Verbose {
		c.sendOK()
	}
	return nil
}

func (c *client) processRemoteSub(argo []byte, leafSub, hasOrigin bool) (err error) {
	// Indicate activity.
	c.in.subs++

	srv := c.srv
	if srv == nil {
		return nil
	}

	// We copy `argo` to not reference the read buffer. However, we will
	// prefix with a code that says if the remote sub is for a leaf
	// (hasOrigin == true) or not to prevent key collisions. Imagine:
	// "RS+ foo bar baz 1\r\n" => "foo bar baz" (a routed queue sub)
	// "LS+ foo bar baz\r\n"   => "foo bar baz" (a route leaf sub on "baz",
	// for account "bar" with origin "foo").
	//
	// The sub.sid/key will be set respectively to "R foo bar baz" and
	// "L foo bar baz".
	//
	// We also no longer add the account if it was not present (due to
	// pinned-account route) since there is no need really.
	//
	// For routes to older server, we will still create the "arg" with
	// the above layout, but we will create the sub.sid/key as before,
	// that is, not including the origin for LS+ because older server
	// only send LS- without origin, so we would not be able to find
	// the sub in the map.
	c.mu.Lock()
	accountName := string(c.route.accName)
	lnSupport := c.route.ln
	oldStyle := !c.route.lnocu && !(leafSub && !hasOrigin)
	c.mu.Unlock()

	// Indicate if the account name should be in the protocol. It would be the
	// case if accountName is empty.
	accInProto := accountName == _EMPTY_

	// Copy so we do not reference a potentially large buffer.
	// Add 2 more bytes for the routed sub type.
	arg := make([]byte, 0, 2+len(argo))
	if leafSub {
		arg = append(arg, keyRoutedLeafSubByte)
	} else {
		arg = append(arg, keyRoutedSubByte)
	}
	arg = append(arg, ' ')
	arg = append(arg, argo...)

	// Now split to get all fields. Unroll splitArgs to avoid runtime/heap issues.
	a := [MAX_RSUB_ARGS][]byte{}
	args := a[:0]
	start := -1
	for i, b := range arg {
		switch b {
		case ' ', '\t', '\r', '\n':
			if start >= 0 {
				args = append(args, arg[start:i])
				start = -1
			}
		default:
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		args = append(args, arg[start:])
	}

	delta := int32(1)
	sub := &subscription{client: c, leaf: leafSub}

	// There will always be at least a subject, but its location will depend
	// on if there is an origin, an account name, etc.. Since we know that
	// we have added the sub type indicator as the first field, the subject
	// position will be at minimum at index 1.
	subjIdx := 1
	if hasOrigin {
		subjIdx++
	}
	if accInProto {
		subjIdx++
	}
	switch len(args) {
	case subjIdx + 1:
		sub.queue = nil
	case subjIdx + 3:
		sub.queue = args[subjIdx+1]
		sub.qw = int32(parseSize(args[subjIdx+2]))
		// A missing or malformed weight counts as a single member.
		if sub.qw < 1 {
			sub.qw = 1
		}
		// A new sub can have a weight > 1 after a reconnect or server restart.
		delta = sub.qw
	default:
		return fmt.Errorf("processRemoteSub Parse Error: '%s'", arg)
	}
	// We know that the number of fields is correct. So we can access args[] based
	// on where we expect the fields to be.

	// If there is an origin, it will be at index 1. The negotiated sentinel
	// denotes leaf interest without an actual origin cluster.
	noOrigin := lnSupport && hasOrigin && bytesToString(args[1]) == leafNoOriginCluster
	if hasOrigin && !noOrigin {
		sub.origin = args[1]
	}
	// For subject, use subjIdx.
	sub.subject = args[subjIdx]
	// If the account name is in the protocol, it will be before the subject.
	if accInProto {
		accountName = bytesToString(args[subjIdx-1])
	}
	// Now set the sub.sid from the arg slice. However, we will have a different
	// one if we use the origin or not.
	start = 0
	end := len(arg)
	if sub.queue != nil {
		// Remove the ' <weight>' from the arg length.
		end -= 1 + len(args[subjIdx+2])
	}
	if oldStyle {
		// We will start at the account (if present) or at the subject.
		// We first skip the "R " or "L "
		start = 2
		// And if there is an origin skip that.
		if hasOrigin {
			start += len(sub.origin) + 1
		}
		// Here we are pointing at the account (if present), or at the subject.
	}
	sub.sid = arg[start:end]

	// Lookup account while avoiding fetch.
	// A slow fetch delays subsequent remote messages. It also avoids the expired check (see below).
	// With all but memory resolver lookup can be delayed or fail.
	// It is also possible that the account can't be resolved yet.
	// This does not apply to the memory resolver.
	// When used, perform the fetch.
	staticResolver := true
	if res := srv.AccountResolver(); res != nil {
		if _, ok := res.(*MemAccResolver); !ok {
			staticResolver = false
		}
	}
	var acc *Account
	if staticResolver {
		acc, _ = srv.LookupAccount(accountName)
	} else if v, ok := srv.accounts.Load(accountName); ok {
		acc = v.(*Account)
	}
	if acc == nil {
		// if the option of retrieving accounts later exists, create an expired one.
		// When a client comes along, expiration will prevent it from being used,
		// cause a fetch and update the account to what is should be.
		if staticResolver {
			c.Errorf("Unknown account %q for remote subject %q", accountName, sub.subject)
			return
		}
		c.Debugf("Unknown account %q for remote subject %q", accountName, sub.subject)

		var isNew bool
		if acc, isNew = srv.LookupOrRegisterAccount(accountName); isNew {
			acc.mu.Lock()
			acc.expired.Store(true)
			acc.incomplete = true
			acc.mu.Unlock()
		}
	}

	c.mu.Lock()
	if c.isClosed() {
		c.mu.Unlock()
		return nil
	}

	// Check permissions if applicable.
	if c.perms != nil && !c.canExport(string(sub.subject)) {
		c.mu.Unlock()
		c.Debugf("Can not export %q, ignoring remote subscription request", sub.subject)
		return nil
	}

	// Check if we have a maximum on the number of subscriptions.
	if c.subsAtLimit() {
		c.mu.Unlock()
		c.maxSubsExceeded()
		return nil
	}

	acc.mu.RLock()
	// For routes (this can be called by leafnodes), check if the account is
	// transitioning (from pool to dedicated route) and this route is not a
	// per-account route (route.poolIdx >= 0). If so, ignore this subscription.
	// Exclude "no pool" routes from this check.
	if c.kind == ROUTER && !c.route.noPool &&
		acc.routePoolIdx == accTransitioningToDedicatedRoute && c.route.poolIdx >= 0 {
		acc.mu.RUnlock()
		c.mu.Unlock()
		// Do not return an error, which would cause the connection to be closed.
		return nil
	}
	sl := acc.sl
	acc.mu.RUnlock()

	// We use the sub.sid for the key of the c.subs map.
	key := bytesToString(sub.sid)
	osub := c.subs[key]
	if osub == nil {
		c.subs[key] = sub
		// Now place into the account sl.
		if err = sl.Insert(sub); err != nil {
			delete(c.subs, key)
			c.mu.Unlock()
			c.Errorf("Could not insert subscription: %v", err)
			c.sendErr("Invalid Subscription")
			return nil
		}
	} else if sub.queue != nil {
		// For a queue we need to update the weight.
		delta = sub.qw - atomic.LoadInt32(&osub.qw)
		atomic.StoreInt32(&osub.qw, sub.qw)
		sl.UpdateRemoteQSub(osub)
	}
	c.mu.Unlock()

	if srv.gateway.enabled {
		srv.gatewayUpdateSubInterest(acc.Name, sub, delta)
	}

	// Now check on leafnode updates.
	acc.updateLeafNodes(sub, delta)

	if c.opts.Verbose {
		c.sendOK()
	}

	return nil
}

// Lock is held on entry
func (c *client) addRouteSubOrUnsubProtoToBuf(buf []byte, accName string, sub *subscription, isSubProto bool) []byte {
	// If we have an origin cluster and the other side supports leafnode origin clusters
	// send an LS+/LS- version instead.
	if len(sub.origin) > 0 && c.route.lnoc {
		if isSubProto {
			buf = append(buf, lSubBytes...)
			buf = append(buf, sub.origin...)
			buf = append(buf, ' ')
		} else {
			buf = append(buf, lUnsubBytes...)
			if c.route.lnocu {
				buf = append(buf, sub.origin...)
				buf = append(buf, ' ')
			}
		}
	} else if sub.leaf && len(sub.origin) == 0 && c.route.ln {
		if isSubProto {
			buf = append(buf, lSubBytes...)
		} else {
			buf = append(buf, lUnsubBytes...)
		}
		buf = append(buf, leafNoOriginCluster...)
		buf = append(buf, ' ')
	} else {
		if isSubProto {
			buf = append(buf, rSubBytes...)
		} else {
			buf = append(buf, rUnsubBytes...)
		}
	}
	if len(c.route.accName) == 0 {
		buf = append(buf, accName...)
		buf = append(buf, ' ')
	}
	buf = append(buf, sub.subject...)
	if len(sub.queue) > 0 {
		buf = append(buf, ' ')
		buf = append(buf, sub.queue...)
		// Send our weight if we are a sub proto
		if isSubProto {
			buf = append(buf, ' ')
			var b [12]byte
			var i = len(b)
			for l := sub.qw; l > 0; l /= 10 {
				i--
				b[i] = digits[l%10]
			}
			buf = append(buf, b[i:]...)
		}
	}
	buf = append(buf, CR_LF...)
	return buf
}

// sendSubsToRoute will send over our subject interest to
// the remote side. For each account we will send the
// complete interest for all subjects, both normal as a binary
// and queue group weights.
//
// Server lock held on entry.
func (s *Server) sendSubsToRoute(route *client, idx int, account string) {
	var noPool bool
	if idx >= 0 {
		// We need to check if this route is "no_pool" in which case we
		// need to select all accounts.
		route.mu.Lock()
		noPool = route.route.noPool
		route.mu.Unlock()
	}
	// Estimated size of all protocols. It does not have to be accurate at all.
	var eSize int
	estimateProtosSize := func(a *Account, addAccountName bool) {
		if ns := len(a.rm); ns > 0 {
			var accSize int
			if addAccountName {
				accSize = len(a.Name) + 1
			}
			// Proto looks like: "RS+ [<account name> ]<subject>[ <queue> <weight>]\r\n"
			eSize += ns * (len(rSubBytes) + 1 + accSize)
			for key := range a.rm {
				// Key contains "<subject>[ <queue>]"
				eSize += len(key)
				// In case this is a queue, just add some bytes for the queue weight.
				// If we want to be accurate, would have to check if "key" has a space,
				// if so, then figure out how many bytes we need to represent the weight.
				eSize += 5
			}
		}
	}
	// Send over our account subscriptions.
	accs := make([]*Account, 0, 1024)
	if idx < 0 || account != _EMPTY_ {
		if ai, ok := s.accounts.Load(account); ok {
			a := ai.(*Account)
			a.mu.RLock()
			// Estimate size and add account name in protocol if idx is not -1
			estimateProtosSize(a, idx >= 0)
			accs = append(accs, a)
			a.mu.RUnlock()
		}
	} else {
		s.accounts.Range(func(k, v any) bool {
			a := v.(*Account)
			a.mu.RLock()
			// We are here for regular or pooled routes (not per-account).
			// So we collect all accounts whose routePoolIdx matches the
			// one for this route, or only the account provided, or all
			// accounts if dealing with a "no pool" route.
			if a.routePoolIdx == idx || noPool {
				estimateProtosSize(a, true)
				accs = append(accs, a)
			}
			a.mu.RUnlock()
			return true
		})
	}

	buf := make([]byte, 0, eSize)

	route.mu.Lock()
	for _, a := range accs {
		a.mu.RLock()
		for key, n := range a.rm {
			var origin, qn []byte
			s := strings.Fields(key)
			// Subject will always be the second field (index 1).
			subj := stringToBytes(s[1])
			// Check if the key is for a leaf (will be field 0).
			leafWithOrigin := s[0] == keyRoutedLeafSub
			leafWithoutOrigin := s[0] == keyRoutedLeafNoOriginSub
			forLeaf := leafWithOrigin || leafWithoutOrigin
			// For queue, regular and no-origin leaf keys have 3 fields,
			// while a leaf key with an origin has 4.
			if l := len(s); (!leafWithOrigin && l == 3) || (leafWithOrigin && l == 4) {
				qn = stringToBytes(s[2])
			}
			if leafWithOrigin {
				// The leaf origin will be the last field.
				origin = stringToBytes(s[len(s)-1])
			}
			// s[1] is the subject and already as a string, so use that
			// instead of converting back `subj` to a string.
			if !route.canImport(s[1]) {
				continue
			}
			sub := subscription{leaf: forLeaf, origin: origin, subject: subj, queue: qn, qw: queueWeight(n)}
			buf = route.addRouteSubOrUnsubProtoToBuf(buf, a.Name, &sub, true)
		}
		a.mu.RUnlock()
	}
	if len(buf) > 0 {
		route.enqueueProto(buf)
		route.Debugf("Sent local subscriptions to route")
	}
	route.mu.Unlock()
}

// Sends SUBs protocols for the given subscriptions. If a filter is specified, it is
// invoked for each subscription. If the filter returns false, the subscription is skipped.
// This function may release the route's lock due to flushing of outbound data. A boolean
// is returned to indicate if the connection has been closed during this call.
// Lock is held on entry.
func (c *client) sendRouteSubProtos(subs []*subscription, trace bool, filter func(sub *subscription) bool) {
	c.sendRouteSubOrUnSubProtos(subs, true, trace, filter)
}

// Sends UNSUBs protocols for the given subscriptions. If a filter is specified, it is
// invoked for each subscription. If the filter returns false, the subscription is skipped.
// This function may release the route's lock due to flushing of outbound data. A boolean
// is returned to indicate if the connection has been closed during this call.
// Lock is held on entry.
func (c *client) sendRouteUnSubProtos(subs []*subscription, trace bool, filter func(sub *subscription) bool) {
	c.sendRouteSubOrUnSubProtos(subs, false, trace, filter)
}

// Low-level function that sends RS+ or RS- protocols for the given subscriptions.
// This can now also send LS+ and LS- for origin cluster based leafnode subscriptions for cluster no-echo.
// Use sendRouteSubProtos or sendRouteUnSubProtos instead for clarity.
// Lock is held on entry.
func (c *client) sendRouteSubOrUnSubProtos(subs []*subscription, isSubProto, trace bool, filter func(sub *subscription) bool) {
	var (
		_buf [1024]byte
		buf  = _buf[:0]
	)

	for _, sub := range subs {
		if filter != nil && !filter(sub) {
			continue
		}
		// Determine the account. If sub has an ImportMap entry, use that, otherwise scoped to
		// client. Default to global if all else fails.
		var accName string
		if sub.client != nil && sub.client != c {
			sub.client.mu.Lock()
		}
		if sub.im != nil {
			accName = sub.im.acc.Name
		} else if sub.client != nil && sub.client.acc != nil {
			accName = sub.client.acc.Name
		} else {
			c.Debugf("Falling back to default account for sending subs")
			accName = globalAccountName
		}
		if sub.client != nil && sub.client != c {
			sub.client.mu.Unlock()
		}

		as := len(buf)
		buf = c.addRouteSubOrUnsubProtoToBuf(buf, accName, sub, isSubProto)
		if trace {
			c.traceOutOp("", buf[as:len(buf)-LEN_CR_LF])
		}
	}
	c.enqueueProto(buf)
}

func (s *Server) createRoute(conn net.Conn, rURL *url.URL, rtype RouteType, gossipMode byte, accName string) *client {
	// Snapshot server options.
	opts := s.getOpts()

	didSolicit := rURL != nil
	r := &route{routeType: rtype, didSolicit: didSolicit, poolIdx: -1, gossipMode: gossipMode}

	c := &client{srv: s, nc: conn, opts: ClientOpts{}, kind: ROUTER, msubs: -1, mpay: -1, route: r, start: time.Now()}

	// Is the server configured for compression?
	compressionConfigured := needsCompression(opts.Cluster.Compression.Mode)

	var infoJSON []byte
	// Grab server variables and generates route INFO Json. Note that we set
	// and reset some of s.routeInfo fields when that happens, so we need
	// the server write lock.
	s.mu.Lock()
	// If we are creating a pooled connection and this is the server soliciting
	// the connection, we will delay sending the INFO after we have processed
	// the incoming INFO from the remote. Also delay if configured for compression.
	delayInfo := didSolicit && (compressionConfigured || routeShouldDelayInfo(accName, opts))
	if !delayInfo {
		infoJSON = s.generateRouteInitialInfoJSON(accName, opts.Cluster.Compression.Mode, 0, gossipMode)
	}
	authRequired := s.routeInfo.AuthRequired
	tlsRequired := s.routeInfo.TLSRequired
	clusterName := s.info.Cluster
	tlsName := s.routeTLSName
	s.mu.Unlock()

	// Grab lock
	c.mu.Lock()

	// Initialize
	c.initClient()

	if didSolicit {
		// Do this before the TLS code, otherwise, in case of failure
		// and if route is explicit, it would try to reconnect to 'nil'...
		r.url = rURL
		r.accName = []byte(accName)
	} else {
		c.flags.set(expectConnect)
	}

	// Check for TLS
	if tlsRequired {
		tlsConfig := opts.Cluster.TLSConfig
		if didSolicit {
			// Copy off the config to add in ServerName if we need to.
			tlsConfig = tlsConfig.Clone()
		}
		// Perform (server or client side) TLS handshake.
		if resetTLSName, err := c.doTLSHandshake("route", didSolicit, rURL, tlsConfig, tlsName, opts.Cluster.TLSTimeout, opts.Cluster.TLSPinnedCerts); err != nil {
			c.mu.Unlock()
			if resetTLSName {
				s.mu.Lock()
				s.routeTLSName = _EMPTY_
				s.mu.Unlock()
			}
			return nil
		}
	}

	// Do final client initialization

	// Initialize the per-account cache.
	c.in.pacache = make(map[string]*perAccountCache)
	if didSolicit {
		// Set permissions associated with the route user (if applicable).
		// No lock needed since we are already under client lock.
		c.setRoutePermissions(opts.Cluster.Permissions)
	}

	// We can't safely send the pings until we have negotiated compression
	// with the remote, but we want to protect against a connection that
	// does not perform the handshake. We will start a timer that will close
	// the connection as stale based on the ping interval and max out values,
	// but without actually sending pings.
	if compressionConfigured {
		pingInterval := opts.PingInterval
		pingMax := opts.MaxPingsOut
		if opts.Cluster.PingInterval > 0 {
			pingInterval = opts.Cluster.PingInterval
		}
		if opts.Cluster.MaxPingsOut > 0 {
			pingMax = opts.Cluster.MaxPingsOut
		}
		c.watchForStaleConnection(adjustPingInterval(ROUTER, pingInterval), pingMax)
	} else {
		// Set the Ping timer
		c.setFirstPingTimer()
	}

	// For routes, the "client" is added to s.routes only when processing
	// the INFO protocol, that is much later.
	// In the meantime, if the server shutsdown, there would be no reference
	// to the client (connection) to be closed, leaving this readLoop
	// uinterrupted, causing the Shutdown() to wait indefinitively.
	// We need to store the client in a special map, under a special lock.
	if !s.addToTempClients(c.cid, c) {
		c.mu.Unlock()
		c.setNoReconnect()
		c.closeConnection(ServerShutdown)
		return nil
	}

	// Check for Auth required state for incoming connections.
	// Make sure to do this before spinning up readLoop.
	if authRequired && !didSolicit {
		ttl := secondsToDuration(opts.Cluster.AuthTimeout)
		c.setAuthTimer(ttl)
	}

	// Spin up the read loop.
	s.startGoRoutine(func() { c.readLoop(nil) })

	// Spin up the write loop.
	s.startGoRoutine(func() { c.writeLoop() })

	if tlsRequired {
		c.Debugf("TLS handshake complete")
		cs := c.nc.(*tls.Conn).ConnectionState()
		c.Debugf("TLS version %s, cipher suite %s", tlsVersion(cs.Version), tls.CipherSuiteName(cs.CipherSuite))
	}

	// Queue Connect proto if we solicited the connection.
	if didSolicit {
		c.Debugf("Route connect msg sent")
		if err := c.sendRouteConnect(clusterName, tlsRequired); err != nil {
			c.mu.Unlock()
			c.closeConnection(ProtocolViolation)
			return nil
		}
	}

	if !delayInfo {
		// Send our info to the other side.
		// Our new version requires dynamic information for accounts and a nonce.
		c.enqueueProto(infoJSON)
	}
	c.mu.Unlock()

	if didSolicit {
		c.Noticef("Route connection created (%s)", rURL.Host)
	} else {
		c.Noticef("Route connection created")
	}
	return c
}

func routeShouldDelayInfo(accName string, opts *Options) bool {
	return accName == _EMPTY_ && opts.Cluster.PoolSize >= 1
}

// Generates a nonce and set some route info's fields before marshal'ing into JSON.
// To be used only when a route is created (to send the initial INFO protocol).
//
// Server lock held on entry.
func (s *Server) generateRouteInitialInfoJSON(accName, compression string, poolIdx int, gossipMode byte) []byte {
	// New proto wants a nonce (although not used in routes, that is, not signed in CONNECT)
	var raw [nonceLen]byte
	nonce := raw[:]
	s.generateNonce(nonce)
	ri := &s.routeInfo
	// Override compression with s2_auto instead of actual compression level.
	if s.getOpts().Cluster.Compression.Mode == CompressionS2Auto {
		compression = CompressionS2Auto
	}
	ri.Nonce, ri.RouteAccount, ri.RoutePoolIdx, ri.Compression, ri.GossipMode = string(nonce), accName, poolIdx, compression, gossipMode
	infoJSON := generateInfoJSON(&s.routeInfo)
	// Clear now that it has been serialized. Will prevent nonce to be included in async INFO that we may send.
	// Same for some other fields.
	ri.Nonce, ri.RouteAccount, ri.RoutePoolIdx, ri.Compression, ri.GossipMode = _EMPTY_, _EMPTY_, 0, _EMPTY_, 0
	return infoJSON
}

const (
	_CRLF_  = "\r\n"
	_EMPTY_ = ""
)

func (s *Server) addRoute(c *client, didSolicit, sendDelayedInfo bool, gossipMode byte, info *Info, accName string) bool {
	id := info.ID

	var acc *Account
	if accName != _EMPTY_ {
		var err error
		acc, err = s.LookupAccount(accName)
		if err != nil {
			c.sendErrAndErr(fmt.Sprintf("Unable to lookup account %q: %v", accName, err))
			c.closeConnection(MissingAccount)
			return false
		}
	}

	s.mu.Lock()
	if !s.isRunning() || s.routesReject {
		s.mu.Unlock()
		return false
	}
	var invProtoErr string

	opts := s.getOpts()

	// Assume we are in pool mode if info.RoutePoolSize is set. We may disable
	// in some cases.
	pool := info.RoutePoolSize > 0
	// This is used to prevent a server with pooling to constantly trying
	// to connect to a server with no pooling (for instance old server) after
	// the first connection is established.
	var noReconnectForOldServer bool

	// To allow rolling updates, we now allow servers with different pool sizes
	// so we will use as the effective pool size here, the max between our
	// configured size and the size we receive in the info protocol.
	effectivePoolSize := max(s.routesPoolSize, info.RoutePoolSize)

	// If the remote is an old server, info.RoutePoolSize will be 0, or if
	// this server's Cluster.PoolSize is negative, we will behave as an old
	// server and need to handle things differently.
	if info.RoutePoolSize <= 0 || opts.Cluster.PoolSize < 0 {
		if accName != _EMPTY_ {
			invProtoErr = fmt.Sprintf("Not possible to have a dedicated route for account %q between those servers", accName)
			// In this case, make sure this route does not attempt to reconnect
			c.setNoReconnect()
		} else if info.RoutePoolIdx != 0 {
			// Without pooling, the incoming's RoutePoolIdx should not be set.
			invProtoErr = fmt.Sprintf("Route pool index should not be set but is set to %v", info.RoutePoolIdx)
		} else {
			// We will accept, but treat this remote has "no pool"
			pool, noReconnectForOldServer = false, true
			c.mu.Lock()
			c.route.poolIdx = 0
			c.route.noPool = true
			c.mu.Unlock()
			// Keep track of number of routes like that. We will use that when
			// sending subscriptions over routes.
			s.routesNoPool++
		}
	} else if didSolicit {
		// For solicited route, the incoming's RoutePoolIdx should not be set.
		if info.RoutePoolIdx != 0 {
			invProtoErr = fmt.Sprintf("Route pool index should not be set but is set to %v", info.RoutePoolIdx)
		}
	} else if info.RoutePoolIdx < 0 || info.RoutePoolIdx >= effectivePoolSize {
		// For non solicited routes, if the remote sends a RoutePoolIdx, make
		// sure it is a valid one (in range of the pool size).
		invProtoErr = fmt.Sprintf("Invalid route pool index: %v - pool size is %v", info.RoutePoolIdx, info.RoutePoolSize)
	}
	if invProtoErr != _EMPTY_ {
		s.mu.Unlock()
		c.sendErrAndErr(invProtoErr)
		c.closeConnection(ProtocolViolation)
		return false
	}
	// If accName is set, we are dealing with a per-account connection.
	if accName != _EMPTY_ {
		// When an account has its own route, it will be an error if the given
		// account name is not found in s.accRoutes map.
		conns, exists := s.accRoutes[accName]
		if !exists {
			s.mu.Unlock()
			c.sendErrAndErr(fmt.Sprintf("No route for account %q", accName))
			c.closeConnection(ProtocolViolation)
			return false
		}
		remote, exists := conns[id]
		if !exists {
			conns[id] = c
			c.mu.Lock()
			idHash := c.route.idHash
			cid := c.cid
			rtype := c.route.routeType
			if sendDelayedInfo {
				cm := compressionModeForInfoProtocol(&opts.Cluster.Compression, c.route.compression)
				c.enqueueProto(s.generateRouteInitialInfoJSON(accName, cm, 0, gossipMode))
			}
			if c.last.IsZero() {
				c.last = time.Now()
			}
			if acc != nil {
				c.acc = acc
			}
			c.mu.Unlock()

			// Store this route with key being the route id hash + account name
			s.storeRouteByHash(idHash+accName, c)

			// Now that we have registered the route, we can remove from the temp map.
			s.removeFromTempClients(cid)

			// We don't need to send if the only route is the one we just accepted.
			if len(conns) > 1 {
				s.forwardNewRouteInfoToKnownServers(info, rtype, didSolicit, gossipMode)
			}

			// Send subscription interest
			s.sendSubsToRoute(c, -1, accName)
		} else {
			handleDuplicateRoute(remote, c, true)
		}
		s.mu.Unlock()
		return !exists
	}
	var remote *client
	// That will be the position of the connection in the slice, we initialize
	// to -1 to indicate that no space was found.
	idx := -1
	// This will be the size (or number of connections) in a given slice.
	sz := 0
	// Check if we know about the remote server
	conns, exists := s.routes[id]
	if !exists {
		// Now, create a slice for route connections of the size of the pool
		// or 1 when not in pool mode.
		conns = make([]*client, effectivePoolSize)
		// Track this slice for this remote server.
		s.routes[id] = conns
		// Set the index to info.RoutePoolIdx because if this is a solicited
		// route, this value will be 0, which is what we want, otherwise, we
		// will use whatever index the remote has chosen.
		idx = info.RoutePoolIdx
	} else if pool {
		// The remote could have done a config reload and increased the pool size.
		// It will close the connections before soliciting again, however, if
		// on this side, one of the route is not yet fully removed, but the
		// first one is, it would accept the new connection (with a greater pool
		// size) and we would not go through the phase of `!exists` above creating
		// the slice with the right size. So we need to check here and add new empty
		// entries to complete the effective pool size.
		if n := effectivePoolSize - len(conns); n > 0 {
			for range n {
				conns = append(conns, nil)
			}
			s.routes[id] = conns
		}
		// The remote was found. If this is a non solicited route, we will place
		// the connection in the pool at the index given by info.RoutePoolIdx.
		// But if there is already one, close this incoming connection as a
		// duplicate.
		if !didSolicit {
			idx = info.RoutePoolIdx
			if remote = conns[idx]; remote != nil {
				handleDuplicateRoute(remote, c, false)
				s.mu.Unlock()
				return false
			}
			// Look if there is a solicited route in the pool. If there is one,
			// they should all be, so stop at the first.
			if url, rtype, hasSolicited := hasSolicitedRoute(conns); hasSolicited {
				upgradeRouteToSolicited(c, url, rtype)
			}
		} else {
			// If we solicit, upgrade to solicited all non-solicited routes that
			// we may have registered.
			c.mu.Lock()
			url := c.route.url
			rtype := c.route.routeType
			c.mu.Unlock()
			for _, r := range conns {
				upgradeRouteToSolicited(r, url, rtype)
			}
		}
		// For all cases (solicited and not) we need to count how many connections
		// we already have, and for solicited route, we will find a free spot in
		// the slice.
		for i, r := range conns {
			if idx == -1 && r == nil {
				idx = i
			} else if r != nil {
				sz++
			}
		}
	} else {
		remote = conns[0]
	}
	// If there is a spot, idx will be greater or equal to 0.
	if idx >= 0 {
		c.mu.Lock()
		c.route.connectURLs = info.ClientConnectURLs
		c.route.wsConnURLs = info.WSConnectURLs
		c.route.poolIdx = idx
		rtype := c.route.routeType
		cid := c.cid
		idHash := c.route.idHash
		rHash := c.route.hash
		rn := c.route.remoteName
		url := c.route.url
		if sendDelayedInfo {
			cm := compressionModeForInfoProtocol(&opts.Cluster.Compression, c.route.compression)
			c.enqueueProto(s.generateRouteInitialInfoJSON(_EMPTY_, cm, idx, gossipMode))
		}
		if c.last.IsZero() {
			c.last = time.Now()
		}
		c.mu.Unlock()

		// With pooling, we keep track of the remote's configured route pool size.
		// We do so when adding the connection in the first slot, not when `sz == 1`
		// because there could be situations where we have old connections that have
		// not yet been removed and so we would not have `sz == `. However, we will
		// always have the condition where we are adding the new connection at `idx==0`
		// so use that as the condition to store the remote pool size.
		if pool && idx == 0 {
			if s.remoteRoutePoolSize == nil {
				s.remoteRoutePoolSize = make(map[string]int)
			}
			s.remoteRoutePoolSize[id] = info.RoutePoolSize
		}

		// Add to the slice and bump the count of connections for this remote
		conns[idx] = c
		sz++
		// This boolean will indicate that we are registering the only
		// connection in non pooled situation or we stored the very first
		// connection for a given remote server.
		doOnce := !pool || sz == 1
		if doOnce {
			// check to be consistent and future proof. but will be same domain
			if s.sameDomain(info.Domain) {
				s.nodeToInfo.Store(rHash, nodeInfo{
					name:            rn,
					version:         s.info.Version,
					cluster:         s.info.Cluster,
					domain:          info.Domain,
					id:              id,
					tags:            nil,
					cfg:             nil,
					stats:           nil,
					offline:         false,
					js:              info.JetStream,
					binarySnapshots: true, // Updated default to true. Versions 2.10.0+ support it.
					accountNRG:      false,
				})
			}
		}

		// Store this route using the hash as the key
		if pool {
			idHash += strconv.Itoa(idx)
		}
		s.storeRouteByHash(idHash, c)

		// Now that we have registered the route, we can remove from the temp map.
		s.removeFromTempClients(cid)

		if doOnce {
			// If the INFO contains a Gateway URL, add it to the list for our cluster.
			if info.GatewayURL != _EMPTY_ && s.addGatewayURL(info.GatewayURL) {
				s.sendAsyncGatewayInfo()
			}

			// We don't need to send if the only route is the one we just accepted.
			if len(s.routes) > 1 {
				s.forwardNewRouteInfoToKnownServers(info, rtype, didSolicit, gossipMode)
			}

			// Send info about the known gateways to this route.
			s.sendGatewayConfigsToRoute(c)

			// Unless disabled, possibly update the server's INFO protocol
			// and send to clients that know how to handle async INFOs.
			if !opts.Cluster.NoAdvertise {
				s.addConnectURLsAndSendINFOToClients(info.ClientConnectURLs, info.WSConnectURLs)
			}

			// Add the remote's leafnodeURL to our list of URLs and send the update
			// to all LN connections. (Note that when coming from a route, LeafNodeURLs
			// is an array of size 1 max).
			if len(info.LeafNodeURLs) == 1 && s.addLeafNodeURL(info.LeafNodeURLs[0]) {
				s.sendAsyncLeafNodeInfo()
			}
		}

		// Send the subscriptions interest.
		s.sendSubsToRoute(c, idx, _EMPTY_)

		// In pool mode, if we did not yet reach the cap, try to connect a new connection,
		// but do so only after receiving the first PONG to our PING, which will ensure
		// that we have proper authentication.
		if pool && didSolicit && sz != effectivePoolSize {
			c.mu.Lock()
			c.route.startNewRoute = &routeInfo{
				url:        url,
				rtype:      rtype,
				gossipMode: gossipMode,
			}
			c.sendPing()
			c.mu.Unlock()
		}
	}
	s.mu.Unlock()
	if pool {
		if idx == -1 {
			// Was full, so need to close connection
			c.Debugf("Route pool size reached, closing extra connection to %q", id)
			handleDuplicateRoute(nil, c, true)
			return false
		}
		return true
	}
	// This is for non-pool mode at this point.
	if exists {
		handleDuplicateRoute(remote, c, noReconnectForOldServer)
	}

	return !exists
}

func hasSolicitedRoute(conns []*client) (*url.URL, RouteType, bool) {
	var url *url.URL
	var rtype RouteType
	for _, r := range conns {
		if r == nil {
			continue
		}
		r.mu.Lock()
		if r.route.didSolicit {
			url = r.route.url
			rtype = r.route.routeType
		}
		r.mu.Unlock()
		if url != nil {
			return url, rtype, true
		}
	}
	return nil, 0, false
}

func upgradeRouteToSolicited(r *client, url *url.URL, rtype RouteType) {
	if r == nil {
		return
	}
	r.mu.Lock()
	// On disconnect, an explicit route only reconnects if the connection's
	// URL matches a configured route. So adopt the given URL rather than
	// keeping a gossiped one, which may not match any configured route.
	if !r.route.didSolicit || (rtype == Explicit && r.route.routeType != Explicit) {
		r.route.url = url
	}
	r.route.didSolicit = true
	if rtype == Explicit {
		r.route.routeType = Explicit
	}
	r.mu.Unlock()
}

func handleDuplicateRoute(remote, c *client, setNoReconnect bool) {
	// We used to clear some fields when closing a duplicate connection
	// to prevent sending INFO protocols for the remotes to update
	// their leafnode/gateway URLs. This is no longer needed since
	// removeRoute() now does the right thing of doing that only when
	// the closed connection was an added route connection.
	c.mu.Lock()
	didSolicit := c.route.didSolicit
	url := c.route.url
	rtype := c.route.routeType
	if setNoReconnect {
		c.flags.set(noReconnect)
	}
	c.mu.Unlock()

	if remote == nil {
		return
	}

	remote.mu.Lock()
	if didSolicit {
		// On disconnect, an explicit route only reconnects if the connection's
		// URL matches a configured route. So adopt the given URL rather than
		// keeping a gossiped one, which may not match any configured route.
		if !remote.route.didSolicit || (rtype == Explicit && remote.route.routeType != Explicit) {
			remote.route.url = url
		}
		remote.route.didSolicit = true
	}
	// The extra route might be an configured explicit route
	// so keep the state that the remote was configured.
	if rtype == Explicit {
		remote.route.routeType = rtype
	}
	// This is to mitigate the issue where both sides add the route
	// on the opposite connection, and therefore end-up with both
	// connections being dropped.
	remote.route.retry = true
	remote.mu.Unlock()
}

// Import filter check.
func (c *client) importFilter(sub *subscription) bool {
	if c.perms == nil {
		return true
	}
	return c.canImport(string(sub.subject))
}

// updateRouteSubscriptionMap will make sure to update the route map for the subscription. Will
// also forward to all routes if needed.
func (s *Server) updateRouteSubscriptionMap(acc *Account, sub *subscription, delta int32) {
	if acc == nil || sub == nil {
		return
	}

	// We only store state on local subs for transmission across all other routes.
	if sub.client == nil || sub.client.kind == ROUTER || sub.client.kind == GATEWAY {
		return
	}

	if sub.si {
		return
	}

	// Copy to hold outside acc lock.
	var n int64
	var ok bool

	isq := len(sub.queue) > 0

	accLock := func() {
		// Not required for code correctness, but helps reduce the number of
		// updates sent to the routes when processing high number of concurrent
		// queue subscriptions updates (sub/unsub).
		// See https://github.com/nats-io/nats-server/pull/1126 for more details.
		if isq {
			acc.smu.Lock()
		}
		acc.mu.Lock()
	}
	accUnlock := func() {
		acc.mu.Unlock()
		if isq {
			acc.smu.Unlock()
		}
	}

	accLock()

	// This is non-nil when we know we are in cluster mode.
	rm, lws := acc.rm, acc.lws
	if rm == nil {
		accUnlock()
		return
	}

	// Create the subscription key which will prevent collisions between regular
	// and leaf routed subscriptions. See keyFromSubWithOrigin() for details.
	key := keyFromSubWithOrigin(sub)

	// Decide whether we need to send an update out to all the routes.
	update := isq

	// This is where we do update to account. For queues we need to take
	// special care that this order of updates is same as what is sent out
	// over routes.
	if n, ok = rm[key]; ok {
		n += int64(delta)
		if n <= 0 {
			delete(rm, key)
			delete(lws, key)
			update = true // Update for deleting (N->0)
		} else {
			rm[key] = n
		}
	} else if delta > 0 {
		n = int64(delta)
		rm[key] = n
		update = true // Adding a new entry for normal sub means update (0->1)
	}

	accUnlock()

	if !update {
		return
	}

	// If we are sending a queue sub, make a copy and place in the queue weight.
	// FIXME(dlc) - We can be smarter here and avoid copying and acquiring the lock.
	if isq {
		sub.client.mu.Lock()
		nsub := *sub
		sub.client.mu.Unlock()
		nsub.qw = queueWeight(n)
		sub = &nsub
	}

	// We need to send out this update. Gather routes
	var _routes [32]*client
	routes := _routes[:0]

	s.mu.RLock()
	// The account's routePoolIdx field is set/updated under the server lock
	// (but also the account's lock). So we don't need to acquire the account's
	// lock here to get the value.
	if poolIdx := acc.routePoolIdx; poolIdx < 0 {
		if conns, ok := s.accRoutes[acc.Name]; ok {
			for _, r := range conns {
				routes = append(routes, r)
			}
		}
		if s.routesNoPool > 0 {
			// We also need to look for "no pool" remotes (that is, routes to older
			// servers or servers that have explicitly disabled pooling).
			s.forEachRemote(func(r *client) {
				r.mu.Lock()
				if r.route.noPool {
					routes = append(routes, r)
				}
				r.mu.Unlock()
			})
		}
	} else {
		// We can't use s.forEachRouteIdx here since we want to check/get the
		// "no pool" route ONLY if we don't find a route at the given `poolIdx`.
		for _, conns := range s.routes {
			if r := conns[poolIdx]; r != nil {
				routes = append(routes, r)
			} else if s.routesNoPool > 0 {
				// Check if we have a "no pool" route at index 0, and if so, it
				// means that for this remote, we have a single connection because
				// that server does not have pooling.
				if r := conns[0]; r != nil {
					r.mu.Lock()
					if r.route.noPool {
						routes = append(routes, r)
					}
					r.mu.Unlock()
				}
			}
		}
	}
	trace := atomic.LoadInt32(&s.logging.trace) == 1
	s.mu.RUnlock()

	// We need to make sure our updates are serialized from potential multiple connections. We want
	// to make sure that the order above is preserved here but not necessarily all updates need to
	// be sent. We need to block and recheck the n count with the lock held through sending here.
	//
	// However, we can't hold the acc.mu lock since we allow client.mu.Lock -> acc.mu.Lock
	// but not the opposite. So use a dedicated lock while holding the route's lock.
	acc.smu.Lock()
	defer acc.smu.Unlock()

	acc.mu.Lock()
	n = rm[key]
	qw := queueWeight(n)
	if isq {
		sub.qw = qw
	}
	// Check the last sent value here. If same, then someone beat us to it and
	// we can just return here. Otherwise update.
	if ls, ok := lws[key]; ok && ls == qw {
		acc.mu.Unlock()
		return
	} else if n > 0 {
		lws[key] = qw
	}
	acc.mu.Unlock()

	// Snapshot into array
	subs := []*subscription{sub}

	// Deliver to all routes.
	for _, route := range routes {
		route.mu.Lock()
		// Note that queue unsubs where n > 0 are still
		// subscribes with a smaller weight.
		route.sendRouteSubOrUnSubProtos(subs, n > 0, trace, route.importFilter)
		route.mu.Unlock()
	}
}

// This starts the route accept loop in a go routine, unless it
// is detected that the server has already been shutdown.
// It will also start soliciting explicit routes.
func (s *Server) startRouteAcceptLoop() {
	if s.isShuttingDown() {
		return
	}

	// Snapshot server options.
	opts := s.getOpts()

	// Snapshot server options.
	port := opts.Cluster.Port

	if port == -1 {
		port = 0
	}

	// This requires lock, so do this outside of may block.
	clusterName := s.ClusterName()

	s.mu.Lock()
	s.Noticef("Cluster name is %s", clusterName)
	if s.isClusterNameDynamic() {
		s.Warnf("Cluster name was dynamically generated, consider setting one")
	}

	hp := net.JoinHostPort(opts.Cluster.Host, strconv.Itoa(port))
	l, e := natsListen("tcp", hp)
	s.routeListenerErr = e
	if e != nil {
		s.mu.Unlock()
		s.Fatalf("Error listening on router port: %d - %v", opts.Cluster.Port, e)
		return
	}
	s.Noticef("Listening for route connections on %s",
		net.JoinHostPort(opts.Cluster.Host, strconv.Itoa(l.Addr().(*net.TCPAddr).Port)))

	// Check for TLSConfig
	tlsReq := opts.Cluster.TLSConfig != nil
	info := Info{
		ID:           s.info.ID,
		Name:         s.info.Name,
		Version:      s.info.Version,
		GoVersion:    runtime.Version(),
		AuthRequired: false,
		TLSRequired:  tlsReq,
		TLSVerify:    tlsReq,
		MaxPayload:   s.info.MaxPayload,
		JetStream:    s.info.JetStream,
		Proto:        s.getServerProto(),
		GatewayURL:   s.getGatewayURL(),
		Headers:      s.supportsHeaders(),
		Cluster:      s.info.Cluster,
		Domain:       s.info.Domain,
		Dynamic:      s.isClusterNameDynamic(),
		LNOC:         true,
		LNOCU:        true,
		LN:           true,
	}
	// For tests that want to simulate old servers, do not set the compression
	// on the INFO protocol if configured with CompressionNotSupported.
	if cm := opts.Cluster.Compression.Mode; cm != CompressionNotSupported {
		info.Compression = cm
	}
	if ps := opts.Cluster.PoolSize; ps > 0 {
		info.RoutePoolSize = ps
	}
	// Set this if only if advertise is not disabled
	if !opts.Cluster.NoAdvertise {
		info.ClientConnectURLs = s.clientConnectURLs
		info.WSConnectURLs = s.websocket.connectURLs
	}
	// If we have selected a random port...
	if port == 0 {
		// Write resolved port back to options.
		opts.Cluster.Port = l.Addr().(*net.TCPAddr).Port
	}
	// Check for Auth items
	if opts.Cluster.Username != "" {
		info.AuthRequired = true
	}
	// Check for permissions.
	if opts.Cluster.Permissions != nil {
		info.Import = opts.Cluster.Permissions.Import
		info.Export = opts.Cluster.Permissions.Export
	}
	// If this server has a LeafNode accept loop, s.leafNodeInfo.IP is,
	// at this point, set to the host:port for the leafnode accept URL,
	// taking into account possible advertise setting. Use the LeafNodeURLs
	// and set this server's leafnode accept URL. This will be sent to
	// routed servers.
	if !opts.LeafNode.NoAdvertise && s.leafNodeInfo.IP != _EMPTY_ {
		info.LeafNodeURLs = []string{s.leafNodeInfo.IP}
	}
	s.routeInfo = info
	// Possibly override Host/Port and set IP based on Cluster.Advertise
	if err := s.setRouteInfoHostPortAndIP(); err != nil {
		s.Fatalf("Error setting route INFO with Cluster.Advertise value of %s, err=%v", opts.Cluster.Advertise, err)
		l.Close()
		s.mu.Unlock()
		return
	}
	// Setup state that can enable shutdown
	s.routeListener = l
	// Warn if using Cluster.Insecure
	if tlsReq && opts.Cluster.TLSConfig.InsecureSkipVerify {
		s.Warnf(clusterTLSInsecureWarning)
	}

	// Now that we have the port, keep track of all ip:port that resolve to this server.
	if interfaceAddr, err := net.InterfaceAddrs(); err == nil {
		var localIPs []string
		for i := 0; i < len(interfaceAddr); i++ {
			interfaceIP, _, _ := net.ParseCIDR(interfaceAddr[i].String())
			ipStr := interfaceIP.String()
			if net.ParseIP(ipStr) != nil {
				localIPs = append(localIPs, ipStr)
			}
		}
		var portStr = strconv.FormatInt(int64(s.routeInfo.Port), 10)
		for _, ip := range localIPs {
			ipPort := net.JoinHostPort(ip, portStr)
			s.routesToSelf[ipPort] = struct{}{}
		}
	}

	// Start the accept loop in a different go routine.
	go s.acceptConnections(l, "Route", func(conn net.Conn) { s.createRoute(conn, nil, Implicit, gossipDefault, _EMPTY_) }, nil)

	// Solicit Routes if applicable. This will not block.
	s.solicitRoutes(opts.Routes, opts.Cluster.PinnedAccounts)

	s.mu.Unlock()
}

// Similar to setInfoHostPortAndGenerateJSON, but for routeInfo.
func (s *Server) setRouteInfoHostPortAndIP() error {
	opts := s.getOpts()
	if opts.Cluster.Advertise != _EMPTY_ {
		advHost, advPort, err := parseHostPort(opts.Cluster.Advertise, opts.Cluster.Port)
		if err != nil {
			return err
		}
		s.routeInfo.Host = advHost
		s.routeInfo.Port = advPort
		s.routeInfo.IP = fmt.Sprintf("nats-route://%s/", net.JoinHostPort(advHost, strconv.Itoa(advPort)))
	} else {
		s.routeInfo.Host = opts.Cluster.Host
		s.routeInfo.Port = opts.Cluster.Port
		s.routeInfo.IP = ""
	}
	return nil
}

// StartRouting will start the accept loop on the cluster host:port
// and will actively try to connect to listed routes.
func (s *Server) StartRouting(clientListenReady chan struct{}) {
	defer s.grWG.Done()

	// Wait for the client and leafnode listen ports to be opened,
	// and the possible ephemeral ports to be selected.
	<-clientListenReady

	// Start the accept loop and solicitation of explicit routes (if applicable)
	s.startRouteAcceptLoop()

}

func (s *Server) reConnectToRoute(rURL *url.URL, rtype RouteType, accName string) {
	// If A connects to B, and B to A (regardless if explicit or
	// implicit - due to auto-discovery), and if each server first
	// registers the route on the opposite TCP connection, the
	// two connections will end-up being closed.
	// Add some random delay to reduce risk of repeated failures.
	delay := time.Duration(rand.IntN(100)) * time.Millisecond
	if rtype == Explicit {
		delay += routeReconnectDelay
	}
	select {
	case <-time.After(delay):
	case <-s.quitCh:
		s.grWG.Done()
		return
	}
	s.connectToRoute(rURL, rtype, false, gossipDefault, accName)
}

// Checks to make sure the route is still valid.
func (s *Server) routeStillValid(rURL *url.URL) bool {
	for _, ri := range s.getOpts().Routes {
		if urlsAreEqual(ri, rURL) {
			return true
		}
	}
	return false
}

func (s *Server) connectToRoute(rURL *url.URL, rtype RouteType, firstConnect bool, gossipMode byte, accName string) {
	defer s.grWG.Done()
	if rURL == nil {
		return
	}
	// For explicit routes, we will try to connect until we succeed. For implicit
	// we will try only based on the number of ConnectRetries optin.
	tryForEver := rtype == Explicit

	// Snapshot server options.
	opts := s.getOpts()

	const connErrFmt = "Error trying to connect to route (attempt %v): %v"

	s.mu.RLock()
	resolver := s.routeResolver
	excludedAddresses := s.routesToSelf
	s.mu.RUnlock()

	attemptDelay := routeConnectDelay
	reconnectTimer := time.NewTimer(attemptDelay)
	reconnectTimer.Stop()
	defer stopAndClearTimer(&reconnectTimer)

	for attempts := 0; s.isRunning(); {
		if tryForEver {
			if !s.routeStillValid(rURL) {
				s.Debugf("Not attempting to connect to explicit route %q, no longer part of configured routes", rURL.Redacted())
				return
			}
			if accName != _EMPTY_ {
				s.mu.RLock()
				_, valid := s.accRoutes[accName]
				s.mu.RUnlock()
				if !valid {
					s.Debugf("Not attempting to connect to route %q for account %q, no longer a per-account route", rURL.Redacted(), accName)
					return
				}
			}
		}
		var conn net.Conn
		address, err := s.getRandomIP(resolver, rURL.Host, excludedAddresses)
		if err == errNoIPAvail {
			// This is ok, we are done.
			return
		}
		if err == nil {
			s.Debugf("Trying to connect to route on %s (%s)", rURL.Host, address)
			conn, err = natsDialTimeout("tcp", address, DEFAULT_ROUTE_DIAL)
		}
		if err != nil {
			attempts++
			if s.shouldReportConnectErr(firstConnect, attempts) {
				s.Errorf(connErrFmt, attempts, err)
			} else {
				s.Debugf(connErrFmt, attempts, err)
			}
			if !tryForEver {
				if opts.Cluster.ConnectRetries <= 0 {
					return
				}
				if attempts > opts.Cluster.ConnectRetries {
					return
				}
			}
			reconnectTimer.Reset(attemptDelay)
			select {
			case <-s.quitCh:
				return
			case <-reconnectTimer.C:
				if opts.Cluster.ConnectBackoff {
					// Use exponential backoff for connection attempts.
					attemptDelay *= 2
					if attemptDelay > routeConnectMaxDelay {
						attemptDelay = routeConnectMaxDelay
					}
				}
				continue
			}
		}

		if tryForEver && !s.routeStillValid(rURL) {
			conn.Close()
			return
		}

		// We have a route connection here.
		// Go ahead and create it and exit this func.
		s.createRoute(conn, rURL, rtype, gossipMode, accName)
		return
	}
}

func (c *client) isSolicitedRoute() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.kind == ROUTER && c.route != nil && c.route.didSolicit
}

// Save the first hostname found in route URLs. This will be used in gossip mode
// when trying to create a TLS connection by setting the tlsConfig.ServerName.
// Lock is held on entry
func (s *Server) saveRouteTLSName(routes []*url.URL) {
	for _, u := range routes {
		if s.routeTLSName == _EMPTY_ && net.ParseIP(u.Hostname()) == nil {
			s.routeTLSName = u.Hostname()
		}
	}
}

// Start connection process to provided routes. Each route connection will
// be started in a dedicated go routine.
// Lock is held on entry
func (s *Server) solicitRoutes(routes []*url.URL, accounts []string) {
	s.saveRouteTLSName(routes)
	for _, r := range routes {
		route := r
		s.startGoRoutine(func() { s.connectToRoute(route, Explicit, true, gossipDefault, _EMPTY_) })
	}
	// Now go over possible per-account routes and create them.
	for _, an := range accounts {
		for _, r := range routes {
			route, accName := r, an
			s.startGoRoutine(func() { s.connectToRoute(route, Explicit, true, gossipDefault, accName) })
		}
	}
}

func (c *client) processRouteConnect(srv *Server, arg []byte, lang string) error {
	// Way to detect clients that incorrectly connect to the route listen
	// port. Client provide Lang in the CONNECT protocol while ROUTEs don't.
	if lang != "" {
		c.sendErrAndErr(ErrClientConnectedToRoutePort.Error())
		c.closeConnection(WrongPort)
		return ErrClientConnectedToRoutePort
	}
	// Unmarshal as a route connect protocol
	proto := &connectInfo{}

	if err := json.Unmarshal(arg, proto); err != nil {
		return err
	}
	// Reject if this has Gateway which means that it would be from a gateway
	// connection that incorrectly connects to the Route port.
	if proto.Gateway != "" {
		errTxt := fmt.Sprintf("Rejecting connection from gateway %q on the Route port", proto.Gateway)
		c.Errorf(errTxt)
		c.sendErr(errTxt)
		c.closeConnection(WrongGateway)
		return ErrWrongGateway
	}

	if srv == nil {
		return ErrServerNotRunning
	}
	if proto.Cluster == leafNoOriginCluster {
		errTxt := fmt.Sprintf("Rejecting connection, cluster name %q is reserved", proto.Cluster)
		c.Errorf(errTxt)
		c.sendErr(errTxt)
		c.closeConnection(ClusterNameConflict)
		return ErrClusterNameReserved
	}

	perms := srv.getOpts().Cluster.Permissions
	clusterName := srv.ClusterName()

	// If we have a cluster name set, make sure it matches ours.
	if proto.Cluster != clusterName {
		shouldReject := true
		// If we have a dynamic name we will do additional checks.
		if srv.isClusterNameDynamic() {
			if !proto.Dynamic || strings.Compare(clusterName, proto.Cluster) < 0 {
				// We will take on their name since theirs is configured or higher then ours.
				srv.setClusterName(proto.Cluster)
				if !proto.Dynamic {
					srv.optsMu.Lock()
					srv.opts.Cluster.Name = proto.Cluster
					srv.optsMu.Unlock()
				}
				c.mu.Lock()
				remoteID := c.opts.Name
				c.mu.Unlock()
				srv.removeAllRoutesExcept(remoteID)
				shouldReject = false
			}
		}
		if shouldReject {
			errTxt := fmt.Sprintf("Rejecting connection, cluster name %q does not match %q", proto.Cluster, clusterName)
			c.Errorf(errTxt)
			c.sendErr(errTxt)
			c.closeConnection(ClusterNameConflict)
			return ErrClusterNameRemoteConflict
		}
	}

	supportsHeaders := c.srv.supportsHeaders()

	// Grab connection name of remote route.
	c.mu.Lock()
	c.route.remoteID = c.opts.Name
	c.route.ln = proto.LN // ... also implies LNOC+LNOCU
	c.route.lnoc = proto.LNOC || c.route.ln
	c.route.lnocu = proto.LNOCU || c.route.ln
	c.setRoutePermissions(perms)
	c.headers = supportsHeaders && proto.Headers
	c.mu.Unlock()
	return nil
}

// Called when we update our cluster name during negotiations with remotes.
func (s *Server) removeAllRoutesExcept(remoteID string) {
	s.mu.Lock()
	routes := make([]*client, 0, s.numRoutes())
	for rID, conns := range s.routes {
		if rID == remoteID {
			continue
		}
		for _, r := range conns {
			if r != nil {
				routes = append(routes, r)
			}
		}
	}
	for _, conns := range s.accRoutes {
		for rID, r := range conns {
			if rID == remoteID {
				continue
			}
			routes = append(routes, r)
		}
	}
	s.mu.Unlock()

	for _, r := range routes {
		r.closeConnection(ClusterNameConflict)
	}
}

func (s *Server) removeRoute(c *client) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		rID           string
		lnURL         string
		gwURL         string
		idHash        string
		accName       string
		poolIdx       = -1
		connectURLs   []string
		wsConnectURLs []string
		opts          = s.getOpts()
		rURL          *url.URL
		noPool        bool
		rtype         RouteType
	)
	c.mu.Lock()
	cid := c.cid
	r := c.route
	if r != nil {
		rID = r.remoteID
		lnURL = r.leafnodeURL
		idHash = r.idHash
		gwURL = r.gatewayURL
		poolIdx = r.poolIdx
		accName = bytesToString(r.accName)
		if r.noPool {
			s.routesNoPool--
			noPool = true
		}
		connectURLs = r.connectURLs
		wsConnectURLs = r.wsConnURLs
		rURL = r.url
		rtype = r.routeType
	}
	c.mu.Unlock()
	if accName != _EMPTY_ {
		if conns, ok := s.accRoutes[accName]; ok {
			if r := conns[rID]; r == c {
				s.removeRouteByHash(idHash + accName)
				delete(conns, rID)
				// Do not remove or set to nil when all remotes have been
				// removed from the map. The configured accounts must always
				// be in the accRoutes map and addRoute expects "conns" map
				// to be created.
			}
		}
	}
	// If this is still -1, it means that it was not added to the routes
	// so simply remove from temp clients and we are done.
	if poolIdx == -1 || accName != _EMPTY_ {
		s.removeFromTempClients(cid)
		return
	}
	if conns, ok := s.routes[rID]; ok {
		// If this route was not the one stored, simply remove from the
		// temporary map and be done.
		if conns[poolIdx] != c {
			s.removeFromTempClients(cid)
			return
		}
		conns[poolIdx] = nil
		// Now check if this was the last connection to be removed.
		empty := true
		for _, c := range conns {
			if c != nil {
				empty = false
				break
			}
		}
		// This was the last route for this remote. Remove the remote entry
		// and possibly send some async INFO protocols regarding gateway
		// and leafnode URLs.
		if empty {
			delete(s.routes, rID)

			// Since this is the last route for this remote, possibly update
			// the client connect URLs and send an update to connected
			// clients.
			if (len(connectURLs) > 0 || len(wsConnectURLs) > 0) && !opts.Cluster.NoAdvertise {
				s.removeConnectURLsAndSendINFOToClients(connectURLs, wsConnectURLs)
			}
			// Remove the remote's gateway URL from our list and
			// send update to inbound Gateway connections.
			if gwURL != _EMPTY_ && s.removeGatewayURL(gwURL) {
				s.sendAsyncGatewayInfo()
			}
			// Remove the remote's leafNode URL from
			// our list and send update to LN connections.
			if lnURL != _EMPTY_ && s.removeLeafNodeURL(lnURL) {
				s.sendAsyncLeafNodeInfo()
			}
			// We can remove the configured route pool size of this remote.
			delete(s.remoteRoutePoolSize, rID)
			// If this server has pooling/pinned accounts and the route for
			// this remote was a "no pool" route, attempt to reconnect.
			if noPool {
				if s.routesPoolSize > 1 {
					s.startGoRoutine(func() { s.connectToRoute(rURL, rtype, true, gossipDefault, _EMPTY_) })
				}
				if len(opts.Cluster.PinnedAccounts) > 0 {
					for _, an := range opts.Cluster.PinnedAccounts {
						accName := an
						s.startGoRoutine(func() { s.connectToRoute(rURL, rtype, true, gossipDefault, accName) })
					}
				}
			}
		}
		// This is for gateway code. Remove this route from a map that uses
		// the route hash in combination with the pool index as the key.
		if s.routesPoolSize > 1 {
			idHash += strconv.Itoa(poolIdx)
		}
		s.removeRouteByHash(idHash)
	}
	s.removeFromTempClients(cid)
}

func (s *Server) isDuplicateServerName(name string) bool {
	if name == _EMPTY_ {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.info.Name == name {
		return true
	}
	for _, conns := range s.routes {
		for _, r := range conns {
			if r != nil {
				r.mu.Lock()
				duplicate := r.route.remoteName == name
				r.mu.Unlock()
				if duplicate {
					return true
				}
				break
			}
		}
	}
	return false
}

// Goes over each non-nil route connection for all remote servers
// and invokes the function `f`. It does not go over per-account
// routes.
// Server lock is held on entry.
func (s *Server) forEachNonPerAccountRoute(f func(r *client)) {
	for _, conns := range s.routes {
		for _, r := range conns {
			if r != nil {
				f(r)
			}
		}
	}
}

// Goes over each non-nil route connection for all remote servers
// and invokes the function `f`. This also includes the per-account
// routes.
// Server lock is held on entry.
func (s *Server) forEachRoute(f func(r *client)) {
	s.forEachNonPerAccountRoute(f)
	for _, conns := range s.accRoutes {
		for _, r := range conns {
			f(r)
		}
	}
}

// Goes over each non-nil route connection at the given pool index
// location in the slice and invokes the function `f`. If the
// callback returns `true`, this function moves to the next remote.
// Otherwise, the iteration over removes stops.
// This does not include per-account routes.
// Server lock is held on entry.
func (s *Server) forEachRouteIdx(idx int, f func(r *client) bool) {
	for _, conns := range s.routes {
		if r := conns[idx]; r != nil {
			if !f(r) {
				return
			}
		}
	}
}

// Goes over each remote and for the first non nil route connection,
// invokes the function `f`.
// Server lock is held on entry.
func (s *Server) forEachRemote(f func(r *client)) {
	for _, conns := range s.routes {
		for _, r := range conns {
			if r != nil {
				f(r)
				break
			}
		}
	}
}
