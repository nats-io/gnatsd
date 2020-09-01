// Copyright 2013-2020 The NATS Authors
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
	"math/rand"
	"net"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
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

const (
	// RouteProtoZero is the original Route protocol from 2009.
	// http://nats.io/documentation/internals/nats-protocol/
	RouteProtoZero = iota
	// RouteProtoInfo signals a route can receive more then the original INFO block.
	// This can be used to update remote cluster permissions, etc...
	RouteProtoInfo
	// RouteProtoV2 is the new route/cluster protocol that provides account support.
	RouteProtoV2
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

// Used by tests
func setRouteProtoForTest(wantedProto int) int {
	return (wantedProto + 1) * -1
}

type route struct {
	remoteID     string
	remoteName   string
	didSolicit   bool
	retry        bool
	lnoc         bool
	routeType    RouteType
	url          *url.URL
	authRequired bool
	tlsRequired  bool
	connectURLs  []string
	wsConnURLs   []string
	replySubs    map[*subscription]*time.Timer
	gatewayURL   string
	leafnodeURL  string
	hash         string

	// Remember the last cluster name and cluster dynamic state
	// from a route in case it has been announced to us.
	clusterName    string
	clusterDynamic bool
}

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
	Gateway  string `json:"gateway,omitempty"`
}

// Route protocol constants
const (
	ConProto  = "CONNECT %s" + _CRLF_
	InfoProto = "INFO %s" + _CRLF_
)

const (
	// Used to decide if the sending of the route SUBs list should be
	// done in place or in separate go routine.
	sendRouteSubsInGoRoutineThreshold = 1024 * 1024 // 1MB

	// Warning when user configures cluster TLS insecure
	clusterTLSInsecureWarning = "TLS certificate chain and hostname of solicited routes will not be verified. DO NOT USE IN PRODUCTION!"
)

// Can be changed for tests
var routeConnectDelay = DEFAULT_ROUTE_CONNECT

// removeReplySub is called when we trip the max on remoteReply subs.
func (c *client) removeReplySub(sub *subscription) {
	if sub == nil {
		return
	}
	// Lookup the account based on sub.sid.
	if i := bytes.Index(sub.sid, []byte(" ")); i > 0 {
		// First part of SID for route is account name.
		if v, ok := c.srv.accounts.Load(string(sub.sid[:i])); ok {
			(v.(*Account)).sl.Remove(sub)
		}
		c.mu.Lock()
		c.removeReplySubTimeout(sub)
		delete(c.subs, string(sub.sid))
		c.mu.Unlock()
	}
}

// removeReplySubTimeout will remove a timer if it exists.
// Lock should be held upon entering.
func (c *client) removeReplySubTimeout(sub *subscription) {
	// Remove any reply sub timer if it exists.
	if c.route == nil || c.route.replySubs == nil {
		return
	}
	if t, ok := c.route.replySubs[sub]; ok {
		t.Stop()
		delete(c.route.replySubs, sub)
	}
}

func (c *client) processAccountSub(arg []byte) error {
	accName := string(arg)
	if c.kind == GATEWAY {
		return c.processGatewayAccountSub(accName)
	}
	return nil
}

func (c *client) processAccountUnsub(arg []byte) {
	accName := string(arg)
	if c.kind == GATEWAY {
		c.processGatewayAccountUnsub(accName)
	}
}

// Process an inbound LMSG specification from the remote route. This means
// we have an origin cluster and we force header semantics.
func (c *client) processRoutedOriginClusterMsgArgs(arg []byte) error {
	// Unroll splitArgs to avoid runtime/heap issues
	a := [MAX_HMSG_ARGS + 1][]byte{}
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
	c.pa.pacache = arg[len(args[0])+1 : len(args[0])+len(args[1])+len(args[2])+2]

	return nil
}

// Process an inbound HMSG specification from the remote route.
func (c *client) processRoutedHeaderMsgArgs(arg []byte) error {
	// Unroll splitArgs to avoid runtime/heap issues
	a := [MAX_HMSG_ARGS][]byte{}
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
	c.pa.pacache = arg[:len(args[0])+len(args[1])+1]
	return nil
}

// Process an inbound RMSG or LMSG specification from the remote route.
func (c *client) processRoutedMsgArgs(arg []byte) error {
	// Unroll splitArgs to avoid runtime/heap issues
	a := [MAX_RMSG_ARGS][]byte{}
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
	c.pa.pacache = arg[:len(args[0])+len(args[1])+1]
	return nil
}

// processInboundRouteMsg is called to process an inbound msg from a route.
func (c *client) processInboundRoutedMsg(msg []byte) {
	// Update statistics
	c.in.msgs++
	// The msg includes the CR_LF, so pull back out for accounting.
	c.in.bytes += int32(len(msg) - LEN_CR_LF)

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
	}

	b, err := json.Marshal(cinfo)
	if err != nil {
		c.Errorf("Error marshaling CONNECT to route: %v\n", err)
		return err
	}
	c.enqueueProto([]byte(fmt.Sprintf(ConProto, b)))
	return nil
}

// Process the info message if we are a route.
func (c *client) processRouteInfo(info *Info) {
	// We may need to update route permissions and will need the account
	// sublist. Since getting the account requires server lock, do the
	// lookup now.

	// FIXME(dlc) - Add account scoping.
	gacc := c.srv.globalAccount()
	gacc.mu.RLock()
	sl := gacc.sl
	gacc.mu.RUnlock()

	supportsHeaders := c.srv.supportsHeaders()
	clusterName := c.srv.ClusterName()
	isSusceptible := c.srv.isClusterNameSusceptibleToChanges()

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

	// Detect if we have a mismatch of cluster names.
	if info.Cluster != "" && info.Cluster != clusterName {
		c.mu.Unlock()

		isDynamic := s.isClusterNameDynamic()
		remoteIsDynamic := info.Dynamic
		if isDynamic && !remoteIsDynamic {
			// The remote INFO sent to this node contains what should be the name of the cluster.
			// Update the name marking that no longer susceptible to dynamic cluster updates since
			// there is an explicit name in the cluster and cause other nodes to reconnect to
			// this node using the explicit name.
			s.setClusterName(info.Cluster, false)
			s.removeAllRoutesExcept(c)
			c.mu.Lock()

			c.route.clusterName = info.Cluster
			c.route.clusterDynamic = info.Dynamic
		} else if !isDynamic && remoteIsDynamic {
			c.srv.mu.Lock()
			routeInfoJSON := s.routeInfoJSON
			c.srv.mu.Unlock()

			c.mu.Lock()

			// In case there is a mismatch with what we know about this route,
			// we will send it another INFO message with the explicit cluster name
			// so that the remote dynamic node uses our cluster name.
			if c.route.clusterName != info.Cluster {
				c.enqueueProto(routeInfoJSON)
			}

			// Override the cluster name from this route with ours since it should win.
			c.route.clusterName = s.info.Cluster
			c.route.clusterDynamic = info.Dynamic
		} else if isDynamic && remoteIsDynamic {
			if isSusceptible {
				// Ignore the message unless we are susceptible to dynamic cluster updates and their
				// dynamic cluster name is a better fit than what we have. If we are not susceptible,
				// it means that there is an explicit name in the cluster and eventually nodes with the
				// explicit cluster name will make this node use the explicit cluster name via INFO messages.
				if strings.Compare(clusterName, info.Cluster) < 0 {
					s.setClusterName(info.Cluster, true)
					s.removeAllRoutesExcept(c)
				}
			} else if !s.nonDynamicClusterNameRoutePresent(info.ID) {
				// If the route moved from being static to dynamic (thus also changing its name),
				// then only use their name if no other static, non dynamic nodes exist and it beats
				// this node's default unique cluster name.  This can happen for example when the last node
				// from a cluster of nodes has its config reloaded to remove the name and go back to dynamic mode.

				// Find any other route with an explicit name but the one that just went back to dynamic mode.
				// NOTE: Sometimes nodes with dynamic names can also send their INFOs but due to races,
				// by the time others have received it they have already changed its name to an explicit one
				// due to a CONNECT message that was sent.  This check also covers that scenario.

				// Use the correct name and recluster with that name.
				if strings.Compare(s.defaultClusterName, info.Cluster) < 0 {
					// Use theirs, let other nodes reconnect using their cluster name.
					s.setClusterName(info.Cluster, true)
					s.removeAllRoutesExcept(c)
				} else {
					// Use ours, let other nodes reconnect using our cluster name.
					s.setClusterName(s.defaultClusterName, true)
					s.removeAllRoutesExcept(c)
				}
			}

			c.mu.Lock()
			c.route.clusterName = info.Cluster
			c.route.clusterDynamic = info.Dynamic
		} else {
			// We have an explicit cluster name and reject explicit cluster name changes.
			c.closeConnection(ClusterNameConflict)
			return
		}
	}

	// If this is an async INFO from an existing route...
	if c.flags.isSet(infoReceived) {
		remoteID := c.route.remoteID

		// Check if this is an INFO for gateways...
		if info.Gateway != "" {
			c.mu.Unlock()
			// If this server has no gateway configured, report error and return.
			if !s.gateway.enabled {
				// FIXME: Should this be a Fatalf()?
				s.Errorf("Received information about gateway %q from %s, but gateway is not configured",
					info.Gateway, remoteID)
				return
			}
			s.processGatewayInfoFromRoute(info, remoteID, c)
			return
		}

		// We receive an INFO from a server that informs us about another server,
		// so the info.ID in the INFO protocol does not match the ID of this route.
		if remoteID != "" && remoteID != info.ID {
			c.mu.Unlock()

			// Process this implicit route. We will check that it is not an explicit
			// route and/or that it has not been connected already.
			s.processImplicitRoute(info)
			return
		}

		var connectURLs []string
		var wsConnectURLs []string

		// If we are notified that the remote is going into LDM mode, capture route's connectURLs.
		if info.LameDuckMode {
			connectURLs = c.route.connectURLs
			wsConnectURLs = c.route.wsConnURLs
		} else {
			// If this is an update due to config reload on the remote server,
			// need to possibly send local subs to the remote server.
			c.updateRemoteRoutePerms(sl, info)
		}
		c.mu.Unlock()

		// If the remote is going into LDM and there are client connect URLs
		// associated with this route and we are allowed to advertise, remove
		// those URLs and update our clients.
		if (len(connectURLs) > 0 || len(wsConnectURLs) > 0) && !s.getOpts().Cluster.NoAdvertise {
			s.removeConnectURLsAndSendINFOToClients(connectURLs, wsConnectURLs)
		}
		return
	}

	// Mark that the INFO protocol has been received, so we can detect updates.
	c.flags.set(infoReceived)

	// Get the route's proto version
	c.opts.Protocol = info.Proto

	// Headers
	c.headers = supportsHeaders && info.Headers

	// Copy over important information.
	c.route.remoteID = info.ID
	c.route.authRequired = info.AuthRequired
	c.route.tlsRequired = info.TLSRequired
	c.route.gatewayURL = info.GatewayURL
	c.route.remoteName = info.Name
	c.route.lnoc = info.LNOC

	// When sent through route INFO, if the field is set, it should be of size 1.
	if len(info.LeafNodeURLs) == 1 {
		c.route.leafnodeURL = info.LeafNodeURLs[0]
	}
	// Compute the hash of this route based on remoteID
	c.route.hash = string(getHash(info.ID))

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

	// Check to see if we have this remote already registered.
	// This can happen when both servers have routes to each other.
	c.mu.Unlock()

	if added, sendInfo := s.addRoute(c, info); added {
		c.Debugf("Registering remote route %q", info.ID)

		// Send our subs to the other side.
		s.sendSubsToRoute(c)

		// Send info about the known gateways to this route.
		s.sendGatewayConfigsToRoute(c)

		// sendInfo will be false if the route that we just accepted
		// is the only route there is.
		if sendInfo {
			// The incoming INFO from the route will have IP set
			// if it has Cluster.Advertise. In that case, use that
			// otherwise contruct it from the remote TCP address.
			if info.IP == "" {
				// Need to get the remote IP address.
				c.mu.Lock()
				switch conn := c.nc.(type) {
				case *net.TCPConn, *tls.Conn:
					addr := conn.RemoteAddr().(*net.TCPAddr)
					info.IP = fmt.Sprintf("nats-route://%s/", net.JoinHostPort(addr.IP.String(),
						strconv.Itoa(info.Port)))
				default:
					info.IP = c.route.url.String()
				}
				c.mu.Unlock()
			}
			// Now let the known servers know about this new route
			s.forwardNewRouteInfoToKnownServers(info)
		}
		// Unless disabled, possibly update the server's INFO protocol
		// and send to clients that know how to handle async INFOs.
		if !s.getOpts().Cluster.NoAdvertise {
			s.addConnectURLsAndSendINFOToClients(info.ClientConnectURLs, info.WSConnectURLs)
		}
	} else {
		c.Debugf("Detected duplicate remote route %q", info.ID)
		c.closeConnection(DuplicateRoute)
	}
}

// Possibly sends local subscriptions interest to this route
// based on changes in the remote's Export permissions.
// Lock assumed held on entry
func (c *client) updateRemoteRoutePerms(sl *Sublist, info *Info) {
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

	var (
		_localSubs [4096]*subscription
		localSubs  = _localSubs[:0]
	)
	sl.localSubs(&localSubs)

	c.sendRouteSubProtos(localSubs, false, func(sub *subscription) bool {
		subj := string(sub.subject)
		// If the remote can now export but could not before, and this server can import this
		// subject, then send SUB protocol.
		if newPermsTester.canExport(subj) && !oldPermsTester.canExport(subj) && c.canImport(subj) {
			return true
		}
		return false
	})
}

// sendAsyncInfoToClients sends an INFO protocol to all
// connected clients that accept async INFO updates.
// The server lock is held on entry.
func (s *Server) sendAsyncInfoToClients(regCli, wsCli bool) {
	// If there are no clients supporting async INFO protocols, we are done.
	// Also don't send if we are shutting down...
	if s.cproto == 0 || s.shutdown {
		return
	}
	info := s.copyInfo()

	for _, c := range s.clients {
		c.mu.Lock()
		// Here, we are going to send only to the clients that are fully
		// registered (server has received CONNECT and first PING). For
		// clients that are not at this stage, this will happen in the
		// processing of the first PING (see client.processPing)
		if ((regCli && c.ws == nil) || (wsCli && c.ws != nil)) &&
			c.opts.Protocol >= ClientProtoInfo &&
			c.flags.isSet(firstPongSent) {
			// sendInfo takes care of checking if the connection is still
			// valid or not, so don't duplicate tests here.
			c.enqueueProto(c.generateClientInfoJSON(info))
		}
		c.mu.Unlock()
	}
}

// This will process implicit route information received from another server.
// We will check to see if we have configured or are already connected,
// and if so we will ignore. Otherwise we will attempt to connect.
func (s *Server) processImplicitRoute(info *Info) {
	remoteID := info.ID

	s.mu.Lock()
	defer s.mu.Unlock()

	// Don't connect to ourself
	if remoteID == s.info.ID {
		return
	}
	// Check if this route already exists
	if _, exists := s.remotes[remoteID]; exists {
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

	// Snapshot server options.
	opts := s.getOpts()

	if info.AuthRequired {
		r.User = url.UserPassword(opts.Cluster.Username, opts.Cluster.Password)
	}
	s.startGoRoutine(func() { s.connectToRoute(r, false, true) })
}

// hasThisRouteConfigured returns true if info.Host:info.Port is present
// in the server's opts.Routes, false otherwise.
// Server lock is assumed to be held by caller.
func (s *Server) hasThisRouteConfigured(info *Info) bool {
	urlToCheckExplicit := strings.ToLower(net.JoinHostPort(info.Host, strconv.Itoa(info.Port)))
	for _, ri := range s.getOpts().Routes {
		if strings.ToLower(ri.Host) == urlToCheckExplicit {
			return true
		}
	}
	return false
}

// forwardNewRouteInfoToKnownServers sends the INFO protocol of the new route
// to all routes known by this server. In turn, each server will contact this
// new route.
func (s *Server) forwardNewRouteInfoToKnownServers(info *Info) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Note: nonce is not used in routes.
	// That being said, the info we get is the initial INFO which
	// contains a nonce, but we now forward this to existing routes,
	// so clear it now.
	info.Nonce = _EMPTY_
	b, _ := json.Marshal(info)
	infoJSON := []byte(fmt.Sprintf(InfoProto, b))

	for _, r := range s.routes {
		r.mu.Lock()
		if r.route.remoteID != info.ID {
			r.enqueueProto(infoJSON)
		}
		r.mu.Unlock()
	}
}

// canImport is whether or not we will send a SUB for interest to the other side.
// This is for ROUTER connections only.
// Lock is held on entry.
func (c *client) canImport(subject string) bool {
	// Use pubAllowed() since this checks Publish permissions which
	// is what Import maps to.
	return c.pubAllowedFullCheck(subject, false)
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

// removeRemoteSubs will walk the subs and remove them from the appropriate account.
func (c *client) removeRemoteSubs() {
	// We need to gather these on a per account basis.
	// FIXME(dlc) - We should be smarter about this..
	as := map[string]*asubs{}
	c.mu.Lock()
	srv := c.srv
	subs := c.subs
	c.subs = make(map[string]*subscription)
	c.mu.Unlock()

	for key, sub := range subs {
		c.mu.Lock()
		sub.max = 0
		c.mu.Unlock()
		// Grab the account
		accountName := strings.Fields(key)[0]
		ase := as[accountName]
		if ase == nil {
			if v, ok := srv.accounts.Load(accountName); ok {
				as[accountName] = &asubs{acc: v.(*Account), subs: []*subscription{sub}}
			} else {
				continue
			}
		} else {
			ase.subs = append(ase.subs, sub)
		}
		if srv.gateway.enabled {
			srv.gatewayUpdateSubInterest(accountName, sub, -1)
		}
	}

	// Now remove the subs by batch for each account sublist.
	for _, ase := range as {
		c.Debugf("Removing %d subscriptions for account %q", len(ase.subs), ase.acc.Name)
		ase.acc.sl.RemoveBatch(ase.subs)
	}
}

func (c *client) parseUnsubProto(arg []byte) (string, []byte, []byte, error) {
	// Indicate any activity, so pub and sub or unsubs.
	c.in.subs++

	args := splitArg(arg)
	var queue []byte

	switch len(args) {
	case 2:
	case 3:
		queue = args[2]
	default:
		return "", nil, nil, fmt.Errorf("parse error: '%s'", arg)
	}
	return string(args[0]), args[1], queue, nil
}

// Indicates no more interest in the given account/subject for the remote side.
func (c *client) processRemoteUnsub(arg []byte) (err error) {
	srv := c.srv
	if srv == nil {
		return nil
	}
	accountName, subject, _, err := c.parseUnsubProto(arg)
	if err != nil {
		return fmt.Errorf("processRemoteUnsub %s", err.Error())
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

	updateGWs := false
	// We store local subs by account and subject and optionally queue name.
	// RS- will have the arg exactly as the key.
	key := string(arg)
	sub, ok := c.subs[key]
	if ok {
		delete(c.subs, key)
		acc.sl.Remove(sub)
		c.removeReplySubTimeout(sub)
		updateGWs = srv.gateway.enabled
	}
	c.mu.Unlock()

	if updateGWs {
		srv.gatewayUpdateSubInterest(accountName, sub, -1)
	}

	// Now check on leafnode updates.
	srv.updateLeafNodes(acc, sub, -1)

	if c.opts.Verbose {
		c.sendOK()
	}
	return nil
}

func (c *client) processRemoteSub(argo []byte, hasOrigin bool) (err error) {
	// Indicate activity.
	c.in.subs++

	srv := c.srv
	if srv == nil {
		return nil
	}

	// Copy so we do not reference a potentially large buffer
	arg := make([]byte, len(argo))
	copy(arg, argo)

	args := splitArg(arg)
	sub := &subscription{client: c}

	var off int
	if hasOrigin {
		off = 1
		sub.origin = args[0]
	}

	switch len(args) {
	case 2 + off:
		sub.queue = nil
	case 4 + off:
		sub.queue = args[2+off]
		sub.qw = int32(parseSize(args[3+off]))
	default:
		return fmt.Errorf("processRemoteSub Parse Error: '%s'", arg)
	}
	sub.subject = args[1+off]

	// Lookup the account
	// FIXME(dlc) - This may start having lots of contention?
	accountName := string(args[0+off])
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
		expire := false
		isNew := false
		if !srv.NewAccountsAllowed() {
			// if the option of retrieving accounts later exists, create an expired one.
			// When a client comes along, expiration will prevent it from being used,
			// cause a fetch and update the account to what is should be.
			if staticResolver {
				c.Errorf("Unknown account %q for remote subject %q", accountName, sub.subject)
				return
			}
			c.Debugf("Unknown account %q for remote subject %q", accountName, sub.subject)
			expire = true
		}
		if acc, isNew = srv.LookupOrRegisterAccount(accountName); isNew && expire {
			acc.mu.Lock()
			acc.expired = true
			acc.mu.Unlock()
		}
	}

	c.mu.Lock()
	if c.isClosed() {
		c.mu.Unlock()
		return nil
	}

	// Check permissions if applicable.
	if !c.canExport(string(sub.subject)) {
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

	// We store local subs by account and subject and optionally queue name.
	// If we have a queue it will have a trailing weight which we do not want.
	if sub.queue != nil {
		sub.sid = arg[:len(arg)-len(args[3+off])-1]
	} else {
		sub.sid = arg
	}
	key := string(sub.sid)

	osub := c.subs[key]
	updateGWs := false
	if osub == nil {
		c.subs[key] = sub
		// Now place into the account sl.
		if err = acc.sl.Insert(sub); err != nil {
			delete(c.subs, key)
			c.mu.Unlock()
			c.Errorf("Could not insert subscription: %v", err)
			c.sendErr("Invalid Subscription")
			return nil
		}
		updateGWs = srv.gateway.enabled
	} else if sub.queue != nil {
		// For a queue we need to update the weight.
		atomic.StoreInt32(&osub.qw, sub.qw)
		acc.sl.UpdateRemoteQSub(osub)
	}
	c.mu.Unlock()

	if updateGWs {
		srv.gatewayUpdateSubInterest(acc.Name, sub, 1)
	}

	// Now check on leafnode updates.
	srv.updateLeafNodes(acc, sub, 1)

	if c.opts.Verbose {
		c.sendOK()
	}

	return nil
}

// sendSubsToRoute will send over our subject interest to
// the remote side. For each account we will send the
// complete interest for all subjects, both normal as a binary
// and queue group weights.
func (s *Server) sendSubsToRoute(route *client) {
	s.mu.Lock()
	// Estimated size of all protocols. It does not have to be accurate at all.
	eSize := 0
	// Send over our account subscriptions.
	// copy accounts into array first
	accs := make([]*Account, 0, 32)
	s.accounts.Range(func(k, v interface{}) bool {
		a := v.(*Account)
		accs = append(accs, a)
		a.mu.RLock()
		// Proto looks like: "RS+ <account name> <subject>[ <queue weight>]\r\n"
		// If we wanted to have better estimates (or even accurate), we would
		// collect the subs here instead of capturing the accounts and then
		// later going over each account.
		eSize += len(a.rm) * (4 + len(a.Name) + 256)
		a.mu.RUnlock()
		return true
	})
	s.mu.Unlock()

	sendSubs := func(accs []*Account) {
		var raw [32]*subscription

		route.mu.Lock()
		for _, a := range accs {
			subs := raw[:0]

			a.mu.RLock()
			c := a.randomClient()
			if c == nil {
				nsubs := len(a.rm)
				accName := a.Name
				a.mu.RUnlock()
				if nsubs > 0 {
					route.Warnf("Ignoring account %q with %d subs, no clients", accName, nsubs)
				}
				continue
			}
			for key, n := range a.rm {
				// FIXME(dlc) - Just pass rme around.
				// Construct a sub on the fly. We need to place
				// a client (or im) to properly set the account.
				var subj, qn []byte
				s := strings.Split(key, " ")
				subj = []byte(s[0])
				if len(s) > 1 {
					qn = []byte(s[1])
				}
				// TODO(dlc) - This code needs to change, but even if left alone could be more
				// efficient with these tmp subs.
				sub := &subscription{client: c, subject: subj, queue: qn, qw: n}
				subs = append(subs, sub)
			}
			a.mu.RUnlock()

			route.sendRouteSubProtos(subs, false, route.importFilter)
		}
		route.mu.Unlock()
		route.Debugf("Sent local subscriptions to route")
	}
	// Decide if we call above function in go routine or in place.
	if eSize > sendRouteSubsInGoRoutineThreshold {
		s.startGoRoutine(func() {
			sendSubs(accs)
			s.grWG.Done()
		})
	} else {
		sendSubs(accs)
	}
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

		// If we have an origin cluster and the other side supports leafnode origin clusters
		// send an LS+/LS- version instead.
		if len(sub.origin) > 0 && c.route.lnoc {
			if isSubProto {
				buf = append(buf, lSubBytes...)
			} else {
				buf = append(buf, lUnsubBytes...)
			}
			buf = append(buf, sub.origin...)
			buf = append(buf, ' ')
		} else {
			if isSubProto {
				buf = append(buf, rSubBytes...)
			} else {
				buf = append(buf, rUnsubBytes...)
			}
		}
		buf = append(buf, accName...)
		buf = append(buf, ' ')
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
		if trace {
			c.traceOutOp("", buf[as:])
		}
		buf = append(buf, CR_LF...)
	}

	c.enqueueProto(buf)
}

func (s *Server) createRoute(conn net.Conn, rURL *url.URL) *client {
	// Snapshot server options.
	opts := s.getOpts()

	didSolicit := rURL != nil
	r := &route{didSolicit: didSolicit}
	for _, route := range opts.Routes {
		if rURL != nil && (strings.EqualFold(rURL.Host, route.Host)) {
			r.routeType = Explicit
		}
	}

	c := &client{srv: s, nc: conn, opts: clientOpts{}, kind: ROUTER, msubs: -1, mpay: -1, route: r}

	// Grab server variables
	s.mu.Lock()
	// New proto wants a nonce (although not used in routes, that is, not signed in CONNECT)
	var raw [nonceLen]byte
	nonce := raw[:]
	s.generateNonce(nonce)
	s.routeInfo.Nonce = string(nonce)
	s.generateRouteInfoJSON()
	// Clear now that it has been serialized. Will prevent nonce to be included in async INFO that we may send.
	s.routeInfo.Nonce = _EMPTY_
	infoJSON := s.routeInfoJSON
	authRequired := s.routeInfo.AuthRequired
	tlsRequired := s.routeInfo.TLSRequired
	clusterName := s.info.Cluster
	s.mu.Unlock()

	// Grab lock
	c.mu.Lock()

	// Initialize
	c.initClient()

	if didSolicit {
		// Do this before the TLS code, otherwise, in case of failure
		// and if route is explicit, it would try to reconnect to 'nil'...
		r.url = rURL
	} else {
		c.flags.set(expectConnect)
	}

	// Check for TLS
	if tlsRequired {
		// Copy off the config to add in ServerName if we need to.
		tlsConfig := opts.Cluster.TLSConfig.Clone()

		// If we solicited, we will act like the client, otherwise the server.
		if didSolicit {
			c.Debugf("Starting TLS route client handshake")
			// Specify the ServerName we are expecting.
			host, _, _ := net.SplitHostPort(rURL.Host)
			tlsConfig.ServerName = host
			c.nc = tls.Client(c.nc, tlsConfig)
		} else {
			c.Debugf("Starting TLS route server handshake")
			c.nc = tls.Server(c.nc, tlsConfig)
		}

		conn := c.nc.(*tls.Conn)

		// Setup the timeout
		ttl := secondsToDuration(opts.Cluster.TLSTimeout)
		time.AfterFunc(ttl, func() { tlsTimeout(c, conn) })
		conn.SetReadDeadline(time.Now().Add(ttl))

		c.mu.Unlock()
		if err := conn.Handshake(); err != nil {
			c.Errorf("TLS route handshake error: %v", err)
			c.sendErr("Secure Connection - TLS Required")
			c.closeConnection(TLSHandshakeError)
			return nil
		}
		// Reset the read deadline
		conn.SetReadDeadline(time.Time{})

		// Re-Grab lock
		c.mu.Lock()

		// To be consistent with client, set this flag to indicate that handshake is done
		c.flags.set(handshakeComplete)

		// Verify that the connection did not go away while we released the lock.
		if c.isClosed() {
			c.mu.Unlock()
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

	// Set the Ping timer
	s.setFirstPingTimer(c)

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
		c.Debugf("TLS version %s, cipher suite %s", tlsVersion(cs.Version), tlsCipher(cs.CipherSuite))
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

	// Send our info to the other side.
	// Our new version requires dynamic information for accounts and a nonce.
	c.enqueueProto(infoJSON)
	c.mu.Unlock()

	c.Noticef("Route connection created")
	return c
}

const (
	_CRLF_  = "\r\n"
	_EMPTY_ = ""
)

func (s *Server) addRoute(c *client, info *Info) (bool, bool) {
	id := c.route.remoteID
	sendInfo := false

	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return false, false
	}
	remote, exists := s.remotes[id]
	if !exists {
		s.routes[c.cid] = c
		s.remotes[id] = c
		c.mu.Lock()
		c.route.connectURLs = info.ClientConnectURLs
		c.route.wsConnURLs = info.WSConnectURLs
		cid := c.cid
		hash := string(c.route.hash)
		c.mu.Unlock()

		// Store this route using the hash as the key
		s.routesByHash.Store(hash, c)

		// Now that we have registered the route, we can remove from the temp map.
		s.removeFromTempClients(cid)

		// we don't need to send if the only route is the one we just accepted.
		sendInfo = len(s.routes) > 1

		// If the INFO contains a Gateway URL, add it to the list for our cluster.
		if info.GatewayURL != "" && s.addGatewayURL(info.GatewayURL) {
			s.sendAsyncGatewayInfo()
		}

		// Add the remote's leafnodeURL to our list of URLs and send the update
		// to all LN connections. (Note that when coming from a route, LeafNodeURLs
		// is an array of size 1 max).
		if len(info.LeafNodeURLs) == 1 && s.addLeafNodeURL(info.LeafNodeURLs[0]) {
			s.sendAsyncLeafNodeInfo()
		}
	}
	s.mu.Unlock()

	if exists {
		var r *route

		c.mu.Lock()
		// upgrade to solicited?
		if c.route.didSolicit {
			// Make a copy
			rs := *c.route
			r = &rs
		}
		// Since this duplicate route is going to be removed, make sure we clear
		// c.route.leafnodeURL, otherwise, when processing the disconnect, this
		// would cause the leafnode URL for that remote server to be removed
		// from our list.
		c.route.leafnodeURL = _EMPTY_
		// Same for the route hash otherwise it would be removed from s.routesByHash.
		c.route.hash = _EMPTY_
		c.mu.Unlock()

		remote.mu.Lock()
		// r will be not nil if c.route.didSolicit was true
		if r != nil {
			// If we upgrade to solicited, we still want to keep the remote's
			// connectURLs. So transfer those.
			r.connectURLs = remote.route.connectURLs
			r.wsConnURLs = remote.route.wsConnURLs
			remote.route = r
		}
		// This is to mitigate the issue where both sides add the route
		// on the opposite connection, and therefore end-up with both
		// connections being dropped.
		remote.route.retry = true
		remote.mu.Unlock()
	}

	return !exists, sendInfo
}

// Import filter check.
func (c *client) importFilter(sub *subscription) bool {
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

	// Copy to hold outside acc lock.
	var n int32
	var ok bool

	isq := len(sub.queue) > 0

	accLock := func() {
		// Not required for code correctness, but helps reduce the number of
		// updates sent to the routes when processing high number of concurrent
		// queue subscriptions updates (sub/unsub).
		// See https://github.com/nats-io/nats-server/pull/1126 for more details.
		if isq {
			acc.sqmu.Lock()
		}
		acc.mu.Lock()
	}
	accUnlock := func() {
		acc.mu.Unlock()
		if isq {
			acc.sqmu.Unlock()
		}
	}

	accLock()

	// This is non-nil when we know we are in cluster mode.
	rm, lqws := acc.rm, acc.lqws
	if rm == nil {
		accUnlock()
		return
	}

	// Create the fast key which will use the subject or 'subject<spc>queue' for queue subscribers.
	key := keyFromSub(sub)

	// Decide whether we need to send an update out to all the routes.
	update := isq

	// This is where we do update to account. For queues we need to take
	// special care that this order of updates is same as what is sent out
	// over routes.
	if n, ok = rm[key]; ok {
		n += delta
		if n <= 0 {
			delete(rm, key)
			if isq {
				delete(lqws, key)
			}
			update = true // Update for deleting (N->0)
		} else {
			rm[key] = n
		}
	} else if delta > 0 {
		n = delta
		rm[key] = delta
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
		nsub.qw = n
		sub = &nsub
	}

	// We need to send out this update. Gather routes
	var _routes [32]*client
	routes := _routes[:0]

	s.mu.Lock()
	for _, route := range s.routes {
		routes = append(routes, route)
	}
	trace := atomic.LoadInt32(&s.logging.trace) == 1
	s.mu.Unlock()

	// If we are a queue subscriber we need to make sure our updates are serialized from
	// potential multiple connections. We want to make sure that the order above is preserved
	// here but not necessarily all updates need to be sent. We need to block and recheck the
	// n count with the lock held through sending here. We will suppress duplicate sends of same qw.
	if isq {
		// However, we can't hold the acc.mu lock since we allow client.mu.Lock -> acc.mu.Lock
		// but not the opposite. So use a dedicated lock while holding the route's lock.
		acc.sqmu.Lock()
		defer acc.sqmu.Unlock()

		acc.mu.Lock()
		n = rm[key]
		sub.qw = n
		// Check the last sent weight here. If same, then someone
		// beat us to it and we can just return here. Otherwise update
		if ls, ok := lqws[key]; ok && ls == n {
			acc.mu.Unlock()
			return
		} else {
			lqws[key] = n
		}
		acc.mu.Unlock()
	}

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
	if s.shutdown {
		s.mu.Unlock()
		return
	}
	s.Noticef("Cluster name is %s", clusterName)
	if s.isClusterNameDynamic() {
		s.Warnf("Cluster name was dynamically generated, consider setting one")
	}

	hp := net.JoinHostPort(opts.Cluster.Host, strconv.Itoa(port))
	l, e := natsListen("tcp", hp)
	if e != nil {
		s.mu.Unlock()
		s.Fatalf("Error listening on router port: %d - %v", opts.Cluster.Port, e)
		return
	}
	s.Noticef("Listening for route connections on %s",
		net.JoinHostPort(opts.Cluster.Host, strconv.Itoa(l.Addr().(*net.TCPAddr).Port)))

	proto := RouteProtoV2
	// For tests, we want to be able to make this server behave
	// as an older server so check this option to see if we should override
	if opts.routeProto < 0 {
		// We have a private option that allows test to override the route
		// protocol. We want this option initial value to be 0, however,
		// since original proto is RouteProtoZero, tests call setRouteProtoForTest(),
		// which sets as negative value the (desired proto + 1) * -1.
		// Here we compute back the real value.
		proto = (opts.routeProto * -1) - 1
	}
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
		Proto:        proto,
		GatewayURL:   s.getGatewayURL(),
		Headers:      s.supportsHeaders(),
		Cluster:      s.info.Cluster,
		Dynamic:      s.isClusterNameDynamic(),
		LNOC:         true,
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
		s.Fatalf("Error setting route INFO with Cluster.Advertise value of %s, err=%v", s.opts.Cluster.Advertise, err)
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

	// Start the accept loop in a different go routine.
	go s.acceptConnections(l, "Route", func(conn net.Conn) { s.createRoute(conn, nil) }, nil)

	// Solicit Routes if applicable. This will not block.
	s.solicitRoutes(opts.Routes)

	s.mu.Unlock()
}

// Similar to setInfoHostPortAndGenerateJSON, but for routeInfo.
func (s *Server) setRouteInfoHostPortAndIP() error {
	if s.opts.Cluster.Advertise != "" {
		advHost, advPort, err := parseHostPort(s.opts.Cluster.Advertise, s.opts.Cluster.Port)
		if err != nil {
			return err
		}
		s.routeInfo.Host = advHost
		s.routeInfo.Port = advPort
		s.routeInfo.IP = fmt.Sprintf("nats-route://%s/", net.JoinHostPort(advHost, strconv.Itoa(advPort)))
	} else {
		s.routeInfo.Host = s.opts.Cluster.Host
		s.routeInfo.Port = s.opts.Cluster.Port
		s.routeInfo.IP = ""
	}
	// (re)generate the routeInfoJSON byte array
	s.generateRouteInfoJSON()
	return nil
}

// StartRouting will start the accept loop on the cluster host:port
// and will actively try to connect to listed routes.
func (s *Server) StartRouting(clientListenReady chan struct{}) {
	defer s.grWG.Done()

	// Wait for the client and and leafnode listen ports to be opened,
	// and the possible ephemeral ports to be selected.
	<-clientListenReady

	// Start the accept loop and solicitation of explicit routes (if applicable)
	s.startRouteAcceptLoop()

}

func (s *Server) reConnectToRoute(rURL *url.URL, rtype RouteType) {
	tryForEver := rtype == Explicit
	// If A connects to B, and B to A (regardless if explicit or
	// implicit - due to auto-discovery), and if each server first
	// registers the route on the opposite TCP connection, the
	// two connections will end-up being closed.
	// Add some random delay to reduce risk of repeated failures.
	delay := time.Duration(rand.Intn(100)) * time.Millisecond
	if tryForEver {
		delay += DEFAULT_ROUTE_RECONNECT
	}
	select {
	case <-time.After(delay):
	case <-s.quitCh:
		s.grWG.Done()
		return
	}
	s.connectToRoute(rURL, tryForEver, false)
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

func (s *Server) connectToRoute(rURL *url.URL, tryForEver, firstConnect bool) {
	// Snapshot server options.
	opts := s.getOpts()

	defer s.grWG.Done()

	const connErrFmt = "Error trying to connect to route (attempt %v): %v"

	attempts := 0
	for s.isRunning() && rURL != nil {
		if tryForEver && !s.routeStillValid(rURL) {
			return
		}
		s.Debugf("Trying to connect to route on %s", rURL.Host)
		conn, err := natsDialTimeout("tcp", rURL.Host, DEFAULT_ROUTE_DIAL)
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
			select {
			case <-s.quitCh:
				return
			case <-time.After(routeConnectDelay):
				continue
			}
		}

		if tryForEver && !s.routeStillValid(rURL) {
			conn.Close()
			return
		}

		// We have a route connection here.
		// Go ahead and create it and exit this func.
		s.createRoute(conn, rURL)
		return
	}
}

func (c *client) isSolicitedRoute() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.kind == ROUTER && c.route != nil && c.route.didSolicit
}

func (s *Server) solicitRoutes(routes []*url.URL) {
	for _, r := range routes {
		route := r
		s.startGoRoutine(func() { s.connectToRoute(route, true, true) })
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
	var perms *RoutePermissions
	if srv != nil {
		perms = srv.getOpts().Cluster.Permissions
	}

	clusterName := srv.ClusterName()
	isSusceptible := srv.isClusterNameSusceptibleToChanges()

	// If we have a cluster name set, make sure it matches ours.
	if proto.Cluster != clusterName {
		// If we have a dynamic name we will do additional checks.
		isDynamic := srv.isClusterNameDynamic()
		remoteIsDynamic := proto.Dynamic

		if isDynamic && !remoteIsDynamic {
			// c.Noticef("Updating the explicit name from the CONNECT message: ", proto.Cluster)
			// Use the explicit name and mark current node as not susceptible
			// to dynamic cluster name changes.
			srv.setClusterName(proto.Cluster, false)
			srv.removeAllRoutesExcept(c)
		} else if !isDynamic && remoteIsDynamic {
			// Do nothing since the remote may use this node's cluster name
			// when it is sent an INFO message. Also override the cluster name
			// from this route with ours since should win.
			c.mu.Lock()
			c.route.clusterName = srv.info.Cluster
			c.mu.Unlock()
		} else if isDynamic && remoteIsDynamic {
			// If we haven't learned an explicit name, we will take on their name if theirs is higher then ours.
			if isSusceptible && strings.Compare(clusterName, proto.Cluster) < 0 {
				srv.setClusterName(proto.Cluster, isSusceptible)
				srv.removeAllRoutesExcept(c)
			}
		} else {
			// We have an explicit cluster name and reject explicit cluster name changes.
			errTxt := fmt.Sprintf("Rejecting connection, cluster name %q does not match %q", proto.Cluster, srv.info.Cluster)
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
	c.route.lnoc = proto.LNOC
	c.setRoutePermissions(perms)
	c.headers = supportsHeaders && proto.Headers

	// Remember their cluster name.
	c.route.clusterName = proto.Cluster
	c.route.clusterDynamic = proto.Dynamic
	c.mu.Unlock()
	return nil
}

// Called when we update our cluster name during negotiations with remotes.
func (s *Server) removeAllRoutesExcept(c *client) {
	s.mu.Lock()
	routes := make([]*client, 0, len(s.routes))
	for _, r := range s.routes {
		if r != c {
			routes = append(routes, r)
		}
	}
	s.mu.Unlock()

	for _, r := range routes {
		r.closeConnection(ClusterNameConflict)
	}
}

// Called when we update our cluster name during negotiations with remotes.
func (s *Server) nonDynamicClusterNameRoutePresent(remoteID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.routes {
		// Check all routes but this one.
		if r.route.remoteID == remoteID {
			continue
		}
		if r.route.clusterName != "" && !r.route.clusterDynamic {
			return true
		}
	}

	return false
}

func (s *Server) removeRoute(c *client) {
	var rID string
	var lnURL string
	var gwURL string
	var hash string
	c.mu.Lock()
	cid := c.cid
	r := c.route
	if r != nil {
		rID = r.remoteID
		lnURL = r.leafnodeURL
		hash = r.hash
		gwURL = r.gatewayURL
	}
	c.mu.Unlock()
	s.mu.Lock()

	// Check whether we do not have any other routes that are not using explicit names,
	// since in that case this node should go back to using a dynamic cluster name
	// that will be negotiated among the remaining members of the cluster.
	var revertClusterName bool
	if s.isClusterNameDynamic() && !s.susceptible {
		var nonDynamicRouteFound bool
		for _, r := range s.routes {
			if r.route.clusterName != "" && !r.route.clusterDynamic {
				nonDynamicRouteFound = true
			}
		}

		// Check whether there are no more routes, if so go back to use the original dynamic cluster name.
		if len(s.routes) <= 1 && nonDynamicRouteFound {
			revertClusterName = true
		}
	}

	delete(s.routes, cid)
	if r != nil {
		rc, ok := s.remotes[rID]
		// Only delete it if it is us..
		if ok && c == rc {
			delete(s.remotes, rID)
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
		s.routesByHash.Delete(hash)
	}
	s.removeFromTempClients(cid)
	s.mu.Unlock()

	// When the last route to this cluster is disconnected
	// and we were in dynamic mode, then we will go back
	// to using the original dynamic name that we had.
	if revertClusterName {
		s.mu.Lock()
		name := s.defaultClusterName
		s.mu.Unlock()
		s.setClusterName(name, true)
	}
}
