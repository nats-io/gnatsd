// Copyright 2012-2026 The NATS Authors
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
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/jwt/v2"
)

// Type of client connection.
const (
	// CLIENT is an end user.
	CLIENT = iota
	// ROUTER represents another server in the cluster.
	ROUTER
	// GATEWAY is a link between 2 clusters.
	GATEWAY
	// SYSTEM is an internal system client.
	SYSTEM
	// LEAF is for leaf node connections.
	LEAF
	// JETSTREAM is an internal jetstream client.
	JETSTREAM
	// ACCOUNT is for the internal client for accounts.
	ACCOUNT
)

// Internal clients. kind should be SYSTEM, JETSTREAM or ACCOUNT
func isInternalClient(kind int) bool {
	return kind == SYSTEM || kind == JETSTREAM || kind == ACCOUNT
}

// Extended type of a CLIENT connection. This is returned by c.clientType()
// and indicate what type of client connection we are dealing with.
// If invoked on a non CLIENT connection, NON_CLIENT type is returned.
const (
	// If the connection is not a CLIENT connection.
	NON_CLIENT = iota
	// Regular NATS client.
	NATS
	// MQTT client.
	MQTT
	// Websocket client.
	WS
)

const (
	// ClientProtoZero is the original Client protocol from 2009.
	// http://nats.io/documentation/internals/nats-protocol/
	ClientProtoZero = iota
	// ClientProtoInfo signals a client can receive more then the original INFO block.
	// This can be used to update clients on other cluster members, etc.
	ClientProtoInfo
)

const (
	pingProto = "PING" + _CRLF_
	pongProto = "PONG" + _CRLF_
	errProto  = "-ERR '%s'" + _CRLF_
	okProto   = "+OK" + _CRLF_
)

// TLS Hanshake client types
const (
	tlsHandshakeLeaf = "leafnode"
	tlsHandshakeMQTT = "mqtt"
)

const (
	// Scratch buffer size for the processMsg() calls.
	msgScratchSize  = 1024
	msgHeadProto    = "RMSG "
	msgHeadProtoLen = len(msgHeadProto)

	// For controlling dynamic buffer sizes.
	startBufSize    = 512   // For INFO/CONNECT block
	minBufSize      = 64    // Smallest to shrink to for PING/PONG
	maxBufSize      = 65536 // 64k
	shortsToShrink  = 2     // Trigger to shrink dynamic buffers
	maxFlushPending = 10    // Max fsps to have in order to wait for writeLoop
	readLoopReport  = 2 * time.Second

	// Server should not send a PING (for RTT) before the first PONG has
	// been sent to the client. However, in case some client libs don't
	// send CONNECT+PING, cap the maximum time before server can send
	// the RTT PING.
	maxNoRTTPingBeforeFirstPong = 2 * time.Second

	// For stalling fast producers
	stallClientMinDuration = 2 * time.Millisecond
	stallClientMaxDuration = 5 * time.Millisecond
	stallTotalAllowed      = 10 * time.Millisecond
)

var readLoopReportThreshold = readLoopReport

// Represent client booleans with a bitmask
type clientFlag uint16

const (
	hdrLine      = "NATS/1.0\r\n"
	emptyHdrLine = "NATS/1.0\r\n\r\n"
)

// Some client state represented as flags
const (
	connectReceived        clientFlag = 1 << iota // The CONNECT proto has been received
	infoReceived                                  // The INFO protocol has been received
	firstPongSent                                 // The first PONG has been sent
	handshakeComplete                             // For TLS clients, indicate that the handshake is complete
	flushOutbound                                 // Marks client as having a flushOutbound call in progress.
	noReconnect                                   // Indicate that on close, this connection should not attempt a reconnect
	closeConnection                               // Marks that closeConnection has already been called.
	connMarkedClosed                              // Marks that markConnAsClosed has already been called.
	writeLoopStarted                              // Marks that the writeLoop has been started.
	skipFlushOnClose                              // Marks that flushOutbound() should not be called on connection close.
	expectConnect                                 // Marks if this connection is expected to send a CONNECT
	connectProcessFinished                        // Marks if this connection has finished the connect process.
	compressionNegotiated                         // Marks if this connection has negotiated compression level with remote.
	didTLSFirst                                   // Marks if this connection requested and was accepted doing the TLS handshake first (prior to INFO).
	isSlowConsumer                                // Marks connection as a slow consumer.
	authCallout                                   // Marks connection as authorized by the auth callout.
)

// set the flag (would be equivalent to set the boolean to true)
func (cf *clientFlag) set(c clientFlag) {
	*cf |= c
}

// clear the flag (would be equivalent to set the boolean to false)
func (cf *clientFlag) clear(c clientFlag) {
	*cf &= ^c
}

// isSet returns true if the flag is set, false otherwise
func (cf clientFlag) isSet(c clientFlag) bool {
	return cf&c != 0
}

// setIfNotSet will set the flag `c` only if that flag was not already
// set and return true to indicate that the flag has been set. Returns
// false otherwise.
func (cf *clientFlag) setIfNotSet(c clientFlag) bool {
	if *cf&c == 0 {
		*cf |= c
		return true
	}
	return false
}

// ClosedState is the reason client was closed. This will
// be passed into calls to clearConnection, but will only
// be stored in ConnInfo for monitoring.
type ClosedState int

const (
	ClientClosed = ClosedState(iota + 1)
	AuthenticationTimeout
	AuthenticationViolation
	TLSHandshakeError
	SlowConsumerPendingBytes
	SlowConsumerWriteDeadline
	WriteError
	ReadError
	ParseError
	StaleConnection
	ProtocolViolation
	BadClientProtocolVersion
	WrongPort
	MaxAccountConnectionsExceeded
	MaxConnectionsExceeded
	MaxPayloadExceeded
	MaxControlLineExceeded
	MaxSubscriptionsExceeded
	DuplicateRoute
	RouteRemoved
	ServerShutdown
	AuthenticationExpired
	WrongGateway
	MissingAccount
	Revocation
	InternalClient
	MsgHeaderViolation
	NoRespondersRequiresHeaders
	ClusterNameConflict
	DuplicateRemoteLeafnodeConnection
	DuplicateClientID
	DuplicateServerName
	MinimumVersionRequired
	ClusterNamesIdentical
	Kicked
	ProxyNotTrusted
	ProxyRequired
)

// Some flags passed to processMsgResults
const pmrNoFlag int = 0

const (
	pmrCollectQueueNames int = 1 << iota
	pmrIgnoreEmptyQueueFilter
	pmrAllowSendFromRouteToRoute
	pmrMsgImportedFromService
)

type WriteTimeoutPolicy uint8

const (
	WriteTimeoutPolicyDefault = iota
	WriteTimeoutPolicyClose
	WriteTimeoutPolicyRetry
)

// String returns a human-friendly value. Only used in varz.
func (p WriteTimeoutPolicy) String() string {
	switch p {
	case WriteTimeoutPolicyClose:
		return "close"
	case WriteTimeoutPolicyRetry:
		return "retry"
	default:
		return _EMPTY_
	}
}

type client struct {
	// Here first because of use of atomics, and memory alignment.
	stats
	gwReplyMapping
	kind  int
	srv   *Server
	acc   *Account
	perms *permissions
	in    readCache
	parseState
	opts       ClientOpts
	rrTracking *rrTracking
	mpay       int32
	msubs      int32
	mcl        int32
	mu         sync.RWMutex
	cid        uint64
	start      time.Time
	nonce      []byte
	pubKey     string
	nc         net.Conn
	ncs        atomic.Value
	ncsAcc     atomic.Value
	ncsUser    atomic.Value
	out        outbound
	user       *NkeyUser
	host       string
	port       uint16
	subs       map[string]*subscription
	replies    map[string]*resp
	mperms     *msgDeny
	darray     []*subscription // Parsed subscribe deny entries used to build the delivery-time filter.
	pcd        map[*client]struct{}
	atmr       *time.Timer
	expires    time.Time
	ping       pinfo
	msgb       [msgScratchSize]byte
	last       time.Time
	lastIn     time.Time
	proxyKey   string

	repliesSincePrune uint16
	lastReplyPrune    time.Time

	headers bool

	rtt      time.Duration
	rttStart time.Time

	route *route
	gw    *gateway
	leaf  *leaf
	ws    *websocket
	mqtt  *mqtt

	flags clientFlag // Compact booleans into a single field. Size will be increased when needed.

	rref byte

	trace bool
	echo  bool
	noIcb bool
	iproc bool // In-Process connection, set at creation and immutable.

	tags    jwt.TagList
	nameTag string

	tlsTo *time.Timer

	// Authentication error override. This is used because the authentication
	// stack is simply returning a boolean, and the only authentication error
	// reported is the generic `ErrAuthentication`. In the authentication code,
	// if we want to report a different error, we can now set this field
	// and `authViolation()` will use that one.
	authErr error
}

type rrTracking struct {
	rmap map[string]*remoteLatency
	ptmr *time.Timer
	lrt  time.Duration
}

// Struct for PING initiation from the server.
type pinfo struct {
	tmr *time.Timer
	out int
}

// outbound holds pending data for a socket.
type outbound struct {
	nb  net.Buffers        // Pending buffers for send, each has fixed capacity as per nbPool below.
	wnb net.Buffers        // Working copy of "nb", reused on each flushOutbound call, partial writes may leave entries here for next iteration.
	pb  int64              // Total pending/queued bytes.
	fsp int32              // Flush signals that are pending per producer from readLoop's pcd.
	wtp WriteTimeoutPolicy // What do we do on a write timeout?
	sg  *sync.Cond         // To signal writeLoop that there is data to flush.
	wdl time.Duration      // Snapshot of write deadline.
	mp  int64              // Snapshot of max pending for client.
	lft time.Duration      // Last flush time for Write.
	stc chan struct{}      // Stall chan we create to slow down producers on overrun, e.g. fan-in.
	cw  *s2.Writer
}

const nbMaxVectorSize = 1024 // == IOV_MAX on Linux/Darwin and most other Unices (except Solaris/AIX)

const (
	nbPoolSizeSmall  = 512   // Underlying array size of small buffer
	nbPoolSizeMedium = 4096  // Underlying array size of medium buffer
	nbPoolSizeLarge  = 65536 // Underlying array size of large buffer
)

var nbPoolSmall = &sync.Pool{
	New: func() any {
		b := [nbPoolSizeSmall]byte{}
		return &b
	},
}

var nbPoolMedium = &sync.Pool{
	New: func() any {
		b := [nbPoolSizeMedium]byte{}
		return &b
	},
}

var nbPoolLarge = &sync.Pool{
	New: func() any {
		b := [nbPoolSizeLarge]byte{}
		return &b
	},
}

// nbPoolGet returns a frame that is a best-effort match for the given size.
// Once a pooled frame is no longer needed, it should be recycled by passing
// it to nbPoolPut.
func nbPoolGet(sz int) []byte {
	switch {
	case sz <= nbPoolSizeSmall:
		return nbPoolSmall.Get().(*[nbPoolSizeSmall]byte)[:0]
	case sz <= nbPoolSizeMedium:
		return nbPoolMedium.Get().(*[nbPoolSizeMedium]byte)[:0]
	default:
		return nbPoolLarge.Get().(*[nbPoolSizeLarge]byte)[:0]
	}
}

// nbPoolPut recycles a frame that was retrieved from nbPoolGet. It is not
// safe to return multiple slices referring to chunks of the same underlying
// array as this may create overlaps when the buffers are returned to their
// original size, resulting in race conditions.
func nbPoolPut(b []byte) {
	switch cap(b) {
	case nbPoolSizeSmall:
		b := (*[nbPoolSizeSmall]byte)(b[0:nbPoolSizeSmall])
		nbPoolSmall.Put(b)
	case nbPoolSizeMedium:
		b := (*[nbPoolSizeMedium]byte)(b[0:nbPoolSizeMedium])
		nbPoolMedium.Put(b)
	case nbPoolSizeLarge:
		b := (*[nbPoolSizeLarge]byte)(b[0:nbPoolSizeLarge])
		nbPoolLarge.Put(b)
	default:
		// Ignore frames that are the wrong size, this might happen
		// with WebSocket/MQTT messages as they are framed
	}
}

type perm struct {
	allow *Sublist
	deny  *Sublist
}

type permissions struct {
	// Have these 2 first for memory alignment due to the use of atomic.
	pcsz   int32
	prun   int32
	sub    perm
	pub    perm
	resp   *ResponsePermission
	pcache sync.Map
}

// This is used to dynamically track responses and reply subjects
// for dynamic permissioning.
type resp struct {
	t time.Time
	n int
}

// msgDeny is used when a user permission for subscriptions has a deny
// clause but a subscription could be made that is of broader scope.
// e.g. deny = "foo", but user subscribes to "*". That subscription should
// succeed but no message sent on foo should be delivered.
type msgDeny struct {
	deny   *Sublist
	dcache map[msgDenyKey]bool
}

type msgDenyKey struct {
	subject string
	queue   string
}

// routeTarget collects information regarding routes and queue groups for
// sending information to a remote.
type routeTarget struct {
	sub    *subscription
	qs     []byte
	qsubs  []*subscription
	_qs    [32]byte
	_qsubs [4]*subscription
}

const (
	maxResultCacheSize   = 512
	maxDenyPermCacheSize = 256
	maxPermCacheSize     = 128
	pruneSize            = 32
	routeTargetInit      = 8
	replyPermLimit       = 4096
	replyPruneTime       = time.Second
)

// Represent read cache booleans with a bitmask
type readCacheFlag uint16

const (
	hasMappings         readCacheFlag = 1 << iota // For account subject mappings.
	switchToCompression readCacheFlag = 1 << 1
)

const sysGroup = "_sys_"

// Used in readloop to cache hot subject lookups and group statistics.
type readCache struct {
	// These are for clients who are bound to a single account.
	genid   uint64
	results map[string]*SublistResult

	// This is for routes and gateways to have their own L1 as well that is account aware.
	pacache map[string]*perAccountCache

	// This is for when we deliver messages across a route. We use this structure
	// to make sure to only send one message and properly scope to queues as needed.
	rts []routeTarget

	// These are all temporary totals for an invocation of a read in readloop.
	msgs  int32
	bytes int32
	subs  int32

	rsz int32 // Read buffer size
	srs int32 // Short reads, used for dynamic buffer resizing.

	// These are for readcache flags to avoid locks.
	flags readCacheFlag

	// Capture the time we started processing our readLoop.
	start time.Time

	// Total time stalled so far for readLoop processing.
	tst time.Duration
}

// set the flag (would be equivalent to set the boolean to true)
func (rcf *readCacheFlag) set(c readCacheFlag) {
	*rcf |= c
}

// clear the flag (would be equivalent to set the boolean to false)
func (rcf *readCacheFlag) clear(c readCacheFlag) {
	*rcf &= ^c
}

// isSet returns true if the flag is set, false otherwise
func (rcf readCacheFlag) isSet(c readCacheFlag) bool {
	return rcf&c != 0
}

const (
	defaultMaxPerAccountCacheSize  = 8192
	defaultClosedSubsCheckInterval = 5 * time.Minute
)

var (
	maxPerAccountCacheSize  = defaultMaxPerAccountCacheSize
	closedSubsCheckInterval = defaultClosedSubsCheckInterval
)

// perAccountCache is for L1 semantics for inbound messages from a route or gateway to mimic the performance of clients.
type perAccountCache struct {
	acc     *Account
	results *SublistResult
	genid   uint64
}

func (c *client) String() (id string) {
	loaded := c.ncs.Load()
	if loaded != nil {
		return loaded.(string)
	}

	return _EMPTY_
}

// GetNonce returns the nonce that was presented to the user on connection
func (c *client) GetNonce() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.nonce
}

// GetID returns the client ID
func (c *client) GetID() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cid
}

// GetName returns the application supplied name for the connection.
func (c *client) GetName() string {
	c.mu.Lock()
	name := c.opts.Name
	c.mu.Unlock()
	return name
}

// GetOpts returns the client options provided by the application.
func (c *client) GetOpts() *ClientOpts {
	return &c.opts
}

// GetTLSConnectionState returns the TLS ConnectionState if TLS is enabled, nil
// otherwise. Implements the ClientAuth interface.
func (c *client) GetTLSConnectionState() *tls.ConnectionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nc == nil {
		return nil
	}
	tc, ok := c.nc.(*tls.Conn)
	if !ok {
		return nil
	}
	state := tc.ConnectionState()
	return &state
}

// For CLIENT connections, this function returns the client type, that is,
// NATS (for regular clients), MQTT or WS for websocket.
// If this is invoked for a non CLIENT connection, NON_CLIENT is returned.
//
// This function does not lock the client and accesses fields that are supposed
// to be immutable and therefore it can be invoked outside of the client's lock.
func (c *client) clientType() int {
	switch c.kind {
	case CLIENT:
		if c.isMqtt() {
			return MQTT
		} else if c.isWebsocket() {
			return WS
		}
		return NATS
	default:
		return NON_CLIENT
	}
}

var clientTypeStringMap = map[int]string{
	NON_CLIENT: _EMPTY_,
	NATS:       "nats",
	WS:         "websocket",
	MQTT:       "mqtt",
}

func (c *client) clientTypeString() string {
	if typeStringVal, ok := clientTypeStringMap[c.clientType()]; ok {
		return typeStringVal
	}
	return _EMPTY_
}

// This is the main subscription struct that indicates
// interest in published messages.
// FIXME(dlc) - This is getting bloated for normal subs, need
// to optionally have an opts section for non-normal stuff.
type subscription struct {
	client  *client
	im      *streamImport // This is for import stream support.
	rsi     bool
	si      bool
	leaf    bool            // Subscription interest originated from a leaf node.
	shadow  []*subscription // This is to track shadowed accounts.
	icb     msgHandler
	subject []byte
	queue   []byte
	sid     []byte
	origin  []byte
	nm      int64
	max     int64
	qw      int32
	closed  int32
	mqtt    *mqttSub
}

// Indicate that this subscription is closed.
// This is used in pruning of route and gateway cache items.
func (s *subscription) close() {
	atomic.StoreInt32(&s.closed, 1)
}

// Return true if this subscription was unsubscribed
// or its connection has been closed.
func (s *subscription) isClosed() bool {
	return atomic.LoadInt32(&s.closed) == 1
}

type ClientOpts struct {
	Echo         bool   `json:"echo"`
	Verbose      bool   `json:"verbose"`
	Pedantic     bool   `json:"pedantic"`
	TLSRequired  bool   `json:"tls_required"`
	Nkey         string `json:"nkey,omitempty"`
	JWT          string `json:"jwt,omitempty"`
	Sig          string `json:"sig,omitempty"`
	Token        string `json:"auth_token,omitempty"`
	Username     string `json:"user,omitempty"`
	Password     string `json:"pass,omitempty"`
	Name         string `json:"name"`
	Lang         string `json:"lang"`
	Version      string `json:"version"`
	Protocol     int    `json:"protocol"`
	Account      string `json:"account,omitempty"`
	AccountNew   bool   `json:"new_account,omitempty"`
	Headers      bool   `json:"headers,omitempty"`
	NoResponders bool   `json:"no_responders,omitempty"`

	// Routes and Leafnodes only
	Import *SubjectPermission `json:"import,omitempty"`
	Export *SubjectPermission `json:"export,omitempty"`

	// Leafnodes
	RemoteAccount string `json:"remote_account,omitempty"`

	// Proxy would include its own nonce signature.
	ProxySig string `json:"proxy_sig,omitempty"`
}

var (
	defaultOpts  = ClientOpts{Verbose: true, Pedantic: true, Echo: true}
	internalOpts = ClientOpts{Verbose: false, Pedantic: false, Echo: false}
)

func (c *client) setTraceLevel() {
	if c.kind == SYSTEM && !(atomic.LoadInt32(&c.srv.logging.traceSysAcc) != 0) {
		c.trace = false
	} else {
		c.trace = (atomic.LoadInt32(&c.srv.logging.trace) != 0)
	}
}

// Lock should be held
func (c *client) initClient() {
	s := c.srv
	c.cid = atomic.AddUint64(&s.gcid, 1)

	// Outbound data structure setup
	c.out.sg = sync.NewCond(&c.mu)
	opts := s.getOpts()
	// Snapshots to avoid mutex access in fast paths.
	c.out.wdl = opts.WriteDeadline
	switch {
	case c.kind == ROUTER && opts.Cluster.WriteDeadline > 0:
		c.out.wdl = opts.Cluster.WriteDeadline
	case c.kind == GATEWAY && opts.Gateway.WriteDeadline > 0:
		c.out.wdl = opts.Gateway.WriteDeadline
	case c.kind == LEAF && opts.LeafNode.WriteDeadline > 0:
		c.out.wdl = opts.LeafNode.WriteDeadline
	}
	switch c.kind {
	case ROUTER:
		if c.out.wtp = opts.Cluster.WriteTimeout; c.out.wtp == WriteTimeoutPolicyDefault {
			c.out.wtp = WriteTimeoutPolicyRetry
		}
	case LEAF:
		if c.out.wtp = opts.LeafNode.WriteTimeout; c.out.wtp == WriteTimeoutPolicyDefault {
			c.out.wtp = WriteTimeoutPolicyRetry
		}
	case GATEWAY:
		if c.out.wtp = opts.Gateway.WriteTimeout; c.out.wtp == WriteTimeoutPolicyDefault {
			c.out.wtp = WriteTimeoutPolicyRetry
		}
	default:
		if c.out.wtp = opts.WriteTimeout; c.out.wtp == WriteTimeoutPolicyDefault {
			c.out.wtp = WriteTimeoutPolicyClose
		}
	}
	c.out.mp = opts.MaxPending
	// Snapshot max control line since currently can not be changed on reload and we
	// were checking it on each call to parse. If this changes and we allow MaxControlLine
	// to be reloaded without restart, this code will need to change.
	c.mcl = int32(opts.MaxControlLine)
	if c.mcl == 0 {
		c.mcl = MAX_CONTROL_LINE_SIZE
	}

	c.subs = make(map[string]*subscription)
	c.echo = true

	c.setTraceLevel()

	// This is a scratch buffer used for processMsg()
	// The msg header starts with "RMSG ", which can be used
	// for both local and routes.
	// in bytes that is [82 77 83 71 32].
	c.msgb = [msgScratchSize]byte{82, 77, 83, 71, 32}

	// This is to track pending clients that have data to be flushed
	// after we process inbound msgs from our own connection.
	c.pcd = make(map[*client]struct{})

	// snapshot the string version of the connection
	var conn string
	if c.nc != nil {
		if addr := c.nc.RemoteAddr(); addr != nil {
			if conn = addr.String(); conn != _EMPTY_ {
				host, port, _ := net.SplitHostPort(conn)
				iPort, _ := strconv.ParseUint(port, 10, 16)
				c.host, c.port = host, uint16(iPort)
				if c.isWebsocket() && c.ws.clientIP != _EMPTY_ {
					cip := c.ws.clientIP
					// Surround IPv6 addresses with square brackets, as
					// net.JoinHostPort would do...
					if strings.Contains(cip, ":") {
						cip = "[" + cip + "]"
					}
					conn = fmt.Sprintf("%s/%s", cip, conn)
				}
				// Now that we have extracted host and port, escape
				// the string because it is going to be used in Sprintf
				conn = strings.ReplaceAll(conn, "%", "%%")
			}
		}
	}

	switch c.kind {
	case CLIENT:
		switch c.clientType() {
		case NATS:
			c.ncs.Store(fmt.Sprintf("%s - cid:%d", conn, c.cid))
		case WS:
			c.ncs.Store(fmt.Sprintf("%s - wid:%d", conn, c.cid))
		case MQTT:
			var ws string
			if c.isWebsocket() {
				ws = "_ws"
			}
			c.ncs.Store(fmt.Sprintf("%s - mid%s:%d", conn, ws, c.cid))
		}
	case ROUTER:
		c.ncs.Store(fmt.Sprintf("%s - rid:%d", conn, c.cid))
	case GATEWAY:
		c.ncs.Store(fmt.Sprintf("%s - gid:%d", conn, c.cid))
	case LEAF:
		var ws string
		if c.isWebsocket() {
			ws = "_ws"
		}
		c.ncs.Store(fmt.Sprintf("%s - lid%s:%d", conn, ws, c.cid))
	case SYSTEM:
		c.ncs.Store("SYSTEM")
	case JETSTREAM:
		c.ncs.Store("JETSTREAM")
	case ACCOUNT:
		c.ncs.Store("ACCOUNT")
	}
}

// RemoteAddress expose the Address of the client connection,
// nil when not connected or unknown
func (c *client) RemoteAddress() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nc == nil {
		return nil
	}

	return c.nc.RemoteAddr()
}

// Helper function to report errors.
func (c *client) reportErrRegisterAccount(acc *Account, err error) {
	if err == ErrTooManyAccountConnections {
		// Record the reason so that processConnect() can tell a real account
		// connection limit apart from an authentication failure, instead of
		// having to guess from the account the client is bound to.
		c.setAuthError(err)
		c.maxAccountConnExceeded()
		return
	}
	c.Errorf("Problem registering with account %q: %s", acc.Name, err)
	c.sendErr("Failed Account Registration")
}

// Kind returns the client kind and will be one of the defined constants like CLIENT, ROUTER, GATEWAY, LEAF
func (c *client) Kind() int {
	c.mu.Lock()
	kind := c.kind
	c.mu.Unlock()

	return kind
}

// registerWithAccount will register the given user with a specific
// account. This will change the subject namespace.
func (c *client) registerWithAccount(acc *Account) error {
	if acc == nil {
		return ErrBadAccount
	}
	acc.mu.RLock()
	bad := acc.sl == nil
	acc.mu.RUnlock()
	if bad {
		return ErrBadAccount
	}
	// If we were previously registered, usually to $G, do accounting here to remove.
	c.mu.Lock()
	prevAcc := c.acc
	c.mu.Unlock()
	if prevAcc != nil {
		if prev := prevAcc.removeClient(c); prev == 1 && c.srv != nil {
			c.srv.decActiveAccounts()
		}
	}

	c.mu.Lock()
	// This check does not apply to SYSTEM or JETSTREAM or ACCOUNT clients (because they don't have a `nc`...)
	if c.isClosed() && !isInternalClient(c.kind) {
		c.mu.Unlock()
		return ErrConnectionClosed
	}
	kind := c.kind
	srv := c.srv
	c.acc = acc
	c.applyAccountLimits()
	c.mu.Unlock()

	// Check if we have a max connections violation
	if kind == CLIENT && acc.MaxTotalConnectionsReached() {
		return ErrTooManyAccountConnections
	} else if kind == LEAF {
		// Check if we are already connected to this cluster.
		if rc := c.remoteCluster(); rc != _EMPTY_ && acc.hasLeafNodeCluster(rc) {
			return ErrLeafNodeLoop
		}
		if acc.MaxTotalLeafNodesReached() {
			return ErrTooManyAccountConnections
		}
	}

	// Add in new one.
	if prev := acc.addClient(c); prev == 0 && srv != nil {
		srv.incActiveAccounts()
	}

	return nil
}

// Helper to determine if we have met or exceeded max subs.
func (c *client) subsAtLimit() bool {
	return c.msubs != jwt.NoLimit && len(c.subs) >= int(c.msubs)
}

func minLimit(value *int32, limit int32) bool {
	v := atomic.LoadInt32(value)
	if v != jwt.NoLimit {
		if limit != jwt.NoLimit {
			if limit < v {
				atomic.StoreInt32(value, limit)
				return true
			}
		}
	} else if limit != jwt.NoLimit {
		atomic.StoreInt32(value, limit)
		return true
	}
	return false
}

// Apply account limits
// Lock is held on entry.
// FIXME(dlc) - Should server be able to override here?
func (c *client) applyAccountLimits() {
	if c.acc == nil || (c.kind != CLIENT && c.kind != LEAF) {
		return
	}
	atomic.StoreInt32(&c.mpay, jwt.NoLimit)
	c.msubs = jwt.NoLimit
	if c.opts.JWT != _EMPTY_ { // user jwt implies account
		if uc, _ := jwt.DecodeUserClaims(c.opts.JWT); uc != nil {
			atomic.StoreInt32(&c.mpay, clampInt64ToInt32(uc.Limits.Payload))
			c.msubs = clampInt64ToInt32(uc.Limits.Subs)
			if uc.IssuerAccount != _EMPTY_ && uc.IssuerAccount != uc.Issuer {
				if scope, ok := c.acc.hasIssuer(uc.Issuer); ok {
					if userScope, ok := scope.(*jwt.UserScope); ok {
						// if signing key disappeared or changed and we don't get here, the client will be disconnected
						atomic.StoreInt32(&c.mpay, clampInt64ToInt32(userScope.Template.Limits.Payload))
						c.msubs = clampInt64ToInt32(userScope.Template.Limits.Subs)
					}
				}
			}
		}
	}

	c.acc.mu.RLock()
	minLimit(&c.mpay, c.acc.mpay)
	minLimit(&c.msubs, c.acc.msubs)
	c.acc.mu.RUnlock()

	s := c.srv
	opts := s.getOpts()
	mPay := opts.MaxPayload
	// options encode unlimited differently
	if mPay == 0 {
		mPay = jwt.NoLimit
	}
	mSubs := int32(opts.MaxSubs)
	if mSubs == 0 {
		mSubs = jwt.NoLimit
	}
	wasUnlimited := atomic.LoadInt32(&c.mpay) == jwt.NoLimit
	if minLimit(&c.mpay, mPay) && !wasUnlimited {
		c.Debugf("Max Payload set to %d from server overrides account or user config", opts.MaxPayload)
	}
	wasUnlimited = c.msubs == jwt.NoLimit
	if minLimit(&c.msubs, mSubs) && !wasUnlimited {
		c.Debugf("Max Subscriptions set to %d from server overrides account or user config", opts.MaxSubs)
	}
	if c.subsAtLimit() {
		go func() {
			c.maxSubsExceeded()
			time.Sleep(20 * time.Millisecond)
			c.closeConnection(MaxSubscriptionsExceeded)
		}()
	}
}

// RegisterUser allows auth to call back into a new client
// with the authenticated user. This is used to map
// any permissions into the client and setup accounts.
func (c *client) RegisterUser(user *User) {
	c.registerUser(user)
}

func (c *client) registerUser(user *User) error {
	// Register with proper account and sublist.
	if user.Account != nil {
		if err := c.registerWithAccount(user.Account); err != nil {
			c.reportErrRegisterAccount(user.Account, err)
			return err
		}
	}

	c.mu.Lock()

	// Assign permissions.
	if user.Permissions == nil {
		// Reset perms to nil in case client previously had them.
		c.perms = nil
		c.mperms = nil
		c.darray = nil
	} else {
		c.setPermissions(user.Permissions)
	}

	// allows custom authenticators to set a username to be reported in
	// server events and more
	if user.Username != _EMPTY_ {
		c.opts.Username = user.Username
	}

	// if a deadline time stamp is set we start a timer to disconnect the user at that time
	if !user.ConnectionDeadline.IsZero() {
		c.setExpirationTimerUnlocked(time.Until(user.ConnectionDeadline))
	}

	c.mu.Unlock()
	return nil
}

// RegisterNkeyUser allows auth to call back into a new nkey
// client with the authenticated user. This is used to map
// any permissions into the client and setup accounts.
func (c *client) RegisterNkeyUser(user *NkeyUser) error {
	// Register with proper account and sublist.
	if user.Account != nil {
		if err := c.registerWithAccount(user.Account); err != nil {
			c.reportErrRegisterAccount(user.Account, err)
			return err
		}
	}

	c.mu.Lock()
	c.user = user
	// Assign permissions.
	if user.Permissions == nil {
		// Reset perms to nil in case client previously had them.
		c.perms = nil
		c.mperms = nil
		c.darray = nil
	} else {
		c.setPermissions(user.Permissions)
	}
	c.mu.Unlock()
	return nil
}

func (c *client) updateDefaultPermissions(perms *Permissions) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.user == nil || !c.user.defaultPerms {
		return false
	}
	// A leafnode's perms are set up at connect, like on config reload.
	if c.kind == LEAF {
		return false
	}
	if perms == nil {
		c.user.Permissions = nil
		c.perms = nil
		c.mperms = nil
		c.darray = nil
		c.replies = nil
		return true
	}
	var responsePermissionsUnchanged bool
	if c.user.Permissions != nil {
		responsePermissionsUnchanged = sameResponsePermissions(c.user.Permissions.Response, perms.Response)
	}
	replies := c.replies
	c.user.Permissions = perms.clone()
	c.setPermissions(c.user.Permissions)
	if responsePermissionsUnchanged {
		c.replies = replies
	} else if c.user.Permissions.Response == nil {
		c.replies = nil
	}
	for _, sub := range c.subs {
		if len(sub.queue) > 0 {
			c.canSubscribe(string(sub.subject), string(sub.queue))
		} else {
			c.canSubscribe(string(sub.subject))
		}
	}
	return true
}

func sameResponsePermissions(current, updated *ResponsePermission) bool {
	return current != nil && updated != nil && *current == *updated
}

func splitSubjectQueue(sq string) ([]byte, []byte, error) {
	vals := strings.Fields(strings.TrimSpace(sq))
	if len(vals) == 0 {
		return nil, nil, fmt.Errorf("invalid subject-queue %q", sq)
	}
	s := []byte(vals[0])
	var q []byte
	if len(vals) == 2 {
		q = []byte(vals[1])
	} else if len(vals) > 2 {
		return nil, nil, fmt.Errorf("invalid subject-queue %q", sq)
	}
	if !IsValidSubject(vals[0]) || (len(q) > 0 && !IsValidSubject(vals[1])) {
		return nil, nil, fmt.Errorf("invalid subject-queue %q", sq)
	}
	return s, q, nil
}

// Initializes client.perms structure.
// Lock is held on entry.
func (c *client) setPermissions(perms *Permissions) {
	if perms == nil {
		return
	}
	c.perms = &permissions{}
	c.mperms = nil
	c.darray = nil
	slcache := c.srv != nil && !c.srv.getOpts().NoSublistCache

	// Loop over publish permissions
	if perms.Publish != nil {
		if perms.Publish.Allow != nil {
			c.perms.pub.allow = NewSublist(slcache)
		}
		for _, pubSubject := range perms.Publish.Allow {
			if !IsValidSubject(pubSubject) {
				c.Errorf("invalid publish subject %q", pubSubject)
				continue
			}
			sub := &subscription{subject: []byte(pubSubject)}
			if err := c.perms.pub.allow.Insert(sub); err != nil {
				c.Errorf("invalid publish subject %q", pubSubject)
			}
		}
		if len(perms.Publish.Deny) > 0 {
			c.perms.pub.deny = NewSublist(slcache)
		}
		for _, pubSubject := range perms.Publish.Deny {
			subject := []byte(pubSubject)
			if !IsValidSubject(pubSubject) {
				var err error
				subject, _, err = splitSubjectQueue(pubSubject)
				if err != nil {
					c.Errorf("invalid publish deny subject %q", pubSubject)
					continue
				}
				// Queue qualifiers have no meaning for publish permissions. If
				// one reaches this defensive path, retain the deny's subject
				// scope instead of silently creating an unreachable trie node.
				c.Errorf("queue qualifier is not valid for publish deny subject %q", pubSubject)
			}
			if err := c.perms.pub.deny.Insert(&subscription{subject: subject}); err != nil {
				c.Errorf("invalid publish deny subject %q", pubSubject)
			}
		}
	}

	// Check if we are allowed to send responses.
	if perms.Response != nil {
		rp := *perms.Response
		c.perms.resp = &rp
		c.replies = make(map[string]*resp)
	}

	// Loop over subscribe permissions
	if perms.Subscribe != nil {
		var err error
		if len(perms.Subscribe.Allow) > 0 {
			c.perms.sub.allow = NewSublistNoCache()
		}
		for _, subSubject := range perms.Subscribe.Allow {
			sub := &subscription{}
			sub.subject, sub.queue, err = splitSubjectQueue(subSubject)
			if err != nil {
				c.Errorf("%s", err.Error())
				continue
			}
			if err := c.perms.sub.allow.Insert(sub); err != nil {
				c.Errorf("invalid subscribe allow subject %q", subSubject)
			}
		}
		if len(perms.Subscribe.Deny) > 0 {
			c.perms.sub.deny = NewSublistNoCache()
		}
		for _, subSubject := range perms.Subscribe.Deny {
			sub := &subscription{}
			sub.subject, sub.queue, err = splitSubjectQueue(subSubject)
			if err != nil {
				c.Errorf("%s", err.Error())
				continue
			}
			if err := c.perms.sub.deny.Insert(sub); err != nil {
				c.Errorf("invalid subscribe deny subject %q", subSubject)
				continue
			}
			// Retain the parsed representation for delivery-time filtering.
			c.darray = append(c.darray, sub)
		}
	}

	// If we are a leafnode and we are the hub copy the extracted perms
	// to resend back to soliciting server. These are reversed from the
	// way routes interpret them since this is how the soliciting server
	// will receive these back in an update INFO.
	if c.isHubLeafNode() {
		c.opts.Import = perms.Subscribe
		c.opts.Export = perms.Publish
	}
}

// Build public permissions from internal ones.
// Used for user info requests.
func (c *client) publicPermissions() *Permissions {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.perms == nil {
		return nil
	}
	perms := &Permissions{
		Publish:   &SubjectPermission{},
		Subscribe: &SubjectPermission{},
	}

	_subs := [32]*subscription{}

	// Publish
	if c.perms.pub.allow != nil {
		subs := _subs[:0]
		c.perms.pub.allow.All(&subs)
		for _, sub := range subs {
			perms.Publish.Allow = append(perms.Publish.Allow, string(sub.subject))
		}
	}
	if c.perms.pub.deny != nil {
		subs := _subs[:0]
		c.perms.pub.deny.All(&subs)
		for _, sub := range subs {
			perms.Publish.Deny = append(perms.Publish.Deny, string(sub.subject))
		}
	}
	// Subsribe
	if c.perms.sub.allow != nil {
		subs := _subs[:0]
		c.perms.sub.allow.All(&subs)
		for _, sub := range subs {
			perms.Subscribe.Allow = append(perms.Subscribe.Allow, subjectQueueString(sub))
		}
	}
	if c.perms.sub.deny != nil {
		subs := _subs[:0]
		c.perms.sub.deny.All(&subs)
		for _, sub := range subs {
			perms.Subscribe.Deny = append(perms.Subscribe.Deny, subjectQueueString(sub))
		}
	}
	// Responses.
	if c.perms.resp != nil {
		rp := *c.perms.resp
		perms.Response = &rp
	}

	return perms
}

func subjectQueueString(sub *subscription) string {
	if len(sub.queue) == 0 {
		return string(sub.subject)
	}
	return string(sub.subject) + " " + string(sub.queue)
}

type denyType int

const (
	pub = denyType(iota + 1)
	sub
	both
)

// Merge client.perms structure with additional pub deny permissions
// Lock is held on entry.
func (c *client) mergeDenyPermissions(what denyType, denyPubs []string) {
	if len(denyPubs) == 0 {
		return
	}
	if c.perms == nil {
		c.perms = &permissions{}
	}
	if what == pub || what == both {
		if c.perms.pub.deny == nil {
			c.perms.pub.deny = NewSublistForServer(c.srv)
		}
		mergeDenyPerm(&c.perms.pub, denyPubs, false)
	}
	if what == sub || what == both {
		if c.perms.sub.deny == nil {
			// Avoid sublist cache contention in canSubscribe.
			c.perms.sub.deny = NewSublistNoCache()
		}
		c.darray = append(c.darray, mergeDenyPerm(&c.perms.sub, denyPubs, true)...)
	}
}

// mergeDenyPerm inserts new deny permissions, skipping subjects that already exist.
func mergeDenyPerm(p *perm, denyPubs []string, allowQueue bool) []*subscription {
	var inserted []*subscription
FOR_DENY:
	for _, deny := range denyPubs {
		subject, queue, err := splitSubjectQueue(deny)
		if err != nil {
			continue
		}
		if !allowQueue {
			queue = nil
		}
		r := p.deny.Match(string(subject))
		for _, v := range r.qsubs {
			for _, s := range v {
				if bytes.Equal(s.subject, subject) && bytes.Equal(s.queue, queue) {
					continue FOR_DENY
				}
			}
		}
		for _, s := range r.psubs {
			if bytes.Equal(s.subject, subject) && len(queue) == 0 {
				continue FOR_DENY
			}
		}
		sub := &subscription{subject: subject, queue: queue}
		if p.deny.Insert(sub) == nil {
			inserted = append(inserted, sub)
		}
	}
	return inserted
}

// Merge client.perms structure with additional pub deny permissions
// Client lock must not be held on entry
func (c *client) mergeDenyPermissionsLocked(what denyType, denyPubs []string) {
	c.mu.Lock()
	c.mergeDenyPermissions(what, denyPubs)
	c.mu.Unlock()
}

// Check to see if we have an expiration for the user JWT via base claims.
// FIXME(dlc) - Clear on connect with new JWT.
func (c *client) setExpiration(claims *jwt.ClaimsData, validFor time.Duration) {
	if claims.Expires == 0 {
		if validFor != 0 {
			c.setExpirationTimer(validFor)
		}
		return
	}
	expiresAt := time.Duration(0)
	tn := time.Now().Unix()
	if claims.Expires > tn {
		expiresAt = time.Duration(claims.Expires-tn) * time.Second
	}
	if validFor != 0 && validFor < expiresAt {
		c.setExpirationTimer(validFor)
	} else {
		c.setExpirationTimer(expiresAt)
	}
}

// This will load up the deny structure used for filtering delivered
// messages based on a deny clause for subscriptions.
// Lock should be held.
func (c *client) loadMsgDenyFilter() {
	c.mperms = &msgDeny{NewSublistWithCache(), make(map[msgDenyKey]bool)}
	for _, sub := range c.darray {
		c.mperms.deny.Insert(&subscription{subject: sub.subject, queue: sub.queue})
	}
}

// writeLoop is the main socket write functionality.
// Runs in its own Go routine.
func (c *client) writeLoop() {
	defer c.srv.grWG.Done()
	c.mu.Lock()
	if c.isClosed() {
		c.mu.Unlock()
		return
	}
	c.flags.set(writeLoopStarted)
	c.mu.Unlock()

	// Used to check that we did flush from last wake up.
	waitOk := true
	var closed bool

	// Main loop. Will wait to be signaled and then will use
	// buffered outbound structure for efficient writev to the underlying socket.
	for {
		c.mu.Lock()
		if closed = c.isClosed(); !closed {
			owtf := c.out.fsp > 0 && c.out.pb < maxBufSize && c.out.fsp < maxFlushPending
			if waitOk && (c.out.pb == 0 || owtf) {
				c.out.sg.Wait()
				// Check that connection has not been closed while lock was released
				// in the conditional wait.
				closed = c.isClosed()
			}
		}
		if closed {
			c.flushAndClose(false)
			c.mu.Unlock()

			// We should always call closeConnection() to ensure that state is
			// properly cleaned-up. It will be a no-op if already done.
			c.closeConnection(WriteError)

			// Now explicitly call reconnect(). Thanks to ref counting, we know
			// that the reconnect will execute only after connection has been
			// removed from the server state.
			c.reconnect()
			return
		}
		// Flush data
		waitOk = c.flushOutbound()
		c.mu.Unlock()
	}
}

// flushClients will make sure to flush any clients we may have
// sent to during processing. We pass in a budget as a time.Duration
// for how much time to spend in place flushing for this client.
func (c *client) flushClients(budget time.Duration) time.Time {
	last := time.Now()

	// Check pending clients for flush.
	for cp := range c.pcd {
		// TODO(dlc) - Wonder if it makes more sense to create a new map?
		delete(c.pcd, cp)

		// Queue up a flush for those in the set
		cp.mu.Lock()
		// Update last activity for message delivery
		cp.last = last
		// Remove ourselves from the pending list.
		cp.out.fsp--

		// Just ignore if this was closed.
		if cp.isClosed() {
			cp.mu.Unlock()
			continue
		}

		if budget > 0 && cp.out.lft < 2*budget && cp.flushOutbound() {
			budget -= cp.out.lft
		} else {
			cp.flushSignal()
		}

		cp.mu.Unlock()
	}
	return last
}

func (c *client) resetReadLoopStallTime() {
	if c.in.tst >= stallClientMaxDuration {
		c.rateLimitFormatWarnf("Producer was stalled for a total of %v", c.in.tst.Round(time.Millisecond))
	}
	c.in.tst = 0
}

// readLoop is the main socket read functionality.
// Runs in its own Go routine.
func (c *client) readLoop(pre []byte) {
	// Grab the connection off the client, it will be cleared on a close.
	// We check for that after the loop, but want to avoid a nil dereference
	c.mu.Lock()
	s := c.srv
	defer s.grWG.Done()
	if c.isClosed() {
		c.mu.Unlock()
		return
	}
	nc := c.nc
	ws := c.isWebsocket()
	if c.isMqtt() {
		c.mqtt.r = &mqttReader{reader: nc}
	}
	c.in.rsz = startBufSize

	// Check the per-account-cache for closed subscriptions
	cpacc := c.kind == ROUTER || c.kind == GATEWAY
	// Last per-account-cache check for closed subscriptions
	lpacc := time.Now()
	acc := c.acc
	var masking bool
	if ws {
		masking = c.ws.maskread
	}
	checkCompress := c.kind == ROUTER || c.kind == LEAF
	c.mu.Unlock()

	defer func() {
		if c.isMqtt() {
			s.mqttHandleClosedClient(c)
		}
		// These are used only in the readloop, so we can set them to nil
		// on exit of the readLoop.
		c.in.results, c.in.pacache = nil, nil
	}()

	// Start read buffer.
	b := make([]byte, c.in.rsz)

	// Websocket clients will return several slices if there are multiple
	// websocket frames in the blind read. For non WS clients though, we
	// will always have 1 slice per loop iteration. So we define this here
	// so non WS clients will use bufs[0] = b[:n].
	var _bufs [1][]byte
	bufs := _bufs[:1]

	var wsr *wsReadInfo
	if ws {
		wsr = &wsReadInfo{mask: masking}
		wsr.init()
	}

	var decompress bool
	var reader io.Reader
	reader = nc

	for {
		var n int
		var err error

		// If we have a pre buffer parse that first.
		if len(pre) > 0 {
			b = pre
			n = len(pre)
			pre = nil
		} else {
			n, err = reader.Read(b)
			// If we have any data we will try to parse and exit at the end.
			if n == 0 && err != nil {
				c.closeConnection(closedStateForErr(err))
				return
			}
		}
		// Check if the account has mappings and if so set the local readcache flag.
		// We check here to make sure any changes such as config reload are reflected here.
		if c.kind == CLIENT || c.kind == LEAF {
			if acc.hasMappings() {
				c.in.flags.set(hasMappings)
			} else {
				c.in.flags.clear(hasMappings)
			}
		}

		c.in.start = time.Now()

		// Clear inbound stats cache
		c.in.msgs = 0
		c.in.bytes = 0
		c.in.subs = 0

		if ws {
			err = c.wsReadAndParse(wsr, reader, b[:n])
			if err != nil {
				// Match the normal parse path: any already-buffered deliveries
				// need their pending flush signals drained before we close.
				c.flushClients(0)
				if err != io.EOF {
					c.Errorf("read error: %v", err)
				}
				c.closeConnection(closedStateForErr(err))
				return
			}
			c.resetReadLoopStallTime()
			goto postParse
		} else {
			bufs[0] = b[:n]
		}

		// Main call into parser for inbound data. This will generate callouts
		// to process messages, etc.
		for i := 0; i < len(bufs); i++ {
			if err := c.parse(bufs[i]); err != nil {
				if err == ErrMinimumVersionRequired {
					// Special case here, currently only for leaf node connections.
					// processLeafConnect() already sent the rejection and closed
					// the connection, so there is nothing else to do here.
					return
				}
				if dur := time.Since(c.in.start); dur >= readLoopReportThreshold {
					c.Warnf("Readloop processing time: %v", dur)
				}
				// Need to call flushClients because some of the clients have been
				// assigned messages and their "fsp" incremented, and need now to be
				// decremented and their writeLoop signaled.
				c.flushClients(0)
				// Handled inline, or the connection was already closed
				// (e.g. account registration failure in processConnect).
				if err != ErrMaxPayload && err != ErrAuthentication && err != ErrConnectionClosed {
					c.Error(err)
					c.closeConnection(ProtocolViolation)
				}
				return
			}
			c.resetReadLoopStallTime()
		}

	postParse:
		// If we are a ROUTER/LEAF and have processed an INFO, it is possible that
		// we are asked to switch to compression now.
		if checkCompress && c.in.flags.isSet(switchToCompression) {
			c.in.flags.clear(switchToCompression)
			// For now we support only s2 compression...
			reader = s2.NewReader(nc)
			decompress = true
		}

		// Updates stats for client and server that were collected
		// from parsing through the buffer.
		if c.in.msgs > 0 {
			inMsgs := int64(c.in.msgs)
			inBytes := int64(c.in.bytes)

			atomic.AddInt64(&c.inMsgs, inMsgs)
			atomic.AddInt64(&c.inBytes, inBytes)

			// A per-account route has its account set, but routed messages were
			// already added to the account's stats in processInboundRoutedMsg.
			if acc != nil && c.kind != ROUTER {
				acc.stats.Lock()
				acc.stats.inMsgs += inMsgs
				acc.stats.inBytes += inBytes
				if c.kind == LEAF {
					acc.stats.ln.inMsgs += int64(inMsgs)
					acc.stats.ln.inBytes += int64(inBytes)
				}
				acc.stats.Unlock()
			}

			if c.kind == CLIENT {
				atomic.AddInt64(&s.inClientMsgs, inMsgs)
				atomic.AddInt64(&s.inClientBytes, inBytes)
			}

			atomic.AddInt64(&s.inMsgs, inMsgs)
			atomic.AddInt64(&s.inBytes, inBytes)
		}

		// Signal to writeLoop to flush to socket.
		last := c.flushClients(0)

		// Update activity, check read buffer size.
		c.mu.Lock()

		// Activity based on interest changes or data/msgs.
		// Also update last receive activity for ping sender
		if c.in.msgs > 0 || c.in.subs > 0 {
			c.last = last
			c.lastIn = last
		}

		if n >= cap(b) {
			c.in.srs = 0
		} else if n < cap(b)/2 { // divide by 2 b/c we want less than what we would shrink to.
			c.in.srs++
		}

		// Update read buffer size as/if needed.
		if n >= cap(b) && cap(b) < maxBufSize {
			// Grow
			c.in.rsz = int32(cap(b) * 2)
			b = make([]byte, c.in.rsz)
		} else if n < cap(b) && cap(b) > minBufSize && c.in.srs > shortsToShrink {
			// Shrink, for now don't accelerate, ping/pong will eventually sort it out.
			c.in.rsz = int32(cap(b) / 2)
			b = make([]byte, c.in.rsz)
		}
		// re-snapshot the account since it can change during reload, etc.
		acc = c.acc
		// Refresh nc because in some cases, we have upgraded c.nc to TLS.
		if nc != c.nc {
			nc = c.nc
			if decompress && nc != nil {
				// For now we support only s2 compression...
				reader.(*s2.Reader).Reset(nc)
			} else if !decompress {
				reader = nc
			}
		}
		c.mu.Unlock()

		// Connection was closed
		if nc == nil {
			return
		}

		if dur := time.Since(c.in.start); dur >= readLoopReportThreshold {
			c.Warnf("Readloop processing time: %v", dur)
		}

		// We could have had a read error from above but still read some data.
		// If so do the close here unconditionally.
		if err != nil {
			c.closeConnection(closedStateForErr(err))
			return
		}

		if cpacc && c.in.start.Sub(lpacc) >= closedSubsCheckInterval {
			c.pruneClosedSubFromPerAccountCache()
			lpacc = time.Now()
		}
	}
}

// Returns the appropriate closed state for a given read error.
func closedStateForErr(err error) ClosedState {
	if err == io.EOF {
		return ClientClosed
	}
	return ReadError
}

// collapsePtoNB will either returned framed WebSocket buffers or it will
// return a reference to c.out.nb.
func (c *client) collapsePtoNB() (net.Buffers, int64) {
	if c.isWebsocket() {
		return c.wsCollapsePtoNB()
	}
	return c.out.nb, c.out.pb
}

// flushOutbound will flush outbound buffer to a client.
// Will return true if data was attempted to be written.
// Lock must be held
func (c *client) flushOutbound() bool {
	if c.flags.isSet(flushOutbound) {
		// For CLIENT connections, it is possible that the readLoop calls
		// flushOutbound(). If writeLoop and readLoop compete and we are
		// here we should release the lock to reduce the risk of spinning.
		c.mu.Unlock()
		runtime.Gosched()
		c.mu.Lock()
		return false
	}
	c.flags.set(flushOutbound)
	defer func() {
		// Check flushAndClose() for explanation on why we do this.
		if c.isClosed() {
			for i := range c.out.wnb {
				nbPoolPut(c.out.wnb[i])
			}
			c.out.wnb = nil
		}
		c.flags.clear(flushOutbound)
	}()

	// Check for nothing to do.
	if c.nc == nil || c.srv == nil || c.out.pb == 0 {
		return true // true because no need to queue a signal.
	}

	// In the case of a normal socket connection, "collapsed" is just a ref
	// to "nb". In the case of WebSockets, additional framing is added to
	// anything that is waiting in "nb". Also keep a note of how many bytes
	// were queued before we release the mutex.
	collapsed, attempted := c.collapsePtoNB()

	// Frustratingly, (net.Buffers).WriteTo() modifies the receiver so we
	// can't work on "nb" directly — while the mutex is unlocked during IO,
	// something else might call queueOutbound and modify it. So instead we
	// need a working copy — we'll operate on "wnb" instead. Note that in
	// the case of a partial write, "wnb" may have remaining data from the
	// previous write, and in the case of WebSockets, that data may already
	// be framed, so we are careful not to re-frame "wnb" here. Instead we
	// will just frame up "nb" and append it onto whatever is left on "wnb".
	// "nb" will be set to nil so that we can manipulate "collapsed" outside
	// of the client's lock, which is interesting in case of compression.
	c.out.nb = nil

	// In case it goes away after releasing the lock.
	nc := c.nc

	// Capture this (we change the value in some tests)
	wdl := c.out.wdl

	// Check for compression
	cw := c.out.cw
	if cw != nil {
		// Replace only the bytes being compressed. Pending bytes also include
		// already-compressed data left in wnb after a partial write.
		attempted = 0
		for _, buf := range collapsed {
			attempted += int64(len(buf))
		}
		c.out.pb -= attempted
		if c.isWebsocket() {
			c.ws.fs -= attempted
		}
	}

	// Do NOT hold lock during actual IO.
	c.mu.Unlock()

	// Compress outside of the lock
	if cw != nil {
		var err error
		bb := bytes.Buffer{}

		cw.Reset(&bb)
		for _, buf := range collapsed {
			if err == nil {
				_, err = cw.Write(buf)
			}
			// Return always after consumed or error.
			nbPoolPut(buf)
		}
		if err == nil {
			err = cw.Close()
		}
		if err != nil {
			c.Errorf("Error compressing data: %v", err)
			// We need to grab the lock now before marking as closed and exiting
			c.mu.Lock()
			c.markConnAsClosed(WriteError)
			return false
		}
		collapsed = append(net.Buffers(nil), bb.Bytes())
		attempted = int64(len(collapsed[0]))
	}

	// This is safe to do outside of the lock since "collapsed" is no longer
	// referenced in c.out.nb (which can be modified in queueOutbound() while
	// the lock is released).
	c.out.wnb = append(c.out.wnb, collapsed...)
	var _orig [nbMaxVectorSize][]byte
	orig := append(_orig[:0], c.out.wnb...)

	// Since WriteTo is lopping things off the beginning, we need to remember
	// the start position of the underlying array so that we can get back to it.
	// Otherwise we'll always "slide forward" and that will result in reallocs.
	startOfWnb := c.out.wnb[0:]

	// flush here
	start := time.Now()

	var n int64   // Total bytes written
	var wn int64  // Bytes written per loop
	var err error // Error from last write, if any
	for len(c.out.wnb) > 0 {
		// Limit the number of vectors to no more than nbMaxVectorSize,
		// which if 1024, will mean a maximum of 64MB in one go.
		wnb := c.out.wnb
		if len(wnb) > nbMaxVectorSize {
			wnb = wnb[:nbMaxVectorSize]
		}
		consumed := len(wnb)

		// Actual write to the socket. The deadline applies to each batch
		// rather than the total write, such that the configured deadline
		// can be tuned to a known maximum quantity (64MB).
		nc.SetWriteDeadline(time.Now().Add(wdl))
		wn, err = wnb.WriteTo(nc)

		// Update accounting, move wnb slice onwards if needed, or stop
		// if a write error was reported that wasn't a short write.
		n += wn
		c.out.wnb = c.out.wnb[consumed-len(wnb):]
		if err != nil && err != io.ErrShortWrite {
			break
		}
	}

	// The deadline is overwritten at the start of each iteration anyway,
	// so only needed to clear the write deadline once after the loop.
	nc.SetWriteDeadline(time.Time{})

	lft := time.Since(start)

	// Re-acquire client lock.
	c.mu.Lock()

	// Adjust if we were compressing.
	if cw != nil {
		c.out.pb += attempted
		if c.isWebsocket() {
			c.ws.fs += attempted
		}
	}

	// In case of a partial write, WriteTo will have resliced the buffer
	// to drop the written bytes. As a side effect this will also change
	// its capacity so it can't be recycled by nbPoolPut as originally intended
	// and may either leak or land in the wrong pool.
	// Prevent this by putting the remaining bytes back to the start of the slice
	// so that it keeps its full capacity and is recycled once fully written or
	// when the connection is closed.
	if rem := len(c.out.wnb); rem > 0 && rem <= len(orig) {
		if k := len(orig) - rem; len(c.out.wnb[0]) < len(orig[k]) {
			n := copy(orig[k], c.out.wnb[0])
			c.out.wnb[0] = orig[k][:n]
		}
	}

	// At this point, "wnb" has been mutated by WriteTo and any consumed
	// buffers have been lopped off the beginning, so in order to return
	// them to the pool, we need to look at the difference between "orig"
	// and "wnb".
	for i := 0; i < len(orig)-len(c.out.wnb); i++ {
		nbPoolPut(orig[i])
	}

	// At this point it's possible that "nb" has been modified by another
	// call to queueOutbound while the lock was released, so we'll leave
	// those for the next iteration. Meanwhile it's possible that we only
	// managed a partial write of "wnb", so we'll shift anything that
	// remains up to the beginning of the array to prevent reallocating.
	// Anything left in "wnb" has already been framed for WebSocket conns
	// so leave them alone for the next call to flushOutbound.
	c.out.wnb = append(startOfWnb[:0], c.out.wnb...)

	// If we've written everything but the underlying array of our working
	// buffer has grown excessively then free it — the GC will tidy it up
	// and we can allocate a new one next time.
	if len(c.out.wnb) == 0 && cap(c.out.wnb) > nbMaxVectorSize {
		c.out.wnb = nil
	}

	// Ignore ErrShortWrite errors, they will be handled as partials.
	var gotWriteTimeout bool
	if err != nil && err != io.ErrShortWrite {
		// Handle timeout error (slow consumer) differently
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			gotWriteTimeout = true
			if closed := c.handleWriteTimeout(n, attempted, len(orig)); closed {
				return true
			}
		} else {
			// Other errors will cause connection to be closed.
			// For clients, report as debug but for others report as error.
			report := c.Debugf
			if c.kind != CLIENT {
				report = c.Errorf
			}
			report("Error flushing: %v", err)
			c.markConnAsClosed(WriteError)
			return true
		}
	}

	// Update flush time statistics.
	c.out.lft = lft

	// Subtract from pending bytes and messages.
	c.out.pb -= n
	if c.isWebsocket() {
		c.ws.fs -= n
	}

	// Check that if there is still data to send and writeLoop is in wait,
	// then we need to signal.
	if c.out.pb > 0 {
		c.flushSignal()
	}

	// Check if we have a stalled gate and if so and we are recovering release
	// any stalled producers. Only kind==CLIENT will stall.
	if c.out.stc != nil && (n == attempted || c.out.pb < c.out.mp/4*3) {
		close(c.out.stc)
		c.out.stc = nil
	}
	// Check if the connection is recovering from being a slow consumer.
	if !gotWriteTimeout && c.flags.isSet(isSlowConsumer) {
		c.Noticef("Slow Consumer Recovered: Flush took %.3fs with %d chunks of %d total bytes.", time.Since(start).Seconds(), len(orig), attempted)
		c.flags.clear(isSlowConsumer)
	}

	return true
}

// This is invoked from flushOutbound() for io/timeout error (slow consumer).
// Returns a boolean to indicate if the connection has been closed or not.
// Lock is held on entry.
func (c *client) handleWriteTimeout(written, attempted int64, numChunks int) bool {
	if tlsConn, ok := c.nc.(*tls.Conn); ok {
		if !tlsConn.ConnectionState().HandshakeComplete {
			// Likely a TLSTimeout error instead...
			c.markConnAsClosed(TLSHandshakeError)
			// Would need to coordinate with tlstimeout()
			// to avoid double logging, so skip logging
			// here, and don't report a slow consumer error.
			return true
		}
	} else if c.flags.isSet(expectConnect) && !c.flags.isSet(connectReceived) {
		// Under some conditions, a connection may hit a slow consumer write deadline
		// before the authorization timeout. If that is the case, then we handle
		// as slow consumer though we do not increase the counter as that can be
		// misleading.
		c.markConnAsClosed(SlowConsumerWriteDeadline)
		return true
	}
	alreadySC := c.flags.isSet(isSlowConsumer)
	scState := "Detected"
	if alreadySC {
		scState = "State"
	}

	// Aggregate slow consumers.
	atomic.AddInt64(&c.srv.slowConsumers, 1)
	switch c.kind {
	case CLIENT:
		c.srv.scStats.clients.Add(1)
	case ROUTER:
		// Only count each Slow Consumer event once.
		if !alreadySC {
			c.srv.scStats.routes.Add(1)
		}
	case GATEWAY:
		c.srv.scStats.gateways.Add(1)
	case LEAF:
		c.srv.scStats.leafs.Add(1)
	}
	if c.acc != nil {
		c.acc.stats.Lock()
		c.acc.stats.slowConsumers++
		c.acc.stats.Unlock()
	}
	c.Noticef("Slow Consumer %s: WriteDeadline of %v exceeded with %d chunks of %d total bytes.",
		scState, c.out.wdl, numChunks, attempted)

	// We always close CLIENT connections, or when nothing was written at all...
	if c.out.wtp == WriteTimeoutPolicyClose || written == 0 {
		c.markConnAsClosed(SlowConsumerWriteDeadline)
		return true
	} else {
		c.flags.setIfNotSet(isSlowConsumer)
	}
	return false
}

// Marks this connection has closed with the given reason.
// Sets the connMarkedClosed flag and skipFlushOnClose depending on the reason.
// Depending on the kind of connection, the connection will be saved.
// If a writeLoop has been started, the final flush will be done there, otherwise
// flush and close of TCP connection is done here in place.
// Returns true if closed in place, flase otherwise.
// Lock is held on entry.
func (c *client) markConnAsClosed(reason ClosedState) {
	// Possibly set skipFlushOnClose flag even if connection has already been
	// mark as closed. The rationale is that a connection may be closed with
	// a reason that justifies a flush (say after sending an -ERR), but then
	// the flushOutbound() gets a write error. If that happens, connection
	// being lost, there is no reason to attempt to flush again during the
	// teardown when the writeLoop exits.
	var skipFlush bool
	switch reason {
	case ReadError, WriteError, SlowConsumerPendingBytes, SlowConsumerWriteDeadline, TLSHandshakeError:
		c.flags.set(skipFlushOnClose)
		skipFlush = true
	case StaleConnection:
		// Track stale connections statistics.
		atomic.AddInt64(&c.srv.staleConnections, 1)
		switch c.kind {
		case CLIENT:
			c.srv.staleStats.clients.Add(1)
		case ROUTER:
			c.srv.staleStats.routes.Add(1)
		case GATEWAY:
			c.srv.staleStats.gateways.Add(1)
		case LEAF:
			c.srv.staleStats.leafs.Add(1)
		}
	}
	if c.flags.isSet(connMarkedClosed) {
		return
	}
	c.flags.set(connMarkedClosed)
	// For a websocket client, unless we are told not to flush, enqueue
	// a websocket CloseMessage based on the reason.
	if !skipFlush && c.isWebsocket() && !c.ws.closeSent {
		c.wsEnqueueCloseMessage(reason)
	}
	// Be consistent with the creation: for routes, gateways and leaf,
	// we use Noticef on create, so use that too for delete.
	if c.srv != nil {
		if c.kind == LEAF || c.kind == ROUTER || c.kind == GATEWAY {
			var tags []string
			var remoteName string
			var remoteURL *url.URL
			switch {
			case c.kind == LEAF && c.leaf != nil:
				remoteName = c.leaf.remoteServer
				if c.leaf.remote != nil {
					remoteURL = c.leaf.remote.getCurrentURL()
				}
			case c.kind == ROUTER && c.route != nil:
				remoteName = c.route.remoteName
				remoteURL = c.route.url
			case c.kind == GATEWAY && c.gw != nil:
				remoteName = c.gw.remoteName
				remoteURL = c.gw.connectURL
			}
			if remoteName != _EMPTY_ {
				tags = append(tags, fmt.Sprintf("Remote: %s", remoteName))
			}
			if remoteURL != nil {
				tags = append(tags, fmt.Sprintf("Host: %s", remoteURL.Host))
			}
			if len(tags) > 0 {
				c.Noticef("%s connection closed: %s - %s", c.kindString(), reason, strings.Join(tags, ", "))
			} else {
				c.Noticef("%s connection closed: %s", c.kindString(), reason)
			}
		} else { // Client, System, Jetstream, and Account connections.
			c.Debugf("%s connection closed: %s", c.kindString(), reason)
		}
	}

	// Save off the connection if its a client or leafnode.
	if c.kind == CLIENT || c.kind == LEAF {
		if nc := c.nc; nc != nil && c.srv != nil {
			// TODO: May want to send events to single go routine instead
			// of creating a new go routine for each save.
			// Pass the c.subs as a reference. It may be set to nil in
			// closeConnection.
			go c.srv.saveClosedClient(c, nc, c.subs, reason)
		}
	}
	// If writeLoop exists, let it do the final flush, close and teardown.
	if c.flags.isSet(writeLoopStarted) {
		// Since we want the writeLoop to do the final flush and tcp close,
		// we want the reconnect to be done there too. However, it should'nt
		// happen before the connection has been removed from the server
		// state (end of closeConnection()). This ref count allows us to
		// guarantee that.
		c.rref++
		c.flushSignal()
		return
	}
	// Flush (if skipFlushOnClose is not set) and close in place. If flushing,
	// use a small WriteDeadline.
	c.flushAndClose(true)
}

// flushSignal will use server to queue the flush IO operation to a pool of flushers.
// Lock must be held.
func (c *client) flushSignal() {
	// Check that sg is not nil, which will happen if the connection is closed.
	if c.out.sg != nil {
		c.out.sg.Signal()
	}
}

// Traces a message.
// Will NOT check if tracing is enabled, does NOT need the client lock.
func (c *client) traceMsgInternal(msg []byte, delivered bool, hdrSize int) {
	opts := c.srv.getOpts()
	maxTrace := opts.MaxTracedMsgLen
	headersOnly := opts.TraceHeaders
	suffix := LEN_CR_LF

	// If TraceHeaders is enabled, extract only the header portion of the msg.
	// If a header is present, it ends with an additional trailing CRLF.
	if headersOnly {
		if hdrSize > 0 && len(msg) >= hdrSize {
			msg = msg[:hdrSize]
			suffix += LEN_CR_LF
		} else {
			// No headers present, so nothing to trace.
			return
		}
	}

	// Do not emit a log line for zero-length payloads.
	l := len(msg) - suffix
	if l <= 0 {
		return
	}

	const (
		traceInPrefix  string = "<<-"
		traceOutPrefix string = "->>"
	)
	var prefix string
	if delivered {
		prefix = traceOutPrefix
	} else {
		prefix = traceInPrefix
	}
	if maxTrace > 0 && l > maxTrace {
		tm := fmt.Sprintf("%q", msg[:maxTrace])
		c.Tracef("%s MSG_PAYLOAD: [\"%s...\"]", prefix, tm[1:len(tm)-1])
	} else {
		c.Tracef("%s MSG_PAYLOAD: [%q]", prefix, msg[:l])
	}
}

func (c *client) traceMsg(msg []byte) {
	c.traceMsgInternal(msg, false, c.pa.hdr)
}

func (c *client) traceMsgDelivery(msg []byte, hdrSize int) {
	c.traceMsgInternal(msg, true, hdrSize)
}

// Traces an incoming operation.
// Will NOT check if tracing is enabled, does NOT need the client lock.
func (c *client) traceInOp(op string, arg []byte) {
	c.traceOp("<<- %s", op, arg)
}

// Traces an outgoing operation.
// Will NOT check if tracing is enabled, does NOT need the client lock.
func (c *client) traceOutOp(op string, arg []byte) {
	c.traceOp("->> %s", op, arg)
}

func (c *client) traceOp(format, op string, arg []byte) {
	opa := []any{}
	if op != _EMPTY_ {
		opa = append(opa, op)
	}
	if arg != nil {
		opa = append(opa, bytesToString(arg))
	}
	c.Tracef(format, opa)
}

// Process the information messages from Clients and other Routes.
func (c *client) processInfo(arg []byte) error {
	info := Info{}
	if err := json.Unmarshal(arg, &info); err != nil {
		return err
	}
	switch c.kind {
	case ROUTER:
		c.processRouteInfo(&info)
	case GATEWAY:
		c.processGatewayInfo(&info)
	case LEAF:
		c.processLeafnodeInfo(&info)
	}
	return nil
}

func (c *client) processErr(errStr string) {
	close := true
	switch c.kind {
	case CLIENT:
		c.Errorf("Client Error %s", errStr)
	case ROUTER:
		c.Errorf("Route Error %s", errStr)
	case GATEWAY:
		c.Errorf("Gateway Error %s", errStr)
	case LEAF:
		c.Errorf("Leafnode Error %s", errStr)
		c.leafProcessErr(errStr)
		close = false
	case JETSTREAM:
		c.Errorf("JetStream Error %s", errStr)
	}
	if close {
		c.closeConnection(ParseError)
	}
}

// Matcher for pass/password and auth_token fields.
var prefixAuthPat = regexp.MustCompile(`"?\s*(?:auth_token\S*?|pass\S*?)"?\s*[:=]\s*"?([^",\r\n}]*)`)

// Exact matcher for fields sig, proxy_sig and nkey.
// Overlapping field "sig" does not match inside "proxy_sig".
var exactAuthPat = regexp.MustCompile(`(?:^|[^A-Za-z0-9_])"?\s*(?:proxy_sig|nkey|sig)"?\s*[:=]\s*"?([^",\r\n}]*)`)

// removeSecretsFromTrace removes any notion of passwords/tokens from trace
// messages for logging.
func removeSecretsFromTrace(arg []byte) []byte {
	buf := redact(prefixAuthPat, arg)
	return redact(exactAuthPat, buf)
}

func redact(pat *regexp.Regexp, proto []byte) []byte {
	m := pat.FindAllSubmatchIndex(proto, -1)
	if len(m) == 0 {
		return proto
	}
	// Take a copy of the connect proto just for the trace message.
	var _arg [4096]byte
	buf := append(_arg[:0], proto...)
	redactedPass := []byte("[REDACTED]")
	for i := len(m) - 1; i >= 0; i-- {
		match := m[i]
		if len(match) < 4 {
			continue
		}
		start, end := match[2], match[3]
		// Replace value substring.
		buf = append(buf[:start], append(redactedPass, buf[end:]...)...)
	}
	return buf
}

// Returns the RTT by computing the elapsed time since now and `start`.
// On Windows VM where I (IK) run tests, time.Since() will return 0
// (I suspect some time granularity issues). So return at minimum 1ns.
func computeRTT(start time.Time) time.Duration {
	rtt := time.Since(start)
	if rtt <= 0 {
		rtt = time.Nanosecond
	}
	return rtt
}

// processConnect will process a client connect op.
func (c *client) processConnect(arg []byte) error {
	supportsHeaders := c.srv.supportsHeaders()
	c.mu.Lock()
	// If we can't stop the timer because the callback is in progress...
	if !c.clearAuthTimer() {
		// wait for it to finish and handle sending the failure back to
		// the client.
		for !c.isClosed() {
			c.mu.Unlock()
			time.Sleep(25 * time.Millisecond)
			c.mu.Lock()
		}
		c.mu.Unlock()
		return nil
	}
	c.last = time.Now().UTC()
	// Estimate RTT to start.
	if c.kind == CLIENT {
		c.rtt = computeRTT(c.start)
		if c.srv != nil {
			c.clearPingTimer()
			c.setFirstPingTimer()
		}
	}
	kind := c.kind
	srv := c.srv

	// Moved unmarshalling of clients' Options under the lock.
	// The client has already been added to the server map, so it is possible
	// that other routines lookup the client, and access its options under
	// the client's lock, so unmarshalling the options outside of the lock
	// would cause data RACEs.
	if err := json.Unmarshal(arg, &c.opts); err != nil {
		c.mu.Unlock()
		return err
	}
	// Indicate that the CONNECT protocol has been received, and that the
	// server now knows which protocol this client supports.
	firstConnect := !c.flags.isSet(connectReceived)
	c.flags.set(connectReceived)
	// Capture these under lock
	c.echo = c.opts.Echo
	proto := c.opts.Protocol
	verbose := c.opts.Verbose
	lang := c.opts.Lang
	account := c.opts.Account
	accountNew := c.opts.AccountNew

	// if websocket client, maybe some options through cookies
	if ws := c.ws; ws != nil {
		// if JWT not in the CONNECT, use the cookie JWT (possibly empty).
		if c.opts.JWT == _EMPTY_ {
			c.opts.JWT = ws.cookieJwt
		}
		// if user not in the CONNECT, use the cookie user (possibly empty)
		if c.opts.Username == _EMPTY_ {
			c.opts.Username = ws.cookieUsername
		}
		// if pass not in the CONNECT, use the cookie password (possibly empty).
		if c.opts.Password == _EMPTY_ {
			c.opts.Password = ws.cookiePassword
		}
		// if token not in the CONNECT, use the cookie token (possibly empty).
		if c.opts.Token == _EMPTY_ {
			c.opts.Token = ws.cookieToken
		}
	}

	// when not in operator mode, discard the jwt
	if srv != nil && srv.trustedKeys == nil {
		c.opts.JWT = _EMPTY_
	}

	// For headers both client and server need to support.
	c.headers = supportsHeaders && c.opts.Headers
	c.mu.Unlock()

	if srv != nil {
		// Applicable to clients only:
		// As soon as c.opts is unmarshalled and if the proto is at
		// least ClientProtoInfo, we need to increment the following counter.
		// This is decremented when client is removed from the server's
		// clients map.
		if kind == CLIENT && proto >= ClientProtoInfo && firstConnect {
			srv.mu.Lock()
			srv.cproto++
			srv.mu.Unlock()
		}

		// A second CONNECT may move the client into a different account via
		// checkAuthentication. Drop any previously-registered subscriptions
		// from the current account first so they don't leak in that account's
		// sublist after the client switches. Also, clear any cached sublist
		// results before switching accounts.
		if !firstConnect {
			c.in.genid = 0
			c.in.results = nil
			c.clearAccountSubs(false)
		}

		// Check for Auth
		if ok := srv.checkAuthentication(c); !ok {
			// We may fail here because we reached the account connection limit.
			// In that case registerWithAccount() already recorded the reason,
			// notified the client and closed the connection, so report the
			// limit instead of an authentication violation.
			if c.getAuthError() == ErrTooManyAccountConnections {
				return ErrTooManyAccountConnections
			}
			// Account registration failures already sent the error and closed the connection.
			c.mu.Lock()
			closed := c.isClosed()
			c.mu.Unlock()
			if closed {
				return ErrConnectionClosed
			}
			c.authViolation()
			return ErrAuthentication
		}

		// Check for Account designation, we used to have this as an optional feature for dynamic
		// sandbox environments. Now its considered an error.
		if accountNew || account != _EMPTY_ {
			c.authViolation()
			return ErrAuthentication
		}

		// If no account designation.
		// Do this only for CLIENT and LEAF connections.
		if c.acc == nil && (c.kind == CLIENT || c.kind == LEAF) {
			// By default register with the global account.
			c.registerWithAccount(srv.globalAccount())
		}

		// Initialize user info used in logs.
		c.mu.Lock()
		acc := c.acc
		if c.getRawAuthUser() != _EMPTY_ {
			c.ncsUser.Store(c.getAuthUserLabel())
		}
		c.mu.Unlock()
		if acc != nil {
			acc.mu.RLock()
			c.ncsAcc.Store(acc.traceLabel())
			acc.mu.RUnlock()
		}

		// Enable logging connection details and auth info for this client.
		if c.kind == CLIENT && firstConnect && c.srv != nil {
			var ncs string
			if c.opts.Version != _EMPTY_ {
				ncs = fmt.Sprintf("v%s", c.opts.Version)
			}
			if c.opts.Lang != _EMPTY_ {
				if c.opts.Version == _EMPTY_ {
					ncs = c.opts.Lang
				} else {
					ncs = fmt.Sprintf("%s:%s", ncs, c.opts.Lang)
				}
			}
			if c.opts.Name != _EMPTY_ {
				if c.opts.Version == _EMPTY_ && c.opts.Lang == _EMPTY_ {
					ncs = c.opts.Name
				} else {
					ncs = fmt.Sprintf("%s:%s", ncs, c.opts.Name)
				}
			}
			var acs string
			accl := c.ncsAcc.Load()
			authUser := c.ncsUser.Load()
			if accl != nil && authUser != nil {
				acs = fmt.Sprintf("%s/%s", accl, authUser)
			}
			switch {
			case ncs != _EMPTY_ && acs != _EMPTY_:
				c.ncs.Store(fmt.Sprintf("%s - %q - %q", c, ncs, acs))
			case ncs != _EMPTY_:
				c.ncs.Store(fmt.Sprintf("%s - %q", c, ncs))
			case acs != _EMPTY_:
				c.ncs.Store(fmt.Sprintf("%s - %q", c, acs))
			}
		}
	}

	switch kind {
	case CLIENT:
		// Check client protocol request if it exists.
		if proto < ClientProtoZero || proto > ClientProtoInfo {
			c.sendErr(ErrBadClientProtocol.Error())
			c.closeConnection(BadClientProtocolVersion)
			return ErrBadClientProtocol
		}
		// Check to see that if no_responders is requested
		// they have header support on as well.
		c.mu.Lock()
		misMatch := c.opts.NoResponders && !c.headers
		c.mu.Unlock()
		if misMatch {
			c.sendErr(ErrNoRespondersRequiresHeaders.Error())
			c.closeConnection(NoRespondersRequiresHeaders)
			return ErrNoRespondersRequiresHeaders
		}
		if verbose {
			c.sendOK()
		}
	case ROUTER:
		// Delegate the rest of processing to the route
		return c.processRouteConnect(srv, arg, lang)
	case GATEWAY:
		// Delegate the rest of processing to the gateway
		return c.processGatewayConnect(arg)
	case LEAF:
		// Delegate the rest of processing to the leaf node
		return c.processLeafNodeConnect(srv, arg, lang)
	}
	return nil
}

func (c *client) sendErrAndErr(err string) {
	c.sendErr(err)
	c.RateLimitErrorf(err)
}

func (c *client) sendErrAndDebug(err string) {
	c.sendErr(err)
	c.RateLimitDebugf(err)
}

func (c *client) authTimeout() {
	c.sendErrAndDebug("Authentication Timeout")
	c.closeConnection(AuthenticationTimeout)
}

func (c *client) authExpired() {
	c.sendErrAndDebug("User Authentication Expired")
	c.closeConnection(AuthenticationExpired)
}

func (c *client) accountAuthExpired() {
	c.sendErrAndDebug("Account Authentication Expired")
	c.closeConnection(AuthenticationExpired)
}

func (c *client) authViolation() {
	authErr := c.getAuthError()
	if authErr == nil {
		authErr = ErrAuthentication
	}
	reason := getAuthErrClosedState(authErr)

	var s *Server
	if s = c.srv; s != nil {
		defer s.sendAuthErrorEvent(c, reason.String())
		if c.getRawAuthUser() != _EMPTY_ {
			c.Errorf("%v - %s", ErrAuthentication, c.getAuthUser())
		} else {
			c.Errorf(ErrAuthentication.Error())
		}
	}
	if c.isMqtt() {
		c.mqttEnqueueConnAck(mqttConnAckRCNotAuthorized, false)
	} else {
		// Send this to client, regardless of the authErr override.
		c.sendErr("Authorization Violation")
	}
	c.closeConnection(reason)
}

func (c *client) maxAccountConnExceeded() {
	c.sendErrAndErr(ErrTooManyAccountConnections.Error())
	c.closeConnection(MaxAccountConnectionsExceeded)
}

func (c *client) maxConnExceeded() {
	c.sendErrAndErr(ErrTooManyConnections.Error())
	c.closeConnection(MaxConnectionsExceeded)
}

func (c *client) maxSubsExceeded() {
	if c.acc.shouldLogMaxSubErr() {
		c.Errorf(ErrTooManySubs.Error())
	}
	c.sendErr(ErrTooManySubs.Error())
}

func (c *client) maxPayloadViolation(sz int, max int32) {
	c.Errorf("%s: %d vs %d", ErrMaxPayload.Error(), sz, max)
	c.sendErr("Maximum Payload Violation")
	c.closeConnection(MaxPayloadExceeded)
}

// queueOutbound queues data for a clientconnection.
// Lock should be held.
func (c *client) queueOutbound(data []byte) {
	// Do not keep going if closed
	if c.isClosed() {
		return
	}

	// Add to pending bytes total.
	c.out.pb += int64(len(data))

	// Take a copy of the slice ref so that we can chop bits off the beginning
	// without affecting the original "data" slice.
	toBuffer := data

	// All of the queued []byte have a fixed capacity, so if there's a []byte
	// at the tail of the buffer list that isn't full yet, we should top that
	// up first. This helps to ensure we aren't pulling more []bytes from the
	// pool than we need to.
	if len(c.out.nb) > 0 {
		last := &c.out.nb[len(c.out.nb)-1]
		if free := cap(*last) - len(*last); free > 0 {
			if l := len(toBuffer); l < free {
				free = l
			}
			*last = append(*last, toBuffer[:free]...)
			toBuffer = toBuffer[free:]
		}
	}

	// Now we can push the rest of the data into new []bytes from the pool
	// in fixed size chunks. This ensures we don't go over the capacity of any
	// of the buffers and end up reallocating.
	for len(toBuffer) > 0 {
		new := nbPoolGet(len(toBuffer))
		n := copy(new[:cap(new)], toBuffer)
		c.out.nb = append(c.out.nb, new[:n])
		toBuffer = toBuffer[n:]
	}

	// Check for slow consumer via pending bytes limit.
	// ok to return here, client is going away.
	if c.kind == CLIENT && c.out.pb > c.out.mp {
		// Perf wise, it looks like it is faster to optimistically add than
		// checking current pb+len(data) and then add to pb.
		c.out.pb -= int64(len(data))

		// Increment the total and client's slow consumer counters.
		atomic.AddInt64(&c.srv.slowConsumers, 1)
		c.srv.scStats.clients.Add(1)
		if c.acc != nil {
			c.acc.stats.Lock()
			c.acc.stats.slowConsumers++
			c.acc.stats.Unlock()
		}
		c.Noticef("Slow Consumer Detected: MaxPending of %d Exceeded", c.out.mp)
		c.markConnAsClosed(SlowConsumerPendingBytes)
		return
	}

	// Check here if we should create a stall channel if we are falling behind.
	// We do this here since if we wait for consumer's writeLoop it could be
	// too late with large number of fan in producers.
	// If the outbound connection is > 75% of maximum pending allowed, create a stall gate.
	if c.out.pb > c.out.mp/4*3 && c.out.stc == nil {
		c.out.stc = make(chan struct{})
	}
}

// Assume the lock is held upon entry.
func (c *client) enqueueProtoAndFlush(proto []byte, doFlush bool) {
	if c.isClosed() {
		return
	}
	c.queueOutbound(proto)
	if !(doFlush && c.flushOutbound()) {
		c.flushSignal()
	}
}

// Queues and then flushes the connection. This should only be called when
// the writeLoop cannot be started yet. Use enqueueProto() otherwise.
// Lock is held on entry.
func (c *client) sendProtoNow(proto []byte) {
	c.enqueueProtoAndFlush(proto, true)
}

// Enqueues the given protocol and signal the writeLoop if necessary.
// Lock is held on entry.
func (c *client) enqueueProto(proto []byte) {
	c.enqueueProtoAndFlush(proto, false)
}

// Assume the lock is held upon entry.
func (c *client) sendPong() {
	if c.trace {
		c.traceOutOp("PONG", nil)
	}
	c.enqueueProto([]byte(pongProto))
}

// Used to kick off a RTT measurement for latency tracking.
func (c *client) sendRTTPing() bool {
	c.mu.Lock()
	sent := c.sendRTTPingLocked()
	c.mu.Unlock()
	return sent
}

// Used to kick off a RTT measurement for latency tracking.
// This is normally called only when the caller has checked that
// the c.rtt is 0 and wants to force an update by sending a PING.
// Client lock held on entry.
func (c *client) sendRTTPingLocked() bool {
	if c.isMqtt() {
		return false
	}
	// Most client libs send a CONNECT+PING and wait for a PONG from the
	// server. So if firstPongSent flag is set, it is ok for server to
	// send the PING. But in case we have client libs that don't do that,
	// allow the send of the PING if more than 2 secs have elapsed since
	// the client TCP connection was accepted.
	if !c.isClosed() &&
		(c.flags.isSet(firstPongSent) || time.Since(c.start) > maxNoRTTPingBeforeFirstPong) {
		c.sendPing()
		return true
	}
	return false
}

// Assume the lock is held upon entry.
func (c *client) sendPing() {
	c.rttStart = time.Now().UTC()
	c.ping.out++
	if c.trace {
		c.traceOutOp("PING", nil)
	}
	c.enqueueProto([]byte(pingProto))
}

// Generates the INFO to be sent to the client with the client ID included.
// info arg will be copied since passed by value.
// Assume lock is held.
func (c *client) generateClientInfoJSON(info Info, includeClientIP bool) []byte {
	info.CID = c.cid
	if includeClientIP {
		info.ClientIP = c.host
	}
	info.MaxPayload = atomic.LoadInt32(&c.mpay)
	if c.isWebsocket() {
		info.ClientConnectURLs = info.WSConnectURLs
		// Otherwise lame duck info can panic
		if c.srv != nil {
			ws := &c.srv.websocket
			info.TLSAvailable, info.TLSRequired = ws.tls, ws.tls
			info.Host, info.Port = ws.host, ws.port
		}
	}
	info.WSConnectURLs = nil
	return generateInfoJSON(&info)
}

func (c *client) sendErr(err string) {
	c.mu.Lock()
	if c.trace {
		c.traceOutOp("-ERR", []byte(err))
	}
	if !c.isMqtt() {
		c.enqueueProto([]byte(fmt.Sprintf(errProto, err)))
	}
	c.mu.Unlock()
}

func (c *client) sendOK() {
	c.mu.Lock()
	if c.trace {
		c.traceOutOp("OK", nil)
	}
	c.enqueueProto([]byte(okProto))
	c.mu.Unlock()
}

func (c *client) processPing() {
	c.mu.Lock()

	if c.isClosed() {
		c.mu.Unlock()
		return
	}

	c.sendPong()

	// Record this to suppress us sending one if this
	// is within a given time interval for activity.
	c.lastIn = time.Now()

	// If not a CLIENT, we are done. Also the CONNECT should
	// have been received, but make sure it is so before proceeding
	if c.kind != CLIENT || !c.flags.isSet(connectReceived) {
		c.mu.Unlock()
		return
	}

	// If we are here, the CONNECT has been received so we know
	// if this client supports async INFO or not.
	var (
		sendConnectInfo bool
		srv             = c.srv
	)
	// For the first PING (so firstPongSet is false) and for clients
	// that support async INFO protocols, we will send one with ConnectInfo=true,
	// the name of the account the client is bound to, and if the
	// account is the system account.
	if !c.flags.isSet(firstPongSent) {
		// Flip the flag.
		c.flags.set(firstPongSent)
		// Evaluate if we should send the INFO protocol.
		sendConnectInfo = srv != nil && c.opts.Protocol >= ClientProtoInfo
	}
	c.mu.Unlock()

	if sendConnectInfo {
		srv.mu.Lock()
		info := srv.copyInfo()
		c.mu.Lock()
		// Keep the in-process tls_required override from the initial INFO,
		// otherwise this async INFO would flip it back to true.
		if c.iproc && info.TLSRequired && !c.flags.isSet(didTLSFirst) {
			info.TLSRequired = false
			info.TLSAvailable = true
		}
		info.RemoteAccount = c.acc.Name
		info.IsSystemAccount = c.acc == srv.SystemAccount()
		info.ConnectInfo = true
		c.enqueueProto(c.generateClientInfoJSON(info, true))
		c.mu.Unlock()
		srv.mu.Unlock()
	}
}

func (c *client) processPong() {
	c.mu.Lock()
	c.ping.out = 0
	c.rtt = computeRTT(c.rttStart)
	srv := c.srv
	reorderGWs := c.kind == GATEWAY && c.gw.outbound
	var ri *routeInfo
	// For a route with pooling, we may be instructed to start a new route.
	if c.kind == ROUTER && c.route != nil && c.route.startNewRoute != nil {
		ri = c.route.startNewRoute
		c.route.startNewRoute = nil
	}
	// If compression is currently active for a route/leaf connection, if the
	// compression configuration is s2_auto, check if we should change
	// the compression level.
	if c.kind == ROUTER && needsCompression(c.route.compression) {
		c.updateS2AutoCompressionLevel(&srv.getOpts().Cluster.Compression, &c.route.compression)
	} else if c.kind == LEAF && needsCompression(c.leaf.compression) {
		var co *CompressionOpts
		if r := c.leaf.remote; r != nil {
			co = &r.Compression
		} else {
			co = &srv.getOpts().LeafNode.Compression
		}
		c.updateS2AutoCompressionLevel(co, &c.leaf.compression)
	}
	c.mu.Unlock()
	if reorderGWs {
		srv.gateway.orderOutboundConnections()
	}
	if ri != nil {
		srv.startGoRoutine(func() {
			srv.connectToRoute(ri.url, ri.rtype, true, ri.gossipMode, _EMPTY_)
		})
	}
}

// Select the s2 compression level based on the client's current RTT and the configured
// RTT thresholds slice. If current level is different than selected one, save the
// new compression level string and create a new s2 writer.
// Lock held on entry.
func (c *client) updateS2AutoCompressionLevel(co *CompressionOpts, compression *string) {
	if co.Mode != CompressionS2Auto {
		return
	}
	if cm := selectS2AutoModeBasedOnRTT(c.rtt, co.RTTThresholds); cm != *compression {
		*compression = cm
		c.out.cw = s2.NewWriter(nil, s2WriterOptions(cm)...)
	}
}

// Will return the parts from the raw wire msg.
// We return the `hdr` as a slice that is capped to the length of the headers
// so that if the caller later tries to append to the returned header slice it
// does not affect the message content.
func (c *client) msgParts(data []byte) (hdr []byte, msg []byte) {
	if c != nil && c.pa.hdr > 0 {
		return data[:c.pa.hdr:c.pa.hdr], data[c.pa.hdr:]
	}
	return nil, data
}

// Header pubs take form HPUB <subject> [reply] <hdr_len> <total_len>\r\n
func (c *client) processHeaderPub(arg, remaining []byte) error {
	if !c.headers {
		return ErrMsgHeadersNotSupported
	}

	// Unroll splitArgs to avoid runtime/heap issues
	a := [MAX_HPUB_ARGS][]byte{}
	args := a[:0]
	start := -1
	for i, b := range arg {
		switch b {
		case ' ', '\t':
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
	case 3:
		c.pa.subject = args[0]
		c.pa.reply = nil
		c.pa.hdr = parseSize(args[1])
		c.pa.size = parseSize(args[2])
		c.pa.hdb = args[1]
		c.pa.szb = args[2]
	case 4:
		c.pa.subject = args[0]
		c.pa.reply = args[1]
		c.pa.hdr = parseSize(args[2])
		c.pa.size = parseSize(args[3])
		c.pa.hdb = args[2]
		c.pa.szb = args[3]
	default:
		return fmt.Errorf("processHeaderPub Parse Error: %q", arg)
	}
	if c.pa.hdr < 0 {
		return fmt.Errorf("processHeaderPub Bad or Missing Header Size: %q", arg)
	}
	// If number overruns an int64, parseSize() will have returned a negative value
	if c.pa.size < 0 {
		return fmt.Errorf("processHeaderPub Bad or Missing Total Size: %q", arg)
	}
	if c.pa.hdr > c.pa.size {
		return fmt.Errorf("processHeaderPub Header Size larger then TotalSize: %q", arg)
	}
	maxPayload := atomic.LoadInt32(&c.mpay)
	// Use int64() to avoid int32 overrun...
	if maxPayload != jwt.NoLimit && int64(c.pa.size) > int64(maxPayload) {
		// If we are given the remaining read buffer (since we do blind reads
		// we may have the beginning of the message header/payload), we will
		// look for the tracing header and if found, we will generate a
		// trace event with the max payload ingress error.
		// Do this only for CLIENT connections.
		if c.kind == CLIENT && c.pa.hdr > 0 && len(remaining) > 0 {
			hdr := remaining[:min(len(remaining), c.pa.hdr)]
			c.sendMsgTraceIngressErrEvent(hdr, ErrMaxPayload)
		}
		c.maxPayloadViolation(c.pa.size, maxPayload)
		return ErrMaxPayload
	}
	if c.opts.Pedantic && !IsValidLiteralSubject(bytesToString(c.pa.subject)) {
		c.sendErr("Invalid Publish Subject")
	}
	return nil
}

func (c *client) processPub(arg []byte) error {
	// Unroll splitArgs to avoid runtime/heap issues
	a := [MAX_PUB_ARGS][]byte{}
	args := a[:0]
	start := -1
	for i, b := range arg {
		switch b {
		case ' ', '\t':
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
	case 2:
		c.pa.subject = args[0]
		c.pa.reply = nil
		c.pa.size = parseSize(args[1])
		c.pa.szb = args[1]
	case 3:
		c.pa.subject = args[0]
		c.pa.reply = args[1]
		c.pa.size = parseSize(args[2])
		c.pa.szb = args[2]
	default:
		return fmt.Errorf("processPub Parse Error: %q", arg)
	}
	// If number overruns an int64, parseSize() will have returned a negative value
	if c.pa.size < 0 {
		return fmt.Errorf("processPub Bad or Missing Size: %q", arg)
	}
	maxPayload := atomic.LoadInt32(&c.mpay)
	// Use int64() to avoid int32 overrun...
	if maxPayload != jwt.NoLimit && int64(c.pa.size) > int64(maxPayload) {
		c.maxPayloadViolation(c.pa.size, maxPayload)
		return ErrMaxPayload
	}
	if c.opts.Pedantic && !IsValidLiteralSubject(bytesToString(c.pa.subject)) {
		c.sendErr("Invalid Publish Subject")
	}
	return nil
}

func splitArg(arg []byte) [][]byte {
	a := [MAX_MSG_ARGS][]byte{}
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
	return args
}

func (c *client) parseSub(argo []byte, noForward bool) error {
	// Copy so we do not reference a potentially large buffer
	// FIXME(dlc) - make more efficient.
	arg := make([]byte, len(argo))
	copy(arg, argo)
	args := splitArg(arg)
	var (
		subject []byte
		queue   []byte
		sid     []byte
	)
	switch len(args) {
	case 2:
		subject = args[0]
		queue = nil
		sid = args[1]
	case 3:
		subject = args[0]
		queue = args[1]
		sid = args[2]
	default:
		return fmt.Errorf("processSub Parse Error: %q", arg)
	}
	// If there was an error, it has been sent to the client. We don't return an
	// error here to not close the connection as a parsing error.
	c.processSub(subject, queue, sid, nil, noForward)
	return nil
}

func (c *client) processSub(subject, queue, bsid []byte, cb msgHandler, noForward bool) (*subscription, error) {
	return c.processSubEx(subject, queue, bsid, cb, noForward, false, false)
}

func (c *client) processSubEx(subject, queue, bsid []byte, cb msgHandler, noForward, si, rsi bool) (*subscription, error) {
	// Create the subscription
	sub := &subscription{client: c, subject: subject, queue: queue, sid: bsid, icb: cb, si: si, rsi: rsi}

	c.mu.Lock()

	// Indicate activity.
	c.in.subs++

	// Grab connection type, account and server info.
	kind := c.kind
	acc := c.acc
	srv := c.srv

	sid := bytesToString(sub.sid)

	// This check does not apply to SYSTEM or JETSTREAM or ACCOUNT clients (because they don't have a `nc`...)
	// When a connection is closed though, we set c.subs to nil. So check for the map to not be nil.
	if (c.isClosed() && !isInternalClient(kind)) || (c.subs == nil) {
		c.mu.Unlock()
		return nil, ErrConnectionClosed
	}

	// Check permissions if applicable.
	if kind == CLIENT {
		// First do a pass whether queue subscription is valid. This does not necessarily
		// mean that it will not be able to plain subscribe.
		//
		// allow = ["foo"]            -> can subscribe or queue subscribe to foo using any queue
		// allow = ["foo v1"]         -> can only queue subscribe to 'foo v1', no plain subs allowed.
		// allow = ["foo", "foo v1"]  -> can subscribe to 'foo' but can only queue subscribe to 'foo v1'
		//
		if sub.queue != nil {
			if !c.canSubscribe(string(sub.subject), string(sub.queue)) || string(sub.queue) == sysGroup {
				c.mu.Unlock()
				c.subPermissionViolation(sub)
				return nil, ErrSubscribePermissionViolation
			}
		} else if !c.canSubscribe(string(sub.subject)) {
			c.mu.Unlock()
			c.subPermissionViolation(sub)
			return nil, ErrSubscribePermissionViolation
		}

		if opts := srv.getOpts(); opts != nil && opts.MaxSubTokens > 0 {
			if len(bytes.Split(sub.subject, []byte(tsep))) > int(opts.MaxSubTokens) {
				c.mu.Unlock()
				c.maxTokensViolation(sub)
				return nil, ErrTooManySubTokens
			}
		}
	}

	// Check if we have a maximum on the number of subscriptions.
	if c.subsAtLimit() {
		c.mu.Unlock()
		c.maxSubsExceeded()
		return nil, ErrTooManySubs
	}

	var updateGWs bool
	var err error

	// Subscribe here.
	es := c.subs[sid]
	if es == nil {
		c.subs[sid] = sub
		if acc != nil && acc.sl != nil {
			err = acc.sl.Insert(sub)
			if err != nil {
				delete(c.subs, sid)
			} else {
				updateGWs = c.srv.gateway.enabled
			}
		}
	}
	// Unlocked from here onward
	c.mu.Unlock()

	if err != nil {
		c.sendErr("Invalid Subject")
		return nil, ErrMalformedSubject
	} else if c.opts.Verbose && kind != SYSTEM {
		c.sendOK()
	}

	// If it was already registered, return it.
	if es != nil {
		return es, nil
	}

	// No account just return.
	if acc == nil {
		return sub, nil
	}

	if err := c.addShadowSubscriptions(acc, sub); err != nil {
		c.Errorf(err.Error())
	}

	if noForward {
		return sub, nil
	}

	// If we are routing and this is a local sub, add to the route map for the associated account.
	if kind == CLIENT || kind == SYSTEM || kind == JETSTREAM || kind == ACCOUNT {
		srv.updateRouteSubscriptionMap(acc, sub, 1)
		if updateGWs {
			srv.gatewayUpdateSubInterest(acc.Name, sub, 1)
		}
	}
	// Now check on leafnode updates.
	acc.updateLeafNodes(sub, 1)
	return sub, nil
}

// Used to pass stream import matches to addShadowSub
type ime struct {
	im          *streamImport
	overlapSubj string
	dyn         bool
}

// If the client's account has stream imports and there are matches for this
// subscription's subject, then add shadow subscriptions in the other accounts
// that export this subject.
func (c *client) addShadowSubscriptions(acc *Account, sub *subscription) error {
	if acc == nil {
		return ErrMissingAccount
	}

	var (
		_ims           [16]ime
		ims            = _ims[:0]
		imTsa          [32]string
		tokens         []string
		tsa            [32]string
		hasWC          bool
		tokensModified bool
	)

	acc.mu.RLock()
	// If this is from a service import, ignore.
	if sub.si {
		acc.mu.RUnlock()
		return nil
	}
	subj := bytesToString(sub.subject)
	if len(acc.imports.streams) > 0 {
		tokens = tokenizeSubjectIntoSlice(tsa[:0], subj)
		for _, tk := range tokens {
			if tk == pwcs {
				hasWC = true
				break
			}
		}
		if !hasWC && tokens[len(tokens)-1] == fwcs {
			hasWC = true
		}
	}
	// Loop over the import subjects. We have 4 scenarios. If we have an
	// exact match or a superset match we should use the from field from
	// the import. If we are a subset or overlap, we have to dynamically calculate
	// the subject. On overlap, ime requires the overlap subject.
	for _, im := range acc.imports.streams {
		if im.invalid {
			continue
		}
		if subj == im.to {
			ims = append(ims, ime{im, _EMPTY_, false})
			continue
		}
		if tokensModified {
			// re-tokenize subj to overwrite modifications from a previous iteration
			tokens = tokenizeSubjectIntoSlice(tsa[:0], subj)
			tokensModified = false
		}
		imTokens := tokenizeSubjectIntoSlice(imTsa[:0], im.to)

		if isSubsetMatchTokenized(tokens, imTokens) {
			ims = append(ims, ime{im, _EMPTY_, true})
		} else if hasWC {
			if isSubsetMatchTokenized(imTokens, tokens) {
				ims = append(ims, ime{im, _EMPTY_, false})
			} else {
				imTokensLen := len(imTokens)
				for i, t := range tokens {
					if i >= imTokensLen {
						break
					}
					if t == pwcs && imTokens[i] != fwcs {
						tokens[i] = imTokens[i]
						tokensModified = true
					}
				}
				tokensLen := len(tokens)
				lastIdx := tokensLen - 1
				if tokens[lastIdx] == fwcs {
					if imTokensLen >= tokensLen {
						// rewrite ">" in tokens to be more specific
						tokens[lastIdx] = imTokens[lastIdx]
						tokensModified = true
						if imTokensLen > tokensLen {
							// copy even more specific parts from import
							tokens = append(tokens, imTokens[tokensLen:]...)
						}
					}
				}
				if isSubsetMatchTokenized(tokens, imTokens) {
					// As isSubsetMatchTokenized was already called with tokens and imTokens,
					// we wouldn't be here if it where not for tokens being modified.
					// Hence, Join to re compute the subject string
					ims = append(ims, ime{im, strings.Join(tokens, tsep), true})
				}
			}
		}
	}
	acc.mu.RUnlock()

	var shadow []*subscription

	if len(ims) > 0 {
		shadow = make([]*subscription, 0, len(ims))
	}

	// Now walk through collected stream imports that matched.
	for i := 0; i < len(ims); i++ {
		ime := &ims[i]
		// We will create a shadow subscription.
		nsub, err := c.addShadowSub(sub, ime)
		if err != nil {
			return err
		}
		shadow = append(shadow, nsub)
	}

	if shadow != nil {
		c.mu.Lock()
		sub.shadow = shadow
		c.mu.Unlock()
	}

	return nil
}

// Add in the shadow subscription.
func (c *client) addShadowSub(sub *subscription, ime *ime) (*subscription, error) {
	c.mu.Lock()
	nsub := *sub // copy
	c.mu.Unlock()

	im := ime.im
	nsub.im = im
	// A leafnode subscription's origin cluster only applies to its own account. The shadow is interest in another
	// account, which that cluster's leaf links never saw, so it must not carry the origin to leafnodes or routes.
	nsub.origin = nil

	if !im.usePub && ime.dyn && im.tr != nil {
		if im.rtr == nil {
			im.rtr = im.tr.reverse()
		}
		s := bytesToString(nsub.subject)
		if ime.overlapSubj != _EMPTY_ {
			s = ime.overlapSubj
		}
		subj := im.rtr.TransformSubject(s)

		nsub.subject = []byte(subj)
	} else if !im.usePub || (im.usePub && ime.overlapSubj != _EMPTY_) || !ime.dyn {
		if ime.overlapSubj != _EMPTY_ {
			nsub.subject = []byte(ime.overlapSubj)
		} else {
			nsub.subject = []byte(im.from)
		}
	}
	// Else use original subject

	c.Debugf("Creating import subscription on %q from account %q", nsub.subject, im.acc.Name)

	if err := im.acc.sl.Insert(&nsub); err != nil {
		errs := fmt.Sprintf("Could not add shadow import subscription for account %q", im.acc.Name)
		c.Debugf(errs)
		return nil, errors.New(errs)
	}

	// Update our route map here. But only if we are not a leaf node or a hub leafnode.
	if c.kind != LEAF || c.isHubLeafNode() {
		c.srv.updateRemoteSubscription(im.acc, &nsub, 1)
	} else if c.kind == LEAF {
		// Update all leafnodes that connect to this server. Note that we could have
		// used the updateLeafNodes() function since when it does invoke updateSmap()
		// this function already takes care of not sending to a spoke leafnode since
		// the `nsub` here is already from a spoke leafnode, but to be explicit, we
		// use this version that updates only leafnodes that connect to this server.
		im.acc.updateLeafNodesEx(&nsub, 1, true)
	}

	return &nsub, nil
}

// canSubscribeInternal determines if the client is authorized to subscribe to
// the given subject. Assumes caller is holding at least a read lock.
func (c *client) canSubscribeInternal(subject string, optQueue ...string) bool {
	if c.perms == nil {
		return true
	}

	allowed, checkAllow := true, true

	// Optional queue group.
	var queue string
	if len(optQueue) > 0 {
		queue = optQueue[0]
	}

	// For CLIENT connections that are MQTT we will implicitly allow anything that starts with
	// the "$MQTT.sub." or "$MQTT.deliver.pubrel." prefix. For other non-LEAF connections, we
	// will implicitly allow anything that starts with the full "$MQTT." prefix. However,
	// we don't just return here, we skip the check for "allow" but will check "deny".
	if (c.isMqtt() && (strings.HasPrefix(subject, mqttSubPrefix) || strings.HasPrefix(subject, mqttPubRelDeliverySubjectPrefix))) ||
		(c.kind != CLIENT && c.kind != LEAF && strings.HasPrefix(subject, mqttPrefix)) {
		checkAllow = false
	}
	// Check allow list. If no allow list that means all are allowed. Deny can overrule.
	if checkAllow && c.perms.sub.allow != nil {
		r := c.perms.sub.allow.Match(subject)
		allowed = len(r.psubs) > 0
		if queue != _EMPTY_ && len(r.qsubs) > 0 {
			// If the queue appears in the allow list, then DO allow.
			allowed = queueMatches(queue, r.qsubs)
		}
		// Leafnodes operate slightly differently in that they allow broader scoped subjects.
		// They will prune based on publish perms before sending to a leafnode client.
		if !allowed && c.kind == LEAF && subjectHasWildcard(subject) {
			r := c.perms.sub.allow.ReverseMatch(subject)
			allowed = len(r.psubs) != 0
		}
	}
	// If we have a deny list and we think we are allowed, check that as well.
	if allowed && c.perms.sub.deny != nil {
		r := c.perms.sub.deny.Match(subject)
		allowed = len(r.psubs) == 0

		if allowed && queue != _EMPTY_ && len(r.qsubs) > 0 {
			// If the queue appears in the deny list, then DO NOT allow.
			allowed = !queueMatches(queue, r.qsubs)
		}
	}
	return allowed
}

// canSubscribe determines if the client is authorized to subscribe to the
// given subject and initializes the delivery-time deny filter when needed.
// Assumes caller is holding the write lock.
func (c *client) canSubscribe(subject string, optQueue ...string) bool {
	if !c.canSubscribeInternal(subject, optQueue...) {
		return false
	}
	c.loadMsgDenyFilterIfNeeded(subject, len(optQueue) > 0 && optQueue[0] != _EMPTY_)
	return true
}

// Initializes the delivery-time deny filter when a wildcard subscription, or
// an exact queue subscription, can overlap a deny entry. Assumes caller is
// holding the write lock.
func (c *client) loadMsgDenyFilterIfNeeded(subject string, hasQueue bool) {
	// A solicited leafnode's subscribe denies only limit what we receive.
	// Deliveries to it are checked against its publish perms in deliverMsg().
	if c.isSolicitedLeafNode() {
		return
	}
	// We use the actual subscription to signal us to spin up the deny mperms
	// and cache. We check if the subject is a wildcard that intersects any of
	// the deny clauses.
	// FIXME(dlc) - We could be smarter and track when these go away and remove.
	if c.mperms == nil && (hasQueue || subjectHasWildcard(subject)) {
		// Whip through the deny array and check if this wildcard subject can
		// overlap with any denied deliveries.
		for _, sub := range c.darray {
			if SubjectsCollide(bytesToString(sub.subject), subject) {
				c.loadMsgDenyFilter()
				break
			}
		}
	}
}

func queueMatches(queue string, qsubs [][]*subscription) bool {
	if len(qsubs) == 0 {
		return true
	}
	for _, qsub := range qsubs {
		qs := qsub[0]
		qname := bytesToString(qs.queue)

		// NOTE: '*' and '>' tokens can also be valid
		// queue names so we first check against the
		// literal name.  e.g. v1.* == v1.*
		if queue == qname || (subjectHasWildcard(qname) && subjectIsSubsetMatch(queue, qname)) {
			return true
		}
	}
	return false
}

// Low level unsubscribe for a given client.
func (c *client) unsubscribe(acc *Account, sub *subscription, force, remove bool) {
	if s := c.srv; s != nil && s.isShuttingDown() {
		return
	}

	c.mu.Lock()
	if !force && sub.max > 0 && sub.nm < sub.max {
		c.Debugf("Deferring actual UNSUB(%s): %d max, %d received", sub.subject, sub.max, sub.nm)
		c.mu.Unlock()
		return
	}

	if c.trace {
		c.traceOp("<-> %s", "DELSUB", sub.sid)
	}

	// Remove accounting if requested. This will be false when we close a connection
	// with open subscriptions.
	if remove {
		delete(c.subs, bytesToString(sub.sid))
		if acc != nil {
			acc.sl.Remove(sub)
		}
	}

	// Check to see if we have shadow subscriptions.
	var updateRoute bool
	var isSpokeLeaf bool
	shadowSubs := sub.shadow
	sub.shadow = nil
	if len(shadowSubs) > 0 {
		isSpokeLeaf = c.isSpokeLeafNode()
		updateRoute = !isSpokeLeaf && (c.kind == CLIENT || c.kind == SYSTEM || c.kind == LEAF || c.kind == JETSTREAM) && c.srv != nil
	}
	sub.close()
	c.mu.Unlock()

	// Process shadow subs if we have them.
	for _, nsub := range shadowSubs {
		if err := nsub.im.acc.sl.Remove(nsub); err != nil {
			c.Debugf("Could not remove shadow import subscription for account %q", nsub.im.acc.Name)
		}
		if updateRoute {
			c.srv.updateRemoteSubscription(nsub.im.acc, nsub, -1)
		} else if isSpokeLeaf {
			nsub.im.acc.updateLeafNodesEx(nsub, -1, true)
		}
	}

	// Now check to see if this was part of a respMap entry for service imports.
	// We can skip subscriptions on reserved replies.
	if acc != nil && !isReservedReply(sub.subject) {
		acc.checkForReverseEntry(string(sub.subject), nil, true)
	}
}

func (c *client) processUnsub(arg []byte) error {
	args := splitArg(arg)
	var sid []byte
	max := int64(-1)

	switch len(args) {
	case 1:
		sid = args[0]
	case 2:
		sid = args[0]
		max = int64(parseSize(args[1]))
	default:
		return fmt.Errorf("processUnsub Parse Error: %q", arg)
	}

	var sub *subscription
	var ok, unsub bool

	c.mu.Lock()

	// Indicate activity.
	c.in.subs++

	// Grab connection type.
	kind := c.kind
	srv := c.srv
	var acc *Account

	updateGWs := false
	if sub, ok = c.subs[string(sid)]; ok {
		acc = c.acc
		if max > 0 && max > sub.nm {
			sub.max = max
		} else {
			// Clear it here to override
			sub.max = 0
			unsub = true
		}
		updateGWs = srv.gateway.enabled
	}
	c.mu.Unlock()

	if c.opts.Verbose {
		c.sendOK()
	}

	if unsub {
		c.unsubscribe(acc, sub, false, true)
		if acc != nil && (kind == CLIENT || kind == SYSTEM || kind == ACCOUNT || kind == JETSTREAM) {
			srv.updateRouteSubscriptionMap(acc, sub, -1)
			if updateGWs {
				srv.gatewayUpdateSubInterest(acc.Name, sub, -1)
			}
		}
		// Now check on leafnode updates.
		acc.updateLeafNodes(sub, -1)
	}

	return nil
}

// checkDenySub will check if we are allowed to deliver this message in the
// presence of deny clauses for subscriptions. Deny clauses will not prevent
// larger scoped wildcard subscriptions, so we need to check at delivery time.
// Lock should be held.
func (c *client) checkDenySub(subject, queue string) bool {
	key := msgDenyKey{subject, queue}
	if denied, ok := c.mperms.dcache[key]; ok {
		return denied
	}
	r := c.mperms.deny.Match(subject)
	denied := len(r.psubs) != 0
	if !denied && queue != _EMPTY_ && len(r.qsubs) != 0 {
		denied = queueMatches(queue, r.qsubs)
	}
	c.mperms.dcache[key] = denied
	if len(c.mperms.dcache) > maxDenyPermCacheSize {
		c.pruneDenyCache()
	}
	return denied
}

// importTargetSubject returns the subject that will actually be delivered to
// this subscription. For a shadow subscription created by a stream import, the
// delivered subject is the import's local (post-transform) form, which can
// differ from the subject the message was published on in the exporting
// account. For any other subscription the subject is returned unchanged.
func (s *subscription) importTargetSubject(subj []byte) []byte {
	if s == nil || s.im == nil {
		return subj
	}
	if s.im.tr != nil {
		return []byte(s.im.tr.TransformSubject(bytesToString(subj)))
	}
	if !s.im.usePub {
		return []byte(s.im.to)
	}
	return subj
}

// Create a message header for routes or leafnodes. Header and origin cluster aware.
func (c *client) msgHeaderForRouteOrLeaf(subj, reply []byte, rt *routeTarget, acc *Account, noOrigin bool) []byte {
	hasHeader := c.pa.hdr > 0
	subclient := rt.sub.client
	canReceiveHeader := subclient.headers

	mh := c.msgb[:msgHeadProtoLen]
	kind := subclient.kind
	var lnoc bool

	if kind == ROUTER {
		// If we are coming from a leaf with an origin cluster we need to handle differently
		// if we can. We will send a route based LMSG which has origin cluster and headers
		// by default.
		if c.kind == LEAF && !noOrigin && c.remoteCluster() != _EMPTY_ {
			subclient.mu.Lock()
			lnoc = subclient.route.lnoc
			subclient.mu.Unlock()
		}
		if lnoc {
			mh[0] = 'L'
			mh = append(mh, c.remoteCluster()...)
			mh = append(mh, ' ')
		} else {
			// Router (and Gateway) nodes are RMSG. Set here since leafnodes may rewrite.
			mh[0] = 'R'
		}
		if len(subclient.route.accName) == 0 {
			mh = append(mh, acc.Name...)
			mh = append(mh, ' ')
		}
	} else {
		// Leaf nodes are LMSG
		mh[0] = 'L'
		// Remap subject if its a shadow subscription, treat like a normal client.
		subj = rt.sub.importTargetSubject(subj)
	}
	mh = append(mh, subj...)
	mh = append(mh, ' ')

	if len(rt.qs) > 0 {
		if len(reply) > 0 {
			mh = append(mh, "+ "...) // Signal that there is a reply.
			mh = append(mh, reply...)
			mh = append(mh, ' ')
		} else {
			mh = append(mh, "| "...) // Only queues
		}
		mh = append(mh, rt.qs...)
	} else if len(reply) > 0 {
		mh = append(mh, reply...)
		mh = append(mh, ' ')
	}

	if lnoc {
		// leafnode origin LMSG always have a header entry even if zero.
		if c.pa.hdr <= 0 {
			mh = append(mh, '0')
		} else {
			mh = append(mh, c.pa.hdb...)
		}
		mh = append(mh, ' ')
		mh = append(mh, c.pa.szb...)
	} else if hasHeader {
		if canReceiveHeader {
			mh[0] = 'H'
			mh = append(mh, c.pa.hdb...)
			mh = append(mh, ' ')
			mh = append(mh, c.pa.szb...)
		} else {
			// If we are here we need to truncate the payload size
			nsz := strconv.Itoa(c.pa.size - c.pa.hdr)
			mh = append(mh, nsz...)
		}
	} else {
		mh = append(mh, c.pa.szb...)
	}
	return append(mh, _CRLF_...)
}

// Create a message header for clients. Header aware.
func (c *client) msgHeader(subj, reply []byte, sub *subscription) []byte {
	// See if we should do headers. We have to have a headers msg and
	// the client we are going to deliver to needs to support headers as well.
	hasHeader := c.pa.hdr > 0
	canReceiveHeader := sub.client != nil && sub.client.headers

	var mh []byte
	if hasHeader && canReceiveHeader {
		mh = c.msgb[:msgHeadProtoLen]
		mh[0] = 'H'
	} else {
		mh = c.msgb[1:msgHeadProtoLen]
	}
	mh = append(mh, subj...)
	mh = append(mh, ' ')

	if len(sub.sid) > 0 {
		mh = append(mh, sub.sid...)
		mh = append(mh, ' ')
	}
	if reply != nil {
		mh = append(mh, reply...)
		mh = append(mh, ' ')
	}
	if hasHeader {
		if canReceiveHeader {
			mh = append(mh, c.pa.hdb...)
			mh = append(mh, ' ')
			mh = append(mh, c.pa.szb...)
		} else {
			// If we are here we need to truncate the payload size
			nsz := strconv.Itoa(c.pa.size - c.pa.hdr)
			mh = append(mh, nsz...)
		}
	} else {
		mh = append(mh, c.pa.szb...)
	}
	mh = append(mh, _CRLF_...)
	return mh
}

func (c *client) stalledWait(producer *client) {
	// Check to see if we have exceeded our total wait time per readLoop invocation.
	if producer.in.tst > stallTotalAllowed {
		return
	}

	// Grab stall channel which the slow consumer will close when caught up.
	stall := c.out.stc

	// Calculate stall time.
	ttl := stallClientMinDuration
	if c.out.pb >= c.out.mp {
		ttl = stallClientMaxDuration
	}

	c.mu.Unlock()
	defer c.mu.Lock()

	// Track per client and total client stalls.
	atomic.AddInt64(&c.stalls, 1)
	if c.srv != nil {
		atomic.AddInt64(&c.srv.stalls, 1)
	}

	// Now check if we are close to total allowed.
	if producer.in.tst+ttl > stallTotalAllowed {
		ttl = stallTotalAllowed - producer.in.tst
	}
	delay := time.NewTimer(ttl)
	defer delay.Stop()

	start := time.Now()
	select {
	case <-stall:
	case <-delay.C:
		producer.Debugf("Timed out of fast producer stall (%v)", ttl)
	}
	producer.in.tst += time.Since(start)
}

// Used to treat maps as efficient set
var needFlush = struct{}{}

// deliverMsg will deliver a message to a matching subscription and its underlying client.
// We process all connection/client types. mh is the part that will be protocol/client specific.
func (c *client) deliverMsg(prodIsMQTT bool, sub *subscription, acc *Account, subject, reply, mh, msg []byte, gwrply bool) bool {
	// Check if message tracing is enabled.
	mt, traceOnly := c.isMsgTraceEnabled()

	client := sub.client
	// Check sub client and check echo. Only do this if not a service import.
	if client == nil || (c == client && !client.echo && !sub.si && !sub.rsi) {
		if client != nil && mt != nil {
			client.mu.Lock()
			mt.addEgressEvent(client, sub, errMsgTraceNoEcho)
			client.mu.Unlock()
		}
		return false
	}

	client.mu.Lock()

	// Check if we have a subscribe deny clause. This will trigger us to check the subject
	// for a match against the denied subjects.
	if client.mperms != nil && client.checkDenySub(string(subject), bytesToString(sub.queue)) {
		mt.addEgressEvent(client, sub, errMsgTraceSubDeny)
		client.mu.Unlock()
		return false
	}

	// New race detector forces this now.
	if sub.isClosed() {
		mt.addEgressEvent(client, sub, errMsgTraceSubClosed)
		client.mu.Unlock()
		return false
	}

	// Check if we are a leafnode and have perms to check.
	if client.kind == LEAF && client.perms != nil {
		// For a shadow subscription created by a stream import, the subject that
		// goes on the wire is the import's local (post-transform) form, see
		// msgHeaderForRouteOrLeaf() above. Check the permissions against that
		// form: the exporting account's subject is never sent to the leafnode,
		// and requiring a permission for it would grant the leafnode an
		// unrelated capability in its own account.
		// The import transform is applied first and an internal gateway reply
		// prefix is stripped from its result, because the header path transforms
		// the complete subject as well. Stripping first could authorize a
		// subject that differs from the one placed on the wire.
		subjectToCheck, _ := getGWRoutedSubjectOrSelf(sub.importTargetSubject(subject))
		if !client.pubAllowedFullCheck(string(subjectToCheck), true, true) {
			mt.addEgressEvent(client, sub, errMsgTracePubViolation)
			client.mu.Unlock()
			client.Debugf("Not permitted to deliver to %q", subjectToCheck)
			return false
		}
	}

	var mtErr string
	if mt != nil {
		// For non internal subscription, and if the remote does not support
		// the tracing feature...
		if sub.icb == nil && !client.msgTraceSupport() {
			if traceOnly {
				// We are not sending the message at all because the user
				// expects a trace-only and the remote does not support
				// tracing, which means that it would process/deliver this
				// message, which may break applications.
				// Add the Egress with the no-support error message.
				mt.addEgressEvent(client, sub, errMsgTraceOnlyNoSupport)
				client.mu.Unlock()
				return false
			}
			// If we are doing delivery, we will still forward the message,
			// but we add an error to the Egress event to hint that one should
			// not expect a tracing event from that remote.
			mtErr = errMsgTraceNoSupport
		}
		// For ROUTER, GATEWAY and LEAF, even if we intend to do tracing only,
		// we will still deliver the message. The remote side will
		// generate an event based on what happened on that server.
		if traceOnly && (client.kind == ROUTER || client.kind == GATEWAY || client.kind == LEAF) {
			traceOnly = false
		}
		// If we skip delivery and this is not for a service import, we are done.
		if traceOnly && (sub.icb == nil || c.noIcb) {
			mt.addEgressEvent(client, sub, _EMPTY_)
			client.mu.Unlock()
			// Although the message is not actually delivered, for the
			// purpose of "didDeliver", we need to return "true" here.
			return true
		}
	}

	srv := client.srv

	// We don't want to bump the number of delivered messages to the subscription
	// if we are doing trace-only (since really we are not sending it to the sub).
	if !traceOnly {
		sub.nm++
	}

	// Check if we should auto-unsubscribe.
	if sub.max > 0 {
		if client.kind == ROUTER && sub.nm >= sub.max {
			// The only router based messages that we will see here are remoteReplies.
			// We handle these slightly differently.
			defer client.removeReplySub(sub)
		} else {
			// For routing..
			shouldForward := client.kind == CLIENT || client.kind == SYSTEM && client.srv != nil
			// If we are at the exact number, unsubscribe but
			// still process the message in hand, otherwise
			// unsubscribe and drop message on the floor.
			if sub.nm == sub.max {
				client.Debugf("Auto-unsubscribe limit of %d reached for sid '%s'", sub.max, sub.sid)
				// Due to defer, reverse the code order so that execution
				// is consistent with other cases where we unsubscribe.
				if shouldForward {
					defer srv.updateRemoteSubscription(client.acc, sub, -1)
				}
				defer client.unsubscribe(client.acc, sub, true, true)
			} else if sub.nm > sub.max {
				client.Debugf("Auto-unsubscribe limit [%d] exceeded", sub.max)
				mt.addEgressEvent(client, sub, errMsgTraceAutoSubExceeded)
				client.mu.Unlock()
				client.unsubscribe(client.acc, sub, true, true)
				if shouldForward {
					srv.updateRemoteSubscription(client.acc, sub, -1)
				}
				return false
			}
		}
	}

	// Check here if we have a header with our message. If this client can not
	// support we need to strip the headers from the payload.
	// The actual header would have been processed correctly for us, so just
	// need to update payload.
	hdrSize := c.pa.hdr
	if c.pa.hdr > 0 && !sub.client.headers {
		msg = msg[c.pa.hdr:]
	}

	// Update statistics

	// The msg includes the CR_LF, so pull back out for accounting.
	msgSize := int64(len(msg))
	// MQTT producers send messages without CR_LF, so don't remove it for them.
	if !prodIsMQTT {
		msgSize -= int64(LEN_CR_LF)
	}

	// We do not update the outbound stats if we are doing trace only since
	// this message will not be sent out.
	// Also do not update on internal callbacks.
	if !traceOnly && sub.icb == nil {
		// No atomic needed since accessed under client lock.
		// Monitor is reading those also under client's lock.
		client.outMsgs++
		client.outBytes += msgSize
	}

	// Check for internal subscriptions.
	if sub.icb != nil && !c.noIcb {
		if gwrply {
			// We will store in the account, not the client since it will likely
			// be a different client that will send the reply.
			srv.trackGWReply(nil, client.acc, reply, c.pa.reply)
		}
		client.mu.Unlock()

		// For service imports, track if we delivered.
		didDeliver := true

		// Internal account clients are for service imports and need the '\r\n'.
		start := time.Now()
		if client.kind == ACCOUNT {
			sub.icb(sub, c, acc, string(subject), string(reply), msg)
			// If we are a service import check to make sure we delivered the message somewhere.
			if sub.si {
				didDeliver = c.pa.delivered
			}
		} else {
			sub.icb(sub, c, acc, string(subject), string(reply), msg[:msgSize])
		}
		if dur := time.Since(start); dur >= readLoopReportThreshold {
			srv.Warnf("Internal subscription on %q took too long: %v", subject, dur)
		}

		return didDeliver
	}

	// If we are a client and we detect that the consumer we are
	// sending to is in a stalled state, go ahead and wait here
	// with a limit.
	if c.kind == CLIENT && client.out.stc != nil {
		if srv.getOpts().NoFastProducerStall {
			mt.addEgressEvent(client, sub, errMsgTraceFastProdNoStall)
			client.mu.Unlock()
			return false
		}
		client.stalledWait(c)
	}

	// Check for closed connection
	if client.isClosed() {
		mt.addEgressEvent(client, sub, errMsgTraceClientClosed)
		client.mu.Unlock()
		return false
	}

	// We have passed cases where we could possibly fail to deliver.
	// Do not call for service-import.
	if mt != nil && sub.icb == nil {
		mt.addEgressEvent(client, sub, mtErr)
	}

	// Do a fast check here to see if we should be tracking this from a latency
	// perspective. This will be for a request being received for an exported service.
	// This needs to be from a non-client (otherwise tracking happens at requestor).
	//
	// Also this check captures if the original reply (c.pa.reply) is a GW routed
	// reply (since it is known to be > minReplyLen). If that is the case, we need to
	// track the binding between the routed reply and the reply set in the message
	// header (which is c.pa.reply without the _GR_ routing prefix).
	if client.kind == CLIENT && len(c.pa.reply) > minReplyLen {
		if gwrply {
			// Note that we keep track of the GW routed reply in the destination
			// connection (`client`). The routed reply subject is in `c.pa.reply`,
			// should that change, we would have to pass the GW routed reply as
			// a parameter of deliverMsg().
			srv.trackGWReply(client, nil, reply, c.pa.reply)
		}

		// If we do not have a registered RTT queue that up now.
		if client.rtt == 0 {
			client.sendRTTPingLocked()
		}
		// FIXME(dlc) - We may need to optimize this.
		// We will have tagged this with a suffix ('.T') if we are tracking. This is
		// needed from sampling. Not all will be tracked.
		if c.kind != CLIENT && isTrackedReply(c.pa.reply) {
			client.trackRemoteReply(string(subject), string(c.pa.reply))
		}
	}

	// Queue to outbound buffer
	client.queueOutbound(mh)
	client.queueOutbound(msg)
	if prodIsMQTT {
		// Need to add CR_LF since MQTT producers don't send CR_LF
		client.queueOutbound([]byte(CR_LF))
	}

	// If we are tracking dynamic publish permissions that track reply subjects,
	// do that accounting here. We only look at client.replies which will be non-nil.
	// Only reply subject permissions if the client is not already allowed to publish to the reply subject.
	if client.replies != nil && len(reply) > 0 && !client.pubAllowedFullCheck(string(reply), true, true) {
		client.replies[string(reply)] = &resp{time.Now(), 0}
		client.repliesSincePrune++
		if client.repliesSincePrune > replyPermLimit || time.Since(client.lastReplyPrune) > replyPruneTime {
			client.pruneReplyPerms()
		}
	}

	// Check outbound threshold and queue IO flush if needed.
	// This is specifically looking at situations where we are getting behind and may want
	// to intervene before this producer goes back to top of readloop. We are in the producer's
	// readloop go routine at this point.
	// FIXME(dlc) - We may call this alot, maybe suppress after first call?
	if len(client.out.nb) != 0 {
		client.flushSignal()
	}

	// Add the data size we are responsible for here. This will be processed when we
	// return to the top of the readLoop.
	c.addToPCD(client)

	if client.trace {
		client.traceOutOp(bytesToString(mh[:len(mh)-LEN_CR_LF]), nil)
		client.traceMsgDelivery(msg, hdrSize)
	}

	client.mu.Unlock()

	return true
}

// Add the given sub's client to the list of clients that need flushing.
// This must be invoked from `c`'s readLoop. No lock for c is required,
// however, `client` lock must be held on entry. This holds true even
// if `client` is same than `c`.
func (c *client) addToPCD(client *client) {
	if _, ok := c.pcd[client]; !ok {
		client.out.fsp++
		c.pcd[client] = needFlush
	}
}

// This will track a remote reply for an exported service that has requested
// latency tracking.
// Lock assumed to be held.
func (c *client) trackRemoteReply(subject, reply string) {
	a := c.acc
	if a == nil {
		return
	}

	var lrt time.Duration
	var respThresh time.Duration

	a.mu.RLock()
	se := a.getServiceExport(subject)
	if se != nil {
		lrt = a.lowestServiceExportResponseTime()
		respThresh = se.respThresh
	}
	a.mu.RUnlock()

	if se == nil {
		return
	}

	if c.rrTracking == nil {
		c.rrTracking = &rrTracking{
			rmap: make(map[string]*remoteLatency),
			ptmr: time.AfterFunc(lrt, c.pruneRemoteTracking),
			lrt:  lrt,
		}
	}
	rl := remoteLatency{
		Account:    a.Name,
		ReqId:      reply,
		respThresh: respThresh,
	}
	rl.M2.RequestStart = time.Now().UTC()
	c.rrTracking.rmap[reply] = &rl
}

// pruneRemoteTracking will prune any remote tracking objects
// that are too old. These are orphaned when a service is not
// sending reponses etc.
// Lock should be held upon entry.
func (c *client) pruneRemoteTracking() {
	c.mu.Lock()
	if c.rrTracking == nil {
		c.mu.Unlock()
		return
	}
	now := time.Now()
	for subject, rl := range c.rrTracking.rmap {
		if now.After(rl.M2.RequestStart.Add(rl.respThresh)) {
			delete(c.rrTracking.rmap, subject)
		}
	}
	if len(c.rrTracking.rmap) > 0 {
		t := c.rrTracking.ptmr
		t.Stop()
		t.Reset(c.rrTracking.lrt)
	} else {
		c.rrTracking.ptmr.Stop()
		c.rrTracking = nil
	}
	c.mu.Unlock()
}

// pruneReplyPerms will remove any stale or expired entries
// in our reply cache. We make sure to not check too often.
func (c *client) pruneReplyPerms() {
	// Make sure we do not check too often.
	if c.perms.resp == nil {
		return
	}

	mm := c.perms.resp.MaxMsgs
	ttl := c.perms.resp.Expires
	now := time.Now()

	for k, resp := range c.replies {
		if mm > 0 && resp.n >= mm {
			delete(c.replies, k)
		} else if ttl > 0 && now.Sub(resp.t) > ttl {
			delete(c.replies, k)
		}
	}

	c.repliesSincePrune = 0
	c.lastReplyPrune = now
}

// pruneDenyCache will prune the deny cache via randomly
// deleting items. Doing so pruneSize items at a time.
// Lock must be held for this one since it is shared under
// deliverMsg.
func (c *client) pruneDenyCache() {
	r := 0
	for key := range c.mperms.dcache {
		delete(c.mperms.dcache, key)
		if r++; r > pruneSize {
			break
		}
	}
}

// prunePubPermsCache will prune the cache via randomly
// deleting items. Doing so pruneSize items at a time.
func (c *client) prunePubPermsCache() {
	// With parallel additions to the cache, it is possible that this function
	// would not be able to reduce the cache to its max size in one go. We
	// will try a few times but will release/reacquire the "lock" at each
	// attempt to give a chance to another go routine to take over and not
	// have this go routine do too many attempts.
	for i := 0; i < 5; i++ {
		// There is a case where we can invoke this from multiple go routines,
		// (in deliverMsg() if sub.client is a LEAF), so we make sure to prune
		// from only one go routine at a time.
		if !atomic.CompareAndSwapInt32(&c.perms.prun, 0, 1) {
			return
		}
		const maxPruneAtOnce = 1000
		r := 0
		c.perms.pcache.Range(func(k, _ any) bool {
			c.perms.pcache.Delete(k)
			if r++; (r > pruneSize && atomic.LoadInt32(&c.perms.pcsz) < int32(maxPermCacheSize)) ||
				(r > maxPruneAtOnce) {
				return false
			}
			return true
		})
		n := atomic.AddInt32(&c.perms.pcsz, -int32(r))
		atomic.StoreInt32(&c.perms.prun, 0)
		if n <= int32(maxPermCacheSize) {
			return
		}
	}
}

// pubAllowed checks on publish permissioning.
// Lock should not be held.
func (c *client) pubAllowed(subject string) bool {
	return c.pubAllowedFullCheck(subject, true, false)
}

// pubAllowedFullCheck checks on all publish permissioning depending
// on the flag for dynamic reply permissions.
func (c *client) pubAllowedFullCheck(subject string, fullCheck, hasLock bool) bool {
	if c.perms == nil || (c.perms.pub.allow == nil && c.perms.pub.deny == nil) {
		return true
	}
	// Check if published subject is allowed if we have permissions in place.
	v, ok := c.perms.pcache.Load(subject)
	if ok {
		return v.(bool)
	}
	allowed, checkAllow := true, true
	// For any connections, other than CLIENT and LEAF, we will implicitly allow anything that
	// starts with the "$MQTT." prefix. However, we don't just return here,
	// we skip the check for "allow" but will check "deny".
	if c.kind != CLIENT && c.kind != LEAF && strings.HasPrefix(subject, mqttPrefix) {
		checkAllow = false
	}
	// Cache miss, check allow then deny as needed.
	if checkAllow && c.perms.pub.allow != nil {
		np, _ := c.perms.pub.allow.NumInterest(subject)
		allowed = np != 0
	}
	// If we have a deny list and are currently allowed, check that as well.
	if allowed && c.perms.pub.deny != nil {
		np, _ := c.perms.pub.deny.NumInterest(subject)
		allowed = np == 0
	}

	// If we are tracking reply subjects
	// dynamically, check to see if we are allowed here but avoid pcache.
	// We need to acquire the lock though.
	if !allowed && fullCheck && c.perms.resp != nil {
		if !hasLock {
			c.mu.Lock()
		}
		allowed = c.responseAllowed(subject)
		if !hasLock {
			c.mu.Unlock()
		}
	} else {
		// Update our cache here.
		c.perms.pcache.Store(subject, allowed)
		if n := atomic.AddInt32(&c.perms.pcsz, 1); n > maxPermCacheSize {
			c.prunePubPermsCache()
		}
	}
	return allowed
}

// Returns true if this subject matches a tracked dynamic reply permission.
// Lock must be held.
func (c *client) responseAllowed(subject string) bool {
	if c.perms == nil || c.perms.resp == nil {
		return false
	}
	if resp := c.replies[subject]; resp != nil {
		resp.n++
		if c.perms.resp.MaxMsgs > 0 && resp.n > c.perms.resp.MaxMsgs {
			delete(c.replies, subject)
		} else if c.perms.resp.Expires > 0 && time.Since(resp.t) > c.perms.resp.Expires {
			delete(c.replies, subject)
		} else {
			return true
		}
	}
	return false
}

// Test whether a reply subject is a service import reply.
func isServiceReply(reply []byte) bool {
	// This function is inlined and checking this way is actually faster
	// than byte-by-byte comparison.
	return len(reply) > 3 && bytesToString(reply[:4]) == replyPrefix
}

// Test whether a subject is a JetStream ACK.
func isJSAckSubject(subject []byte) bool {
	return len(subject) > jsAckPreLen && bytesToString(subject[:jsAckPreLen]) == jsAckPre
}

// Test whether a message is a JetStream pull request, either directly or with an ack.
func isJSPullRequest(subject string, payload []byte) bool {
	return strings.HasPrefix(subject, jsRequestNextPre) ||
		(isJSAckSubject(stringToBytes(subject)) && bytes.HasPrefix(payload, AckNext))
}

// jsAckDeliverIdx returns the byte offset of the `@` separator in an encoded
// `$JS.ACK....@<deliver>` reply, or -1 if reply is not in that form. Stream,
// consumer, and subject tokens may legally contain `@`, so we accept only the
// first `@` that follows the eight dots of the JS ACK token:
//
//	$JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>@<deliver>
func jsAckDeliverIdx(reply []byte) int {
	if !isJSAckSubject(reply) {
		return -1
	}
	dots := 0
	for i, b := range reply {
		switch b {
		case '.':
			dots++
		case '@':
			if dots >= 8 {
				return i
			}
		}
	}
	return -1
}

// replyHasJSAckSuffix reports whether reply is already in `$JS.ACK....@<deliver>`
// form, so callers don't double-append the suffix on a re-entrant pass
// (service-import or chained JS push).
func replyHasJSAckSuffix(reply []byte) bool {
	return jsAckDeliverIdx(reply) != -1
}

// Test whether a reply subject is a service import or a gateway routed reply.
func isReservedReply(reply []byte) bool {
	if isServiceReply(reply) {
		return true
	}
	// Faster to check with string([:]) than byte-by-byte
	if isJSAckSubject(reply) {
		return true
	}
	return hasGWRoutedReplyPrefix(reply)
}

// This will decide to call the client code or router code.
func (c *client) processInboundMsg(msg []byte) {
	switch c.kind {
	case CLIENT:
		c.processInboundClientMsg(msg)
	case ROUTER:
		c.processInboundRoutedMsg(msg)
	case GATEWAY:
		c.processInboundGatewayMsg(msg)
	case LEAF:
		c.processInboundLeafMsg(msg)
	}
}

// selectMappedSubject will choose the mapped subject based on the client's inbound subject.
func (c *client) selectMappedSubject() bool {
	nsubj, changed := c.acc.selectMappedSubject(bytesToString(c.pa.subject))
	if changed {
		c.pa.mapped = c.pa.subject
		c.pa.subject = []byte(nsubj)
	}
	return changed
}

// clientNRGPrefix is used in processInboundClientMsg to detect if publishes
// are being made from normal clients to NRG subjects.
var clientNRGPrefix = []byte("$NRG.")

// processInboundClientMsg is called to process an inbound msg from a client.
// Return if the message was delivered, and if the message was not delivered
// due to a permission issue.
func (c *client) processInboundClientMsg(msg []byte) (bool, bool) {
	// Update statistics
	// The msg includes the CR_LF, so pull back out for accounting.
	c.in.msgs++
	c.in.bytes += int32(len(msg) - LEN_CR_LF)

	// Check that client (could be here with SYSTEM) is not publishing on reserved "_GR_" or legacy "$GR" prefix.
	if c.kind == CLIENT && hasGWRoutedReplyPrefix(c.pa.subject) {
		c.pubPermissionViolation(c.pa.subject)
		return false, true
	}

	// Mostly under testing scenarios.
	c.mu.Lock()
	if c.srv == nil || c.acc == nil {
		c.mu.Unlock()
		return false, false
	}
	acc := c.acc
	genidAddr := &acc.sl.genid

	// Check pub permissions
	if c.perms != nil && (c.perms.pub.allow != nil || c.perms.pub.deny != nil) {
		if !c.pubAllowedFullCheck(string(c.pa.subject), true, true) {
			c.mu.Unlock()
			c.pubPermissionViolation(c.pa.subject)
			return false, true
		}
	}
	c.mu.Unlock()

	// Check if the client is trying to publish to reserved NRG subjects.
	// Doesn't apply to NRGs themselves as they use SYSTEM-kind clients instead.
	if c.kind == CLIENT && bytes.HasPrefix(c.pa.subject, clientNRGPrefix) && acc != c.srv.SystemAccount() {
		c.pubPermissionViolation(c.pa.subject)
		return false, true
	}

	// Now check for reserved replies. These are used for service imports.
	if c.kind == CLIENT && len(c.pa.reply) > 0 && isReservedReply(c.pa.reply) {
		c.replySubjectViolation(c.pa.reply)
		return false, true
	}

	if c.opts.Verbose {
		c.sendOK()
	}

	// If MQTT client, check for retain flag now that we have passed permissions check
	if c.isMqtt() {
		c.mqttHandlePubRetain()
	}

	// Doing this inline as opposed to create a function (which otherwise has a measured
	// performance impact reported in our bench)
	var isGWRouted bool
	if c.kind != CLIENT {
		if atomic.LoadInt32(&acc.gwReplyMapping.check) > 0 {
			acc.mu.RLock()
			c.pa.subject, isGWRouted = acc.gwReplyMapping.get(c.pa.subject)
			acc.mu.RUnlock()
		}
	} else if atomic.LoadInt32(&c.gwReplyMapping.check) > 0 {
		c.mu.Lock()
		c.pa.subject, isGWRouted = c.gwReplyMapping.get(c.pa.subject)
		c.mu.Unlock()
	}

	// If we have an exported service and we are doing remote tracking, check this subject
	// to see if we need to report the latency.
	if c.rrTracking != nil {
		c.mu.Lock()
		rl := c.rrTracking.rmap[string(c.pa.subject)]
		if rl != nil {
			delete(c.rrTracking.rmap, bytesToString(c.pa.subject))
		}
		c.mu.Unlock()

		if rl != nil {
			sl := &rl.M2
			// Fill this in and send it off to the other side.
			sl.Status = 200
			sl.Responder = c.getClientInfo(true)
			sl.ServiceLatency = time.Since(sl.RequestStart) - sl.Responder.RTT
			sl.TotalLatency = sl.ServiceLatency + sl.Responder.RTT
			sanitizeLatencyMetric(sl)
			lsub := remoteLatencySubjectForResponse(c.pa.subject)
			c.srv.sendInternalAccountMsg(nil, lsub, rl) // Send to SYS account
		}
	}

	// If the subject was converted to the gateway routed subject, then handle it now
	// and be done with the rest of this function.
	if isGWRouted {
		c.handleGWReplyMap(msg)
		return true, false
	}

	// Match the subscriptions. We will use our own L1 map if
	// it's still valid, avoiding contention on the shared sublist.
	var r *SublistResult
	var ok bool

	genid := atomic.LoadUint64(genidAddr)
	if genid == c.in.genid && c.in.results != nil {
		r, ok = c.in.results[string(c.pa.subject)]
	} else {
		// Reset our L1 completely.
		c.in.results = make(map[string]*SublistResult)
		c.in.genid = genid
	}

	// Go back to the sublist data structure.
	if !ok {
		// Match may use the subject here to populate a cache, so can not use bytesToString here.
		r = acc.sl.Match(string(c.pa.subject))
		if len(r.psubs)+len(r.qsubs) > 0 {
			// Prune the results cache. Keeps us from unbounded growth. Random delete.
			if len(c.in.results) >= maxResultCacheSize {
				n := 0
				for subject := range c.in.results {
					delete(c.in.results, subject)
					if n++; n > pruneSize {
						break
					}
				}
			}
			// Then add the new cache entry.
			c.in.results[string(c.pa.subject)] = r
		}
	}

	// Indication if we attempted to deliver the message to anyone.
	var didDeliver bool
	var qnames [][]byte

	// Check for no interest, short circuit if so.
	// This is the fanout scale.
	if len(r.psubs)+len(r.qsubs) > 0 {
		flag := pmrNoFlag
		// If there are matching queue subs and we are in gateway mode,
		// we need to keep track of the queue names the messages are
		// delivered to. When sending to the GWs, the RMSG will include
		// those names so that the remote clusters do not deliver messages
		// to their queue subs of the same names.
		if len(r.qsubs) > 0 && c.srv.gateway.enabled &&
			atomic.LoadInt64(&c.srv.gateway.totalQSubs) > 0 {
			flag |= pmrCollectQueueNames
		}
		didDeliver, qnames = c.processMsgResults(acc, r, msg, c.pa.deliver, c.pa.subject, c.pa.reply, flag)
	}

	// Now deal with gateways
	if c.srv.gateway.enabled {
		reply := c.pa.reply
		if len(c.pa.deliver) > 0 && c.kind == JETSTREAM && len(reply) > 0 && !replyHasJSAckSuffix(reply) {
			reply = append(slices.Clip(reply), '@')
			reply = append(reply, c.pa.deliver...)
		}
		didDeliver = c.sendMsgToGateways(acc, msg, c.pa.subject, reply, qnames, false) || didDeliver
	}

	// Check to see if we did not deliver to anyone and the client has a reply subject set
	// and wants notification of no_responders.
	if !didDeliver && len(c.pa.reply) > 0 {
		c.mu.Lock()
		if c.opts.NoResponders {
			if sub := c.subForReply(c.pa.reply); sub != nil {
				hdrLen := 32 /* header without the subject */ + len(c.pa.subject)
				proto := fmt.Sprintf("HMSG %s %s %d %d\r\nNATS/1.0 503\r\nNats-Subject: %s\r\n\r\n\r\n", c.pa.reply, sub.sid, hdrLen, hdrLen, c.pa.subject)
				c.queueOutbound([]byte(proto))
				c.addToPCD(c)
			}
		}
		c.mu.Unlock()
	}

	return didDeliver, false
}

// Return the subscription for this reply subject. Only look at normal subs for this client.
func (c *client) subForReply(reply []byte) *subscription {
	r := c.acc.sl.Match(string(reply))
	for _, sub := range r.psubs {
		if sub.client == c {
			return sub
		}
	}
	return nil
}

// This is invoked knowing that c.pa.subject has been set to the gateway routed subject.
// This function will send the message to possibly LEAFs and directly back to the origin
// gateway.
func (c *client) handleGWReplyMap(msg []byte) bool {
	// Check for leaf nodes
	if c.srv.gwLeafSubs.Count() > 0 {
		if r := c.srv.gwLeafSubs.MatchBytes(c.pa.subject); len(r.psubs) > 0 {
			c.processMsgResults(c.acc, r, msg, c.pa.deliver, c.pa.subject, c.pa.reply, pmrNoFlag)
		}
	}
	if c.srv.gateway.enabled {
		reply := c.pa.reply
		if len(c.pa.deliver) > 0 && c.kind == JETSTREAM && len(reply) > 0 && !replyHasJSAckSuffix(reply) {
			reply = append(slices.Clip(reply), '@')
			reply = append(reply, c.pa.deliver...)
		}
		c.sendMsgToGateways(c.acc, msg, c.pa.subject, reply, nil, false)
	}
	return true
}

// Used to setup the response map for a service import request that has a reply subject.
func (c *client) setupResponseServiceImport(acc *Account, si *serviceImport, hasLatency, tracking bool, header http.Header) *serviceImport {
	rsi := si.acc.addRespServiceImport(acc, string(c.pa.reply), si, tracking, header, nil)
	if hasLatency {
		if c.rtt == 0 {
			// We have a service import that we are tracking but have not established RTT.
			c.sendRTTPing()
		}
		si.acc.mu.Lock()
		rsi.rc = c
		si.acc.mu.Unlock()
	}
	return rsi
}

// Will remove a status and description from the header if present.
func removeHeaderStatusIfPresent(hdr []byte) []byte {
	k := []byte("NATS/1.0")
	kl, i := len(k), bytes.IndexByte(hdr, '\r')
	if !bytes.HasPrefix(hdr, k) || i <= kl {
		return hdr
	}
	hdr = append(hdr[:kl], hdr[i:]...)
	if len(hdr) == len(emptyHdrLine) {
		return nil
	}
	return hdr
}

// Will remove a header if present.
func removeHeaderIfPresent(hdr []byte, key string) []byte {
	for {
		start := getHeaderKeyIndex(key, hdr)
		// key can't be first and we want to check that it is preceded by a '\n'
		if start < 1 || hdr[start-1] != '\n' {
			return hdr
		}
		index := start + len(key)
		if index >= len(hdr) || hdr[index] != ':' {
			return hdr
		}
		end := bytes.Index(hdr[start:], []byte(_CRLF_))
		if end < 0 {
			return hdr
		}
		hdr = append(hdr[:start], hdr[start+end+len(_CRLF_):]...)
		if len(hdr) <= len(emptyHdrLine) {
			return nil
		}
	}
}

func removeHeaderIfPrefixPresent(hdr []byte, prefix string) []byte {
	var index int
	for {
		if index >= len(hdr) {
			return hdr
		}

		start := bytes.Index(hdr[index:], []byte(prefix))
		if start < 0 {
			return hdr
		}
		index += start
		if index < 1 || hdr[index-1] != '\n' {
			index += len(prefix)
			continue
		}

		end := bytes.Index(hdr[index+len(prefix):], []byte(_CRLF_))
		if end < 0 {
			return hdr
		}

		hdr = append(hdr[:index], hdr[index+end+len(prefix)+len(_CRLF_):]...)
		if len(hdr) <= len(emptyHdrLine) {
			return nil
		}
	}
}

// Generate a new header based on optional original header and key value.
// More used in JetStream layers.
func genHeader(hdr []byte, key, value string) []byte {
	var bb bytes.Buffer
	if len(hdr) > LEN_CR_LF {
		bb.Write(hdr[:len(hdr)-LEN_CR_LF])
	} else {
		bb.WriteString(hdrLine)
	}
	http.Header{key: []string{value}}.Write(&bb)
	bb.WriteString(CR_LF)
	return bb.Bytes()
}

// This will set a header for the message.
// Lock does not need to be held but this should only be called
// from the inbound go routine. We will update the pubArgs.
// This will replace any previously set header and not add to it per normal spec.
func (c *client) setHeader(key, value string, msg []byte) []byte {
	var bb bytes.Buffer
	var omi int
	// Write original header if present.
	if c.pa.hdr > LEN_CR_LF {
		omi = c.pa.hdr
		// Need to copy since we're removing the header in place.
		hdr := removeHeaderIfPresent(copyBytes(msg[:c.pa.hdr-LEN_CR_LF]), key)
		if len(hdr) == 0 {
			bb.WriteString(hdrLine)
		} else {
			bb.Write(hdr)
		}
	} else {
		bb.WriteString(hdrLine)
	}
	http.Header{key: []string{value}}.Write(&bb)
	bb.WriteString(CR_LF)
	nhdr := bb.Len()
	// Put the original message back.
	// FIXME(dlc) - This is inefficient.
	bb.Write(msg[omi:])
	nsize := bb.Len() - LEN_CR_LF
	// MQTT producers don't have CRLF, so add it back.
	if c.isMqtt() {
		nsize += LEN_CR_LF
	}
	// Update pubArgs
	// If others will use this later we need to save and restore original.
	c.pa.hdr = nhdr
	c.pa.size = nsize
	c.pa.hdb = []byte(strconv.Itoa(nhdr))
	c.pa.szb = []byte(strconv.Itoa(nsize))
	return bb.Bytes()
}

// Will return a copy of the value for the header denoted by key or nil if it does not exist.
// If you know that it is safe to refer to the underlying hdr slice for the period that the
// return value is used, then sliceHeader() will be faster.
func getHeader(key string, hdr []byte) []byte {
	v := sliceHeader(key, hdr)
	if v == nil {
		return nil
	}
	return append(make([]byte, 0, len(v)), v...)
}

// Will return the sliced value for the header denoted by key or nil if it does not exists.
// This function ignores errors and tries to achieve speed and no additional allocations.
func sliceHeader(key string, hdr []byte) []byte {
	if len(hdr) == 0 {
		return nil
	}
	index := getHeaderKeyIndex(key, hdr)
	if index == -1 {
		return nil
	}
	// Skip over the key and the : separator.
	index += len(key) + 1
	hdrLen := len(hdr)
	// Skip over whitespace before the value.
	for index < hdrLen && hdr[index] == ' ' {
		index++
	}
	// Collect together the rest of the value until we hit a CRLF.
	start := index
	for index < hdrLen {
		if hdr[index] == '\r' && index < hdrLen-1 && hdr[index+1] == '\n' {
			break
		}
		index++
	}
	return hdr[start:index:index]
}

// getHeaderKeyIndex returns an index into the header slice for the given key.
// Returns -1 if not found.
func getHeaderKeyIndex(key string, hdr []byte) int {
	if len(hdr) == 0 {
		return -1
	}
	bkey := stringToBytes(key)
	keyLen, hdrLen := len(key), len(hdr)
	var offset int
	for {
		index := bytes.Index(hdr[offset:], bkey)
		// Check that we have enough characters, this will handle the -1 case of the key not
		// being found and will also handle not having enough characters for trailing CRLF.
		if index < 2 {
			return -1
		}
		index += offset
		// There should be a terminating CRLF.
		if index >= hdrLen-1 || hdr[index-1] != '\n' || hdr[index-2] != '\r' {
			offset = index + keyLen
			continue
		}
		// The key should be immediately followed by a : separator.
		if index+keyLen >= hdrLen {
			return -1
		}
		if hdr[index+keyLen] != ':' {
			offset = index + keyLen
			continue
		}
		return index
	}
}

// setHeader will replace the value of the first existing key `key`
// with the given value `val`, or add this new key at the end of
// the headers.
//
// Note: If the key does not exist, or if it exists but the new value
// would make the resulting byte slice larger than the original one,
// a new byte slice is returned and the original is left untouched.
// This is to prevent situations where caller may have a `hdr` and
// `msg` that are the parts of an underlying buffer. Extending the
// `hdr` would otherwise overwrite the `msg` part.
//
// If the new value is smaller, then the original `hdr` byte slice
// is modified.
func setHeader(key, val string, hdr []byte) []byte {
	start := getHeaderKeyIndex(key, hdr)
	if start >= 0 {
		valStart := start + len(key) + 1
		// Preserve single whitespace if used.
		hdrLen := len(hdr)
		if valStart < hdrLen && hdr[valStart] == ' ' {
			valStart++
		}
		valEnd := bytes.Index(hdr[valStart:], []byte("\r"))
		if valEnd < 0 {
			return hdr // malformed headers
		}
		valEnd += valStart
		// Length of the existing value (before the `\r`)
		oldValLen := valEnd - valStart
		// This is how many extra bytes we need for the new value.
		// If <= 0, it means that we need less and so will reuse the `hdr` buffer.
		if extra := len(val) - oldValLen; extra > 0 {
			// Check that we don't overflow an "int".
			if rem := math.MaxInt - hdrLen; rem < extra {
				// We don't grow, and return the existing header.
				return hdr
			}
			// The new size is the old size plus the extra bytes.
			newHdrSize := hdrLen + extra
			newHdr := make([]byte, newHdrSize)
			// Copy the parts from `hdr` and `val` into the new buffer.
			n := copy(newHdr, hdr[:valStart])
			n += copy(newHdr[n:], val)
			copy(newHdr[n:], hdr[valEnd:])
			return newHdr
		}
		// We can write in place since it fits in the existing `hdr` buffer.
		n := copy(hdr[valStart:], val)
		n += copy(hdr[valStart+n:], hdr[valEnd:])
		hdr = hdr[:valStart+n]
		return hdr
	}
	if len(hdr) > 0 && bytes.HasSuffix(hdr, []byte("\r\n")) {
		hdr = hdr[:len(hdr)-2]
		val += "\r\n"
	}
	// Create the new buffer based on length of existing one and
	// length of the new "<key>: <value>\r\n". Protect against "int" overflow.
	newSize := uint64(len(hdr)) + uint64(len(key)) + 1 + 1 + uint64(len(val)) + 2
	if newSize > uint64(math.MaxInt) {
		// We don't grow, and return the existing header.
		return hdr
	}
	newHdr := make([]byte, 0, int(newSize))
	newHdr = append(newHdr, hdr...)
	return fmt.Appendf(newHdr, "%s: %s\r\n", key, val)
}

// For bytes.HasPrefix below.
var (
	jsRequestNextPreB = []byte(jsRequestNextPre)
	jsDirectGetPreB   = []byte(jsDirectGetPre)
)

// processServiceImport is an internal callback when a subscription matches an imported service
// from another account. This includes response mappings as well.
func (c *client) processServiceImport(si *serviceImport, acc *Account, msg []byte) bool {
	// If we are a GW and this is not a direct serviceImport ignore.
	isResponse := si.isRespServiceImport()
	if (c.kind == GATEWAY || c.kind == ROUTER) && !isResponse {
		return false
	}
	// We need to protect `si` fields with account's read lock.
	acc.mu.RLock()
	// Detect cycles and ignore (return) when we detect one.
	if len(c.pa.psi) > 0 {
		for i := len(c.pa.psi) - 1; i >= 0; i-- {
			if psi := c.pa.psi[i]; psi.se == si.se {
				acc.mu.RUnlock()
				return false
			}
		}
	}
	var checkJS bool
	shouldReturn := si.invalid || acc.sl == nil
	if !shouldReturn && !isResponse && si.to == jsAllAPI {
		if bytes.HasPrefix(c.pa.subject, jsDirectGetPreB) || bytes.HasPrefix(c.pa.subject, jsRequestNextPreB) {
			checkJS = true
		}
	}
	siAcc := si.acc
	allowTrace := si.atrc
	isMsgTraceResp := isResponse && si.mt != nil
	siSe := si.se
	siLat := si.latency
	acc.mu.RUnlock()

	// We have a special case where JetStream pulls in all service imports through one export.
	// However the GetNext for consumers and DirectGet for streams are a no-op and causes buildups of service imports,
	// response service imports and rrMap entries which all will need to simply expire.
	// TODO(dlc) - Come up with something better.
	if shouldReturn {
		return false
	}
	if checkJS && siSe != nil {
		// siSe.acc is updated by configureAccounts() under the exporting
		// account's lock (siAcc), so read it under that lock.
		siAcc.mu.RLock()
		viaSysAcc := siSe.acc == c.srv.SystemAccount()
		siAcc.mu.RUnlock()
		if viaSysAcc {
			return false
		}
	}

	mt, traceOnly := c.isMsgTraceEnabled()

	var nrr []byte
	var rsi *serviceImport

	// Check if there is a reply present and set up a response.
	tracking, headers := shouldSample(siLat, c)
	if len(c.pa.reply) > 0 {
		// Special case for now, need to formalize.
		// TODO(dlc) - Formalize as a service import option for reply rewrite.
		// For now we can't do $JS.ACK since that breaks pull consumers across accounts.
		if !bytes.HasPrefix(c.pa.reply, []byte(jsAckPre)) {
			if rsi = c.setupResponseServiceImport(acc, si, siLat != nil, tracking, headers); rsi != nil {
				nrr = []byte(rsi.from)
			}
		} else {
			// This only happens when we do a pull subscriber that trampolines through another account.
			// Normally this code is not called.
			nrr = c.pa.reply
		}
	} else if !isResponse && siLat != nil && tracking {
		// Check to see if this was a bad request with no reply and we were supposed to be tracking.
		siAcc.sendBadRequestTrackingLatency(si, c, headers)
	}

	// Send tracking info here if we are tracking this response.
	// This is always a response.
	var didSendTL bool
	if si.tracking && !si.didDeliver {
		// Stamp that we attempted delivery.
		si.didDeliver = true
		didSendTL = acc.sendTrackingLatency(si, c)
	}

	// Pick correct "to" subject. If we matched on a wildcard use the literal publish subject.
	to, subject := si.to, string(c.pa.subject)

	if si.tr != nil {
		// FIXME(dlc) - This could be slow, may want to look at adding cache to bare transforms?
		to = si.tr.TransformSubject(subject)
	} else if si.usePub {
		to = subject
	}

	// Copy our pubArg since this gets modified as we process the service import itself.
	pacopy := c.pa

	// Now check to see if this account has mappings that could affect the service import.
	// Can't use non-locked trick like in processInboundClientMsg, so just call into selectMappedSubject
	// so we only lock once.
	nsubj, changed := siAcc.selectMappedSubject(to)
	if changed {
		c.pa.mapped = []byte(to)
		to = nsubj
	}

	// Set previous service import to detect chaining.
	lpsi := len(c.pa.psi)
	hadPrevSi, share := lpsi > 0, si.share
	if hadPrevSi {
		share = c.pa.psi[lpsi-1].share
	}
	c.pa.psi = append(c.pa.psi, si)

	// Place our client info for the request in the original message.
	// This will survive going across routes, etc.
	if !isResponse {
		isSysImport := siAcc == c.srv.SystemAccount()
		var ci *ClientInfo
		var cis *ClientInfo
		if c.pa.hdr >= 0 {
			var hci ClientInfo
			if err := json.Unmarshal(sliceHeader(ClientInfoHdr, msg[:c.pa.hdr]), &hci); err == nil {
				cis = &hci
			}
		}
		if c.kind == LEAF && c.pa.hdr >= 0 && len(sliceHeader(ClientInfoHdr, msg[:c.pa.hdr])) > 0 {
			// Leaf nodes may forward a Nats-Request-Info from a remote domain,
			// but the local server must replace it with the identity of the
			// authenticated leaf connection instead of trusting forwarded values.
			ci = c.getClientInfo(share)
			if hadPrevSi && cis != nil && cis.Reply != _EMPTY_ {
				ci.Reply = cis.Reply
			} else if bytes.HasSuffix(c.pa.reply, []byte(FastBatchSuffix)) {
				// Fast batch requires knowledge of the original reply subject.
				ci.Reply = bytesToString(c.pa.reply)
			}
			if hadPrevSi {
				ci.Service = acc.Name
				if !share && (si.share || isSysImport) {
					c.addServerAndClusterInfo(ci)
				}
			} else if !share && isSysImport {
				c.addServerAndClusterInfo(ci)
			}
		} else if hadPrevSi && cis != nil {
			ci = cis
			ci.Service = acc.Name
			// Check if we are moving into a share details account from a non-shared
			// and add in server and cluster details.
			if !share && (si.share || isSysImport) {
				c.addServerAndClusterInfo(ci)
			}
		} else if c.kind != LEAF || c.pa.hdr < 0 || len(sliceHeader(ClientInfoHdr, msg[:c.pa.hdr])) == 0 {
			ci = c.getClientInfo(share)
			// Fast batch requires knowledge of the original reply subject.
			if bytes.HasSuffix(c.pa.reply, []byte(FastBatchSuffix)) {
				ci.Reply = bytesToString(c.pa.reply)
			} else if ci != nil && len(c.pa.reply) > 0 && isJSPullRequest(to, msg[max(c.pa.hdr, 0):]) {
				// So do pull requests, for a consumer leader on another server to track the requester's interest.
				ci.Reply = bytesToString(c.pa.reply)
			}
			// If we did not share but the imports destination is the system account add in the server and cluster info.
			if !share && isSysImport {
				c.addServerAndClusterInfo(ci)
			}
		}
		// Set clientInfo if present.
		if ci != nil {
			if b, _ := json.Marshal(ci); b != nil {
				msg = c.setHeader(ClientInfoHdr, bytesToString(b), msg)
			}
		}
	}

	// Set our optional subject(to) and reply.
	if !isResponse && to != subject {
		c.pa.subject = []byte(to)
	}
	c.pa.reply = nrr

	if changed && c.isMqtt() && c.pa.hdr > 0 {
		c.srv.mqttStoreQoSMsgForAccountOnNewSubject(c.pa.hdr, msg, siAcc.GetName(), to)
	}

	// FIXME(dlc) - Do L1 cache trick like normal client?
	rr := siAcc.sl.Match(to)

	// If we are a route or gateway or leafnode and this message is flipped to a queue subscriber we
	// need to handle that since the processMsgResults will want a queue filter.
	flags := pmrMsgImportedFromService
	if c.kind == GATEWAY || c.kind == ROUTER || c.kind == LEAF {
		flags |= pmrIgnoreEmptyQueueFilter
	}

	// If this is a response service import that arrived via a route, allow delivery
	// to route subscriptions. Service import replies use one-time _R_ subjects.
	if isResponse && c.kind == ROUTER {
		flags |= pmrAllowSendFromRouteToRoute
	}

	// We will be calling back into processMsgResults since we are now being called as a normal sub.
	// We need to take care of the c.in.rts, so save off what is there and use a local version. We
	// will put back what was there after.

	orts := c.in.rts

	var lrts [routeTargetInit]routeTarget
	c.in.rts = lrts[:0]

	var skipProcessing bool
	var mtrsi *serviceImport
	// If message tracing enabled, add the service import trace.
	if mt != nil {
		mt.addServiceImportEvent(siAcc.GetName(), string(pacopy.subject), to)
		// If we are not allowing tracing and doing trace only, we stop at this level.
		if !allowTrace {
			if traceOnly {
				skipProcessing = true
			} else {
				// We are going to do normal processing, and possibly chainning
				// with other server imports, but the rest won't be traced.
				// We do so by setting the c.pa.trace to nil (it will be restored
				// with c.pa = pacopy).
				c.pa.trace = nil
				// We also need to disable the message trace headers so that
				// if the message is routed, it does not initialize tracing in the
				// remote.
				msg = c.setHeader(MsgTraceDest, MsgTraceDestDisabled, msg)
			}
		} else {
			// This code is invoked from a single thread so mutation of mt.dest
			// here is safe.
			dest := mt.dest
			defer func() { mt.dest = dest }()
			mtrsi, msg = mt.setupResponseServiceImport(c, acc, si, msg)
		}
	}

	var didDeliver bool

	if !skipProcessing {
		// If this is not a gateway connection but gateway is enabled,
		// try to send this converted message to all gateways.
		if c.srv.gateway.enabled {
			flags |= pmrCollectQueueNames
			var queues [][]byte
			didDeliver, queues = c.processMsgResults(siAcc, rr, msg, c.pa.deliver, []byte(to), nrr, flags)
			didDeliver = c.sendMsgToGateways(siAcc, msg, []byte(to), nrr, queues, false) || didDeliver
		} else {
			didDeliver, _ = c.processMsgResults(siAcc, rr, msg, c.pa.deliver, []byte(to), nrr, flags)
		}
	}

	// Restore to original values.
	c.in.rts = orts
	c.pa = pacopy

	// Mark in the c.pa if the message was delivered or not (no responders status).
	c.pa.delivered = didDeliver

	// Determine if we should remove this service import. This is for response service imports.
	// We will remove if we did not deliver, or if we are a response service import and we are
	// a singleton, or we have an EOF message.
	shouldRemove := !didDeliver || (isResponse && (si.rt == Singleton || len(msg) == LEN_CR_LF))
	// If we are tracking and we did not actually send the latency info we need to suppress the removal.
	if si.tracking && !didSendTL {
		shouldRemove = false
	}
	// If we are streamed or chunked we need to update our timestamp to avoid cleanup.
	if si.rt != Singleton && didDeliver {
		acc.mu.Lock()
		si.ts = time.Now().UnixNano()
		acc.mu.Unlock()
	}
	// This is a message trace...
	if mt != nil {
		// If it was not routed anywhere, we can remove mtrsi that we created earlier.
		mt.siMu.RLock()
		remove := mt.rsi == nil
		mt.siMu.RUnlock()
		if remove {
			reason := rsiOk
			if !didDeliver {
				reason = rsiNoDelivery
			}
			siAcc.removeRespServiceImport(mtrsi, reason)
		}
		// If we skip last-mile delivery, we need to remove the rsi that we
		// may have created if there was a reply subject.
		if rsi != nil && traceOnly {
			shouldRemove = true
		}

	} else if isMsgTraceResp {
		shouldRemove = false
		if !strings.HasPrefix(si.to, replyPrefix) {
			var evt MsgTraceEvent
			if err := json.Unmarshal(msg, &evt); err == nil {
				si.mt.handleRespServiceImport(&evt)
			}
		}
	}

	// Cleanup of a response service import
	if shouldRemove {
		reason := rsiOk
		if !didDeliver {
			reason = rsiNoDelivery
		}
		if isResponse {
			acc.removeRespServiceImport(si, reason)
		} else {
			// This is a main import and since we could not even deliver to the exporting account
			// go ahead and remove the respServiceImport we created above.
			siAcc.removeRespServiceImport(rsi, reason)
		}
	}

	return didDeliver
}

// isLeafEcho reports whether delivering to sub would send a message back where it came from.
func (c *client) isLeafEcho(sub *subscription, leafOrigin string, importedFromService bool) bool {
	return (leafOrigin != _EMPTY_ || importedFromService || sub.im != nil) && c.checkLeafEcho(sub, leafOrigin, importedFromService)
}

// Imported messages may go back to their origin leafnode cluster, which never saw them in this account.
func (c *client) checkLeafEcho(sub *subscription, leafOrigin string, importedFromService bool) bool {
	dc := sub.client
	if sub.im == nil && !importedFromService {
		return leafOrigin != _EMPTY_ && leafOrigin == dc.remoteCluster()
	}
	// Returning it to the hub it came from lets imports on both sides loop it.
	if !c.isSpokeLeafNode() || !dc.isSpokeLeafNode() {
		return false
	}
	// Unnamed hubs advertise an empty cluster name even when clustered, so treat that as possibly the same hub.
	rc, drc := c.remoteCluster(), dc.remoteCluster()
	return rc == _EMPTY_ || drc == _EMPTY_ || rc == drc
}

func (c *client) addSubToRouteTargets(sub *subscription) {
	if c.in.rts == nil {
		c.in.rts = make([]routeTarget, 0, routeTargetInit)
	}

	for i := range c.in.rts {
		rt := &c.in.rts[i]
		if rt.sub.client == sub.client {
			if sub.queue != nil {
				rt.qs = append(rt.qs, sub.queue...)
				rt.qs = append(rt.qs, ' ')
				rt.qsubs = append(rt.qsubs, sub)
			}
			return
		}
	}

	var rt *routeTarget
	lrts := len(c.in.rts)

	// If we are here we do not have the sub yet in our list
	// If we have to grow do so here.
	if lrts == cap(c.in.rts) {
		c.in.rts = append(c.in.rts, routeTarget{})
	}

	c.in.rts = c.in.rts[:lrts+1]
	rt = &c.in.rts[lrts]
	rt.sub = sub
	rt.qs = rt._qs[:0]
	rt.qsubs = rt._qsubs[:0]
	if sub.queue != nil {
		rt.qs = append(rt.qs, sub.queue...)
		rt.qs = append(rt.qs, ' ')
		rt.qsubs = append(rt.qsubs, sub)
	}
}

// Filters queue groups in a coalesced route or leaf target against the
// destination connection's delivery-time subscription denies.
func filterRouteTargetDeny(subject []byte, rt *routeTarget) bool {
	dc := rt.sub.client
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if dc.mperms == nil {
		return true
	}

	dsubject := string(subject)
	if len(rt.sub.queue) == 0 {
		if dc.checkDenySub(dsubject, _EMPTY_) {
			return false
		}
	} else {
		rt.sub = nil
	}

	qs := rt.qs[:0]
	qsubs := rt.qsubs[:0]
	for _, qsub := range rt.qsubs {
		if dc.checkDenySub(dsubject, bytesToString(qsub.queue)) {
			continue
		}
		if rt.sub == nil {
			rt.sub = qsub
		}
		qs = append(qs, qsub.queue...)
		qs = append(qs, ' ')
		qsubs = append(qsubs, qsub)
	}
	rt.qs = qs
	rt.qsubs = qsubs
	return rt.sub != nil
}

// This processes the sublist results for a given message.
// Returns if the message was delivered to at least target and queue filters.
func (c *client) processMsgResults(acc *Account, r *SublistResult, msg, deliver, subject, reply []byte, flags int) (bool, [][]byte) {
	// For sending messages across routes and leafnodes.
	// Reset if we have one since we reuse this data structure.
	if c.in.rts != nil {
		c.in.rts = c.in.rts[:0]
	}

	var rplyHasGWPrefix bool
	creply := reply

	// If the reply subject is a GW routed reply, we will perform some
	// tracking in deliverMsg(). We also want to send to the user the
	// reply without the prefix. `creply` will be set to that and be
	// used to create the message header for client connections.
	if rplyHasGWPrefix = isGWRoutedReply(reply); rplyHasGWPrefix {
		creply = reply[gwSubjectOffset:]
	}

	// With JetStream we now have times where we want to match a subscription
	// on one subject, but deliver it with another. e.g. JetStream deliverables.
	// This only works for last mile, meaning to a client. For other types we need
	// to use the original subject.
	subj := subject
	if len(deliver) > 0 {
		subj = deliver
	}

	// Check for JetStream encoded reply subjects.
	// For now these will only be on $JS.ACK prefixed reply subjects.
	var remapped bool
	if len(creply) > 0 && c.kind != CLIENT && !isInternalClient(c.kind) {
		// We need to rewrite the subject and the reply.
		if li := jsAckDeliverIdx(creply); li != -1 && li < len(creply)-1 {
			remapped = true
			subj, creply = creply[li+1:], creply[:li]
		}
	}

	var didDeliver bool

	// delivery subject for clients
	var dsubj []byte
	// Used as scratch if mapping
	var _dsubj [128]byte

	// For stats, we will keep track of the number of messages that have been
	// delivered and then multiply by the size of that message and update
	// server and account stats in a "single" operation (instead of per-sub).
	// However, we account for situations where the message is possibly changed
	// by having an extra size
	var dlvMsgs int64
	var dlvExtraSize int64
	var dlvRouteMsgs int64
	var dlvLeafMsgs int64
	var dlvClientMsgs int64

	// We need to know if this is a MQTT producer because they send messages
	// without CR_LF (we otherwise remove the size of CR_LF from message size).
	prodIsMQTT := c.isMqtt()

	updateStats := func() {
		if dlvMsgs == 0 {
			return
		}

		totalBytes := dlvMsgs*int64(len(msg)) + dlvExtraSize
		routeBytes := dlvRouteMsgs*int64(len(msg)) + dlvExtraSize
		leafBytes := dlvLeafMsgs*int64(len(msg)) + dlvExtraSize
		// dlvExtraSize applies to route/leaf header overhead, not client deliveries
		clientBytes := dlvClientMsgs * int64(len(msg))

		// For non MQTT producers, remove the CR_LF * number of messages
		if !prodIsMQTT {
			totalBytes -= dlvMsgs * int64(LEN_CR_LF)
			routeBytes -= dlvRouteMsgs * int64(LEN_CR_LF)
			leafBytes -= dlvLeafMsgs * int64(LEN_CR_LF)
			clientBytes -= dlvClientMsgs * int64(LEN_CR_LF)
		}

		if acc != nil {
			acc.stats.Lock()
			acc.stats.outMsgs += dlvMsgs
			acc.stats.outBytes += totalBytes
			if dlvRouteMsgs > 0 {
				acc.stats.rt.outMsgs += dlvRouteMsgs
				acc.stats.rt.outBytes += routeBytes
			}
			if dlvLeafMsgs > 0 {
				acc.stats.ln.outMsgs += dlvLeafMsgs
				acc.stats.ln.outBytes += leafBytes
			}
			acc.stats.Unlock()
		}

		if srv := c.srv; srv != nil {
			atomic.AddInt64(&srv.outMsgs, dlvMsgs)
			atomic.AddInt64(&srv.outBytes, totalBytes)

			atomic.AddInt64(&srv.outClientMsgs, dlvClientMsgs)
			atomic.AddInt64(&srv.outClientBytes, clientBytes)
		}
	}

	mt, traceOnly := c.isMsgTraceEnabled()
	importedFromService := flags&pmrMsgImportedFromService != 0

	// Loop over all normal subscriptions that match.
	for _, sub := range r.psubs {
		// Check if this is a send to a ROUTER. We now process
		// these after everything else.
		switch sub.client.kind {
		case ROUTER:
			if (c.kind != ROUTER && !c.isSpokeLeafNode()) || (flags&pmrAllowSendFromRouteToRoute != 0) {
				c.addSubToRouteTargets(sub)
			}
			continue
		case GATEWAY:
			// Never send to gateway from here.
			continue
		case LEAF:
			// We handle similarly to routes and use the same data structures.
			// Leaf node delivery audience is different however.
			// Also leaf nodes are always no echo, so we make sure we are not
			// going to send back to ourselves here. For messages from routes we want
			// to suppress in general unless we know from the hub or its a service reply.
			if c != sub.client && (c.kind != ROUTER || sub.client.isHubLeafNode() || isServiceReply(c.pa.subject)) {
				c.addSubToRouteTargets(sub)
			}
			continue
		}

		// Assume delivery subject is the normal subject to this point.
		dsubj = subj

		// We may need to disable tracing, by setting c.pa.trace to `nil`
		// before the call to deliverMsg, if so, this will indicate that
		// we need to put it back.
		var restorePaTrace bool

		// Check for stream import mapped subs (shadow subs). These apply to local subs only.
		if sub.im != nil {
			// If this message was a service import do not re-export to an exported stream.
			if importedFromService {
				continue
			}
			if sub.im.tr != nil {
				to := sub.im.tr.TransformSubject(bytesToString(subject))
				dsubj = append(_dsubj[:0], to...)
			} else if sub.im.usePub {
				dsubj = append(_dsubj[:0], subj...)
			} else {
				dsubj = append(_dsubj[:0], sub.im.to...)
			}

			if mt != nil {
				mt.addStreamExportEvent(sub.client, dsubj)
				// If allow_trace is false...
				if !sub.im.atrc {
					// If we are doing only message tracing, we can move to the
					//  next sub.
					if traceOnly {
						// Although the message was not delivered, for the purpose
						// of didDeliver, we need to set to true (to avoid possible
						// no responders).
						didDeliver = true
						continue
					}
					// If we are delivering the message, we need to disable tracing
					// before calling deliverMsg().
					c.pa.trace, restorePaTrace = nil, true
				}
			}

			// Make sure deliver is set if inbound from a route.
			if remapped && (c.kind == GATEWAY || c.kind == ROUTER || c.kind == LEAF) {
				deliver = subj
			}
			// If we are mapping for a deliver subject we will reverse roles.
			// The original subj we set from above is correct for the msg header,
			// but we need to transform the deliver subject to properly route.
			if len(deliver) > 0 {
				dsubj, subj = subj, dsubj
			}
		}

		// Remap to the original subject if internal.
		if sub.icb != nil && sub.rsi {
			dsubj = subject
		}

		// Normal delivery
		mh := c.msgHeader(dsubj, creply, sub)
		if c.deliverMsg(prodIsMQTT, sub, acc, dsubj, creply, mh, msg, rplyHasGWPrefix) {
			// We don't count internal deliveries, so do only when sub.icb is nil.
			if sub.icb == nil {
				dlvMsgs++
				if sub.client.kind == CLIENT {
					dlvClientMsgs++
				}
			}
			didDeliver = true
		}
		if restorePaTrace {
			c.pa.trace = mt
		}
	}

	// Set these up to optionally filter based on the queue lists.
	// This is for messages received from routes which will have directed
	// guidance on which queue groups we should deliver to.
	qf := c.pa.queues

	// Declared here because of goto.
	var queues [][]byte

	var leafOrigin string
	switch c.kind {
	case ROUTER:
		if len(c.pa.origin) > 0 {
			// Picture a message sent from a leafnode to a server that then routes
			// this message: CluserA -leaf-> HUB1 -route-> HUB2
			// Here we are in HUB2, so c.kind is a ROUTER, but the message will
			// contain a c.pa.origin set to "ClusterA" to indicate that this message
			// originated from that leafnode cluster.
			leafOrigin = bytesToString(c.pa.origin)
		}
	case LEAF:
		leafOrigin = c.remoteCluster()
	}
	// The leafnode cluster never published a service-imported message in this account.
	if importedFromService {
		leafOrigin = _EMPTY_
	}

	// For all routes/leaf/gateway connections, we may still want to send messages to
	// leaf nodes or routes even if there are no queue filters since we collect
	// them above and do not process inline like normal clients.
	// However, do select queue subs if asked to ignore empty queue filter.
	if (c.kind == LEAF || c.kind == ROUTER || c.kind == GATEWAY) && len(qf) == 0 && flags&pmrIgnoreEmptyQueueFilter == 0 {
		goto sendToRoutesOrLeafs
	}

	// Process queue subs
	for i := 0; i < len(r.qsubs); i++ {
		qsubs := r.qsubs[i]
		// If we have a filter check that here. We could make this a map or someting more
		// complex but linear search since we expect queues to be small. Should be faster
		// and more cache friendly.
		if qf != nil && len(qsubs) > 0 {
			tqn := qsubs[0].queue
			for _, qn := range qf {
				if bytes.Equal(qn, tqn) {
					goto selectQSub
				}
			}
			continue
		}

	selectQSub:
		// We will hold onto remote or lead qsubs when we are coming from
		// a route or a leaf node just in case we can no longer do local delivery.
		var rsub, sub *subscription
		var _ql [32]*subscription

		// Weighted remote qsubs are at the front of the queue group.
		var nw int
		if i < len(r.qwn) {
			nw = r.qwn[i]
		}

		src := c.kind
		// If we just came from a route we want to prefer local subs.
		// So only select from local subs but remember the first rsub
		// in case all else fails.
		if src == ROUTER {
			ql := _ql[:0]
			// Total queue weight of the rsub candidates seen so far.
			var lw uint64
			for i := 0; i < len(qsubs); i++ {
				sub = qsubs[i]
				if dst := sub.client.kind; dst == LEAF || dst == ROUTER {
					// If the destination is a LEAF, we first need to make sure
					// that we would not pick one that was the origin of this
					// message.
					if dst == LEAF && c.isLeafEcho(sub, leafOrigin, importedFromService) {
						continue
					}
					// If we have assigned a ROUTER rsub already, replace if
					// the destination is a LEAF since we want to favor that.
					if rsub == nil || (rsub.client.kind == ROUTER && dst == LEAF) {
						rsub, lw = sub, qsubWeight(sub)
					} else if dst == LEAF {
						// We already have a LEAF and this is another one.
						// Swap it with a chance relative to its queue weight.
						// See https://github.com/nats-io/nats-server/issues/6040
						w := qsubWeight(sub)
						lw += w
						if rand.Uint64N(lw) < w {
							rsub = sub
						}
					}
				} else {
					ql = append(ql, sub)
				}
			}
			// Only local subs remain, which all have weight 1.
			qsubs, nw = ql, 0
		}

		sindex := 0
		lqs := len(qsubs)
		if lqs > 1 {
			sindex = weightedQSubIndex(qsubs, nw)
		}

		// Find a subscription that is able to deliver this message starting at a random index.
		// Note that if the message came from a ROUTER, we will only have CLIENT or LEAF
		// queue subs here, otherwise we can have all types.
		for i := 0; i < lqs; i++ {
			if sindex+i < lqs {
				sub = qsubs[sindex+i]
			} else {
				sub = qsubs[(sindex+i)%lqs]
			}
			if sub == nil {
				continue
			}

			// If we are a spoke leaf node make sure to not forward across routes.
			// This mimics same behavior for normal subs above.
			if c.kind == LEAF && c.isSpokeLeafNode() && sub.client.kind == ROUTER {
				continue
			}

			// We have taken care of preferring local subs for a message from a route above.
			// Here we just care about a client or leaf and skipping a leaf and preferring locals.
			if dst := sub.client.kind; dst == ROUTER || dst == LEAF {
				if (src == LEAF || src == CLIENT) && dst == LEAF {
					// If we come from a LEAF and are about to pick a LEAF connection,
					// make sure this is not the same leaf cluster.
					if src == LEAF && c.isLeafEcho(sub, leafOrigin, importedFromService) {
						continue
					}
					// Remember that leaf in case we don't find any other candidate.
					// We already start randomly in lqs slice, so we don't need
					// to do a random swap if we already have an rsub like we do
					// when src == ROUTER above.
					if rsub == nil {
						rsub = sub
					}
					continue
				} else {
					// We want to favor qsubs in our own cluster. If the routed
					// qsub is on behalf of a leaf, we need to treat it differently.
					if sub.leaf {
						// If we already have an rsub, nothing to do. Also, do
						// not pick a routed qsub for a LEAF origin cluster
						// that is the same than where the message comes from.
						if rsub == nil && (leafOrigin == _EMPTY_ || leafOrigin != bytesToString(sub.origin)) {
							rsub = sub
						}
						continue
					}
					// This is a qsub that is local on the remote server (or
					// we are connected to an older server and we don't know).
					// Pick this one and be done.
					rsub = sub
					break
				}
			}

			// Assume delivery subject is normal subject to this point.
			dsubj = subj

			// We may need to disable tracing, by setting c.pa.trace to `nil`
			// before the call to deliverMsg, if so, this will indicate that
			// we need to put it back.
			var restorePaTrace bool
			var skipDelivery bool

			// Check for stream import mapped subs. These apply to local subs only.
			if sub.im != nil {
				// If this message was a service import do not re-export to an exported stream.
				if importedFromService {
					continue
				}
				if sub.im.tr != nil {
					to := sub.im.tr.TransformSubject(bytesToString(subject))
					dsubj = append(_dsubj[:0], to...)
				} else if sub.im.usePub {
					dsubj = append(_dsubj[:0], subj...)
				} else {
					dsubj = append(_dsubj[:0], sub.im.to...)
				}

				if mt != nil {
					mt.addStreamExportEvent(sub.client, dsubj)
					// If allow_trace is false...
					if !sub.im.atrc {
						// If we are doing only message tracing, we are done
						// with this queue group.
						if traceOnly {
							skipDelivery = true
						} else {
							// If we are delivering, we need to disable tracing
							// before the call to deliverMsg()
							c.pa.trace, restorePaTrace = nil, true
						}
					}
				}

				// Make sure deliver is set if inbound from a route.
				if remapped && (c.kind == GATEWAY || c.kind == ROUTER || c.kind == LEAF) {
					deliver = subj
				}
				// If we are mapping for a deliver subject we will reverse roles.
				// The original subj we set from above is correct for the msg header,
				// but we need to transform the deliver subject to properly route.
				if len(deliver) > 0 {
					dsubj, subj = subj, dsubj
				}
			}

			var delivered bool
			if !skipDelivery {
				mh := c.msgHeader(dsubj, creply, sub)
				delivered = c.deliverMsg(prodIsMQTT, sub, acc, dsubj, creply, mh, msg, rplyHasGWPrefix)
				if restorePaTrace {
					c.pa.trace = mt
				}
			}
			if skipDelivery || delivered {
				// Update only if not skipped.
				if !skipDelivery && sub.icb == nil {
					dlvMsgs++
					switch sub.client.kind {
					case ROUTER:
						dlvRouteMsgs++
					case LEAF:
						dlvLeafMsgs++
					case CLIENT:
						dlvClientMsgs++
					}
				}
				// Do the rest even when message delivery was skipped.
				didDeliver = true
				// Clear rsub
				rsub = nil
				if flags&pmrCollectQueueNames != 0 {
					queues = append(queues, sub.queue)
				}
				break
			}
		}

		if rsub != nil {
			// We are here if we have selected a leaf or route as the destination,
			// or if we tried to deliver to a local qsub but failed.
			c.addSubToRouteTargets(rsub)
		}
	}

sendToRoutesOrLeafs:

	// If no messages for routes or leafnodes return here.
	if len(c.in.rts) == 0 {
		updateStats()
		return didDeliver, queues
	}

	// If we do have a deliver subject we need to do something with it.
	// Again this is when JetStream (but possibly others) wants the system
	// to rewrite the delivered subject. The way we will do that is place it
	// at the end of the reply subject if it exists. But only if this wasn't
	// already performed, otherwise we'd end up with a duplicate '@' suffix
	// resulting in a protocol error.
	if len(deliver) > 0 && len(reply) > 0 && !remapped && !replyHasJSAckSuffix(reply) {
		reply = append(slices.Clip(reply), '@')
		reply = append(reply, deliver...)
	}

	// Copy off original pa in case it changes.
	pa := c.pa

	// We address by index to avoid struct copy.
	// We have inline structs for memory layout and cache coherency.
	for i := range c.in.rts {
		rt := &c.in.rts[i]
		if (len(rt.qsubs) > 1 || (len(rt.sub.queue) == 0 && len(rt.qsubs) > 0)) &&
			!filterRouteTargetDeny(subject, rt) {
			continue
		}
		dc := rt.sub.client
		dmsg, hset := msg, false

		// Check if we have an origin cluster set from a leafnode message.
		// If so make sure we do not send it back to the same cluster for a different
		// leafnode. Cluster wide no echo.
		if dc.kind == LEAF {
			// leafOrigin covers both inbound from a route (c.pa.origin) and leaf to leaf.
			if c.isLeafEcho(rt.sub, leafOrigin, importedFromService) {
				continue
			}

			// We need to check if this is a request that has a stamped client information header.
			// This will contain an account but will represent the account from the leafnode. If
			// they are not named the same this would cause an account lookup failure trying to
			// process the request for something like JetStream or other system services that rely
			// on the client info header. We can just check for reply and the presence of a header
			// to avoid slow downs for all traffic.
			if len(c.pa.reply) > 0 && c.pa.hdr >= 0 {
				dmsg, hset = c.checkLeafClientInfoHeader(msg)
			}
		}

		if mt != nil {
			dmsg = mt.setHopHeader(c, dmsg)
			hset = true
		}

		mh := c.msgHeaderForRouteOrLeaf(subject, reply, rt, acc, importedFromService)
		if c.deliverMsg(prodIsMQTT, rt.sub, acc, subject, reply, mh, dmsg, false) {
			if flags&pmrCollectQueueNames != 0 {
				for _, qsub := range rt.qsubs {
					queues = append(queues, qsub.queue)
				}
			}
			if rt.sub.icb == nil {
				dlvMsgs++
				switch dc.kind {
				case ROUTER:
					dlvRouteMsgs++
				case LEAF:
					dlvLeafMsgs++
				}
				dlvExtraSize += int64(len(dmsg) - len(msg))
			}
			didDeliver = true
		}

		// If we set the header reset the origin pub args.
		if hset {
			c.pa = pa
		}
	}
	updateStats()
	return didDeliver, queues
}

// Check and swap accounts on a client info header destined across a leafnode.
func (c *client) checkLeafClientInfoHeader(msg []byte) (dmsg []byte, setHdr bool) {
	if c.pa.hdr < 0 || len(msg) < c.pa.hdr {
		return msg, false
	}
	cir := sliceHeader(ClientInfoHdr, msg[:c.pa.hdr])
	if len(cir) == 0 {
		return msg, false
	}

	dmsg = msg

	var ci ClientInfo
	if err := json.Unmarshal(cir, &ci); err == nil {
		if v, _ := c.srv.leafRemoteAccounts.Load(ci.Account); v != nil {
			remoteAcc := v.(string)
			if ci.Account != remoteAcc {
				ci.Account = remoteAcc
				if b, _ := json.Marshal(ci); b != nil {
					dmsg, setHdr = c.setHeader(ClientInfoHdr, bytesToString(b), msg), true
				}
			}
		}
	}
	return dmsg, setHdr
}

// Bind a client info header received from a leafnode to the account of the leafnode connection.
func (c *client) bindLeafClientInfoHeader(acc *Account, msg []byte) []byte {
	if c.pa.hdr < 0 || len(msg) < c.pa.hdr {
		return msg
	}
	cir := sliceHeader(ClientInfoHdr, msg[:c.pa.hdr])
	if len(cir) == 0 {
		return msg
	}
	var ci ClientInfo
	if err := json.Unmarshal(cir, &ci); err != nil || ci.Account == acc.Name {
		return msg
	}
	ci.Account = acc.Name
	if b, _ := json.Marshal(ci); b != nil {
		msg = c.setHeader(ClientInfoHdr, bytesToString(b), msg)
	}
	return msg
}

func (c *client) pubPermissionViolation(subject []byte) {
	errTxt := fmt.Sprintf("Permissions Violation for Publish to %q", subject)
	if mt, _ := c.isMsgTraceEnabled(); mt != nil {
		mt.setIngressError(errTxt)
	}
	c.sendErr(errTxt)
	c.Errorf("Publish Violation - Subject %q", subject)
}

func (c *client) subPermissionViolation(sub *subscription) {
	errTxt := fmt.Sprintf("Permissions Violation for Subscription to %q", sub.subject)
	logTxt := fmt.Sprintf("Subscription Violation - Subject %q, SID %s", sub.subject, sub.sid)

	if sub.queue != nil {
		errTxt = fmt.Sprintf("Permissions Violation for Subscription to %q using queue %q", sub.subject, sub.queue)
		logTxt = fmt.Sprintf("Subscription Violation - Subject %q, Queue: %q, SID %s", sub.subject, sub.queue, sub.sid)
	}

	c.sendErr(errTxt)
	c.Errorf(logTxt)
}

func (c *client) replySubjectViolation(reply []byte) {
	errTxt := fmt.Sprintf("Permissions Violation for Publish with Reply of %q", reply)
	if mt, _ := c.isMsgTraceEnabled(); mt != nil {
		mt.setIngressError(errTxt)
	}
	c.sendErr(errTxt)
	c.Errorf("Publish Violation - Reply %q", reply)
}

func (c *client) maxTokensViolation(sub *subscription) {
	errTxt := fmt.Sprintf("Permissions Violation for Subscription to %q, too many tokens", sub.subject)
	logTxt := fmt.Sprintf("Subscription Violation Too Many Tokens - Subject %q, SID %s", sub.subject, sub.sid)
	c.sendErr(errTxt)
	c.Errorf(logTxt)
}

func (c *client) processPingTimer() {
	c.mu.Lock()
	c.ping.tmr = nil
	// Check if connection is still opened
	if c.isClosed() {
		c.mu.Unlock()
		return
	}

	c.Debugf("%s Ping Timer", c.kindString())

	var sendPing bool

	opts := c.srv.getOpts()
	pingInterval := opts.PingInterval
	if c.kind == ROUTER && opts.Cluster.PingInterval > 0 {
		pingInterval = opts.Cluster.PingInterval
	}
	if c.kind == LEAF && opts.LeafNode.PingInterval > 0 {
		pingInterval = opts.LeafNode.PingInterval
	}
	if c.isWebsocket() && opts.Websocket.PingInterval > 0 {
		pingInterval = opts.Websocket.PingInterval
	}
	pingInterval = adjustPingInterval(c.kind, pingInterval)
	now := time.Now()
	needRTT := c.rtt == 0 || now.Sub(c.rttStart) > DEFAULT_RTT_MEASUREMENT_INTERVAL

	// Do not delay PINGs for ROUTER, GATEWAY or spoke LEAF connections.
	if c.kind == ROUTER || c.kind == GATEWAY || c.isSpokeLeafNode() {
		sendPing = true
	} else {
		// If we received client data or a ping from the other side within the PingInterval,
		// then there is no need to send a ping.
		if delta := now.Sub(c.lastIn); delta < pingInterval && !needRTT {
			c.Debugf("Delaying PING due to remote client data or ping %v ago", delta.Round(time.Second))
		} else {
			sendPing = true
		}
	}

	if sendPing {
		// Check for violation
		maxPingsOut := opts.MaxPingsOut
		if c.kind == ROUTER && opts.Cluster.MaxPingsOut > 0 {
			maxPingsOut = opts.Cluster.MaxPingsOut
		}
		if c.kind == LEAF && opts.LeafNode.MaxPingsOut > 0 {
			maxPingsOut = opts.LeafNode.MaxPingsOut
		}
		if c.ping.out+1 > maxPingsOut {
			c.Debugf("Stale Client Connection - Closing")
			c.enqueueProto([]byte(fmt.Sprintf(errProto, "Stale Connection")))
			c.mu.Unlock()
			c.closeConnection(StaleConnection)
			return
		}
		// Send PING
		c.sendPing()
	}

	// Reset to fire again.
	c.setPingTimer()
	c.mu.Unlock()
}

// Returns the smallest value between the given `d` and some max value
// based on the connection kind.
func adjustPingInterval(kind int, d time.Duration) time.Duration {
	switch kind {
	case ROUTER:
		if d > routeMaxPingInterval {
			return routeMaxPingInterval
		}
	case GATEWAY:
		if d > gatewayMaxPingInterval {
			return gatewayMaxPingInterval
		}
	}
	return d
}

// This is used when a connection cannot yet start to send PINGs because
// the remote would not be able to handle them (case of compression,
// or outbound gateway, etc...), but we still want to close the connection
// if the timer has not been reset by the time we reach the time equivalent
// to have sent the max number of pings.
//
// Lock should be held
func (c *client) watchForStaleConnection(pingInterval time.Duration, pingMax int) {
	c.ping.tmr = time.AfterFunc(pingInterval*time.Duration(pingMax+1), func() {
		c.mu.Lock()
		c.Debugf("Stale Client Connection - Closing")
		c.enqueueProto([]byte(fmt.Sprintf(errProto, "Stale Connection")))
		c.mu.Unlock()
		c.closeConnection(StaleConnection)
	})
}

// Lock should be held
func (c *client) setPingTimer() {
	if c.srv == nil {
		return
	}
	opts := c.srv.getOpts()
	d := opts.PingInterval
	if c.kind == ROUTER && opts.Cluster.PingInterval > 0 {
		d = opts.Cluster.PingInterval
	}
	if c.kind == LEAF && opts.LeafNode.PingInterval > 0 {
		d = opts.LeafNode.PingInterval
	}
	if c.isWebsocket() && opts.Websocket.PingInterval > 0 {
		d = opts.Websocket.PingInterval
	}
	d = adjustPingInterval(c.kind, d)
	c.ping.tmr = time.AfterFunc(d, c.processPingTimer)
}

// Lock should be held
func (c *client) clearPingTimer() {
	if c.ping.tmr == nil {
		return
	}
	c.ping.tmr.Stop()
	c.ping.tmr = nil
}

func (c *client) clearTlsToTimer() {
	if c.tlsTo == nil {
		return
	}
	c.tlsTo.Stop()
	c.tlsTo = nil
}

// Lock should be held
func (c *client) setAuthTimer(d time.Duration) {
	c.atmr = time.AfterFunc(d, c.authTimeout)
}

// Lock should be held
func (c *client) clearAuthTimer() bool {
	if c.atmr == nil {
		return true
	}
	stopped := c.atmr.Stop()
	c.atmr = nil
	return stopped
}

// Track whether the parser should still enforce pre-CONNECT rules.
// This is handshake state, not timer state, since some handshakes
// use a different timer while still expecting CONNECT.
// Lock assume held on entry.
func (c *client) awaitingAuth() bool {
	return c.flags.isSet(expectConnect) && !c.flags.isSet(connectReceived)
}

// This will set the atmr for the JWT expiration time.
// We will lock on entry.
func (c *client) setExpirationTimer(d time.Duration) {
	c.mu.Lock()
	c.setExpirationTimerUnlocked(d)
	c.mu.Unlock()
}

// This will set the atmr for the JWT expiration time. client lock should be held before call
func (c *client) setExpirationTimerUnlocked(d time.Duration) {
	c.atmr = time.AfterFunc(d, c.authExpired)
	// This is an JWT expiration.
	if c.flags.isSet(connectReceived) {
		c.expires = time.Now().Add(d).Truncate(time.Second)
	}
}

// Return when this client expires via a claim, or 0 if not set.
func (c *client) claimExpiration() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.expires.IsZero() {
		return 0
	}
	return time.Until(c.expires).Truncate(time.Second)
}

// Possibly flush the connection and then close the low level connection.
// The boolean `minimalFlush` indicates if the flush operation should have a
// minimal write deadline.
// Lock is held on entry.
func (c *client) flushAndClose(minimalFlush bool) {
	if !c.flags.isSet(skipFlushOnClose) && c.out.pb > 0 {
		if minimalFlush {
			const lowWriteDeadline = 100 * time.Millisecond

			// Reduce the write deadline if needed.
			if c.out.wdl > lowWriteDeadline {
				c.out.wdl = lowWriteDeadline
			}
		}
		c.flushOutbound()
	}
	for i := range c.out.nb {
		nbPoolPut(c.out.nb[i])
	}
	c.out.nb = nil
	// We can't touch c.out.wnb when a flushOutbound is in progress since it
	// is accessed outside the lock there. If in progress, the cleanup will be
	// done in flushOutbound when detecting that connection is closed.
	if !c.flags.isSet(flushOutbound) {
		for i := range c.out.wnb {
			nbPoolPut(c.out.wnb[i])
		}
		c.out.wnb = nil
	}
	// This seem to be important (from experimentation) for the GC to release
	// the connection.
	c.out.sg = nil

	// Close the low level connection.
	if c.nc != nil {
		// Starting with Go 1.16, the low level close will set its own deadline
		// of 5 seconds, so setting our own deadline does not work. Instead,
		// we will close the TLS connection in separate go routine.
		nc := c.nc
		c.nc = nil
		if _, ok := nc.(*tls.Conn); ok {
			go func() { nc.Close() }()
		} else {
			nc.Close()
		}
	}
}

var kindStringMap = map[int]string{
	CLIENT:    "Client",
	ROUTER:    "Router",
	GATEWAY:   "Gateway",
	LEAF:      "Leafnode",
	JETSTREAM: "JetStream",
	ACCOUNT:   "Account",
	SYSTEM:    "System",
}

func (c *client) kindString() string {
	if kindStringVal, ok := kindStringMap[c.kind]; ok {
		return kindStringVal
	}
	return "Unknown Type"
}

// isAuthorizedByCallout returns true if the auth callout authorized this client.
func (c *client) isAuthorizedByCallout() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flags.isSet(authCallout)
}

// swapAccountAfterReload will check to make sure the bound account for this client
// is current. Under certain circumstances after a reload we could be pointing to
// an older one.
func (c *client) swapAccountAfterReload() {
	c.mu.Lock()
	srv := c.srv
	an := c.acc.GetName()
	c.mu.Unlock()
	if srv == nil {
		return
	}
	if acc, _ := srv.LookupAccount(an); acc != nil {
		c.mu.Lock()
		if c.acc != acc {
			c.acc = acc
		}
		c.mu.Unlock()
	}
}

// processSubsOnConfigReload removes any subscriptions the client has that are no
// longer authorized, and checks for imports (accounts) due to a config reload.
func (c *client) processSubsOnConfigReload(awcsti map[string]struct{}) {
	c.mu.Lock()
	var (
		// A leafnode's subscribe perms only limit what we receive, not the remote's interest.
		checkPerms = c.perms != nil && c.kind != LEAF
		checkAcc   = c.acc != nil
		acc        = c.acc
	)
	if checkAcc {
		// We actually only want to check if stream imports have changed.
		if _, ok := awcsti[acc.Name]; !ok {
			checkAcc = false
		}
	}
	if !checkPerms && !checkAcc {
		c.mu.Unlock()
		return
	}
	var (
		_subs    [32]*subscription
		subs     = _subs[:0]
		_removed [32]*subscription
		removed  = _removed[:0]
		srv      = c.srv
	)
	// We will clear any mperms we have here. It will rebuild on the fly with canSubscribe,
	// so we do that here as we collect them. We will check result down below.
	if checkPerms {
		c.mperms = nil
	}
	// Collect client's subs under the lock
	for _, sub := range c.subs {
		// Just checking to rebuild mperms under the lock, will collect removed though here.
		// Only collect under subs array of canSubscribe and checkAcc true.
		allowed := true
		if checkPerms {
			if len(sub.queue) > 0 {
				allowed = c.canSubscribe(string(sub.subject), string(sub.queue))
			} else {
				allowed = c.canSubscribe(string(sub.subject))
			}
		}
		if !allowed {
			removed = append(removed, sub)
		} else if checkAcc {
			subs = append(subs, sub)
		}
	}
	c.mu.Unlock()

	// This list is all subs who are allowed and we need to check accounts.
	for _, sub := range subs {
		c.mu.Lock()
		oldShadows := sub.shadow
		sub.shadow = nil
		c.mu.Unlock()
		c.addShadowSubscriptions(acc, sub)
		for _, nsub := range oldShadows {
			nsub.im.acc.sl.Remove(nsub)
		}
	}

	// Unsubscribe all that need to be removed and report back to client and logs.
	for _, sub := range removed {
		c.unsubscribe(acc, sub, true, true)
		if len(sub.queue) > 0 {
			c.sendErr(fmt.Sprintf("Permissions Violation for Subscription to %q using queue %q (sid %q)", sub.subject, sub.queue, sub.sid))
			srv.Noticef("Removed sub %q using queue %q (sid %q) for %s - not authorized", sub.subject, sub.queue, sub.sid, c.getAuthUser())
		} else {
			c.sendErr(fmt.Sprintf("Permissions Violation for Subscription to %q (sid %q)", sub.subject, sub.sid))
			srv.Noticef("Removed sub %q (sid %q) for %s - not authorized", sub.subject, sub.sid, c.getAuthUser())
		}
	}
}

// Allows us to count up all the queue subscribers during close.
type qsub struct {
	sub *subscription
	n   int32
}

func (c *client) closeConnection(reason ClosedState) {
	c.mu.Lock()
	if c.flags.isSet(closeConnection) {
		c.mu.Unlock()
		return
	}
	// Note that we may have markConnAsClosed() invoked before closeConnection(),
	// so don't set this to 1, instead bump the count.
	c.rref++
	c.flags.set(closeConnection)
	c.clearAuthTimer()
	c.clearPingTimer()
	c.clearTlsToTimer()
	c.markConnAsClosed(reason)

	// Unblock anyone who is potentially stalled waiting on us.
	if c.out.stc != nil {
		close(c.out.stc)
		c.out.stc = nil
	}

	// If we have remote latency tracking running shut that down.
	if c.rrTracking != nil {
		c.rrTracking.ptmr.Stop()
		c.rrTracking = nil
	}

	// If we are shutting down, no need to do all the accounting on subs, etc.
	// During LDM we'll still do the accounting, otherwise account limits could close others after this reconnects.
	if reason == ServerShutdown && c.srv.isShuttingDown() {
		s := c.srv
		c.mu.Unlock()
		if s != nil {
			// Unregister
			s.removeClient(c)
		}
		return
	}

	var (
		kind        = c.kind
		srv         = c.srv
		noReconnect = c.flags.isSet(noReconnect)
		acc         = c.acc
	)
	c.mu.Unlock()

	if kind == ROUTER {
		c.removeRemoteSubs()
	}
	if srv != nil {
		// Unregister
		srv.removeClient(c)

		if acc != nil {
			// Update remote subscriptions.
			if kind == CLIENT || kind == LEAF || kind == JETSTREAM {
				// Remove client's subscriptions from the account and unregister
				// client from that account. Keep c.subs populated because
				// saveClosedClient (started as a goroutine in markConnAsClosed)
				// still needs to read it.
				c.clearAccountSubs(true)
			}
			// Always remove from the account, otherwise we can leak clients.
			// Note that SYSTEM and ACCOUNT types from above cleanup their own subs.
			if prev := acc.removeClient(c); prev == 1 {
				srv.decActiveAccounts()
			}
		}
	}

	// Now that we are done with subscriptions, clear the field so that the
	// connection can be released and gc'ed.
	if kind == CLIENT || kind == LEAF {
		c.mu.Lock()
		c.subs = nil
		c.mu.Unlock()
	}

	// Don't reconnect connections that have been marked with
	// the no reconnect flag.
	if noReconnect {
		return
	}

	c.reconnect()
}

// clearAccountSubs removes the client's subscriptions from its current account
// and unregisters it from that account. If close is true, c.subs is left
// populated for saveClosedClient; otherwise c.subs is cleared and c.acc
// registered back to the global account.
// Client lock MUST NOT be held on entry.
func (c *client) clearAccountSubs(close bool) {
	c.mu.Lock()
	kind := c.kind
	srv := c.srv
	acc := c.acc
	if acc == nil || (kind != CLIENT && kind != LEAF && kind != JETSTREAM) {
		c.mu.Unlock()
		return
	}
	var _subs [32]*subscription
	subs := _subs[:0]
	// Do not set c.subs to nil or delete the sub from c.subs here because
	// it will be needed in saveClosedClient (which has been started as a
	// go routine in markConnAsClosed). Cleanup will be done there.
	for _, sub := range c.subs {
		// Auto-unsubscribe subscriptions must be unsubscribed forcibly.
		sub.max = 0
		sub.close()
		subs = append(subs, sub)
		if !close {
			delete(c.subs, string(sub.sid))
		}
	}
	spoke := c.isSpokeLeafNode()
	c.mu.Unlock()

	acc.sl.RemoveBatch(subs)

	if srv != nil {
		qsubs := map[string]*qsub{}
		for _, sub := range subs {
			// Call unsubscribe here to cleanup shadow subscriptions and such.
			c.unsubscribe(acc, sub, true, false)
			// Update route as normal for a normal subscriber.
			if sub.queue == nil {
				if !spoke {
					srv.updateRouteSubscriptionMap(acc, sub, -1)
					if srv.gateway.enabled {
						srv.gatewayUpdateSubInterest(acc.Name, sub, -1)
					}
				}
				acc.updateLeafNodes(sub, -1)
			} else {
				// We handle queue subscribers special in case we
				// have a bunch we can just send one update to the
				// connected routes.
				num := int32(1)
				if kind == LEAF {
					num = sub.qw
				}
				key := keyFromSub(sub)
				if esub, ok := qsubs[key]; ok {
					esub.n += num
				} else {
					qsubs[key] = &qsub{sub, num}
				}
			}
		}
		// Process any qsubs here.
		for _, esub := range qsubs {
			if !spoke {
				srv.updateRouteSubscriptionMap(acc, esub.sub, -esub.n)
				if srv.gateway.enabled {
					srv.gatewayUpdateSubInterest(acc.Name, esub.sub, -esub.n)
				}
			}
			acc.updateLeafNodes(esub.sub, -esub.n)
		}
	}

	if !close {
		// Register back to global account, mimicking the state after client initialization.
		c.registerWithAccount(srv.globalAccount())
	}
}

// Depending on the kind of connections, this may attempt to recreate a connection.
// The actual reconnect attempt will be started in a go routine.
func (c *client) reconnect() {
	var (
		retryImplicit bool
		gwName        string
		gwIsOutbound  bool
		gwCfg         *gatewayCfg
		leafCfg       *leafNodeCfg
	)

	c.mu.Lock()
	// Decrease the ref count and perform the reconnect only if == 0.
	c.rref--
	if c.flags.isSet(noReconnect) || c.rref > 0 {
		c.mu.Unlock()
		return
	}
	if c.route != nil {
		// A route is marked as solicited if it was given an URL to connect to,
		// which would be the case even with implicit (due to gossip), so mark this
		// as a retry for a route that is solicited and not explicit.
		retryImplicit = c.route.retry || (c.route.didSolicit && c.route.routeType == Implicit)
	}
	kind := c.kind
	switch kind {
	case GATEWAY:
		gwName = c.gw.name
		gwIsOutbound = c.gw.outbound
		gwCfg = c.gw.cfg
	case LEAF:
		if c.isSolicitedLeafNode() {
			leafCfg = c.leaf.remote
		}
	}
	srv := c.srv
	c.mu.Unlock()

	// Check for a solicited route. If it was, start up a reconnect unless
	// we are already connected to the other end.
	if didSolicit := c.isSolicitedRoute(); didSolicit || retryImplicit {
		srv.mu.Lock()
		defer srv.mu.Unlock()

		// Capture these under lock
		c.mu.Lock()
		rid := c.route.remoteID
		rtype := c.route.routeType
		rurl := c.route.url
		accName := string(c.route.accName)
		checkRID := accName == _EMPTY_ && srv.getOpts().Cluster.PoolSize < 1 && rid != _EMPTY_
		c.mu.Unlock()

		// It is possible that the server is being shutdown.
		// If so, don't try to reconnect
		if !srv.isRunning() {
			return
		}

		if checkRID && srv.routes[rid] != nil {
			// This is the case of "no pool". Make sure that the registered one
			// is upgraded to solicited if the connection trying to reconnect
			// was a solicited one.
			if didSolicit {
				if remote := srv.routes[rid][0]; remote != nil {
					upgradeRouteToSolicited(remote, rurl, rtype)
				}
			}
			srv.Debugf("Not attempting reconnect for solicited route, already connected to %q", rid)
			return
		} else if rid == srv.info.ID {
			srv.Debugf("Detected route to self, ignoring %q", rurl.Redacted())
			return
		} else if rtype != Implicit || retryImplicit {
			srv.Debugf("Attempting reconnect for solicited route %q", rurl.Redacted())
			// Keep track of this go-routine so we can wait for it on
			// server shutdown.
			srv.startGoRoutine(func() { srv.reConnectToRoute(rurl, rtype, accName) })
		}
	} else if srv != nil && kind == GATEWAY && gwIsOutbound {
		if gwCfg != nil {
			srv.Debugf("Attempting reconnect for gateway %q", gwName)
			// Run this as a go routine since we may be called within
			// the solicitGateway itself if there was an error during
			// the creation of the gateway connection.
			srv.startGoRoutine(func() { srv.reconnectGateway(gwCfg) })
		} else {
			srv.Debugf("Gateway %q not in configuration, not attempting reconnect", gwName)
		}
	} else if leafCfg != nil {
		// This is a solicited leaf node. Start up a reconnect.
		srv.startGoRoutine(func() { srv.reConnectToRemoteLeafNode(leafCfg) })
	}
}

// Set the noReconnect flag. This is used before a call to closeConnection()
// to prevent the connection to reconnect (routes, gateways).
func (c *client) setNoReconnect() {
	c.mu.Lock()
	c.flags.set(noReconnect)
	c.mu.Unlock()
}

// Returns the client's RTT value with the protection of the client's lock.
func (c *client) getRTTValue() time.Duration {
	c.mu.Lock()
	rtt := c.rtt
	c.mu.Unlock()
	return rtt
}

// This function is used by ROUTER and GATEWAY connections to
// look for a subject on a given account (since these type of
// connections are not bound to a specific account).
// If the c.pa.subject is found in the cache, the cached result
// is returned, otherwse, we match the account's sublist and update
// the cache. The cache is pruned if reaching a certain size.
func (c *client) getAccAndResultFromCache() (*Account, *SublistResult) {
	var (
		acc *Account
		pac *perAccountCache
		r   *SublistResult
		ok  bool
	)
	// Check our cache.
	if pac, ok = c.in.pacache[string(c.pa.pacache)]; ok {
		// Check the genid to see if it's still valid.
		// Since v2.10.0, the config reload of accounts has been fixed
		// and an account's sublist pointer should not change, so no need to
		// lock to access it.
		sl := pac.acc.sl

		if genid := atomic.LoadUint64(&sl.genid); genid != pac.genid {
			ok = false
			clear(c.in.pacache)
		} else {
			acc = pac.acc
			r = pac.results
		}
	}

	if !ok {
		if c.kind == ROUTER && len(c.route.accName) > 0 {
			if acc = c.acc; acc == nil {
				return nil, nil
			}
		} else {
			// Match correct account and sublist.
			if acc, _ = c.srv.LookupAccount(bytesToString(c.pa.account)); acc == nil {
				return nil, nil
			}
		}
		sl := acc.sl

		// Match against the account sublist.
		r = sl.MatchBytes(c.pa.subject)

		// Check if we need to prune. This should give us a perAccountCache struct
		// to reuse instead of having to allocate a new one.
		// Previously we would have removed multiple entries but now we will only
		// prune the minimum number required to maintain the cache size, so that
		// we reduce the amount of GC pressure and maintain cache stability as best
		// as possible.
		if len(c.in.pacache) >= maxPerAccountCacheSize {
			for cacheKey, p := range c.in.pacache {
				delete(c.in.pacache, cacheKey)
				pac = p
				if len(c.in.pacache) < maxPerAccountCacheSize {
					break
				}
			}
		}

		// If we can reuse the pac from earlier (i.e. we loaded one but it was an
		// old generation or we pruned the cache) then do so.
		if pac == nil {
			pac = &perAccountCache{}
		}
		pac.acc = acc
		pac.results = r
		pac.genid = atomic.LoadUint64(&sl.genid)

		// Store in our cache,make sure to do so after we prune.
		c.in.pacache[string(c.pa.pacache)] = pac
	}
	return acc, r
}

// Account will return the associated account for this client.
func (c *client) Account() *Account {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	acc := c.acc
	c.mu.Unlock()
	return acc
}

// pruneClosedSubFromPerAccountCache remove entries that contain subscriptions
// that have been closed.
func (c *client) pruneClosedSubFromPerAccountCache() {
	for cacheKey, pac := range c.in.pacache {
		for _, sub := range pac.results.psubs {
			if sub.isClosed() {
				goto REMOVE
			}
		}
		for _, qsub := range pac.results.qsubs {
			for _, sub := range qsub {
				if sub.isClosed() {
					goto REMOVE
				}
			}
		}
		continue
	REMOVE:
		delete(c.in.pacache, cacheKey)
	}
}

// Returns our service account for this request.
func (ci *ClientInfo) serviceAccount() string {
	if ci == nil {
		return _EMPTY_
	}
	if ci.Service != _EMPTY_ {
		return ci.Service
	}
	return ci.Account
}

// Add in our server and cluster information to this client info.
func (c *client) addServerAndClusterInfo(ci *ClientInfo) {
	if ci == nil {
		return
	}
	// Server
	if c.kind != LEAF {
		ci.Server = c.srv.Name()
	} else if c.kind == LEAF {
		ci.Server = c.leaf.remoteServer
	}
	// Cluster
	ci.Cluster = c.srv.cachedClusterName()
	// If we have gateways fill in cluster alternates.
	// These will be in RTT asc order.
	if c.srv.gateway.enabled {
		var gws []*client
		c.srv.getOutboundGatewayConnections(&gws)
		for _, c := range gws {
			c.mu.Lock()
			cn := c.gw.name
			c.mu.Unlock()
			ci.Alternates = append(ci.Alternates, cn)
		}
	}
}

// Grabs the information for this client.
func (c *client) getClientInfo(detailed bool) *ClientInfo {
	if c == nil || (c.kind != CLIENT && c.kind != LEAF && c.kind != JETSTREAM && c.kind != ACCOUNT) {
		return nil
	}

	// Result
	var ci ClientInfo

	if detailed {
		c.addServerAndClusterInfo(&ci)
	}

	c.mu.Lock()
	// RTT and Account are always added.
	ci.Account = accForClient(c)
	ci.RTT = c.rtt
	// Detailed signals additional opt in.
	if detailed {
		ci.Start = &c.start
		ci.Host = c.host
		ci.ID = c.cid
		ci.Name = c.opts.Name
		ci.User = c.getRawAuthUser()
		ci.Lang = c.opts.Lang
		ci.Version = c.opts.Version
		ci.Jwt = c.opts.JWT
		ci.IssuerKey = issuerForClient(c)
		ci.NameTag = c.nameTag
		ci.Tags = c.tags
		ci.Kind = c.kindString()
		ci.ClientType = c.clientTypeString()
	}
	c.mu.Unlock()
	return &ci
}

func (c *client) doTLSServerHandshake(typ string, tlsConfig *tls.Config, timeout float64, pCerts PinnedCertSet) error {
	_, err := c.doTLSHandshake(typ, false, nil, tlsConfig, _EMPTY_, timeout, pCerts)
	return err
}

func (c *client) doTLSClientHandshake(typ string, url *url.URL, tlsConfig *tls.Config, tlsName string, timeout float64, pCerts PinnedCertSet) (bool, error) {
	return c.doTLSHandshake(typ, true, url, tlsConfig, tlsName, timeout, pCerts)
}

// Performs either server or client side (if solicit is true) TLS Handshake.
// On error, the TLS handshake error has been logged and the connection
// has been closed.
//
// Lock is held on entry.
func (c *client) doTLSHandshake(typ string, solicit bool, url *url.URL, tlsConfig *tls.Config, tlsName string, timeout float64, pCerts PinnedCertSet) (bool, error) {
	var host string
	var resetTLSName bool
	var err error

	// Capture kind for some debug/error statements.
	kind := c.kind

	// If we solicited, we will act like the client, otherwise the server.
	if solicit {
		c.Debugf("Starting TLS %s client handshake", typ)
		if tlsConfig.ServerName == _EMPTY_ {
			// If the given url is a hostname, use this hostname for the
			// ServerName. If it is an IP, use the cfg's tlsName. If none
			// is available, resort to current IP.
			host = url.Hostname()
			if tlsName != _EMPTY_ && net.ParseIP(host) != nil {
				host = tlsName
			}
			tlsConfig.ServerName = host
		}
		c.nc = tls.Client(c.nc, tlsConfig)
	} else {
		if kind == CLIENT {
			c.Debugf("Starting TLS client connection handshake")
		} else {
			c.Debugf("Starting TLS %s server handshake", typ)
		}
		c.nc = tls.Server(c.nc, tlsConfig)
	}

	conn := c.nc.(*tls.Conn)

	// Setup the timeout
	ttl := secondsToDuration(timeout)
	c.tlsTo = time.AfterFunc(ttl, func() { tlsTimeout(c, conn) })
	conn.SetReadDeadline(time.Now().Add(ttl))

	c.mu.Unlock()
	if err = conn.Handshake(); err != nil {
		if solicit {
			// Based on type of error, possibly clear the saved tlsName
			// See: https://github.com/nats-io/nats-server/issues/1256
			// NOTE: As of Go 1.20, the HostnameError is wrapped so cannot
			// type assert to check directly.
			var hostnameErr x509.HostnameError
			if errors.As(err, &hostnameErr) {
				if host == tlsName {
					resetTLSName = true
				}
			}
		}
	} else if !c.matchesPinnedCert(pCerts) {
		err = ErrCertNotPinned
	}

	if err != nil {
		var detail string
		var subjs []string
		if ve, ok := err.(*tls.CertificateVerificationError); ok {
			for _, cert := range ve.UnverifiedCertificates {
				fp := sha256.Sum256(cert.Raw)
				fph := hex.EncodeToString(fp[:])
				subjs = append(subjs, fmt.Sprintf("%s SHA-256: %s", cert.Subject.String(), fph))
			}
		}
		if len(subjs) > 0 {
			detail = fmt.Sprintf(" (%s)", strings.Join(subjs, "; "))
		}
		if kind == ROUTER || kind == GATEWAY {
			// Always surface these as errors, as these ports shouldn't be behind a load
			// balancer or regularly probed.
			c.Errorf("TLS %s handshake error: %v%s", typ, err, detail)
		} else {
			logf := c.Errorf
			if isClientProbeTLSHandshakeError(err) {
				logf = c.Debugf
			}
			if kind == CLIENT {
				logf("TLS handshake error: %v%s", err, detail)
			} else {
				logf("TLS %s handshake error: %v%s", typ, err, detail)
			}
		}
		c.closeConnection(TLSHandshakeError)

		// Grab the lock before returning since the caller was holding the lock on entry
		c.mu.Lock()
		// Returning any error is fine. Since the connection is closed ErrConnectionClosed
		// is appropriate.
		return resetTLSName, ErrConnectionClosed
	}

	// Reset the read deadline
	conn.SetReadDeadline(time.Time{})

	// Re-Grab lock
	c.mu.Lock()

	// To be consistent with client, set this flag to indicate that handshake is done
	c.flags.set(handshakeComplete)

	// The connection still may have been closed on success handshake due
	// to a race with tls timeout. If that the case, return error indicating
	// that the connection is closed.
	if c.isClosed() {
		err = ErrConnectionClosed
	}

	return false, err
}

func isClientProbeTLSHandshakeError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var recordHeaderErr tls.RecordHeaderError
	// Conn is only set by crypto/tls when the invalid record was the peer's
	// initial handshake bytes, which is the non-TLS probe/load-balancer case.
	return errors.As(err, &recordHeaderErr) && recordHeaderErr.Conn != nil
}

// getRawAuthUser returns the raw auth user for the client.
// Lock should be held.
func (c *client) getRawAuthUser() string {
	switch {
	case c.opts.Nkey != _EMPTY_:
		return c.opts.Nkey
	case c.opts.Username != _EMPTY_:
		return c.opts.Username
	case c.opts.JWT != _EMPTY_:
		return c.pubKey
	case c.opts.Token != _EMPTY_:
		return "[REDACTED]"
	default:
		return _EMPTY_
	}
}

// getAuthUser returns the auth user for the client.
// Lock should be held.
func (c *client) getAuthUser() string {
	switch {
	case c.opts.Nkey != _EMPTY_:
		return fmt.Sprintf("Nkey %q", c.opts.Nkey)
	case c.opts.Username != _EMPTY_:
		return fmt.Sprintf("User %q", c.opts.Username)
	case c.opts.JWT != _EMPTY_:
		return fmt.Sprintf("JWT User %q", c.pubKey)
	case c.opts.Token != _EMPTY_:
		return fmt.Sprintf("Token %q", "[REDACTED]")
	default:
		return `User "N/A"`
	}
}

// getAuthUserLabel returns a label for the auth user for the client.
func (c *client) getAuthUserLabel() string {
	switch {
	case c.opts.Nkey != _EMPTY_:
		return fmt.Sprintf("nkey:%s", c.opts.Nkey)
	case c.opts.Username != _EMPTY_:
		return fmt.Sprintf("user:%s", c.opts.Username)
	case c.opts.JWT != _EMPTY_:
		return fmt.Sprintf("jwt:%s", c.pubKey)
	case c.opts.Token != _EMPTY_:
		return "token"
	default:
		return ""
	}
}

// Given an array of strings, this function converts it to a map as long
// as all the content (converted to upper-case) matches some constants.

// Converts the given array of strings to a map of string.
// The strings are converted to upper-case and added to the map only
// if the server recognize them as valid connection types.
// If there are unknown connection types, the map of valid ones is returned
// along with an error that contains the name of the unknown.
func convertAllowedConnectionTypes(cts []string) (map[string]struct{}, error) {
	var unknown []string
	m := make(map[string]struct{}, len(cts))
	for _, i := range cts {
		i = strings.ToUpper(i)
		switch i {
		case jwt.ConnectionTypeStandard, jwt.ConnectionTypeWebsocket,
			jwt.ConnectionTypeLeafnode, jwt.ConnectionTypeLeafnodeWS,
			jwt.ConnectionTypeMqtt, jwt.ConnectionTypeMqttWS,
			jwt.ConnectionTypeInProcess:
			m[i] = struct{}{}
		default:
			unknown = append(unknown, i)
		}
	}
	var err error
	// We will still return the map of valid ones.
	if len(unknown) != 0 {
		err = fmt.Errorf("invalid connection types %q", unknown)
	}
	return m, err
}

// This will return true if the connection is of a type present in the given `acts` map.
// Note that so far this is used only for CLIENT or LEAF connections.
// But a CLIENT can be standard or websocket (and other types in the future).
func (c *client) connectionTypeAllowed(acts map[string]struct{}) bool {
	// Empty means all type of clients are allowed
	if len(acts) == 0 {
		return true
	}
	var want string
	switch c.kind {
	case CLIENT:
		switch c.clientType() {
		case NATS:
			if c.iproc {
				want = jwt.ConnectionTypeInProcess
			} else {
				want = jwt.ConnectionTypeStandard
			}
		case WS:
			want = jwt.ConnectionTypeWebsocket
		case MQTT:
			if c.isWebsocket() {
				want = jwt.ConnectionTypeMqttWS
			} else {
				want = jwt.ConnectionTypeMqtt
			}
		}
	case LEAF:
		if c.isWebsocket() {
			want = jwt.ConnectionTypeLeafnodeWS
		} else {
			want = jwt.ConnectionTypeLeafnode
		}
	}
	_, ok := acts[want]
	return ok
}

// isClosed returns true if either closeConnection or connMarkedClosed
// flag have been set, or if `nc` is nil, which may happen in tests.
func (c *client) isClosed() bool {
	return c.flags.isSet(closeConnection) || c.flags.isSet(connMarkedClosed) || c.nc == nil
}

func (c *client) format(format string) string {
	if s := c.String(); s != _EMPTY_ {
		return fmt.Sprintf("%s - %s", s, format)
	} else {
		return format
	}
}

func (c *client) formatNoClientInfo(format string) string {
	acc := c.ncsAcc.Load()
	if acc != nil {
		return fmt.Sprintf("%s - Account:%s", format, acc)
	} else {
		return format
	}
}

func (c *client) formatClientSuffix() string {
	user := c.ncsUser.Load()
	if user == nil || user.(string) == _EMPTY_ {
		return _EMPTY_
	}
	return fmt.Sprintf(" - %s", user)
}

// Logging functionality scoped to a client or route.
func (c *client) Error(err error) {
	c.srv.Errorf(c.format(err.Error()))
}

func (c *client) Errorf(format string, v ...any) {
	c.srv.Errorf(c.format(format), v...)
}

func (c *client) Debugf(format string, v ...any) {
	c.srv.Debugf(c.format(format), v...)
}

func (c *client) Noticef(format string, v ...any) {
	c.srv.Noticef(c.format(format), v...)
}

func (c *client) Tracef(format string, v ...any) {
	c.srv.Tracef(c.format(format), v...)
}

func (c *client) Warnf(format string, v ...any) {
	c.srv.Warnf(c.format(format), v...)
}

func (c *client) RateLimitErrorf(format string, v ...any) {
	// Do the check before adding the client info to the format...
	statement := fmt.Sprintf(c.formatNoClientInfo(format), v...)
	if _, loaded := c.srv.rateLimitLogging.LoadOrStore(statement, time.Now()); loaded {
		return
	}
	if s := c.String(); s != _EMPTY_ {
		c.srv.Errorf("%s - %s%s", c, statement, c.formatClientSuffix())
	} else {
		c.srv.Errorf("%s%s", statement, c.formatClientSuffix())
	}
}

func (c *client) rateLimitFormatWarnf(format string, v ...any) {
	// Do the check before adding the client info to the format...
	format = c.formatNoClientInfo(format)
	if _, loaded := c.srv.rateLimitLogging.LoadOrStore(format, time.Now()); loaded {
		return
	}
	statement := fmt.Sprintf(format, v...)
	if s := c.String(); s != _EMPTY_ {
		c.srv.Warnf("%s - %s%s", c, statement, c.formatClientSuffix())
	} else {
		c.srv.Warnf("%s%s", statement, c.formatClientSuffix())
	}
}

func (c *client) RateLimitWarnf(format string, v ...any) {
	// Do the check before adding the client info to the format...
	statement := fmt.Sprintf(c.formatNoClientInfo(format), v...)
	if _, loaded := c.srv.rateLimitLogging.LoadOrStore(statement, time.Now()); loaded {
		return
	}
	if s := c.String(); s != _EMPTY_ {
		c.srv.Warnf("%s - %s%s", c, statement, c.formatClientSuffix())
	} else {
		c.srv.Warnf("%s%s", statement, c.formatClientSuffix())
	}
}

func (c *client) RateLimitDebugf(format string, v ...any) {
	// Do the check before adding the client info to the format...
	statement := fmt.Sprintf(c.formatNoClientInfo(format), v...)
	if _, loaded := c.srv.rateLimitLogging.LoadOrStore(statement, time.Now()); loaded {
		return
	}
	if s := c.String(); s != _EMPTY_ {
		c.srv.Debugf("%s - %s%s", c, statement, c.formatClientSuffix())
	} else {
		c.srv.Debugf("%s%s", statement, c.formatClientSuffix())
	}
}

// Set the very first PING to a lower interval to capture the initial RTT.
// After that the PING interval will be set to the user defined value.
// Client lock should be held.
func (c *client) setFirstPingTimer() {
	s := c.srv
	if s == nil {
		return
	}
	opts := s.getOpts()
	d := opts.PingInterval

	if c.kind == ROUTER && opts.Cluster.PingInterval > 0 {
		d = opts.Cluster.PingInterval
	}
	if c.kind == LEAF && opts.LeafNode.PingInterval > 0 {
		d = opts.LeafNode.PingInterval
	}
	if c.isWebsocket() && opts.Websocket.PingInterval > 0 {
		d = opts.Websocket.PingInterval
	}
	if !opts.DisableShortFirstPing {
		if c.kind != CLIENT {
			if d > firstPingInterval {
				d = firstPingInterval
			}
			d = adjustPingInterval(c.kind, d)
		} else if d > firstClientPingInterval {
			d = firstClientPingInterval
		}
	}
	// We randomize the first one by an offset up to 20%, e.g. 2m ~= max 24s.
	addDelay := rand.Int64N(int64(d / 5))
	d += time.Duration(addDelay)
	// In the case of ROUTER/LEAF and when compression is configured, it is possible
	// that this timer was already set, but just to detect a stale connection
	// since we have to delay the first PING after compression negotiation
	// occurred.
	if c.ping.tmr != nil {
		c.ping.tmr.Stop()
	}
	c.ping.tmr = time.AfterFunc(d, c.processPingTimer)
}

// Sets this error as the authentication error. To be used in authViolation()
// to report an error different of `ErrAuthentication`.
func (c *client) setAuthError(err error) {
	c.mu.Lock()
	c.authErr = err
	c.mu.Unlock()
}

// Returns the authentication error set in the connection, possibly nil.
func (c *client) getAuthError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.authErr
}
