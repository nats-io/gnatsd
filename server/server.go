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
	"context"
	"crypto/fips140"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	// Allow dynamic profiling.
	_ "net/http/pprof"

	"expvar"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

const (
	// Interval for the first PING for non client connections.
	firstPingInterval = time.Second

	// This is for the first ping for client connections.
	firstClientPingInterval = 2 * time.Second
)

// These are protocol versions sent between server connections: ROUTER, LEAF and
// GATEWAY. We may have protocol versions that have a meaning only for a certain
// type of connections, but we don't have to have separate enums for that.
// However, it is CRITICAL to not change the order of those constants since they
// are exchanged between servers. When adding a new protocol version, add to the
// end of the list, don't try to group them by connection types.
const (
	// RouteProtoZero is the original Route protocol from 2009.
	// http://nats.io/documentation/internals/nats-protocol/
	RouteProtoZero = iota
	// RouteProtoInfo signals a route can receive more then the original INFO block.
	// This can be used to update remote cluster permissions, etc...
	RouteProtoInfo
	// RouteProtoV2 is the new route/cluster protocol that provides account support.
	RouteProtoV2
	// MsgTraceProto indicates that this server understands distributed message tracing.
	MsgTraceProto
)

// Will return the latest server-to-server protocol versions, unless the
// option to override it is set.
func (s *Server) getServerProto() int {
	opts := s.getOpts()
	// Initialize with the latest protocol version.
	proto := MsgTraceProto
	// For tests, we want to be able to make this server behave
	// as an older server so check this option to see if we should override.
	if opts.overrideProto < 0 {
		// The option overrideProto is set to 0 by default (when creating an
		// Options structure). Since this is the same value than the original
		// proto RouteProtoZero, tests call setServerProtoForTest() with the
		// desired protocol level, which sets it as negative value equal to:
		// (wantedProto + 1) * -1. Here we compute back the real value.
		proto = (opts.overrideProto * -1) - 1
	}
	return proto
}

// Used by tests.
func setServerProtoForTest(wantedProto int) int {
	return (wantedProto + 1) * -1
}

// Info is the information sent to clients, routes, gateways, and leaf nodes,
// to help them understand information about this server.
type Info struct {
	ID                string   `json:"server_id"`
	Name              string   `json:"server_name"`
	Version           string   `json:"version"`
	Proto             int      `json:"proto"`
	GitCommit         string   `json:"git_commit,omitempty"`
	GoVersion         string   `json:"go"`
	Host              string   `json:"host"`
	Port              int      `json:"port"`
	Headers           bool     `json:"headers"`
	AuthRequired      bool     `json:"auth_required,omitempty"`
	TLSRequired       bool     `json:"tls_required,omitempty"`
	TLSVerify         bool     `json:"tls_verify,omitempty"`
	TLSAvailable      bool     `json:"tls_available,omitempty"`
	MaxPayload        int32    `json:"max_payload"`
	JetStream         bool     `json:"jetstream,omitempty"`
	IP                string   `json:"ip,omitempty"`
	CID               uint64   `json:"client_id,omitempty"`
	ClientIP          string   `json:"client_ip,omitempty"`
	Nonce             string   `json:"nonce,omitempty"`
	Cluster           string   `json:"cluster,omitempty"`
	Dynamic           bool     `json:"cluster_dynamic,omitempty"`
	Domain            string   `json:"domain,omitempty"`
	ClientConnectURLs []string `json:"connect_urls,omitempty"`    // Contains URLs a client can connect to.
	WSConnectURLs     []string `json:"ws_connect_urls,omitempty"` // Contains URLs a ws client can connect to.
	LameDuckMode      bool     `json:"ldm,omitempty"`
	Compression       string   `json:"compression,omitempty"`
	ConnectInfo       bool     `json:"connect_info,omitempty"`   // When true this is the server INFO response to CONNECT
	RemoteAccount     string   `json:"remote_account,omitempty"` // Lets the client or leafnode side know the remote account that they bind to.
	IsSystemAccount   bool     `json:"acc_is_sys,omitempty"`     // Indicates if the account is a system account.
	JSApiLevel        int      `json:"api_lvl,omitempty"`

	// Route Specific
	Import        *SubjectPermission `json:"import,omitempty"`
	Export        *SubjectPermission `json:"export,omitempty"`
	LNOC          bool               `json:"lnoc,omitempty"`
	LNOCU         bool               `json:"lnocu,omitempty"`
	LN            bool               `json:"ln,omitempty"`              // Support for LS+/LS- leaf interest using a sentinel origin cluster, implies LNOC+LNOCU
	InfoOnConnect bool               `json:"info_on_connect,omitempty"` // When true the server will respond to CONNECT with an INFO
	RoutePoolSize int                `json:"route_pool_size,omitempty"`
	RoutePoolIdx  int                `json:"route_pool_idx,omitempty"`
	RouteAccount  string             `json:"route_account,omitempty"`
	RouteAccReqID string             `json:"route_acc_add_reqid,omitempty"`
	GossipMode    byte               `json:"gossip_mode,omitempty"`

	// Gateways Specific
	Gateway           string   `json:"gateway,omitempty"`             // Name of the origin Gateway (sent by gateway's INFO)
	GatewayURLs       []string `json:"gateway_urls,omitempty"`        // Gateway URLs in the originating cluster (sent by gateway's INFO)
	GatewayURL        string   `json:"gateway_url,omitempty"`         // Gateway URL on that server (sent by route's INFO)
	GatewayCmd        byte     `json:"gateway_cmd,omitempty"`         // Command code for the receiving server to know what to do
	GatewayCmdPayload []byte   `json:"gateway_cmd_payload,omitempty"` // Command payload when needed
	GatewayNRP        bool     `json:"gateway_nrp,omitempty"`         // Uses the _GR_. prefix for mapped replies
	GatewayIOM        bool     `json:"gateway_iom,omitempty"`         // Indicate that all accounts will be switched to InterestOnly mode "right away"

	// LeafNode Specific
	LeafNodeURLs []string `json:"leafnode_urls,omitempty"` // LeafNode URLs that the server can reconnect to.

	XKey string `json:"xkey,omitempty"` // Public server's x25519 key.
}

// Server is our main struct.
type Server struct {
	// Fields accessed with atomic operations need to be 64-bit aligned
	gcid uint64
	// How often user logon fails due to the issuer account not being pinned.
	pinnedAccFail uint64
	stats
	scStats
	staleStats
	mu                  sync.RWMutex
	reloadMu            sync.RWMutex // Write-locked when a config reload is taking place ONLY
	kp                  nkeys.KeyPair
	xkp                 nkeys.KeyPair
	xpub                string
	info                Info
	configFile          string
	optsMu              sync.RWMutex
	opts                *Options
	running             atomic.Bool
	shutdown            atomic.Bool
	listener            net.Listener
	listenerErr         error
	gacc                *Account
	sys                 *internal
	sysAcc              atomic.Pointer[Account]
	js                  atomic.Pointer[jetStream]
	dios                *diskIOSemaphore
	isMetaLeader        atomic.Bool
	jsClustered         atomic.Bool
	accounts            sync.Map
	tmpAccounts         sync.Map // Temporarily stores accounts that are being built
	activeAccounts      int32
	accResolver         AccountResolver
	clients             map[uint64]*client
	routes              map[string][]*client
	remoteRoutePoolSize map[string]int                // Map for remote's configure route pool size
	routesPoolSize      int                           // Configured pool size
	routesReject        bool                          // During reload, we may want to reject adding routes until some conditions are met
	routesNoPool        int                           // Number of routes that don't use pooling (connecting to older server for instance)
	accRoutes           map[string]map[string]*client // Key is account name, value is key=remoteID/value=route connection
	accRouteByHash      sync.Map                      // Key is account name, value is nil or a pool index
	accAddedCh          chan struct{}
	accAddedReqID       string
	leafs               map[uint64]*client
	users               map[string]*User
	nkeys               map[string]*NkeyUser
	totalClients        uint64
	closed              *closedRingBuffer
	done                chan bool
	start               time.Time
	http                net.Listener
	httpHandler         http.Handler
	httpBasePath        string
	profiler            net.Listener
	httpReqStats        map[string]uint64
	routeListener       net.Listener
	routeListenerErr    error
	routeInfo           Info
	routeResolver       netResolver
	routesToSelf        map[string]struct{}
	routeTLSName        string
	leafNodeListener    net.Listener
	leafNodeListenerErr error
	leafNodeInfo        Info
	leafNodeInfoJSON    []byte
	leafURLsMap         refCountedUrlSet
	leafNodeOpts        struct {
		resolver    netResolver
		dialTimeout time.Duration
		dialer      leafNodeDialer
	}
	leafRemoteCfgs     map[*leafNodeCfg]struct{}
	rmLeafRemoteCfgs   map[string]*leafNodeCfg
	leafRemoteAccounts sync.Map
	leafNodeEnabled    bool
	leafDisableConnect bool // Used in test only
	leafNoCluster      bool // Indicate that this server has only remotes and no cluster defined

	quitCh           chan struct{}
	startupComplete  chan struct{}
	shutdownComplete chan struct{}

	// Tracking Go routines
	grMu         sync.Mutex
	grTmpClients map[uint64]*client
	grRunning    bool
	grWG         sync.WaitGroup // to wait on various go routines

	cproto     int64     // number of clients supporting async INFO
	configTime time.Time // last time config was loaded

	logging struct {
		sync.RWMutex
		logger      Logger
		trace       int32
		debug       int32
		traceSysAcc int32
	}

	clientConnectURLs []string

	// Used internally for quick look-ups.
	clientConnectURLsMap refCountedUrlSet

	// For Gateways
	gatewayListener    net.Listener // Accept listener
	gatewayListenerErr error
	gateway            *srvGateway

	// Used by tests to check that http.Servers do
	// not set any timeout.
	monitoringServer *http.Server
	profilingServer  *http.Server

	// LameDuck mode
	ldm   bool
	ldmCh chan bool

	// Trusted public operator keys.
	trustedKeys []string
	// map of trusted keys to operator setting StrictSigningKeyUsage
	strictSigningKeyUsage map[string]struct{}

	// We use this to minimize mem copies for requests to monitoring
	// endpoint /varz (when it comes from http).
	varzMu sync.Mutex
	varz   *Varz
	// This is set during a config reload if we detect that we have
	// added/removed routes. The monitoring code then check that
	// to know if it should update the cluster's URLs array.
	varzUpdateRouteURLs bool

	// Keeps a sublist of subscriptions attached to leafnode connections
	// for the _GR_.> subject so that a server can send back a mapped
	// gateway reply.
	gwLeafSubs *Sublist

	// Used for expiration of mapped GW replies
	gwrm struct {
		w  int32
		ch chan time.Duration
		m  sync.Map
	}

	// For eventIDs
	eventIdsMu sync.Mutex
	eventIds   *nuid.NUID

	// Websocket structure
	websocket srvWebsocket

	// MQTT structure
	mqtt srvMQTT

	// OCSP monitoring
	ocsps []*OCSPMonitor

	// OCSP peer verification (at least one TLS block)
	ocspPeerVerify bool

	// OCSP response cache
	ocsprc OCSPResponseCache

	// exporting account name the importer experienced issues with
	incompleteAccExporterMap sync.Map

	// Holds cluster name under different lock for mapping
	cnMu sync.RWMutex
	cn   string

	// For registering raft nodes with the server.
	rnMu      sync.RWMutex
	raftNodes map[string]RaftNode

	// For mapping from a raft node name back to a server name and cluster. Node has to be in the same domain.
	nodeToInfo sync.Map

	// For out of resources to not log errors too fast.
	rerrMu   sync.Mutex
	rerrLast time.Time

	connRateCounter *rateCounter

	// If there is a system account configured, to still support the $G account,
	// the server will create a fake user and add it to the list of users.
	// Keep track of what that user name is for config reload purposes.
	sysAccOnlyNoAuthUser string

	// IPQueues map
	ipQueues sync.Map

	// To limit logging frequency
	rateLimitLogging   sync.Map
	rateLimitLoggingCh chan time.Duration

	// Total outstanding catchup bytes in flight.
	gcbMu     sync.RWMutex
	gcbOut    int64
	gcbOutMax int64 // Taken from JetStreamMaxCatchup or defaultMaxTotalCatchupOutBytes
	// A global chanel to kick out stalled catchup sequences.
	gcbKick chan struct{}

	// Total outbound syncRequests
	syncOutSem chan struct{}

	// Queue to process JS API requests that come from routes (or gateways)
	jsAPIRoutedReqs     *ipQueue[*jsAPIRoutedReq]
	jsAPIRoutedInfoReqs *ipQueue[*jsAPIRoutedReq]

	// Delayed API responses.
	delayedAPIResponses *ipQueue[*delayedAPIResponse]

	// Whether moving NRG traffic into accounts is permitted on this server.
	// Controls whether or not the account NRG capability is set in statsz.
	// Currently used by unit tests to simulate nodes not supporting account NRG.
	accountNRGAllowed atomic.Bool

	// List of proxies trusted keys in `KeyPair` form so we can do signature
	// verification when processing incoming proxy connections.
	proxiesKeyPairs []nkeys.KeyPair
	proxiedConns    map[string]map[uint64]*client
}

// For tracking JS nodes.
type nodeInfo struct {
	name            string
	version         string
	cluster         string
	domain          string
	id              string
	tags            jwt.TagList
	cfg             *JetStreamConfig
	stats           *JetStreamStats
	offline         bool
	js              bool
	binarySnapshots bool // Deprecated, binary snapshots are always used; versions 2.10.0+ support it.
	accountNRG      bool
}

// selectable reports whether we've had STATSZ from this node. Peer selection
// skips nodes without config or stats, so until then there's nothing we could
// place on it.
func (ni nodeInfo) selectable() bool {
	return ni.cfg != nil && ni.stats != nil
}

type stats struct {
	inMsgs           int64
	inBytes          int64
	inClientMsgs     int64
	inClientBytes    int64
	outMsgs          int64
	outBytes         int64
	outClientMsgs    int64
	outClientBytes   int64
	slowConsumers    int64
	staleConnections int64
	stalls           int64
}

// scStats includes the total and per connection counters of Slow Consumers.
type scStats struct {
	clients  atomic.Uint64
	routes   atomic.Uint64
	leafs    atomic.Uint64
	gateways atomic.Uint64
}

// staleStats includes the total and per connection counters of Stale Connections.
type staleStats struct {
	clients  atomic.Uint64
	routes   atomic.Uint64
	leafs    atomic.Uint64
	gateways atomic.Uint64
}

// This is used by tests so we can run all server tests with a default route
// or leafnode compression mode. For instance:
// go test -race -v ./server -cluster_compression=fast
var (
	testDefaultClusterCompression  string
	testDefaultLeafNodeCompression string
)

// Compression modes.
const (
	CompressionNotSupported   = "not supported"
	CompressionOff            = "off"
	CompressionAccept         = "accept"
	CompressionS2Auto         = "s2_auto"
	CompressionS2Uncompressed = "s2_uncompressed"
	CompressionS2Fast         = "s2_fast"
	CompressionS2Better       = "s2_better"
	CompressionS2Best         = "s2_best"
)

// defaultCompressionS2AutoRTTThresholds is the default of RTT thresholds for
// the CompressionS2Auto mode.
var defaultCompressionS2AutoRTTThresholds = []time.Duration{
	// [0..10ms] -> CompressionS2Uncompressed
	10 * time.Millisecond,
	// ]10ms..50ms] -> CompressionS2Fast
	50 * time.Millisecond,
	// ]50ms..100ms] -> CompressionS2Better
	100 * time.Millisecond,
	// ]100ms..] -> CompressionS2Best
}

// For a given user provided string, matches to one of the compression mode
// constant and updates the provided string to that constant. Returns an
// error if the provided compression mode is not known.
// The parameter `chosenModeForOn` indicates which compression mode to use
// when the user selects "on" (or enabled, true, etc..). This is because
// we may have different defaults depending on where the compression is used.
func validateAndNormalizeCompressionOption(c *CompressionOpts, chosenModeForOn string) error {
	if c == nil {
		return nil
	}
	cmtl := strings.ToLower(c.Mode)
	// First, check for the "on" case so that we set to the default compression
	// mode for that. The other switch/case will finish setup if needed (for
	// instance if the default mode is s2Auto).
	switch cmtl {
	case "on", "enabled", "true":
		cmtl = chosenModeForOn
	default:
	}
	// Check (again) with the proper mode.
	switch cmtl {
	case "not supported", "not_supported":
		c.Mode = CompressionNotSupported
	case "disabled", "off", "false":
		c.Mode = CompressionOff
	case "accept":
		c.Mode = CompressionAccept
	case "auto", "s2_auto":
		var rtts []time.Duration
		if len(c.RTTThresholds) == 0 {
			rtts = defaultCompressionS2AutoRTTThresholds
		} else {
			for _, n := range c.RTTThresholds {
				// Do not error on negative, but simply set to 0
				if n < 0 {
					n = 0
				}
				// Make sure they are properly ordered. However, it is possible
				// to have a "0" anywhere in the list to indicate that this
				// compression level should not be used.
				if l := len(rtts); l > 0 && n != 0 {
					for _, v := range rtts {
						if n < v {
							return fmt.Errorf("RTT threshold values %v should be in ascending order", c.RTTThresholds)
						}
					}
				}
				rtts = append(rtts, n)
			}
			if len(rtts) > 0 {
				// Trim 0 that are at the end.
				stop := -1
				for i := len(rtts) - 1; i >= 0; i-- {
					if rtts[i] != 0 {
						stop = i
						break
					}
				}
				rtts = rtts[:stop+1]
			}
			if len(rtts) > 4 {
				// There should be at most values for "uncompressed", "fast",
				// "better" and "best" (when some 0 are present).
				return fmt.Errorf("compression mode %q should have no more than 4 RTT thresholds: %v", c.Mode, c.RTTThresholds)
			} else if len(rtts) == 0 {
				// But there should be at least 1 if the user provided the slice.
				// We would be here only if it was provided by say with values
				// being a single or all zeros.
				return fmt.Errorf("compression mode %q requires at least one RTT threshold", c.Mode)
			}
		}
		c.Mode = CompressionS2Auto
		c.RTTThresholds = rtts
	case "fast", "s2_fast":
		c.Mode = CompressionS2Fast
	case "better", "s2_better":
		c.Mode = CompressionS2Better
	case "best", "s2_best":
		c.Mode = CompressionS2Best
	default:
		return fmt.Errorf("unsupported compression mode %q", c.Mode)
	}
	return nil
}

// Returns `true` if the compression mode `m` indicates that the server
// will negotiate compression with the remote server, `false` otherwise.
// Note that the provided compression mode is assumed to have been
// normalized and validated.
func needsCompression(m string) bool {
	return m != _EMPTY_ && m != CompressionOff && m != CompressionNotSupported
}

// Compression is asymmetric, meaning that one side can have a different
// compression level than the other. However, we need to check for cases
// when this server `scm` or the remote `rcm` do not support compression
// (say older server, or test to make it behave as it is not), or have
// the compression off.
// Note that `scm` is assumed to not be "off" or "not supported".
func selectCompressionMode(scm, rcm string) (mode string, err error) {
	if rcm == CompressionNotSupported || rcm == _EMPTY_ {
		return CompressionNotSupported, nil
	}
	switch rcm {
	case CompressionOff:
		// If the remote explicitly disables compression, then we won't
		// use compression.
		return CompressionOff, nil
	case CompressionAccept:
		// If the remote is ok with compression (but is not initiating it),
		// and if we too are in this mode, then it means no compression.
		if scm == CompressionAccept {
			return CompressionOff, nil
		}
		// Otherwise use our compression mode.
		return scm, nil
	case CompressionS2Auto, CompressionS2Uncompressed, CompressionS2Fast, CompressionS2Better, CompressionS2Best:
		// This case is here to make sure that if we don't recognize a
		// compression setting, we error out.
		if scm == CompressionAccept {
			// If our compression mode is "accept", then we will use the remote
			// compression mode, except if it is "auto", in which case we will
			// default to "fast". This is not a configuration (auto in one
			// side and accept in the other) that would be recommended.
			if rcm == CompressionS2Auto {
				return CompressionS2Fast, nil
			}
			// Use their compression mode.
			return rcm, nil
		}
		// Otherwise use our compression mode.
		return scm, nil
	default:
		return _EMPTY_, fmt.Errorf("unsupported route compression mode %q", rcm)
	}
}

// If the configured compression mode is "auto" then will return that,
// otherwise will return the given `cm` compression mode.
func compressionModeForInfoProtocol(co *CompressionOpts, cm string) string {
	if co.Mode == CompressionS2Auto {
		return CompressionS2Auto
	}
	return cm
}

// Given a connection RTT and a list of thresholds durations, this
// function will return an S2 compression level such as "uncompressed",
// "fast", "better" or "best". For instance, with the following slice:
// [5ms, 10ms, 15ms, 20ms], a RTT of up to 5ms will result
// in the compression level "uncompressed", ]5ms..10ms] will result in
// "fast" compression, etc..
// However, the 0 value allows for disabling of some compression levels.
// For instance, the following slice: [0, 0, 20, 30] means that a RTT of
// [0..20ms] would result in the "better" compression - effectively disabling
// the use of "uncompressed" and "fast", then anything above 20ms would
// result in the use of "best" level (the 30 in the list has no effect
// and the list could have been simplified to [0, 0, 20]).
func selectS2AutoModeBasedOnRTT(rtt time.Duration, rttThresholds []time.Duration) string {
	var idx int
	var found bool
	for i, d := range rttThresholds {
		if rtt <= d {
			idx = i
			found = true
			break
		}
	}
	if !found {
		// If we did not find but we have all levels, then use "best",
		// otherwise use the last one in array.
		if l := len(rttThresholds); l >= 3 {
			idx = 3
		} else {
			idx = l - 1
		}
	}
	switch idx {
	case 0:
		return CompressionS2Uncompressed
	case 1:
		return CompressionS2Fast
	case 2:
		return CompressionS2Better
	}
	return CompressionS2Best
}

// Returns an array of s2 WriterOption based on the route compression mode.
// So far we return a single option, but this way we can call s2.NewWriter()
// with a nil []s2.WriterOption, but not with a nil s2.WriterOption, so
// this is more versatile.
func s2WriterOptions(cm string) []s2.WriterOption {
	_opts := [2]s2.WriterOption{}
	opts := append(
		_opts[:0],
		s2.WriterConcurrency(1), // Stop asynchronous flushing in separate goroutines
	)
	switch cm {
	case CompressionS2Uncompressed:
		return append(opts, s2.WriterUncompressed())
	case CompressionS2Fast:
		return opts
	case CompressionS2Best:
		return append(opts, s2.WriterBestCompression())
	case CompressionS2Better:
		return append(opts, s2.WriterBetterCompression())
	default:
		return nil
	}
}

// New will setup a new server struct after parsing the options.
// DEPRECATED: Use NewServer(opts)
func New(opts *Options) *Server {
	s, _ := NewServer(opts)
	return s
}

func NewServerFromConfig(opts *Options) (*Server, error) {
	if opts.ConfigFile != _EMPTY_ && opts.configDigest == "" {
		if err := opts.ProcessConfigFile(opts.ConfigFile); err != nil {
			return nil, err
		}
	}
	return NewServer(opts)
}

// NewServer will setup a new server struct after parsing the options.
// Could return an error if options can not be validated.
// The provided Options type should not be re-used afterwards.
// Either use Options.Clone() to pass a copy, or make a new one.
func NewServer(opts *Options) (*Server, error) {
	setBaselineOptions(opts)

	// Process TLS options, including whether we require client certificates.
	tlsReq := opts.TLSConfig != nil
	verify := (tlsReq && opts.TLSConfig.ClientAuth == tls.RequireAndVerifyClientCert)

	// Create our server's nkey identity.
	kp, _ := nkeys.CreateServer()
	pub, _ := kp.PublicKey()

	// Create an xkey for encrypting messages from this server.
	var xkp nkeys.KeyPair
	var xpub string
	if !fips140.Enabled() {
		xkp, _ = nkeys.CreateCurveKeys()
		xpub, _ = xkp.PublicKey()
	}

	serverName := pub
	if opts.ServerName != _EMPTY_ {
		serverName = opts.ServerName
	}

	httpBasePath := normalizeBasePath(opts.HTTPBasePath)

	// Validate some options. This is here because we cannot assume that
	// server will always be started with configuration parsing (that could
	// report issues). Its options can be (incorrectly) set by hand when
	// server is embedded. If there is an error, return nil.
	if err := validateOptions(opts); err != nil {
		return nil, err
	}

	info := Info{
		ID:           pub,
		XKey:         xpub,
		Version:      VERSION,
		Proto:        PROTO,
		GitCommit:    gitCommit,
		GoVersion:    runtime.Version(),
		Name:         serverName,
		Host:         opts.Host,
		Port:         opts.Port,
		AuthRequired: false,
		TLSRequired:  tlsReq && !opts.AllowNonTLS,
		TLSVerify:    verify,
		MaxPayload:   opts.MaxPayload,
		JetStream:    opts.JetStream,
		Headers:      !opts.NoHeaderSupport,
		Cluster:      opts.Cluster.Name,
		Domain:       opts.JetStreamDomain,
		JSApiLevel:   JSApiLevel,
	}

	if tlsReq && !info.TLSRequired {
		info.TLSAvailable = true
	}

	now := time.Now()

	s := &Server{
		kp:                 kp,
		xkp:                xkp,
		xpub:               xpub,
		configFile:         opts.ConfigFile,
		info:               info,
		opts:               opts,
		done:               make(chan bool, 1),
		start:              now,
		configTime:         now,
		gwLeafSubs:         NewSublistWithCache(),
		httpBasePath:       httpBasePath,
		eventIds:           nuid.New(),
		routesToSelf:       make(map[string]struct{}),
		httpReqStats:       make(map[string]uint64), // Used to track HTTP requests
		rateLimitLoggingCh: make(chan time.Duration, 1),
		leafNodeEnabled:    opts.LeafNode.Port != 0 || len(opts.LeafNode.Remotes) > 0,
		syncOutSem:         make(chan struct{}, maxConcurrentSyncRequests),
		dios:               newDiskIOSemaphore(opts.JetStreamConcurrentIOs),
	}

	// Delayed API response queue. Create regardless if JetStream is configured
	// or not (since it can be enabled/disabled with config reload, we want this
	// queue to exist at all times).
	s.delayedAPIResponses = newIPQueue[*delayedAPIResponse](s, "delayed API responses")

	// By default we'll allow account NRG.
	s.accountNRGAllowed.Store(true)

	// Fill up the maximum in flight syncRequests for this server.
	// Used in JetStream catchup semantics.
	for i := 0; i < maxConcurrentSyncRequests; i++ {
		s.syncOutSem <- struct{}{}
	}

	if opts.TLSRateLimit > 0 {
		s.connRateCounter = newRateCounter(opts.tlsConfigOpts.RateLimit)
	}

	// Trusted root operator keys.
	if !s.processTrustedKeys() {
		return nil, fmt.Errorf("Error processing trusted operator keys")
	}

	// If we have solicited leafnodes but no clustering and no clustername.
	// However we may need a stable clustername so use the server name.
	if len(opts.LeafNode.Remotes) > 0 && opts.Cluster.Port == 0 && opts.Cluster.Name == _EMPTY_ {
		s.leafNoCluster = true
		opts.Cluster.Name = opts.ServerName
	}

	if opts.Cluster.Name != _EMPTY_ {
		// Also place into mapping cn with cnMu lock.
		s.cnMu.Lock()
		s.cn = opts.Cluster.Name
		s.cnMu.Unlock()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// If there are proxies trusted public keys in the configuration
	// this will fill create the corresponding list of nkeys.KeyPair
	// that we can use for signature verification.
	s.processProxiesTrustedKeys()

	// Place ourselves in the JetStream nodeInfo if needed.
	if opts.JetStream {
		ourNode := getHash(serverName)
		s.nodeToInfo.Store(ourNode, nodeInfo{
			name:            serverName,
			version:         VERSION,
			cluster:         opts.Cluster.Name,
			domain:          opts.JetStreamDomain,
			id:              info.ID,
			tags:            opts.Tags,
			cfg:             &JetStreamConfig{MaxMemory: opts.JetStreamMaxMemory, MaxStore: opts.JetStreamMaxStore, CompressOK: true},
			stats:           nil,
			offline:         false,
			js:              true,
			binarySnapshots: true,
			accountNRG:      true,
		})
	}

	s.routeResolver = opts.Cluster.resolver
	if s.routeResolver == nil {
		s.routeResolver = net.DefaultResolver
	}

	// Used internally for quick look-ups.
	s.clientConnectURLsMap = make(refCountedUrlSet)
	s.websocket.connectURLsMap = make(refCountedUrlSet)
	s.leafURLsMap = make(refCountedUrlSet)

	// Ensure that non-exported options (used in tests) are properly set.
	s.setLeafNodeNonExportedOptions()

	// Setup OCSP Stapling and OCSP Peer. This will abort server from starting if there
	// are no valid staples and OCSP Stapling policy is set to Always or MustStaple.
	if err := s.enableOCSP(); err != nil {
		return nil, err
	}

	// Call this even if there is no gateway defined. It will
	// initialize the structure so we don't have to check for
	// it to be nil or not in various places in the code.
	if err := s.newGateway(opts); err != nil {
		return nil, err
	}

	// If we have a cluster definition but do not have a cluster name, create one.
	if opts.Cluster.Port != 0 && opts.Cluster.Name == _EMPTY_ {
		s.info.Cluster = nuid.Next()
	} else if opts.Cluster.Name != _EMPTY_ {
		// Likewise here if we have a cluster name set.
		s.info.Cluster = opts.Cluster.Name
	}

	// This is normally done in the AcceptLoop, once the
	// listener has been created (possibly with random port),
	// but since some tests may expect the INFO to be properly
	// set after New(), let's do it now.
	s.setInfoHostPort()

	// For tracking clients
	s.clients = make(map[uint64]*client)

	// For tracking closed clients.
	s.closed = newClosedRingBuffer(opts.MaxClosedClients)

	// For tracking connections that are not yet registered
	// in s.routes, but for which readLoop has started.
	s.grTmpClients = make(map[uint64]*client)

	// For tracking routes and their remote ids
	s.initRouteStructures(opts)

	// For tracking leaf nodes.
	s.leafs = make(map[uint64]*client)

	// Used to kick out all go routines possibly waiting on server
	// to shutdown.
	s.quitCh = make(chan struct{})

	// Closed when startup is complete. ReadyForConnections() will block on
	// this before checking the presence of listening sockets.
	s.startupComplete = make(chan struct{})

	// Closed when Shutdown() is complete. Allows WaitForShutdown() to block
	// waiting for complete shutdown.
	s.shutdownComplete = make(chan struct{})

	// Check for configured account resolvers.
	if err := s.configureResolver(); err != nil {
		return nil, err
	}
	// If there is an URL account resolver, do basic test to see if anyone is home.
	if ar := opts.AccountResolver; ar != nil {
		if ur, ok := ar.(*URLAccResolver); ok {
			if _, err := ur.Fetch(_EMPTY_); err != nil {
				return nil, err
			}
		}
	}
	// For other resolver:
	// In operator mode, when the account resolver depends on an external system and
	// the system account can't be fetched, inject a temporary one.
	if ar := s.accResolver; len(opts.TrustedOperators) == 1 && ar != nil &&
		opts.SystemAccount != _EMPTY_ && opts.SystemAccount != DEFAULT_SYSTEM_ACCOUNT {
		if _, ok := ar.(*MemAccResolver); !ok {
			s.mu.Unlock()
			var a *Account
			// perform direct lookup to avoid warning trace
			if _, err := fetchAccount(ar, opts.SystemAccount); err == nil {
				a, _ = s.lookupAccount(opts.SystemAccount)
			}
			s.mu.Lock()
			if a == nil {
				sac := NewAccount(opts.SystemAccount)
				sac.Issuer = opts.TrustedOperators[0].Issuer
				sac.signingKeys = map[string]jwt.Scope{}
				sac.signingKeys[opts.SystemAccount] = nil
				s.registerAccountNoLock(sac)
			}
		}
	}

	// For tracking accounts
	if _, err := s.configureAccounts(false); err != nil {
		return nil, err
	}

	// Used to setup Authorization.
	s.configureAuthorization()

	// Start signal handler
	s.handleSignals()

	return s, nil
}

// Initializes route structures based on pooling and/or per-account routes.
//
// Server lock is held on entry
func (s *Server) initRouteStructures(opts *Options) {
	s.routes = make(map[string][]*client)
	if ps := opts.Cluster.PoolSize; ps > 0 {
		s.routesPoolSize = ps
	} else {
		s.routesPoolSize = 1
	}
	// If we have per-account routes, we create accRoutes and initialize it
	// with nil values. The presence of an account as the key will allow us
	// to know if a given account is supposed to have dedicated routes.
	if l := len(opts.Cluster.PinnedAccounts); l > 0 {
		s.accRoutes = make(map[string]map[string]*client, l)
		for _, acc := range opts.Cluster.PinnedAccounts {
			s.accRoutes[acc] = make(map[string]*client)
		}
	}
}

func (s *Server) logRejectedTLSConns() {
	defer s.grWG.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.quitCh:
			return
		case <-t.C:
			blocked := s.connRateCounter.countBlocked()
			if blocked > 0 {
				s.Warnf("Rejected %d connections due to TLS rate limiting", blocked)
			}
		}
	}
}

// clusterName returns our cluster name which could be dynamic.
func (s *Server) ClusterName() string {
	s.mu.RLock()
	cn := s.info.Cluster
	s.mu.RUnlock()
	return cn
}

// Grabs cluster name with cluster name specific lock.
func (s *Server) cachedClusterName() string {
	s.cnMu.RLock()
	cn := s.cn
	s.cnMu.RUnlock()
	return cn
}

// setClusterName will update the cluster name for this server.
func (s *Server) setClusterName(name string) {
	s.mu.Lock()
	var resetCh chan struct{}
	if s.sys != nil && s.info.Cluster != name {
		// can't hold the lock as go routine reading it may be waiting for lock as well
		resetCh = s.sys.resetCh
	}
	s.info.Cluster = name
	s.routeInfo.Cluster = name

	// Need to close solicited leaf nodes. The close has to be done outside of the server lock.
	var leafs []*client
	for _, c := range s.leafs {
		c.mu.Lock()
		if c.leaf != nil && c.leaf.remote != nil {
			leafs = append(leafs, c)
		}
		c.mu.Unlock()
	}
	s.mu.Unlock()

	// Also place into mapping cn with cnMu lock.
	s.cnMu.Lock()
	s.cn = name
	s.cnMu.Unlock()

	for _, l := range leafs {
		l.closeConnection(ClusterNameConflict)
	}
	if resetCh != nil {
		select {
		case resetCh <- struct{}{}:
		case <-s.quitCh:
		}
	}
	s.Noticef("Cluster name updated to %s", name)
}

// Return whether the cluster name is dynamic.
func (s *Server) isClusterNameDynamic() bool {
	// We need to lock the whole "Cluster.Name" check and not use s.getOpts()
	// because otherwise this could cause a data race with setting the name in
	// route.go's processRouteConnect().
	s.optsMu.RLock()
	dynamic := s.opts.Cluster.Name == _EMPTY_
	s.optsMu.RUnlock()
	return dynamic
}

// Returns our configured serverName.
func (s *Server) serverName() string {
	return s.getOpts().ServerName
}

// ClientURL returns the URL used to connect clients.
// Helpful in tests and with in-process servers using a random client port (-1).
func (s *Server) ClientURL() string {
	// FIXME(dlc) - should we add in user and pass if defined single?
	opts := s.getOpts()
	var u url.URL
	u.Scheme = "nats"
	if opts.TLSConfig != nil {
		u.Scheme = "tls"
	}
	u.Host = net.JoinHostPort(opts.Host, fmt.Sprintf("%d", opts.Port))
	return u.String()
}

// WebsocketURL returns the URL used to connect websocket clients.
// Helpful in tests and with in-process servers using a random websocket port (-1).
func (s *Server) WebsocketURL() string {
	opts := s.getOpts()
	var u url.URL
	u.Scheme = "ws"
	if opts.Websocket.TLSConfig != nil {
		u.Scheme = "wss"
	}
	u.Host = net.JoinHostPort(opts.Websocket.Host, fmt.Sprintf("%d", opts.Websocket.Port))
	return u.String()
}

func validateCluster(o *Options) error {
	if o.Cluster.Name != _EMPTY_ && strings.Contains(o.Cluster.Name, " ") {
		return ErrClusterNameHasSpaces
	}
	if p := o.Cluster.Permissions; p != nil {
		perms := &Permissions{Publish: p.Import, Subscribe: p.Export}
		if err := checkClusterPermissionSubjects(perms); err != nil {
			return err
		}
	}
	if o.Cluster.Compression.Mode != _EMPTY_ {
		if err := validateAndNormalizeCompressionOption(&o.Cluster.Compression, CompressionS2Fast); err != nil {
			return err
		}
	}
	if err := validatePinnedCerts(o.Cluster.TLSPinnedCerts); err != nil {
		return fmt.Errorf("cluster: %v", err)
	}
	// Check that cluster name if defined matches any gateway name.
	// Note that we have already verified that the gateway name does not have spaces.
	if o.Gateway.Name != _EMPTY_ && o.Gateway.Name != o.Cluster.Name {
		if o.Cluster.Name != _EMPTY_ {
			return ErrClusterNameConfigConflict
		}
		// Set this here so we do not consider it dynamic.
		o.Cluster.Name = o.Gateway.Name
	}
	clusterName := o.Cluster.Name
	if clusterName == _EMPTY_ && len(o.LeafNode.Remotes) > 0 && o.Cluster.Port == 0 {
		clusterName = o.ServerName
	}
	if clusterName == leafNoOriginCluster {
		return ErrClusterNameReserved
	}
	if l := len(o.Cluster.PinnedAccounts); l > 0 {
		if o.Cluster.PoolSize < 0 {
			return fmt.Errorf("pool_size cannot be negative if pinned accounts are specified")
		}
		m := make(map[string]struct{}, l)
		for _, a := range o.Cluster.PinnedAccounts {
			if _, exists := m[a]; exists {
				return fmt.Errorf("found duplicate account name %q in pinned accounts list %q", a, o.Cluster.PinnedAccounts)
			}
			m[a] = struct{}{}
		}
	}
	return nil
}

func validatePinnedCerts(pinned PinnedCertSet) error {
	re := regexp.MustCompile("^[a-f0-9]{64}$")
	for certId := range pinned {
		entry := strings.ToLower(certId)
		if !re.MatchString(entry) {
			return fmt.Errorf("error parsing 'pinned_certs' key %s does not look like lower case hex-encoded sha256 of DER encoded SubjectPublicKeyInfo", entry)
		}
	}
	return nil
}

func validateOptions(o *Options) error {
	if o.LameDuckDuration > 0 && o.LameDuckGracePeriod >= o.LameDuckDuration {
		return fmt.Errorf("lame duck grace period (%v) should be strictly lower than lame duck duration (%v)",
			o.LameDuckGracePeriod, o.LameDuckDuration)
	}
	if int64(o.MaxPayload) > o.MaxPending {
		return fmt.Errorf("max_payload (%v) cannot be higher than max_pending (%v)",
			o.MaxPayload, o.MaxPending)
	}
	if o.ServerName != _EMPTY_ && strings.Contains(o.ServerName, " ") {
		return errors.New("server name cannot contain spaces")
	}
	// Check that the trust configuration is correct.
	if err := validateTrustedOperators(o); err != nil {
		return err
	}
	// Check on leaf nodes which will require a system
	// account when gateways are also configured.
	if err := validateLeafNode(o); err != nil {
		return err
	}
	// Check that authentication is properly configured.
	if err := validateAuth(o); err != nil {
		return err
	}
	// Check that proxies is properly configured.
	if err := validateProxies(o); err != nil {
		return err
	}
	// Check that gateway is properly configured. Returns no error
	// if there is no gateway defined.
	if err := validateGatewayOptions(o); err != nil {
		return err
	}
	// Check that cluster name if defined matches any gateway name.
	if err := validateCluster(o); err != nil {
		return err
	}
	if err := validateMQTTOptions(o); err != nil {
		return err
	}
	if err := validateJetStreamOptions(o); err != nil {
		return err
	}
	// Finally check websocket options.
	return validateWebsocketOptions(o)
}

func (s *Server) getOpts() *Options {
	s.optsMu.RLock()
	opts := s.opts
	s.optsMu.RUnlock()
	return opts
}

func (s *Server) setOpts(opts *Options) {
	s.optsMu.Lock()
	s.opts = opts
	s.optsMu.Unlock()
}

func (s *Server) globalAccount() *Account {
	s.mu.RLock()
	gacc := s.gacc
	s.mu.RUnlock()
	return gacc
}

// Used to setup or update Accounts.
// Returns a map that indicates which accounts have had their stream imports
// changed (in case of an update in configuration reload).
// Lock is held upon entry, but will be released/reacquired in this function.
func (s *Server) configureAccounts(reloading bool) (map[string]struct{}, error) {
	awcsti := make(map[string]struct{})

	// Create the global account.
	if s.gacc == nil {
		s.gacc = NewAccount(globalAccountName)
		s.registerAccountNoLock(s.gacc)
	}

	opts := s.getOpts()

	// We need to track service imports since we can not swap them out (unsub and re-sub)
	// until the proper server struct accounts have been swapped in properly. Doing it in
	// place could lead to data loss or server panic since account under new si has no real
	// account and hence no sublist, so will panic on inbound message.
	siMap := make(map[*Account][][]byte)

	// Check opts and walk through them. We need to copy them here
	// so that we do not keep a real one sitting in the options.
	for _, acc := range opts.Accounts {
		var a *Account
		create := true
		// For the global account, we want to skip the reload process
		// and fall back into the "create" case which will in that
		// case really be just an update (shallowCopy will make sure
		// that mappings are copied over).
		if reloading && acc.Name != globalAccountName {
			if ai, ok := s.accounts.Load(acc.Name); ok {
				a = ai.(*Account)
				// Before updating the account, check if stream imports have changed.
				if !a.checkStreamImportsEqual(acc) {
					awcsti[acc.Name] = struct{}{}
				}
				a.mu.Lock()
				// Collect the sids for the service imports since we are going to
				// replace with new ones.
				var sids [][]byte
				for _, sis := range a.imports.services {
					for _, si := range sis {
						if si.sid != nil {
							sids = append(sids, si.sid)
						}
					}
				}
				// Setup to process later if needed.
				if len(sids) > 0 || len(acc.imports.services) > 0 {
					siMap[a] = sids
				}

				// Now reset all export/imports fields since they are going to be
				// filled in shallowCopy()
				a.imports.streams, a.imports.services = nil, nil
				a.exports.streams, a.exports.services = nil, nil
				// We call shallowCopy from the account `acc` (the one in Options)
				// and pass `a` (our existing account) to get it updated.
				acc.shallowCopy(a)
				a.mu.Unlock()
				create = false
			}
		}
		// Track old mappings if global account.
		var oldGMappings []*mapping
		if create {
			if acc.Name == globalAccountName {
				a = s.gacc
				a.mu.Lock()
				oldGMappings = append(oldGMappings, a.mappings...)
				a.mu.Unlock()
			} else {
				a = NewAccount(acc.Name)
			}
			// Locking matters in the case of an update of the global account
			a.mu.Lock()
			acc.shallowCopy(a)
			a.mu.Unlock()
			// Will be a no-op in case of the global account since it is already registered.
			s.registerAccountNoLock(a)
		}

		// The `acc` account is stored in options, not in the server, and these can be cleared.
		acc.sl, acc.clients, acc.mappings = nil, nil, nil

		// Check here if we have been reloaded and we have a global account with mappings that may have changed.
		// If we have leafnodes they need to be updated.
		if reloading && a == s.gacc {
			a.mu.Lock()
			mappings := make(map[string]*mapping)
			if len(a.mappings) > 0 && a.nleafs > 0 {
				for _, em := range a.mappings {
					mappings[em.src] = em
				}
			}
			a.mu.Unlock()
			if len(mappings) > 0 || len(oldGMappings) > 0 {
				a.lmu.RLock()
				for _, lc := range a.lleafs {
					for _, em := range mappings {
						lc.forceAddToSmap(em.src)
					}
					// Remove any old ones if needed.
					for _, em := range oldGMappings {
						// Only remove if not in the new ones.
						if _, ok := mappings[em.src]; !ok {
							lc.forceRemoveFromSmap(em.src)
						}
					}
				}
				a.lmu.RUnlock()
			}
		}

		// If we see an account defined using $SYS we will make sure that is set as system account.
		if acc.Name == DEFAULT_SYSTEM_ACCOUNT && opts.SystemAccount == _EMPTY_ {
			opts.SystemAccount = DEFAULT_SYSTEM_ACCOUNT
		}
	}

	// Now that we have this we need to remap any referenced accounts in
	// import or export maps to the new ones.
	swapApproved := func(ea *exportAuth) {
		for sub, a := range ea.approved {
			var acc *Account
			if v, ok := s.accounts.Load(a.Name); ok {
				acc = v.(*Account)
			}
			ea.approved[sub] = acc
		}
	}
	var numAccounts int
	s.accounts.Range(func(k, v any) bool {
		numAccounts++
		acc := v.(*Account)
		acc.mu.Lock()
		// Exports
		for _, se := range acc.exports.streams {
			if se != nil {
				swapApproved(&se.exportAuth)
			}
		}
		for _, se := range acc.exports.services {
			if se != nil {
				// Swap over the bound account for service exports.
				if se.acc != nil {
					if v, ok := s.accounts.Load(se.acc.Name); ok {
						se.acc = v.(*Account)
					}
				}
				swapApproved(&se.exportAuth)
			}
		}
		// Imports
		for _, si := range acc.imports.streams {
			if v, ok := s.accounts.Load(si.acc.Name); ok {
				si.acc = v.(*Account)
			}
		}
		for _, sis := range acc.imports.services {
			for _, si := range sis {
				if v, ok := s.accounts.Load(si.acc.Name); ok {
					si.acc = v.(*Account)

					// It is possible to allow for latency tracking inside your
					// own account, so lock only when not the same account.
					if si.acc == acc {
						si.se = si.acc.getServiceExport(si.to)
						continue
					}
					si.acc.mu.RLock()
					si.se = si.acc.getServiceExport(si.to)
					si.acc.mu.RUnlock()
				}
			}
		}
		// Make sure the subs are running, but only if not reloading.
		if len(acc.imports.services) > 0 && acc.ic == nil && !reloading {
			acc.ic = s.createInternalAccountClient()
			acc.ic.acc = acc
			// Need to release locks to invoke this function.
			acc.mu.Unlock()
			s.mu.Unlock()
			acc.addAllServiceImportSubs()
			s.mu.Lock()
			acc.mu.Lock()
		}
		acc.updated = time.Now()
		acc.mu.Unlock()
		return true
	})

	// Check if we need to process service imports pending from above.
	// This processing needs to be after we swap in the real accounts above.
	for acc, sids := range siMap {
		c := acc.ic
		for _, sid := range sids {
			c.processUnsub(sid)
		}
		acc.addAllServiceImportSubs()
		s.mu.Unlock()
		s.registerSystemImports(acc)
		s.mu.Lock()
	}

	// Set the system account if it was configured.
	// Otherwise create a default one.
	if opts.SystemAccount != _EMPTY_ {
		// Lock may be acquired in lookupAccount, so release to call lookupAccount.
		s.mu.Unlock()
		acc, err := s.lookupAccount(opts.SystemAccount)
		s.mu.Lock()
		if err == nil && s.sys != nil && acc != s.sys.account {
			// sys.account.clients (including internal client)/respmap/etc... are transferred separately
			s.sys.account = acc
			s.sysAcc.Store(acc)
		}
		if err != nil {
			return awcsti, fmt.Errorf("error resolving system account: %v", err)
		}

		// If we have defined a system account here check to see if its just us and the $G account.
		// We would do this to add user/pass to the system account. If this is the case add in
		// no-auth-user for $G.
		// Only do this if non-operator mode and we did not have an authorization block defined.
		if len(opts.TrustedOperators) == 0 && numAccounts == 2 && opts.NoAuthUser == _EMPTY_ && !opts.authBlockDefined {
			// If we come here from config reload, let's not recreate the fake user name otherwise
			// it will cause currently clients to be disconnected.
			uname := s.sysAccOnlyNoAuthUser
			if uname == _EMPTY_ {
				// Create a unique name so we do not collide.
				var b [8]byte
				rn := rand.Int64()
				for i, l := 0, rn; i < len(b); i++ {
					b[i] = digits[l%base]
					l /= base
				}
				uname = fmt.Sprintf("nats-%s", b[:])
				s.sysAccOnlyNoAuthUser = uname
			}
			opts.Users = append(opts.Users, &User{Username: uname, Password: uname[6:], Account: s.gacc})
			opts.NoAuthUser = uname
		}
	}

	// Add any required exports from system account.
	if s.sys != nil {
		sysAcc := s.sys.account
		s.mu.Unlock()
		s.addSystemAccountExports(sysAcc)
		s.mu.Lock()
		// On reload the system account's exports were rebuilt from the
		// options above, which dropped the internally added ones (such as
		// the JetStream API export) until addSystemAccountExports put them
		// back. Imports resolved in between were left with a nil export,
		// so re-resolve them now that every export exists again.
		if reloading {
			s.accounts.Range(func(_, v any) bool {
				acc := v.(*Account)
				acc.mu.Lock()
				for _, sis := range acc.imports.services {
					for _, si := range sis {
						if si.se != nil || si.acc == nil {
							continue
						}
						if si.acc == acc {
							si.se = acc.getServiceExport(si.to)
							continue
						}
						si.acc.mu.RLock()
						si.se = si.acc.getServiceExport(si.to)
						si.acc.mu.RUnlock()
					}
				}
				acc.mu.Unlock()
				return true
			})
		}
	}

	return awcsti, nil
}

// Setup the account resolver. For memory resolver, make sure the JWTs are
// properly formed but do not enforce expiration etc.
// Lock is held on entry, but may be released/reacquired during this call.
func (s *Server) configureResolver() error {
	opts := s.getOpts()
	s.accResolver = opts.AccountResolver
	if opts.AccountResolver != nil {
		// For URL resolver, set the TLSConfig if specified.
		if opts.AccountResolverTLSConfig != nil {
			if ar, ok := opts.AccountResolver.(*URLAccResolver); ok {
				if t, ok := ar.c.Transport.(*http.Transport); ok {
					t.CloseIdleConnections()
					t.TLSClientConfig = opts.AccountResolverTLSConfig.Clone()
				}
			}
		}
		if len(opts.resolverPreloads) > 0 {
			// Lock ordering is account resolver -> server, so we need to release
			// the lock and reacquire it when done with account resolver's calls.
			ar := s.accResolver
			s.mu.Unlock()
			defer s.mu.Lock()
			if ar.IsReadOnly() {
				return fmt.Errorf("resolver preloads only available for writeable resolver types MEM/DIR/CACHE_DIR")
			}
			for k, v := range opts.resolverPreloads {
				_, err := jwt.DecodeAccountClaims(v)
				if err != nil {
					return fmt.Errorf("preload account error for %q: %v", k, err)
				}
				ar.Store(k, v)
			}
		}
	}
	return nil
}

// This will check preloads for validation issues.
func (s *Server) checkResolvePreloads() {
	opts := s.getOpts()
	// We can just check the read-only opts versions here, that way we do not need
	// to grab server lock or access s.accResolver.
	for k, v := range opts.resolverPreloads {
		claims, err := jwt.DecodeAccountClaims(v)
		if err != nil {
			s.Errorf("Preloaded account [%s] not valid", k)
			continue
		}
		// Check if it is expired.
		vr := jwt.CreateValidationResults()
		claims.Validate(vr)
		if vr.IsBlocking(true) {
			s.Warnf("Account [%s] has validation issues:", k)
			for _, v := range vr.Issues {
				s.Warnf("  - %s", v.Description)
			}
		}
	}
}

// Determines if we are in pre NATS 2.0 setup with no accounts.
func (s *Server) globalAccountOnly() bool {
	var hasOthers bool

	if s.trustedKeys != nil {
		return false
	}

	s.mu.RLock()
	s.accounts.Range(func(k, v any) bool {
		acc := v.(*Account)
		// Ignore global and system
		if acc == s.gacc || (s.sys != nil && acc == s.sys.account) {
			return true
		}
		hasOthers = true
		return false
	})
	s.mu.RUnlock()

	return !hasOthers
}

// Determines if this server is in standalone mode, meaning no routes or gateways.
func (s *Server) standAloneMode() bool {
	opts := s.getOpts()
	return opts.Cluster.Port == 0 && opts.Gateway.Port == 0
}

func (s *Server) configuredRoutes() int {
	return len(s.getOpts().Routes)
}

// activePeers is used in bootstrapping raft groups like the JetStream meta controller.
func (s *Server) ActivePeers() (peers []string) {
	s.nodeToInfo.Range(func(k, v any) bool {
		si := v.(nodeInfo)
		if !si.offline {
			peers = append(peers, k.(string))
		}
		return true
	})
	return peers
}

// isTrustedIssuer will check that the issuer is a trusted public key.
// This is used to make sure an account was signed by a trusted operator.
func (s *Server) isTrustedIssuer(issuer string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// If we are not running in trusted mode and there is no issuer, that is ok.
	if s.trustedKeys == nil && issuer == _EMPTY_ {
		return true
	}
	for _, tk := range s.trustedKeys {
		if tk == issuer {
			return true
		}
	}
	return false
}

// processTrustedKeys will process binary stamped and
// options-based trusted nkeys. Returns success.
func (s *Server) processTrustedKeys() bool {
	s.strictSigningKeyUsage = map[string]struct{}{}
	opts := s.getOpts()
	if trustedKeys != _EMPTY_ && !s.initStampedTrustedKeys() {
		return false
	} else if opts.TrustedKeys != nil {
		for _, key := range opts.TrustedKeys {
			if !nkeys.IsValidPublicOperatorKey(key) {
				return false
			}
		}
		s.trustedKeys = append([]string(nil), opts.TrustedKeys...)
		for _, claim := range opts.TrustedOperators {
			if !claim.StrictSigningKeyUsage {
				continue
			}
			for _, key := range claim.SigningKeys {
				s.strictSigningKeyUsage[key] = struct{}{}
			}
		}
	}
	return true
}

// checkTrustedKeyString will check that the string is a valid array
// of public operator nkeys.
func checkTrustedKeyString(keys string) []string {
	tks := strings.Fields(keys)
	if len(tks) == 0 {
		return nil
	}
	// Walk all the keys and make sure they are valid.
	for _, key := range tks {
		if !nkeys.IsValidPublicOperatorKey(key) {
			return nil
		}
	}
	return tks
}

// initStampedTrustedKeys will check the stamped trusted keys
// and will set the server field 'trustedKeys'. Returns whether
// it succeeded or not.
func (s *Server) initStampedTrustedKeys() bool {
	// Check to see if we have an override in options, which will cause us to fail.
	if len(s.getOpts().TrustedKeys) > 0 {
		return false
	}
	tks := checkTrustedKeyString(trustedKeys)
	if len(tks) == 0 {
		return false
	}
	s.trustedKeys = tks
	return true
}

// PrintAndDie is exported for access in other packages.
func PrintAndDie(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

// PrintServerAndExit will print our version and exit.
func PrintServerAndExit() {
	fmt.Printf("nats-server: v%s\n", VERSION)
	os.Exit(0)
}

// ProcessCommandLineArgs takes the command line arguments
// validating and setting flags for handling in case any
// sub command was present.
func ProcessCommandLineArgs(cmd *flag.FlagSet) (showVersion bool, showHelp bool, err error) {
	if len(cmd.Args()) > 0 {
		arg := cmd.Args()[0]
		switch strings.ToLower(arg) {
		case "version":
			return true, false, nil
		case "help":
			return false, true, nil
		default:
			return false, false, fmt.Errorf("unrecognized command: %q", arg)
		}
	}

	return false, false, nil
}

// Public version.
func (s *Server) Running() bool {
	return s.isRunning()
}

// Protected check on running state
func (s *Server) isRunning() bool {
	return s.running.Load()
}

func (s *Server) logPid() error {
	pidStr := strconv.Itoa(os.Getpid())
	return os.WriteFile(s.getOpts().PidFile, []byte(pidStr), defaultFilePerms)
}

// numReservedAccounts will return the number of reserved accounts configured in the server.
// Currently this is 1, one for the global default account.
func (s *Server) numReservedAccounts() int {
	return 1
}

// NumActiveAccounts reports number of active accounts on this server.
func (s *Server) NumActiveAccounts() int32 {
	return atomic.LoadInt32(&s.activeAccounts)
}

// incActiveAccounts() just adds one under lock.
func (s *Server) incActiveAccounts() {
	atomic.AddInt32(&s.activeAccounts, 1)
}

// decActiveAccounts() just subtracts one under lock.
func (s *Server) decActiveAccounts() {
	atomic.AddInt32(&s.activeAccounts, -1)
}

// This should be used for testing only. Will be slow since we have to
// range over all accounts in the sync.Map to count.
func (s *Server) numAccounts() int {
	count := 0
	s.mu.RLock()
	s.accounts.Range(func(k, v any) bool {
		count++
		return true
	})
	s.mu.RUnlock()
	return count
}

// NumLoadedAccounts returns the number of loaded accounts.
func (s *Server) NumLoadedAccounts() int {
	return s.numAccounts()
}

// LookupOrRegisterAccount will return the given account if known or create a new entry.
func (s *Server) LookupOrRegisterAccount(name string) (account *Account, isNew bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.accounts.Load(name); ok {
		return v.(*Account), false
	}
	acc := NewAccount(name)
	s.registerAccountNoLock(acc)
	return acc, true
}

// RegisterAccount will register an account. The account must be new
// or this call will fail.
func (s *Server) RegisterAccount(name string) (*Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.accounts.Load(name); ok {
		return nil, ErrAccountExists
	}
	acc := NewAccount(name)
	s.registerAccountNoLock(acc)
	return acc, nil
}

// SetSystemAccount will set the internal system account.
// If root operators are present it will also check validity.
func (s *Server) SetSystemAccount(accName string) error {
	// Lookup from sync.Map first.
	if v, ok := s.accounts.Load(accName); ok {
		return s.setSystemAccount(v.(*Account))
	}

	// If we are here we do not have local knowledge of this account.
	// Do this one by hand to return more useful error.
	ac, jwt, err := s.fetchAccountClaims(accName)
	if err != nil {
		return err
	}
	acc := s.buildInternalAccount(ac)
	acc.claimJWT = jwt
	// Due to race, we need to make sure that we are not
	// registering twice.
	if racc := s.registerAccount(acc); racc != nil {
		return nil
	}
	return s.setSystemAccount(acc)
}

// SystemAccount returns the system account if set.
func (s *Server) SystemAccount() *Account {
	return s.sysAcc.Load()
}

// GlobalAccount returns the global account.
// Default clients will use the global account.
func (s *Server) GlobalAccount() *Account {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gacc
}

// SetDefaultSystemAccount will create a default system account if one is not present.
func (s *Server) SetDefaultSystemAccount() error {
	if _, isNew := s.LookupOrRegisterAccount(DEFAULT_SYSTEM_ACCOUNT); !isNew {
		return nil
	}
	s.Debugf("Created system account: %q", DEFAULT_SYSTEM_ACCOUNT)
	return s.SetSystemAccount(DEFAULT_SYSTEM_ACCOUNT)
}

// Assign a system account. Should only be called once.
// This sets up a server to send and receive messages from
// inside the server itself.
func (s *Server) setSystemAccount(acc *Account) error {
	if acc == nil {
		return ErrMissingAccount
	}
	// Don't try to fix this here.
	if acc.IsExpired() {
		return ErrAccountExpired
	}
	// If we are running with trusted keys for an operator
	// make sure we check the account is legit.
	if !s.isTrustedIssuer(acc.Issuer) {
		return ErrAccountValidation
	}

	s.mu.Lock()

	if s.sys != nil {
		s.mu.Unlock()
		return ErrAccountExists
	}

	// This is here in an attempt to quiet the race detector and not have to place
	// locks on fast path for inbound messages and checking service imports.
	acc.mu.Lock()
	if acc.imports.services == nil {
		acc.imports.services = make(map[string][]*serviceImport)
	}
	acc.mu.Unlock()

	s.sys = &internal{
		account: acc,
		client:  s.createInternalSystemClient(),
		seq:     1,
		sid:     1,
		servers: make(map[string]*serverUpdate),
		replies: make(map[string]msgHandler),
		sendq:   newIPQueue[*pubMsg](s, "System sendQ"),
		recvq:   newIPQueue[*inSysMsg](s, "System recvQ"),
		recvqp:  newIPQueue[*inSysMsg](s, "System recvQ Pings"),
		resetCh: make(chan struct{}),
		sq:      s.newSendQ(acc),
		statsz:  statsHBInterval,
		orphMax: 5 * eventsHBInterval,
		chkOrph: 3 * eventsHBInterval,
	}
	recvq, recvqp := s.sys.recvq, s.sys.recvqp
	s.sys.wg.Add(1)
	s.mu.Unlock()

	// Store in atomic for fast lookup.
	s.sysAcc.Store(acc)

	// Register with the account.
	s.sys.client.registerWithAccount(acc)

	s.addSystemAccountExports(acc)

	// Start our internal loop to serialize outbound messages.
	// We do our own wg here since we will stop first during shutdown.
	go s.internalSendLoop(&s.sys.wg)

	// Start the internal loop for inbound messages.
	go s.internalReceiveLoop(recvq)
	// Start the internal loop for inbound STATSZ/Ping messages.
	go s.internalReceiveLoop(recvqp)

	// Start up our general subscriptions
	s.initEventTracking()

	// Track for dead remote servers.
	s.wrapChk(s.startRemoteServerSweepTimer)()

	// Send out statsz updates periodically.
	s.wrapChk(s.startStatszTimer)()

	// If we have existing accounts make sure we enable account tracking.
	s.mu.Lock()
	s.accounts.Range(func(k, v any) bool {
		acc := v.(*Account)
		s.enableAccountTracking(acc)
		return true
	})
	s.mu.Unlock()

	return nil
}

// Creates an internal system client.
func (s *Server) createInternalSystemClient() *client {
	return s.createInternalClient(SYSTEM)
}

// Creates an internal jetstream client.
func (s *Server) createInternalJetStreamClient() *client {
	return s.createInternalClient(JETSTREAM)
}

// Creates an internal client for Account.
func (s *Server) createInternalAccountClient() *client {
	return s.createInternalClient(ACCOUNT)
}

// Internal clients. kind should be SYSTEM, JETSTREAM or ACCOUNT
func (s *Server) createInternalClient(kind int) *client {
	if !isInternalClient(kind) {
		return nil
	}
	now := time.Now()
	c := &client{srv: s, kind: kind, opts: internalOpts, msubs: -1, mpay: -1, start: now, last: now}
	c.initClient()
	c.echo = false
	c.headers = true
	c.flags.set(noReconnect)
	return c
}

// Determine if accounts should track subscriptions for
// efficient propagation.
// Lock should be held on entry.
func (s *Server) shouldTrackSubscriptions() bool {
	opts := s.getOpts()
	return (opts.Cluster.Port != 0 || opts.Gateway.Port != 0)
}

// Invokes registerAccountNoLock under the protection of the server lock.
// That is, server lock is acquired/released in this function.
// See registerAccountNoLock for comment on returned value.
func (s *Server) registerAccount(acc *Account) *Account {
	s.mu.Lock()
	racc := s.registerAccountNoLock(acc)
	s.mu.Unlock()
	return racc
}

// Helper to set the sublist based on preferences.
func (s *Server) setAccountSublist(acc *Account) {
	if acc != nil && acc.sl == nil {
		acc.sl = NewSublistForServer(s)
	}
}

// Registers an account in the server.
// Due to some locking considerations, we may end-up trying
// to register the same account twice. This function will
// then return the already registered account.
// Lock should be held on entry.
func (s *Server) registerAccountNoLock(acc *Account) *Account {
	// We are under the server lock. Lookup from map, if present
	// return existing account.
	if a, _ := s.accounts.Load(acc.Name); a != nil {
		s.tmpAccounts.Delete(acc.Name)
		return a.(*Account)
	}
	// Finish account setup and store.
	s.setAccountSublist(acc)

	acc.mu.Lock()
	s.setRouteInfo(acc)
	if acc.clients == nil {
		acc.clients = make(map[*client]struct{})
	}

	// If we are capable of routing we will track subscription
	// information for efficient interest propagation.
	// During config reload, it is possible that account was
	// already created (global account), so use locking and
	// make sure we create only if needed.
	// TODO(dlc)- Double check that we need this for GWs.
	if acc.rm == nil && s.opts != nil && s.shouldTrackSubscriptions() {
		acc.rm = make(map[string]int64)
		acc.lws = make(map[string]int32)
	}
	acc.srv = s
	acc.updated = time.Now()
	accName := acc.Name
	jsEnabled := len(acc.jsLimits) > 0
	acc.mu.Unlock()

	if opts := s.getOpts(); opts != nil && len(opts.JsAccDefaultDomain) > 0 {
		if defDomain, ok := opts.JsAccDefaultDomain[accName]; ok {
			if jsEnabled {
				s.Warnf("Skipping Default Domain %q, set for JetStream enabled account %q", defDomain, accName)
			} else if defDomain != _EMPTY_ {
				for src, dest := range generateJSMappingTable(defDomain) {
					// flip src and dest around so the domain is inserted
					s.Noticef("Adding default domain mapping %q -> %q to account %q %p", dest, src, accName, acc)
					if err := acc.AddMapping(dest, src); err != nil {
						s.Errorf("Error adding JetStream default domain mapping: %v", err)
					}
				}
			}
		}
	}

	s.accounts.Store(acc.Name, acc)
	s.tmpAccounts.Delete(acc.Name)
	s.enableAccountTracking(acc)

	// Can not have server lock here.
	s.mu.Unlock()
	s.registerSystemImports(acc)
	// Starting 2.9.0, we are phasing out the optimistic mode, so change
	// the account to interest-only mode (except if instructed not to do
	// it in some tests).
	if s.gateway.enabled && !gwDoNotForceInterestOnlyMode {
		s.switchAccountToInterestMode(acc.GetName())
	}
	s.mu.Lock()

	return nil
}

// Sets the account's routePoolIdx depending on presence or not of
// pooling or per-account routes. Also updates a map used by
// gateway code to retrieve a route based on some route hash.
//
// Both Server and Account lock held on entry.
func (s *Server) setRouteInfo(acc *Account) {
	// If there is a dedicated route configured for this account
	if _, ok := s.accRoutes[acc.Name]; ok {
		// We want the account name to be in the map, but we don't
		// need a value (we could store empty string)
		s.accRouteByHash.Store(acc.Name, nil)
		// Set the route pool index to -1 so that it is easy when
		// ranging over accounts to exclude those accounts when
		// trying to get accounts for a given pool index.
		acc.routePoolIdx = accDedicatedRoute
	} else {
		// If pool size more than 1, we will compute a hash code and
		// use modulo to assign to an index of the pool slice. For 1
		// and below, all accounts will be bound to the single connection
		// at index 0.
		acc.routePoolIdx = computeRoutePoolIdx(s.routesPoolSize, acc.Name)
		if s.routesPoolSize > 1 {
			s.accRouteByHash.Store(acc.Name, acc.routePoolIdx)
		}
	}
}

// lookupAccount is a function to return the account structure
// associated with an account name.
// Lock MUST NOT be held upon entry.
func (s *Server) lookupAccount(name string) (*Account, error) {
	return s.lookupOrFetchAccount(name, true)
}

// lookupOrFetchAccount is a function to return the account structure
// associated with an account name.
// Lock MUST NOT be held upon entry.
func (s *Server) lookupOrFetchAccount(name string, fetch bool) (*Account, error) {
	var acc *Account
	if v, ok := s.accounts.Load(name); ok {
		acc = v.(*Account)
	}
	if acc != nil {
		// If we are expired and we have a resolver, then
		// return the latest information from the resolver.
		if acc.IsExpired() {
			s.Debugf("Requested account [%s] has expired", name)
			if s.AccountResolver() != nil && fetch {
				if err := s.updateAccount(acc); err != nil {
					// This error could mask expired, so just return expired here.
					return nil, ErrAccountExpired
				}
			} else {
				return nil, ErrAccountExpired
			}
		}
		return acc, nil
	}
	// If we have a resolver see if it can fetch the account.
	if s.AccountResolver() == nil || !fetch {
		return nil, ErrMissingAccount
	}
	return s.fetchAccount(name)
}

// LookupAccount is a public function to return the account structure
// associated with name.
func (s *Server) LookupAccount(name string) (*Account, error) {
	return s.lookupAccount(name)
}

// This will fetch new claims and if found update the account with new claims.
// Lock MUST NOT be held upon entry.
func (s *Server) updateAccount(acc *Account) error {
	acc.mu.RLock()
	// TODO(dlc) - Make configurable
	if !acc.incomplete && time.Since(acc.updated) < time.Second {
		acc.mu.RUnlock()
		s.Debugf("Requested account update for [%s] ignored, too soon", acc.Name)
		return ErrAccountResolverUpdateTooSoon
	}
	acc.mu.RUnlock()
	claimJWT, err := s.fetchRawAccountClaims(acc.Name)
	if err != nil {
		return err
	}
	return s.updateAccountWithClaimJWT(acc, claimJWT)
}

// updateAccountWithClaimJWT will check and apply the claim update.
// Lock MUST NOT be held upon entry.
func (s *Server) updateAccountWithClaimJWT(acc *Account, claimJWT string) error {
	if acc == nil {
		return ErrMissingAccount
	}
	acc.mu.RLock()
	sameClaim := acc.claimJWT != _EMPTY_ && acc.claimJWT == claimJWT && !acc.incomplete
	acc.mu.RUnlock()
	if sameClaim {
		s.Debugf("Requested account update for [%s], same claims detected", acc.Name)
		return nil
	}
	accClaims, _, err := s.verifyAccountClaims(claimJWT)
	if err == nil && accClaims != nil {
		acc.mu.Lock()
		// if an account is updated with a different operator signing key, we want to
		// show a consistent issuer.
		acc.Issuer = accClaims.Issuer
		if acc.Name != accClaims.Subject {
			acc.mu.Unlock()
			return ErrAccountValidation
		}
		acc.mu.Unlock()
		s.UpdateAccountClaims(acc, accClaims)
		acc.mu.Lock()
		// needs to be set after update completed.
		// This causes concurrent calls to return with sameClaim=true if the change is effective.
		acc.claimJWT = claimJWT
		acc.mu.Unlock()
		return nil
	}
	return err
}

// fetchRawAccountClaims will grab raw account claims iff we have a resolver.
// Lock is NOT held upon entry.
func (s *Server) fetchRawAccountClaims(name string) (string, error) {
	accResolver := s.AccountResolver()
	if accResolver == nil {
		return _EMPTY_, ErrNoAccountResolver
	}
	// Need to do actual Fetch
	start := time.Now()
	claimJWT, err := fetchAccount(accResolver, name)
	fetchTime := time.Since(start)
	if fetchTime > time.Second {
		s.Warnf("Account [%s] fetch took %v", name, fetchTime)
	} else {
		s.Debugf("Account [%s] fetch took %v", name, fetchTime)
	}
	if err != nil {
		s.Warnf("Account fetch failed: %v", err)
		return "", err
	}
	return claimJWT, nil
}

// fetchAccountClaims will attempt to fetch new claims if a resolver is present.
// Lock is NOT held upon entry.
func (s *Server) fetchAccountClaims(name string) (*jwt.AccountClaims, string, error) {
	claimJWT, err := s.fetchRawAccountClaims(name)
	if err != nil {
		return nil, _EMPTY_, err
	}
	var claim *jwt.AccountClaims
	claim, claimJWT, err = s.verifyAccountClaims(claimJWT)
	if claim != nil && claim.Subject != name {
		return nil, _EMPTY_, ErrAccountValidation
	}
	return claim, claimJWT, err
}

// verifyAccountClaims will decode and validate any account claims.
func (s *Server) verifyAccountClaims(claimJWT string) (*jwt.AccountClaims, string, error) {
	accClaims, err := jwt.DecodeAccountClaims(claimJWT)
	if err != nil {
		return nil, _EMPTY_, err
	}
	if !s.isTrustedIssuer(accClaims.Issuer) {
		return nil, _EMPTY_, ErrAccountValidation
	}
	vr := jwt.CreateValidationResults()
	accClaims.Validate(vr)
	if vr.IsBlocking(true) {
		return nil, _EMPTY_, ErrAccountValidation
	}
	return accClaims, claimJWT, nil
}

// This will fetch an account from a resolver if defined.
// Lock is NOT held upon entry.
func (s *Server) fetchAccount(name string) (*Account, error) {
	accClaims, claimJWT, err := s.fetchAccountClaims(name)
	if accClaims == nil {
		return nil, err
	}
	acc := s.buildInternalAccount(accClaims)
	// Due to possible race, if registerAccount() returns a non
	// nil account, it means the same account was already
	// registered and we should use this one.
	if racc := s.registerAccount(acc); racc != nil {
		// Update with the new claims in case they are new.
		if err = s.updateAccountWithClaimJWT(racc, claimJWT); err != nil {
			return nil, err
		}
		return racc, nil
	}
	// The sub imports may have been setup but will not have had their
	// subscriptions properly setup. Do that here.
	var needImportSubs bool

	acc.mu.Lock()
	acc.claimJWT = claimJWT
	if len(acc.imports.services) > 0 {
		if acc.ic == nil {
			acc.ic = s.createInternalAccountClient()
			acc.ic.acc = acc
		}
		needImportSubs = true
	}
	acc.mu.Unlock()

	// Do these outside the lock.
	if needImportSubs {
		acc.addAllServiceImportSubs()
	}

	return acc, nil
}

// Start up the server, this will not block.
//
// WaitForShutdown can be used to block and wait for the server to shutdown properly if needed
// after calling s.Shutdown()
func (s *Server) Start() {
	s.Noticef("Starting nats-server")

	gc := gitCommit
	if gc == _EMPTY_ {
		gc = "not set"
	}

	// Snapshot server options.
	opts := s.getOpts()

	// Capture if this server is a leaf that has no cluster, so we don't
	// display the cluster name if that is the case.
	s.mu.RLock()
	leafNoCluster := s.leafNoCluster
	s.mu.RUnlock()

	var clusterName string
	if !leafNoCluster {
		clusterName = s.ClusterName()
	}

	s.Noticef("  Version:  %s", VERSION)
	s.Noticef("  Git:      [%s]", gc)
	s.Debugf("  Go build: %s", s.info.GoVersion)
	if clusterName != _EMPTY_ {
		s.Noticef("  Cluster:  %s", clusterName)
	}
	s.Noticef("  Name:     %s", s.info.Name)
	if opts.JetStream {
		s.Noticef("  Node:     %s", getHash(s.info.Name))
	}
	s.Noticef("  ID:       %s", s.info.ID)
	s.printFeatureFlags(opts)

	defer s.Noticef("Server is ready")

	// Check for insecure configurations.
	s.checkAuthforWarnings()

	// Avoid RACE between Start() and Shutdown()
	s.running.Store(true)
	s.mu.Lock()
	// Update leafNodeEnabled in case options have changed post NewServer()
	// and before Start() (we should not be able to allow that, but server has
	// direct reference to user-provided options - at least before a Reload() is
	// performed.
	s.leafNodeEnabled = opts.LeafNode.Port != 0 || len(opts.LeafNode.Remotes) > 0
	s.mu.Unlock()

	s.grMu.Lock()
	s.grRunning = true
	s.grMu.Unlock()

	s.startRateLimitLogExpiration()

	// Pprof http endpoint for the profiler.
	if opts.ProfPort != 0 {
		s.StartProfiler()
	} else {
		// It's still possible to access this profile via a SYS endpoint, so set
		// this anyway. (Otherwise StartProfiler would have called it.)
		s.setBlockProfileRate(opts.ProfBlockRate)
	}

	if opts.ConfigFile != _EMPTY_ {
		var cd string
		if opts.configDigest != "" {
			cd = fmt.Sprintf("(%s)", opts.configDigest)
		}
		s.Noticef("Using configuration file: %s %s", opts.ConfigFile, cd)
	}

	hasOperators := len(opts.TrustedOperators) > 0
	if hasOperators {
		s.Noticef("Trusted Operators")
	}
	for _, opc := range opts.TrustedOperators {
		s.Noticef("  System  : %q", opc.Audience)
		s.Noticef("  Operator: %q", opc.Name)
		s.Noticef("  Issued  : %v", time.Unix(opc.IssuedAt, 0))
		switch opc.Expires {
		case 0:
			s.Noticef("  Expires : Never")
		default:
			s.Noticef("  Expires : %v", time.Unix(opc.Expires, 0))
		}
	}
	if hasOperators && opts.SystemAccount == _EMPTY_ {
		s.Warnf("Trusted Operators should utilize a System Account")
	}
	if opts.MaxPayload > MAX_PAYLOAD_MAX_SIZE {
		s.Warnf("Maximum payloads over %v are generally discouraged and could lead to poor performance",
			friendlyBytes(int64(MAX_PAYLOAD_MAX_SIZE)))
	}

	if len(opts.JsAccDefaultDomain) > 0 {
		s.Warnf("The option `default_js_domain` is a temporary backwards compatibility measure and will be removed")
	}

	// If we have a memory resolver, check the accounts here for validation exceptions.
	// This allows them to be logged right away vs when they are accessed via a client.
	if hasOperators && len(opts.resolverPreloads) > 0 {
		s.checkResolvePreloads()
	}

	// Log the pid to a file.
	if opts.PidFile != _EMPTY_ {
		if err := s.logPid(); err != nil {
			s.Fatalf("Could not write pidfile: %v", err)
			return
		}
	}

	// Setup system account which will start the eventing stack.
	if sa := opts.SystemAccount; sa != _EMPTY_ {
		if err := s.SetSystemAccount(sa); err != nil {
			s.Fatalf("Can't set system account: %v", err)
			return
		}
	} else if !opts.NoSystemAccount {
		// We will create a default system account here.
		s.SetDefaultSystemAccount()
	}

	// Start monitoring before enabling other subsystems of the
	// server to be able to monitor during startup.
	if err := s.StartMonitoring(); err != nil {
		s.Fatalf("Can't start monitoring: %v", err)
		return
	}

	// Start up resolver machinery.
	if ar := s.AccountResolver(); ar != nil {
		if err := ar.Start(s); err != nil {
			s.Fatalf("Could not start resolver: %v", err)
			return
		}
		// In operator mode, when the account resolver depends on an external system and
		// the system account is the bootstrapping account, start fetching it.
		if len(opts.TrustedOperators) == 1 && opts.SystemAccount != _EMPTY_ && opts.SystemAccount != DEFAULT_SYSTEM_ACCOUNT {
			opts := s.getOpts()
			_, isMemResolver := ar.(*MemAccResolver)
			if v, ok := s.accounts.Load(opts.SystemAccount); !isMemResolver && ok && v.(*Account).claimJWT == _EMPTY_ {
				s.Noticef("Using bootstrapping system account")
				s.startGoRoutine(func() {
					defer s.grWG.Done()
					t := time.NewTicker(time.Second)
					defer t.Stop()
					for {
						select {
						case <-s.quitCh:
							return
						case <-t.C:
							sacc := s.SystemAccount()
							if claimJWT, err := fetchAccount(ar, opts.SystemAccount); err != nil {
								continue
							} else if err = s.updateAccountWithClaimJWT(sacc, claimJWT); err != nil {
								continue
							}
							s.Noticef("System account fetched and updated")
							return
						}
					}
				})
			}
		}
	}

	// Start expiration of mapped GW replies, regardless if
	// this server is configured with gateway or not.
	s.startGWReplyMapExpiration()

	// Check if JetStream has been enabled. This needs to be after
	// the system account setup above. JetStream will create its
	// own system account if one is not present.
	if opts.JetStream {
		// Make sure someone is not trying to enable on the system account.
		if sa := s.SystemAccount(); sa != nil && len(sa.jsLimits) > 0 {
			s.Fatalf("Not allowed to enable JetStream on the system account")
		}
		cfg := &JetStreamConfig{
			StoreDir:     opts.StoreDir,
			SyncInterval: opts.SyncInterval,
			SyncAlways:   opts.SyncAlways,
			Strict:       !opts.NoJetStreamStrict,
			MaxMemory:    opts.JetStreamMaxMemory,
			MaxStore:     opts.JetStreamMaxStore,
			Domain:       opts.JetStreamDomain,
			CompressOK:   true,
			UniqueTag:    opts.JetStreamUniqueTag,
		}
		if err := s.EnableJetStream(cfg); err != nil {
			s.Fatalf("Can't start JetStream: %v", err)
			return
		}
	} else {
		// Check to see if any configured accounts have JetStream enabled.
		sa, ga := s.SystemAccount(), s.GlobalAccount()
		var hasSys, hasGlobal bool
		var total int

		s.accounts.Range(func(k, v any) bool {
			total++
			acc := v.(*Account)
			if acc == sa {
				hasSys = true
			} else if acc == ga {
				hasGlobal = true
			}
			acc.mu.RLock()
			hasJs := len(acc.jsLimits) > 0
			acc.mu.RUnlock()
			if hasJs {
				s.checkJetStreamExports()
				acc.enableAllJetStreamServiceImportsAndMappings()
			}
			return true
		})
		// If we only have the system account and the global account and we are not standalone,
		// go ahead and enable JS on $G in case we are in simple mixed mode setup.
		if total == 2 && hasSys && hasGlobal && !s.standAloneMode() {
			ga.mu.Lock()
			ga.jsLimits = map[string]JetStreamAccountLimits{
				_EMPTY_: dynamicJSAccountLimits,
			}
			ga.mu.Unlock()
			s.checkJetStreamExports()
			ga.enableAllJetStreamServiceImportsAndMappings()
		}
	}

	// Delayed API response handling. Start regardless of JetStream being
	// currently configured or not (since it can be enabled/disabled with
	// configuration reload).
	s.startGoRoutine(s.delayedAPIResponder)

	// Start OCSP Stapling monitoring for TLS certificates if enabled. Hook TLS handshake for
	// OCSP check on peers (LEAF and CLIENT kind) if enabled.
	s.startOCSPMonitoring()

	// Configure OCSP Response Cache for peer OCSP checks if enabled.
	s.initOCSPResponseCache()

	// Start up gateway if needed. Do this before starting the routes, because
	// we want to resolve the gateway host:port so that this information can
	// be sent to other routes.
	if opts.Gateway.Port != 0 {
		s.startGateways()
	}

	// Start websocket server if needed. Do this before starting the routes, and
	// leaf node because we want to resolve the gateway host:port so that this
	// information can be sent to other routes.
	if opts.Websocket.Port != 0 {
		s.startWebsocketServer()
	}

	// Start up listen if we want to accept leaf node connections.
	if opts.LeafNode.Port != 0 {
		// Will resolve or assign the advertise address for the leafnode listener.
		// We need that in StartRouting().
		s.startLeafNodeAcceptLoop()
	}

	// Solicit remote servers for leaf node connections.
	if len(opts.LeafNode.Remotes) > 0 {
		s.solicitLeafNodeRemotes(opts.LeafNode.Remotes)
	}

	// TODO (ik): I wanted to refactor this by starting the client
	// accept loop first, that is, it would resolve listen spec
	// in place, but start the accept-for-loop in a different go
	// routine. This would get rid of the synchronization between
	// this function and StartRouting, which I also would have wanted
	// to refactor, but both AcceptLoop() and StartRouting() have
	// been exported and not sure if that would break users using them.
	// We could mark them as deprecated and remove in a release or two...

	// The Routing routine needs to wait for the client listen
	// port to be opened and potential ephemeral port selected.
	clientListenReady := make(chan struct{})

	// MQTT
	if opts.MQTT.Port != 0 {
		s.startMQTT()
	}

	// Start up routing as well if needed.
	if opts.Cluster.Port != 0 {
		s.startGoRoutine(func() {
			s.StartRouting(clientListenReady)
		})
	}

	if opts.PortsFileDir != _EMPTY_ {
		s.logPorts()
	}

	if opts.TLSRateLimit > 0 {
		s.startGoRoutine(s.logRejectedTLSConns)
	}

	// We've finished starting up.
	close(s.startupComplete)

	// Wait for clients.
	if !opts.DontListen {
		s.AcceptLoop(clientListenReady)
	}

	// Bring OSCP Response cache online after accept loop started in anticipation of NATS-enabled cache types
	s.startOCSPResponseCache()
}

func (s *Server) isShuttingDown() bool {
	return s.shutdown.Load()
}

// Shutdown will shutdown the server instance by kicking out the AcceptLoop
// and closing all associated clients.
func (s *Server) Shutdown() {
	if s == nil {
		return
	}
	// Prevent issues with multiple calls.
	if !s.shutdown.CompareAndSwap(false, true) {
		return
	}
	// This is for JetStream R1 Pull Consumers to allow signaling
	// that pending pull requests are invalid.
	s.signalPullConsumers()

	s.mu.Lock()
	clients := make([]*client, 0, len(s.clients))
	for _, c := range s.clients {
		clients = append(clients, c)
	}
	s.mu.Unlock()

	// Client cleanup can publish MQTT Wills, so keep JetStream, eventing,
	// and inter-server connections available until it has finished.
	for _, c := range clients {
		c.closeConnection(ServerShutdown)
	}
	if opts := s.getOpts(); opts.MQTT.Port != 0 {
		timeout := opts.MQTT.JSAPITimeout
		if timeout == 0 {
			timeout = mqttDefaultJSAPITimeout
		}
		done := make(chan struct{})
		go func() {
			s.mqtt.readLoopWG.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(timeout):
			s.Warnf("Timed out waiting for MQTT clients to close")
		}
	}

	// Transfer off any raft nodes that we are a leader by stepping them down.
	s.stepdownRaftNodes()

	// Shutdown the eventing system as needed.
	// This is done first to send out any messages for
	// account status. We will also clean up any
	// eventing items associated with accounts.
	s.shutdownEventing()

	s.mu.Lock()
	s.Noticef("Initiating Shutdown...")

	accRes := s.accResolver

	opts := s.getOpts()

	s.running.Store(false)
	s.grMu.Lock()
	s.grRunning = false
	s.grMu.Unlock()
	s.mu.Unlock()

	if accRes != nil {
		accRes.Close()
	}

	// Now check and shutdown jetstream.
	s.shutdownJetStream()

	// Now shutdown the nodes
	s.shutdownRaftNodes()

	s.mu.Lock()
	conns := make(map[uint64]*client)

	// Copy off the clients
	for i, c := range s.clients {
		conns[i] = c
	}
	// Copy off the connections that are not yet registered
	// in s.routes, but for which the readLoop has started
	s.grMu.Lock()
	for i, c := range s.grTmpClients {
		conns[i] = c
	}
	s.grMu.Unlock()
	// Copy off the routes
	s.forEachRoute(func(r *client) {
		r.mu.Lock()
		conns[r.cid] = r
		r.mu.Unlock()
	})
	// Copy off the gateways
	s.getAllGatewayConnections(conns)

	// Copy off the leaf nodes
	for i, c := range s.leafs {
		conns[i] = c
	}

	// Number of done channel responses we expect.
	doneExpected := 0

	// Kick client AcceptLoop()
	if s.listener != nil {
		doneExpected++
		s.listener.Close()
		s.listener = nil
	}

	// Kick websocket server
	doneExpected += s.closeWebsocketServer()

	// Kick MQTT accept loop
	if s.mqtt.listener != nil {
		doneExpected++
		s.mqtt.listener.Close()
		s.mqtt.listener = nil
	}

	// Kick leafnodes AcceptLoop()
	if s.leafNodeListener != nil {
		doneExpected++
		s.leafNodeListener.Close()
		s.leafNodeListener = nil
	}

	// Kick route AcceptLoop()
	if s.routeListener != nil {
		doneExpected++
		s.routeListener.Close()
		s.routeListener = nil
	}

	// Kick Gateway AcceptLoop()
	if s.gatewayListener != nil {
		doneExpected++
		s.gatewayListener.Close()
		s.gatewayListener = nil
	}

	// Kick HTTP monitoring if its running
	if s.http != nil {
		doneExpected++
		s.http.Close()
		s.http = nil
	}

	// Kick Profiling if its running
	if s.profiler != nil {
		doneExpected++
		s.profiler.Close()
	}

	s.mu.Unlock()

	// Release go routines that wait on that channel
	close(s.quitCh)

	// Close client and route connections
	for _, c := range conns {
		c.setNoReconnect()
		c.closeConnection(ServerShutdown)
	}

	// Block until the accept loops exit
	for doneExpected > 0 {
		<-s.done
		doneExpected--
	}

	// Wait for go routines to be done.
	s.grWG.Wait()

	if opts.PortsFileDir != _EMPTY_ {
		s.deletePortsFile(opts.PortsFileDir)
	}

	s.Noticef("Server Exiting..")

	// Stop OCSP Response Cache
	if s.ocsprc != nil {
		s.ocsprc.Stop(s)
	}

	// Close logger if applicable. It allows tests on Windows
	// to be able to do proper cleanup (delete log file).
	s.logging.RLock()
	log := s.logging.logger
	s.logging.RUnlock()
	if log != nil {
		if l, ok := log.(*logger.Logger); ok {
			l.Close()
		}
	}
	// Notify that the shutdown is complete
	close(s.shutdownComplete)
}

// Close the websocket server if running. If so, returns 1, else 0.
// Server lock held on entry.
func (s *Server) closeWebsocketServer() int {
	ws := &s.websocket
	ws.mu.Lock()
	hs := ws.server
	if hs != nil {
		ws.server = nil
		ws.listener = nil
	}
	ws.mu.Unlock()
	if hs != nil {
		hs.Close()
		return 1
	}
	return 0
}

// WaitForShutdown will block until the server has been fully shutdown.
func (s *Server) WaitForShutdown() {
	<-s.shutdownComplete
}

// AcceptLoop is exported for easier testing.
func (s *Server) AcceptLoop(clr chan struct{}) {
	// If we were to exit before the listener is setup properly,
	// make sure we close the channel.
	defer func() {
		if clr != nil {
			close(clr)
		}
	}()

	if s.isShuttingDown() {
		return
	}

	// Snapshot server options.
	opts := s.getOpts()

	// Setup state that can enable shutdown
	s.mu.Lock()
	hp := net.JoinHostPort(opts.Host, strconv.Itoa(opts.Port))
	l, e := s.getServerListener(hp)
	s.listenerErr = e
	if e != nil {
		s.mu.Unlock()
		s.Fatalf("Error listening on port: %s, %q", hp, e)
		return
	}
	s.Noticef("Listening for client connections on %s",
		net.JoinHostPort(opts.Host, strconv.Itoa(l.Addr().(*net.TCPAddr).Port)))

	// Alert if PROXY protocol is enabled
	if opts.ProxyProtocol {
		s.Noticef("PROXY protocol enabled for client connections")
	}

	// Alert of TLS enabled.
	if opts.TLSConfig != nil {
		// "TLS Handshake First" without a fallback delay always requires the
		// handshake, which overrides "allow_non_tls".
		tlsHandshakeFirstOnly := opts.TLSHandshakeFirst && opts.TLSHandshakeFirstFallback == 0
		if opts.AllowNonTLS && !tlsHandshakeFirstOnly {
			s.Noticef("TLS available for client connections")
		} else {
			s.Noticef("TLS required for client connections")
		}
		if tlsHandshakeFirstOnly {
			s.Warnf("Clients that are not using \"TLS Handshake First\" option will fail to connect")
		}
	}

	// If server was started with RANDOM_PORT (-1), opts.Port would be equal
	// to 0 at the beginning this function. So we need to get the actual port
	if opts.Port == 0 {
		// Write resolved port back to options.
		opts.Port = l.Addr().(*net.TCPAddr).Port
	}

	// Now that port has been set (if it was set to RANDOM), set the
	// server's info Host/Port with either values from Options or
	// ClientAdvertise.
	if err := s.setInfoHostPort(); err != nil {
		s.Fatalf("Error setting server INFO with ClientAdvertise value of %s, err=%v", opts.ClientAdvertise, err)
		l.Close()
		s.mu.Unlock()
		return
	}
	// Keep track of client connect URLs. We may need them later.
	s.clientConnectURLs = s.getClientConnectURLs()
	s.listener = l

	go s.acceptConnections(l, "Client", func(conn net.Conn) { s.createClient(conn) },
		func(_ error) bool {
			if s.isLameDuckMode() {
				// Signal that we are not accepting new clients
				s.ldmCh <- true
				// Now wait for the Shutdown...
				<-s.quitCh
				return true
			}
			return false
		})
	s.mu.Unlock()

	// Let the caller know that we are ready
	close(clr)
	clr = nil
}

// getServerListener returns a network listener for the given host-port address.
// If the Server already has an active listener (s.listener), it returns that listener
// along with any previous error (s.listenerErr). Otherwise, it creates and returns
// a new TCP listener on the specified address using natsListen.
func (s *Server) getServerListener(hp string) (net.Listener, error) {
	if s.listener != nil {
		return s.listener, s.listenerErr
	}

	return natsListen("tcp", hp)
}

// InProcessConn returns an in-process connection to the server,
// avoiding the need to use a TCP listener for local connectivity
// within the same process. This can be used regardless of the
// state of the DontListen option.
func (s *Server) InProcessConn() (net.Conn, error) {
	pl, pr := net.Pipe()
	if !s.startGoRoutine(func() {
		s.createClientInProcess(pl)
		s.grWG.Done()
	}) {
		pl.Close()
		pr.Close()
		return nil, fmt.Errorf("failed to create connection")
	}
	return pr, nil
}

func (s *Server) acceptConnections(l net.Listener, acceptName string, createFunc func(conn net.Conn), errFunc func(err error) bool) {
	tmpDelay := ACCEPT_MIN_SLEEP

	for {
		conn, err := l.Accept()
		if err != nil {
			if errFunc != nil && errFunc(err) {
				return
			}
			if tmpDelay = s.acceptError(acceptName, err, tmpDelay); tmpDelay < 0 {
				break
			}
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP
		if !s.startGoRoutine(func() {
			s.reloadMu.RLock()
			createFunc(conn)
			s.reloadMu.RUnlock()
			s.grWG.Done()
		}) {
			conn.Close()
		}
	}
	s.Debugf(acceptName + " accept loop exiting..")
	s.done <- true
}

// This function sets the server's info Host/Port based on server Options.
// Note that this function may be called during config reload, this is why
// Host/Port may be reset to original Options if the ClientAdvertise option
// is not set (since it may have previously been).
func (s *Server) setInfoHostPort() error {
	// When this function is called, opts.Port is set to the actual listen
	// port (if option was originally set to RANDOM), even during a config
	// reload. So use of s.opts.Port is safe.
	opts := s.getOpts()
	if opts.ClientAdvertise != _EMPTY_ {
		h, p, err := parseHostPort(opts.ClientAdvertise, opts.Port)
		if err != nil {
			return err
		}
		s.info.Host = h
		s.info.Port = p
	} else {
		s.info.Host = opts.Host
		s.info.Port = opts.Port
	}
	return nil
}

// StartProfiler is called to enable dynamic profiling.
func (s *Server) StartProfiler() {
	if s.isShuttingDown() {
		return
	}

	// Snapshot server options.
	opts := s.getOpts()

	port := opts.ProfPort

	// Check for Random Port
	if port == -1 {
		port = 0
	}

	s.mu.Lock()
	hp := net.JoinHostPort(opts.Host, strconv.Itoa(port))
	l, err := net.Listen("tcp", hp)

	if err != nil {
		s.mu.Unlock()
		s.Fatalf("error starting profiler: %s", err)
		return
	}
	s.Noticef("profiling port: %d", l.Addr().(*net.TCPAddr).Port)

	srv := &http.Server{
		Addr:           hp,
		Handler:        http.DefaultServeMux,
		MaxHeaderBytes: 1 << 20,
		ReadTimeout:    time.Second * 5,
	}
	s.profiler = l
	s.profilingServer = srv

	s.setBlockProfileRate(opts.ProfBlockRate)

	go func() {
		// if this errors out, it's probably because the server is being shutdown
		err := srv.Serve(l)
		if err != nil {
			if !s.isShuttingDown() {
				s.Fatalf("error starting profiler: %s", err)
			}
		}
		srv.Close()
		s.done <- true
	}()
	s.mu.Unlock()
}

func (s *Server) setBlockProfileRate(rate int) {
	// Passing i ProfBlockRate <= 0 here will disable or > 0 will enable.
	runtime.SetBlockProfileRate(rate)

	if rate > 0 {
		s.Warnf("Block profiling is enabled (rate %d), this may have a performance impact", rate)
	}
}

// StartHTTPMonitoring will enable the HTTP monitoring port.
// DEPRECATED: Should use StartMonitoring.
func (s *Server) StartHTTPMonitoring() {
	s.startMonitoring(false)
}

// StartHTTPSMonitoring will enable the HTTPS monitoring port.
// DEPRECATED: Should use StartMonitoring.
func (s *Server) StartHTTPSMonitoring() {
	s.startMonitoring(true)
}

// StartMonitoring starts the HTTP or HTTPs server if needed.
func (s *Server) StartMonitoring() error {
	// Snapshot server options.
	opts := s.getOpts()

	// Specifying both HTTP and HTTPS ports is a misconfiguration
	if opts.HTTPPort != 0 && opts.HTTPSPort != 0 {
		return fmt.Errorf("can't specify both HTTP (%v) and HTTPs (%v) ports", opts.HTTPPort, opts.HTTPSPort)
	}
	var err error
	if opts.HTTPPort != 0 {
		err = s.startMonitoring(false)
	} else if opts.HTTPSPort != 0 {
		if opts.TLSConfig == nil {
			return fmt.Errorf("TLS cert and key required for HTTPS")
		}
		err = s.startMonitoring(true)
	}
	return err
}

// HTTP endpoints
const (
	RootPath         = "/"
	VarzPath         = "/varz"
	ConnzPath        = "/connz"
	RoutezPath       = "/routez"
	GatewayzPath     = "/gatewayz"
	LeafzPath        = "/leafz"
	SubszPath        = "/subsz"
	StackszPath      = "/stacksz"
	AccountzPath     = "/accountz"
	AccountStatzPath = "/accstatz"
	JszPath          = "/jsz"
	HealthzPath      = "/healthz"
	IPQueuesPath     = "/ipqueuesz"
	RaftzPath        = "/raftz"
	ExpvarzPath      = "/debug/vars"
)

func (s *Server) basePath(p string) string {
	return path.Join(s.httpBasePath, p)
}

type captureHTTPServerLog struct {
	s      *Server
	prefix string
}

func (cl *captureHTTPServerLog) Write(p []byte) (int, error) {
	var buf [128]byte
	var b = buf[:0]

	b = append(b, []byte(cl.prefix)...)
	offset := 0
	if bytes.HasPrefix(p, []byte("http:")) {
		offset = 6
	}
	b = append(b, p[offset:]...)
	cl.s.Errorf(string(b))
	return len(p), nil
}

// The TLS configuration is passed to the listener when the monitoring
// "server" is setup. That prevents TLS configuration updates on reload
// from being used. By setting this function in tls.Config.GetConfigForClient
// we instruct the TLS handshake to ask for the tls configuration to be
// used for a specific client. We don't care which client, we always use
// the same TLS configuration.
func (s *Server) getMonitoringTLSConfig(_ *tls.ClientHelloInfo) (*tls.Config, error) {
	opts := s.getOpts()
	tc := opts.TLSConfig.Clone()
	tc.ClientAuth = tls.NoClientCert
	return tc, nil
}

// Start the monitoring server
func (s *Server) startMonitoring(secure bool) error {
	if s.isShuttingDown() {
		return nil
	}

	// Snapshot server options.
	opts := s.getOpts()

	var (
		hp           string
		err          error
		httpListener net.Listener
		port         int
	)

	monitorProtocol := "http"

	if secure {
		monitorProtocol += "s"
		port = opts.HTTPSPort
		if port == -1 {
			port = 0
		}
		hp = net.JoinHostPort(opts.HTTPHost, strconv.Itoa(port))
		config := opts.TLSConfig.Clone()
		if !s.ocspPeerVerify {
			config.GetConfigForClient = s.getMonitoringTLSConfig
			config.ClientAuth = tls.NoClientCert
		}
		httpListener, err = tls.Listen("tcp", hp, config)

	} else {
		port = opts.HTTPPort
		if port == -1 {
			port = 0
		}
		hp = net.JoinHostPort(opts.HTTPHost, strconv.Itoa(port))
		httpListener, err = net.Listen("tcp", hp)
	}

	if err != nil {
		return fmt.Errorf("can't listen to the monitor port: %v", err)
	}

	rport := httpListener.Addr().(*net.TCPAddr).Port
	s.Noticef("Starting %s monitor on %s", monitorProtocol, net.JoinHostPort(opts.HTTPHost, strconv.Itoa(rport)))

	mux := http.NewServeMux()

	// Root
	mux.HandleFunc(s.basePath(RootPath), s.HandleRoot)
	// Varz
	mux.HandleFunc(s.basePath(VarzPath), s.HandleVarz)
	// Connz
	mux.HandleFunc(s.basePath(ConnzPath), s.HandleConnz)
	// Routez
	mux.HandleFunc(s.basePath(RoutezPath), s.HandleRoutez)
	// Gatewayz
	mux.HandleFunc(s.basePath(GatewayzPath), s.HandleGatewayz)
	// Leafz
	mux.HandleFunc(s.basePath(LeafzPath), s.HandleLeafz)
	// Subz
	mux.HandleFunc(s.basePath(SubszPath), s.HandleSubsz)
	// Subz alias for backwards compatibility
	mux.HandleFunc(s.basePath("/subscriptionsz"), s.HandleSubsz)
	// Stacksz
	mux.HandleFunc(s.basePath(StackszPath), s.HandleStacksz)
	// Accountz
	mux.HandleFunc(s.basePath(AccountzPath), s.HandleAccountz)
	// Accstatz
	mux.HandleFunc(s.basePath(AccountStatzPath), s.HandleAccountStatz)
	// Jsz
	mux.HandleFunc(s.basePath(JszPath), s.HandleJsz)
	// Healthz
	mux.HandleFunc(s.basePath(HealthzPath), s.HandleHealthz)
	// IPQueuesz
	mux.HandleFunc(s.basePath(IPQueuesPath), s.HandleIPQueuesz)
	// Raftz
	mux.HandleFunc(s.basePath(RaftzPath), s.HandleRaftz)
	// Expvarz
	mux.Handle(s.basePath(ExpvarzPath), expvar.Handler())

	// Do not set a WriteTimeout because it could cause cURL/browser
	// to return empty response or unable to display page if the
	// server needs more time to build the response.
	srv := &http.Server{
		Addr:              hp,
		Handler:           mux,
		MaxHeaderBytes:    1 << 20,
		ErrorLog:          log.New(&captureHTTPServerLog{s, "monitoring: "}, _EMPTY_, 0),
		ReadHeaderTimeout: time.Second * 5,
	}
	s.mu.Lock()
	s.http = httpListener
	s.httpHandler = mux
	s.monitoringServer = srv
	s.mu.Unlock()

	go func() {
		if err := srv.Serve(httpListener); err != nil {
			if !s.isShuttingDown() {
				s.Fatalf("Error starting monitor on %q: %v", hp, err)
			}
		}
		srv.Close()
		s.mu.Lock()
		s.httpHandler = nil
		s.mu.Unlock()
		s.done <- true
	}()

	return nil
}

// HTTPHandler returns the http.Handler object used to handle monitoring
// endpoints. It will return nil if the server is not configured for
// monitoring, or if the server has not been started yet (Server.Start()).
func (s *Server) HTTPHandler() http.Handler {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.httpHandler
}

// Perform a conditional deep copy due to reference nature of [Client|WS]ConnectURLs.
// If updates are made to Info, this function should be consulted and updated.
// Assume lock is held.
func (s *Server) copyInfo() Info {
	info := s.info
	if len(info.ClientConnectURLs) > 0 {
		info.ClientConnectURLs = append([]string(nil), s.info.ClientConnectURLs...)
	}
	if len(info.WSConnectURLs) > 0 {
		info.WSConnectURLs = append([]string(nil), s.info.WSConnectURLs...)
	}
	return info
}

// tlsMixConn is used when we can receive both TLS and non-TLS connections on same port.
type tlsMixConn struct {
	net.Conn
	pre *bytes.Buffer
}

// Read for our mixed multi-reader.
func (c *tlsMixConn) Read(b []byte) (int, error) {
	if c.pre != nil {
		n, err := c.pre.Read(b)
		if c.pre.Len() == 0 {
			c.pre = nil
		}
		return n, err
	}
	return c.Conn.Read(b)
}

func (s *Server) createClient(conn net.Conn) *client {
	return s.createClientEx(conn, false)
}

func (s *Server) createClientInProcess(conn net.Conn) *client {
	return s.createClientEx(conn, true)
}

func (s *Server) createClientEx(conn net.Conn, inProcess bool) *client {
	// Snapshot server options.
	opts := s.getOpts()

	maxPay := int32(opts.MaxPayload)
	maxSubs := int32(opts.MaxSubs)
	// For system, maxSubs of 0 means unlimited, so re-adjust here.
	if maxSubs == 0 {
		maxSubs = -1
	}
	now := time.Now()

	c := &client{
		srv:   s,
		nc:    conn,
		opts:  defaultOpts,
		mpay:  maxPay,
		msubs: maxSubs,
		start: now,
		last:  now,
		iproc: inProcess,
	}

	c.registerWithAccount(s.globalAccount())

	var info Info
	var authRequired bool

	s.mu.Lock()
	// Grab JSON info string
	info = s.copyInfo()
	if s.nonceRequired() {
		// Nonce handling
		var raw [nonceLen]byte
		nonce := raw[:]
		s.generateNonce(nonce)
		info.Nonce = string(nonce)
	}
	c.nonce = []byte(info.Nonce)
	authRequired = info.AuthRequired

	// Check to see if we have auth_required set but we also have a no_auth_user.
	// If so set back to false.
	if info.AuthRequired && opts.NoAuthUser != _EMPTY_ && opts.NoAuthUser != s.sysAccOnlyNoAuthUser {
		info.AuthRequired = false
	}

	// Check to see if this is an in-process connection with tls_required.
	// If so, set as not required, but available.
	if inProcess && info.TLSRequired {
		info.TLSRequired = false
		info.TLSAvailable = true
	}

	s.totalClients++
	s.mu.Unlock()

	// Grab lock
	c.mu.Lock()
	if authRequired {
		c.flags.set(expectConnect)
	}

	// Initialize
	c.initClient()

	c.Debugf("Client connection created")

	// Save info.TLSRequired value since we may neeed to change it back and forth.
	orgInfoTLSReq := info.TLSRequired

	var tlsFirstFallback time.Duration
	// Check if we should do TLS first.
	tlsFirst := opts.TLSConfig != nil && opts.TLSHandshakeFirst
	if tlsFirst {
		// Make sure info.TLSRequired is set to true (it could be false
		// if AllowNonTLS is enabled).
		info.TLSRequired = true
		// Get the fallback delay value if applicable.
		if f := opts.TLSHandshakeFirstFallback; f > 0 {
			tlsFirstFallback = f
		} else if inProcess {
			// For in-process connection, we will always have a fallback
			// delay. It allows support for non-TLS, TLS and "TLS First"
			// in-process clients to successfully connect.
			tlsFirstFallback = DEFAULT_TLS_HANDSHAKE_FIRST_FALLBACK_DELAY
		}
	}

	// Decide if we are going to require TLS or not and generate INFO json.
	// If we have ProxyProtocol enabled then we won't include the client
	// IP in the initial INFO, as that would leak the proxy IP itself.
	// In that case we'll send another INFO after the client introduces itself.
	tlsRequired := info.TLSRequired
	infoBytes := c.generateClientInfoJSON(info, !opts.ProxyProtocol)

	// Send our information, except if TLS and TLSHandshakeFirst is requested.
	if !tlsFirst {
		// Need to be sent in place since writeLoop cannot be started until
		// TLS handshake is done (if applicable).
		c.sendProtoNow(infoBytes)
	}

	// Unlock to register
	c.mu.Unlock()

	// Register with the server.
	s.mu.Lock()
	// If server is not running, Shutdown() may have already gathered the
	// list of connections to close. It won't contain this one, so we need
	// to bail out now otherwise the readLoop started down there would not
	// be interrupted. Skip also if in lame duck mode.
	if !s.isRunning() || s.isShuttingDown() || s.ldm {
		// There are some tests that create a server but don't start it,
		// and use "async" clients and perform the parsing manually. Such
		// clients would branch here (since server is not running). However,
		// when a server is shutting down or in lame duck mode, we must
		// close this connection.
		if s.isShuttingDown() || s.ldm {
			s.mu.Unlock()
			c.closeConnection(ServerShutdown)
			return nil
		}
		s.mu.Unlock()
		return c
	}

	// If there is a max connections specified, check that adding
	// this new client would not push us over the max
	if opts.MaxConn < 0 || (opts.MaxConn > 0 && len(s.clients) >= opts.MaxConn) {
		s.mu.Unlock()
		c.maxConnExceeded()
		return nil
	}
	s.clients[c.cid] = c

	s.mu.Unlock()

	// Re-Grab lock
	c.mu.Lock()

	var pre []byte
	// We need first to check for "TLS First" fallback delay.
	if !c.isClosed() && tlsFirstFallback > 0 {
		// We wait and see if we are getting any data. Since we did not send
		// the INFO protocol yet, only clients that use TLS first should be
		// sending data (the TLS handshake). We don't really check the content:
		// if it is a rogue agent and not an actual client performing the
		// TLS handshake, the error will be detected when performing the
		// handshake on our side.
		pre = make([]byte, 4)
		nc := c.nc
		c.mu.Unlock()
		_ = nc.SetReadDeadline(time.Now().Add(tlsFirstFallback))
		n, _ := io.ReadFull(nc, pre[:])
		_ = nc.SetReadDeadline(time.Time{})
		c.mu.Lock()
		// If we get any data (regardless of possible timeout), we will proceed
		// with the TLS handshake.
		if n > 0 {
			pre = pre[:n]
		} else {
			// We did not get anything so we will send the INFO protocol.
			pre = nil

			// Restore the original info.TLSRequired value if it is
			// different that the current value and regenerate infoBytes.
			if orgInfoTLSReq != info.TLSRequired {
				info.TLSRequired = orgInfoTLSReq
				infoBytes = c.generateClientInfoJSON(info, !opts.ProxyProtocol)
			}
			c.sendProtoNow(infoBytes)
			// Set the boolean to false for the rest of the function.
			tlsFirst = false
		}
	}
	// If we have both TLS and non-TLS allowed we need to see which
	// one the client wants. We'll always allow this for in-process
	// connections.
	sniffTLS := !tlsFirst && opts.TLSConfig != nil && (inProcess || opts.AllowNonTLS)
	if !c.isClosed() && sniffTLS {
		pre = make([]byte, 6) // Minimum 6 bytes for proxy proto in next step.
		nc := c.nc
		c.mu.Unlock()
		_ = nc.SetReadDeadline(time.Now().Add(secondsToDuration(opts.TLSTimeout)))
		n, _ := io.ReadFull(nc, pre[:])
		_ = nc.SetReadDeadline(time.Time{})
		c.mu.Lock()
		pre = pre[:n]
		tlsRequired = n > 0 && pre[0] == 0x16
	}

	// Check for proxy protocol if enabled. The PROXY header is sent as
	// plaintext before any TLS handshake per the spec, so we must read it
	// before doing TLS even when TLS is required. Any bytes read past the
	// header are kept in `pre` and replayed into the TLS handshake (or the
	// non-TLS protocol parser) by the tlsMixConn wrapper used below.
	if !c.isClosed() && opts.ProxyProtocol {
		if len(pre) == 0 {
			// There has been no pre-read yet, do so so we can work out
			// if the client is trying to negotiate PROXY.
			pre = make([]byte, 6)
			nc := c.nc
			c.mu.Unlock()
			_ = nc.SetReadDeadline(time.Now().Add(proxyProtoReadTimeout))
			n, _ := io.ReadFull(nc, pre)
			_ = nc.SetReadDeadline(time.Time{})
			c.mu.Lock()
			pre = pre[:n]
		}
		conn = &tlsMixConn{conn, bytes.NewBuffer(pre)}
		c.mu.Unlock()
		addr, proxyPre, err := readProxyProtoHeader(conn)
		c.mu.Lock()
		if c.isClosed() {
			c.mu.Unlock()
			return nil
		}
		if err != nil && err != errProxyProtoUnrecognized {
			// err != errProxyProtoUnrecognized implies that we detected a proxy
			// protocol header but we failed to parse it, so don't continue.
			c.mu.Unlock()
			s.Warnf("Error reading PROXY protocol header from %s: %v", conn.RemoteAddr(), err)
			c.closeConnection(ProtocolViolation)
			return nil
		}
		// If addr is nil, it was a LOCAL/UNKNOWN command (health check)
		// Use the connection as-is
		if addr != nil {
			c.nc = &proxyConn{
				Conn:       conn,
				remoteAddr: addr,
			}
			// These were set already by initClient, override them.
			c.host = addr.srcIP.String()
			c.port = addr.srcPort
		}
		// At this point, err is either:
		//  - nil => we parsed the proxy protocol header successfully and
		//    proxyPre holds any bytes read past the header
		//  - errProxyProtoUnrecognized => we didn't detect proxy protocol at
		//    all and proxyPre holds the bytes that detection consumed (which
		//    may include bytes read directly from the socket beyond the
		//    original pre-read), so they must be replayed to the next step.
		pre = proxyPre
		if err == nil && sniffTLS {
			// If we sniffed for TLS-vs-plaintext above, the byte we looked
			// at belonged to the PROXY header, not to the client's actual
			// traffic. Re-evaluate using the first byte that follows the
			// header (reading it from the connection if needed).
			if len(pre) == 0 {
				buf := make([]byte, 1)
				nc := c.nc
				c.mu.Unlock()
				_ = nc.SetReadDeadline(time.Now().Add(secondsToDuration(opts.TLSTimeout)))
				n, _ := io.ReadFull(nc, buf)
				_ = nc.SetReadDeadline(time.Time{})
				c.mu.Lock()
				pre = buf[:n]
			}
			tlsRequired = len(pre) > 0 && pre[0] == 0x16
		}
		// Because we have ProxyProtocol enabled, our earlier INFO message didn't
		// include the client_ip. If we need to send it again then we will include
		// it, but sending it here immediately can confuse clients who have just
		// PING'd.
		infoBytes = c.generateClientInfoJSON(info, true)
	}

	// Check for TLS
	if !c.isClosed() && tlsRequired {
		if s.connRateCounter != nil && !s.connRateCounter.allow() {
			c.mu.Unlock()
			c.sendErr("Connection throttling is active. Please try again later.")
			c.closeConnection(MaxConnectionsExceeded)
			return nil
		}

		// If we have a prebuffer create a multi-reader.
		if len(pre) > 0 {
			c.nc = &tlsMixConn{c.nc, bytes.NewBuffer(pre)}
			// Clear pre so it is not parsed.
			pre = nil
		}
		// Performs server-side TLS handshake.
		if err := c.doTLSServerHandshake(_EMPTY_, opts.TLSConfig, opts.TLSTimeout, opts.TLSPinnedCerts); err != nil {
			c.mu.Unlock()
			return nil
		}
	}

	// Now, send the INFO if it was delayed
	if !c.isClosed() && tlsFirst {
		c.flags.set(didTLSFirst)
		c.sendProtoNow(infoBytes)
	}

	// Connection could have been closed while sending the INFO proto.
	if c.isClosed() {
		c.mu.Unlock()
		// We need to call closeConnection() to make sure that proper cleanup is done.
		c.closeConnection(WriteError)
		return nil
	}

	// Check for Auth. We schedule this timer after the TLS handshake to avoid
	// the race where the timer fires during the handshake and causes the
	// server to write bad data to the socket. See issue #432.
	if authRequired {
		c.setAuthTimer(secondsToDuration(opts.AuthTimeout))
	}

	// Do final client initialization

	// Set the Ping timer. Will be reset once connect was received.
	c.setPingTimer()

	// Spin up the read loop.
	s.startGoRoutine(func() { c.readLoop(pre) })

	// Spin up the write loop.
	s.startGoRoutine(func() { c.writeLoop() })

	if tlsRequired {
		c.Debugf("TLS handshake complete")
		cs := c.nc.(*tls.Conn).ConnectionState()
		c.Debugf("TLS version %s, cipher suite %s", tlsVersion(cs.Version), tls.CipherSuiteName(cs.CipherSuite))
	}

	c.mu.Unlock()

	return c
}

// This will save off a closed client in a ring buffer such that
// /connz can inspect. Useful for debugging, etc.
func (s *Server) saveClosedClient(c *client, nc net.Conn, subs map[string]*subscription, reason ClosedState) {
	now := time.Now()

	s.accountDisconnectEvent(c, now, reason.String())

	c.mu.Lock()

	cc := &closedClient{}
	cc.fill(c, nc, now, false)
	// Note that cc.fill is using len(c.subs), which may have been set to nil by now,
	// so replace cc.NumSubs with len(subs).
	cc.NumSubs = uint32(len(subs))
	cc.Stop = &now
	cc.Reason = reason.String()

	// Do subs, do not place by default in main ConnInfo
	if len(subs) > 0 {
		cc.subs = make([]SubDetail, 0, len(subs))
		for _, sub := range subs {
			cc.subs = append(cc.subs, newSubDetail(sub))
		}
	}
	// Hold user as well.
	cc.user = c.getRawAuthUser()
	// Hold account name if not the global account.
	if c.acc != nil && c.acc.Name != globalAccountName {
		cc.acc = c.acc.Name
	}
	cc.JWT = redactBearerJWT(c.opts.JWT)
	cc.IssuerKey = issuerForClient(c)
	cc.Tags = c.tags
	cc.NameTag = c.nameTag
	c.mu.Unlock()

	// Place in the ring buffer
	if s.closed != nil {
		s.closed.append(cc)
	}
}

// Adds to the list of client and websocket clients connect URLs.
// If there was a change, an INFO protocol is sent to registered clients
// that support async INFO protocols.
// Server lock held on entry.
func (s *Server) addConnectURLsAndSendINFOToClients(curls, wsurls []string) {
	s.updateServerINFOAndSendINFOToClients(curls, wsurls, true)
}

// Removes from the list of client and websocket clients connect URLs.
// If there was a change, an INFO protocol is sent to registered clients
// that support async INFO protocols.
// Server lock held on entry.
func (s *Server) removeConnectURLsAndSendINFOToClients(curls, wsurls []string) {
	s.updateServerINFOAndSendINFOToClients(curls, wsurls, false)
}

// Updates the list of client and websocket clients connect URLs and if any change
// sends an async INFO update to clients that support it.
// Server lock held on entry.
func (s *Server) updateServerINFOAndSendINFOToClients(curls, wsurls []string, add bool) {
	remove := !add
	// Will return true if we need alter the server's Info object.
	updateMap := func(urls []string, m refCountedUrlSet) bool {
		wasUpdated := false
		for _, url := range urls {
			if add && m.addUrl(url) {
				wasUpdated = true
			} else if remove && m.removeUrl(url) {
				wasUpdated = true
			}
		}
		return wasUpdated
	}
	cliUpdated := updateMap(curls, s.clientConnectURLsMap)
	wsUpdated := updateMap(wsurls, s.websocket.connectURLsMap)

	updateInfo := func(infoURLs *[]string, urls []string, m refCountedUrlSet) {
		// Recreate the info's slice from the map
		*infoURLs = (*infoURLs)[:0]
		// Add this server client connect ULRs first...
		*infoURLs = append(*infoURLs, urls...)
		// Then the ones from the map
		for url := range m {
			*infoURLs = append(*infoURLs, url)
		}
	}
	if cliUpdated {
		updateInfo(&s.info.ClientConnectURLs, s.clientConnectURLs, s.clientConnectURLsMap)
	}
	if wsUpdated {
		updateInfo(&s.info.WSConnectURLs, s.websocket.connectURLs, s.websocket.connectURLsMap)
	}
	if cliUpdated || wsUpdated {
		// Send to all registered clients that support async INFO protocols.
		s.sendAsyncInfoToClients(cliUpdated, wsUpdated)
	}
}

// Handle closing down a connection when the handshake has timedout.
func tlsTimeout(c *client, conn *tls.Conn) {
	c.mu.Lock()
	closed := c.isClosed()
	c.mu.Unlock()
	// Check if already closed
	if closed {
		return
	}
	cs := conn.ConnectionState()
	if !cs.HandshakeComplete {
		if c.kind == CLIENT || c.kind == LEAF {
			c.Debugf("TLS handshake timeout")
		} else {
			c.Errorf("TLS handshake timeout")
		}
		c.sendErr("Secure Connection - TLS Required")
		c.closeConnection(TLSHandshakeError)
	}
}

// Seems silly we have to write these
func tlsVersion(ver uint16) string {
	switch ver {
	case tls.VersionTLS10:
		return "1.0"
	case tls.VersionTLS11:
		return "1.1"
	case tls.VersionTLS12:
		return "1.2"
	case tls.VersionTLS13:
		return "1.3"
	}
	return fmt.Sprintf("Unknown [0x%x]", ver)
}

func tlsVersionFromString(ver string) (uint16, error) {
	switch ver {
	case "1.0":
		return tls.VersionTLS10, nil
	case "1.1":
		return tls.VersionTLS11, nil
	case "1.2":
		return tls.VersionTLS12, nil
	case "1.3":
		return tls.VersionTLS13, nil
	}
	return 0, fmt.Errorf("unknown version: %v", ver)
}

// Remove a client or route from our internal accounting.
func (s *Server) removeClient(c *client) {
	// kind is immutable, so can check without lock
	switch c.kind {
	case CLIENT:
		c.mu.Lock()
		cid := c.cid
		updateProtoInfoCount := false
		if c.kind == CLIENT && c.opts.Protocol >= ClientProtoInfo {
			updateProtoInfoCount = true
		}
		proxyKey := c.proxyKey
		c.mu.Unlock()

		s.mu.Lock()
		delete(s.clients, cid)
		if updateProtoInfoCount {
			s.cproto--
		}
		if proxyKey != _EMPTY_ {
			s.removeProxiedConn(proxyKey, cid)
		}
		s.mu.Unlock()
	case ROUTER:
		s.removeRoute(c)
	case GATEWAY:
		s.removeRemoteGatewayConnection(c)
	case LEAF:
		s.removeLeafNodeConnection(c)
	}
}

// Remove the connection with id `cid` from the map of connections
// under the public key `key` of the trusted proxies.
//
// Server lock must be held on entry.
func (s *Server) removeProxiedConn(key string, cid uint64) {
	conns := s.proxiedConns[key]
	delete(conns, cid)
	if len(conns) == 0 {
		delete(s.proxiedConns, key)
	}
}

func (s *Server) removeFromTempClients(cid uint64) {
	s.grMu.Lock()
	delete(s.grTmpClients, cid)
	s.grMu.Unlock()
}

func (s *Server) addToTempClients(cid uint64, c *client) bool {
	added := false
	s.grMu.Lock()
	if s.grRunning {
		s.grTmpClients[cid] = c
		added = true
	}
	s.grMu.Unlock()
	return added
}

/////////////////////////////////////////////////////////////////
// These are some helpers for accounting in functional tests.
/////////////////////////////////////////////////////////////////

// NumRoutes will report the number of registered routes.
func (s *Server) NumRoutes() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.numRoutes()
}

// numRoutes will report the number of registered routes.
// Server lock held on entry
func (s *Server) numRoutes() int {
	var nr int
	s.forEachRoute(func(c *client) {
		nr++
	})
	return nr
}

// NumRemotes will report number of registered remotes.
func (s *Server) NumRemotes() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.numRemotes()
}

// numRemotes will report number of registered remotes.
// Server lock held on entry
func (s *Server) numRemotes() int {
	return len(s.routes)
}

// NumLeafNodes will report number of leaf node connections.
func (s *Server) NumLeafNodes() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.leafs)
}

// NumClients will report the number of registered clients.
func (s *Server) NumClients() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.clients)
}

// GetClient will return the client associated with cid.
func (s *Server) GetClient(cid uint64) *client {
	return s.getClient(cid)
}

// getClient will return the client associated with cid.
func (s *Server) getClient(cid uint64) *client {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clients[cid]
}

// GetLeafNode returns the leafnode associated with the cid.
func (s *Server) GetLeafNode(cid uint64) *client {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.leafs[cid]
}

// NumSubscriptions will report how many subscriptions are active.
func (s *Server) NumSubscriptions() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.numSubscriptions()
}

// numSubscriptions will report how many subscriptions are active.
// Lock should be held.
func (s *Server) numSubscriptions() uint32 {
	var subs int
	s.accounts.Range(func(k, v any) bool {
		acc := v.(*Account)
		subs += acc.TotalSubs()
		return true
	})
	return uint32(subs)
}

// NumSlowConsumers will report the number of slow consumers.
func (s *Server) NumSlowConsumers() int64 {
	return atomic.LoadInt64(&s.slowConsumers)
}

// NumStalledClients will report the total number of times clients have been stalled.
func (s *Server) NumStalledClients() int64 {
	return atomic.LoadInt64(&s.stalls)
}

// NumSlowConsumersClients will report the number of slow consumers clients.
func (s *Server) NumSlowConsumersClients() uint64 {
	return s.scStats.clients.Load()
}

// NumSlowConsumersRoutes will report the number of slow consumers routes.
func (s *Server) NumSlowConsumersRoutes() uint64 {
	return s.scStats.routes.Load()
}

// NumSlowConsumersGateways will report the number of slow consumers leafs.
func (s *Server) NumSlowConsumersGateways() uint64 {
	return s.scStats.gateways.Load()
}

// NumSlowConsumersLeafs will report the number of slow consumers leafs.
func (s *Server) NumSlowConsumersLeafs() uint64 {
	return s.scStats.leafs.Load()
}

// NumStaleConnections will report the number of stale connections.
func (s *Server) NumStaleConnections() int64 {
	return atomic.LoadInt64(&s.staleConnections)
}

// NumStaleConnectionsClients will report the number of stale client connections.
func (s *Server) NumStaleConnectionsClients() uint64 {
	return s.staleStats.clients.Load()
}

// NumStaleConnectionsRoutes will report the number of stale route connections.
func (s *Server) NumStaleConnectionsRoutes() uint64 {
	return s.staleStats.routes.Load()
}

// NumStaleConnectionsGateways will report the number of stale gateway connections.
func (s *Server) NumStaleConnectionsGateways() uint64 {
	return s.staleStats.gateways.Load()
}

// NumStaleConnectionsLeafs will report the number of stale leaf connections.
func (s *Server) NumStaleConnectionsLeafs() uint64 {
	return s.staleStats.leafs.Load()
}

// ConfigTime will report the last time the server configuration was loaded.
func (s *Server) ConfigTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.configTime
}

// Addr will return the net.Addr object for the current listener.
func (s *Server) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// MonitorAddr will return the net.Addr object for the monitoring listener.
func (s *Server) MonitorAddr() *net.TCPAddr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.http == nil {
		return nil
	}
	return s.http.Addr().(*net.TCPAddr)
}

// ClusterAddr returns the net.Addr object for the route listener.
func (s *Server) ClusterAddr() *net.TCPAddr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.routeListener == nil {
		return nil
	}
	return s.routeListener.Addr().(*net.TCPAddr)
}

// ProfilerAddr returns the net.Addr object for the profiler listener.
func (s *Server) ProfilerAddr() *net.TCPAddr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.profiler == nil {
		return nil
	}
	return s.profiler.Addr().(*net.TCPAddr)
}

func (s *Server) readyForConnections(d time.Duration) error {
	// Snapshot server options.
	opts := s.getOpts()

	type info struct {
		ok  bool
		err error
	}
	chk := make(map[string]info)

	end := time.Now().Add(d)
	for time.Now().Before(end) {
		s.mu.RLock()
		chk["server"] = info{ok: s.listener != nil || opts.DontListen, err: s.listenerErr}
		chk["route"] = info{ok: (opts.Cluster.Port == 0 || s.routeListener != nil), err: s.routeListenerErr}
		chk["gateway"] = info{ok: (opts.Gateway.Name == _EMPTY_ || s.gatewayListener != nil), err: s.gatewayListenerErr}
		chk["leafnode"] = info{ok: (opts.LeafNode.Port == 0 || s.leafNodeListener != nil), err: s.leafNodeListenerErr}
		chk["websocket"] = info{ok: (opts.Websocket.Port == 0 || s.websocket.listener != nil), err: s.websocket.listenerErr}
		chk["mqtt"] = info{ok: (opts.MQTT.Port == 0 || s.mqtt.listener != nil), err: s.mqtt.listenerErr}
		s.mu.RUnlock()

		var numOK int
		for _, inf := range chk {
			if inf.ok {
				numOK++
			}
		}
		if numOK == len(chk) {
			// In the case of DontListen option (no accept loop), we still want
			// to make sure that Start() has done all the work, so we wait on
			// that.
			if opts.DontListen {
				select {
				case <-s.startupComplete:
				case <-time.After(d):
					return fmt.Errorf("failed to be ready for connections after %s: startup did not complete", d)
				}
			}
			return nil
		}
		if d > 25*time.Millisecond {
			time.Sleep(25 * time.Millisecond)
		}
	}

	failed := make([]string, 0, len(chk))
	for name, inf := range chk {
		if inf.ok && inf.err != nil {
			failed = append(failed, fmt.Sprintf("%s(ok, but %s)", name, inf.err))
		}
		if !inf.ok && inf.err == nil {
			failed = append(failed, name)
		}
		if !inf.ok && inf.err != nil {
			failed = append(failed, fmt.Sprintf("%s(%s)", name, inf.err))
		}
	}

	return fmt.Errorf(
		"failed to be ready for connections after %s: %s",
		d, strings.Join(failed, ", "),
	)
}

// ReadyForConnections returns `true` if the server is ready to accept clients
// and, if routing is enabled, route connections. If after the duration
// `dur` the server is still not ready, returns `false`.
func (s *Server) ReadyForConnections(dur time.Duration) bool {
	return s.readyForConnections(dur) == nil
}

// Quick utility to function to tell if the server supports headers.
func (s *Server) supportsHeaders() bool {
	if s == nil {
		return false
	}
	return !(s.getOpts().NoHeaderSupport)
}

// ID returns the server's ID
func (s *Server) ID() string {
	return s.info.ID
}

// NodeName returns the node name for this server.
func (s *Server) NodeName() string {
	return getHash(s.info.Name)
}

// Name returns the server's name. This will be the same as the ID if it was not set.
func (s *Server) Name() string {
	return s.info.Name
}

func (s *Server) String() string {
	return s.info.Name
}

type pprofLabels map[string]string

func setGoRoutineLabels(tags ...pprofLabels) {
	var labels []string
	for _, m := range tags {
		for k, v := range m {
			labels = append(labels, k, v)
		}
	}
	if len(labels) > 0 {
		pprof.SetGoroutineLabels(
			pprof.WithLabels(context.Background(), pprof.Labels(labels...)),
		)
	}
}

func (s *Server) startGoRoutine(f func(), tags ...pprofLabels) bool {
	var started bool
	s.grMu.Lock()
	defer s.grMu.Unlock()
	if s.grRunning {
		s.grWG.Add(1)
		go func() {
			setGoRoutineLabels(tags...)
			f()
		}()
		started = true
	}
	return started
}

// getClientConnectURLs returns suitable URLs for clients to connect to the listen
// port based on the server options' Host and Port. If the Host corresponds to
// "any" interfaces, this call returns the list of resolved IP addresses.
// If ClientAdvertise is set, returns the client advertise host and port.
// The server lock is assumed held on entry.
func (s *Server) getClientConnectURLs() []string {
	// Snapshot server options.
	opts := s.getOpts()
	// Ignore error here since we know that if there is client advertise, the
	// parseHostPort is correct because we did it right before calling this
	// function in Server.New().
	urls, _ := s.getConnectURLs(opts.ClientAdvertise, opts.Host, opts.Port)
	return urls
}

// Generic version that will return an array of URLs based on the given
// advertise, host and port values.
func (s *Server) getConnectURLs(advertise, host string, port int) ([]string, error) {
	urls := make([]string, 0, 1)

	// short circuit if advertise is set
	if advertise != "" {
		h, p, err := parseHostPort(advertise, port)
		if err != nil {
			return nil, err
		}
		urls = append(urls, net.JoinHostPort(h, strconv.Itoa(p)))
	} else {
		sPort := strconv.Itoa(port)
		_, ips, err := s.getNonLocalIPsIfHostIsIPAny(host, true)
		for _, ip := range ips {
			urls = append(urls, net.JoinHostPort(ip, sPort))
		}
		if err != nil || len(urls) == 0 {
			// We are here if s.opts.Host is not "0.0.0.0" nor "::", or if for some
			// reason we could not add any URL in the loop above.
			// We had a case where a Windows VM was hosed and would have err == nil
			// and not add any address in the array in the loop above, and we
			// ended-up returning 0.0.0.0, which is problematic for Windows clients.
			// Check for 0.0.0.0 or :: specifically, and ignore if that's the case.
			if host == "0.0.0.0" || host == "::" {
				s.Errorf("Address %q can not be resolved properly", host)
			} else {
				urls = append(urls, net.JoinHostPort(host, sPort))
			}
		}
	}
	return urls, nil
}

// Returns an array of non local IPs if the provided host is
// 0.0.0.0 or ::. It returns the first resolved if `all` is
// false.
// The boolean indicate if the provided host was 0.0.0.0 (or ::)
// so that if the returned array is empty caller can decide
// what to do next.
func (s *Server) getNonLocalIPsIfHostIsIPAny(host string, all bool) (bool, []string, error) {
	ip := net.ParseIP(host)
	// If this is not an IP, we are done
	if ip == nil {
		return false, nil, nil
	}
	// If this is not 0.0.0.0 or :: we have nothing to do.
	if !ip.IsUnspecified() {
		return false, nil, nil
	}
	s.Debugf("Get non local IPs for %q", host)
	var ips []string
	ifaces, _ := net.Interfaces()
	for _, i := range ifaces {
		addrs, _ := i.Addrs()
		for _, addr := range addrs {
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			ipStr := ip.String()
			// Skip non global unicast addresses
			if !ip.IsGlobalUnicast() || ip.IsUnspecified() {
				ip = nil
				continue
			}
			s.Debugf("  ip=%s", ipStr)
			ips = append(ips, ipStr)
			if !all {
				break
			}
		}
	}
	return true, ips, nil
}

// if the ip is not specified, attempt to resolve it
func resolveHostPorts(addr net.Listener) []string {
	hostPorts := make([]string, 0)
	hp := addr.Addr().(*net.TCPAddr)
	port := strconv.Itoa(hp.Port)
	if hp.IP.IsUnspecified() {
		var ip net.IP
		ifaces, _ := net.Interfaces()
		for _, i := range ifaces {
			addrs, _ := i.Addrs()
			for _, addr := range addrs {
				switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
					hostPorts = append(hostPorts, net.JoinHostPort(ip.String(), port))
				case *net.IPAddr:
					ip = v.IP
					hostPorts = append(hostPorts, net.JoinHostPort(ip.String(), port))
				default:
					continue
				}
			}
		}
	} else {
		hostPorts = append(hostPorts, net.JoinHostPort(hp.IP.String(), port))
	}
	return hostPorts
}

// format the address of a net.Listener with a protocol
func formatURL(protocol string, addr net.Listener) []string {
	hostports := resolveHostPorts(addr)
	for i, hp := range hostports {
		hostports[i] = fmt.Sprintf("%s://%s", protocol, hp)
	}
	return hostports
}

// Ports describes URLs that the server can be contacted in
type Ports struct {
	Nats       []string `json:"nats,omitempty"`
	Monitoring []string `json:"monitoring,omitempty"`
	Cluster    []string `json:"cluster,omitempty"`
	Profile    []string `json:"profile,omitempty"`
	WebSocket  []string `json:"websocket,omitempty"`
}

// PortsInfo attempts to resolve all the ports. If after maxWait the ports are not
// resolved, it returns nil. Otherwise it returns a Ports struct
// describing ports where the server can be contacted
func (s *Server) PortsInfo(maxWait time.Duration) *Ports {
	if s.readyForListeners(maxWait) {
		opts := s.getOpts()

		s.mu.RLock()
		tls := s.info.TLSRequired
		listener := s.listener
		httpListener := s.http
		clusterListener := s.routeListener
		profileListener := s.profiler
		wsListener := s.websocket.listener
		wss := s.websocket.tls
		s.mu.RUnlock()

		ports := Ports{}

		if listener != nil {
			natsProto := "nats"
			if tls {
				natsProto = "tls"
			}
			ports.Nats = formatURL(natsProto, listener)
		}

		if httpListener != nil {
			monProto := "http"
			if opts.HTTPSPort != 0 {
				monProto = "https"
			}
			ports.Monitoring = formatURL(monProto, httpListener)
		}

		if clusterListener != nil {
			clusterProto := "nats"
			if opts.Cluster.TLSConfig != nil {
				clusterProto = "tls"
			}
			ports.Cluster = formatURL(clusterProto, clusterListener)
		}

		if profileListener != nil {
			ports.Profile = formatURL("http", profileListener)
		}

		if wsListener != nil {
			protocol := wsSchemePrefix
			if wss {
				protocol = wsSchemePrefixTLS
			}
			ports.WebSocket = formatURL(protocol, wsListener)
		}

		return &ports
	}

	return nil
}

// Returns the portsFile. If a non-empty dirHint is provided, the dirHint
// path is used instead of the server option value
func (s *Server) portFile(dirHint string) string {
	dirname := s.getOpts().PortsFileDir
	if dirHint != "" {
		dirname = dirHint
	}
	if dirname == _EMPTY_ {
		return _EMPTY_
	}
	return filepath.Join(dirname, fmt.Sprintf("%s_%d.ports", filepath.Base(os.Args[0]), os.Getpid()))
}

// Delete the ports file. If a non-empty dirHint is provided, the dirHint
// path is used instead of the server option value
func (s *Server) deletePortsFile(hintDir string) {
	portsFile := s.portFile(hintDir)
	if portsFile != "" {
		if err := os.Remove(portsFile); err != nil {
			s.Errorf("Error cleaning up ports file %s: %v", portsFile, err)
		}
	}
}

// Writes a file with a serialized Ports to the specified ports_file_dir.
// The name of the file is `exename_pid.ports`, typically nats-server_pid.ports.
// if ports file is not set, this function has no effect
func (s *Server) logPorts() {
	opts := s.getOpts()
	portsFile := s.portFile(opts.PortsFileDir)
	if portsFile != _EMPTY_ {
		go func() {
			info := s.PortsInfo(5 * time.Second)
			if info == nil {
				s.Errorf("Unable to resolve the ports in the specified time")
				return
			}
			data, err := json.Marshal(info)
			if err != nil {
				s.Errorf("Error marshaling ports file: %v", err)
				return
			}
			if err := os.WriteFile(portsFile, data, 0666); err != nil {
				s.Errorf("Error writing ports file (%s): %v", portsFile, err)
				return
			}

		}()
	}
}

// waits until a calculated list of listeners is resolved or a timeout
func (s *Server) readyForListeners(dur time.Duration) bool {
	end := time.Now().Add(dur)
	for time.Now().Before(end) {
		s.mu.RLock()
		listeners := s.serviceListeners()
		s.mu.RUnlock()
		if len(listeners) == 0 {
			return false
		}

		ok := true
		for _, l := range listeners {
			if l == nil {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
		select {
		case <-s.quitCh:
			return false
		case <-time.After(25 * time.Millisecond):
			// continue - unable to select from quit - we are still running
		}
	}
	return false
}

// returns a list of listeners that are intended for the process
// if the entry is nil, the interface is yet to be resolved
func (s *Server) serviceListeners() []net.Listener {
	listeners := make([]net.Listener, 0)
	opts := s.getOpts()
	listeners = append(listeners, s.listener)
	if opts.Cluster.Port != 0 {
		listeners = append(listeners, s.routeListener)
	}
	if opts.HTTPPort != 0 || opts.HTTPSPort != 0 {
		listeners = append(listeners, s.http)
	}
	if opts.ProfPort != 0 {
		listeners = append(listeners, s.profiler)
	}
	if opts.Websocket.Port != 0 {
		listeners = append(listeners, s.websocket.listener)
	}
	return listeners
}

// Returns true if in lame duck mode.
func (s *Server) isLameDuckMode() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ldm
}

// LameDuckShutdown will perform a lame duck shutdown of NATS, whereby
// the client listener is closed, existing client connections are
// kicked, Raft leaderships are transferred, JetStream is shutdown
// and then finally shutdown the NATS Server itself.
// This function blocks and will not return until the NATS Server
// has completed the entire shutdown operation.
func (s *Server) LameDuckShutdown() {
	s.lameDuckMode()
}

// This function will close the client listener then close the clients
// at some interval to avoid a reconnect storm.
// We will also transfer any raft leaders and shutdown JetStream.
func (s *Server) lameDuckMode() {
	if expected, ok := s.enterLameDuckMode(); ok {
		s.runLameDuckMode(expected)
	}
}

// startLameDuckMode enters lame duck mode before returning, and runs the rest of it in a goroutine.
func (s *Server) startLameDuckMode() {
	if expected, ok := s.enterLameDuckMode(); ok {
		go s.runLameDuckMode(expected)
	}
}

// enterLameDuckMode sets lame duck mode and closes the listeners.
// Returns false if lame duck mode was not entered.
func (s *Server) enterLameDuckMode() (int, bool) {
	s.mu.Lock()
	// Check if there is actually anything to do
	if s.isShuttingDown() || s.ldm || s.listener == nil {
		s.mu.Unlock()
		return 0, false
	}
	s.Noticef("Entering lame duck mode, stop accepting new clients")
	s.ldm = true
	s.sendLDMShutdownEventLocked()
	expected := 1
	s.listener.Close()
	s.listener = nil
	expected += s.closeWebsocketServer()
	if s.mqtt.listener != nil {
		expected++
		s.mqtt.listener.Close()
		s.mqtt.listener = nil
	}
	if s.leafNodeListener != nil {
		expected++
		s.leafNodeListener.Close()
		s.leafNodeListener = nil
	}
	s.ldmCh = make(chan bool, expected)
	s.mu.Unlock()
	return expected, true
}

// runLameDuckMode transfers leaders and drains clients before shutting down.
func (s *Server) runLameDuckMode(expected int) {
	opts := s.getOpts()
	gp := opts.LameDuckGracePeriod
	// For tests, we want the grace period to be in some cases bigger
	// than the ldm duration, so to by-pass the validateOptions() check,
	// we use negative number and flip it here.
	if gp < 0 {
		gp *= -1
	}

	// If we are running any raftNodes transfer leaders.
	if hadTransfers := s.transferRaftLeaders(); hadTransfers {
		// They will transfer leadership quickly, but wait here for a second.
		select {
		case <-time.After(time.Second):
		case <-s.quitCh:
			return
		}
	}

	// Wait for accept loops to be done to make sure that no new
	// client can connect
	for range expected {
		<-s.ldmCh
	}

	s.mu.Lock()
	// Need to recheck few things
	if s.isShuttingDown() || len(s.clients)+len(s.leafs) == 0 {
		s.mu.Unlock()
		// If there is no client, we need to call Shutdown() to complete
		// the LDMode. If server has been shutdown while lock was released,
		// calling Shutdown() should be no-op.
		s.Shutdown()
		return
	}
	dur := int64(opts.LameDuckDuration)
	dur -= int64(gp)
	if dur <= 0 {
		dur = int64(time.Second)
	}
	numClients := int64(len(s.clients) + len(s.leafs))
	batch := 1
	// Sleep interval between each client or leaf connection close.
	var si int64
	if numClients != 0 {
		si = dur / numClients
	}
	if si < 1 {
		// Should not happen (except in test with very small LD duration), but
		// if there are too many clients, batch the number of close and
		// use a tiny sleep interval that will result in yield likely.
		si = 1
		batch = int(numClients / dur)
	} else if si > int64(time.Second) {
		// Conversely, there is no need to sleep too long between clients
		// and spread say 10 clients for the 2min duration. Sleeping no
		// more than 1sec.
		si = int64(time.Second)
	}

	clients := make([]*client, 0, len(s.clients)+len(s.leafs))
	for _, client := range s.clients {
		clients = append(clients, client)
	}
	for _, leaf := range s.leafs {
		clients = append(clients, leaf)
	}
	rand.Shuffle(len(clients), func(i, j int) { clients[i], clients[j] = clients[j], clients[i] })
	// Now that we know that no new client can be accepted,
	// send INFO to routes and clients to notify this state.
	s.sendLDMToRoutes()
	s.sendLDMToClients()
	s.mu.Unlock()

	t := time.NewTimer(gp)
	// Delay start of closing of client connections in case
	// we have several servers that we want to signal to enter LD mode
	// and not have their client reconnect to each other.
	select {
	case <-t.C:
		s.Noticef("Closing existing clients")
	case <-s.quitCh:
		t.Stop()
		return
	}
	for i, client := range clients {
		client.closeConnection(ServerShutdown)
		if i == len(clients)-1 {
			break
		}
		if batch == 1 || i%batch == 0 {
			// We pick a random interval which will be at least si/2
			v := max(rand.Int64N(si), si/2)
			t.Reset(time.Duration(v))
			// Sleep for given interval or bail out if kicked by Shutdown().
			select {
			case <-t.C:
			case <-s.quitCh:
				t.Stop()
				return
			}
		}
	}
	s.Shutdown()
	s.WaitForShutdown()
}

// Send an INFO update to routes with the indication that this server is in LDM mode.
// Server lock is held on entry.
func (s *Server) sendLDMToRoutes() {
	s.routeInfo.LameDuckMode = true
	infoJSON := generateInfoJSON(&s.routeInfo)
	s.forEachRemote(func(r *client) {
		r.mu.Lock()
		r.enqueueProto(infoJSON)
		r.mu.Unlock()
	})
	// Clear now so that we notify only once, should we have to send other INFOs.
	s.routeInfo.LameDuckMode = false
}

// Send an INFO update to clients with the indication that this server is in
// LDM mode and with only URLs of other nodes.
// Server lock is held on entry.
func (s *Server) sendLDMToClients() {
	s.info.LameDuckMode = true
	// Clear this so that if there are further updates, we don't send our URLs.
	s.clientConnectURLs = s.clientConnectURLs[:0]
	if s.websocket.connectURLs != nil {
		s.websocket.connectURLs = s.websocket.connectURLs[:0]
	}
	// Reset content first.
	s.info.ClientConnectURLs = s.info.ClientConnectURLs[:0]
	s.info.WSConnectURLs = s.info.WSConnectURLs[:0]
	// Only add the other nodes if we are allowed to.
	if !s.getOpts().Cluster.NoAdvertise {
		for url := range s.clientConnectURLsMap {
			s.info.ClientConnectURLs = append(s.info.ClientConnectURLs, url)
		}
		for url := range s.websocket.connectURLsMap {
			s.info.WSConnectURLs = append(s.info.WSConnectURLs, url)
		}
	}
	// Send to all registered clients that support async INFO protocols.
	s.sendAsyncInfoToClients(true, true)
	// We now clear the info.LameDuckMode flag so that if there are
	// cluster updates and we send the INFO, we don't have the boolean
	// set which would cause multiple LDM notifications to clients.
	s.info.LameDuckMode = false
}

// If given error is a net.Error and is temporary, sleeps for the given
// delay and double it, but cap it to ACCEPT_MAX_SLEEP. The sleep is
// interrupted if the server is shutdown.
// An error message is displayed depending on the type of error.
// Returns the new (or unchanged) delay, or a negative value if the
// server has been or is being shutdown.
func (s *Server) acceptError(acceptName string, err error, tmpDelay time.Duration) time.Duration {
	if !s.isRunning() {
		return -1
	}
	//lint:ignore SA1019 We want to retry on a bunch of errors here.
	if ne, ok := err.(net.Error); ok && ne.Temporary() { // nolint:staticcheck
		s.Errorf("Temporary %s Accept Error(%v), sleeping %dms", acceptName, ne, tmpDelay/time.Millisecond)
		select {
		case <-time.After(tmpDelay):
		case <-s.quitCh:
			return -1
		}
		tmpDelay *= 2
		if tmpDelay > ACCEPT_MAX_SLEEP {
			tmpDelay = ACCEPT_MAX_SLEEP
		}
	} else {
		s.Errorf("%s Accept error: %v", acceptName, err)
	}
	return tmpDelay
}

var errNoIPAvail = errors.New("no IP available")

func (s *Server) getRandomIP(resolver netResolver, url string, excludedAddresses map[string]struct{}) (string, error) {
	host, port, err := net.SplitHostPort(url)
	if err != nil {
		return "", err
	}
	// If already an IP, skip.
	if net.ParseIP(host) != nil {
		return url, nil
	}
	ips, err := resolver.LookupHost(context.Background(), host)
	if err != nil {
		return "", fmt.Errorf("lookup for host %q: %v", host, err)
	}
	if len(excludedAddresses) > 0 {
		for i := 0; i < len(ips); i++ {
			ip := ips[i]
			addr := net.JoinHostPort(ip, port)
			if _, excluded := excludedAddresses[addr]; excluded {
				if len(ips) == 1 {
					ips = nil
					break
				}
				ips[i] = ips[len(ips)-1]
				ips = ips[:len(ips)-1]
				i--
			}
		}
		if len(ips) == 0 {
			return "", errNoIPAvail
		}
	}
	var address string
	if len(ips) == 0 {
		s.Warnf("Unable to get IP for %s, will try with %s: %v", host, url, err)
		address = url
	} else {
		var ip string
		if len(ips) == 1 {
			ip = ips[0]
		} else {
			ip = ips[rand.Int32N(int32(len(ips)))]
		}
		// add the port
		address = net.JoinHostPort(ip, port)
	}
	return address, nil
}

// Returns true for the first attempt and depending on the nature
// of the attempt (first connect or a reconnect), when the number
// of attempts is equal to the configured report attempts.
func (s *Server) shouldReportConnectErr(firstConnect bool, attempts int) bool {
	opts := s.getOpts()
	if firstConnect {
		if attempts == 1 || attempts%opts.ConnectErrorReports == 0 {
			return true
		}
		return false
	}
	if attempts == 1 || attempts%opts.ReconnectErrorReports == 0 {
		return true
	}
	return false
}

func (s *Server) updateRemoteSubscription(acc *Account, sub *subscription, delta int32) {
	s.updateRouteSubscriptionMap(acc, sub, delta)
	if s.gateway.enabled {
		s.gatewayUpdateSubInterest(acc.Name, sub, delta)
	}

	acc.updateLeafNodes(sub, delta)
}

func (s *Server) startRateLimitLogExpiration() {
	interval := time.Second
	s.startGoRoutine(func() {
		defer s.grWG.Done()

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.quitCh:
				return
			case interval = <-s.rateLimitLoggingCh:
				ticker.Reset(interval)
			case <-ticker.C:
				s.rateLimitLogging.Range(func(k, v any) bool {
					start := v.(time.Time)
					if time.Since(start) >= interval {
						s.rateLimitLogging.Delete(k)
					}
					return true
				})
			}
		}
	})
}

func (s *Server) changeRateLimitLogInterval(d time.Duration) {
	if d <= 0 {
		return
	}
	select {
	case s.rateLimitLoggingCh <- d:
	default:
	}
}

// DisconnectClientByID disconnects a client by connection ID
func (s *Server) DisconnectClientByID(id uint64) error {
	if s == nil {
		return ErrServerNotRunning
	}
	if client := s.getClient(id); client != nil {
		client.closeConnection(Kicked)
		return nil
	} else if client = s.GetLeafNode(id); client != nil {
		client.closeConnection(Kicked)
		return nil
	}
	return errors.New("no such client or leafnode id")
}

// LDMClientByID sends a Lame Duck Mode info message to a client by connection ID
func (s *Server) LDMClientByID(id uint64) error {
	if s == nil {
		return ErrServerNotRunning
	}
	s.mu.RLock()
	c := s.clients[id]
	if c == nil {
		s.mu.RUnlock()
		return errors.New("no such client id")
	}
	info := s.copyInfo()
	info.LameDuckMode = true
	s.mu.RUnlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.opts.Protocol >= ClientProtoInfo && c.flags.isSet(firstPongSent) {
		// sendInfo takes care of checking if the connection is still
		// valid or not, so don't duplicate tests here.
		c.Debugf("Sending Lame Duck Mode info to client")
		c.enqueueProto(c.generateClientInfoJSON(info, true))
		return nil
	} else {
		return errors.New("client does not support Lame Duck Mode or is not ready to receive the notification")
	}
}

func (s *Server) diskIOSemaphore() *diskIOSemaphore {
	if s == nil || s.dios == nil {
		return defaultDiskIOSemaphore()
	}
	return s.dios
}
