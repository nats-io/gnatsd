// Copyright 2020-2026 The NATS Authors
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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server/avl"
	"github.com/nats-io/nats-server/v2/server/gsl"
	"github.com/nats-io/nats-server/v2/server/stree"
	"github.com/nats-io/nuid"
)

// References to "spec" here is from https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.pdf

const (
	mqttPacketConnect    = byte(0x10)
	mqttPacketConnectAck = byte(0x20)
	mqttPacketPub        = byte(0x30)
	mqttPacketPubAck     = byte(0x40)
	mqttPacketPubRec     = byte(0x50)
	mqttPacketPubRel     = byte(0x60)
	mqttPacketPubComp    = byte(0x70)
	mqttPacketSub        = byte(0x80)
	mqttPacketSubAck     = byte(0x90)
	mqttPacketUnsub      = byte(0xa0)
	mqttPacketUnsubAck   = byte(0xb0)
	mqttPacketPing       = byte(0xc0)
	mqttPacketPingResp   = byte(0xd0)
	mqttPacketDisconnect = byte(0xe0)
	mqttPacketMask       = byte(0xf0)
	mqttPacketFlagMask   = byte(0x0f)

	mqttProtoLevel = byte(0x4)

	// Connect flags
	mqttConnFlagReserved     = byte(0x1)
	mqttConnFlagCleanSession = byte(0x2)
	mqttConnFlagWillFlag     = byte(0x04)
	mqttConnFlagWillQoS      = byte(0x18)
	mqttConnFlagWillRetain   = byte(0x20)
	mqttConnFlagPasswordFlag = byte(0x40)
	mqttConnFlagUsernameFlag = byte(0x80)

	// Publish flags
	mqttPubFlagRetain = byte(0x01)
	mqttPubFlagQoS    = byte(0x06)
	mqttPubFlagDup    = byte(0x08)
	mqttPubFlags      = mqttPubFlagRetain | mqttPubFlagQoS | mqttPubFlagDup // 0x0f, the fixed-header flags nibble
	mqttPubQos1       = byte(0x1 << 1)
	mqttPubQoS2       = byte(0x2 << 1)

	// Subscribe flags
	mqttSubscribeFlags = byte(0x2)
	mqttSubAckFailure  = byte(0x80)

	// Unsubscribe flags
	mqttUnsubscribeFlags = byte(0x2)

	// ConnAck returned codes
	mqttConnAckRCConnectionAccepted          = byte(0x0)
	mqttConnAckRCUnacceptableProtocolVersion = byte(0x1)
	mqttConnAckRCIdentifierRejected          = byte(0x2)
	mqttConnAckRCServerUnavailable           = byte(0x3)
	mqttConnAckRCBadUserOrPassword           = byte(0x4)
	mqttConnAckRCNotAuthorized               = byte(0x5)
	mqttConnAckRCQoS2WillRejected            = byte(0x10)

	// Maximum payload size of a control packet
	mqttMaxPayloadSize = 0xFFFFFFF

	// Packet overhead allowed above max_payload once connected: fixed header (5), largest topic or filter (2+65535) and packet identifier (2).
	mqttMaxPacketOverhead = 5 + 2 + 65535 + 2

	// Topic/Filter characters
	mqttTopicLevelSep = '/'
	mqttSingleLevelWC = '+'
	mqttMultiLevelWC  = '#'
	mqttReservedPre   = '$'

	// This is appended to the sid of a subscription that is
	// created on the upper level subject because of the MQTT
	// wildcard '#' semantic.
	mqttMultiLevelSidSuffix = " fwc"

	// This is the prefix used for all subjects used by MQTT code.
	mqttPrefix = "$MQTT."

	// This is the prefix for NATS subscriptions subjects associated as delivery
	// subject of JS consumer. We want to make them unique so will prevent users
	// MQTT subscriptions to start with this.
	mqttSubPrefix = mqttPrefix + "sub."

	// Stream name for MQTT messages on a given account
	mqttStreamName          = "$MQTT_msgs"
	mqttStreamSubjectPrefix = mqttPrefix + "msgs."

	// Stream name for MQTT retained messages on a given account
	mqttRetainedMsgsStreamName    = "$MQTT_rmsgs"
	mqttRetainedMsgsStreamSubject = mqttPrefix + "rmsgs."

	// Stream name for MQTT sessions on a given account
	mqttSessStreamName          = "$MQTT_sess"
	mqttSessStreamSubjectPrefix = mqttPrefix + "sess."

	// Stream name prefix for MQTT sessions on a given account
	mqttSessionsStreamNamePrefix = "$MQTT_sess_"

	// Stream name and subject for incoming MQTT QoS2 messages
	mqttQoS2IncomingMsgsStreamName          = "$MQTT_qos2in"
	mqttQoS2IncomingMsgsStreamSubjectPrefix = mqttPrefix + "qos2.in."

	// Nats-Msg-Id prefix of a QoS2 delivery into $MQTT_msgs, followed by
	// the held copy's sequence (mqttQoS2DeliveryMsgId).
	mqttQoS2DeliveryMsgIdPrefix = "q2-"

	// Stream name and subjects for outgoing MQTT QoS (PUBREL) messages
	mqttOutStreamName               = "$MQTT_out"
	mqttOutSubjectPrefix            = mqttPrefix + "out."
	mqttPubRelSubjectPrefix         = mqttPrefix + "out.pubrel."
	mqttPubRelDeliverySubjectPrefix = mqttPrefix + "deliver.pubrel."
	mqttPubRelConsumerDurablePrefix = "$MQTT_PUBREL_"

	// As per spec, MQTT server may not redeliver QoS 1 and 2 messages to
	// clients, except after client reconnects. However, NATS Server will
	// redeliver unacknowledged messages after this default interval. This can
	// be changed with the server.Options.MQTT.AckWait option.
	mqttDefaultAckWait = 30 * time.Second

	// This is the default for the outstanding number of pending QoS 1
	// messages sent to a session with QoS 1 subscriptions.
	mqttDefaultMaxAckPending = 1024

	// A session's list of subscriptions cannot have a cumulative MaxAckPending
	// of more than this limit.
	mqttMaxAckTotalLimit = 0xFFFF

	// Prefix of the reply subject for JS API requests.
	mqttJSARepliesPrefix = mqttPrefix + "JSA."

	// Those are tokens that are used for the reply subject of JS API requests.
	// For instance "$MQTT.JSA.<node id>.SC.<number>" is the reply subject
	// for a request to create a stream (where <node id> is the server name hash),
	// while "$MQTT.JSA.<node id>.SL.<number>" is for a stream lookup, etc...
	mqttJSAIdTokenPos     = 3
	mqttJSATokenPos       = 4
	mqttJSAClientIDPos    = 5
	mqttJSAStreamCreate   = "SC"
	mqttJSAStreamUpdate   = "SU"
	mqttJSAStreamLookup   = "SL"
	mqttJSAStreamDel      = "SD"
	mqttJSAConsumerCreate = "CC"
	mqttJSAConsumerLookup = "CL"
	mqttJSAConsumerDel    = "CD"
	mqttJSAMsgStore       = "MS"
	mqttJSAMsgLoad        = "ML"
	mqttJSAMsgDelete      = "MD"
	mqttJSASessPersist    = "SP"
	mqttJSARetainedMsgDel = "RD"
	mqttJSAStreamNames    = "SN"

	// This is how long to keep a client in the flappers map before closing the
	// connection. This prevent quick reconnect from those clients that keep
	// wanting to connect with a client ID already in use.
	mqttSessFlappingJailDur = time.Second

	// This is how frequently the timer to cleanup the sessions flappers map is firing.
	mqttSessFlappingCleanupInterval = 5 * time.Second

	// Default retry delay if transfer of old session streams to new one fails
	mqttDefaultTransferRetry = 5 * time.Second

	// For Websocket URLs
	mqttWSPath = "/mqtt"

	mqttInitialPubHeader        = 16 // An overkill, should need 7 bytes max
	mqttProcessSubTooLong       = 100 * time.Millisecond
	mqttDefaultRetainedCacheTTL = 2 * time.Minute
	mqttRetainedTransferTimeout = 10 * time.Second
	mqttDefaultJSAPITimeout     = 5 * time.Second
	mqttRetainedFlagDelMarker   = '-'
)

const (
	sparkbNBIRTH = "NBIRTH"
	sparkbDBIRTH = "DBIRTH"
	sparkbNDEATH = "NDEATH"
	sparkbDDEATH = "DDEATH"
)

var (
	sparkbNamespaceTopicPrefix    = []byte("spBv1.0/")
	sparkbCertificatesTopicPrefix = []byte("$sparkplug/certificates/")
)

var (
	mqttPingResponse     = []byte{mqttPacketPingResp, 0x0}
	mqttProtoName        = []byte("MQTT")
	mqttOldProtoName     = []byte("MQIsdp")
	mqttSessJailDur      = mqttSessFlappingJailDur
	mqttFlapCleanItvl    = mqttSessFlappingCleanupInterval
	mqttRetainedCacheTTL = mqttDefaultRetainedCacheTTL
)

var (
	errMQTTNotWebsocketPort           = errors.New("MQTT clients over websocket must connect to the Websocket port, not the MQTT port")
	errMQTTTopicFilterCannotBeEmpty   = errors.New("topic filter cannot be empty")
	errMQTTMalformedVarInt            = errors.New("malformed variable int")
	errMQTTSecondConnectPacket        = errors.New("received a second CONNECT packet")
	errMQTTServerNameMustBeSet        = errors.New("mqtt requires server name to be explicitly set")
	errMQTTUserMixWithUsersNKeys      = errors.New("mqtt authentication username not compatible with presence of users/nkeys")
	errMQTTTokenMixWIthUsersNKeys     = errors.New("mqtt authentication token not compatible with presence of users/nkeys")
	errMQTTAckWaitMustBePositive      = errors.New("ack wait must be a positive value")
	errMQTTJSAPITimeoutMustBePositive = errors.New("JS API timeout must be a positive value")
	errMQTTStandaloneNeedsJetStream   = errors.New("mqtt requires JetStream to be enabled if running in standalone mode")
	errMQTTConnFlagReserved           = errors.New("connect flags reserved bit not set to 0")
	errMQTTWillAndRetainFlag          = errors.New("if Will flag is set to 0, Will Retain flag must be 0 too")
	errMQTTPasswordFlagAndNoUser      = errors.New("password flag set but username flag is not")
	errMQTTCIDEmptyNeedsCleanFlag     = errors.New("when client ID is empty, clean session flag must be set to 1")
	errMQTTEmptyWillTopic             = errors.New("empty Will topic not allowed")
	errMQTTEmptyUsername              = errors.New("empty user name not allowed")
	errMQTTTopicIsEmpty               = errors.New("topic cannot be empty")
	errMQTTPacketIdentifierIsZero     = errors.New("packet identifier cannot be 0")
	errMQTTUnsupportedCharacters      = errors.New("character not supported for MQTT topics")
	errMQTTInvalidSession             = errors.New("invalid MQTT session")
	errMQTTInvalidRetainFlags         = errors.New("invalid retained message flags")
	errMQTTInvalidRetainedMessage     = errors.New("invalid retained message")
	errMQTTInvalidQoS2HeldMessage     = errors.New("invalid message in QoS2 PUBREL stream")
	errMQTTSessionCollision           = errors.New("stored session does not match client ID")
	errMQTTInvalidPublishLength       = errors.New("invalid publish message, variable header exceeds remaining length")
	errMQTTAckPipelineStopped         = errors.New("ack pipeline stopped")
)

type srvMQTT struct {
	listener     net.Listener
	listenerErr  error
	authOverride bool
	sessmgr      mqttSessionManager
	readLoopWG   sync.WaitGroup
}

type mqttSessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*mqttAccountSessionManager // key is account name
}

type mqttAccountSessionManager struct {
	mu         sync.RWMutex
	sessions   map[string]*mqttSession                // key is MQTT client ID
	sessByHash map[string]*mqttSession                // key is MQTT client ID hash
	sessLocked map[string]struct{}                    // key is MQTT client ID and indicate that a session can not be taken by a new client at this time
	flappers   map[string]time.Time                   // When connection connects with client ID already in use
	flapTimer  *time.Timer                            // Timer to perform some cleanup of the flappers map
	retmsgs    *stree.SubjectTree[mqttRetainedMsgRef] // retained message metadata
	rmsCache   *sync.Map                              // map[subject]mqttRetainedMsg
	jsa        mqttJSA
	domainTk   string // Domain (with trailing "."), or possibly empty. This is added to session subject.
}

type mqttJSAResponse struct {
	reply string // will be used to map to the original request in jsa.NewRequestExMulti
	value any
}

type mqttJSA struct {
	mu        sync.Mutex
	id        string
	c         *client
	sendq     *ipQueue[*mqttJSPubMsg]
	rplyr     string
	replies   sync.Map // [string]chan *mqttJSAResponse
	nuid      *nuid.NUID
	quitCh    chan struct{}
	domain    string // Domain or possibly empty. This is added to session subject.
	domainSet bool   // covers if domain was set, even to empty
	timeout   time.Duration
	// JS API delete subjects, by stream.
	deleteSubjs map[string]string
}

type mqttJSPubMsg struct {
	subj  string
	reply string
	hdr   int
	msg   []byte
	// Added as a Nats-Msg-Id header by the sendq consumer.
	msgId string
}

type mqttRetMsgDel struct {
	Subject string `json:"subject"`
	Seq     uint64 `json:"seq"`
}

type mqttSession struct {
	// subsMu is a "quick" version of the session lock, sufficient for the QoS0
	// callback. It only guarantees that a new subscription is initialized, and
	// its retained messages if any have been queued up for delivery. The QoS12
	// callback uses the session lock.
	mu     sync.Mutex
	subsMu sync.RWMutex

	id                     string // client ID
	idHash                 string // client ID hash
	c                      *client
	jsa                    *mqttJSA
	subs                   map[string]byte // Key is MQTT SUBSCRIBE filter, value is the subscription QoS
	cons                   map[string]*ConsumerConfig
	pubRelConsumer         *ConsumerConfig
	pubRelSubscribed       bool
	pubRelDeliverySubject  string
	pubRelDeliverySubjectB []byte
	pubRelSubject          string
	seq                    uint64

	// pendingPublish maps packet identifiers (PI) to JetStream ACK subjects for
	// QoS1 and 2 PUBLISH messages pending delivery to the session's client.
	pendingPublish map[uint16]*mqttPending

	// pendingPubRel maps PIs to JetStream ACK subjects for QoS2 PUBREL
	// messages pending delivery to the session's client.
	pendingPubRel map[uint16]*mqttPending

	// cpending maps delivery attempts (that come with a JS ACK subject) to
	// existing PIs.
	cpending map[string]map[uint64]uint16 // composite key: jsDur, sseq

	// "Last used" publish packet identifier (PI). starting point searching for the next available.
	last_pi uint16

	// Maximum number of pending acks for this session.
	maxp     uint16
	tmaxack  int
	clean    bool
	domainTk string
}

type mqttPersistedSession struct {
	Origin string                     `json:"origin,omitempty"`
	ID     string                     `json:"id,omitempty"`
	Clean  bool                       `json:"clean,omitempty"`
	Subs   map[string]byte            `json:"subs,omitempty"`
	Cons   map[string]*ConsumerConfig `json:"cons,omitempty"`
	PubRel *ConsumerConfig            `json:"pubrel,omitempty"`
}

type mqttRetainedMsg struct {
	Origin  string `json:"origin,omitempty"`
	Subject string `json:"subject,omitempty"`
	Topic   string `json:"topic,omitempty"`
	Msg     []byte `json:"msg,omitempty"`
	Flags   byte   `json:"flags,omitempty"`
	Source  string `json:"source,omitempty"`

	expiresFromCache time.Time
}

type mqttRetainedMsgRef struct {
	sseq uint64
}

// mqttSub contains fields associated with a MQTT subscription, and is added to
// the main subscription struct for MQTT message delivery subscriptions. The
// delivery callbacks may get invoked before sub.mqtt is set up, so they should
// acquire either sess.mu or sess.subsMu before accessing it.
type mqttSub struct {
	// The sub's QOS and the JS durable name. They can change when
	// re-subscribing, and are used in the delivery callbacks. They can be
	// quickly accessed using sess.subsMu.RLock, or under the main session lock.
	qos   byte
	jsDur string

	// closed marks the subscription as torn down (QoS downgrade to 0, or
	// unsubscribe) so QoS 1/2 delivery callbacks stop tracking new messages for
	// it. Guarded like qos/jsDur (sess.mu or sess.subsMu). Unlike clearing
	// sub.mqtt, this keeps the struct valid for an in-flight enqueue.
	closed bool

	// Pending serialization of retained messages to be sent when subscription
	// is registered. The sub's delivery callbacks must wait until `prm` is
	// ready (can block on sess.mu for that, too).
	prm [][]byte

	// If this subscription needs to be checked for being reserved. E.g. '#' or
	// '*' or '*/'.  It is set up at the time of subscription and is immutable
	// after that.
	reserved bool
}

type mqtt struct {
	r    *mqttReader
	cp   *mqttConnectProto
	pp   *mqttPublish
	asm  *mqttAccountSessionManager // quick reference to account session manager, immutable after processConnect()
	sess *mqttSession               // quick reference to session, immutable after processConnect()
	cid  string                     // client ID

	// Pipelines PUBACK/PUBREC/PUBCOMP for inbound QoS1/2 packets. readLoop-owned.
	pipe *mqttAckPipeline

	// The source of truth for inbound QoS2 messages while the async
	// JetStream operations are in flight: qos2Exchanges caches the pending
	// ones, qos2Released the PIs whose PUBREL was processed. A PI missing
	// from both falls back to JetStream. readLoop-owned.
	qos2Exchanges map[uint16]*mqttQoS2Exchange
	qos2Released  avl.SequenceSet
	// Bytes held by the qos2Exchanges copies, capped by mqttMaxCachedQoS2Bytes.
	qos2CachedBytes int

	// rejectQoS2Pub tells the MQTT client to not accept QoS2 PUBLISH, instead
	// error and terminate the connection.
	rejectQoS2Pub bool

	// downgradeQOS2Sub tells the MQTT client to downgrade QoS2 SUBSCRIBE
	// requests to QoS1.
	downgradeQoS2Sub bool
}

type mqttPending struct {
	sseq         uint64 // stream sequence
	jsAckSubject string // the ACK subject to send the ack to
	jsDur        string // JS durable name
	qos          byte   // QoS the PUBLISH was delivered with
}

type mqttConnectProto struct {
	rd    time.Duration
	will  *mqttWill
	flags byte
}

type mqttIOReader interface {
	io.Reader
	SetReadDeadline(time.Time) error
}

type mqttReader struct {
	reader mqttIOReader
	buf    []byte
	pos    int
	pstart int
	pbuf   []byte
	owned  bool // buf is backed by pbuf, not the caller's read buffer
}

type mqttWriter struct {
	bytes.Buffer
}

type mqttWill struct {
	topic   []byte
	subject []byte
	mapped  []byte
	message []byte
	qos     byte
	retain  bool
}

type mqttFilter struct {
	filter string
	qos    byte
	// Used only for tracing and should not be used after parsing of (un)sub protocols.
	ttopic []byte
}

type mqttPublish struct {
	topic   []byte
	subject []byte
	mapped  []byte
	msg     []byte
	sz      int
	pi      uint16
	flags   byte
}

// When we re-encode incoming MQTT PUBLISH messages for NATS delivery, we add
// the following headers:
//   - "Nmqtt-Pub" (*always) indicates that the message originated from MQTT, and
//     contains the original message QoS.
//   - "Nmqtt-Subject" contains the original MQTT subject from mqttParsePub.
//   - "Nmqtt-Mapped" contains the mapping during mqttParsePub.
//
// When we submit a PUBREL for delivery, we add a "Nmqtt-PubRel" header that
// contains the PI.
const (
	// NATS header that indicates that the message originated from MQTT and
	// stores the published message QOS.
	mqttNatsHeader = "Nmqtt-Pub"
	// A staged QoS2 message's mqttNatsHeader value carries a second byte after
	// the QoS: the MQTT PUBLISH flags nibble (mqttPubFlags) as one hex char,
	// e.g. "25" for a retained QoS2 message (0x5 = retain|QoS2). The value is a
	// persisted, cross-version contract: byte 0 stays the bare QoS forever
	// (older servers read only it), a missing flags byte reads as no flags, and
	// extensions may only append bytes (a second hex char = a full private
	// byte), never change the meaning of existing ones.

	// NATS headers to store retained message metadata (along with the original
	// message as binary).
	mqttNatsRetainedMessageTopic  = "Nmqtt-RTopic"
	mqttNatsRetainedMessageOrigin = "Nmqtt-ROrigin"
	mqttNatsRetainedMessageFlags  = "Nmqtt-RFlags"
	mqttNatsRetainedMessageSource = "Nmqtt-RSource"

	// NATS header that indicates that the message is an MQTT PubRel and stores
	// the PI.
	mqttNatsPubRelHeader = "Nmqtt-PubRel"

	// NATS headers to store the original MQTT subject and the subject mapping.
	mqttNatsHeaderSubject = "Nmqtt-Subject"
	mqttNatsHeaderMapped  = "Nmqtt-Mapped"
)

type mqttParsedPublishNATSHeader struct {
	qos      byte
	retained bool
	subject  []byte
	mapped   []byte
}

func (s *Server) startMQTT() {
	if s.isShuttingDown() {
		return
	}

	sopts := s.getOpts()
	o := &sopts.MQTT

	var hl net.Listener
	var err error

	port := o.Port
	if port == -1 {
		port = 0
	}
	hp := net.JoinHostPort(o.Host, strconv.Itoa(port))
	s.mu.Lock()
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	hl, err = natsListen("tcp", hp)
	s.mqtt.listenerErr = err
	if err != nil {
		s.mu.Unlock()
		s.Fatalf("Unable to listen for MQTT connections: %v", err)
		return
	}
	if port == 0 {
		o.Port = hl.Addr().(*net.TCPAddr).Port
	}
	s.mqtt.listener = hl
	scheme := "mqtt"
	if o.TLSConfig != nil {
		scheme = "tls"
	}
	s.Noticef("Listening for MQTT clients on %s://%s:%d", scheme, o.Host, o.Port)
	go s.acceptConnections(hl, "MQTT", func(conn net.Conn) { s.createMQTTClient(conn, nil) },
		func(_ error) bool {
			if s.isLameDuckMode() {
				s.ldmCh <- true
				<-s.quitCh
				return true
			}
			return false
		})
	s.mu.Unlock()
}

// This is similar to createClient() but has some modifications specifi to MQTT clients.
// The comments have been kept to minimum to reduce code size. Check createClient() for
// more details.
func (s *Server) createMQTTClient(conn net.Conn, ws *websocket) *client {
	opts := s.getOpts()

	maxPay := int32(opts.MaxPayload)
	maxSubs := int32(opts.MaxSubs)
	if maxSubs == 0 {
		maxSubs = -1
	}
	now := time.Now()

	mqtt := &mqtt{
		rejectQoS2Pub:    opts.MQTT.rejectQoS2Pub,
		downgradeQoS2Sub: opts.MQTT.downgradeQoS2Sub,
	}
	c := &client{srv: s, nc: conn, mpay: maxPay, msubs: maxSubs, start: now, last: now, mqtt: mqtt, ws: ws}
	c.headers = true
	c.mqtt.pp = &mqttPublish{}
	// MQTT clients don't send NATS CONNECT protocols. So make it an "echo"
	// client, but disable verbose and pedantic (by not setting them).
	c.opts.Echo = true

	c.registerWithAccount(s.globalAccount())

	s.mu.Lock()
	// Check auth, override if applicable.
	authRequired := s.info.AuthRequired || s.mqtt.authOverride
	s.totalClients++
	s.mu.Unlock()

	c.mu.Lock()
	if authRequired {
		c.flags.set(expectConnect)
	}
	c.initClient()
	c.Debugf("Client connection created")
	c.mu.Unlock()

	s.mu.Lock()
	if !s.isRunning() || s.isShuttingDown() || s.ldm {
		s.mu.Unlock()
		c.closeConnection(ServerShutdown)
		return nil
	}

	if opts.MaxConn < 0 || (opts.MaxConn > 0 && len(s.clients) >= opts.MaxConn) {
		s.mu.Unlock()
		c.maxConnExceeded()
		return nil
	}
	s.clients[c.cid] = c
	// Register before releasing the server lock so shutdown also waits for
	// clients that are still completing their TLS handshake.
	s.mqtt.readLoopWG.Add(1)
	readLoopStarted := false
	defer func() {
		if !readLoopStarted {
			s.mqtt.readLoopWG.Done()
		}
	}()

	// Websocket TLS handshake is already done when getting to this function.
	tlsRequired := opts.MQTT.TLSConfig != nil && ws == nil
	s.mu.Unlock()

	c.mu.Lock()

	// In case connection has already been closed
	if c.isClosed() {
		c.mu.Unlock()
		c.closeConnection(WriteError)
		return nil
	}

	var pre []byte
	if tlsRequired && opts.AllowNonTLS {
		pre = make([]byte, 4)
		c.nc.SetReadDeadline(time.Now().Add(secondsToDuration(opts.MQTT.TLSTimeout)))
		n, _ := io.ReadFull(c.nc, pre[:])
		c.nc.SetReadDeadline(time.Time{})
		pre = pre[:n]
		if n > 0 && pre[0] == 0x16 {
			tlsRequired = true
		} else {
			tlsRequired = false
		}
	}

	if tlsRequired {
		if len(pre) > 0 {
			c.nc = &tlsMixConn{c.nc, bytes.NewBuffer(pre)}
			pre = nil
		}

		// Perform server-side TLS handshake.
		if err := c.doTLSServerHandshake(tlsHandshakeMQTT, opts.MQTT.TLSConfig, opts.MQTT.TLSTimeout, opts.MQTT.TLSPinnedCerts); err != nil {
			c.mu.Unlock()
			return nil
		}
	}

	if authRequired {
		timeout := opts.AuthTimeout
		// Possibly override with MQTT specific value.
		if opts.MQTT.AuthTimeout != 0 {
			timeout = opts.MQTT.AuthTimeout
		}
		c.setAuthTimer(secondsToDuration(timeout))
	}

	// No Ping timer for MQTT clients...

	readLoopStarted = s.startGoRoutine(func() {
		defer s.mqtt.readLoopWG.Done()
		c.readLoop(pre)
	})
	s.startGoRoutine(func() { c.writeLoop() })

	if tlsRequired {
		c.Debugf("TLS handshake complete")
		cs := c.nc.(*tls.Conn).ConnectionState()
		c.Debugf("TLS version %s, cipher suite %s", tlsVersion(cs.Version), tls.CipherSuiteName(cs.CipherSuite))
	}

	c.mu.Unlock()

	return c
}

// Given the mqtt options, we check if any auth configuration
// has been provided. If so, possibly create users/nkey users and
// store them in s.mqtt.users/nkeys.
// Also update a boolean that indicates if auth is required for
// mqtt clients.
// Server lock is held on entry.
func (s *Server) mqttConfigAuth(opts *MQTTOpts) {
	mqtt := &s.mqtt
	// If any of those is specified, we consider that there is an override.
	mqtt.authOverride = opts.Username != _EMPTY_ || opts.Token != _EMPTY_ || opts.NoAuthUser != _EMPTY_
}

// Validate the mqtt related options.
func validateMQTTOptions(o *Options) error {
	mo := &o.MQTT
	// If no port is defined, we don't care about other options
	if mo.Port == 0 {
		return nil
	}
	// We have to force the server name to be explicitly set and be unique when
	// in cluster mode.
	if o.ServerName == _EMPTY_ && (o.Cluster.Port != 0 || o.Gateway.Port != 0) {
		return errMQTTServerNameMustBeSet
	}
	// If there is a NoAuthUser, we need to have Users defined and
	// the user to be present.
	if mo.NoAuthUser != _EMPTY_ {
		if err := validateNoAuthUser(o, mo.NoAuthUser); err != nil {
			return err
		}
	}
	// Token/Username not possible if there are users/nkeys
	if len(o.Users) > 0 || len(o.Nkeys) > 0 {
		if mo.Username != _EMPTY_ {
			return errMQTTUserMixWithUsersNKeys
		}
		if mo.Token != _EMPTY_ {
			return errMQTTTokenMixWIthUsersNKeys
		}
	}
	if mo.AckWait < 0 {
		return errMQTTAckWaitMustBePositive
	}
	if mo.JSAPITimeout < 0 {
		return errMQTTJSAPITimeoutMustBePositive
	}
	// If strictly standalone and there is no JS enabled, then it won't work...
	// For leafnodes, we could either have remote(s) and it would be ok, or no
	// remote but accept from a remote side that has "hub" property set, which
	// then would ok too. So we fail only if we have no leafnode config at all.
	if !o.JetStream && o.Cluster.Port == 0 && o.Gateway.Port == 0 &&
		o.LeafNode.Port == 0 && len(o.LeafNode.Remotes) == 0 {
		return errMQTTStandaloneNeedsJetStream
	}
	if err := validatePinnedCerts(mo.TLSPinnedCerts); err != nil {
		return fmt.Errorf("mqtt: %v", err)
	}
	if mo.ConsumerReplicas > 0 && mo.StreamReplicas > 0 && mo.ConsumerReplicas > mo.StreamReplicas {
		return fmt.Errorf("mqtt: consumer_replicas (%v) cannot be higher than stream_replicas (%v)",
			mo.ConsumerReplicas, mo.StreamReplicas)
	}
	return nil
}

// Returns true if this connection is from a MQTT client.
// Lock held on entry.
func (c *client) isMqtt() bool {
	return c.mqtt != nil
}

// If this is an MQTT client, returns the session client ID,
// otherwise returns the empty string.
// Lock held on entry
func (c *client) getMQTTClientID() string {
	if !c.isMqtt() {
		return _EMPTY_
	}
	return c.mqtt.cid
}

// Parse protocols inside the given buffer.
// This is invoked from the readLoop.
func (c *client) mqttParse(buf []byte) error {
	c.mu.Lock()
	s := c.srv
	trace := c.trace
	connected := c.flags.isSet(connectReceived)
	mqtt := c.mqtt
	r := mqtt.r
	var rd time.Duration
	if mqtt.cp != nil {
		rd = mqtt.cp.rd
		if rd > 0 {
			r.reader.SetReadDeadline(time.Time{})
		}
	}
	hasMappings := c.in.flags.isSet(hasMappings)
	c.mu.Unlock()

	r.reset(buf)

	var err error
	var b byte
	var pl int
	var complete bool

	for err == nil && r.hasMore() {

		// Keep track of the starting of the packet, in case we have a partial
		r.pstart = r.pos

		// Read packet type and flags
		if b, err = r.readByte("packet type"); err != nil {
			break
		}

		// Packet type
		pt := b & mqttPacketMask

		// If client was not connected yet, the first packet must be
		// a mqttPacketConnect otherwise we fail the connection.
		if !connected && pt != mqttPacketConnect {
			// If the buffer indicates that it may be a websocket handshake
			// but the client is not websocket, it means that the client
			// connected to the MQTT port instead of the Websocket port.
			if bytes.HasPrefix(buf, []byte("GET ")) && !c.isWebsocket() {
				err = errMQTTNotWebsocketPort
			} else {
				err = fmt.Errorf("the first packet should be a CONNECT (%v), got %v", mqttPacketConnect, pt)
			}
			break
		}
		if err = mqttCheckFixedHeaderFlags(pt, b&mqttPacketFlagMask); err != nil {
			break
		}

		mpay := atomic.LoadInt32(&c.mpay)
		maxLen := mpay
		if connected && maxLen != jwt.NoLimit && maxLen <= math.MaxInt32-mqttMaxPacketOverhead {
			// Allow room for a topic or filter, which is not capped by max_payload.
			maxLen += mqttMaxPacketOverhead
		}
		pl, complete, err = r.readPacketLen(pt, maxLen)
		if err != nil || !complete {
			if err == ErrMaxPayload {
				c.maxPayloadViolation(pl, mpay)
			}
			break
		}

		switch pt {
		// Packets that we receive back when we act as the "sender": PUBACK,
		// PUBREC, PUBCOMP.
		case mqttPacketPubAck:
			var pi uint16
			pi, err = mqttParsePIPacket(r)
			if trace {
				c.traceInOp("PUBACK", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				err = c.mqttProcessPubAck(pi)
			}

		case mqttPacketPubRec:
			var pi uint16
			pi, err = mqttParsePIPacket(r)
			if trace {
				c.traceInOp("PUBREC", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				err = c.mqttProcessPubRec(pi)
			}

		case mqttPacketPubComp:
			var pi uint16
			pi, err = mqttParsePIPacket(r)
			if trace {
				c.traceInOp("PUBCOMP", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				c.mqttProcessPubComp(pi)
			}

		// Packets where we act as the "receiver": PUBLISH, PUBREL, SUBSCRIBE, UNSUBSCRIBE.
		case mqttPacketPub:
			pp := c.mqtt.pp
			pp.flags = b & mqttPacketFlagMask
			err = c.mqttParsePub(r, pl, pp, hasMappings)
			if trace {
				c.traceInOp("PUBLISH", errOrTrace(err, mqttPubTrace(pp)))
				if err == nil {
					c.mqttTraceMsg(pp.msg)
				}
			}
			if err == nil {
				err = s.mqttProcessPub(c, pp)
			}

		case mqttPacketPubRel:
			var pi uint16
			pi, err = mqttParsePIPacket(r)
			if trace {
				c.traceInOp("PUBREL", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				err = s.mqttProcessPubRel(c, pi)
			}

		case mqttPacketSub:
			var pi uint16 // packet identifier
			var filters []*mqttFilter
			var subs []*subscription
			pi, filters, err = c.mqttParseSubs(r, b, pl)
			if trace {
				c.traceInOp("SUBSCRIBE", errOrTrace(err, mqttSubscribeTrace(pi, filters)))
			}
			if err == nil {
				subs, err = c.mqttProcessSubs(filters)
				if err == nil && trace {
					c.traceOutOp("SUBACK", []byte(fmt.Sprintf("pi=%v", pi)))
				}
			}
			if err == nil {
				c.mqttEnqueueSubAck(pi, filters)
				c.mqttSendRetainedMsgsToNewSubs(subs)
			}

		case mqttPacketUnsub:
			var pi uint16 // packet identifier
			var filters []*mqttFilter
			pi, filters, err = c.mqttParseUnsubs(r, b, pl)
			if trace {
				c.traceInOp("UNSUBSCRIBE", errOrTrace(err, mqttUnsubscribeTrace(pi, filters)))
			}
			if err == nil {
				err = c.mqttProcessUnsubs(filters)
				if err == nil && trace {
					c.traceOutOp("UNSUBACK", []byte(fmt.Sprintf("pi=%v", pi)))
				}
			}
			if err == nil {
				c.mqttEnqueueUnsubAck(pi)
			}

		// Packets that we get both as a receiver and sender: PING, CONNECT, DISCONNECT
		case mqttPacketPing:
			if trace {
				c.traceInOp("PINGREQ", nil)
			}
			c.mqttEnqueuePingResp()
			if trace {
				c.traceOutOp("PINGRESP", nil)
			}

		case mqttPacketConnect:
			// It is an error to receive a second connect packet
			if connected {
				err = errMQTTSecondConnectPacket
				break
			}
			var rc byte
			var cp *mqttConnectProto
			var sessp bool
			rc, cp, err = c.mqttParseConnect(r, hasMappings)
			// Add the client id to the client's string, regardless of error.
			// We may still get the client_id if the call above fails somewhere
			// after parsing the client ID itself.
			c.ncs.Store(fmt.Sprintf("%s - %q", c, c.mqtt.cid))
			if trace && cp != nil {
				c.traceInOp("CONNECT", errOrTrace(err, c.mqttConnectTrace(cp)))
			}
			if rc != 0 {
				c.mqttEnqueueConnAck(rc, sessp)
				if trace {
					c.traceOutOp("CONNACK", []byte(fmt.Sprintf("sp=%v rc=%v", sessp, rc)))
				}
			} else if err == nil {
				if err = s.mqttProcessConnect(c, cp, trace); err != nil {
					err = fmt.Errorf("unable to connect: %v", err)
				} else {
					// Add this debug statement so users running in Debug mode
					// will have the client id printed here for the first time.
					c.Debugf("Client connected")
					connected = true
					rd = cp.rd
				}
			}

		case mqttPacketDisconnect:
			if trace {
				c.traceInOp("DISCONNECT", nil)
			}
			// Normal disconnect, we need to discard the will.
			// Spec [MQTT-3.1.2-8]
			c.mu.Lock()
			if c.mqtt.cp != nil {
				c.mqtt.cp.will = nil
			}
			c.mu.Unlock()
			s.mqttHandleClosedClient(c)
			c.closeConnection(ClientClosed)
			return nil

		default:
			err = fmt.Errorf("received unknown packet type %d", pt>>4)
		}
	}
	if err == nil && rd > 0 {
		r.reader.SetReadDeadline(time.Now().Add(rd))
	}
	return err
}

func mqttCheckFixedHeaderFlags(packetType, flags byte) error {
	var expected byte
	switch packetType {
	case mqttPacketConnect, mqttPacketPubAck, mqttPacketPubRec, mqttPacketPubComp,
		mqttPacketPing, mqttPacketDisconnect:
		expected = 0
	case mqttPacketPubRel, mqttPacketSub, mqttPacketUnsub:
		expected = 0x2
	case mqttPacketPub:
		return nil
	default:
		return nil
	}
	if flags != expected {
		return fmt.Errorf("invalid fixed header flags %x for packet type %x", flags, packetType)
	}
	return nil
}

func mqttCheckRemainingLength(packetType byte, pl int) error {
	var expected int
	switch packetType {
	case mqttPacketConnect, mqttPacketPub, mqttPacketSub, mqttPacketUnsub:
		return nil
	case mqttPacketPubAck, mqttPacketPubRec, mqttPacketPubRel, mqttPacketPubComp:
		expected = 2
	case mqttPacketPing, mqttPacketDisconnect:
		expected = 0
	default:
		return nil
	}
	if pl != expected {
		return fmt.Errorf("invalid remaining length %d for packet type %x", pl, packetType)
	}
	return nil
}

func (c *client) mqttTraceMsg(msg []byte) {
	maxTrace := c.srv.getOpts().MaxTracedMsgLen
	if maxTrace > 0 && len(msg) > maxTrace {
		c.Tracef("<<- MSG_PAYLOAD: [\"%s...\"]", msg[:maxTrace])
	} else {
		c.Tracef("<<- MSG_PAYLOAD: [%q]", msg)
	}
}

// The MQTT client connection has been closed, or the DISCONNECT packet was received.
// For a "clean" session, we will delete the session, otherwise, simply removing
// the binding. We will also send the "will" message if applicable.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (s *Server) mqttHandleClosedClient(c *client) {
	c.mu.Lock()
	asm := c.mqtt.asm
	sess := c.mqtt.sess
	c.mu.Unlock()

	// Dropping pending entries is safe: the client re-sends unacknowledged
	// PUBLISH and PUBREL packets on reconnect, Spec [MQTT-4.4.0-1].
	if pipe := c.mqtt.pipe; pipe != nil {
		pipe.shutdown()
	}

	// If asm or sess are nil, it means that we have failed a client
	// before it was associated with a session, so nothing more to do.
	if asm == nil || sess == nil {
		return
	}

	// Add this session to the locked map for the rest of the execution.
	if err := asm.lockSession(sess, c); err != nil {
		return
	}
	defer asm.unlockSession(sess)

	asm.mu.Lock()
	// Clear the client from the session, but session may stay.
	sess.mu.Lock()
	sess.c = nil
	doClean := sess.clean
	sess.mu.Unlock()
	// If it was a clean session, then we remove from the account manager,
	// and we will call clear() outside of any lock.
	if doClean {
		asm.removeSession(sess, false)
	}
	// Remove in case it was in the flappers map.
	asm.removeSessFromFlappers(sess.id)
	asm.mu.Unlock()

	// This needs to be done outside of any lock.
	if doClean {
		if err := sess.clear(true); err != nil {
			c.Errorf(err.Error())
		}
	}

	// Now handle the "will". This function will be a no-op if there is no "will" to send.
	s.mqttHandleWill(c)
}

// Updates the MaxAckPending for all MQTT sessions, updating the
// JetStream consumers and updating their max ack pending and forcing
// a expiration of pending messages.
//
// Runs from a server configuration reload routine.
// No lock held on entry.
func (s *Server) mqttUpdateMaxAckPending(newmaxp uint16) {
	// Same default as mqttSessionCreate: an unset option must not leave the
	// sessions with a limit of 0, which would stop all QoS 1 and 2 deliveries.
	if newmaxp == 0 {
		newmaxp = mqttDefaultMaxAckPending
	}
	msm := &s.mqtt.sessmgr
	s.accounts.Range(func(k, _ any) bool {
		accName := k.(string)
		msm.mu.RLock()
		asm := msm.sessions[accName]
		msm.mu.RUnlock()
		if asm == nil {
			// Move to next account
			return true
		}
		asm.mu.RLock()
		for _, sess := range asm.sessions {
			sess.mu.Lock()
			sess.maxp = newmaxp
			sess.mu.Unlock()
		}
		asm.mu.RUnlock()
		return true
	})
}

func (s *Server) mqttGetJSAForAccount(acc string) *mqttJSA {
	sm := &s.mqtt.sessmgr

	sm.mu.RLock()
	asm := sm.sessions[acc]
	sm.mu.RUnlock()

	if asm == nil {
		return nil
	}

	asm.mu.RLock()
	jsa := &asm.jsa
	asm.mu.RUnlock()
	return jsa
}

func (s *Server) mqttStoreQoSMsgForAccountOnNewSubject(hdr int, msg []byte, acc, subject string) {
	if s == nil || hdr <= 0 {
		return
	}
	h := mqttParsePublishNATSHeader(msg[:hdr])
	if h == nil || h.qos == 0 {
		return
	}
	jsa := s.mqttGetJSAForAccount(acc)
	if jsa == nil {
		return
	}
	jsa.storeMsg(mqttStreamSubjectPrefix+subject, hdr, msg)
}

// Encodes the MQTT PUBLISH flags nibble (mqttPubFlags) as one hex char, the
// flags byte that follows the QoS in a mqttNatsHeader value.
func mqttNatsHeaderEncodeFlags(ppFlags byte) byte {
	return "0123456789abcdef"[ppFlags&mqttPubFlags]
}

// Decodes the MQTT flags nibble carried after the QoS in a mqttNatsHeader
// value; values without the flags byte read as no flags set. Callers test the
// result with the mqttPubFlag* bits.
func mqttNatsHeaderDecodeFlags(value []byte) byte {
	if len(value) < 2 {
		return 0
	}
	switch c := value[1]; {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	}
	return 0
}

func mqttParsePublishNATSHeader(headerBytes []byte) *mqttParsedPublishNATSHeader {
	if len(headerBytes) == 0 {
		return nil
	}

	pubValue := getHeader(mqttNatsHeader, headerBytes)
	if len(pubValue) == 0 {
		return nil
	}
	return &mqttParsedPublishNATSHeader{
		qos:      pubValue[0] - '0',
		retained: mqttIsRetained(mqttNatsHeaderDecodeFlags(pubValue)),
		subject:  getHeader(mqttNatsHeaderSubject, headerBytes),
		mapped:   getHeader(mqttNatsHeaderMapped, headerBytes),
	}
}

func mqttParsePubRelNATSHeader(headerBytes []byte) uint16 {
	if len(headerBytes) == 0 {
		return 0
	}

	pubrelValue := getHeader(mqttNatsPubRelHeader, headerBytes)
	if len(pubrelValue) == 0 {
		return 0
	}
	pi, _ := strconv.ParseUint(string(pubrelValue), 10, 16)
	return uint16(pi)
}

// Returns the MQTT sessions manager for a given account.
// If new, creates the required JetStream streams/consumers
// for handling of sessions and messages.
func (s *Server) getOrCreateMQTTAccountSessionManager(c *client) (*mqttAccountSessionManager, error) {
	sm := &s.mqtt.sessmgr

	c.mu.Lock()
	acc := c.acc
	c.mu.Unlock()
	accName := acc.GetName()

	sm.mu.RLock()
	asm, ok := sm.sessions[accName]
	sm.mu.RUnlock()

	if ok {
		return asm, nil
	}

	// We will pass the quitCh to the account session manager if we happen to create it.
	s.mu.Lock()
	quitCh := s.quitCh
	s.mu.Unlock()

	// Not found, now take the write lock and check again
	sm.mu.Lock()
	defer sm.mu.Unlock()
	asm, ok = sm.sessions[accName]
	if ok {
		return asm, nil
	}
	// Need to create one here.
	asm, err := s.mqttCreateAccountSessionManager(acc, quitCh)
	if err != nil {
		return nil, err
	}
	sm.sessions[accName] = asm
	return asm, nil
}

// Creates JS streams/consumers for handling of sessions and messages for this account.
//
// Global session manager lock is held on entry.
func (s *Server) mqttCreateAccountSessionManager(acc *Account, quitCh chan struct{}) (*mqttAccountSessionManager, error) {
	var err error

	accName := acc.GetName()

	opts := s.getOpts()
	c := s.createInternalAccountClient()
	c.acc = acc

	id := s.NodeName()

	mqttJSAPITimeout := opts.MQTT.JSAPITimeout
	if mqttJSAPITimeout == 0 {
		mqttJSAPITimeout = mqttDefaultJSAPITimeout
	}

	replicas := opts.MQTT.StreamReplicas
	if replicas <= 0 {
		replicas = s.mqttDetermineReplicas()
	}
	qname := fmt.Sprintf("[ACC:%s] MQTT ", accName)
	as := &mqttAccountSessionManager{
		sessions:   make(map[string]*mqttSession),
		sessByHash: make(map[string]*mqttSession),
		sessLocked: make(map[string]struct{}),
		flappers:   make(map[string]time.Time),
		jsa: mqttJSA{
			id:      id,
			c:       c,
			rplyr:   mqttJSARepliesPrefix + id + ".",
			sendq:   newIPQueue[*mqttJSPubMsg](s, qname+"send"),
			nuid:    nuid.New(),
			quitCh:  quitCh,
			timeout: mqttJSAPITimeout,
		},
		rmsCache: &sync.Map{},
	}
	// TODO record domain name in as here

	// The domain to communicate with may be required for JS calls.
	// Search from specific (per account setting) to generic (mqtt setting)
	if opts.JsAccDefaultDomain != nil {
		if d, ok := opts.JsAccDefaultDomain[accName]; ok {
			if d != _EMPTY_ {
				as.jsa.domain = d
			}
			as.jsa.domainSet = true
		}
		// in case domain was set to empty, check if there are more generic domain overwrites
	}
	if as.jsa.domain == _EMPTY_ {
		if d := opts.MQTT.JsDomain; d != _EMPTY_ {
			as.jsa.domain = d
			as.jsa.domainSet = true
		}
	}
	// We need to include the domain in the subject prefix used to store sessions in the $MQTT_sess stream.
	if as.jsa.domainSet {
		if as.jsa.domain != _EMPTY_ {
			as.domainTk = as.jsa.domain + "."
		}
	} else if d := s.getOpts().JetStreamDomain; d != _EMPTY_ {
		as.domainTk = d + "."
	}
	as.jsa.deleteSubjs = make(map[string]string)
	for _, stream := range []string{mqttSessStreamName, mqttRetainedMsgsStreamName, mqttQoS2IncomingMsgsStreamName} {
		as.jsa.deleteSubjs[stream] = as.jsa.prefixDomain(fmt.Sprintf(JSApiMsgDeleteT, stream))
	}
	if as.jsa.domainSet {
		s.Noticef("Creating MQTT streams/consumers with replicas %v for account %q in domain %q", replicas, accName, as.jsa.domain)
	} else {
		s.Noticef("Creating MQTT streams/consumers with replicas %v for account %q", replicas, accName)
	}

	var subs []*subscription
	var success bool
	closeCh := make(chan struct{})

	defer func() {
		if success {
			return
		}
		for _, sub := range subs {
			c.processUnsub(sub.sid)
		}
		close(closeCh)
	}()

	// We create all subscriptions before starting the go routine that will do
	// sends otherwise we could get races.
	// Note that using two different clients (one for the subs, one for the
	// sends) would cause other issues such as registration of recent subs in
	// the "sub" client would be invisible to the check for GW routed replies
	// (shouldMapReplyForGatewaySend) since the client there would be the "sender".

	jsa := &as.jsa
	sid := int64(1)
	// This is a subscription that will process all JS API replies. We could split to
	// individual subscriptions if needed, but since there is a bit of common code,
	// that seemed like a good idea to be all in one place.
	if err := as.createSubscription(jsa.rplyr+">",
		as.processJSAPIReplies, &sid, &subs); err != nil {
		return nil, err
	}

	// We will listen for replies to session persist requests so that we can
	// detect the use of a session with the same client ID anywhere in the cluster.
	//   `$MQTT.JSA.{js-id}.SP.{client-id-hash}.{uuid}`
	if err := as.createSubscription(mqttJSARepliesPrefix+"*."+mqttJSASessPersist+".*.*",
		as.processSessionPersist, &sid, &subs); err != nil {
		return nil, err
	}

	// We create the subscription on "$MQTT.sub.<nuid>" to limit the subjects
	// that a user would allow permissions on.
	rmsubj := mqttSubPrefix + nuid.Next()
	if err := as.createSubscription(rmsubj, as.processRetainedMsg, &sid, &subs); err != nil {
		return nil, err
	}

	// Create a subscription to be notified of retained messages delete requests.
	rmdelsubj := mqttJSARepliesPrefix + "*." + mqttJSARetainedMsgDel
	if err := as.createSubscription(rmdelsubj, as.processRetainedMsgDel, &sid, &subs); err != nil {
		return nil, err
	}

	// No more creation of subscriptions past this point otherwise RACEs may happen.

	// Start the go routine that will send JS API requests.
	s.startGoRoutine(func() {
		defer s.grWG.Done()
		as.sendJSAPIrequests(s, c, accName, closeCh)
	})

	// Start the go routine that will clean up cached retained messages that expired.
	s.startGoRoutine(func() {
		defer s.grWG.Done()
		as.cleanupRetainedMessageCache(s, closeCh)
	})

	lookupStream := func(stream, txt string) (*StreamInfo, error) {
		si, err := jsa.lookupStream(stream)
		if err != nil {
			if IsNatsErr(err, JSStreamNotFoundErr) {
				return nil, nil
			}
			return nil, fmt.Errorf("lookup %s stream for account %q: %v", txt, accName, err)
		}
		if opts.MQTT.StreamReplicas == 0 {
			return si, nil
		}
		sr := 1
		if si.Cluster != nil {
			sr += len(si.Cluster.Replicas)
		}
		if replicas != sr {
			s.Warnf("MQTT %s stream replicas mismatch: current is %v but configuration is %v for '%s > %s'",
				txt, sr, replicas, accName, stream)
		}
		return si, nil
	}

	if si, err := lookupStream(mqttSessStreamName, "sessions"); err != nil {
		return nil, err
	} else if si == nil {
		// Create the stream for the sessions.
		cfg := &StreamConfig{
			Name:       mqttSessStreamName,
			Subjects:   []string{mqttSessStreamSubjectPrefix + as.domainTk + ">"},
			Storage:    FileStorage,
			Retention:  LimitsPolicy,
			Replicas:   replicas,
			MaxMsgsPer: 1,
		}
		if _, created, err := jsa.createStream(cfg); err == nil && created {
			as.transferUniqueSessStreamsToMuxed(s)
		} else if isErrorOtherThan(err, JSStreamNameExistErr) {
			return nil, fmt.Errorf("create sessions stream for account %q: %v", accName, err)
		}
	}

	if si, err := lookupStream(mqttStreamName, "messages"); err != nil {
		return nil, err
	} else if si == nil {
		// Create the stream for the messages.
		cfg := &StreamConfig{
			Name:      mqttStreamName,
			Subjects:  []string{mqttStreamSubjectPrefix + ">"},
			Storage:   FileStorage,
			Retention: InterestPolicy,
			Replicas:  replicas,
		}
		if _, _, err := jsa.createStream(cfg); isErrorOtherThan(err, JSStreamNameExistErr) {
			return nil, fmt.Errorf("create messages stream for account %q: %v", accName, err)
		}
	}

	if si, err := lookupStream(mqttQoS2IncomingMsgsStreamName, "QoS2 incoming messages"); err != nil {
		return nil, err
	} else if si == nil {
		// Create the stream for the incoming QoS2 messages that have not been
		// PUBREL-ed by the sender. Subject is
		// "$MQTT.qos2.<session>.<PI>", the .PI is to achieve exactly
		// once for each PI.
		cfg := &StreamConfig{
			Name:          mqttQoS2IncomingMsgsStreamName,
			Subjects:      []string{mqttQoS2IncomingMsgsStreamSubjectPrefix + ">"},
			Storage:       FileStorage,
			Retention:     LimitsPolicy,
			Discard:       DiscardNew,
			MaxMsgsPer:    1,
			DiscardNewPer: true,
			Replicas:      replicas,
		}
		if _, _, err := jsa.createStream(cfg); isErrorOtherThan(err, JSStreamNameExistErr) {
			return nil, fmt.Errorf("create QoS2 incoming messages stream for account %q: %v", accName, err)
		}
	}

	if si, err := lookupStream(mqttOutStreamName, "QoS2 outgoing PUBREL"); err != nil {
		return nil, err
	} else if si == nil {
		// Create the stream for the incoming QoS2 messages that have not been
		// PUBREL-ed by the sender. NATS messages are submitted as
		// "$MQTT.pubrel.<session hash>"
		cfg := &StreamConfig{
			Name:      mqttOutStreamName,
			Subjects:  []string{mqttOutSubjectPrefix + ">"},
			Storage:   FileStorage,
			Retention: InterestPolicy,
			Replicas:  replicas,
		}
		if _, _, err := jsa.createStream(cfg); isErrorOtherThan(err, JSStreamNameExistErr) {
			return nil, fmt.Errorf("create QoS2 outgoing PUBREL stream for account %q: %v", accName, err)
		}
	}

	// This is the only case where we need "si" after lookup/create
	needToTransfer := true
	si, err := lookupStream(mqttRetainedMsgsStreamName, "retained messages")
	switch {
	case err != nil:
		return nil, err

	case si == nil:
		// Create the stream for retained messages.
		cfg := &StreamConfig{
			Name:       mqttRetainedMsgsStreamName,
			Subjects:   []string{mqttRetainedMsgsStreamSubject + ">"},
			Storage:    FileStorage,
			Retention:  LimitsPolicy,
			Replicas:   replicas,
			MaxMsgsPer: 1,
		}
		// We will need "si" outside of this block.
		si, _, err = jsa.createStream(cfg)
		if err != nil {
			if isErrorOtherThan(err, JSStreamNameExistErr) {
				return nil, fmt.Errorf("create retained messages stream for account %q: %v", accName, err)
			}
			// Suppose we had a race and the stream was actually created by another
			// node, we really need "si" after that, so lookup the stream again here.
			si, err = lookupStream(mqttRetainedMsgsStreamName, "retained messages")
			if err != nil {
				return nil, err
			}
		}
		needToTransfer = false

	default:
		needToTransfer = si.Config.MaxMsgsPer != 1
	}
	// Guard before dereferencing si.Config below.
	if si == nil {
		return nil, fmt.Errorf("could not look up or create the retained messages stream for account %q", accName)
	}

	// Doing this check outside of above if/else due to possible race when
	// creating the stream.
	wantedSubj := mqttRetainedMsgsStreamSubject + ">"
	if len(si.Config.Subjects) != 1 || si.Config.Subjects[0] != wantedSubj {
		// Update only the Subjects at this stage, not MaxMsgsPer yet.
		si.Config.Subjects = []string{wantedSubj}
		if si, err = jsa.updateStream(&si.Config); err != nil {
			return nil, fmt.Errorf("failed to update stream config: %w", err)
		}
	}

	transferRMS := func() error {
		if !needToTransfer {
			return nil
		}

		as.transferRetainedToPerKeySubjectStream(s)

		// We need another lookup to have up-to-date si.State values in order
		// to load all retained messages.
		si, err = lookupStream(mqttRetainedMsgsStreamName, "retained messages")
		if err != nil {
			return err
		}
		needToTransfer = false
		return nil
	}

	// Attempt to transfer all "single subject" retained messages to new
	// subjects. It may fail, will log its own error; ignore it the first time
	// and proceed to updating MaxMsgsPer. Then we invoke transferRMS() again,
	// which will get another chance to resolve the error; if not we bail there.
	if err = transferRMS(); err != nil {
		return nil, err
	}
	// Guard before dereferencing si.Config below.
	if si == nil {
		return nil, fmt.Errorf("could not look up the retained messages stream for account %q", accName)
	}

	// Now, if the stream does not have MaxMsgsPer set to 1, and there are no
	// more messages on the single $MQTT.rmsgs subject, update the stream again.
	if si.Config.MaxMsgsPer != 1 {
		si.Config.MaxMsgsPer = 1
		// We will need an up-to-date si, so don't use local variable here.
		if si, err = jsa.updateStream(&si.Config); err != nil {
			return nil, fmt.Errorf("failed to update stream config: %w", err)
		}
	}

	// If we failed the first time, there is now at most one lingering message
	// in the old subject. Try again (it will be a NO-OP if succeeded the first
	// time).
	if err = transferRMS(); err != nil {
		return nil, err
	}

	// Opportunistically delete the old (legacy) consumer, from v2.10.10 and
	// before. Ignore any errors that might arise.
	rmLegacyDurName := mqttRetainedMsgsStreamName + "_" + jsa.id
	jsa.deleteConsumer(mqttRetainedMsgsStreamName, rmLegacyDurName, true)

	// Create a new, uniquely names consumer for retained messages for this
	// server. The prior one will expire eventually.
	ccfg := &CreateConsumerRequest{
		Stream: mqttRetainedMsgsStreamName,
		Config: ConsumerConfig{
			Name:              mqttRetainedMsgsStreamName + "_" + nuid.Next(),
			FilterSubject:     mqttRetainedMsgsStreamSubject + ">",
			DeliverSubject:    rmsubj,
			ReplayPolicy:      ReplayInstant,
			AckPolicy:         AckNone,
			InactiveThreshold: 5 * time.Minute,
		},
	}
	if _, err := jsa.createEphemeralConsumer(ccfg); err != nil {
		return nil, fmt.Errorf("create retained messages consumer for account %q: %v", accName, err)
	}

	// Set this so that on defer we don't cleanup.
	success = true

	return as, nil
}

func (s *Server) mqttDetermineReplicas() int {
	// If not clustered, then replica will be 1.
	if !s.JetStreamIsClustered() {
		return 1
	}
	opts := s.getOpts()
	replicas := 0
	for _, u := range opts.Routes {
		host := u.Hostname()
		// If this is an IP just add one.
		if net.ParseIP(host) != nil {
			replicas++
		} else {
			addrs, _ := net.LookupHost(host)
			replicas += len(addrs)
		}
	}
	if replicas < 1 {
		replicas = 1
	} else if replicas > 3 {
		replicas = 3
	}
	return replicas
}

//////////////////////////////////////////////////////////////////////////////
//
// JS APIs related functions
//
//////////////////////////////////////////////////////////////////////////////

func (jsa *mqttJSA) newRequest(kind, subject string, hdr int, msg []byte) (any, error) {
	return jsa.newRequestEx(kind, subject, _EMPTY_, hdr, msg)
}

func (jsa *mqttJSA) prefixDomain(subject string) string {
	if jsa.domain != _EMPTY_ {
		// rewrite js api prefix with domain
		if sub := strings.TrimPrefix(subject, JSApiPrefix+"."); sub != subject {
			subject = fmt.Sprintf("$JS.%s.API.%s", jsa.domain, sub)
		}
	}
	return subject
}

func (jsa *mqttJSA) newRequestEx(kind, subject, cidHash string, hdr int, msg []byte) (any, error) {
	responses, err := jsa.newRequestExMulti(kind, subject, cidHash, []int{hdr}, [][]byte{msg})
	if err != nil {
		return nil, err
	}
	if len(responses) != 1 {
		return nil, fmt.Errorf("unreachable: invalid number of responses (%d)", len(responses))
	}
	return responses[0].value, nil
}

// newRequestExMulti sends multiple messages on the same subject and waits for
// all responses. It returns the same number of responses in the same order as
// msgs parameter. In case of a timeout it returns an error as well as all
// responses received as a sparsely populated array, matching msgs, with nils
// for the values that have not yet been received.
//
// Note that each response may represent an error and should be inspected as
// such by the caller.
func (jsa *mqttJSA) newRequestExMulti(kind, subject, cidHash string, hdrs []int, msgs [][]byte) ([]*mqttJSAResponse, error) {
	if len(hdrs) != len(msgs) {
		return nil, fmt.Errorf("unreachable: invalid number of messages (%d) or header offsets (%d)", len(msgs), len(hdrs))
	}
	responseCh := make(chan *mqttJSAResponse, len(msgs))

	// Generate and queue all outgoing requests, have all results reported to
	// responseCh, and store a map of reply subjects to the original subjects'
	// indices.
	r2i := map[string]int{}
	for i, msg := range msgs {
		hdr := hdrs[i]
		var sb strings.Builder
		// Either we use nuid.Next() which uses a global lock, or our own nuid object, but
		// then it needs to be "write" protected. This approach will reduce across account
		// contention since we won't use the global nuid's lock.
		jsa.mu.Lock()
		uid := jsa.nuid.Next()
		sb.WriteString(jsa.rplyr)
		jsa.mu.Unlock()

		sb.WriteString(kind)
		sb.WriteByte(btsep)
		if cidHash != _EMPTY_ {
			sb.WriteString(cidHash)
			sb.WriteByte(btsep)
		}
		sb.WriteString(uid)
		reply := sb.String()

		// Add responseCh to the reply channel map. It will be cleaned out on
		// timeout (see below), or in processJSAPIReplies upon receiving the
		// response.
		jsa.replies.Store(reply, responseCh)

		subject = jsa.prefixDomain(subject)
		jsa.sendq.push(&mqttJSPubMsg{
			subj:  subject,
			reply: reply,
			hdr:   hdr,
			msg:   msg,
		})
		r2i[reply] = i
	}

	// Wait for all responses to come back, or for the timeout to expire. We
	// don't want to use time.After() which causes memory growth because the
	// timer can't be stopped and will need to expire to then be garbage
	// collected.
	c := 0
	responses := make([]*mqttJSAResponse, len(msgs))
	start := time.Now()
	t := time.NewTimer(jsa.timeout)
	defer t.Stop()
	for {
		select {
		case r := <-responseCh:
			i := r2i[r.reply]
			responses[i] = r
			c++
			if c == len(msgs) {
				return responses, nil
			}

		case <-jsa.quitCh:
			return nil, ErrServerNotRunning

		case <-t.C:
			var reply string
			now := time.Now()
			for reply = range r2i { // preserve the last value for Errorf
				jsa.replies.Delete(reply)
			}

			if len(msgs) == 1 {
				return responses, fmt.Errorf("timeout after %v: request type %q on %q (reply=%q)", now.Sub(start), kind, subject, reply)
			} else {
				return responses, fmt.Errorf("timeout after %v: request type %q on %q: got %d out of %d", now.Sub(start), kind, subject, c, len(msgs))
			}
		}
	}
}

func (jsa *mqttJSA) sendAck(ackSubject string) {
	// Send to the ack subject with no payload.
	jsa.sendMsg(ackSubject, nil)
}

func (jsa *mqttJSA) sendMsg(subj string, msg []byte) {
	if subj == _EMPTY_ {
		return
	}
	// We pass -1 for the hdr so that the send loop does not need to
	// add the "client info" header. This is not a JS API request per se.
	jsa.sendq.push(&mqttJSPubMsg{subj: subj, msg: msg, hdr: -1})
}

func (jsa *mqttJSA) createEphemeralConsumer(cfg *CreateConsumerRequest) (*JSApiConsumerCreateResponse, error) {
	cfgb, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	subj := fmt.Sprintf(JSApiConsumerCreateT, cfg.Stream)
	ccri, err := jsa.newRequest(mqttJSAConsumerCreate, subj, 0, cfgb)
	if err != nil {
		return nil, err
	}
	ccr := ccri.(*JSApiConsumerCreateResponse)
	return ccr, ccr.ToError()
}

func (jsa *mqttJSA) createDurableConsumer(cfg *CreateConsumerRequest) (*JSApiConsumerCreateResponse, error) {
	cfgb, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	subj := fmt.Sprintf(JSApiDurableCreateT, cfg.Stream, cfg.Config.Durable)
	ccri, err := jsa.newRequest(mqttJSAConsumerCreate, subj, 0, cfgb)
	if err != nil {
		return nil, err
	}
	ccr := ccri.(*JSApiConsumerCreateResponse)
	return ccr, ccr.ToError()
}

// if noWait is specified, does not wait for the JS response, returns nil
func (jsa *mqttJSA) deleteConsumer(streamName, consName string, noWait bool) (*JSApiConsumerDeleteResponse, error) {
	subj := fmt.Sprintf(JSApiConsumerDeleteT, streamName, consName)
	if noWait {
		jsa.sendMsg(subj, nil)
		return nil, nil
	}

	cdri, err := jsa.newRequest(mqttJSAConsumerDel, subj, 0, nil)
	if err != nil {
		return nil, err
	}
	cdr := cdri.(*JSApiConsumerDeleteResponse)
	return cdr, cdr.ToError()
}

func (jsa *mqttJSA) createStream(cfg *StreamConfig) (*StreamInfo, bool, error) {
	cfgb, err := json.Marshal(cfg)
	if err != nil {
		return nil, false, err
	}
	scri, err := jsa.newRequest(mqttJSAStreamCreate, fmt.Sprintf(JSApiStreamCreateT, cfg.Name), 0, cfgb)
	if err != nil {
		return nil, false, err
	}
	scr := scri.(*JSApiStreamCreateResponse)
	if err = scr.ToError(); err != nil {
		return nil, false, err
	}
	if scr.StreamInfo == nil {
		return nil, false, fmt.Errorf("invalid stream create response: missing stream info")
	}
	return scr.StreamInfo, scr.DidCreate, nil
}

func (jsa *mqttJSA) updateStream(cfg *StreamConfig) (*StreamInfo, error) {
	cfgb, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	scri, err := jsa.newRequest(mqttJSAStreamUpdate, fmt.Sprintf(JSApiStreamUpdateT, cfg.Name), 0, cfgb)
	if err != nil {
		return nil, err
	}
	scr := scri.(*JSApiStreamUpdateResponse)
	if err = scr.ToError(); err != nil {
		return nil, err
	}
	if scr.StreamInfo == nil {
		return nil, fmt.Errorf("invalid stream update response: missing stream info")
	}
	return scr.StreamInfo, nil
}

func (jsa *mqttJSA) lookupStream(name string) (*StreamInfo, error) {
	slri, err := jsa.newRequest(mqttJSAStreamLookup, fmt.Sprintf(JSApiStreamInfoT, name), 0, nil)
	if err != nil {
		return nil, err
	}
	slr := slri.(*JSApiStreamInfoResponse)
	if err = slr.ToError(); err != nil {
		return nil, err
	}
	if slr.StreamInfo == nil {
		return nil, NewJSStreamNotFoundError()
	}
	return slr.StreamInfo, nil
}

func (jsa *mqttJSA) deleteStream(name string) (bool, error) {
	sdri, err := jsa.newRequest(mqttJSAStreamDel, fmt.Sprintf(JSApiStreamDeleteT, name), 0, nil)
	if err != nil {
		return false, err
	}
	sdr := sdri.(*JSApiStreamDeleteResponse)
	return sdr.Success, sdr.ToError()
}

func (jsa *mqttJSA) loadLastMsgFor(streamName string, subject string) (*StoredMsg, error) {
	mreq := &JSApiMsgGetRequest{LastFor: subject}
	req, err := json.Marshal(mreq)
	if err != nil {
		return nil, err
	}
	lmri, err := jsa.newRequest(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), 0, req)
	if err != nil {
		return nil, err
	}
	lmr := lmri.(*JSApiMsgGetResponse)
	if err = lmr.ToError(); err != nil {
		return nil, err
	}
	if lmr.Message == nil {
		return nil, NewJSNoMessageFoundError()
	}
	return lmr.Message, nil
}

func (jsa *mqttJSA) loadLastMsgForMulti(streamName string, subjects []string) ([]*JSApiMsgGetResponse, error) {
	marshaled := make([][]byte, 0, len(subjects))
	headerBytes := make([]int, 0, len(subjects))
	for _, subject := range subjects {
		mreq := &JSApiMsgGetRequest{LastFor: subject}
		bb, err := json.Marshal(mreq)
		if err != nil {
			return nil, err
		}
		marshaled = append(marshaled, bb)
		headerBytes = append(headerBytes, 0)
	}

	all, err := jsa.newRequestExMulti(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), _EMPTY_, headerBytes, marshaled)
	// all has the same order as subjects, preserve it as we unmarshal
	responses := make([]*JSApiMsgGetResponse, len(all))
	for i, v := range all {
		if v != nil {
			responses[i] = v.value.(*JSApiMsgGetResponse)
		}
	}
	return responses, err
}

func (jsa *mqttJSA) loadNextMsgFor(streamName string, subject string) (*StoredMsg, error) {
	mreq := &JSApiMsgGetRequest{NextFor: subject}
	req, err := json.Marshal(mreq)
	if err != nil {
		return nil, err
	}
	lmri, err := jsa.newRequest(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), 0, req)
	if err != nil {
		return nil, err
	}
	lmr := lmri.(*JSApiMsgGetResponse)
	if err = lmr.ToError(); err != nil {
		return nil, err
	}
	if lmr.Message == nil {
		return nil, NewJSNoMessageFoundError()
	}
	return lmr.Message, nil
}

func (jsa *mqttJSA) loadMsg(streamName string, seq uint64) (*StoredMsg, error) {
	mreq := &JSApiMsgGetRequest{Seq: seq}
	req, err := json.Marshal(mreq)
	if err != nil {
		return nil, err
	}
	lmri, err := jsa.newRequest(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), 0, req)
	if err != nil {
		return nil, err
	}
	lmr := lmri.(*JSApiMsgGetResponse)
	if err := lmr.ToError(); err != nil {
		return nil, err
	}
	if lmr.Message == nil {
		return nil, NewJSNoMessageFoundError()
	}
	return lmr.Message, nil
}

func (jsa *mqttJSA) storeMsgNoWait(subject string, hdrLen int, msg []byte) {
	jsa.sendq.push(&mqttJSPubMsg{
		subj: subject,
		msg:  msg,
		hdr:  hdrLen,
	})
}

func (jsa *mqttJSA) storeMsg(subject string, headers int, msg []byte) (*JSPubAckResponse, error) {
	smri, err := jsa.newRequest(mqttJSAMsgStore, subject, headers, msg)
	if err != nil {
		return nil, err
	}
	smr := smri.(*JSPubAckResponse)
	return smr, smr.ToError()
}

// Reply subject for a pipelined JS API request; processJSAPIReplies
// dispatches on kind.
func (jsa *mqttJSA) newReplySubject(kind string) string {
	jsa.mu.Lock()
	rplyr, uid := jsa.rplyr, jsa.nuid.Next()
	jsa.mu.Unlock()
	return rplyr + kind + tsep + uid
}

// Like storeMsg, without waiting; the caller registers reply in jsa.replies.
func (jsa *mqttJSA) storeMsgAsync(subject string, hdr int, msg []byte, msgId, reply string) {
	jsa.sendq.push(&mqttJSPubMsg{
		subj:  subject,
		reply: reply,
		hdr:   hdr,
		msg:   msg,
		msgId: msgId,
	})
}

func (jsa *mqttJSA) storeSessionMsg(domainTk, cidHash string, hdr int, msg []byte) (*JSPubAckResponse, error) {
	// Compute subject where the session is being stored
	subject := mqttSessStreamSubjectPrefix + domainTk + cidHash

	// Passing cidHash will add it to the JS reply subject, so that we can use
	// it in processSessionPersist.
	smri, err := jsa.newRequestEx(mqttJSASessPersist, subject, cidHash, hdr, msg)
	if err != nil {
		return nil, err
	}
	smr := smri.(*JSPubAckResponse)
	return smr, smr.ToError()
}

func (jsa *mqttJSA) loadSessionMsg(domainTk, cidHash string) (*StoredMsg, error) {
	subject := mqttSessStreamSubjectPrefix + domainTk + cidHash
	return jsa.loadLastMsgFor(mqttSessStreamName, subject)
}

func (jsa *mqttJSA) deleteSubject(stream string) string {
	if subj, ok := jsa.deleteSubjs[stream]; ok {
		return subj
	}
	return jsa.prefixDomain(fmt.Sprintf(JSApiMsgDeleteT, stream))
}

// JSApiMsgDeleteRequest{Seq: seq, NoErase: true} without the encoder.
func mqttDeleteMsgRequest(seq uint64) []byte {
	req := make([]byte, 0, 44) // 7 + 20 digits at most + 17
	req = append(req, `{"seq":`...)
	req = strconv.AppendUint(req, seq, 10)
	return append(req, `,"no_erase":true}`...)
}

// Deletes seq from stream and waits for the reply.
func (jsa *mqttJSA) deleteMsg(stream string, seq uint64) error {
	dmi, err := jsa.newRequest(mqttJSAMsgDelete, jsa.deleteSubject(stream), 0, mqttDeleteMsgRequest(seq))
	if err != nil {
		return err
	}
	dm := dmi.(*JSApiMsgDeleteResponse)
	return dm.ToError()
}

// Like deleteMsg without waiting: fire and forget with an empty reply,
// otherwise the caller registers reply in jsa.replies.
func (jsa *mqttJSA) deleteMsgAsync(stream string, seq uint64, reply string) {
	jsa.sendq.push(&mqttJSPubMsg{
		subj:  jsa.deleteSubject(stream),
		reply: reply,
		msg:   mqttDeleteMsgRequest(seq),
	})
}

//////////////////////////////////////////////////////////////////////////////
//
// Account Sessions Manager related functions
//
//////////////////////////////////////////////////////////////////////////////

// Returns true if `err` is not nil and does not match the api error with ErrorIdentifier id
func isErrorOtherThan(err error, id ErrorIdentifier) bool {
	return err != nil && !IsNatsErr(err, id)
}

// A delete of a message already gone comes back two ways: as
// JSSequenceNotFoundErrF when the store reports nothing removed, and as
// JSStreamMsgDeleteFailedF with the store's error only in the description
// when the store fails the lookup, hence the text match.
func isMsgAlreadyDeletedErr(err error) bool {
	return IsNatsErr(err, JSSequenceNotFoundErrF) ||
		(IsNatsErr(err, JSStreamMsgDeleteFailedF) &&
			strings.Contains(err.Error(), ErrStoreMsgNotFound.Error()))
}

// Process JS API replies.
//
// Can run from various go routines (consumer's loop, system send loop, etc..).
func (as *mqttAccountSessionManager) processJSAPIReplies(_ *subscription, pc *client, _ *Account, subject, _ string, msg []byte) {
	token := tokenAt(subject, mqttJSATokenPos)
	if token == _EMPTY_ {
		return
	}
	jsa := &as.jsa
	// Claim atomically: dispatch at most once, even on duplicate replies.
	chi, ok := jsa.replies.LoadAndDelete(subject)
	if !ok {
		return
	}
	out := func(value any) {
		switch to := chi.(type) {
		case chan *mqttJSAResponse:
			to <- &mqttJSAResponse{reply: subject, value: value}
		case *mqttPipelined:
			to.complete(pc, value)
		default:
			// Leave a clue; the waiter will hang until its timeout.
			pc.Warnf("Unexpected type %T registered for JS API reply %q", chi, subject)
		}
	}
	switch token {
	case mqttJSAStreamCreate:
		var resp = &JSApiStreamCreateResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAStreamUpdate:
		var resp = &JSApiStreamUpdateResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAStreamLookup:
		var resp = &JSApiStreamInfoResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAStreamDel:
		var resp = &JSApiStreamDeleteResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAConsumerCreate:
		var resp = &JSApiConsumerCreateResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAConsumerDel:
		var resp = &JSApiConsumerDeleteResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAMsgStore, mqttJSASessPersist:
		var resp = &JSPubAckResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAMsgLoad:
		var resp = &JSApiMsgGetResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAStreamNames:
		var resp = &JSApiStreamNamesResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAMsgDelete:
		var resp = &JSApiMsgDeleteResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	default:
		pc.Warnf("Unknown reply code %q", token)
	}
}

// This will both load all retained messages and process updates from the cluster.
//
// Run from various go routines (JS consumer, etc..).
// No lock held on entry.
func (as *mqttAccountSessionManager) processRetainedMsg(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	h, m := c.msgParts(rmsg)
	// We need to strip the trailing "\r\n".
	if l := len(m); l >= LEN_CR_LF {
		m = m[:l-LEN_CR_LF]
	}
	rm, err := mqttDecodeRetainedMessage(subject, h, m)
	if err != nil {
		return
	}
	if strings.IndexByte(rm.Subject, 0x7f) >= 0 {
		c.Warnf("Skipping retained message for subject %q: unsupported character 0x7f", rm.Subject)
		return
	}
	// The as.jsa.id is immutable, so no need to have a rlock here.
	local := rm.Origin == as.jsa.id
	// Get the stream sequence for this message.
	seq, _, _, _, _ := ackReplyInfo(reply)
	if len(m) == 0 {
		// An empty payload means that we need to remove the retained message.
		rmSeq := as.removeRetainedMsg(rm.Subject, 0)
		if local {
			if rmSeq > 0 {
				// This is for backward compatibility reasons.
				// Should be removed in a future release.
				as.notifyRetainedMsgDeleted(rm.Subject, rmSeq)
			}
			// Delete this very message we just processed, we don't need it anymore.
			as.deleteRetainedMsg(seq)
		}
	} else {
		// Add this retained message. The `rm.Msg` references some buffer that we
		// don't own. But addRetainedMsg() will take care of making a copy of
		// `rm.Msg` it `rm` ends-up being stored in the cache.
		as.addRetainedMsg(rm.Subject, seq, rm)
	}
}

// NOTE: This is maintained for backward compatibility reasons. Should be removed in 2.14/2.15?
func (as *mqttAccountSessionManager) processRetainedMsgDel(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	idHash := tokenAt(subject, 3)
	if idHash == _EMPTY_ || idHash == as.jsa.id {
		return
	}
	_, msg := c.msgParts(rmsg)
	if len(msg) < LEN_CR_LF {
		return
	}
	var drm mqttRetMsgDel
	if err := json.Unmarshal(msg, &drm); err != nil {
		return
	}
	as.removeRetainedMsg(drm.Subject, drm.Seq)
}

// This will receive all JS API replies for a request to store a session record,
// including the reply for our own server, which we will ignore.
// This allows us to detect that some application somewhere else in the cluster
// is connecting with the same client ID, and therefore we need to close the
// connection that is currently using this client ID.
//
// Can run from various go routines (system send loop, etc..).
// No lock held on entry.
func (as *mqttAccountSessionManager) processSessionPersist(_ *subscription, pc *client, _ *Account, subject, _ string, rmsg []byte) {
	// Ignore our own responses here (they are handled elsewhere)
	if tokenAt(subject, mqttJSAIdTokenPos) == as.jsa.id {
		return
	}
	cIDHash := tokenAt(subject, mqttJSAClientIDPos)
	_, msg := pc.msgParts(rmsg)
	if len(msg) < LEN_CR_LF {
		return
	}
	var par = &JSPubAckResponse{}
	if err := json.Unmarshal(msg, par); err != nil {
		return
	}
	if err := par.Error; err != nil {
		return
	}
	if par.PubAck == nil {
		return
	}
	as.mu.RLock()
	// Note that as.domainTk includes a terminal '.', so strip to compare to PubAck.Domain.
	dl := len(as.domainTk)
	if dl > 0 {
		dl--
	}
	ignore := par.Domain != as.domainTk[:dl]
	as.mu.RUnlock()
	if ignore {
		return
	}

	as.mu.Lock()
	defer as.mu.Unlock()
	sess, ok := as.sessByHash[cIDHash]
	if !ok {
		return
	}
	// If our current session's stream sequence is higher, it means that this
	// update is stale, so we don't do anything here.
	sess.mu.Lock()
	ignore = par.Sequence < sess.seq
	sess.mu.Unlock()
	if ignore {
		return
	}
	as.removeSession(sess, false)
	sess.mu.Lock()
	if ec := sess.c; ec != nil {
		as.addSessToFlappers(sess.id)
		ec.Warnf("Closing because a remote connection has started with the same client ID: %q", sess.id)
		// Disassociate the client from the session so that on client close,
		// nothing will be done with regards to cleaning up the session,
		// such as deleting stream, etc..
		sess.c = nil
		// Remove in separate go routine.
		go ec.closeConnection(DuplicateClientID)
	}
	sess.mu.Unlock()
}

// Adds this client ID to the flappers map, and if needed start the timer
// for map cleanup.
//
// Lock held on entry.
func (as *mqttAccountSessionManager) addSessToFlappers(clientID string) {
	as.flappers[clientID] = time.Now()
	if as.flapTimer == nil {
		as.flapTimer = time.AfterFunc(mqttFlapCleanItvl, func() {
			as.mu.Lock()
			defer as.mu.Unlock()
			// In case of shutdown, this will be nil
			if as.flapTimer == nil {
				return
			}
			now := time.Now()
			for cID, tm := range as.flappers {
				if now.Sub(tm) > mqttSessJailDur {
					delete(as.flappers, cID)
				}
			}
			as.flapTimer.Reset(mqttFlapCleanItvl)
		})
	}
}

// Remove this client ID from the flappers map.
//
// Lock held on entry.
func (as *mqttAccountSessionManager) removeSessFromFlappers(clientID string) {
	delete(as.flappers, clientID)
	// Do not stop/set timer to nil here. Better leave the timer run at its
	// regular interval and detect that there is nothing to do. The timer
	// will be stopped on shutdown.
}

// Helper to create a subscription. It updates the sid and array of subscriptions.
func (as *mqttAccountSessionManager) createSubscription(subject string, cb msgHandler, sid *int64, subs *[]*subscription) error {
	sub, err := as.jsa.c.processSub([]byte(subject), nil, []byte(strconv.FormatInt(*sid, 10)), cb, false)
	if err != nil {
		return err
	}
	*sid++
	*subs = append(*subs, sub)
	return nil
}

// A timer loop to cleanup up expired cached retained messages for a given MQTT account.
// The closeCh is used by the caller to be able to interrupt this routine
// if the rest of the initialization fails, since the quitCh is really
// only used when the server shutdown.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) cleanupRetainedMessageCache(s *Server, closeCh chan struct{}) {
	tt := time.NewTicker(mqttRetainedCacheTTL)
	defer tt.Stop()
	for {
		select {
		case <-tt.C:
			// Set a limit to the number of retained messages to scan since we
			// lock as for it. Since the map enumeration gives random order we
			// should eventually clean up everything.
			i, maxScan := 0, 10*1000
			now := time.Now()
			as.rmsCache.Range(func(key, value any) bool {
				rm := value.(*mqttRetainedMsg)
				if now.After(rm.expiresFromCache) {
					as.rmsCache.Delete(key)
				}
				i++
				return i < maxScan
			})

		case <-closeCh:
			return
		case <-s.quitCh:
			return
		}
	}
}

// Loop to send JS API requests for a given MQTT account.
// The closeCh is used by the caller to be able to interrupt this routine
// if the rest of the initialization fails, since the quitCh is really
// only used when the server shutdown.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) sendJSAPIrequests(s *Server, c *client, accName string, closeCh chan struct{}) {
	var cluster string
	if s.JetStreamEnabled() && !as.jsa.domainSet {
		// Only request the own cluster when it is clear that
		cluster = s.cachedClusterName()
	}
	as.mu.RLock()
	sendq := as.jsa.sendq
	quitCh := as.jsa.quitCh
	ci := ClientInfo{Account: accName, Cluster: cluster}
	acc := c.acc
	as.mu.RUnlock()

	// The account session manager does not have a suhtdown API per-se, instead,
	// we will cleanup things when this go routine exits after detecting that the
	// server is shutdown or the initialization of the account manager failed.
	defer func() {
		as.mu.Lock()
		if as.flapTimer != nil {
			as.flapTimer.Stop()
			as.flapTimer = nil
		}
		as.mu.Unlock()
	}()

	b, _ := json.Marshal(ci)
	hdrStart := bytes.Buffer{}
	hdrStart.WriteString(hdrLine)
	http.Header{ClientInfoHdr: []string{string(b)}}.Write(&hdrStart)
	hdrStart.WriteString(CR_LF)
	hdrStart.WriteString(CR_LF)
	hdrb := hdrStart.Bytes()

	for {
		select {
		case <-sendq.ch:
			pmis := sendq.pop()
			for _, r := range pmis {
				var nsize int

				msg := r.msg
				// If r.hdr is set to -1, it means that there is no need for any header.
				if r.hdr != -1 {
					bb := bytes.Buffer{}
					if r.hdr > 0 {
						// This means that the header has been set by the caller and is
						// already part of `msg`, so simply set c.pa.hdr to the given value.
						if r.msgId != _EMPTY_ {
							// Rebuild with the Nats-Msg-Id header.
							idLen := len(JSMsgId) + 1 + len(r.msgId) + len(_CRLF_)
							bb.Grow(len(msg) + idLen + len(_CRLF_))
							bb.Write(msg[:r.hdr-LEN_CR_LF])
							bb.WriteString(JSMsgId)
							bb.WriteByte(':')
							bb.WriteString(r.msgId)
							bb.WriteString(_CRLF_)
							bb.Write(msg[r.hdr-LEN_CR_LF:])
							msg = bb.Bytes()
							c.pa.hdr = r.hdr + idLen
						} else {
							c.pa.hdr = r.hdr
						}
						nsize = len(msg)
						msg = append(msg, _CRLF_...)
					} else {
						// We need the ClientInfo header, so add it here.
						bb.Grow(len(hdrb) + len(r.msg) + len(_CRLF_))
						bb.Write(hdrb)
						c.pa.hdr = bb.Len()
						bb.Write(r.msg)
						nsize = bb.Len()
						bb.WriteString(_CRLF_)
						msg = bb.Bytes()
					}
					c.pa.hdb = []byte(strconv.Itoa(c.pa.hdr))
				} else {
					c.pa.hdr = -1
					c.pa.hdb = nil
					nsize = len(msg)
					msg = append(msg, _CRLF_...)
				}

				c.pa.subject = []byte(r.subj)
				c.pa.reply = []byte(r.reply)
				c.pa.size = nsize
				c.pa.szb = []byte(strconv.Itoa(nsize))
				c.pa.mapped = nil

				if acc.hasMappings() {
					if changed := c.selectMappedSubject(); changed {
						c.traceOutOp("MAPPINGS", fmt.Appendf(nil, "%s -> %s", c.pa.mapped, c.pa.subject))
					}
				}
				c.processInboundClientMsg(msg)
				c.flushClients(0)
			}
			sendq.recycle(&pmis)

		case <-closeCh:
			return
		case <-quitCh:
			return
		}
	}
}

// Add/Replace this message from the retained messages map.
// If a message for this topic already existed, the existing record is updated
// with the provided information.
// Lock not held on entry.
func (as *mqttAccountSessionManager) addRetainedMsg(key string, sseq uint64, rm *mqttRetainedMsg) {
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.retmsgs == nil {
		as.retmsgs = stree.NewSubjectTree[mqttRetainedMsgRef]()
	} else {
		// Check if we already had one retained message. If so, update the existing one.
		if erf, exists := as.retmsgs.Find(stringToBytes(key)); exists {
			// Update the stream sequence with the new value.
			erf.sseq = sseq
			// Update the in-memory retained message cache but only for messages
			// that are already in the cache, i.e. have been (recently) used.
			// If that is the case, we ask setCachedRetainedMsg() to make a copy
			// of rm.Msg bytes slice.
			as.setCachedRetainedMsg(key, rm, true, true)
			return
		}
	}
	as.retmsgs.Insert([]byte(key), mqttRetainedMsgRef{sseq: sseq})
}

// Remove the retained message stored with the `subject` key from the map/cache.
// When invoked from the retained message stream's consumer, this function will
// be called with `seq == 0`, this is because add/remove are serialized in this
// stream and so the request is to remove the current retained message.
// But in some conditions, we will invoke this function from some other places
// with `seq > 0` which means that the retained message will be removed only if
// its sequence is the same than the provided one.
// This function returns the sequence associated with the existing retained
// message that is being removed (used with `seq == 0`) and returns 0 if the
// retained message was not removed from the map (not found or sequence did not
// match).
func (as *mqttAccountSessionManager) removeRetainedMsg(subject string, seq uint64) uint64 {
	as.mu.Lock()
	defer as.mu.Unlock()
	rm, ok := as.retmsgs.Find(stringToBytes(subject))
	if !ok || (seq > 0 && rm.sseq != seq) {
		return 0
	}
	rm, _ = as.retmsgs.Delete(stringToBytes(subject))
	as.rmsCache.Delete(subject)
	return rm.sseq
}

// First check if this session's client ID is already in the "locked" map,
// which if it is the case means that another client is now bound to this
// session and this should return an error.
// If not in the "locked" map, but the client is not bound with this session,
// then same error is returned.
// Finally, if all checks ok, then the session's ID is added to the "locked" map.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) lockSession(sess *mqttSession, c *client) error {
	as.mu.Lock()
	defer as.mu.Unlock()
	var fail bool
	if _, fail = as.sessLocked[sess.id]; !fail {
		sess.mu.Lock()
		fail = sess.c != c
		sess.mu.Unlock()
	}
	if fail {
		return fmt.Errorf("another session is in use with client ID %q", sess.id)
	}
	as.sessLocked[sess.id] = struct{}{}
	return nil
}

// Remove the session from the "locked" map.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) unlockSession(sess *mqttSession) {
	as.mu.Lock()
	delete(as.sessLocked, sess.id)
	as.mu.Unlock()
}

// Simply adds the session to the various sessions maps.
// The boolean `lock` indicates if this function should acquire the lock
// prior to adding to the maps.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) addSession(sess *mqttSession, lock bool) {
	if lock {
		as.mu.Lock()
	}
	as.sessions[sess.id] = sess
	as.sessByHash[sess.idHash] = sess
	if lock {
		as.mu.Unlock()
	}
}

// Simply removes the session from the various sessions maps.
// The boolean `lock` indicates if this function should acquire the lock
// prior to removing from the maps.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) removeSession(sess *mqttSession, lock bool) {
	if lock {
		as.mu.Lock()
	}
	delete(as.sessions, sess.id)
	delete(as.sessByHash, sess.idHash)
	if lock {
		as.mu.Unlock()
	}
}

// Helper to set the sub's mqtt fields and possibly serialize (pre-loaded)
// retained messages.
//
// Session lock held on entry. Acquires the subs lock and holds it for
// the duration. Non-MQTT messages coming into mqttDeliverMsgCbQoS0 will be
// waiting.
func (sess *mqttSession) processQOS12Sub(
	c *client, // subscribing client.
	subject, sid []byte, isReserved bool, qos byte, jsDurName string, h msgHandler, // subscription parameters.
) (*subscription, error) {
	return sess.processSub(c, subject, sid, isReserved, qos, jsDurName, h, false, nil, false, nil)
}

func (sess *mqttSession) processSub(
	c *client, // subscribing client.
	subject, sid []byte, isReserved bool, qos byte, jsDurName string, h msgHandler, // subscription parameters.
	initShadow bool, // do we need to scan for shadow subscriptions? (not for QOS1+)
	rms map[string]*mqttRetainedMsg, // preloaded rms (can be empty, or missing items if errors)
	trace bool, // trace serialized retained messages in the log?
	as *mqttAccountSessionManager, // needed only for rms serialization.
) (*subscription, error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		if elapsed > mqttProcessSubTooLong {
			c.Warnf("Took too long to process subscription for %q: %v", subject, elapsed)
		}
	}()

	// Hold subsMu to prevent QOS0 messages callback from doing anything until
	// the (MQTT) sub is initialized.
	sess.subsMu.Lock()
	defer sess.subsMu.Unlock()

	sub, err := c.processSub(subject, nil, sid, h, false)
	if err != nil {
		// c.processSub already called c.Errorf(), so no need here.
		return nil, err
	}
	subs := []*subscription{sub}
	if initShadow {
		subs = append(subs, sub.shadow...)
	}
	for _, ss := range subs {
		if ss.mqtt == nil {
			// reserved is set only once and once the subscription has been
			// created it can be considered immutable.
			ss.mqtt = &mqttSub{
				reserved: isReserved,
			}
		}
		// QOS and jsDurName can be changed on an existing subscription, so
		// accessing it later requires a lock.
		ss.mqtt.qos = qos
		ss.mqtt.jsDur = jsDurName
		// A (re)configured subscription is live; clear any prior teardown mark.
		ss.mqtt.closed = false
	}

	if len(rms) > 0 {
		// Only deal with retained messages for the normal subscription,
		// not the shadow one (which is for a different account and subject).
		as.serializeRetainedMsgsForSub(rms, sess, c, sub, trace)
	}

	return sub, nil
}

// Process subscriptions for the given session/client.
//
// When `fromSubProto` is false, it means that this is invoked from the CONNECT
// protocol, when restoring subscriptions that were saved for this session.
// In that case, there is no need to update the session record.
//
// When `fromSubProto` is true, it means that this call is invoked from the
// processing of the SUBSCRIBE protocol, which means that the session needs to
// be updated. It also means that if a subscription on same subject with same
// QoS already exist, we should not be recreating the subscription/JS durable,
// since it was already done when processing the CONNECT protocol.
//
// Runs from the client's readLoop.
// Lock not held on entry, but session is in the locked map.
func (as *mqttAccountSessionManager) processSubs(sess *mqttSession, c *client,
	filters []*mqttFilter, fromSubProto, trace bool) ([]*subscription, error) {

	// Helper to determine if we need to create a separate top-level
	// subscription for a wildcard.
	fwc := func(subject string) (bool, string, string) {
		if !mqttNeedSubForLevelUp(subject) {
			return false, _EMPTY_, _EMPTY_
		}
		// Say subject is "foo.>", remove the ".>" so that it becomes "foo"
		fwcsubject := subject[:len(subject)-2]
		// Change the sid to "foo fwc"
		fwcsid := fwcsubject + mqttMultiLevelSidSuffix

		return true, fwcsubject, fwcsid
	}

	rmSubjects := map[string]uint64{}
	// Preload retained messages for all requested subscriptions.  Also, since
	// it's the first iteration over the filter list, do some cleanup.
	for _, f := range filters {
		if f.qos > 2 {
			f.qos = 2
		}
		if c.mqtt.downgradeQoS2Sub && f.qos == 2 {
			c.Warnf("Downgrading subscription QoS2 to QoS1 for %q, as configured", f.filter)
			f.qos = 1
		}

		// Do not allow MQTT clients to subscribe directly to internal subjects.
		// Otherwise, subjects such as "$MQTT.msgs.*" could be used to bypass
		// MQTT subscription permissions.
		if strings.HasPrefix(f.filter, mqttPrefix) {
			f.qos = mqttSubAckFailure
			continue
		}

		if f.qos == 2 {
			if err := sess.ensurePubRelConsumerSubscription(c); err != nil {
				c.Errorf("failed to initialize PUBREL processing: %v", err)
				f.qos = mqttSubAckFailure
				continue
			}
		}

		// Find retained messages.
		if fromSubProto {
			as.addRetainedSubjectsForSubject(rmSubjects, f.filter)
			if need, subject, _ := fwc(f.filter); need {
				as.addRetainedSubjectsForSubject(rmSubjects, subject)
			}
		}
	}

	var rms map[string]*mqttRetainedMsg
	if len(rmSubjects) > 0 {
		// Make the best effort to load retained messages.
		rms = as.loadRetainedMessages(rmSubjects, c)
	}

	// Small helper to add the consumer config to the session.
	addJSConsToSess := func(sid string, cc *ConsumerConfig) {
		if cc == nil {
			return
		}
		if sess.cons == nil {
			sess.cons = make(map[string]*ConsumerConfig)
		}
		sess.cons[sid] = cc
	}

	var err error
	subs := make([]*subscription, 0, len(filters))
	for _, f := range filters {
		// Skip what's already been identified as a failure.
		if f.qos == mqttSubAckFailure {
			continue
		}
		subject := f.filter
		bsubject := []byte(subject)
		sid := subject
		bsid := bsubject
		isReserved := isMQTTReservedSubscription(subject)

		var jscons *ConsumerConfig
		var jssub *subscription

		// Note that if a subscription already exists on this subject, the
		// existing sub is returned. Need to update the qos.
		var sub *subscription
		var err error

		const processShadowSubs = true

		as.mu.Lock()
		sess.mu.Lock()
		sub, err = sess.processSub(c,
			bsubject, bsid, isReserved, f.qos, // main subject
			_EMPTY_, mqttDeliverMsgCbQoS0, // no jsDur for QOS0
			processShadowSubs,
			rms, trace, as)
		sess.mu.Unlock()
		as.mu.Unlock()

		if err != nil {
			f.qos = mqttSubAckFailure
			sess.cleanupFailedSub(c, sub, jscons, jssub)
			continue
		}

		// This will create (if not already exist) a JS consumer for
		// subscriptions of QoS >= 1. But if a JS consumer already exists and
		// the subscription for same subject is now a QoS==0, then the JS
		// consumer will be deleted.
		jscons, jssub, err = sess.processJSConsumer(c, subject, sid, f.qos, fromSubProto)
		if err != nil {
			f.qos = mqttSubAckFailure
			sess.cleanupFailedSub(c, sub, jscons, jssub)
			continue
		}

		// Process the wildcard subject if needed.
		if need, fwcsubject, fwcsid := fwc(subject); need {
			var fwjscons *ConsumerConfig
			var fwjssub *subscription
			var fwcsub *subscription

			// See note above about existing subscription.
			as.mu.Lock()
			sess.mu.Lock()
			fwcsub, err = sess.processSub(c,
				[]byte(fwcsubject), []byte(fwcsid), isReserved, f.qos, // FWC (top-level wildcard) subject
				_EMPTY_, mqttDeliverMsgCbQoS0, // no jsDur for QOS0
				processShadowSubs,
				rms, trace, as)
			sess.mu.Unlock()
			as.mu.Unlock()
			if err != nil {
				// c.processSub already called c.Errorf(), so no need here.
				f.qos = mqttSubAckFailure
				sess.cleanupFailedSub(c, sub, jscons, jssub)
				continue
			}

			fwjscons, fwjssub, err = sess.processJSConsumer(c, fwcsubject, fwcsid, f.qos, fromSubProto)
			if err != nil {
				// c.processSub already called c.Errorf(), so no need here.
				f.qos = mqttSubAckFailure
				sess.cleanupFailedSub(c, sub, jscons, jssub)
				sess.cleanupFailedSub(c, fwcsub, fwjscons, fwjssub)
				continue
			}

			subs = append(subs, fwcsub)
			addJSConsToSess(fwcsid, fwjscons)
		}

		subs = append(subs, sub)
		addJSConsToSess(sid, jscons)
	}

	if fromSubProto {
		err = sess.update(filters, true)
	}

	return subs, err
}

// Retained publish messages matching this subscription are serialized in the
// subscription's `prm` mqtt writer. This buffer will be queued for outbound
// after the subscription is processed and SUBACK is sent or possibly when
// server processes an incoming published message matching the newly
// registered subscription.
//
// Runs from the client's readLoop.
// Account session manager lock held on entry.
// Session lock held on entry.
func (as *mqttAccountSessionManager) serializeRetainedMsgsForSub(rms map[string]*mqttRetainedMsg, sess *mqttSession, c *client, sub *subscription, trace bool) {
	if as.retmsgs.Size() == 0 || len(rms) == 0 {
		return
	}
	toTrace := []mqttPublish{}
	as.retmsgs.Match(sub.subject, func(subj []byte, _ *mqttRetainedMsgRef) {
		rm := rms[string(subj)]
		if rm == nil {
			// This should not happen since we pre-load messages into rms before
			// calling serialize.
			return
		}
		// A broad wildcard subscription can overlap a subscribe deny clause.
		c.mu.Lock()
		denied := c.mperms != nil && c.checkDenySub(string(subj), bytesToString(sub.queue))
		c.mu.Unlock()
		if denied {
			return
		}
		var pi uint16
		qos := min(mqttGetQoS(rm.Flags), sub.mqtt.qos)
		if c.mqtt.rejectQoS2Pub && qos == 2 {
			c.Warnf("Rejecting retained message with QoS2 for subscription %q, as configured", sub.subject)
			return
		}
		if qos > 0 {
			pi = sess.trackPublishRetained(string(sub.sid), qos)

			// If we failed to get a PI for this message, send it as a QoS0, the
			// best we can do?
			if pi == 0 {
				qos = 0
			}
		}

		// Need to use the subject for the retained message, not the `sub` subject.
		// We can find the published retained message in rm.sub.subject.
		// Set the RETAIN flag: [MQTT-3.3.1-8].
		flags, headerBytes := mqttMakePublishHeader(pi, qos, false, true, []byte(rm.Topic), len(rm.Msg))
		c.mu.Lock()
		sub.mqtt.prm = append(sub.mqtt.prm, headerBytes, rm.Msg)
		c.mu.Unlock()
		if trace {
			toTrace = append(toTrace, mqttPublish{
				topic: []byte(rm.Topic),
				flags: flags,
				pi:    pi,
				sz:    len(rm.Msg),
			})
		}
	})
	for _, pp := range toTrace {
		c.traceOutOp("PUBLISH", []byte(mqttPubTrace(&pp)))
	}
}

// Appends the stored message subjects for all retained message records that
// match the given subscription's `subject` (which could have wildcards).
//
// Account session manager NOT lock held on entry.
func (as *mqttAccountSessionManager) addRetainedSubjectsForSubject(list map[string]uint64, topSubject string) {
	as.mu.RLock()
	defer as.mu.RUnlock()

	if as.retmsgs.Size() == 0 {
		return
	}

	as.retmsgs.Match(stringToBytes(topSubject), func(subj []byte, ret *mqttRetainedMsgRef) {
		subject := string(subj)
		if _, ok := list[subject]; ok {
			return
		}
		if seq := ret.sseq; seq > 0 {
			list[subject] = seq
		}
	})
}

type warner interface {
	Warnf(format string, v ...any)
}

// Loads a list of retained messages given a list of stored message subjects.
func (as *mqttAccountSessionManager) loadRetainedMessages(subjects map[string]uint64, w warner) map[string]*mqttRetainedMsg {
	rms := make(map[string]*mqttRetainedMsg, len(subjects))
	ss := []string{}
	for s := range subjects {
		if rm := as.getCachedRetainedMsg(s); rm != nil {
			rms[s] = rm
		} else {
			ss = append(ss, mqttRetainedMsgsStreamSubject+s)
		}
	}

	if len(ss) == 0 {
		return rms
	}

	// Although we have the stream sequence for a given subject, we still use
	// the load with "last for subject" because it will cover the cases where a
	// new retained message has arrived since we collected the subject/seq pair.
	// If we were doing a load "by seq" and the message is not found, we would
	// incorrectly remove the retained message from our map.
	results, err := as.jsa.loadLastMsgForMulti(mqttRetainedMsgsStreamName, ss)
	// If an error occurred, warn, but then proceed with what we got.
	if err != nil {
		w.Warnf("error loading retained messages: %v", err)
	}
	for i, result := range results {
		if result == nil {
			continue // skip requests that timed out
		}
		if err := result.ToError(); err != nil {
			// Skip the "$MQTT.rmsgs." prefix...
			subj := ss[i][len(mqttRetainedMsgsStreamSubject):]
			if IsNatsErr(err, JSNoMessageFoundErr) {
				// If there is no message for that subject, delete from our map.
				// The good thing here is that we handle the race where a retained
				// message may just arrive and be replacing it in the map. The
				// removeRetainedMsg() function below will not remove if the sequence
				// does not match.
				seq := subjects[subj]
				as.removeRetainedMsg(subj, seq)
			}
			w.Warnf("failed to load retained message for subject %q: %v", subj, err)
			continue
		}
		// Guard before dereferencing below.
		if result.Message == nil {
			continue
		}
		rm, err := mqttDecodeRetainedMessage(result.Message.Subject, result.Message.Header, result.Message.Data)
		if err != nil {
			// Unlikely that we can recover from that, so remove the message.
			// (see comment above if failing to load the message).
			subj := ss[i][len(mqttRetainedMsgsStreamSubject):]
			seq := subjects[subj]
			as.removeRetainedMsg(subj, seq)
			w.Warnf("failed to decode retained message for subject %q: %v", subj, err)
			continue
		}

		// Add the loaded retained message to the cache, and to the results map.
		// We don't need setCachedRetainedMsg() to clone the `rm.Msg` bytes slice
		// since we own it.
		as.setCachedRetainedMsg(rm.Subject, rm, false, false)
		rms[rm.Subject] = rm
	}
	return rms
}

// Composes a NATS message for a storeable mqttRetainedMsg.
// If the body is empty, the flags are encoded in a way that will cause older
// servers to fail to decode the message in processRetainedMsg callback and
// will simply ignore it, which is what we want.
func mqttEncodeRetainedMessage(rm *mqttRetainedMsg) (natsMsg []byte, headerLen int) {
	delRM := len(rm.Msg) == 0

	// No need to encode the subject, we can restore it from topic.
	l := len(hdrLine)
	l += len(mqttNatsRetainedMessageTopic) + 1 + len(rm.Topic) + 2 // 1 byte for ':', 2 bytes for CRLF
	if rm.Origin != _EMPTY_ {
		l += len(mqttNatsRetainedMessageOrigin) + 1 + len(rm.Origin) + 2 // 1 byte for ':', 2 bytes for CRLF
	}
	if rm.Source != _EMPTY_ {
		l += len(mqttNatsRetainedMessageSource) + 1 + len(rm.Source) + 2 // 1 byte for ':', 2 bytes for CRLF
	}
	l += len(mqttNatsRetainedMessageFlags) + 1 + 2 + 2 // 1 byte for ':', 2 bytes for the flags, 2 bytes for CRLF
	l += 2                                             // 2 bytes for the extra CRLF after the header
	if delRM {
		l++ // Will add the delete marker before the flag
	} else {
		l += len(rm.Msg)
	}

	// +LEN_CR_LF: sendJSAPIrequests appends the CRLF in place.
	buf := bytes.NewBuffer(make([]byte, 0, l+LEN_CR_LF))

	buf.WriteString(hdrLine)

	buf.WriteString(mqttNatsRetainedMessageTopic)
	buf.WriteByte(':')
	buf.WriteString(rm.Topic)
	buf.WriteString(_CRLF_)

	buf.WriteString(mqttNatsRetainedMessageFlags)
	buf.WriteByte(':')
	if delRM {
		buf.WriteByte(mqttRetainedFlagDelMarker)
	}
	buf.WriteString(strconv.FormatUint(uint64(rm.Flags), 16))
	buf.WriteString(_CRLF_)

	if rm.Origin != _EMPTY_ {
		buf.WriteString(mqttNatsRetainedMessageOrigin)
		buf.WriteByte(':')
		buf.WriteString(rm.Origin)
		buf.WriteString(_CRLF_)
	}
	if rm.Source != _EMPTY_ {
		buf.WriteString(mqttNatsRetainedMessageSource)
		buf.WriteByte(':')
		buf.WriteString(rm.Source)
		buf.WriteString(_CRLF_)
	}

	// End of header, finalize
	buf.WriteString(_CRLF_)
	headerLen = buf.Len()
	buf.Write(rm.Msg)
	return buf.Bytes(), headerLen
}

func mqttSliceHeaders(headers map[string][]byte, hdr []byte) {
	// Skip the hdrLine
	if !bytes.HasPrefix(hdr, stringToBytes(hdrLine)) {
		return
	}
	crLFAsBytes := stringToBytes(CR_LF)
	for i := len(hdrLine); i < len(hdr); {
		// Search for key/val delimiter.
		del := bytes.IndexByte(hdr[i:], ':')
		// Not found or key is length 0, we stop.
		if del < 0 || del == i {
			break
		}
		keyStart := i
		// Walk back to remove spaces between the key and ':' if applicable.
		index := keyStart + del - 1
		for index > keyStart && hdr[index] == ' ' {
			index--
		}
		key := hdr[keyStart : index+1]
		// If what we had is only spaces, we stop.
		if len(key) == 0 {
			break
		}
		i += del + 1
		valStart := i
		// Search for `\r\n`.
		nl := bytes.Index(hdr[valStart:], crLFAsBytes)
		// If we don't find, we stop.
		if nl < 0 {
			break
		}
		// Look if the caller is interested in this key.
		if _, ok := headers[bytesToString(key)]; ok {
			index := valStart
			// Remove possible spaces between the ':' and the value.
			for index < valStart+nl && hdr[index] == ' ' {
				index++
			}
			// Create a slice and limit capacity to the value range.
			val := hdr[index : valStart+nl : valStart+nl]
			// Record in the caller's map the value for this key.
			headers[bytesToString(key)] = val
		}
		// Reposition to past the `\r\n`.
		i += nl + 2
	}
}

// Decodes a retained message based on the content of the header `h`.
// The returned `*mqttRetainedMsg` object will hold a reference to `m`.
// If the buffer `m` is not owned by the caller, it is the caller
// responsibility to make a copy of the byte slice.
func mqttDecodeRetainedMessage(subject string, h, m []byte) (*mqttRetainedMsg, error) {
	headers := map[string][]byte{
		mqttNatsRetainedMessageOrigin: nil,
		mqttNatsRetainedMessageFlags:  nil,
		mqttNatsRetainedMessageSource: nil,
	}
	var rm *mqttRetainedMsg
	// Retrieve the values for the above headers.
	mqttSliceHeaders(headers, h)
	// Get the flag header.
	fHeader := headers[mqttNatsRetainedMessageFlags]
	// If we don't, it could be that this is an old retained message that
	// was JSON encoded.
	if len(fHeader) > 0 {
		if len(fHeader) > 1 && fHeader[0] == mqttRetainedFlagDelMarker {
			fHeader = fHeader[1:]
		}
		flagsUint, err := strconv.ParseUint(bytesToString(fHeader), 16, 8)
		if err != nil {
			// Since the error is currently not reported in the server, we
			// will simply replace with this one.
			return nil, errMQTTInvalidRetainFlags
		}
		rm = &mqttRetainedMsg{
			Flags:  byte(flagsUint),
			Origin: string(headers[mqttNatsRetainedMessageOrigin]),
			Source: string(headers[mqttNatsRetainedMessageSource]),
			Msg:    m,
		}
	} else {
		if err := json.Unmarshal(m, &rm); err != nil {
			return nil, err
		}
		if rm == nil {
			return nil, errMQTTInvalidRetainedMessage
		}
	}
	// Now check that the values are correct.
	//
	// For "Flags", anything at or above binary (1111) is too big.
	if rm.Flags >= mqttPacketFlagMask {
		return nil, errMQTTInvalidRetainFlags
	}
	if qos := mqttGetQoS(rm.Flags); qos > 2 {
		return nil, errMQTTInvalidRetainFlags
	}
	// We store `Topic` in the retained message because we used to store
	// all retained messages under the same subject `$MQTT_rmsgs` in
	// the retained messages stream. That is no longer the case, and to
	// cover setups where the retained message stream is sourced from another
	// account and has some subject transforms, simply reconstruct the
	// topic/subject based on the `subject` passed to this function.
	rm.Subject = strings.TrimPrefix(subject, mqttRetainedMsgsStreamSubject)
	rm.Topic = bytesToString(natsSubjectStrToMQTTTopic(rm.Subject))
	return rm, nil
}

// Creates the session stream (limit msgs of 1) for this client ID if it does
// not already exist. If it exists, recover the single record to rebuild the
// state of the session. If there is a session record but this session is not
// registered in the runtime of this server, then a request is made to the
// owner to close the client associated with this session since specification
// [MQTT-3.1.4-2] specifies that if the ClientId represents a Client already
// connected to the Server then the Server MUST disconnect the existing client.
//
// Runs from the client's readLoop.
// Lock not held on entry, but session is in the locked map.
func (as *mqttAccountSessionManager) createOrRestoreSession(clientID string, opts *Options) (*mqttSession, bool, error) {
	jsa := &as.jsa

	hash := getHash(clientID)
	smsg, err := jsa.loadSessionMsg(as.domainTk, hash)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, false, fmt.Errorf("loading session record: %w", err)
		}
		// Message not found, so reate the session...
		// Create a session and indicate that this session did not exist.
		sess := mqttSessionCreate(jsa, clientID, hash, 0, opts)
		sess.domainTk = as.domainTk
		return sess, false, nil
	}
	// We need to recover the existing record now.
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		return nil, false, fmt.Errorf("unmarshal of session record at sequence %v: %w", smsg.Sequence, err)
	}
	if ps.ID != clientID {
		return nil, false, errMQTTSessionCollision
	}
	for sid, cc := range ps.Cons {
		if cc == nil {
			delete(ps.Cons, sid)
		}
	}

	// Restore this session (even if we don't own it), the caller will do the right thing.
	sess := mqttSessionCreate(jsa, clientID, hash, smsg.Sequence, opts)
	sess.domainTk = as.domainTk
	sess.clean = ps.Clean
	sess.subs = ps.Subs
	sess.cons = ps.Cons
	sess.pubRelConsumer = ps.PubRel
	as.addSession(sess, true)
	return sess, true, nil
}

// Sends a request to delete a message, but does not wait for the response.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) deleteRetainedMsg(seq uint64) {
	as.jsa.deleteMsgAsync(mqttRetainedMsgsStreamName, seq, _EMPTY_)
}

// Sends a message indicating that a retained message on a given subject and stream sequence
// is being removed.
// NOTE: This is maintained for backward compatibility reasons. Should be removed in 2.14/2.15?
func (as *mqttAccountSessionManager) notifyRetainedMsgDeleted(subject string, seq uint64) {
	req := mqttRetMsgDel{
		Subject: subject,
		Seq:     seq,
	}
	b, _ := json.Marshal(&req)
	jsa := &as.jsa
	jsa.sendq.push(&mqttJSPubMsg{
		subj: jsa.rplyr + mqttJSARetainedMsgDel,
		msg:  b,
	})
}

func (as *mqttAccountSessionManager) transferUniqueSessStreamsToMuxed(log *Server) {
	// Set retry to true, will be set to false on success.
	retry := true
	defer func() {
		if retry {
			next := mqttDefaultTransferRetry
			log.Warnf("Failed to transfer all MQTT session streams, will try again in %v", next)
			time.AfterFunc(next, func() { as.transferUniqueSessStreamsToMuxed(log) })
		}
	}()

	jsa := &as.jsa
	sni, err := jsa.newRequestEx(mqttJSAStreamNames, JSApiStreams, _EMPTY_, 0, nil)
	if err != nil {
		log.Errorf("Unable to transfer MQTT session streams: %v", err)
		return
	}
	snames := sni.(*JSApiStreamNamesResponse)
	if snames.Error != nil {
		log.Errorf("Unable to transfer MQTT session streams: %v", snames.ToError())
		return
	}
	var oldMQTTSessStreams []string
	for _, sn := range snames.Streams {
		if strings.HasPrefix(sn, mqttSessionsStreamNamePrefix) {
			oldMQTTSessStreams = append(oldMQTTSessStreams, sn)
		}
	}
	ns := len(oldMQTTSessStreams)
	if ns == 0 {
		// Nothing to do
		retry = false
		return
	}
	log.Noticef("Transferring %v MQTT session streams...", ns)
	for _, sn := range oldMQTTSessStreams {
		log.Noticef("  Transferring stream %q to %q", sn, mqttSessStreamName)
		smsg, err := jsa.loadLastMsgFor(sn, sn)
		if err != nil {
			log.Errorf("   Unable to load session record: %v", err)
			return
		}
		ps := &mqttPersistedSession{}
		if err := json.Unmarshal(smsg.Data, ps); err != nil {
			log.Warnf("    Unable to unmarshal the content of this stream, may not be a legitimate MQTT session stream, skipping")
			continue
		}
		// Store record to MQTT session stream
		if _, err := jsa.storeSessionMsg(as.domainTk, getHash(ps.ID), 0, smsg.Data); err != nil {
			log.Errorf("    Unable to transfer the session record: %v", err)
			return
		}
		jsa.deleteStream(sn)
	}
	log.Noticef("Transfer of %v MQTT session streams done!", ns)
	retry = false
}

func (as *mqttAccountSessionManager) transferRetainedToPerKeySubjectStream(log *Server) error {
	jsa := &as.jsa
	var processed int
	var transferred int

	start := time.Now()
	deadline := start.Add(mqttRetainedTransferTimeout)
	for {
		// Try and look up messages on the original undivided "$MQTT.rmsgs" subject.
		// If nothing is returned here, we assume to have migrated all old messages.
		smsg, err := jsa.loadNextMsgFor(mqttRetainedMsgsStreamName, "$MQTT.rmsgs")
		if IsNatsErr(err, JSNoMessageFoundErr) {
			// We've ran out of messages to transfer, done.
			break
		}
		if err != nil {
			log.Warnf("    Unable to transfer a retained message: failed to load from '$MQTT.rmsgs': %s", err)
			return err
		}

		// Unmarshal the message so that we can obtain the subject name. Do not
		// use mqttDecodeRetainedMessage() here because these messages are from
		// older versions, and contain the full JSON encoding in payload.
		var rmsg mqttRetainedMsg
		if err = json.Unmarshal(smsg.Data, &rmsg); err == nil {
			// Store the message again, this time with the new per-key subject.
			subject := mqttRetainedMsgsStreamSubject + rmsg.Subject
			if _, err = jsa.storeMsg(subject, 0, smsg.Data); err != nil {
				log.Errorf("    Unable to transfer the retained message with sequence %d: %v", smsg.Sequence, err)
			}
			transferred++
		} else {
			log.Warnf("    Unable to unmarshal retained message with sequence %d, skipping", smsg.Sequence)
		}

		// Delete the original message.
		if err := jsa.deleteMsg(mqttRetainedMsgsStreamName, smsg.Sequence); err != nil {
			log.Errorf("    Unable to clean up the retained message with sequence %d: %v", smsg.Sequence, err)
			return err
		}
		processed++

		now := time.Now()
		if now.After(deadline) {
			err := fmt.Errorf("timed out while transferring retained messages from '$MQTT.rmsgs' after %v, %d processed, %d successfully transferred", now.Sub(start), processed, transferred)
			log.Noticef(err.Error())
			return err
		}
	}
	if processed > 0 {
		log.Noticef("Processed %d messages from '$MQTT.rmsgs', successfully transferred %d in %v", processed, transferred, time.Since(start))
	} else {
		log.Debugf("No messages found to transfer from '$MQTT.rmsgs'")
	}
	return nil
}

func (as *mqttAccountSessionManager) getCachedRetainedMsg(subject string) *mqttRetainedMsg {
	v, ok := as.rmsCache.Load(subject)
	if !ok {
		return nil
	}
	rm := v.(*mqttRetainedMsg)
	if rm.expiresFromCache.Before(time.Now()) {
		as.rmsCache.Delete(subject)
		return nil
	}
	return rm
}

// If cache is enabled, the expiration for the `rm` is bumped by
// `mqttRetainedCacheTTL` seconds.
// If `onlyReplace` is true, then the `rm` object is stored in the cache using
// the `subject` key only if there was already an object stored under that key.
// If `copyMsgBytes` is true, then the `rm.Msg` bytes are copied (because it
// references some buffer that is not owned by the caller).
//
// Note: currently `onlyReplace` and `cloneMsgBytes` always have the same
// value (all `true` or all `false`) however we use different booleans to
// better express the intent.
func (as *mqttAccountSessionManager) setCachedRetainedMsg(subject string, rm *mqttRetainedMsg, onlyReplace, copyMsgBytes bool) {
	if rm == nil {
		return
	}
	rm.expiresFromCache = time.Now().Add(mqttRetainedCacheTTL)
	if onlyReplace {
		if _, ok := as.rmsCache.Load(subject); !ok {
			return
		}
	}
	if copyMsgBytes {
		rm.Msg = copyBytes(rm.Msg)
	}
	as.rmsCache.Store(subject, rm)
}

//////////////////////////////////////////////////////////////////////////////
//
// MQTT session related functions
//
//////////////////////////////////////////////////////////////////////////////

// Returns a new mqttSession object with max ack pending set based on
// option or use mqttDefaultMaxAckPending if no option set.
func mqttSessionCreate(jsa *mqttJSA, id, idHash string, seq uint64, opts *Options) *mqttSession {
	maxp := opts.MQTT.MaxAckPending
	if maxp == 0 {
		maxp = mqttDefaultMaxAckPending
	}

	return &mqttSession{
		jsa:                    jsa,
		id:                     id,
		idHash:                 idHash,
		seq:                    seq,
		maxp:                   maxp,
		pubRelSubject:          mqttPubRelSubjectPrefix + idHash,
		pubRelDeliverySubject:  mqttPubRelDeliverySubjectPrefix + idHash,
		pubRelDeliverySubjectB: []byte(mqttPubRelDeliverySubjectPrefix + idHash),
	}
}

// Persists a session. Note that if the session's current client does not match
// the given client, nothing is done.
//
// Lock not held on entry.
func (sess *mqttSession) save() error {
	sess.mu.Lock()
	ps := mqttPersistedSession{
		Origin: sess.jsa.id,
		ID:     sess.id,
		Clean:  sess.clean,
		Subs:   sess.subs,
		Cons:   sess.cons,
		PubRel: sess.pubRelConsumer,
	}
	b, _ := json.Marshal(&ps)

	domainTk, cidHash := sess.domainTk, sess.idHash
	seq := sess.seq
	sess.mu.Unlock()

	var hdr int
	if seq != 0 {
		bb := bytes.Buffer{}
		bb.WriteString(hdrLine)
		bb.WriteString(JSExpectedLastSubjSeq)
		bb.WriteString(":")
		bb.WriteString(strconv.FormatInt(int64(seq), 10))
		bb.WriteString(CR_LF)
		bb.WriteString(CR_LF)
		hdr = bb.Len()
		bb.Write(b)
		b = bb.Bytes()
	}

	resp, err := sess.jsa.storeSessionMsg(domainTk, cidHash, hdr, b)
	if err != nil {
		return fmt.Errorf("unable to persist session %q (seq=%v): %v", ps.ID, seq, err)
	}
	// Guard before dereferencing below.
	if resp == nil || resp.PubAck == nil {
		return fmt.Errorf("unable to persist session %q (seq=%v): invalid pub ack response", ps.ID, seq)
	}
	sess.mu.Lock()
	sess.seq = resp.Sequence
	sess.mu.Unlock()
	return nil
}

// Clear the session.
//
// Runs from the client's readLoop.
// Lock not held on entry, but session is in the locked map.
func (sess *mqttSession) clear(noWait bool) error {
	var durs []string
	var pubRelDur string

	sess.mu.Lock()
	id := sess.id
	seq := sess.seq
	if l := len(sess.cons); l > 0 {
		durs = make([]string, 0, l)
	}
	for sid, cc := range sess.cons {
		delete(sess.cons, sid)
		durs = append(durs, cc.Durable)
	}
	if sess.pubRelConsumer != nil {
		pubRelDur = sess.pubRelConsumer.Durable
	}

	sess.subs = nil
	sess.pendingPublish = nil
	sess.pendingPubRel = nil
	sess.cpending = nil
	sess.pubRelConsumer = nil
	sess.seq = 0
	sess.tmaxack = 0
	// Discarded session: reset the PI counter too so a reused session object
	// does not inherit the previous session's identifiers. Spec [MQTT-3.1.2-6].
	sess.last_pi = 0
	sess.mu.Unlock()

	for _, dur := range durs {
		if _, err := sess.jsa.deleteConsumer(mqttStreamName, dur, noWait); isErrorOtherThan(err, JSConsumerNotFoundErr) {
			return fmt.Errorf("unable to delete consumer %q for session %q: %v", dur, sess.id, err)
		}
	}
	if pubRelDur != _EMPTY_ {
		_, err := sess.jsa.deleteConsumer(mqttOutStreamName, pubRelDur, noWait)
		if isErrorOtherThan(err, JSConsumerNotFoundErr) {
			return fmt.Errorf("unable to delete consumer %q for session %q: %v", pubRelDur, sess.id, err)
		}
	}

	if seq > 0 {
		if noWait {
			sess.jsa.deleteMsgAsync(mqttSessStreamName, seq, _EMPTY_)
			return nil
		}
		err := sess.jsa.deleteMsg(mqttSessStreamName, seq)
		// Ignore the various errors indicating that the message (or sequence)
		// is already deleted, can happen in a cluster.
		if err != nil && !isMsgAlreadyDeletedErr(err) {
			return fmt.Errorf("unable to delete session %q record at sequence %v: %v", id, seq, err)
		}
	}
	return nil
}

// This will update the session record for this client in the account's MQTT
// sessions stream if the session had any change in the subscriptions.
//
// Runs from the client's readLoop.
// Lock not held on entry, but session is in the locked map.
func (sess *mqttSession) update(filters []*mqttFilter, add bool) error {
	// Evaluate if we need to persist anything.
	var needUpdate bool
	for _, f := range filters {
		if add {
			if f.qos == mqttSubAckFailure {
				continue
			}
			if qos, ok := sess.subs[f.filter]; !ok || qos != f.qos {
				if sess.subs == nil {
					sess.subs = make(map[string]byte)
				}
				sess.subs[f.filter] = f.qos
				needUpdate = true
			}
		} else {
			if _, ok := sess.subs[f.filter]; ok {
				delete(sess.subs, f.filter)
				needUpdate = true
			}
		}
	}
	var err error
	if needUpdate {
		err = sess.save()
	}
	return err
}

func (sess *mqttSession) bumpPI() uint16 {
	var avail bool
	next := sess.last_pi
	for i := 0; i < 0xFFFF; i++ {
		next++
		if next == 0 {
			next = 1
		}

		_, usedInPublish := sess.pendingPublish[next]
		_, usedInPubRel := sess.pendingPubRel[next]
		if !usedInPublish && !usedInPubRel {
			sess.last_pi = next
			avail = true
			break
		}
	}
	if !avail {
		return 0
	}
	return sess.last_pi
}

// mqttRetainedPendingDur returns the pseudo consumer-durable key under which a
// subscription's in-flight retained QoS deliveries are tracked in cpending.
// Retained deliveries have no JS consumer, but keying them per subscription
// lets unsubscribe/downgrade teardown purge them like consumer deliveries.
// Cannot collide with real durables (idHash+"_"+nuid, $MQTT_PUBREL_ prefix).
func mqttRetainedPendingDur(sid string) string {
	return mqttRetainedMsgsStreamName + "/" + sid
}

// trackPublishRetained is invoked when a retained (QoS) message is published.
// It needs a new PI to be allocated, so we add it to the pendingPublish map,
// and serialize it in cpending under the subscription's pseudo-durable key
// (with the PI as the sequence) so consumer teardown purges it; an ack removes
// both entries via untrackPublish.
//
// Lock held on entry
func (sess *mqttSession) trackPublishRetained(sid string, qos byte) uint16 {
	// Make sure we initialize the tracking maps.
	if sess.pendingPublish == nil {
		sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	if sess.cpending == nil {
		sess.cpending = make(map[string]map[uint64]uint16)
	}

	pi := sess.bumpPI()
	if pi == 0 {
		return 0
	}
	dur := mqttRetainedPendingDur(sid)
	sseqToPi := sess.cpending[dur]
	if sseqToPi == nil {
		sseqToPi = make(map[uint64]uint16)
		sess.cpending[dur] = sseqToPi
	}
	sseqToPi[uint64(pi)] = pi
	sess.pendingPublish[pi] = &mqttPending{jsDur: dur, sseq: uint64(pi), qos: qos}

	return pi
}

// trackPublish is invoked when a (QoS) PUBLISH message is to be delivered. It
// detects an untracked (new) message based on its sequence extracted from its
// delivery-time jsAckSubject, and adds it to the tracking maps. Returns a PI to
// use for the message (new, or previously used), and whether this is a
// duplicate delivery attempt.
//
// Lock held on entry
func (sess *mqttSession) trackPublish(jsDur, jsAckSubject string, qos byte) (uint16, bool) {
	var dup bool
	var pi uint16

	if jsAckSubject == _EMPTY_ || jsDur == _EMPTY_ {
		return 0, false
	}

	// Make sure we initialize the tracking maps.
	if sess.pendingPublish == nil {
		sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	if sess.cpending == nil {
		sess.cpending = make(map[string]map[uint64]uint16)
	}

	// Get the stream sequence and duplicate flag from the ack reply subject.
	sseq, _, dcount, _, _ := ackReplyInfo(jsAckSubject)
	if dcount > 1 {
		dup = true
	}

	var ack *mqttPending
	sseqToPi, ok := sess.cpending[jsDur]
	if !ok {
		sseqToPi = make(map[uint64]uint16)
		sess.cpending[jsDur] = sseqToPi
	} else {
		pi = sseqToPi[sseq]
	}

	if pi != 0 {
		// There is a possible race between a PUBLISH re-delivery calling us,
		// and a PUBREC received already having submitting a PUBREL into JS . If
		// so, indicate no need for (re-)delivery by returning a PI of 0.
		_, usedForPubRel := sess.pendingPubRel[pi]
		if /*dup && */ usedForPubRel {
			return 0, false
		}

		// We should have a pending JS ACK for this PI.
		ack = sess.pendingPublish[pi]
	} else {
		// sess.maxp will always have a value > 0.
		if len(sess.pendingPublish) >= int(sess.maxp) {
			// Indicate that we did not assign a packet identifier.
			// The caller will not send the message to the subscription
			// and JS will redeliver later, based on consumer's AckWait.
			return 0, false
		}

		pi = sess.bumpPI()
		if pi == 0 {
			return 0, false
		}

		sseqToPi[sseq] = pi
	}

	if ack == nil {
		sess.pendingPublish[pi] = &mqttPending{
			jsDur:        jsDur,
			sseq:         sseq,
			jsAckSubject: jsAckSubject,
			qos:          qos,
		}
	} else {
		ack.jsAckSubject = jsAckSubject
		ack.sseq = sseq
		ack.jsDur = jsDur
		ack.qos = qos
	}

	return pi, dup
}

// Stops a PI from being tracked as a PUBLISH. It can still be in use for a
// pending PUBREL.
//
// Lock held on entry
func (sess *mqttSession) untrackPublish(pi uint16) (jsAckSubject string) {
	ack, ok := sess.pendingPublish[pi]
	if !ok {
		return _EMPTY_
	}

	delete(sess.pendingPublish, pi)
	// Do NOT reset last_pi here: it is a monotonic rolling counter (see bumpPI) so
	// a just-freed id is not reused within a delivery burst, which a client would
	// read as a duplicate. Spec [MQTT-2.3.1-4].

	if len(sess.cpending) != 0 && ack.jsDur != _EMPTY_ {
		if sseqToPi := sess.cpending[ack.jsDur]; sseqToPi != nil {
			delete(sseqToPi, ack.sseq)
		}
	}

	return ack.jsAckSubject
}

// trackAsPubRel is invoked in 2 cases: (a) when we receive a PUBREC and we need
// to change from tracking the PI as a PUBLISH to a PUBREL; and (b) when we
// attempt to deliver the PUBREL to record the JS ack subject for it.
//
// Lock held on entry
func (sess *mqttSession) trackAsPubRel(pi uint16, jsAckSubject string) {
	if sess.pubRelConsumer == nil {
		// The cosumer MUST be set up already.
		return
	}
	jsDur := sess.pubRelConsumer.Durable

	if sess.pendingPubRel == nil {
		sess.pendingPubRel = make(map[uint16]*mqttPending)
	}

	if jsAckSubject == _EMPTY_ {
		sess.pendingPubRel[pi] = &mqttPending{
			jsDur: jsDur,
		}
		return
	}

	sseq, _, _, _, _ := ackReplyInfo(jsAckSubject)

	if sess.cpending == nil {
		sess.cpending = make(map[string]map[uint64]uint16)
	}
	sseqToPi := sess.cpending[jsDur]
	if sseqToPi == nil {
		sseqToPi = make(map[uint64]uint16)
		sess.cpending[jsDur] = sseqToPi
	}
	sseqToPi[sseq] = pi
	sess.pendingPubRel[pi] = &mqttPending{
		jsDur:        sess.pubRelConsumer.Durable,
		sseq:         sseq,
		jsAckSubject: jsAckSubject,
	}
}

// Stops a PI from being tracked as a PUBREL.
//
// Lock held on entry
func (sess *mqttSession) untrackPubRel(pi uint16) (jsAckSubject string) {
	ack, ok := sess.pendingPubRel[pi]
	if !ok {
		return _EMPTY_
	}

	delete(sess.pendingPubRel, pi)

	if sess.pubRelConsumer != nil && len(sess.cpending) > 0 {
		if sseqToPi := sess.cpending[ack.jsDur]; sseqToPi != nil {
			delete(sseqToPi, ack.sseq)
		}
	}

	return ack.jsAckSubject
}

// Sends a consumer delete request, but does not wait for response.
//
// Lock not held on entry.
func (sess *mqttSession) deleteConsumer(cc *ConsumerConfig) {
	sess.mu.Lock()
	sess.tmaxack -= cc.MaxAckPending
	sess.jsa.deleteConsumer(mqttStreamName, cc.Durable, true)
	sess.mu.Unlock()
}

//////////////////////////////////////////////////////////////////////////////
//
// CONNECT protocol related functions
//
//////////////////////////////////////////////////////////////////////////////

// Parse the MQTT connect protocol
func (c *client) mqttParseConnect(r *mqttReader, hasMappings bool) (byte, *mqttConnectProto, error) {
	// Protocol name
	proto, err := r.readBytes("protocol name", false)
	if err != nil {
		return 0, nil, err
	}

	// Spec [MQTT-3.1.2-1]
	if !bytes.Equal(proto, mqttProtoName) {
		// Check proto name against v3.1 to report better error
		if bytes.Equal(proto, mqttOldProtoName) {
			return 0, nil, fmt.Errorf("older protocol %q not supported", proto)
		}
		return 0, nil, fmt.Errorf("expected connect packet with protocol name %q, got %q", mqttProtoName, proto)
	}

	// Protocol level
	level, err := r.readByte("protocol level")
	if err != nil {
		return 0, nil, err
	}
	// Spec [MQTT-3.1.2-2]
	if level != mqttProtoLevel {
		return mqttConnAckRCUnacceptableProtocolVersion, nil, fmt.Errorf("unacceptable protocol version of %v", level)
	}

	cp := &mqttConnectProto{}
	// Connect flags
	cp.flags, err = r.readByte("flags")
	if err != nil {
		return 0, nil, err
	}

	// Spec [MQTT-3.1.2-3]
	if cp.flags&mqttConnFlagReserved != 0 {
		return 0, nil, errMQTTConnFlagReserved
	}

	var hasWill bool
	wqos := (cp.flags & mqttConnFlagWillQoS) >> 3
	wretain := cp.flags&mqttConnFlagWillRetain != 0
	// Spec [MQTT-3.1.2-11]
	if cp.flags&mqttConnFlagWillFlag == 0 {
		// Spec [MQTT-3.1.2-13]
		if wqos != 0 {
			return 0, nil, fmt.Errorf("if Will flag is set to 0, Will QoS must be 0 too, got %v", wqos)
		}
		// Spec [MQTT-3.1.2-15]
		if wretain {
			return 0, nil, errMQTTWillAndRetainFlag
		}
	} else {
		// Spec [MQTT-3.1.2-14]
		if wqos == 3 {
			return 0, nil, fmt.Errorf("if Will flag is set to 1, Will QoS can be 0, 1 or 2, got %v", wqos)
		}
		hasWill = true
	}

	if c.mqtt.rejectQoS2Pub && hasWill && wqos == 2 {
		return mqttConnAckRCQoS2WillRejected, nil, fmt.Errorf("server does not accept QoS2 for Will messages")
	}

	// Spec [MQTT-3.1.2-19]
	hasUser := cp.flags&mqttConnFlagUsernameFlag != 0
	// Spec [MQTT-3.1.2-21]
	hasPassword := cp.flags&mqttConnFlagPasswordFlag != 0
	// Spec [MQTT-3.1.2-22]
	if !hasUser && hasPassword {
		return 0, nil, errMQTTPasswordFlagAndNoUser
	}

	// Keep alive
	var ka uint16
	ka, err = r.readUint16("keep alive")
	if err != nil {
		return 0, nil, err
	}
	// Spec [MQTT-3.1.2-24]
	if ka > 0 {
		cp.rd = time.Duration(float64(ka)*1.5) * time.Second
	}

	// Payload starts here and order is mandated by:
	// Spec [MQTT-3.1.3-1]: client ID, will topic, will message, username, password

	// Client ID
	c.mqtt.cid, err = r.readString("client ID")
	if err != nil {
		return 0, nil, err
	}
	// Spec [MQTT-3.1.3-7]
	if c.mqtt.cid == _EMPTY_ {
		if cp.flags&mqttConnFlagCleanSession == 0 {
			return mqttConnAckRCIdentifierRejected, nil, errMQTTCIDEmptyNeedsCleanFlag
		}
		// Spec [MQTT-3.1.3-6]
		c.mqtt.cid = nuid.Next()
	}
	// Spec [MQTT-3.1.3-4] and [MQTT-3.1.3-9]
	if err := mqttValidateString(c.mqtt.cid, "client ID"); err != nil {
		return mqttConnAckRCIdentifierRejected, nil, err
	} else if !isValidName(c.mqtt.cid) {
		// Should not contain characters that make it an invalid name for NATS subjects, etc.
		err = fmt.Errorf("invalid character in %s %q", "client ID", c.mqtt.cid)
		return mqttConnAckRCIdentifierRejected, nil, err
	}

	if hasWill {
		cp.will = &mqttWill{
			qos:    wqos,
			retain: wretain,
		}
		var topic []byte
		// Need to make a copy since we need to hold to this topic after the
		// parsing of this protocol.
		topic, err = r.readBytes("Will topic", true)
		if err != nil {
			return 0, nil, err
		}
		if len(topic) == 0 {
			return 0, nil, errMQTTEmptyWillTopic
		}
		if err := mqttValidateTopic(topic, "Will topic"); err != nil {
			return 0, nil, err
		}
		// Convert MQTT topic to NATS subject
		cp.will.subject, err = mqttTopicToNATSPubSubject(topic)
		if err != nil {
			return 0, nil, err
		}
		// Check for subject mapping.
		if hasMappings {
			// For selectMappedSubject to work, we need to have c.pa.subject set.
			// If there is a change, c.pa.mapped will be set after the call.
			c.pa.subject = cp.will.subject
			if changed := c.selectMappedSubject(); changed {
				// We need to keep track of the NATS subject/mapped in the `cp` structure.
				cp.will.subject = c.pa.subject
				cp.will.mapped = c.pa.mapped
				// We also now need to map the original MQTT topic to the new topic
				// based on the new subject.
				topic = natsSubjectToMQTTTopic(cp.will.subject)
			}
			// Reset those now.
			c.pa.subject, c.pa.mapped = nil, nil
		}
		cp.will.topic = topic
		// Now "will" message.
		// Ask for a copy since we need to hold to this after parsing of this protocol.
		cp.will.message, err = r.readBytes("Will message", true)
		if err != nil {
			return 0, nil, err
		}
	}

	if hasUser {
		c.opts.Username, err = r.readString("user name")
		if err != nil {
			return 0, nil, err
		}
		if c.opts.Username == _EMPTY_ {
			return mqttConnAckRCBadUserOrPassword, nil, errMQTTEmptyUsername
		}
		// Spec [MQTT-3.1.3-11]
		if err := mqttValidateString(c.opts.Username, "user name"); err != nil {
			return mqttConnAckRCBadUserOrPassword, nil, err
		}
	}

	if hasPassword {
		c.opts.Password, err = r.readString("password")
		if err != nil {
			return 0, nil, err
		}
		c.opts.Token = c.opts.Password
	}
	return 0, cp, nil
}

func (c *client) mqttConnectTrace(cp *mqttConnectProto) string {
	trace := fmt.Sprintf("clientID=%s", c.mqtt.cid)
	if cp.rd > 0 {
		trace += fmt.Sprintf(" keepAlive=%v", cp.rd)
	}
	if cp.will != nil {
		trace += fmt.Sprintf(" will=(topic=%s QoS=%v retain=%v)",
			cp.will.topic, cp.will.qos, cp.will.retain)
	}
	if cp.flags&mqttConnFlagCleanSession != 0 {
		trace += " clean"
	}
	if c.opts.Username != _EMPTY_ {
		trace += fmt.Sprintf(" username=%s", c.opts.Username)
	}
	if c.opts.Password != _EMPTY_ {
		trace += " password=****"
	}
	return trace
}

// Process the CONNECT packet.
//
// For the first session on the account, an account session manager will be created,
// along with the JetStream streams/consumer necessary for the working of MQTT.
//
// The session, identified by a client ID, will be registered, or if already existing,
// will be resumed. If the session exists but is associated with an existing client,
// the old client is evicted, as per the specifications.
//
// Due to specific locking requirements around JS API requests, we cannot hold some
// locks for the entire duration of processing of some protocols, therefore, we use
// a map that registers the client ID in a "locked" state. If a different client tries
// to connect and the server detects that the client ID is in that map, it will try
// a little bit until it is not, or fail the new client, since we can't protect
// processing of protocols in the original client. This is not expected to happen often.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (s *Server) mqttProcessConnect(c *client, cp *mqttConnectProto, trace bool) error {
	sendConnAck := func(rc byte, sessp bool) {
		c.mqttEnqueueConnAck(rc, sessp)
		if trace {
			c.traceOutOp("CONNACK", []byte(fmt.Sprintf("sp=%v rc=%v", sessp, rc)))
		}
	}

	c.mu.Lock()
	cid := c.mqtt.cid
	c.clearAuthTimer()
	c.mu.Unlock()
	if !s.isClientAuthorized(c) {
		if trace {
			c.traceOutOp("CONNACK", []byte(fmt.Sprintf("sp=%v rc=%v", false, mqttConnAckRCNotAuthorized)))
		}
		c.authViolation()
		return ErrAuthentication
	}
	// Now that we are authenticated, we have the client bound to the account.
	// Get the account's level MQTT sessions manager. If it does not exists yet,
	// this will create it along with the streams where sessions and messages
	// are stored.
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		return err
	}

	// Most of the session state is altered only in the readLoop so does not
	// need locking. For things that can be access in the readLoop and in
	// callbacks, we will use explicit locking.
	// To prevent other clients to connect with the same client ID, we will
	// add the client ID to a "locked" map so that the connect somewhere else
	// is put on hold.
	// This keep track of how many times this client is detecting that its
	// client ID is in the locked map. After a short amount, the server will
	// fail this inbound client.
	locked := 0

CHECK:
	asm.mu.Lock()
	// Check if different applications keep trying to connect with the same
	// client ID at the same time.
	if tm, ok := asm.flappers[cid]; ok {
		// If the last time it tried to connect was more than 1 sec ago,
		// then accept and remove from flappers map.
		if time.Since(tm) > mqttSessJailDur {
			asm.removeSessFromFlappers(cid)
		} else {
			// Will hold this client for a second and then close it. We
			// do this so that if the client has a reconnect feature we
			// don't end-up with very rapid flapping between apps.
			// We need to wait in place and not schedule the connection
			// close because if this is a misbehaved client that does
			// not wait for the CONNACK and sends other protocols, the
			// server would not have a fully setup client and may panic.
			asm.mu.Unlock()
			select {
			case <-s.quitCh:
			case <-time.After(mqttSessJailDur):
			}
			c.closeConnection(DuplicateClientID)
			return ErrConnectionClosed
		}
	}
	// If an existing session is in the process of processing some packet, we can't
	// evict the old client just yet. So try again to see if the state clears, but
	// if it does not, then we have no choice but to fail the new client instead of
	// the old one.
	if _, ok := asm.sessLocked[cid]; ok {
		asm.mu.Unlock()
		if locked++; locked == 10 {
			return fmt.Errorf("other session with client ID %q is in the process of connecting", cid)
		}
		time.Sleep(100 * time.Millisecond)
		goto CHECK
	}

	// Register this client ID the "locked" map for the duration if this function.
	asm.sessLocked[cid] = struct{}{}
	// And remove it on exit, regardless of error or not.
	defer func() {
		asm.mu.Lock()
		delete(asm.sessLocked, cid)
		asm.mu.Unlock()
	}()

	// Is the client requesting a clean session or not.
	cleanSess := cp.flags&mqttConnFlagCleanSession != 0
	// Session present? Assume false, will be set to true only when applicable.
	sessp := false
	// Do we have an existing session for this client ID
	es, exists := asm.sessions[cid]
	asm.mu.Unlock()
	formatError := func(err error) error {
		return fmt.Errorf("%v for account %q, session %q", err, c.acc.GetName(), cid)
	}

	// The session is not in the map, but may be on disk, so try to recover
	// or create the stream if not.
	if !exists {
		es, exists, err = asm.createOrRestoreSession(cid, s.getOpts())
		if err != nil {
			if err == errMQTTSessionCollision {
				sendConnAck(mqttConnAckRCIdentifierRejected, false)
			}
			return formatError(err)
		}
	}
	if exists {
		// Clear the session if client wants a clean session.
		// Also, Spec [MQTT-3.2.2-1]: don't report session present
		if cleanSess || es.clean {
			// Spec [MQTT-3.1.2-6]: If CleanSession is set to 1, the Client and
			// Server MUST discard any previous Session and start a new one.
			// This Session lasts as long as the Network Connection. State data
			// associated with this Session MUST NOT be reused in any subsequent
			// Session.
			if err := es.clear(false); err != nil {
				asm.removeSession(es, true)
				return err
			}
		} else {
			// Report to the client that the session was present
			sessp = true
		}
		// Spec [MQTT-3.1.4-2]. If the ClientId represents a Client already
		// connected to the Server then the Server MUST disconnect the existing
		// client.
		// Bind with the new client. This needs to be protected because can be
		// accessed outside of the readLoop.
		es.mu.Lock()
		ec := es.c
		es.c = c
		es.clean = cleanSess
		// Clear this flag so we resubscribe to PUBREL subject is needed.
		es.pubRelSubscribed = false
		es.mu.Unlock()
		if ec != nil {
			// Remove "will" of existing client before closing
			ec.mu.Lock()
			ec.mqtt.cp.will = nil
			ec.mu.Unlock()
			// Add to the map of the flappers
			asm.mu.Lock()
			asm.addSessToFlappers(cid)
			asm.mu.Unlock()
			c.Warnf("Replacing old client %q since both have the same client ID %q", ec, cid)
			// Close old client in separate go routine
			go ec.closeConnection(DuplicateClientID)
		}
	} else {
		// Spec [MQTT-3.2.2-3]: if the Server does not have stored Session state,
		// it MUST set Session Present to 0 in the CONNACK packet.
		es.mu.Lock()
		es.c, es.clean = c, cleanSess
		es.mu.Unlock()
		// Now add this new session into the account sessions
		asm.addSession(es, true)
	}
	// We would need to save only if it did not exist previously, but we save
	// always in case we are running in cluster mode. This will notify other
	// running servers that this session is being used.
	if err := es.save(); err != nil {
		asm.removeSession(es, true)
		return err
	}
	c.mu.Lock()
	c.flags.set(connectReceived)
	c.mqtt.cp = cp
	c.mqtt.asm = asm
	c.mqtt.sess = es
	c.mu.Unlock()

	// Spec [MQTT-3.2.0-1]: CONNACK must be the first protocol sent to the session.
	sendConnAck(mqttConnAckRCConnectionAccepted, sessp)

	// Process possible saved subscriptions.
	if l := len(es.subs); l > 0 {
		filters := make([]*mqttFilter, 0, l)
		for subject, qos := range es.subs {
			filters = append(filters, &mqttFilter{filter: subject, qos: qos})
		}
		if _, err := asm.processSubs(es, c, filters, false, trace); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) mqttEnqueueConnAck(rc byte, sessionPresent bool) {
	proto := [4]byte{mqttPacketConnectAck, 2, 0, rc}
	c.mu.Lock()
	// Spec [MQTT-3.2.2-4]. If return code is different from 0, then
	// session present flag must be set to 0.
	if rc == 0 {
		if sessionPresent {
			proto[2] = 1
		}
	}
	c.enqueueProto(proto[:])
	c.mu.Unlock()
}

func (s *Server) mqttHandleWill(c *client) {
	c.mu.Lock()
	if c.mqtt.cp == nil {
		c.mu.Unlock()
		return
	}
	will := c.mqtt.cp.will
	if will == nil {
		c.mu.Unlock()
		return
	}
	// Create a synthetic PUBLISH packet to be delivered to the session's
	// subscriptions, regardless of what is currently in c.mqtt.pp.
	pp := &mqttPublish{
		topic:   will.topic,
		subject: will.subject,
		mapped:  will.mapped,
		msg:     will.message,
		sz:      len(will.message),
		flags:   will.qos << 1,
	}
	if will.retain {
		pp.flags |= mqttPubFlagRetain
	}
	c.mu.Unlock()
	s.mqttInitiateMsgDelivery(c, pp, 0)
	c.flushClients(0)
}

//////////////////////////////////////////////////////////////////////////////
//
// PUBLISH protocol related functions
//
//////////////////////////////////////////////////////////////////////////////

func (c *client) mqttParsePub(r *mqttReader, pl int, pp *mqttPublish, hasMappings bool) error {
	qos := mqttGetQoS(pp.flags)
	if qos > 2 {
		return fmt.Errorf("QoS=%v is invalid in MQTT", qos)
	}

	if c.mqtt.rejectQoS2Pub && qos == 2 {
		return fmt.Errorf("QoS=2 is disabled for PUBLISH messages")
	}

	// Keep track of where we are when starting to read the variable header
	start := r.pos

	var err error
	pp.topic, err = r.readBytes("topic", false)
	if err != nil {
		return err
	}
	if len(pp.topic) == 0 {
		return errMQTTTopicIsEmpty
	}
	if err := mqttValidateTopic(pp.topic, "topic"); err != nil {
		return err
	}
	// Convert the topic to a NATS subject. This call will also check that
	// there is no MQTT wildcards (Spec [MQTT-3.3.2-2] and [MQTT-4.7.1-1])
	// Note that this may not result in a copy if there is no conversion.
	// It is good because after the message is processed we won't have a
	// reference to the buffer and we save a copy.
	pp.subject, err = mqttTopicToNATSPubSubject(pp.topic)
	if err != nil {
		return err
	}

	// Check for subject mapping.
	if hasMappings {
		// For selectMappedSubject to work, we need to have c.pa.subject set.
		// If there is a change, c.pa.mapped will be set after the call.
		c.pa.subject = pp.subject
		if changed := c.selectMappedSubject(); changed {
			// We need to keep track of the NATS subject/mapped in the `pp` structure.
			pp.subject = c.pa.subject
			pp.mapped = c.pa.mapped
			// We also now need to map the original MQTT topic to the new topic
			// based on the new subject.
			pp.topic = natsSubjectToMQTTTopic(pp.subject)
		}
		// Reset those now.
		c.pa.subject, c.pa.mapped = nil, nil
	}

	if qos > 0 {
		pp.pi, err = r.readUint16("packet identifier")
		if err != nil {
			return err
		}
		if pp.pi == 0 {
			return fmt.Errorf("with QoS=%v, packet identifier cannot be 0", qos)
		}
	} else {
		pp.pi = 0
	}

	// The message payload will be the total packet length minus
	// what we have consumed for the variable header
	payloadSize := pl - (r.pos - start)
	if payloadSize < 0 {
		return fmt.Errorf("invalid remaining length %d for PUBLISH packet", pl)
	}
	pp.sz = payloadSize
	if pp.sz > 0 {
		start = r.pos
		r.pos += pp.sz
		pp.msg = r.buf[start:r.pos]
	} else if pp.sz == 0 {
		pp.msg = nil
	} else {
		return errMQTTInvalidPublishLength
	}
	return nil
}

func mqttValidateTopic(topic []byte, field string) error {
	if !utf8.Valid(topic) {
		return fmt.Errorf("invalid utf8 for %s %q", field, topic)
	}
	if bytes.IndexByte(topic, 0) >= 0 {
		return fmt.Errorf("invalid null character in %s %q", field, topic)
	}
	return nil
}

func mqttValidateString(value string, field string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("invalid utf8 for %s %q", field, value)
	}
	if strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("invalid null character in %s %q", field, value)
	}
	return nil
}

func mqttPubTrace(pp *mqttPublish) string {
	dup := pp.flags&mqttPubFlagDup != 0
	qos := mqttGetQoS(pp.flags)
	retain := mqttIsRetained(pp.flags)
	var piStr string
	if pp.pi > 0 {
		piStr = fmt.Sprintf(" pi=%v", pp.pi)
	}
	return fmt.Sprintf("%s dup=%v QoS=%v retain=%v size=%v%s",
		pp.topic, dup, qos, retain, pp.sz, piStr)
}

// mqttComputeNatsMsgSize computes the size the NATS message to be delivered
// based on a MQTT PUBLISH packet.
// encodePP: whether to encode complete MQTT PUBLISH packet header information
//   - false: initial delivery (QoS 0/1) needs only base header
//   - true: QoS2 storage needs to encode Nmqtt-Subject and Nmqtt-Mapped
func mqttComputeNatsMsgSize(pp *mqttPublish, encodePP bool) int {
	size := len(hdrLine) +
		len(mqttNatsHeader) + 2 + 2 + // 2 for ':<qos>', and 2 for CRLF
		2 + // end-of-header CRLF
		pp.sz
	if encodePP {
		size++                                   // for the flags byte
		size += len(mqttNatsHeaderSubject) + 1 + // +1 for ':'
			len(pp.subject) + 2 // 2 for CRLF

		if len(pp.mapped) > 0 {
			size += len(mqttNatsHeaderMapped) + 1 + // +1 for ':'
				len(pp.mapped) + 2 // 2 for CRLF
		}
	}
	return size
}

// Composes a NATS message from a MQTT PUBLISH packet. The message includes an
// internal header containint the original packet's QoS, and for QoS2 packets
// the original subject.
//
// Example (QoS2, subject: "foo.bar"):
//
//	NATS/1.0\r\n
//	Nmqtt-Pub:2foo.bar\r\n
//	\r\n
func mqttNewDeliverableMessage(pp *mqttPublish, encodePP bool) (natsMsg []byte, headerLen int) {
	size := mqttComputeNatsMsgSize(pp, encodePP)

	// +LEN_CR_LF: sendJSAPIrequests appends the CRLF in place.
	buf := bytes.NewBuffer(make([]byte, 0, size+LEN_CR_LF))

	qos := mqttGetQoS(pp.flags)

	buf.WriteString(hdrLine)
	buf.WriteString(mqttNatsHeader)
	buf.WriteByte(':')
	buf.WriteByte(qos + '0')
	if encodePP {
		buf.WriteByte(mqttNatsHeaderEncodeFlags(pp.flags))
	}
	buf.WriteString(_CRLF_)

	if encodePP {
		buf.WriteString(mqttNatsHeaderSubject)
		buf.WriteByte(':')
		buf.Write(pp.subject)
		buf.WriteString(_CRLF_)

		if len(pp.mapped) > 0 {
			buf.WriteString(mqttNatsHeaderMapped)
			buf.WriteByte(':')
			buf.Write(pp.mapped)
			buf.WriteString(_CRLF_)
		}
	}

	// End of header
	buf.WriteString(_CRLF_)

	headerLen = buf.Len()

	buf.Write(pp.msg)
	return buf.Bytes(), headerLen
}

// Composes a NATS message for a pending PUBREL packet. The message includes an
// internal header containing the PI for PUBREL/PUBCOMP.
//
// Example (PI:123):
//
//		NATS/1.0\r\n
//	 	Nmqtt-PubRel:123\r\n
//		\r\n
func mqttNewDeliverablePubRel(pi uint16) (natsMsg []byte, headerLen int) {
	size := len(hdrLine) +
		len(mqttNatsPubRelHeader) + 6 + 2 + // 6 for ':65535', and 2 for CRLF
		2 // end-of-header CRLF
	buf := bytes.NewBuffer(make([]byte, 0, size))
	buf.WriteString(hdrLine)
	buf.WriteString(mqttNatsPubRelHeader)
	buf.WriteByte(':')
	buf.WriteString(strconv.FormatInt(int64(pi), 10))
	buf.WriteString(_CRLF_)
	buf.WriteString(_CRLF_)
	return buf.Bytes(), buf.Len()
}

// Process the PUBLISH packet.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (s *Server) mqttProcessPub(c *client, pp *mqttPublish) error {
	qos := mqttGetQoS(pp.flags)

	// Enforce max_payload using existing client max payload logic (mpay) by
	// checking the total NATS message size that would be processed.
	if maxPayload := atomic.LoadInt32(&c.mpay); maxPayload != jwt.NoLimit {
		if total := mqttComputeNatsMsgSize(pp, qos == 2); total > int(maxPayload) {
			c.maxPayloadViolation(total, maxPayload)
			return ErrMaxPayload
		}
	}

	switch qos {
	case 0:
		return s.mqttInitiateMsgDelivery(c, pp, 0)

	case 1:
		// [MQTT-4.3.2-2]. Initiate onward delivery of the Application Message,
		// Send PUBACK.
		//
		// The receiver is not required to complete delivery of the Application
		// Message before sending the PUBACK. When its original sender receives
		// the PUBACK packet, ownership of the Application Message is
		// transferred to the receiver.
		//
		// The PUBACK is emitted by the pipeline once JetStream acks the
		// store, in the order the PUBLISH packets were received.
		return s.mqttInitiateMsgDelivery(c, pp, 0)

	case 2:
		// [MQTT-4.3.3-2]. Method A, Store message, send PUBREC.
		//
		// The receiver is not required to complete delivery of the Application
		// Message before sending the PUBREC or PUBCOMP. When its original
		// sender receives the PUBREC packet, ownership of the Application
		// Message is transferred to the receiver.
		return s.mqttPipelineHoldAndPubRec(c, pp)

	default:
		return fmt.Errorf("unreachable: invalid QoS in mqttProcessPub: %v", qos)
	}
}

//////////////////////////////////////////////////////////////////////////////
//
// Ack pipeline: PUBACK (QoS1), PUBREC and PUBCOMP (QoS2)
//
//////////////////////////////////////////////////////////////////////////////

// Max pipelined inbound QoS1/2 packets per connection (the server's
// effective "Receive Maximum"). A full window blocks the readLoop,
// backpressuring the client.
const mqttMaxPipelined = 1024

// The most JetStream operations behind one pipelined response: a PUBCOMP's
// delivery store and held-copy delete. An entry's done channel is
// buffered for this many, so the reply processing never blocks on it.
const mqttMaxPipelinedOps = 2

// Max QoS2 PUBLISH copies cached in memory per connection, between PUBREC
// and PUBREL; past this the PUBREL loads from the stream.
const mqttMaxCachedQoS2Msgs = 1024

// Max bytes of QoS2 PUBLISH copies cached per connection; past this, as past
// the count, the PUBREL loads from the stream.
const mqttMaxCachedQoS2Bytes = 4 * 1024 * 1024

// A QoS>0 PUBLISH must carry a non-zero PI, Spec [MQTT-2.3.1-1];
// mqttParsePub zeroes pi for QoS0 and synthesized messages (wills) carry
// none. So pi != 0 means a client awaits an acknowledgment.
func (pp *mqttPublish) hasClientPI() bool {
	return pp.pi != 0
}

// An inbound QoS1 PUBLISH from a client must be acknowledged with a PUBACK
// even if the message itself is dropped (e.g. on a permission violation).
// QoS2 flows are acknowledged with PUBREC/PUBCOMP instead.
func (pp *mqttPublish) owesPubAck() bool {
	return mqttGetQoS(pp.flags) == 1 && pp.hasClientPI()
}

// mqttAckPipeline emits PUBACK/PUBREC/PUBCOMP in packet order as the JS
// acks arrive; one FIFO preserves the PUBACK and PUBREC ordering of
// [MQTT-4.6.0-2] and [MQTT-4.6.0-3]. A failed or timed-out operation closes
// the connection. mqttPipeline* submit here; mqttEnqueue* write directly to
// the outbound buffer.
//
// One entry per packet, not per PI: a PUBLISH retransmitted before its
// PUBREC owes its own PUBREC [MQTT-4.3.3-2], so q can hold several entries
// for one PI. Exchange-spanning state lives in mqtt.qos2Exchanges.
type mqttAckPipeline struct {
	jsa      *mqttJSA
	q        chan *mqttPipelined
	quitCh   chan struct{}
	quitOnce sync.Once
}

// mqttPipelined is a PUBACK, PUBREC or PUBCOMP waiting for JetStream to
// acknowledge the operations behind it: none, a store, a delete, or a
// store and a delete in flight concurrently. A PUBREC's store holds a
// QoS2 PUBLISH in $MQTT_qos2in; with an exchange, the reply processing
// records the hold's outcome there before completing the entry, so it is
// set before the PUBREC and any compliant PUBREL. A PUBCOMP's store is
// the delivery into $MQTT_msgs, with the message id that lets another
// server drop a retransmitted PUBREL's re-delivery (mqttQoS2DeliveryMsgId),
// and its delete removes the held copy.
type mqttPipelined struct {
	resp byte // mqttPacketPubAck, mqttPacketPubRec or mqttPacketPubComp
	pi   uint16

	// The reply subjects registered in the account-scoped jsa.replies,
	// _EMPTY_ where there is no such operation, and the channel receiving
	// one result per registration, buffered for mqttMaxPipelinedOps. Every
	// give-up path must call abandon.
	storeReply string
	delReply   string
	done       chan error

	// A PUBREC with a cached copy: where the hold store's outcome goes.
	exchange *mqttQoS2Exchange
}

// Pool for mqttPipelined re-use; an entry keeps its channel.
var mqttPipelinedPool = sync.Pool{
	New: func() any {
		return &mqttPipelined{done: make(chan error, mqttMaxPipelinedOps)}
	},
}

// A pooled entry for a resp on pi, with no operations yet. readLoop only.
func newMQTTPipelined(resp byte, pi uint16) *mqttPipelined {
	p := mqttPipelinedPool.Get().(*mqttPipelined)
	p.resp, p.pi = resp, pi
	return p
}

// Returns the entry to the pool, keeping its channel: it is empty once
// every expected result has been received. Only a completed entry may be
// returned; on a give-up path, or in the shutdown drain, the reply
// processing that claimed a registration may still be inside complete,
// so those entries are left to the GC.
func (p *mqttPipelined) returnToPool() {
	*p = mqttPipelined{done: p.done}
	mqttPipelinedPool.Put(p)
}

// The number of JetStream acks the entry waits for, at most
// mqttMaxPipelinedOps.
func (p *mqttPipelined) pending() int {
	n := 0
	if p.storeReply != _EMPTY_ {
		n++
	}
	if p.delReply != _EMPTY_ {
		n++
	}
	return n
}

// Removes the reply registrations from jsa.replies.
func (p *mqttPipelined) abandon(jsa *mqttJSA) {
	jsa.replies.Delete(p.storeReply)
	jsa.replies.Delete(p.delReply)
}

// Registers the store's reply and submits it. readLoop only.
func (p *mqttPipelined) submitStore(jsa *mqttJSA, subject string, hdr int, msg []byte, msgId string) {
	p.storeReply = jsa.newReplySubject(mqttJSAMsgStore)
	jsa.replies.Store(p.storeReply, p)
	jsa.storeMsgAsync(subject, hdr, msg, msgId, p.storeReply)
}

// Registers the held copy's delete reply and submits it. readLoop only.
func (p *mqttPipelined) submitDelete(jsa *mqttJSA, seq uint64) {
	p.delReply = jsa.newReplySubject(mqttJSAMsgDelete)
	jsa.replies.Store(p.delReply, p)
	jsa.deleteMsgAsync(mqttQoS2IncomingMsgsStreamName, seq, p.delReply)
}

// Completes one of the entry's operations with its JS API reply. Runs
// from the reply processing, on any goroutine; never blocks.
func (p *mqttPipelined) complete(pc *client, value any) {
	var err error
	switch v := value.(type) {
	case *JSPubAckResponse:
		err = v.ToError()
		switch p.resp {
		case mqttPacketPubRec:
			if mqttIsMaxMsgsPerSubjectErr(err) {
				// Deduped by the stream (a retransmitted PUBLISH), still owed
				// its PUBREC. No sequence came back; the PUBREL loads the copy.
				if p.exchange != nil {
					p.exchange.state.Store(mqttQoS2HoldDeduped)
				}
				err = nil
			} else if err == nil && v.PubAck != nil && p.exchange != nil {
				// seq before state: the PUBREL reads seq only once it sees
				// mqttQoS2HoldStored.
				p.exchange.seq.Store(v.Sequence)
				p.exchange.state.Store(mqttQoS2HoldStored)
			}
		case mqttPacketPubComp:
			if err == nil && v.PubAck != nil && v.Duplicate {
				pc.Debugf("QoS2 delivery (pi=%v) suppressed as a duplicate of stream sequence %v", p.pi, v.Sequence)
			}
			// Note that JSStreamDuplicateMessageConflict ("duplicate message
			// id is in process") is an error like any other. The connection
			// fails and the client retransmits the PUBREL, which then finds
			// the first store if committed, or stores.
		}
	case *JSApiMsgDeleteResponse:
		if err = v.ToError(); isMsgAlreadyDeletedErr(err) {
			err = nil
		}
	default:
		// Leave a clue; the waiter will time out.
		pc.Warnf("Unexpected type %T for JS API reply of a pipelined packet (pi=%v)", v, p.pi)
		return
	}
	p.done <- err
}

// Waits for the entry's JetStream acks, jsa.timeout in total on t, which
// it re-arms. Every failing return has abandoned the entry: the
// operation's error, the timeout, errMQTTAckPipelineStopped, or
// ErrServerNotRunning.
func (pipe *mqttAckPipeline) awaitAcks(p *mqttPipelined, t *time.Timer) error {
	jsa := pipe.jsa
	t.Reset(jsa.timeout)
	for n := p.pending(); n > 0; n-- {
		var err error
		select {
		case err = <-p.done:
		case <-t.C:
			err = fmt.Errorf("no JetStream ack within %v", jsa.timeout)
		case <-pipe.quitCh:
			err = errMQTTAckPipelineStopped
		case <-jsa.quitCh:
			err = ErrServerNotRunning
		}
		if err != nil {
			p.abandon(jsa)
			return err
		}
	}
	return nil
}

// Idempotent, callable from any goroutine; unblocks a readLoop waiting in
// mqttPipelinePush. Queued entries are left for shutdown().
func (pipe *mqttAckPipeline) stop() {
	pipe.quitOnce.Do(func() { close(pipe.quitCh) })
}

// stop(), then delete the queued entries' registrations from the
// account-scoped jsa.replies, which outlives the connection. Must run on
// the readLoop (the sole producer), after its last push.
func (pipe *mqttAckPipeline) shutdown() {
	pipe.stop()
	for {
		select {
		case p := <-pipe.q:
			p.abandon(pipe.jsa)
		default:
			return
		}
	}
}

// Emits each entry's response in packet order as the JS acks arrive; a
// failed or timed-out operation stops the pipeline and closes the connection.
// One goroutine per connection with inbound QoS1/2 traffic.
func (s *Server) mqttAckLoop(c *client, pipe *mqttAckPipeline) {
	// One timer, re-armed per entry by awaitAcks; as of Go 1.23 Stop/Reset
	// guarantee no stale receive, no draining needed.
	t := time.NewTimer(time.Hour)
	defer t.Stop()

	for {
		select {
		case p := <-pipe.q:
			if err := pipe.awaitAcks(p, t); err != nil {
				if err != errMQTTAckPipelineStopped && err != ErrServerNotRunning {
					c.Errorf("JetStream failed a pipelined QoS1/2 operation (pi=%v): %v; closing the connection", p.pi, err)
					c.closeConnection(ProtocolViolation)
				}
				return
			}
			c.mu.Lock()
			trace := c.trace
			c.mu.Unlock()
			c.mqttEnqueuePubResponse(p.resp, p.pi, trace)
			p.returnToPool()

		case <-pipe.quitCh:
			return

		case <-s.quitCh:
			return
		}
	}
}

// Stores a QoS1 PUBLISH to $MQTT_msgs; PUBACK on its ack. readLoop only.
func (s *Server) mqttPipelineStoreAndPubAck(c *client, pi uint16, subject string, hdrLen int, natsMsg []byte) error {
	jsa := c.mqtt.sess.jsa
	p := newMQTTPipelined(mqttPacketPubAck, pi)
	p.submitStore(jsa, subject, hdrLen, natsMsg, _EMPTY_)
	return s.mqttPipelinePush(c, jsa, p)
}

// PUBACK with no store, through the pipeline to preserve ordering. readLoop only.
func (s *Server) mqttPipelinePubAck(c *client, pi uint16) error {
	return s.mqttPipelinePush(c, c.mqtt.sess.jsa, newMQTTPipelined(mqttPacketPubAck, pi))
}

// Stores a QoS2 PUBLISH into $MQTT_qos2in, with the packet encoded in the
// NATS header for the later delivery, and caches a copy for the PUBREL;
// PUBREC on its ack. readLoop only.
func (s *Server) mqttPipelineHoldAndPubRec(c *client, pp *mqttPublish) error {
	jsa := c.mqtt.sess.jsa
	if _, held := c.mqtt.qos2Exchanges[pp.pi]; held {
		// A retransmit of a publication held here, owed a PUBREC and
		// nothing else [MQTT-4.3.3-2]. No second store: the stream would
		// dedupe it, unless the delete from an intervening PUBREL was
		// applied first, which would leave a stray copy under the PI.
		return s.mqttPipelinePush(c, jsa, newMQTTPipelined(mqttPacketPubRec, pp.pi))
	}
	natsMsg, hdrLen := mqttNewDeliverableMessage(pp, true)
	p := newMQTTPipelined(mqttPacketPubRec, pp.pi)
	p.exchange = c.mqttRecordQoS2Publish(pp)
	p.submitStore(jsa, c.mqttQoS2InternalSubject(pp.pi), hdrLen, natsMsg, _EMPTY_)
	return s.mqttPipelinePush(c, jsa, p)
}

// Submits the delivery store and the seq-addressed delete concurrently;
// PUBCOMP on both acks. readLoop only.
func (s *Server) mqttPipelineReleaseAndPubComp(c *client, pi uint16, heldSeq uint64, subject string, hdrLen int, natsMsg []byte) error {
	jsa := c.mqtt.sess.jsa
	p := newMQTTPipelined(mqttPacketPubComp, pi)
	p.submitStore(jsa, subject, hdrLen, natsMsg, mqttQoS2DeliveryMsgId(heldSeq))
	p.submitDelete(jsa, heldSeq)
	return s.mqttPipelinePush(c, jsa, p)
}

// For a PUBREL whose message was dropped: no store, PUBCOMP still gated on
// the delete. readLoop only.
func (s *Server) mqttPipelineDeleteAndPubComp(c *client, pi uint16, heldSeq uint64) error {
	jsa := c.mqtt.sess.jsa
	p := newMQTTPipelined(mqttPacketPubComp, pi)
	p.submitDelete(jsa, heldSeq)
	return s.mqttPipelinePush(c, jsa, p)
}

// PUBCOMP with no store or delete, through the pipeline to preserve
// ordering. readLoop only.
func (s *Server) mqttPipelinePubComp(c *client, pi uint16) error {
	return s.mqttPipelinePush(c, c.mqtt.sess.jsa, newMQTTPipelined(mqttPacketPubComp, pi))
}

// Creates the pipeline and starts its mqttAckLoop goroutine. Returns nil
// if the goroutine cannot be started (server shutting down). readLoop only.
func (s *Server) mqttPipelineStart(c *client, jsa *mqttJSA) *mqttAckPipeline {
	pipe := &mqttAckPipeline{
		jsa:    jsa,
		q:      make(chan *mqttPipelined, mqttMaxPipelined),
		quitCh: make(chan struct{}),
	}
	if !s.startGoRoutine(func() {
		defer s.grWG.Done()

		s.mqttAckLoop(c, pipe)
		// The loop was the sole consumer; release a readLoop possibly
		// waiting for room in push, it observes only pipe.quitCh.
		pipe.stop()
	}) {
		return nil
	}
	return pipe
}

// Admits an entry, creating the pipeline on first use. readLoop only.
func (s *Server) mqttPipelinePush(c *client, jsa *mqttJSA, p *mqttPipelined) error {
	// The field is readLoop-owned (we are the readLoop), no lock.
	pipe := c.mqtt.pipe
	if pipe == nil {
		if pipe = s.mqttPipelineStart(c, jsa); pipe == nil {
			// Server is shutting down; the client re-sends on reconnect.
			p.abandon(jsa)
			return ErrServerNotRunning
		}
		c.mqtt.pipe = pipe
	}

	return pipe.push(p)
}

// Admits an entry into the pipeline. Rejects it once the pipeline is
// stopped: an entry admitted after the connection-close drain
// (mqttHandleClosedClient -> pipe.shutdown) would strand its reply
// registration in the account-scoped jsa.replies. readLoop only.
func (pipe *mqttAckPipeline) push(p *mqttPipelined) error {
	jsa := pipe.jsa
	select {
	case <-pipe.quitCh:
		p.abandon(jsa)
		return errMQTTAckPipelineStopped
	default:
	}

	enqueued := false
	select {
	case pipe.q <- p:
		enqueued = true
	default:
		// Window full: wait for an available slot in the pipeline.
		t := time.NewTimer(jsa.timeout)
		defer t.Stop()
		select {
		case pipe.q <- p:
			enqueued = true
		case <-pipe.quitCh:
		case <-t.C:
			p.abandon(jsa)
			return fmt.Errorf("in-flight window full (%d packets), no JetStream ack for the oldest within %v",
				mqttMaxPipelined, jsa.timeout)
		}
	}
	if !enqueued {
		// Stopped while waiting for a slot, never admitted.
		p.abandon(jsa)
		return errMQTTAckPipelineStopped
	}

	// A stop may have raced the enqueue: the close-time drain could run
	// before the entry landed and miss it. Drain again; both drains only
	// dequeue and delete, so overlapping is harmless.
	select {
	case <-pipe.quitCh:
		pipe.shutdown()
		return errMQTTAckPipelineStopped
	default:
	}
	return nil
}

func (s *Server) mqttInitiateMsgDelivery(c *client, pp *mqttPublish, heldSeq uint64) error {
	natsMsg, headerLen := mqttNewDeliverableMessage(pp, false)

	// The delivered message becomes the client's current publish (it is not
	// the last PARSED packet for a PUBREL- or will-initiated delivery), and
	// c.pa carries its pubargs; one defer restores both. c.mqtt.pp is
	// readLoop-owned, like c.pa.
	prevPP := c.mqtt.pp
	c.mqtt.pp = pp

	c.pa.subject = pp.subject
	c.pa.mapped = pp.mapped
	c.pa.reply = nil
	c.pa.hdr = headerLen
	c.pa.hdb = []byte(strconv.FormatInt(int64(c.pa.hdr), 10))
	c.pa.size = len(natsMsg)
	c.pa.szb = []byte(strconv.FormatInt(int64(c.pa.size), 10))
	defer func() {
		c.mqtt.pp = prevPP
		c.pa.subject = nil
		c.pa.mapped = nil
		c.pa.reply = nil
		c.pa.hdr = -1
		c.pa.hdb = nil
		c.pa.size = 0
		c.pa.szb = nil
	}()

	_, permIssue := c.processInboundClientMsg(natsMsg)
	if permIssue {
		// The message was dropped, but the client may still be owed a
		// PUBACK or a PUBCOMP (routed through the pipeline to preserve
		// ordering); the PUBCOMP stays gated on the delete.
		if heldSeq > 0 {
			return s.mqttPipelineDeleteAndPubComp(c, pp.pi, heldSeq)
		}
		if pp.owesPubAck() {
			return s.mqttPipelinePubAck(c, pp.pi)
		}
		return nil
	}

	// If QoS 0 messages don't need to be stored, other (1 and 2) do. Store them
	// JetStream under "$MQTT.msgs.<delivery-subject>"
	if qos := mqttGetQoS(pp.flags); qos == 0 {
		return nil
	}

	// We need to call flushClients now since this we may have called c.addToPCD
	// with destination clients (possibly a route). Without calling flushClients
	// the following call may then be stuck waiting for a reply that may never
	// come because the destination is not flushed (due to c.out.fsp > 0,
	// see addToPCD and writeLoop for details).
	c.flushClients(0)

	subject := mqttStreamSubjectPrefix + string(c.pa.subject)

	// PUBREL deliveries and QoS1 from the wire are pipelined; wills owe no
	// PUBACK and store synchronously.
	if heldSeq > 0 {
		return s.mqttPipelineReleaseAndPubComp(c, pp.pi, heldSeq, subject, headerLen, natsMsg)
	}
	if pp.owesPubAck() {
		return s.mqttPipelineStoreAndPubAck(c, pp.pi, subject, headerLen, natsMsg)
	}

	_, err := c.mqtt.sess.jsa.storeMsg(subject, headerLen, natsMsg)

	return err
}

var mqttMaxMsgErrPattern = fmt.Sprintf("%s (%v)", ErrMaxMsgsPerSubject.Error(), JSStreamStoreFailedF)

// The store of a QoS2 PUBLISH into $MQTT_qos2in hit max-msgs-per-subject:
// already held for an earlier (duplicate) PUBLISH.
//
// TODO: would prefer a more robust and performant way of checking the
// error, but it comes back wrapped as an API result.
func mqttIsMaxMsgsPerSubjectErr(err error) bool {
	return IsNatsErr(err, JSStreamStoreFailedF) && err.Error() == mqttMaxMsgErrPattern
}

func (c *client) mqttQoS2InternalSubject(pi uint16) string {
	return mqttQoS2IncomingMsgsStreamSubjectPrefix + c.mqtt.cid + "." + strconv.FormatUint(uint64(pi), 10)
}

// Process a PUBREL (QoS2, acting as Receiver).
//
// Runs from the client's readLoop.
// No lock held on entry.
func (s *Server) mqttProcessPubRel(c *client, pi uint16) error {
	var pp *mqttPublish
	var heldSeq uint64
	if exchange, held := c.mqtt.qos2Exchanges[pi]; held {
		switch state := exchange.state.Load(); state {
		case mqttQoS2HoldPending:
			// PUBREL is sent only in response to PUBREC [MQTT-4.3.3-1], and
			// the hold store's outcome lands before the PUBREC, so the
			// client jumped the flow.
			return fmt.Errorf("QoS2 PUBREL (pi=%v) received before its PUBREC was sent", pi)
		case mqttQoS2HoldDeduped:
			// The store was deduped: use the copy in the stream.
			c.mqttDropQoS2Exchange(pi)
		case mqttQoS2HoldStored:
			pp, heldSeq = &exchange.pp, exchange.seq.Load()
		default:
			return fmt.Errorf("unreachable: QoS2 exchange (pi=%v) in hold state %v", pi, state)
		}
		// pp may point into the exchange; the delivery copies it out.
		defer exchange.returnToPool()
	} else if c.mqtt.qos2Released.Exists(uint64(pi)) {
		// A retransmission: a load could still see the async-deleted copy
		// [MQTT-4.3.3-2].
		return s.mqttPipelinePubComp(c, pi)
	}
	if pp == nil {
		var err error
		if pp, heldSeq, err = c.mqttLoadHeldQoS2Msg(pi); err != nil {
			return err
		}
		if pp == nil {
			return s.mqttPipelinePubComp(c, pi)
		}
	}

	c.mqttMarkQoS2Released(pi)

	return s.mqttInitiateMsgDelivery(c, pp, heldSeq)
}

// Loads the held QoS2 message for pi and its sequence; (nil, 0, nil) when
// none is held. readLoop only.
func (c *client) mqttLoadHeldQoS2Msg(pi uint16) (*mqttPublish, uint64, error) {
	jsa := c.mqtt.sess.jsa
	stored, err := jsa.loadLastMsgFor(mqttQoS2IncomingMsgsStreamName, c.mqttQoS2InternalSubject(pi))
	if err != nil {
		if IsNatsErr(err, JSNoMessageFoundErr) {
			return nil, 0, nil
		}
		// The copy may still be held. A PUBCOMP now would end the
		// exchange on the client side, and nothing else ever delivers
		// the copy; failing the connection gets the PUBREL retransmitted.
		return nil, 0, fmt.Errorf("unable to load the held QoS2 message (pi=%v): %v", pi, err)
	}

	// Only MQTT QoS2 messages should be here, and they must have a subject.
	h := mqttParsePublishNATSHeader(stored.Header)
	if h == nil || h.qos != 2 || len(h.subject) == 0 {
		// Delete it so the retried PUBREL converges to a bare PUBCOMP.
		jsa.deleteMsgAsync(mqttQoS2IncomingMsgsStreamName, stored.Sequence, _EMPTY_)
		return nil, 0, errMQTTInvalidQoS2HeldMessage
	}

	flags := h.qos << 1
	if h.retained {
		flags |= mqttPubFlagRetain
	}
	pp := &mqttPublish{
		topic:   natsSubjectToMQTTTopic(h.subject),
		subject: h.subject,
		mapped:  h.mapped,
		msg:     stored.Data,
		sz:      len(stored.Data),
		pi:      pi,
		flags:   flags,
	}
	return pp, stored.Sequence, nil
}

// Message id of a QoS2 delivery, so a PUBREL retransmitted to another
// server re-stores under the same id and is deduped. The held copy's
// sequence is stable across retransmits of one publication and distinct
// across publications; client id plus PI would drop a new one on a reused PI.
func mqttQoS2DeliveryMsgId(heldSeq uint64) string {
	return mqttQoS2DeliveryMsgIdPrefix + strconv.FormatUint(heldSeq, 10)
}

// The outcome of an exchange's hold store, as the PUBREL sees it.
const (
	// No ack yet, so no PUBREC sent: a PUBREL now jumped the flow.
	mqttQoS2HoldPending uint32 = iota
	// Acked, the held copy's sequence is in exchange.seq.
	mqttQoS2HoldStored
	// Rejected by max-msgs-per-subject: an earlier PUBLISH's copy is in the
	// stream, no sequence came back, the PUBREL loads it.
	mqttQoS2HoldDeduped
)

// A pending entry in qos2Exchanges.
type mqttQoS2Exchange struct {
	pp mqttPublish
	// The cached copy's backing buffer, from nbPoolGet up to the medium class.
	buf []byte

	// Written seq then state by the JS API reply processing before the
	// PUBREC is sent; read by the readLoop on PUBREL, which may come early.
	state atomic.Uint32
	seq   atomic.Uint64
}

// Pool for mqttQoS2Exchange re-use.
var mqttQoS2ExchangePool = sync.Pool{
	New: func() any {
		return new(mqttQoS2Exchange)
	},
}

// Returns a dropped exchange to the pool. Only once the readLoop has seen
// the hold's outcome (state Stored or Deduped): the reply processing
// writes nothing after the state, and its entry, which may still point
// here until the ack loop returns it, never reads the exchange after
// complete. The PUBREL's delivery, if any, copies the cached PUBLISH out
// first. A Pending exchange fails the connection and is left to the GC.
func (exchange *mqttQoS2Exchange) returnToPool() {
	nbPoolPut(exchange.buf)
	*exchange = mqttQoS2Exchange{}
	mqttQoS2ExchangePool.Put(exchange)
}

func (pp *mqttPublish) cachedSize() int {
	return len(pp.topic) + len(pp.subject) + len(pp.mapped) + len(pp.msg)
}

// Copies the PUBLISH so that the PUBREL can deliver it without a JetStream
// load. Returns nil if we decide not to keep one, in which case the PUBREL will
// load the message from the stream. The PI is not held. readLoop only.
func (c *client) mqttRecordQoS2Publish(pp *mqttPublish) *mqttQoS2Exchange {
	m := c.mqtt
	// On a released PI this is a new publication, DUP or not [MQTT-4.3.3-2].
	// A DUP unknown here may duplicate a copy from a previous connection;
	// only the stream knows which was accepted, so let the PUBREL load.
	if !m.qos2Released.Delete(uint64(pp.pi)) && pp.flags&mqttPubFlagDup != 0 {
		return nil
	}

	n := pp.cachedSize()
	if len(m.qos2Exchanges) >= mqttMaxCachedQoS2Msgs || m.qos2CachedBytes+n > mqttMaxCachedQoS2Bytes {
		return nil
	}
	if m.qos2Exchanges == nil {
		m.qos2Exchanges = make(map[uint16]*mqttQoS2Exchange)
	}

	// Copy the slices since they point to the readLoop's buffer.
	exchange := mqttQoS2ExchangePool.Get().(*mqttQoS2Exchange)
	exchange.pp = *pp
	cp := &exchange.pp
	var buf []byte
	if n <= nbPoolSizeMedium {
		// Sets the length to n; the capacity stays the pool's frame size,
		// which is how nbPoolPut recognizes the frame.
		buf = nbPoolGet(n)[:n]
	} else {
		buf = make([]byte, n)
	}
	exchange.buf = buf
	// All four fields share buf. Each is sliced with its capacity limited
	// to its own length, so appending to one cannot overwrite the next.
	i := copy(buf, pp.topic)
	cp.topic = buf[:i:i]
	j := i + copy(buf[i:], pp.subject)
	cp.subject = buf[i:j:j]
	k := j + copy(buf[j:], pp.mapped)
	cp.mapped = buf[j:k:k]
	copy(buf[k:], pp.msg)
	cp.msg = buf[k:n:n]
	m.qos2Exchanges[pp.pi] = exchange
	m.qos2CachedBytes += n
	return exchange
}

// Removes the entry, if any, and returns its bytes to the budget. readLoop only.
func (c *client) mqttDropQoS2Exchange(pi uint16) {
	m := c.mqtt
	if exchange, held := m.qos2Exchanges[pi]; held {
		m.qos2CachedBytes -= exchange.pp.cachedSize()
		delete(m.qos2Exchanges, pi)
	}
}

// pending -> released. readLoop only.
func (c *client) mqttMarkQoS2Released(pi uint16) {
	c.mqttDropQoS2Exchange(pi)
	c.mqtt.qos2Released.Insert(uint64(pi))
}

// Invoked when processing an inbound client message. If the "retain" flag is
// set, the message is stored so it can be later resent to (re)starting
// subscriptions that match the subject.
//
// Invoked from the MQTT publisher's readLoop. No client lock is held on entry.
func (c *client) mqttHandlePubRetain() {
	pp := c.mqtt.pp
	retainMQTT := mqttIsRetained(pp.flags)
	isBirth, _, isCertificate := sparkbParseBirthDeathTopic(pp.topic)
	retainSparkbBirth := isBirth && !isCertificate

	// [tck-id-topics-nbirth-mqtt] NBIRTH messages MUST be published with MQTT
	// QoS equal to 0 and retain equal to false.
	//
	// [tck-id-conformance-mqtt-aware-nbirth-mqtt-retain] A Sparkplug Aware MQTT
	// Server MUST make NBIRTH messages available on the topic:
	// $sparkplug/certificates/namespace/group_id/NBIRTH/edge_node_id with the
	// MQTT retain flag set to true.
	if retainMQTT == retainSparkbBirth {
		// (retainSparkbBirth && retainMQTT) : not valid, so ignore altogether.
		// (!retainSparkbBirth && !retainMQTT) : nothing to do.
		return
	}

	asm := c.mqtt.asm
	key := string(pp.subject)

	// Always clear the retain flag to deliver a normal published message.
	defer func() {
		pp.flags &= ^mqttPubFlagRetain
	}()

	// Spec [MQTT-3.3.1-11]. Payload of size 0 removes the retained message, but
	// should still be delivered as a normal message.
	//
	// We used to delete the message here from our map, the stream, and notify
	// the network about the delete. We no longer do that. Instead, we store
	// the message with an empty body. When servers will get the empty body
	// in processRetainedMsg, then will remove the message from their map. This
	// effectively serializes all add/remove of retained messages without the
	// need for "network" notifications about deletes (we still support that
	// for backward compatibility but will be pulled in future releases).

	rm := &mqttRetainedMsg{
		Origin: asm.jsa.id,
		Msg:    pp.msg, // will copy these bytes later as we process rm.
		Flags:  pp.flags,
		Source: c.opts.Username,
	}

	if retainSparkbBirth {
		// [tck-id-conformance-mqtt-aware-store] A Sparkplug Aware MQTT Server
		// MUST store NBIRTH and DBIRTH messages as they pass through the MQTT
		// Server.
		//
		// [tck-id-conformance-mqtt-aware-nbirth-mqtt-topic]. A Sparkplug Aware
		// MQTT Server MUST make NBIRTH messages available on a topic of the
		// form: $sparkplug/certificates/namespace/group_id/NBIRTH/edge_node_id
		//
		// [tck-id-conformance-mqtt-aware-dbirth-mqtt-topic] A Sparkplug Aware
		// MQTT Server MUST make DBIRTH messages available on a topic of the
		// form:
		// $sparkplug/certificates/namespace/group_id/DBIRTH/edge_node_id/device_id
		topic := append(sparkbCertificatesTopicPrefix, pp.topic...)
		subject, _ := mqttTopicToNATSPubSubject(topic)
		rm.Topic = string(topic)
		rm.Subject = string(subject)

		// will use to save the retained message.
		key = string(subject)

		// Store the retained message with the RETAIN flag set.
		rm.Flags |= mqttPubFlagRetain

		if pp.sz > 0 {
			// Copy the payload out of pp since we will be sending the message
			// asynchronously.
			msg := make([]byte, pp.sz)
			copy(msg, pp.msg[:pp.sz])
			asm.jsa.sendMsg(key, msg)
		}

	} else { // isRetained
		// Spec [MQTT-3.3.1-5]. Store the retained message with its QoS.
		//
		// When coming from a publish protocol, `pp` is referencing a stack
		// variable that itself possibly references the read buffer.
		rm.Topic = string(pp.topic)
	}

	// Set the key to the subject of the message for retained, or the composed
	// $sparkplug subject for sparkB.
	rm.Subject = key
	rmBytes, hdr := mqttEncodeRetainedMessage(rm) // will copy the payload bytes
	_, err := asm.jsa.storeMsg(mqttRetainedMsgsStreamSubject+key, hdr, rmBytes)
	if err != nil {
		c.mu.Lock()
		acc := c.acc
		c.mu.Unlock()
		c.Errorf("unable to store retained message for account %q, subject %q: %v",
			acc.GetName(), key, err)
	}
}

// After a config reload, it is possible that the source of a publish retained
// message is no longer allowed to publish on the given topic. If that is the
// case, the retained message is removed from the map and will no longer be
// sent to (re)starting subscriptions.
//
// Server lock MUST NOT be held on entry.
func (s *Server) mqttCheckPubRetainedPerms() {
	sm := &s.mqtt.sessmgr
	sm.mu.RLock()
	done := len(sm.sessions) == 0
	sm.mu.RUnlock()

	if done {
		return
	}

	s.mu.Lock()
	users := make(map[string]*User, len(s.users))
	for un, u := range s.users {
		users[un] = u
	}
	s.mu.Unlock()

	// First get a list of all of the sessions.
	sm.mu.RLock()
	asms := make([]*mqttAccountSessionManager, 0, len(sm.sessions))
	for _, asm := range sm.sessions {
		asms = append(asms, asm)
	}
	sm.mu.RUnlock()

	// For each session we will obtain a list of retained messages.
	var _rms [128]uint64
	rms := _rms[:0]
	for _, asm := range asms {
		// Get all of the retained messages. Then we will sort them so
		// that they are in sequence order, which should help the file
		// store to not have to load out-of-order blocks so often.
		asm.mu.RLock()
		rms = rms[:0] // reuse slice
		// Copy the sequence out of the tree. The tree entry itself can be
		// updated concurrently by addRetainedMsg() after we release the lock,
		// so keeping a pointer here would race with the later sort.
		asm.retmsgs.IterOrdered(func(_ []byte, rm *mqttRetainedMsgRef) bool {
			rms = append(rms, rm.sseq)
			return true
		})
		jsaID := asm.jsa.id
		asm.mu.RUnlock()
		slices.Sort(rms)

		perms := map[string]*mqttPerm{}
		for _, rf := range rms {
			jsm, err := asm.jsa.loadMsg(mqttRetainedMsgsStreamName, rf)
			if err != nil || jsm == nil {
				continue
			}
			rm, err := mqttDecodeRetainedMessage(jsm.Subject, jsm.Header, jsm.Data)
			if err != nil {
				continue
			}
			// We deal only with messages that have a source (the username that produced
			// this message) and were produced on this server.
			if rm.Source == _EMPTY_ || rm.Origin != jsaID {
				continue
			}
			// Lookup source from global users.
			u := users[rm.Source]
			if u != nil {
				p, ok := perms[rm.Source]
				if !ok {
					p = generatePubPerms(u.Permissions)
					perms[rm.Source] = p // possibly nil
				}
				// If there is permission and no longer allowed to publish in
				// the subject, remove the publish retained message from the map.
				if p != nil && !pubAllowed(p, rm.Subject) {
					u = nil
				}
			}

			// Not present or permissions have changed such that the source can't
			// publish on that subject anymore: delete this retained message.
			if u == nil {
				// Set the payload to empty to notify that we are deleting this
				// retained message. We will send this message async.
				rm.Msg = nil
				rmBytes, hdrLen := mqttEncodeRetainedMessage(rm)
				asm.jsa.storeMsgNoWait(mqttRetainedMsgsStreamSubject+rm.Subject, hdrLen, rmBytes)
			}
		}
	}
}

// Helper to generate only pub permissions from a Permissions object
type mqttPerm struct {
	allow *gsl.SimpleSublist
	deny  *gsl.SimpleSublist
}

func generatePubPerms(perms *Permissions) *mqttPerm {
	// If given permissions is `nil`, then it means that permissions block
	// has been removed (so the user is now allowed to publish on everything)
	// or was never there in the first place. Returning `nil` will let the
	// caller know that there are no permissions to enforce.
	if perms == nil {
		return nil
	}
	var p *mqttPerm
	if perms.Publish.Allow != nil {
		p = &mqttPerm{}
		p.allow = gsl.NewSimpleSublist()
		for _, pubSubject := range perms.Publish.Allow {
			_ = p.allow.Insert(pubSubject, struct{}{})
		}
	}
	if len(perms.Publish.Deny) > 0 {
		if p == nil {
			p = &mqttPerm{}
		}
		p.deny = gsl.NewSimpleSublist()
		for _, pubSubject := range perms.Publish.Deny {
			_ = p.deny.Insert(pubSubject, struct{}{})
		}
	}
	return p
}

// Helper that checks if given `perms` allow to publish on the given `subject`
func pubAllowed(perms *mqttPerm, subject string) bool {
	if perms == nil {
		return true
	}
	allowed := true
	if perms.allow != nil {
		allowed = perms.allow.HasInterest(subject)
	}
	// If we have a deny list and are currently allowed, check that as well.
	if allowed && perms.deny != nil {
		allowed = !perms.deny.HasInterest(subject)
	}
	return allowed
}

func (c *client) mqttEnqueuePubResponse(packetType byte, pi uint16, trace bool) {
	proto := [4]byte{packetType, 0x2, 0, 0}
	proto[2] = byte(pi >> 8)
	proto[3] = byte(pi)

	// Bits 3,2,1 and 0 of the fixed header in the PUBREL Control Packet are
	// reserved and MUST be set to 0,0,1 and 0 respectively. The Server MUST treat
	// any other value as malformed and close the Network Connection [MQTT-3.6.1-1].
	if packetType == mqttPacketPubRel {
		proto[0] |= 0x2
	}

	c.mu.Lock()
	c.enqueueProto(proto[:4])
	c.mu.Unlock()

	if trace {
		name := "(???)"
		switch packetType {
		case mqttPacketPubAck:
			name = "PUBACK"
		case mqttPacketPubRec:
			name = "PUBREC"
		case mqttPacketPubRel:
			name = "PUBREL"
		case mqttPacketPubComp:
			name = "PUBCOMP"
		}
		c.traceOutOp(name, []byte(fmt.Sprintf("pi=%v", pi)))
	}
}

func mqttParsePIPacket(r *mqttReader) (uint16, error) {
	pi, err := r.readUint16("packet identifier")
	if err != nil {
		return 0, err
	}
	if pi == 0 {
		return 0, errMQTTPacketIdentifierIsZero
	}
	return pi, nil
}

// Process a PUBACK (QoS1) or a PUBREC (QoS2) packet, acting as Sender. Set
// isPubRec to false to process as a PUBACK.
//
// Runs from the client's readLoop. No lock held on entry.
func (c *client) mqttProcessPublishReceived(pi uint16, isPubRec bool) (err error) {
	sess := c.mqtt.sess
	if sess == nil {
		return errMQTTInvalidSession
	}

	var jsAckSubject string
	sess.mu.Lock()
	// Must be the same client, and the session must have been setup for QoS2.
	if sess.c != c {
		sess.mu.Unlock()
		return errMQTTInvalidSession
	}
	if isPubRec {
		// Reply to a PI not delivered as QoS2 without storing the PUBREL, so the client can still complete.
		if p, ok := sess.pendingPublish[pi]; !ok || p.qos != 2 {
			sess.mu.Unlock()
			c.mu.Lock()
			trace := c.trace
			c.mu.Unlock()
			c.mqttEnqueuePubResponse(mqttPacketPubRel, pi, trace)
			return nil
		}
		// The JS ACK subject for the PUBREL will be filled in at the delivery
		// attempt.
		sess.trackAsPubRel(pi, _EMPTY_)
	}
	jsAckSubject = sess.untrackPublish(pi)
	sess.mu.Unlock()

	if isPubRec {
		natsMsg, headerLen := mqttNewDeliverablePubRel(pi)
		_, err = sess.jsa.storeMsg(sess.pubRelSubject, headerLen, natsMsg)
		if err != nil {
			// Failure to send out PUBREL will terminate the connection.
			return err
		}
	}

	// Send the ack to JS to remove the pending message from the consumer.
	sess.jsa.sendAck(jsAckSubject)
	return nil
}

func (c *client) mqttProcessPubAck(pi uint16) error {
	return c.mqttProcessPublishReceived(pi, false)
}

func (c *client) mqttProcessPubRec(pi uint16) error {
	return c.mqttProcessPublishReceived(pi, true)
}

// Runs from the client's readLoop. No lock held on entry.
func (c *client) mqttProcessPubComp(pi uint16) {
	sess := c.mqtt.sess
	if sess == nil {
		return
	}

	var jsAckSubject string
	sess.mu.Lock()
	if sess.c != c {
		sess.mu.Unlock()
		return
	}
	jsAckSubject = sess.untrackPubRel(pi)
	sess.mu.Unlock()

	// Send the ack to JS to remove the pending message from the consumer.
	sess.jsa.sendAck(jsAckSubject)
}

// Return the QoS from the given PUBLISH protocol's flags
func mqttGetQoS(flags byte) byte {
	return flags & mqttPubFlagQoS >> 1
}

func mqttIsRetained(flags byte) bool {
	return flags&mqttPubFlagRetain != 0
}

func sparkbParseBirthDeathTopic(topic []byte) (isBirth, isDeath, isCertificate bool) {
	if bytes.HasPrefix(topic, sparkbCertificatesTopicPrefix) {
		isCertificate = true
		topic = topic[len(sparkbCertificatesTopicPrefix):]
	}
	if !bytes.HasPrefix(topic, sparkbNamespaceTopicPrefix) {
		return false, false, false
	}
	topic = topic[len(sparkbNamespaceTopicPrefix):]

	parts := bytes.Split(topic, []byte{'/'})
	if len(parts) < 3 || len(parts) > 4 {
		return false, false, false
	}
	typ := bytesToString(parts[1])
	switch typ {
	case sparkbNBIRTH, sparkbDBIRTH:
		isBirth = true
	case sparkbNDEATH, sparkbDDEATH:
		isDeath = true
	default:
		return false, false, false
	}
	return isBirth, isDeath, isCertificate
}

//////////////////////////////////////////////////////////////////////////////
//
// SUBSCRIBE related functions
//
//////////////////////////////////////////////////////////////////////////////

func (c *client) mqttParseSubs(r *mqttReader, b byte, pl int) (uint16, []*mqttFilter, error) {
	return c.mqttParseSubsOrUnsubs(r, b, pl, true)
}

func (c *client) mqttParseSubsOrUnsubs(r *mqttReader, b byte, pl int, sub bool) (uint16, []*mqttFilter, error) {
	var expectedFlag byte
	var action string
	if sub {
		expectedFlag = mqttSubscribeFlags
	} else {
		expectedFlag = mqttUnsubscribeFlags
		action = "un"
	}
	// Spec [MQTT-3.8.1-1], [MQTT-3.10.1-1]
	if rf := b & 0xf; rf != expectedFlag {
		return 0, nil, fmt.Errorf("wrong %ssubscribe reserved flags: %x", action, rf)
	}
	pi, err := mqttParsePIPacket(r)
	if err != nil {
		return 0, nil, err
	}
	end := r.pos + (pl - 2)
	var filters []*mqttFilter
	for r.pos < end {
		// Don't make a copy now because, this will happen during conversion
		// or when processing the sub.
		topic, err := r.readBytes("topic filter", false)
		if err != nil {
			return 0, nil, err
		}
		if len(topic) == 0 {
			return 0, nil, errMQTTTopicFilterCannotBeEmpty
		}
		// Spec [MQTT-3.8.3-1], [MQTT-3.10.3-1]
		if err := mqttValidateTopic(topic, "topic filter"); err != nil {
			return 0, nil, err
		}
		var qos byte
		// We are going to report if we had an error during the conversion,
		// but we don't fail the parsing. When processing the sub, we will
		// have an error then, and the processing of subs code will send
		// the proper mqttSubAckFailure flag for this given subscription.
		filter, err := mqttFilterToNATSSubject(topic)
		if err != nil {
			c.Errorf("invalid topic %q: %v", topic, err)
		}
		if sub {
			qos, err = r.readByte("QoS")
			if err != nil {
				return 0, nil, err
			}
			// Spec [MQTT-3-8.3-4].
			if qos > 2 {
				return 0, nil, fmt.Errorf("subscribe QoS value must be 0, 1 or 2, got %v", qos)
			}
		}
		f := &mqttFilter{ttopic: topic, filter: string(filter), qos: qos}
		filters = append(filters, f)
	}
	// Spec [MQTT-3.8.3-3], [MQTT-3.10.3-2]
	if len(filters) == 0 {
		return 0, nil, fmt.Errorf("%ssubscribe protocol must contain at least 1 topic filter", action)
	}
	return pi, filters, nil
}

func mqttSubscribeTrace(pi uint16, filters []*mqttFilter) string {
	var sep string
	sb := &strings.Builder{}
	sb.WriteString("[")
	for i, f := range filters {
		sb.WriteString(sep)
		sb.Write(f.ttopic)
		sb.WriteString(" (")
		sb.WriteString(f.filter)
		sb.WriteString(") QoS=")
		sb.WriteString(fmt.Sprintf("%v", f.qos))
		if i == 0 {
			sep = ", "
		}
	}
	sb.WriteString(fmt.Sprintf("] pi=%v", pi))
	return sb.String()
}

// For a MQTT QoS0 subscription, we create a single NATS subscription on the
// actual subject, for instance "foo.bar".
//
// For a MQTT QoS1+ subscription, we create 2 subscriptions, one on "foo.bar"
// (as for QoS0, but sub.mqtt.qos will be 1 or 2), and one on the subject
// "$MQTT.sub.<uid>" which is the delivery subject of the JS durable consumer
// with the filter subject "$MQTT.msgs.foo.bar".
//
// This callback delivers messages to the client as QoS0 messages, either
// because: (a) they have been produced as MQTT QoS0 messages (and therefore
// only this callback can receive them); (b) they are MQTT QoS1+ published
// messages but this callback is for a subscription that is QoS0; or (c) the
// published messages come from (other) NATS publishers on the subject.
//
// This callback must reject a message if it is known to be a QoS1+ published
// message and this is the callback for a QoS1+ subscription because in that
// case, it will be handled by the other callback. This avoid getting duplicate
// deliveries.
func mqttDeliverMsgCbQoS0(sub *subscription, pc *client, _ *Account, subject, reply string, rmsg []byte) {
	if pc.kind == JETSTREAM && len(reply) > 0 && strings.HasPrefix(reply, jsAckPre) {
		return
	}

	// This is the client associated with the subscription.
	cc := sub.client

	// This is immutable
	sess := cc.mqtt.sess

	// Lock here, otherwise we may be called with sub.mqtt == nil. Ignore
	// wildcard subscriptions if this subject starts with '$', per Spec
	// [MQTT-4.7.2-1].
	sess.subsMu.RLock()
	subQoS := sub.mqtt.qos
	ignore := mqttMustIgnoreForReservedSub(sub, subject)
	sess.subsMu.RUnlock()

	if ignore {
		return
	}

	hdr, msg := pc.msgParts(rmsg)
	var topic []byte
	if pc.isMqtt() {
		// This is an MQTT publisher directly connected to this server.

		// Check the subscription's QoS. If the message was published with a
		// QoS>0 and the sub has the QoS>0 then the message will be delivered by
		// mqttDeliverMsgCbQoS12.
		msgQoS := mqttGetQoS(pc.mqtt.pp.flags)
		if subQoS > 0 && msgQoS > 0 {
			return
		}
		topic = pc.mqtt.pp.topic
		// If the subject is different than the one in pp.subject, then some
		// mapping/transform occurred and we need to recreate the topic.
		if subject != bytesToString(pc.mqtt.pp.subject) {
			topic = natsSubjectStrToMQTTTopic(subject)
		}

	} else {
		// Non MQTT client, could be NATS publisher, or ROUTER, etc..
		h := mqttParsePublishNATSHeader(hdr)

		// Check the subscription's QoS. If the message was published with a
		// QoS>0 (in the header) and the sub has the QoS>0 then the message will
		// be delivered by mqttDeliverMsgCbQoS12.
		if subQoS > 0 && h != nil && h.qos > 0 {
			return
		}

		// If size is more than what a MQTT client can handle, we should probably reject,
		// for now just truncate.
		if len(msg) > mqttMaxPayloadSize {
			msg = msg[:mqttMaxPayloadSize]
		}
		topic = natsSubjectStrToMQTTTopic(subject)
	}

	// Message never has a packet identifier nor is marked as duplicate.
	pc.mqttEnqueuePublishMsgTo(cc, sub, 0, 0, false, topic, msg)
}

// This is the callback attached to a JS durable subscription for a MQTT QoS 1+
// sub. Only JETSTREAM should be sending a message to this subject (the delivery
// subject associated with the JS durable consumer), but in cluster mode, this
// can be coming from a route, gw, etc... We make sure that if this is the case,
// the message contains a NATS/MQTT header that indicates that this is a
// published QoS1+ message.
func mqttDeliverMsgCbQoS12(sub *subscription, pc *client, _ *Account, subject, reply string, rmsg []byte) {
	// Message on foo.bar is stored under $MQTT.msgs.foo.bar, so the subject has to be
	// at least as long as the stream subject prefix "$MQTT.msgs.", and after removing
	// the prefix, has to be at least 1 character long.
	if len(subject) < len(mqttStreamSubjectPrefix)+1 {
		return
	}

	hdr, msg := pc.msgParts(rmsg)
	h := mqttParsePublishNATSHeader(hdr)
	if pc.kind != JETSTREAM && (h == nil || h.qos == 0) {
		// MQTT QoS 0 messages must be ignored, they will be delivered by the
		// other callback, the direct NATS subscription. All JETSTREAM messages
		// will have the header.
		return
	}

	// This is the client associated with the subscription.
	cc := sub.client

	// This is immutable
	sess := cc.mqtt.sess

	// We lock to check some of the subscription's fields and if we need to keep
	// track of pending acks, etc. There is no need to acquire the subsMu RLock
	// since sess.Lock is overarching for modifying subscriptions.
	sess.mu.Lock()
	if sess.c != cc || sub.mqtt == nil || sub.mqtt.closed {
		sess.mu.Unlock()
		return
	}

	// In this callback we handle only QoS-published messages to QoS
	// subscriptions. Ignore if either is 0, will be delivered by the other
	// callback, mqttDeliverMsgCbQos1.
	var qos byte
	if h != nil {
		qos = h.qos
	}
	if qos > sub.mqtt.qos {
		qos = sub.mqtt.qos
	}
	if qos == 0 {
		sess.mu.Unlock()
		return
	}

	// Check for reserved subject violation. If so, we will send the ack to
	// remove the message, and do nothing else.
	strippedSubj := subject[len(mqttStreamSubjectPrefix):]
	if mqttMustIgnoreForReservedSub(sub, strippedSubj) {
		sess.mu.Unlock()
		sess.jsa.sendAck(reply)
		return
	}

	// A broad wildcard subscription can overlap a subscribe deny clause.
	cc.mu.Lock()
	denied := cc.mperms != nil && cc.checkDenySub(strippedSubj, bytesToString(sub.queue))
	cc.mu.Unlock()
	if denied {
		sess.mu.Unlock()
		sess.jsa.sendAck(reply)
		return
	}

	pi, dup := sess.trackPublish(sub.mqtt.jsDur, reply, qos)
	sess.mu.Unlock()

	if pi == 0 {
		// We have reached max pending, don't send the message now.
		// JS will cause a redelivery and if by then the number of pending
		// messages has fallen below threshold, the message will be resent.
		return
	}

	originalTopic := natsSubjectStrToMQTTTopic(strippedSubj)
	pc.mqttEnqueuePublishMsgTo(cc, sub, pi, qos, dup, originalTopic, msg)
}

func mqttDeliverPubRelCb(sub *subscription, pc *client, _ *Account, subject, reply string, rmsg []byte) {
	if sub.client.mqtt == nil || sub.client.mqtt.sess == nil || reply == _EMPTY_ {
		return
	}

	hdr, _ := pc.msgParts(rmsg)
	pi := mqttParsePubRelNATSHeader(hdr)
	if pi == 0 {
		return
	}

	// This is the client associated with the subscription.
	cc := sub.client

	// This is immutable
	sess := cc.mqtt.sess

	sess.mu.Lock()
	if sess.c != cc || sess.pubRelConsumer == nil {
		sess.mu.Unlock()
		return
	}
	sess.trackAsPubRel(pi, reply)
	trace := cc.trace
	sess.mu.Unlock()

	cc.mqttEnqueuePubResponse(mqttPacketPubRel, pi, trace)
}

// The MQTT Server MUST NOT match Topic Filters starting with a wildcard
// character (# or +) with Topic Names beginning with a $ character, Spec
// [MQTT-4.7.2-1]. We will return true if there is a violation.
//
// Session or subMu lock must be held on entry to protect access to sub.mqtt.
func mqttMustIgnoreForReservedSub(sub *subscription, subject string) bool {
	// If the subject does not start with $ nothing to do here.
	if !sub.mqtt.reserved || len(subject) == 0 || subject[0] != mqttReservedPre {
		return false
	}
	return true
}

// Check if a sub is a reserved wildcard. E.g. '#', '*', or '*/" prefix.
func isMQTTReservedSubscription(subject string) bool {
	if len(subject) == 1 && (subject[0] == fwc || subject[0] == pwc) {
		return true
	}
	// Match "*.<>"
	if len(subject) > 1 && (subject[0] == pwc && subject[1] == btsep) {
		return true
	}
	return false
}

func sparkbReplaceDeathTimestamp(msg []byte) []byte {
	const VARINT = 0
	const TIMESTAMP = 1

	orig := msg
	buf := bytes.NewBuffer(make([]byte, 0, len(msg)+16)) // 16 bytes should be enough if we need to add a timestamp
	writeDeathTimestamp := func() {
		// [tck-id-conformance-mqtt-aware-ndeath-timestamp] A Sparkplug Aware
		// MQTT Server MAY replace the timestamp of NDEATH messages. If it does,
		// it MUST set the timestamp to the UTC time at which it attempts to
		// deliver the NDEATH to subscribed clients
		//
		// sparkB spec: 6.4.1. Google Protocol Buffer Schema
		//      optional uint64 timestamp = 1; // Timestamp at message sending time
		//
		// SparkplugB timestamps are milliseconds since epoch, represented as
		// uint64 in go, transmitted as protobuf varint.
		ts := uint64(time.Now().UnixMilli())
		buf.Write(protoEncodeVarint(TIMESTAMP<<3 | VARINT))
		buf.Write(protoEncodeVarint(ts))
	}

	for len(msg) > 0 {
		fieldNumericID, fieldType, size, err := protoScanField(msg)
		if err != nil {
			return orig
		}
		if fieldType != VARINT || fieldNumericID != TIMESTAMP {
			// Add the field as is
			buf.Write(msg[:size])
			msg = msg[size:]
			continue
		}

		writeDeathTimestamp()

		// Add the rest of the message as is, we are done
		buf.Write(msg[size:])
		return buf.Bytes()
	}

	// Add timestamp if we did not find one.
	writeDeathTimestamp()

	return buf.Bytes()
}

// Common function to mqtt delivery callbacks to serialize and send the message
// to the `cc` client.
func (c *client) mqttEnqueuePublishMsgTo(cc *client, sub *subscription, pi uint16, qos byte, dup bool, topic, msg []byte) {
	// [tck-id-conformance-mqtt-aware-nbirth-mqtt-retain] A Sparkplug Aware
	// MQTT Server MUST make NBIRTH messages available on the topic:
	// $sparkplug/certificates/namespace/group_id/NBIRTH/edge_node_id with
	// the MQTT retain flag set to true
	//
	// [tck-id-conformance-mqtt-aware-dbirth-mqtt-retain] A Sparkplug Aware
	// MQTT Server MUST make DBIRTH messages available on the topic:
	// $sparkplug/certificates/namespace/group_id/DBIRTH/edge_node_id/device_id
	// with the MQTT retain flag set to true
	//
	// $sparkplug/certificates messages are sent as NATS messages, so we
	// need to add the retain flag when sending them to MQTT clients.

	retain := false
	isBirth, isDeath, isCertificate := sparkbParseBirthDeathTopic(topic)
	if isBirth && qos == 0 {
		retain = isCertificate
	} else if isDeath && !isCertificate {
		msg = sparkbReplaceDeathTimestamp(msg)
	}

	flags, headerBytes := mqttMakePublishHeader(pi, qos, dup, retain, topic, len(msg))

	cc.mu.Lock()
	if sub.mqtt.prm != nil {
		for _, data := range sub.mqtt.prm {
			cc.queueOutbound(data)
		}
		sub.mqtt.prm = nil
	}
	cc.queueOutbound(headerBytes)
	cc.queueOutbound(msg)
	c.addToPCD(cc)
	trace := cc.trace
	cc.mu.Unlock()

	if trace {
		pp := mqttPublish{
			topic: topic,
			flags: flags,
			pi:    pi,
			sz:    len(msg),
		}
		cc.traceOutOp("PUBLISH", []byte(mqttPubTrace(&pp)))
	}
}

// Serializes to the given writer the message for the given subject.
func (w *mqttWriter) WritePublishHeader(pi uint16, qos byte, dup, retained bool, topic []byte, msgLen int) byte {
	// Compute len (will have to add packet id if message is sent as QoS>=1)
	pkLen := 2 + len(topic) + msgLen
	var flags byte

	// Set flags for dup/retained/qos1
	if dup {
		flags |= mqttPubFlagDup
	}
	if retained {
		flags |= mqttPubFlagRetain
	}
	if qos > 0 {
		pkLen += 2
		flags |= qos << 1
	}

	w.WriteByte(mqttPacketPub | flags)
	w.WriteVarInt(pkLen)
	w.WriteBytes(topic)
	if qos > 0 {
		w.WriteUint16(pi)
	}

	return flags
}

// Serializes to the given writer the message for the given subject.
func mqttMakePublishHeader(pi uint16, qos byte, dup, retained bool, topic []byte, msgLen int) (byte, []byte) {
	headerBuf := newMQTTWriter(mqttInitialPubHeader + len(topic))
	flags := headerBuf.WritePublishHeader(pi, qos, dup, retained, topic, msgLen)
	return flags, headerBuf.Bytes()
}

// Process the SUBSCRIBE packet.
//
// Process the list of subscriptions and update the given filter
// with the QoS that has been accepted (or failure).
//
// Spec [MQTT-3.8.4-3] says that if an exact same subscription is
// found, it needs to be replaced with the new one (possibly updating
// the qos) and that the flow of publications must not be interrupted,
// which I read as the replacement cannot be a "remove then add" if there
// is a chance that in between the 2 actions, published messages
// would be "lost" because there would not be any matching subscription.
//
// Run from client's readLoop.
// No lock held on entry.
func (c *client) mqttProcessSubs(filters []*mqttFilter) ([]*subscription, error) {
	// Those things are immutable, but since processing subs is not
	// really in the fast path, let's get them under the client lock.
	c.mu.Lock()
	asm := c.mqtt.asm
	sess := c.mqtt.sess
	trace := c.trace
	c.mu.Unlock()

	if err := asm.lockSession(sess, c); err != nil {
		return nil, err
	}
	defer asm.unlockSession(sess)
	return asm.processSubs(sess, c, filters, true, trace)
}

// Cleanup that is performed in processSubs if there was an error.
//
// Runs from client's readLoop.
// Lock not held on entry, but session is in the locked map.
func (sess *mqttSession) cleanupFailedSub(c *client, sub *subscription, cc *ConsumerConfig, jssub *subscription) {
	if sub != nil {
		c.processUnsub(sub.sid)
	}
	if jssub != nil {
		c.processUnsub(jssub.sid)
	}
	if cc != nil {
		sess.deleteConsumer(cc)
	}
}

// Make sure we are set up to deliver PUBREL messages to this QoS2-subscribed
// session.
func (sess *mqttSession) ensurePubRelConsumerSubscription(c *client) error {

	sess.mu.Lock()
	pubRelSubscribed := sess.pubRelSubscribed
	pubRelDeliverySubjectB := sess.pubRelDeliverySubjectB
	pubRelDeliverySubject := sess.pubRelDeliverySubject
	pubRelConsumer := sess.pubRelConsumer
	sess.mu.Unlock()

	// Subscribe before the consumer is created so we don't loose any messages.
	if !pubRelSubscribed {
		_, err := c.processSub(pubRelDeliverySubjectB, nil, pubRelDeliverySubjectB, mqttDeliverPubRelCb, false)
		if err != nil {
			c.Errorf("Unable to create subscription for JetStream consumer on %q: %v", pubRelDeliverySubject, err)
			return err
		}
		sess.mu.Lock()
		sess.pubRelSubscribed = true
		sess.mu.Unlock()
	}

	// If the JS consumer already exists, we are done.
	if pubRelConsumer != nil {
		return nil
	}

	opts := c.srv.getOpts()
	ackWait := opts.MQTT.AckWait
	if ackWait == 0 {
		ackWait = mqttDefaultAckWait
	}
	maxAckPending := int(opts.MQTT.MaxAckPending)
	if maxAckPending == 0 {
		maxAckPending = mqttDefaultMaxAckPending
	}

	sess.mu.Lock()
	pubRelSubject := sess.pubRelSubject
	tmaxack := sess.tmaxack
	idHash := sess.idHash
	id := sess.id
	sess.mu.Unlock()

	// Check that the limit of subs' maxAckPending are not going over the limit
	if after := tmaxack + maxAckPending; after > mqttMaxAckTotalLimit {
		return fmt.Errorf("max_ack_pending for all consumers would be %v which exceeds the limit of %v",
			after, mqttMaxAckTotalLimit)
	}

	ccr := &CreateConsumerRequest{
		Stream: mqttOutStreamName,
		Config: ConsumerConfig{
			DeliverSubject: pubRelDeliverySubject,
			Durable:        mqttPubRelConsumerDurablePrefix + idHash,
			AckPolicy:      AckExplicit,
			DeliverPolicy:  DeliverNew,
			FilterSubject:  pubRelSubject,
			AckWait:        ackWait,
			MaxAckPending:  maxAckPending,
			MemoryStorage:  opts.MQTT.ConsumerMemoryStorage,
		},
	}
	if opts.MQTT.ConsumerInactiveThreshold > 0 {
		ccr.Config.InactiveThreshold = opts.MQTT.ConsumerInactiveThreshold
	}
	if _, err := sess.jsa.createDurableConsumer(ccr); err != nil {
		c.Errorf("Unable to add JetStream consumer for PUBREL for client %q: err=%v", id, err)
		return err
	}
	pubRelConsumer = &ccr.Config
	tmaxack += maxAckPending

	sess.mu.Lock()
	sess.pubRelConsumer = pubRelConsumer
	sess.tmaxack = tmaxack
	sess.mu.Unlock()

	return nil
}

// When invoked with a QoS of 0, looks for an existing JS durable consumer for
// the given sid and if one is found, delete the JS durable consumer and unsub
// the NATS subscription on the delivery subject.
//
// With a QoS > 0, creates or update the existing JS durable consumer along with
// its NATS subscription on a delivery subject.
//
// Session lock is acquired and released as needed. Session is in the locked
// map.
func (sess *mqttSession) processJSConsumer(c *client, subject, sid string,
	qos byte, fromSubProto bool) (*ConsumerConfig, *subscription, error) {

	sess.mu.Lock()
	cc, exists := sess.cons[sid]
	tmaxack := sess.tmaxack
	idHash := sess.idHash
	sess.mu.Unlock()

	// Check if we are already a JS consumer for this SID.
	if exists {
		// If current QoS is 0, it means that we need to delete the existing
		// one (that was QoS > 0)
		if qos == 0 {
			// The JS durable consumer's delivery subject is on a NUID of
			// the form: mqttSubPrefix + <nuid>. It is also used as the sid
			// for the NATS subscription, so use that for the lookup.
			c.mu.Lock()
			sub := c.subs[cc.DeliverSubject]
			c.mu.Unlock()

			// Delete the consumer entry, mark its delivery subscription closed,
			// and purge its pending QoS 1/2 deliveries — all under the session
			// lock. Otherwise those packet identifiers leak and count against the
			// in-flight cap for the life of the session (as mqttProcessUnsubs
			// purges on unsubscribe). deleteConsumer is asynchronous, so an
			// in-flight delivery callback could re-populate the maps after the
			// purge; marking sub.mqtt.closed (guarded by sess.mu and sess.subsMu,
			// same as delivery) makes mqttDeliverMsgCbQoS12 skip instead. The flag
			// leaves sub.mqtt valid so an already-committed enqueue does not panic.
			sess.mu.Lock()
			delete(sess.cons, sid)
			if sub != nil && sub.mqtt != nil {
				sess.subsMu.Lock()
				sub.mqtt.closed = true
				sess.subsMu.Unlock()
			}
			// Also purge this sid's in-flight retained deliveries, tracked
			// under a pseudo-durable key (no JS consumer, no redelivery).
			for _, dur := range [...]string{cc.Durable, mqttRetainedPendingDur(sid)} {
				if seqPis, ok := sess.cpending[dur]; ok {
					delete(sess.cpending, dur)
					for _, pi := range seqPis {
						delete(sess.pendingPublish, pi)
					}
				}
			}
			sess.mu.Unlock()

			sess.deleteConsumer(cc)
			if sub != nil {
				c.processUnsub(sub.sid)
			}
			return nil, nil, nil
		}
		// If this is called when processing SUBSCRIBE protocol, then if
		// the JS consumer already exists, we are done (it was created
		// during the processing of CONNECT).
		if fromSubProto {
			return nil, nil, nil
		}
	}
	// Here it means we don't have a JS consumer and if we are QoS 0,
	// we have nothing to do.
	if qos == 0 {
		return nil, nil, nil
	}
	var err error
	var inbox string
	if exists {
		inbox = cc.DeliverSubject
	} else {
		inbox = mqttSubPrefix + nuid.Next()
		opts := c.srv.getOpts()
		ackWait := opts.MQTT.AckWait
		if ackWait == 0 {
			ackWait = mqttDefaultAckWait
		}
		maxAckPending := int(opts.MQTT.MaxAckPending)
		if maxAckPending == 0 {
			maxAckPending = mqttDefaultMaxAckPending
		}

		// Check that the limit of subs' maxAckPending are not going over the limit
		if after := tmaxack + maxAckPending; after > mqttMaxAckTotalLimit {
			return nil, nil, fmt.Errorf("max_ack_pending for all consumers would be %v which exceeds the limit of %v",
				after, mqttMaxAckTotalLimit)
		}

		durName := idHash + "_" + nuid.Next()
		ccr := &CreateConsumerRequest{
			Stream: mqttStreamName,
			Config: ConsumerConfig{
				DeliverSubject: inbox,
				Durable:        durName,
				AckPolicy:      AckExplicit,
				DeliverPolicy:  DeliverNew,
				FilterSubject:  mqttStreamSubjectPrefix + subject,
				AckWait:        ackWait,
				MaxAckPending:  maxAckPending,
				MemoryStorage:  opts.MQTT.ConsumerMemoryStorage,
			},
		}
		if opts.MQTT.ConsumerInactiveThreshold > 0 {
			ccr.Config.InactiveThreshold = opts.MQTT.ConsumerInactiveThreshold
		}
		if _, err := sess.jsa.createDurableConsumer(ccr); err != nil {
			c.Errorf("Unable to add JetStream consumer for subscription on %q: err=%v", subject, err)
			return nil, nil, err
		}
		cc = &ccr.Config
		tmaxack += maxAckPending
	}

	// This is an internal subscription on subject like "$MQTT.sub.<nuid>" that is setup
	// for the JS durable's deliver subject.
	sess.mu.Lock()
	sess.tmaxack = tmaxack
	sub, err := sess.processQOS12Sub(c, []byte(inbox), []byte(inbox),
		isMQTTReservedSubscription(subject), qos, cc.Durable, mqttDeliverMsgCbQoS12)
	sess.mu.Unlock()

	if err != nil {
		sess.deleteConsumer(cc)
		c.Errorf("Unable to create subscription for JetStream consumer on %q: %v", subject, err)
		return nil, nil, err
	}
	return cc, sub, nil
}

// Queues the published retained messages for each subscription and signals
// the writeLoop.
func (c *client) mqttSendRetainedMsgsToNewSubs(subs []*subscription) {
	c.mu.Lock()
	for _, sub := range subs {
		if sub.mqtt != nil && sub.mqtt.prm != nil {
			for _, data := range sub.mqtt.prm {
				c.queueOutbound(data)
			}
			sub.mqtt.prm = nil
		}
	}
	c.flushSignal()
	c.mu.Unlock()
}

func (c *client) mqttEnqueueSubAck(pi uint16, filters []*mqttFilter) {
	w := newMQTTWriter(7 + len(filters))
	w.WriteByte(mqttPacketSubAck)
	// packet length is 2 (for packet identifier) and 1 byte per filter.
	w.WriteVarInt(2 + len(filters))
	w.WriteUint16(pi)
	for _, f := range filters {
		w.WriteByte(f.qos)
	}
	c.mu.Lock()
	c.enqueueProto(w.Bytes())
	c.mu.Unlock()
}

//////////////////////////////////////////////////////////////////////////////
//
// UNSUBSCRIBE related functions
//
//////////////////////////////////////////////////////////////////////////////

func (c *client) mqttParseUnsubs(r *mqttReader, b byte, pl int) (uint16, []*mqttFilter, error) {
	return c.mqttParseSubsOrUnsubs(r, b, pl, false)
}

// Process the UNSUBSCRIBE packet.
//
// Given the list of topics, this is going to unsubscribe the low level NATS subscriptions
// and delete the JS durable consumers when applicable.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (c *client) mqttProcessUnsubs(filters []*mqttFilter) error {
	// Those things are immutable, but since processing unsubs is not
	// really in the fast path, let's get them under the client lock.
	c.mu.Lock()
	asm := c.mqtt.asm
	sess := c.mqtt.sess
	c.mu.Unlock()

	if err := asm.lockSession(sess, c); err != nil {
		return err
	}
	defer asm.unlockSession(sess)

	removeJSCons := func(sid string) {
		cc, ok := sess.cons[sid]
		if ok {
			delete(sess.cons, sid)
			sess.deleteConsumer(cc)

			c.mu.Lock()
			sub := c.subs[cc.DeliverSubject]
			c.mu.Unlock()

			// Need lock here since these are accessed by callbacks
			sess.mu.Lock()
			// Mark the delivery sub closed so an in-flight QoS 1/2 callback stops
			// tracking new messages after the purge (deleteConsumer is async);
			// same barrier as the QoS 0 downgrade path in processJSConsumer.
			if sub != nil && sub.mqtt != nil {
				sess.subsMu.Lock()
				sub.mqtt.closed = true
				sess.subsMu.Unlock()
			}
			// Purge both the consumer's deliveries and this sid's in-flight
			// retained deliveries (tracked under a pseudo-durable key).
			for _, dur := range [...]string{cc.Durable, mqttRetainedPendingDur(sid)} {
				if seqPis, ok := sess.cpending[dur]; ok {
					delete(sess.cpending, dur)
					for _, pi := range seqPis {
						delete(sess.pendingPublish, pi)
					}
					// last_pi stays monotonic (see untrackPublish); do not reset here.
				}
			}
			sess.mu.Unlock()
		}
	}
	for _, f := range filters {
		sid := f.filter
		// Remove JS Consumer if one exists for this sid
		removeJSCons(sid)
		if err := c.processUnsub([]byte(sid)); err != nil {
			c.Errorf("error unsubscribing from %q: %v", sid, err)
		}
		if mqttNeedSubForLevelUp(sid) {
			subject := sid[:len(sid)-2]
			sid = subject + mqttMultiLevelSidSuffix
			removeJSCons(sid)
			if err := c.processUnsub([]byte(sid)); err != nil {
				c.Errorf("error unsubscribing from %q: %v", subject, err)
			}
		}
	}
	return sess.update(filters, false)
}

func (c *client) mqttEnqueueUnsubAck(pi uint16) {
	w := newMQTTWriter(4)
	w.WriteByte(mqttPacketUnsubAck)
	w.WriteVarInt(2)
	w.WriteUint16(pi)
	c.mu.Lock()
	c.enqueueProto(w.Bytes())
	c.mu.Unlock()
}

func mqttUnsubscribeTrace(pi uint16, filters []*mqttFilter) string {
	var sep string
	sb := strings.Builder{}
	sb.WriteString("[")
	for i, f := range filters {
		sb.WriteString(sep)
		sb.Write(f.ttopic)
		sb.WriteString(" (")
		sb.WriteString(f.filter)
		sb.WriteString(")")
		if i == 0 {
			sep = ", "
		}
	}
	sb.WriteString(fmt.Sprintf("] pi=%v", pi))
	return sb.String()
}

//////////////////////////////////////////////////////////////////////////////
//
// PINGREQ/PINGRESP related functions
//
//////////////////////////////////////////////////////////////////////////////

func (c *client) mqttEnqueuePingResp() {
	c.mu.Lock()
	c.enqueueProto(mqttPingResponse)
	c.mu.Unlock()
}

//////////////////////////////////////////////////////////////////////////////
//
// Trace functions
//
//////////////////////////////////////////////////////////////////////////////

func errOrTrace(err error, trace string) []byte {
	if err != nil {
		return []byte(err.Error())
	}
	return []byte(trace)
}

//////////////////////////////////////////////////////////////////////////////
//
// Subject/Topic conversion functions
//
//////////////////////////////////////////////////////////////////////////////

// Converts an MQTT Topic Name to a NATS Subject (used by PUBLISH)
// See mqttToNATSSubjectConversion() for details.
func mqttTopicToNATSPubSubject(mt []byte) ([]byte, error) {
	return mqttToNATSSubjectConversion(mt, false)
}

// Converts an MQTT Topic Filter to a NATS Subject (used by SUBSCRIBE)
// See mqttToNATSSubjectConversion() for details.
func mqttFilterToNATSSubject(filter []byte) ([]byte, error) {
	return mqttToNATSSubjectConversion(filter, true)
}

// Converts an MQTT Topic Name or Filter to a NATS Subject.
// In MQTT:
// - a Topic Name does not have wildcard (PUBLISH uses only topic names).
// - a Topic Filter can include wildcards (SUBSCRIBE uses those).
// - '+' and '#' are wildcard characters (single and multiple levels respectively)
// - '/' is the topic level separator.
//
// Conversion that occurs:
//   - '/' is replaced with '/.' if it is the first character in mt
//   - '/' is replaced with './' if the last or next character in mt is '/'
//     For instance, foo//bar would become foo./.bar
//   - '/' is replaced with '.' for all other conditions (foo/bar -> foo.bar)
//   - '.' is replaced with '//'.
//   - ' ' cause an error to be returned.
//
// If there is no need to convert anything (say "foo" remains "foo"), then
// the no memory is allocated and the returned slice is the original `mt`.
func mqttToNATSSubjectConversion(mt []byte, wcOk bool) ([]byte, error) {
	var cp bool
	var j int
	res := mt

	makeCopy := func(i int) {
		cp = true
		res = make([]byte, 0, len(mt)+10)
		if i > 0 {
			res = append(res, mt[:i]...)
		}
	}

	end := len(mt) - 1
	for i := 0; i < len(mt); i++ {
		switch mt[i] {
		case mqttTopicLevelSep:
			if i == 0 || res[j-1] == btsep {
				if !cp {
					makeCopy(0)
				}
				res = append(res, mqttTopicLevelSep, btsep)
				j++
			} else if i == end || mt[i+1] == mqttTopicLevelSep {
				if !cp {
					makeCopy(i)
				}
				res = append(res, btsep, mqttTopicLevelSep)
				j++
			} else {
				if !cp {
					makeCopy(i)
				}
				res = append(res, btsep)
			}
		case ' ', '\t', '\n', '\r', '\f':
			// We cannot support whitespace in the MQTT topic/filter — these
			// characters would also corrupt the NATS wire protocol when the
			// subject is forwarded to other connection types (e.g. leaf
			// nodes) where the resulting control line could be split.
			return nil, errMQTTUnsupportedCharacters
		case 0x7f:
			// SubjectTree uses DEL as an internal pivot marker, so retained
			// subjects containing it cannot be indexed safely, including
			// legacy retained messages recovered from the retained-message
			// stream.
			return nil, errMQTTUnsupportedCharacters
		case btsep:
			if !cp {
				makeCopy(i)
			}
			res = append(res, mqttTopicLevelSep, mqttTopicLevelSep)
			j++
		case mqttSingleLevelWC, mqttMultiLevelWC:
			if !wcOk {
				// Spec [MQTT-3.3.2-2] and [MQTT-4.7.1-1]
				// The wildcard characters can be used in Topic Filters, but MUST NOT be used within a Topic Name
				return nil, fmt.Errorf("wildcards not allowed in publish's topic: %q", mt)
			}
			if !cp {
				makeCopy(i)
			}
			if mt[i] == mqttSingleLevelWC {
				res = append(res, pwc)
			} else {
				res = append(res, fwc)
			}
		default:
			if cp {
				res = append(res, mt[i])
			}
		}
		j++
	}
	if cp && res[j-1] == btsep {
		res = append(res, mqttTopicLevelSep)
		j++
	}
	return res[:j], nil
}

// Converts a NATS subject to MQTT topic. This is for publish
// messages only, so there is no checking for wildcards.
// Rules are reversed of mqttToNATSSubjectConversion.
func natsSubjectStrToMQTTTopic(subject string) []byte {
	return natsSubjectToMQTTTopic(stringToBytes(subject))
}

func natsSubjectToMQTTTopic(subject []byte) []byte {
	topic := make([]byte, len(subject))
	end := len(subject) - 1
	var j int
	for i := 0; i < len(subject); i++ {
		switch subject[i] {
		case mqttTopicLevelSep:
			if i < end {
				switch c := subject[i+1]; c {
				case btsep, mqttTopicLevelSep:
					if c == btsep {
						topic[j] = mqttTopicLevelSep
					} else {
						topic[j] = btsep
					}
					j++
					i++
				default:
				}
			}
		case btsep:
			topic[j] = mqttTopicLevelSep
			j++
		default:
			topic[j] = subject[i]
			j++
		}
	}
	return topic[:j]
}

// Returns true if the subject has more than 1 token and ends with ".>"
func mqttNeedSubForLevelUp(subject string) bool {
	if len(subject) < 3 {
		return false
	}
	end := len(subject)
	if subject[end-2] == '.' && subject[end-1] == fwc {
		return true
	}
	return false
}

//////////////////////////////////////////////////////////////////////////////
//
// MQTT Reader functions
//
//////////////////////////////////////////////////////////////////////////////

func (r *mqttReader) reset(buf []byte) {
	r.owned = len(r.pbuf) > 0
	if r.owned {
		// Append to the partial packet so a trickled packet isn't copied in full on every read.
		buf = append(r.pbuf, buf...)
		r.pbuf = nil
	}
	r.buf = buf
	r.pos = 0
	r.pstart = 0
}

func (r *mqttReader) hasMore() bool {
	return r.pos != len(r.buf)
}

func (r *mqttReader) readByte(field string) (byte, error) {
	if r.pos == len(r.buf) {
		return 0, fmt.Errorf("error reading %s: %v", field, io.EOF)
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *mqttReader) readPacketLen(pt byte, maxLen int32) (int, bool, error) {
	v, complete, err := r.readVarInt()
	if err != nil {
		return 0, false, err
	}
	if complete {
		// Reject invalid lengths before buffering a partial packet.
		if err = mqttCheckRemainingLength(pt, v); err != nil {
			return 0, false, err
		}
		packetEnd := r.pos + v
		packetLen := packetEnd - r.pstart
		if maxLen != jwt.NoLimit && int64(packetLen) > int64(maxLen) {
			return packetLen, false, ErrMaxPayload
		}
		if packetEnd <= len(r.buf) {
			return v, true, nil
		}
	}
	// Reuse our own buffer if it still holds only this partial packet.
	if r.owned && r.pstart == 0 {
		r.pbuf = r.buf
	} else {
		r.pbuf = copyBytes(r.buf[r.pstart:])
	}
	return 0, false, nil
}

func (r *mqttReader) readVarInt() (int, bool, error) {
	m := 1
	v := 0
	for {
		var b byte
		if r.pos != len(r.buf) {
			b = r.buf[r.pos]
			r.pos++
		} else {
			break
		}
		v += int(b&0x7f) * m
		if (b & 0x80) == 0 {
			return v, true, nil
		}
		m *= 0x80
		if m > 0x200000 {
			return 0, false, errMQTTMalformedVarInt
		}
	}
	return 0, false, nil
}

func (r *mqttReader) readString(field string) (string, error) {
	var s string
	bs, err := r.readBytes(field, false)
	if err == nil {
		s = string(bs)
	}
	return s, err
}

func (r *mqttReader) readBytes(field string, cp bool) ([]byte, error) {
	luint, err := r.readUint16(field)
	if err != nil {
		return nil, err
	}
	l := int(luint)
	if l == 0 {
		return nil, nil
	}
	start := r.pos
	if start+l > len(r.buf) {
		return nil, fmt.Errorf("error reading %s: %v", field, io.ErrUnexpectedEOF)
	}
	r.pos += l
	b := r.buf[start:r.pos]
	if cp {
		b = copyBytes(b)
	}
	return b, nil
}

func (r *mqttReader) readUint16(field string) (uint16, error) {
	if len(r.buf)-r.pos < 2 {
		return 0, fmt.Errorf("error reading %s: %v", field, io.ErrUnexpectedEOF)
	}
	start := r.pos
	r.pos += 2
	return binary.BigEndian.Uint16(r.buf[start:r.pos]), nil
}

//////////////////////////////////////////////////////////////////////////////
//
// MQTT Writer functions
//
//////////////////////////////////////////////////////////////////////////////

func (w *mqttWriter) WriteUint16(i uint16) {
	w.WriteByte(byte(i >> 8))
	w.WriteByte(byte(i))
}

func (w *mqttWriter) WriteString(s string) {
	w.WriteBytes([]byte(s))
}

func (w *mqttWriter) WriteBytes(bs []byte) {
	w.WriteUint16(uint16(len(bs)))
	w.Write(bs)
}

func (w *mqttWriter) WriteVarInt(value int) {
	for {
		b := byte(value & 0x7f)
		value >>= 7
		if value > 0 {
			b |= 0x80
		}
		w.WriteByte(b)
		if value == 0 {
			break
		}
	}
}

func newMQTTWriter(cap int) *mqttWriter {
	w := &mqttWriter{}
	w.Grow(cap)
	return w
}
