// Copyright 2019-2026 The NATS Authors
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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats-server/v2/server/avl"
	"github.com/nats-io/nats-server/v2/server/gsl"
	"github.com/nats-io/nuid"
	"golang.org/x/time/rate"
)

// Headers sent with Request Timeout
const (
	JSPullRequestPendingMsgs  = "Nats-Pending-Messages"
	JSPullRequestPendingBytes = "Nats-Pending-Bytes"
	JSPullRequestNatsPinId    = "Nats-Pin-Id"
)

var (
	validGroupName = regexp.MustCompile(`^[a-zA-Z0-9/_=-]{1,16}$`)
)

// Headers sent when batch size was completed, but there were remaining bytes.
const JsPullRequestRemainingBytesT = "NATS/1.0 409 Batch Completed\r\n%s: %d\r\n%s: %d\r\n\r\n"

type ConsumerInfo struct {
	Stream         string          `json:"stream_name"`
	Name           string          `json:"name"`
	Created        time.Time       `json:"created"`
	Config         *ConsumerConfig `json:"config,omitempty"`
	Delivered      SequenceInfo    `json:"delivered"`
	AckFloor       SequenceInfo    `json:"ack_floor"`
	NumAckPending  int             `json:"num_ack_pending"`
	NumRedelivered int             `json:"num_redelivered"`
	NumWaiting     int             `json:"num_waiting"`
	NumPending     uint64          `json:"num_pending"`
	Cluster        *ClusterInfo    `json:"cluster,omitempty"`
	PushBound      bool            `json:"push_bound,omitempty"`
	Paused         bool            `json:"paused,omitempty"`
	PauseRemaining time.Duration   `json:"pause_remaining,omitempty"`
	// TimeStamp indicates when the info was gathered
	TimeStamp      time.Time            `json:"ts"`
	PriorityGroups []PriorityGroupState `json:"priority_groups,omitempty"`
}

// consumerInfoClusterResponse is a response used in a cluster to communicate the consumer info
// back to the meta leader as part of a consumer list request.
type consumerInfoClusterResponse struct {
	ConsumerInfo
	OfflineReason string `json:"offline_reason,omitempty"` // Reporting when a consumer is offline.
}

type PriorityGroupState struct {
	Group          string    `json:"group"`
	PinnedClientID string    `json:"pinned_client_id,omitempty"`
	PinnedTS       time.Time `json:"pinned_ts,omitempty"`
}

type ConsumerConfig struct {
	Durable         string          `json:"durable_name,omitempty"`
	Name            string          `json:"name,omitempty"`
	Description     string          `json:"description,omitempty"`
	DeliverPolicy   DeliverPolicy   `json:"deliver_policy"`
	OptStartSeq     uint64          `json:"opt_start_seq,omitempty"`
	OptStartTime    *time.Time      `json:"opt_start_time,omitempty"`
	AckPolicy       AckPolicy       `json:"ack_policy"`
	AckWait         time.Duration   `json:"ack_wait,omitempty"`
	MaxDeliver      int             `json:"max_deliver,omitempty"`
	BackOff         []time.Duration `json:"backoff,omitempty"`
	FilterSubject   string          `json:"filter_subject,omitempty"`
	FilterSubjects  []string        `json:"filter_subjects,omitempty"`
	ReplayPolicy    ReplayPolicy    `json:"replay_policy"`
	RateLimit       uint64          `json:"rate_limit_bps,omitempty"` // Bits per sec
	SampleFrequency string          `json:"sample_freq,omitempty"`
	MaxWaiting      int             `json:"max_waiting,omitempty"`
	MaxAckPending   int             `json:"max_ack_pending,omitempty"`
	FlowControl     bool            `json:"flow_control,omitempty"`
	HeadersOnly     bool            `json:"headers_only,omitempty"`

	// Pull based options.
	MaxRequestBatch    int           `json:"max_batch,omitempty"`
	MaxRequestExpires  time.Duration `json:"max_expires,omitempty"`
	MaxRequestMaxBytes int           `json:"max_bytes,omitempty"`

	// Push based consumers.
	DeliverSubject string        `json:"deliver_subject,omitempty"`
	DeliverGroup   string        `json:"deliver_group,omitempty"`
	Heartbeat      time.Duration `json:"idle_heartbeat,omitempty"`

	// Ephemeral inactivity threshold.
	InactiveThreshold time.Duration `json:"inactive_threshold,omitempty"`

	// Generally inherited by parent stream and other markers, now can be configured directly.
	Replicas int `json:"num_replicas"`
	// Force memory storage.
	MemoryStorage bool `json:"mem_storage,omitempty"`

	// Don't add to general clients.
	Direct   bool `json:"direct,omitempty"`
	Sourcing bool `json:"sourcing,omitempty"`

	// Metadata is additional metadata for the Consumer.
	Metadata map[string]string `json:"metadata,omitempty"`

	// PauseUntil is for suspending the consumer until the deadline.
	PauseUntil *time.Time `json:"pause_until,omitempty"`

	// Priority groups
	PriorityGroups []string       `json:"priority_groups,omitempty"`
	PriorityPolicy PriorityPolicy `json:"priority_policy,omitempty"`
	PinnedTTL      time.Duration  `json:"priority_timeout,omitempty"`
}

// clone performs a deep copy of the ConsumerConfig struct, returning a new clone with
// all values copied.
func (cfg *ConsumerConfig) clone() *ConsumerConfig {
	clone := *cfg
	if cfg.BackOff != nil {
		clone.BackOff = slices.Clone(cfg.BackOff)
	}
	if cfg.FilterSubjects != nil {
		clone.FilterSubjects = slices.Clone(cfg.FilterSubjects)
	}
	if cfg.Metadata != nil {
		clone.Metadata = maps.Clone(cfg.Metadata)
	}
	if cfg.PriorityGroups != nil {
		clone.PriorityGroups = slices.Clone(cfg.PriorityGroups)
	}
	return &clone
}

// SequenceInfo has both the consumer and the stream sequence and last activity.
type SequenceInfo struct {
	Consumer uint64     `json:"consumer_seq"`
	Stream   uint64     `json:"stream_seq"`
	Last     *time.Time `json:"last_active,omitempty"`
}

type CreateConsumerRequest struct {
	Stream   string         `json:"stream_name"`
	Config   ConsumerConfig `json:"config"`
	Action   ConsumerAction `json:"action"`
	Pedantic bool           `json:"pedantic,omitempty"`
}

type ConsumerAction int

const (
	ActionCreateOrUpdate ConsumerAction = iota
	ActionUpdate
	ActionCreate
)

const (
	actionUpdateJSONString         = `"update"`
	actionCreateJSONString         = `"create"`
	actionCreateOrUpdateJSONString = `""`
)

var (
	actionUpdateJSONBytes         = []byte(actionUpdateJSONString)
	actionCreateJSONBytes         = []byte(actionCreateJSONString)
	actionCreateOrUpdateJSONBytes = []byte(actionCreateOrUpdateJSONString)
)

func (a ConsumerAction) String() string {
	switch a {
	case ActionCreateOrUpdate:
		return actionCreateOrUpdateJSONString
	case ActionCreate:
		return actionCreateJSONString
	case ActionUpdate:
		return actionUpdateJSONString
	}
	return actionCreateOrUpdateJSONString
}

func (a ConsumerAction) MarshalJSON() ([]byte, error) {
	switch a {
	case ActionCreate:
		return actionCreateJSONBytes, nil
	case ActionUpdate:
		return actionUpdateJSONBytes, nil
	case ActionCreateOrUpdate:
		return actionCreateOrUpdateJSONBytes, nil
	default:
		return nil, fmt.Errorf("can not marshal %v", a)
	}
}

func (a *ConsumerAction) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case actionCreateJSONString:
		*a = ActionCreate
	case actionUpdateJSONString:
		*a = ActionUpdate
	case actionCreateOrUpdateJSONString:
		*a = ActionCreateOrUpdate
	default:
		return fmt.Errorf("unknown consumer action: %v", string(data))
	}
	return nil
}

// ConsumerNakOptions is for optional NAK values, e.g. delay.
type ConsumerNakOptions struct {
	Delay time.Duration `json:"delay"`
}

// PriorityPolicy determines policy for selecting messages based on priority.
type PriorityPolicy int

const (
	// No priority policy.
	PriorityNone PriorityPolicy = iota
	// Clients will get the messages only if certain criteria are specified.
	PriorityOverflow
	// Single client takes over handling of the messages, while others are on standby.
	PriorityPinnedClient
	// Clients with lowest priority will be selected first.
	PriorityPrioritized
)

const (
	PriorityNoneJSONString         = `"none"`
	PriorityOverflowJSONString     = `"overflow"`
	PriorityPinnedClientJSONString = `"pinned_client"`
	PriorityPrioritizedJSONString  = `"prioritized"`
)

var (
	PriorityNoneJSONBytes         = []byte(PriorityNoneJSONString)
	PriorityOverflowJSONBytes     = []byte(PriorityOverflowJSONString)
	PriorityPinnedClientJSONBytes = []byte(PriorityPinnedClientJSONString)
	PriorityPrioritizedJSONBytes  = []byte(PriorityPrioritizedJSONString)
)

func (pp PriorityPolicy) String() string {
	switch pp {
	case PriorityOverflow:
		return PriorityOverflowJSONString
	case PriorityPinnedClient:
		return PriorityPinnedClientJSONString
	case PriorityPrioritized:
		return PriorityPrioritizedJSONString
	default:
		return PriorityNoneJSONString
	}
}

func (pp PriorityPolicy) MarshalJSON() ([]byte, error) {
	switch pp {
	case PriorityOverflow:
		return PriorityOverflowJSONBytes, nil
	case PriorityPinnedClient:
		return PriorityPinnedClientJSONBytes, nil
	case PriorityPrioritized:
		return PriorityPrioritizedJSONBytes, nil
	case PriorityNone:
		return PriorityNoneJSONBytes, nil
	default:
		return nil, fmt.Errorf("unknown priority policy: %v", pp)
	}
}

func (pp *PriorityPolicy) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case PriorityOverflowJSONString:
		*pp = PriorityOverflow
	case PriorityPinnedClientJSONString:
		*pp = PriorityPinnedClient
	case PriorityPrioritizedJSONString:
		*pp = PriorityPrioritized
	case PriorityNoneJSONString:
		*pp = PriorityNone
	default:
		return fmt.Errorf("unknown priority policy: %v", string(data))
	}
	return nil
}

// DeliverPolicy determines how the consumer should select the first message to deliver.
type DeliverPolicy int

const (
	// DeliverAll will be the default so can be omitted from the request.
	DeliverAll DeliverPolicy = iota
	// DeliverLast will start the consumer with the last sequence received.
	DeliverLast
	// DeliverNew will only deliver new messages that are sent after the consumer is created.
	DeliverNew
	// DeliverByStartSequence will look for a defined starting sequence to start.
	DeliverByStartSequence
	// DeliverByStartTime will select the first messsage with a timestamp >= to StartTime.
	DeliverByStartTime
	// DeliverLastPerSubject will start the consumer with the last message for all subjects received.
	DeliverLastPerSubject
)

func (dp DeliverPolicy) String() string {
	switch dp {
	case DeliverAll:
		return "all"
	case DeliverLast:
		return "last"
	case DeliverNew:
		return "new"
	case DeliverByStartSequence:
		return "by_start_sequence"
	case DeliverByStartTime:
		return "by_start_time"
	case DeliverLastPerSubject:
		return "last_per_subject"
	default:
		return "undefined"
	}
}

// AckPolicy determines how the consumer should acknowledge delivered messages.
type AckPolicy int

const (
	// AckNone requires no acks for delivered messages.
	AckNone AckPolicy = iota
	// AckAll when acking a sequence number, this implicitly acks all sequences below this one as well.
	AckAll
	// AckExplicit requires ack or nack for all messages.
	AckExplicit
	// AckFlowControl functions like AckAll, but acks based on responses to flow control.
	AckFlowControl
)

func (a AckPolicy) String() string {
	switch a {
	case AckNone:
		return "none"
	case AckAll:
		return "all"
	case AckFlowControl:
		return "flow_control"
	default:
		return "explicit"
	}
}

// ReplayPolicy determines how the consumer should replay messages it already has queued in the stream.
type ReplayPolicy int

const (
	// ReplayInstant will replay messages as fast as possible.
	ReplayInstant ReplayPolicy = iota
	// ReplayOriginal will maintain the same timing as the messages were received.
	ReplayOriginal
)

func (r ReplayPolicy) String() string {
	switch r {
	case ReplayInstant:
		return replayInstantPolicyJSONString
	default:
		return replayOriginalPolicyJSONString
	}
}

// OK
const OK = "+OK"

// Ack responses. Note that a nil or no payload is same as AckAck
var (
	// Ack
	AckAck = []byte("+ACK") // nil or no payload to ack subject also means ACK
	AckOK  = []byte(OK)     // deprecated but +OK meant ack as well.

	// Nack
	AckNak = []byte("-NAK")
	// Progress indicator
	AckProgress = []byte("+WPI")
	// Ack + Deliver the next message(s).
	AckNext = []byte("+NXT")
	// Terminate delivery of the message.
	AckTerm = []byte("+TERM")
)

const (
	// reasons to supply when terminating messages using limits
	ackTermLimitsReason        = "Message deleted by stream limits"
	ackTermUnackedLimitsReason = "Unacknowledged message was deleted"
)

// Calculate accurate replicas for the consumer config with the parent stream config.
func (consCfg *ConsumerConfig) replicas(strCfg *StreamConfig) int {
	if consCfg.Replicas == 0 || consCfg.Replicas > strCfg.Replicas {
		if !isDurableConsumer(consCfg) && strCfg.Retention == LimitsPolicy && consCfg.Replicas == 0 {
			// Matches old-school ephemerals only, where the replica count is 0.
			return 1
		}
		return strCfg.Replicas
	}
	return consCfg.Replicas
}

// Consumer is a jetstream consumer.
type consumer struct {
	// Atomic used to notify that we want to process an ack.
	// This will be checked in checkPending to abort processing
	// and let ack be processed in priority.
	awl               int64
	leader            atomic.Bool
	mu                sync.RWMutex
	js                *jetStream
	mset              *stream
	acc               *Account
	srv               *Server
	client            *client
	sysc              *client
	sid               int
	name              string
	stream            string
	sseq              uint64             // next stream sequence
	subjf             subjectFilters     // subject filters and their sequences
	filters           *gsl.SimpleSublist // When we have multiple filters we will use LoadNextMsgMulti and pass this in.
	dseq              uint64             // delivered consumer sequence
	adflr             uint64             // ack delivery floor
	asflr             uint64             // ack store floor
	chkflr            uint64             // our check floor, interest streams only.
	npc               int64              // Num Pending Count
	npf               uint64             // Num Pending Floor Sequence
	dsubj             string
	qgroup            string
	lss               *lastSeqSkipList
	rlimit            *rate.Limiter
	reqSub            *subscription
	resetSub          *subscription
	ackSubOld         *subscription
	ackReplyOldT      string
	ackSubjOld        string
	ackSub            *subscription
	ackReplyT         string
	ackSubj           string
	fcPreOld          string
	fcSubjOld         string
	fcPre             string
	fcSubj            string
	nextMsgSubj       string
	nextMsgReqs       *ipQueue[*nextMsgReq]
	resetSubj         string
	maxp              int
	pblimit           int
	maxpb             int
	pbytes            int
	fcsz              int
	fcid              string
	fcSubOld          *subscription
	fcSub             *subscription
	outq              *jsOutQ
	pending           map[uint64]*Pending
	ptmr              *time.Timer
	ptmrEnd           time.Time
	rdq               []uint64
	rdqi              avl.SequenceSet
	rdc               map[uint64]uint64
	replies           map[uint64]string
	pendingDeliveries map[uint64]*jsPubMsg        // Messages that can be delivered after achieving quorum.
	waitingDeliveries map[string]*waitingDelivery // (Optional) request timeout messages that need to wait for replicated deliveries first.
	maxdc             uint64
	waiting           *waitQueue
	cfg               ConsumerConfig
	direct            bool // Immutable, enforced by checkNewConsumerConfig. Read without o.mu.
	sourcing          bool // Immutable, enforced by checkNewConsumerConfig. Read without o.mu.
	ici               *ConsumerInfo
	store             ConsumerStore
	active            bool
	replay            bool
	useV2Ack          bool
	dtmr              *time.Timer
	uptmr             *time.Timer // Unpause timer
	gwdtmr            *time.Timer
	dthresh           time.Duration
	mch               chan struct{} // Message channel
	qch               chan struct{} // Quit channel
	mqch              chan struct{} // The monitor's quit channel.
	inch              chan bool     // Interest change channel
	sfreq             int32
	ackEventT         string
	nakEventT         string
	deliveryExcEventT string
	created           time.Time
	ldt               time.Time
	lat               time.Time
	lwqic             time.Time
	closed            bool
	restoring         bool
	restoreLeader     bool
	restoreTerm       uint64

	// Clustered.
	ca        *consumerAssignment
	node      RaftNode
	term      uint64 // Raft term, used to determine if we are still the leader for the current term (if applicable, 0 otherwise).
	werr      error  // If a write error was encountered while applying entries, and if so what error.
	infoSub   *subscription
	lqsent    time.Time
	prm       map[string]struct{}
	rsm       map[string]resetRequest // Reset requests that need to be responded to.
	prOk      bool
	uch       chan struct{}
	retention RetentionPolicy

	monitorWg sync.WaitGroup
	monitorMu sync.Mutex // Serializes monitorWg's Add against Wait to prevent a WaitGroup reuse panic.
	inMonitor bool

	// R>1 proposals
	pch   chan struct{}
	phead *proposal
	ptail *proposal

	// Ack queue
	ackMsgs *ipQueue[*jsAckMsg]

	// for stream signaling when multiple filters are set.
	sigSubs []string

	// Priority groups
	// Details described in ADR-42.

	// currentPinId is the current nuid for the pinned consumer.
	// If the Consumer is running in `PriorityPinnedClient` mode, server will
	// pick up a new nuid and assign it to first pending pull request.
	currentPinId string
	/// pinnedTtl is the remaining time before the current PinId expires.
	pinnedTtl *time.Timer
	pinnedTS  time.Time

	// If standalone/single-server, the offline reason needs to be stored directly in the consumer.
	// Otherwise, if clustered it will be part of the consumer assignment.
	offlineReason string
}

// A single subject filter.
type subjectFilter struct {
	subject          string
	tokenizedSubject []string
	hasWildcard      bool
}

type subjectFilters []*subjectFilter

// subjects is a helper function used for updating consumers.
// It is not used and should not be used in hotpath.
func (s subjectFilters) subjects() []string {
	subjects := make([]string, 0, len(s))
	for _, filter := range s {
		subjects = append(subjects, filter.subject)
	}
	return subjects
}

type proposal struct {
	data []byte
	next *proposal
}

const (
	// JsAckWaitDefault is the default AckWait, only applicable on explicit ack policy consumers.
	JsAckWaitDefault = 30 * time.Second
	// JsDeleteWaitTimeDefault is the default amount of time we will wait for non-durable
	// consumers to be in an inactive state before deleting them.
	JsDeleteWaitTimeDefault = 5 * time.Second
	// JsFlowControlMaxPending specifies default pending bytes during flow control that can be outstanding.
	JsFlowControlMaxPending = 32 * 1024 * 1024
	// JsDefaultMaxAckPending is set for consumers with explicit ack that do not set the max ack pending.
	JsDefaultMaxAckPending = 1000
	// JsDefaultPinnedTTL is the default grace period for the pinned consumer to send a new request before a new pin
	// is picked by a server.
	JsDefaultPinnedTTL = 2 * time.Minute
)

// Helper function to set consumer config defaults from above.
func setConsumerConfigDefaults(config *ConsumerConfig, streamCfg *StreamConfig, lim *JSLimitOpts, accLim *JetStreamAccountLimits, pedantic bool) *ApiError {
	// Setup default of -1, meaning no limit for MaxDeliver.
	if config.MaxDeliver == 0 || config.MaxDeliver < -1 {
		if pedantic && config.MaxDeliver < -1 {
			return NewJSPedanticError(errors.New("max_deliver must be set to -1"))
		}
		config.MaxDeliver = -1
	}
	// Setup zero defaults.
	if config.MaxWaiting < 0 {
		if pedantic {
			return NewJSPedanticError(errors.New("max_waiting must not be negative"))
		}
		config.MaxWaiting = 0
	}
	if config.MaxAckPending < -1 {
		if pedantic {
			return NewJSPedanticError(errors.New("max_ack_pending must be set to -1"))
		}
		config.MaxAckPending = -1
	}
	if config.MaxRequestBatch < 0 {
		if pedantic {
			return NewJSPedanticError(errors.New("max_batch must not be negative"))
		}
		config.MaxRequestBatch = 0
	}
	if config.MaxRequestExpires < 0 {
		if pedantic {
			return NewJSPedanticError(errors.New("max_expires must not be negative"))
		}
		config.MaxRequestExpires = 0
	}
	if config.MaxRequestMaxBytes < 0 {
		if pedantic {
			return NewJSPedanticError(errors.New("max_bytes must not be negative"))
		}
		config.MaxRequestMaxBytes = 0
	}
	if config.Heartbeat < 0 {
		if pedantic {
			return NewJSPedanticError(errors.New("idle_heartbeat must not be negative"))
		}
		config.Heartbeat = 0
	}
	if config.InactiveThreshold < 0 {
		if pedantic {
			return NewJSPedanticError(errors.New("inactive_threshold must not be negative"))
		}
		config.InactiveThreshold = 0
	}
	if config.PinnedTTL < 0 {
		if pedantic {
			return NewJSPedanticError(errors.New("priority_timeout must not be negative"))
		}
		config.PinnedTTL = 0
	}

	// Set to default if not specified.
	if config.DeliverSubject == _EMPTY_ && config.MaxWaiting == 0 {
		config.MaxWaiting = JSWaitQueueDefaultMax
	}
	// Setup proper default for ack wait if we are in explicit ack mode.
	if config.AckWait == 0 && (config.AckPolicy == AckExplicit || config.AckPolicy == AckAll) {
		config.AckWait = JsAckWaitDefault
	}
	// If BackOff was specified that will override the AckWait and the MaxDeliver.
	if len(config.BackOff) > 0 {
		if pedantic && config.AckWait != config.BackOff[0] {
			return NewJSPedanticError(errors.New("first backoff value has to equal batch AckWait"))
		}
		config.AckWait = config.BackOff[0]
	}
	if config.MaxAckPending == 0 {
		if pedantic && streamCfg.ConsumerLimits.MaxAckPending > 0 {
			return NewJSPedanticError(errors.New("max_ack_pending must be set if it's configured in stream limits"))
		}
		config.MaxAckPending = streamCfg.ConsumerLimits.MaxAckPending
	}
	if config.InactiveThreshold == 0 {
		if pedantic && streamCfg.ConsumerLimits.InactiveThreshold > 0 {
			return NewJSPedanticError(errors.New("inactive_threshold must be set if it's configured in stream limits"))
		}
		config.InactiveThreshold = streamCfg.ConsumerLimits.InactiveThreshold
	}
	// Set proper default for max ack pending if we are ack explicit and none has been set.
	if config.MaxAckPending == 0 && config.AckPolicy != AckNone {
		ackPending := JsDefaultMaxAckPending
		if lim.MaxAckPending > 0 && lim.MaxAckPending < ackPending {
			ackPending = lim.MaxAckPending
		}
		if accLim.MaxAckPending > 0 && accLim.MaxAckPending < ackPending {
			ackPending = accLim.MaxAckPending
		}
		config.MaxAckPending = ackPending
	}
	// if applicable set max request batch size
	if config.DeliverSubject == _EMPTY_ && config.MaxRequestBatch == 0 && lim.MaxRequestBatch > 0 {
		if pedantic {
			return NewJSPedanticError(errors.New("max_request_batch must be set if it's JetStream limits are set"))
		}
		config.MaxRequestBatch = lim.MaxRequestBatch
	}

	// set the default value only if pinned policy is used.
	if config.PriorityPolicy == PriorityPinnedClient && config.PinnedTTL == 0 {
		config.PinnedTTL = JsDefaultPinnedTTL
	}

	// Set default values for flow control policy.
	if config.AckPolicy == AckFlowControl && !pedantic {
		config.FlowControl = true
		config.Heartbeat = sourceHealthHB
	}
	return nil
}

// Check the consumer config. If we are recovering don't check filter subjects.
func checkConsumerCfg(
	config *ConsumerConfig,
	srvLim *JSLimitOpts,
	cfg *StreamConfig,
	_ *Account,
	accLim *JetStreamAccountLimits,
	isRecovering bool,
) *ApiError {

	if config.Name != _EMPTY_ && !isValidAssetName(config.Name) {
		return NewJSStreamInvalidConfigError(errors.New("consumer name can not contain '.', '*', '>', '\\', '/'"))
	}
	if config.Durable != _EMPTY_ && !isValidAssetName(config.Durable) {
		return NewJSStreamInvalidConfigError(errors.New("consumer durable name can not contain '.', '*', '>', '\\', '/'"))
	}

	// Check if replicas is defined but exceeds parent stream.
	if config.Replicas > 0 && config.Replicas > cfg.Replicas {
		return NewJSConsumerReplicasExceedsStreamError()
	}
	// Check that it is not negative
	if config.Replicas < 0 {
		return NewJSReplicasCountCannotBeNegativeError()
	}
	// If the stream is interest or workqueue retention make sure the replicas
	// match that of the stream. This is REQUIRED for now.
	if cfg.Retention == InterestPolicy || cfg.Retention == WorkQueuePolicy {
		// Only error here if not recovering.
		// We handle recovering in a different spot to allow consumer to come up
		// if previous version allowed it to be created. We do not want it to not come up.
		if !isRecovering && config.Replicas != 0 && config.Replicas != cfg.Replicas {
			return NewJSConsumerReplicasShouldMatchStreamError()
		}
	}

	if _, err := config.AckPolicy.MarshalJSON(); err != nil {
		return NewJSConsumerAckPolicyInvalidError()
	}
	if _, err := config.ReplayPolicy.MarshalJSON(); err != nil {
		return NewJSConsumerReplayPolicyInvalidError()
	}

	// Check not negative AckWait/BackOff
	for _, backoff := range config.BackOff {
		if backoff < 0 {
			return NewJSConsumerBackOffNegativeError()
		}
	}
	if config.AckWait < 0 {
		return NewJSConsumerAckWaitNegativeError()
	}

	// Ack Flow Control policy requires push-based flow-controlled consumer.
	if config.AckPolicy == AckFlowControl {
		if config.DeliverSubject == _EMPTY_ {
			return NewJSConsumerAckFCRequiresPushError()
		}
		if !config.FlowControl {
			return NewJSConsumerAckFCRequiresFCError()
		}
		// We currently limit using heartbeat of 1s, since those are used for ephemeral sourcing consumers as well.
		// We could decide to relax this in the future, but need to be careful to not allow a heartbeat larger
		// than the stalled source timeout.
		if config.Heartbeat != sourceHealthHB {
			return NewJSStreamInvalidConfigError(fmt.Errorf("flow control ack policy heartbeat needs to be 1s"))
		}
		if config.MaxAckPending <= 0 {
			return NewJSConsumerAckFCRequiresMaxAckPendingError()
		}
		if config.AckWait != 0 || len(config.BackOff) > 0 {
			return NewJSConsumerAckFCRequiresNoAckWaitError()
		}
		if config.MaxDeliver > 0 {
			return NewJSConsumerAckFCRequiresNoMaxDeliverError()
		}
	}

	// Check if we have a BackOff defined that MaxDeliver is within range etc.
	if lbo := len(config.BackOff); lbo > 0 && config.MaxDeliver != -1 && lbo > config.MaxDeliver {
		return NewJSConsumerMaxDeliverBackoffError()
	}

	if len(config.Description) > JSMaxDescriptionLen {
		return NewJSConsumerDescriptionTooLongError(JSMaxDescriptionLen)
	}

	// For now expect a literal subject if its not empty. Empty means work queue mode (pull mode).
	if config.DeliverSubject != _EMPTY_ {
		if !subjectIsLiteral(config.DeliverSubject) {
			return NewJSConsumerDeliverToWildcardsError()
		}
		if !IsValidSubject(config.DeliverSubject) {
			return NewJSConsumerInvalidDeliverSubjectError()
		}
		if deliveryFormsCycle(cfg, config.DeliverSubject) {
			return NewJSConsumerDeliverCycleError()
		}
		if config.MaxWaiting != 0 {
			return NewJSConsumerPushMaxWaitingError()
		}
		if config.MaxAckPending > 0 && config.AckPolicy == AckNone {
			return NewJSConsumerMaxPendingAckPolicyRequiredError()
		}
		if config.Heartbeat > 0 && config.Heartbeat < 100*time.Millisecond {
			return NewJSConsumerSmallHeartbeatError()
		}
	} else {
		// Pull mode with work queue retention from the stream requires an explicit ack.
		if config.AckPolicy == AckNone && cfg.Retention == WorkQueuePolicy {
			return NewJSConsumerPullRequiresAckError()
		}
		if config.RateLimit > 0 {
			return NewJSConsumerPullWithRateLimitError()
		}
		if config.MaxWaiting < 0 {
			return NewJSConsumerMaxWaitingNegativeError()
		}
		if config.Heartbeat > 0 {
			return NewJSConsumerHBRequiresPushError()
		}
		if config.FlowControl {
			return NewJSConsumerFCRequiresPushError()
		}
		if config.MaxRequestBatch < 0 {
			return NewJSConsumerMaxRequestBatchNegativeError()
		}
		if config.MaxRequestExpires != 0 && config.MaxRequestExpires < time.Millisecond {
			return NewJSConsumerMaxRequestExpiresTooSmallError()
		}
		if srvLim.MaxRequestBatch > 0 && config.MaxRequestBatch > srvLim.MaxRequestBatch {
			return NewJSConsumerMaxRequestBatchExceededError(srvLim.MaxRequestBatch)
		}
	}
	if srvLim.MaxAckPending > 0 && config.MaxAckPending > srvLim.MaxAckPending {
		return NewJSConsumerMaxPendingAckExcessError(srvLim.MaxAckPending)
	}
	if accLim.MaxAckPending > 0 && config.MaxAckPending > accLim.MaxAckPending {
		return NewJSConsumerMaxPendingAckExcessError(accLim.MaxAckPending)
	}
	if cfg.ConsumerLimits.MaxAckPending > 0 && config.MaxAckPending > cfg.ConsumerLimits.MaxAckPending {
		return NewJSConsumerMaxPendingAckExcessError(cfg.ConsumerLimits.MaxAckPending)
	}
	if cfg.ConsumerLimits.InactiveThreshold > 0 && config.InactiveThreshold > cfg.ConsumerLimits.InactiveThreshold {
		return NewJSConsumerInactiveThresholdExcessError(cfg.ConsumerLimits.InactiveThreshold)
	}

	// Direct need to be non-mapped ephemerals.
	if config.Direct {
		if config.DeliverSubject == _EMPTY_ {
			return NewJSConsumerDirectRequiresPushError()
		}
		if isDurableConsumer(config) {
			return NewJSConsumerDirectRequiresEphemeralError()
		}
	}

	// Do not allow specifying both FilterSubject and FilterSubjects,
	// as that's probably unintentional without any difference from passing
	// all filters in FilterSubjects.
	if config.FilterSubject != _EMPTY_ && len(config.FilterSubjects) > 0 {
		return NewJSConsumerDuplicateFilterSubjectsError()
	}

	if config.FilterSubject != _EMPTY_ && !IsValidSubject(config.FilterSubject) {
		return NewJSStreamInvalidConfigError(ErrBadSubject)
	}

	// We treat FilterSubjects: []string{""} as a misconfig, so we validate against it.
	for _, filter := range config.FilterSubjects {
		if filter == _EMPTY_ {
			return NewJSConsumerEmptyFilterError()
		}
	}
	subjectFilters := gatherSubjectFilters(config.FilterSubject, config.FilterSubjects)

	// Check subject filters do not overlap.
	for outer, subject := range subjectFilters {
		if !IsValidSubject(subject) {
			return NewJSStreamInvalidConfigError(ErrBadSubject)
		}
		for inner, ssubject := range subjectFilters {
			if inner != outer && subjectIsSubsetMatch(subject, ssubject) {
				return NewJSConsumerOverlappingSubjectFiltersError()
			}
		}
	}

	// Helper function to formulate similar errors.
	badStart := func(dp, start string) error {
		return fmt.Errorf("consumer delivery policy is deliver %s, but optional start %s is also set", dp, start)
	}
	notSet := func(dp, notSet string) error {
		return fmt.Errorf("consumer delivery policy is deliver %s, but optional %s is not set", dp, notSet)
	}

	// Check on start position conflicts.
	switch config.DeliverPolicy {
	case DeliverAll:
		if config.OptStartSeq > 0 {
			return NewJSConsumerInvalidPolicyError(badStart("all", "sequence"))
		}
		if config.OptStartTime != nil {
			return NewJSConsumerInvalidPolicyError(badStart("all", "time"))
		}
	case DeliverLast:
		if config.OptStartSeq > 0 {
			return NewJSConsumerInvalidPolicyError(badStart("last", "sequence"))
		}
		if config.OptStartTime != nil {
			return NewJSConsumerInvalidPolicyError(badStart("last", "time"))
		}
	case DeliverLastPerSubject:
		if config.OptStartSeq > 0 {
			return NewJSConsumerInvalidPolicyError(badStart("last per subject", "sequence"))
		}
		if config.OptStartTime != nil {
			return NewJSConsumerInvalidPolicyError(badStart("last per subject", "time"))
		}
		if config.FilterSubject == _EMPTY_ && len(config.FilterSubjects) == 0 {
			return NewJSConsumerInvalidPolicyError(notSet("last per subject", "filter subject"))
		}
	case DeliverNew:
		if config.OptStartSeq > 0 {
			return NewJSConsumerInvalidPolicyError(badStart("new", "sequence"))
		}
		if config.OptStartTime != nil {
			return NewJSConsumerInvalidPolicyError(badStart("new", "time"))
		}
	case DeliverByStartSequence:
		if config.OptStartSeq == 0 {
			return NewJSConsumerInvalidPolicyError(notSet("by start sequence", "start sequence"))
		}
		if config.OptStartTime != nil {
			return NewJSConsumerInvalidPolicyError(badStart("by start sequence", "time"))
		}
	case DeliverByStartTime:
		if config.OptStartTime == nil {
			return NewJSConsumerInvalidPolicyError(notSet("by start time", "start time"))
		}
		if config.OptStartSeq != 0 {
			return NewJSConsumerInvalidPolicyError(badStart("by start time", "start sequence"))
		}
	}

	if config.SampleFrequency != _EMPTY_ {
		s := strings.TrimSuffix(config.SampleFrequency, "%")
		if sampleFreq, err := strconv.Atoi(s); err != nil || sampleFreq < 0 {
			return NewJSConsumerInvalidSamplingError(err)
		}
	}

	// We reject if flow control is set without heartbeats.
	if config.FlowControl && config.Heartbeat == 0 {
		return NewJSConsumerWithFlowControlNeedsHeartbeatsError()
	}

	if config.Durable != _EMPTY_ && config.Name != _EMPTY_ {
		if config.Name != config.Durable {
			return NewJSConsumerCreateDurableAndNameMismatchError()
		}
	}

	var metadataLen int
	for k, v := range config.Metadata {
		metadataLen += len(k) + len(v)
	}
	if metadataLen > JSMaxMetadataLen {
		return NewJSConsumerMetadataLengthError(fmt.Sprintf("%dKB", JSMaxMetadataLen/1024))
	}

	if config.PriorityPolicy != PriorityNone {
		if config.DeliverSubject != "" {
			return NewJSConsumerPushWithPriorityGroupError()
		}
		if len(config.PriorityGroups) == 0 {
			return NewJSConsumerPriorityPolicyWithoutGroupError()
		}

		for _, group := range config.PriorityGroups {
			if group == _EMPTY_ {
				return NewJSConsumerEmptyGroupNameError()
			}
			if !validGroupName.MatchString(group) {
				return NewJSConsumerInvalidGroupNameError()
			}
		}
	} else {
		// If PriorityPolicy is None or not set, reject if PriorityGroups or PinnedTTL are set
		if len(config.PriorityGroups) > 0 {
			return NewJSConsumerPriorityGroupWithPolicyNoneError()
		}
		if config.PinnedTTL > 0 {
			return NewJSConsumerPinnedTTLWithoutPriorityPolicyNoneError()
		}
	}

	// For now don't allow preferred server in placement.
	if cfg.Placement != nil && cfg.Placement.Preferred != _EMPTY_ {
		return NewJSStreamInvalidConfigError(fmt.Errorf("preferred server not permitted in placement"))
	}

	return nil
}

func (mset *stream) addConsumerWithAction(config *ConsumerConfig, action ConsumerAction, pedantic bool) (*consumer, error) {
	return mset.addConsumerWithAssignment(config, _EMPTY_, nil, false, action, pedantic)
}

func (mset *stream) addConsumer(config *ConsumerConfig) (*consumer, error) {
	return mset.addConsumerWithAction(config, ActionCreateOrUpdate, false)
}

func (mset *stream) addConsumerWithAssignment(config *ConsumerConfig, oname string, ca *consumerAssignment, isRecovering bool, action ConsumerAction, pedantic bool) (*consumer, error) {
	return mset.addConsumerWithAssignmentAndMode(config, oname, ca, isRecovering, action, pedantic, false)
}

func (mset *stream) addConsumerForRestore(config *ConsumerConfig) (*consumer, error) {
	return mset.addConsumerWithAssignmentAndMode(config, _EMPTY_, nil, false, ActionCreateOrUpdate, false, true)
}

func (mset *stream) addConsumerWithAssignmentAndMode(config *ConsumerConfig, oname string, ca *consumerAssignment, isRecovering bool, action ConsumerAction, pedantic, restoring bool) (*consumer, error) {
	// Check if this stream has closed.
	if mset.closed.Load() {
		return nil, NewJSStreamInvalidError()
	}

	mset.mu.RLock()
	s, js, jsa, cfg, acc, lseq := mset.srv, mset.js, mset.jsa, mset.cfg, mset.acc, mset.lseq
	mset.mu.RUnlock()

	// If we do not have the consumer currently assigned to us in cluster mode we will proceed but warn.
	// This can happen on startup with restored state where on meta replay we still do not have
	// the assignment. Running in single server mode this always returns true.
	if oname != _EMPTY_ && !jsa.consumerAssigned(mset.name(), oname) {
		s.Debugf("Consumer %q > %q does not seem to be assigned to this server", mset.name(), oname)
	}

	if config == nil {
		return nil, NewJSConsumerConfigRequiredError()
	}

	selectedLimits, _, _, _ := acc.selectLimits(config.replicas(&cfg))
	if selectedLimits == nil {
		return nil, NewJSNoLimitsError()
	}

	srvLim := &s.getOpts().JetStreamLimits
	// Make sure we have sane defaults. Do so with the JS lock, otherwise a
	// badly timed meta snapshot can result in a race condition.
	mset.js.mu.Lock()
	err := setConsumerConfigDefaults(config, &cfg, srvLim, selectedLimits, pedantic)
	mset.js.mu.Unlock()
	if err != nil {
		return nil, err
	}

	if err := checkConsumerCfg(config, srvLim, &cfg, acc, selectedLimits, isRecovering); err != nil {
		return nil, err
	}
	sampleFreq := 0
	if config.SampleFrequency != _EMPTY_ {
		// Can't fail as checkConsumerCfg checks correct format
		sampleFreq, _ = strconv.Atoi(strings.TrimSuffix(config.SampleFrequency, "%"))
	}

	// Grab the client, account and server reference.
	c := mset.client
	if c == nil {
		return nil, NewJSStreamInvalidError()
	}
	var accName string
	c.mu.Lock()
	s, a := c.srv, c.acc
	if a != nil {
		accName = a.Name
	}
	c.mu.Unlock()

	// Hold mset lock here.
	mset.mu.Lock()
	if mset.client == nil || mset.store == nil || mset.consumers == nil {
		mset.mu.Unlock()
		return nil, NewJSStreamInvalidError()
	}

	// If this one is durable and already exists, we let that be ok as long as only updating what should be allowed.
	var cName string
	if isDurableConsumer(config) {
		cName = config.Durable
	} else if config.Name != _EMPTY_ {
		cName = config.Name
	}
	if cName != _EMPTY_ {
		if eo, ok := mset.consumers[cName]; ok {
			if action == ActionCreate {
				ocfg := eo.config()
				copyConsumerMetadata(config, &ocfg)
				if !reflect.DeepEqual(config, &ocfg) {
					mset.mu.Unlock()
					return nil, NewJSConsumerAlreadyExistsError()
				}
			}
			// Check for overlapping subjects if we are a workqueue
			if cfg.Retention == WorkQueuePolicy {
				subjects := gatherSubjectFilters(config.FilterSubject, config.FilterSubjects)
				if !mset.partitionUnique(cName, subjects) {
					mset.mu.Unlock()
					return nil, NewJSConsumerWQConsumerNotUniqueError()
				}
			}
			mset.mu.Unlock()
			err := eo.updateConfig(config)
			if err == nil {
				return eo, nil
			}
			return nil, NewJSConsumerCreateError(err, Unless(err))
		}
	}
	if action == ActionUpdate {
		mset.mu.Unlock()
		return nil, NewJSConsumerDoesNotExistError()
	}

	standalone := !s.JetStreamIsClustered() && s.standAloneMode()

	// If we're clustered we've already done this check, only do this if we're a standalone server.
	// But if we're standalone, only enforce if we're not recovering, since the MaxConsumers could've
	// been updated while we already had more consumers on disk.
	if standalone && !isRecovering && !config.Direct && !config.Sourcing {
		// Check for any limits, if the config for the consumer sets a limit we check against that
		// but if not we use the value from account limits, if account limits is more restrictive
		// than stream config we prefer the account limits to handle cases where account limits are
		// updated during the lifecycle of the stream
		maxc := cfg.MaxConsumers
		if maxc <= 0 || (selectedLimits.MaxConsumers > 0 && selectedLimits.MaxConsumers < maxc) {
			maxc = selectedLimits.MaxConsumers
		}
		// Only apply default limits if the user specified neither stream nor account limit.
		// An unlimited (-1) stream or account limit counts as not set, since that is also
		// the default for accounts, so the default can't be opted out of per stream or account.
		if maxConsumers := srvLim.DefaultMaxConsumers; maxConsumers > 0 && maxc <= 0 {
			maxc = maxConsumers
		}
		if maxc > 0 && mset.numLimitableConsumers() >= maxc {
			mset.mu.Unlock()
			return nil, NewJSMaximumConsumersLimitError()
		}
	}

	// Check on stream type conflicts with WorkQueues.
	if cfg.Retention == WorkQueuePolicy && !config.Direct && !config.Sourcing {
		// Force explicit acks here.
		if config.AckPolicy != AckExplicit && config.AckPolicy != AckFlowControl {
			mset.mu.Unlock()
			return nil, NewJSConsumerWQRequiresExplicitAckError()
		}
		if config.DeliverPolicy != DeliverAll {
			mset.mu.Unlock()
			return nil, NewJSConsumerWQConsumerNotDeliverAllError()
		}
		if mset.numLimitableConsumers() > 0 {
			subjects := gatherSubjectFilters(config.FilterSubject, config.FilterSubjects)
			if len(subjects) == 0 {
				mset.mu.Unlock()
				return nil, NewJSConsumerWQMultipleUnfilteredError()
			} else if !mset.partitionUnique(cName, subjects) {
				// Prior to v2.9.7, on a stream with WorkQueue policy, the servers
				// were not catching the error of having multiple consumers with
				// overlapping filter subjects depending on the scope, for instance
				// creating "foo.*.bar" and then "foo.>" was not detected, while
				// "foo.>" and then "foo.*.bar" would have been. Failing here
				// in recovery mode would leave the rejected consumer in a bad state,
				// so we will simply warn here, asking the user to remove this
				// consumer administratively. Otherwise, if this is the creation
				// of a new consumer, we will return the error.
				if isRecovering {
					s.Warnf("Consumer %q > %q has a filter subject that overlaps "+
						"with other consumers, which is not allowed for a stream "+
						"with WorkQueue policy, it should be administratively deleted",
						cfg.Name, cName)
				} else {
					// We have a partition but it is not unique amongst the others.
					mset.mu.Unlock()
					return nil, NewJSConsumerWQConsumerNotUniqueError()
				}
			}
		}
	}

	// Set name, which will be durable name if set, otherwise we create one at random.
	o := &consumer{
		mset:      mset,
		js:        s.getJetStream(),
		acc:       a,
		srv:       s,
		client:    s.createInternalJetStreamClient(),
		sysc:      s.createInternalJetStreamClient(),
		cfg:       *config,
		direct:    config.Direct,
		sourcing:  config.Sourcing,
		dsubj:     config.DeliverSubject,
		outq:      mset.outq,
		active:    true,
		qch:       make(chan struct{}),
		mqch:      make(chan struct{}),
		uch:       make(chan struct{}, 1),
		mch:       make(chan struct{}, 1),
		sfreq:     int32(sampleFreq),
		maxdc:     uint64(max(config.MaxDeliver, 0)), // MaxDeliver is negative (-1) when infinite.
		maxp:      config.MaxAckPending,
		retention: cfg.Retention,
		created:   time.Now().UTC(),
		restoring: restoring,
	}

	// Add created timestamp used for the store, must match that of the consumer assignment if it exists.
	if ca != nil {
		js.mu.RLock()
		o.created = ca.Created
		js.mu.RUnlock()
	}

	// Bind internal client to the user account.
	o.client.registerWithAccount(a)
	// Bind to the system account.
	o.sysc.registerWithAccount(s.SystemAccount())

	if isDurableConsumer(config) {
		if len(config.Durable) > JSMaxNameLen {
			mset.mu.Unlock()
			// Release the temporary consumer we built; it was never registered.
			_ = o.stop()
			return nil, NewJSConsumerNameTooLongError(JSMaxNameLen)
		}
		o.name = config.Durable
	} else if oname != _EMPTY_ {
		o.name = oname
	} else {
		if config.Name != _EMPTY_ {
			o.name = config.Name
		} else {
			// Legacy ephemeral auto-generated.
			for {
				o.name = createConsumerName()
				if _, ok := mset.consumers[o.name]; !ok {
					break
				}
			}
			config.Name = o.name
		}
	}
	// already under lock, mset.Name() would deadlock
	o.stream = cfg.Name
	o.ackEventT = JSMetricConsumerAckPre + "." + o.stream + "." + o.name
	o.nakEventT = JSAdvisoryConsumerMsgNakPre + "." + o.stream + "." + o.name
	o.deliveryExcEventT = JSAdvisoryConsumerMaxDeliveryExceedPre + "." + o.stream + "." + o.name

	if !isValidAssetName(o.name) {
		mset.mu.Unlock()
		// Release the temporary consumer we built; it was never registered.
		_ = o.stop()
		return nil, NewJSConsumerBadDurableNameError()
	}

	// Check if we already have this one registered.
	if eo, ok := mset.consumers[o.name]; ok {
		mset.mu.Unlock()
		if !o.isDurable() || !o.isPushMode() {
			_ = o.stop()
			return nil, NewJSConsumerNameExistError()
		}
		// If we are here we have already registered this durable. If it is still active that is an error.
		if eo.isActive() {
			_ = o.stop()
			return nil, NewJSConsumerExistingActiveError()
		}
		// Since we are here this means we have a potentially new durable so we should update here.
		// Check that configs are the same.
		if !configsEqualSansDelivery(o.cfg, eo.cfg) {
			_ = o.stop()
			return nil, NewJSConsumerReplacementWithDifferentNameError()
		}
		// Once we are here we have a replacement push-based durable.
		eo.updateDeliverSubject(o.cfg.DeliverSubject)
		// Release the temporary consumer we built; it was never registered.
		_ = o.stop()
		return eo, nil
	}

	// Setup our storage if not a direct consumer.
	if !config.Direct {
		store, err := mset.store.ConsumerStore(o.name, o.created, config)
		if err != nil {
			mset.mu.Unlock()
			// Store creation failed, so just cleanup.
			_ = o.stop()
			return nil, NewJSConsumerStoreFailedError(err)
		}
		o.store = store
	}

	for _, filter := range gatherSubjectFilters(o.cfg.FilterSubject, o.cfg.FilterSubjects) {
		sub := &subjectFilter{
			subject:          filter,
			hasWildcard:      subjectHasWildcard(filter),
			tokenizedSubject: tokenizeSubjectIntoSlice(nil, filter),
		}
		o.subjf = append(o.subjf, sub)
	}

	// If we have multiple filter subjects, create a sublist which we will use
	// in calling store.LoadNextMsgMulti.
	if len(o.subjf) <= 1 {
		o.filters = nil
	} else {
		o.filters = gsl.NewSublist[struct{}]()
		for _, filter := range o.subjf {
			o.filters.Insert(filter.subject, struct{}{})
		}
	}

	if o.store != nil && o.store.HasState() {
		// Restore our saved state.
		o.mu.Lock()
		o.readStoredState()

		replicas := o.cfg.replicas(&mset.cfg)

		// Starting sequence represents the next sequence to be delivered, so decrement it
		// since that's the minimum amount the stream should have as its last sequence.
		sseq := o.sseq
		if sseq > 0 {
			sseq--
		}

		o.mu.Unlock()

		// A stream observing data loss rolls back in its sequence. Check if we need to reconcile the consumer state
		// to ensure new messages aren't skipped.
		// Only performed for non-replicated consumers for now.
		if replicas == 1 && lseq < sseq && isRecovering {
			s.Warnf("JetStream consumer '%s > %s > %s' delivered sequence %d past last stream sequence of %d",
				o.acc.Name, o.stream, o.name, sseq, lseq)

			o.mu.Lock()
			o.reconcileStateWithStream(lseq)

			// Save the reconciled state
			state := &ConsumerState{
				Delivered: SequencePair{
					Stream:   o.sseq - 1,
					Consumer: o.dseq - 1,
				},
				AckFloor: SequencePair{
					Stream:   o.asflr,
					Consumer: o.adflr,
				},
				Pending:     o.pending,
				Redelivered: o.rdc,
			}
			err := o.store.ForceUpdate(state)
			o.mu.Unlock()
			if err != nil {
				s.Errorf("JetStream consumer '%s > %s > %s' errored while updating state: %v", o.acc.Name, o.stream, o.name, err)
				mset.mu.Unlock()
				// Release the temporary consumer we built; it was never registered.
				_ = o.stop()
				return nil, NewJSConsumerStoreFailedError(err)
			}
		}
	} else if config.Direct || standalone {
		// Clustered non-direct consumers defer this to setLeader so the
		// expensive store scans don't block the meta apply goroutine.
		if err := o.selectStartingSeqNo(); err != nil {
			// Delete our store while holding the stream lock, so a concurrent create
			// for the same name cannot have registered and be sharing this on-disk
			// directory. Then release the rest of the consumer non-destructively.
			if o.store != nil {
				_ = o.store.Delete()
				o.store = nil
			}
			mset.mu.Unlock()
			_ = o.stop()
			return nil, err
		}
	}

	// Create ackMsgs queue now that we have a consumer name
	o.ackMsgs = newIPQueue[*jsAckMsg](s, fmt.Sprintf("[ACC:%s] consumer '%s' on stream '%s' ackMsgs", accName, o.name, cfg.Name))

	// Create our request waiting queue.
	if o.isPullMode() {
		o.waiting = newWaitQueue(config.MaxWaiting)
		o.nextMsgReqs = newIPQueue[*nextMsgReq](s, fmt.Sprintf("[ACC:%s] consumer '%s' on stream '%s' pull requests", accName, o.name, cfg.Name))
	}

	// Set up the ack subscription for this consumer. Will use wildcard for all acks.
	// We will remember the template to generate replies with sequence numbers and use
	// that to scanf them back in.
	// Escape '%' in consumer and stream names, as `pre` is used as a template later
	// in consumer.ackReply(), resulting in erroneous formatting of the ack subject.
	mn := strings.ReplaceAll(cfg.Name, "%", "%%")
	on := strings.ReplaceAll(o.name, "%", "%%")
	domain := o.srv.getOpts().JetStreamDomain
	if domain == _EMPTY_ {
		domain = "_"
	}
	dn := strings.ReplaceAll(domain, "%", "%%")
	accHash := getHash(accName)

	o.useV2Ack = s.getOpts().getFeatureFlag(FeatureFlagJsAckFormatV2)

	// v1 format: $JS.(ACK|FC).<stream>.<consumer>.etc.
	o.fcPreOld = jsFlowControlPre
	o.fcSubjOld = fmt.Sprintf(jsFlowControl, cfg.Name, o.name)
	preOld := fmt.Sprintf(jsAckT, mn, on)
	o.ackReplyOldT = fmt.Sprintf("%s.%%d.%%d.%%d.%%d.%%d", preOld)
	o.ackSubjOld = fmt.Sprintf(jsAckT+".*.*.*.*.*", cfg.Name, o.name)

	// v2 format: $JS.(ACK|FC).<domain>.<accHash>.<stream>.<consumer>.etc.
	o.fcPre = fmt.Sprintf("%s%s.%s.", jsFlowControlPre, domain, accHash)
	o.fcSubj = fmt.Sprintf(jsFlowControlV2, domain, accHash, cfg.Name, o.name)
	pre := fmt.Sprintf(jsAckTv2, dn, accHash, mn, on)
	o.ackReplyT = fmt.Sprintf("%s.%%d.%%d.%%d.%%d.%%d", pre)
	// Subscribe on this ack subject for v2, we require 11 tokens, but allow for more tokens/extension.
	o.ackSubj = fmt.Sprintf(jsAckTv2+".*.*.*.*.>", domain, accHash, cfg.Name, o.name)

	o.nextMsgSubj = fmt.Sprintf(JSApiRequestNextT, cfg.Name, o.name)
	o.resetSubj = fmt.Sprintf(JSApiConsumerResetT, cfg.Name, o.name)

	// Check/update the inactive threshold
	o.updateInactiveThreshold(&o.cfg)

	if o.isPushMode() {
		// Check if we are running only 1 replica and that the delivery subject has interest.
		// Check in place here for interest. Will setup properly in setLeader.
		if config.replicas(&cfg) == 1 {
			interest := o.acc.sl.HasInterest(o.cfg.DeliverSubject)
			if !o.hasDeliveryInterest(interest) {
				// Let the interest come to us eventually, but setup delete timer.
				o.updateDeliveryInterest(false)
			}
		}
	}

	// Set our ca.
	if ca != nil {
		o.setConsumerAssignment(ca)
	}

	// Check if we have a rate limit set.
	if config.RateLimit != 0 {
		o.setRateLimit(config.RateLimit)
	}

	mset.setConsumer(o)
	mset.mu.Unlock()

	if config.Sourcing && standalone {
		o.resetStartingSeq(0, _EMPTY_, false, false)
	}
	if config.Direct || standalone {
		o.setLeader(true, 0)
	}

	// This is always true in single server mode.
	if o.IsLeader() {
		// Send advisory.
		var suppress bool
		if !s.standAloneMode() && ca == nil {
			suppress = true
		} else if ca != nil {
			suppress = ca.hasResponded()
		}
		if !suppress {
			o.sendCreateAdvisory()
		}
	}

	return o, nil
}

// Updates the consumer `dthresh` delete timer duration and set
// cfg.InactiveThreshold to JsDeleteWaitTimeDefault for ephemerals
// if not explicitly already specified by the user.
// Lock should be held.
func (o *consumer) updateInactiveThreshold(cfg *ConsumerConfig) {
	// Ephemerals will always have inactive thresholds.
	if !o.isDurable() && cfg.InactiveThreshold <= 0 {
		// Add in 1 sec of jitter above and beyond the default of 5s.
		o.dthresh = JsDeleteWaitTimeDefault + 100*time.Millisecond + time.Duration(rand.Int64N(900))*time.Millisecond
		// Only stamp config with default sans jitter.
		cfg.InactiveThreshold = JsDeleteWaitTimeDefault
	} else if cfg.InactiveThreshold > 0 {
		// Add in up to 1 sec of jitter if pull mode.
		if o.isPullMode() {
			o.dthresh = cfg.InactiveThreshold + 100*time.Millisecond + time.Duration(rand.Int64N(900))*time.Millisecond
		} else {
			o.dthresh = cfg.InactiveThreshold
		}
	} else if cfg.InactiveThreshold <= 0 {
		// We accept InactiveThreshold be set to 0 (for durables)
		o.dthresh = 0
	}
}

// Updates the paused state. If we are the leader and the pause deadline
// hasn't passed yet then we will start a timer to kick the consumer once
// that deadline is reached. Lock should be held.
func (o *consumer) updatePauseState(cfg *ConsumerConfig) {
	if o.uptmr != nil {
		stopAndClearTimer(&o.uptmr)
	}
	if !o.isLeader() {
		// Only the leader will run the timer as only the leader will run
		// loopAndGatherMsgs.
		return
	}
	if cfg.PauseUntil == nil || cfg.PauseUntil.IsZero() || cfg.PauseUntil.Before(time.Now()) {
		// Either the PauseUntil is unset (is effectively zero) or the
		// deadline has already passed, in which case there is nothing
		// to do.
		return
	}
	o.uptmr = time.AfterFunc(time.Until(*cfg.PauseUntil), func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		stopAndClearTimer(&o.uptmr)
		o.sendPauseAdvisoryLocked(&o.cfg)
		o.signalNewMessages()
	})
}

func (o *consumer) consumerAssignment() *consumerAssignment {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.ca
}

func (o *consumer) setConsumerAssignment(ca *consumerAssignment) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.ca = ca
	if ca == nil {
		return
	}
	// Set our node.
	o.node = ca.Group.node

	// Trigger update chan.
	select {
	case o.uch <- struct{}{}:
	default:
	}
}

func (o *consumer) monitorQuitC() <-chan struct{} {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	// Recreate if a prior monitor routine was stopped.
	if o.mqch == nil {
		o.mqch = make(chan struct{})
	}
	return o.mqch
}

// signalMonitorQuit signals to exit the monitor loop. If there's no Raft node,
// this will be the only way to stop the monitor goroutine.
func (o *consumer) signalMonitorQuit() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.mqch != nil {
		close(o.mqch)
		o.mqch = nil
	}
}

func (o *consumer) updateC() <-chan struct{} {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.uch
}

// checkQueueInterest will check on our interest's queue group status.
// Lock should be held.
func (o *consumer) checkQueueInterest() {
	if !o.active || o.cfg.DeliverSubject == _EMPTY_ {
		return
	}
	subj := o.dsubj
	if subj == _EMPTY_ {
		subj = o.cfg.DeliverSubject
	}

	if rr := o.acc.sl.Match(subj); len(rr.qsubs) > 0 {
		// Just grab first
		if qsubs := rr.qsubs[0]; len(qsubs) > 0 {
			if sub := rr.qsubs[0][0]; len(sub.queue) > 0 {
				o.qgroup = string(sub.queue)
			}
		}
	}
}

// clears our node if we have one. When we scale down to 1.
func (o *consumer) clearNode() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.node != nil {
		o.node.Delete()
		o.node = nil
	}
}

// IsLeader will return if we are the current leader.
func (o *consumer) IsLeader() bool {
	return o.isLeader()
}

// Lock should be held.
func (o *consumer) isLeader() bool {
	return o.leader.Load()
}

func (o *consumer) setLeader(isLeader bool, term uint64) error {
	o.mu.Lock()
	if o.restoring {
		o.restoreLeader, o.restoreTerm = isLeader, term
		o.mu.Unlock()
		return nil
	}
	mset, closed := o.mset, o.closed
	wasLeader := o.leader.Swap(isLeader)

	// For clustered new consumers, starting seq selection was deferred from
	// addConsumerWithAssignment so the scan wouldn't block the meta apply
	// goroutine, run it here on leader-elect instead.
	needsSelect := isLeader && !wasLeader && o.dseq == 0 && (o.store == nil || !o.store.HasState())

	// We can skip the teardown if we were leader before and are still the leader now.
	// But only at term 1, since that means scale up from or down to an unreplicated config.
	// R1 assets have no raft node and use the coerced term 1.
	if term < 1 {
		term = 1
	}
	skipTeardown := wasLeader && isLeader && term == 1
	o.term = term

	if skipTeardown {
		movingToClustered := o.node != nil && o.pch == nil
		movingToNonClustered := o.node == nil && o.pch != nil

		// If we detect we are scaling up, make sure to create clustered routines and channels.
		if movingToClustered {
			// We are moving from R1 to clustered.
			o.pch = make(chan struct{}, 1)
			go o.loopAndForwardProposals(o.node, o.qch, o.pch, term)
			if o.phead != nil {
				select {
				case o.pch <- struct{}{}:
				default:
				}
			}
		} else if movingToNonClustered {
			// We are moving from clustered to non-clustered now.
			// Set pch to nil so if we scale back up we will recreate the loopAndForward from above.
			pch := o.pch
			o.pch = nil
			select {
			case pch <- struct{}{}:
			default:
			}
		}
		o.mu.Unlock()
		return nil
	}

	// Shutdown the go routines and the subscriptions.
	if o.qch != nil {
		close(o.qch)
		o.qch = nil
	}
	// Stop any inactivity timers. Should only be running on leaders.
	stopAndClearTimer(&o.dtmr)
	// Stop any unpause timers. Should only be running on leaders.
	stopAndClearTimer(&o.uptmr)
	// Make sure to clear out any re-deliver queues
	o.stopAndClearPtmr()
	o.rdc = nil
	o.rdq = nil
	o.rdqi.Empty()
	o.pending = nil
	o.rsm = nil
	o.resetPendingDeliveries()
	// Reset num pending, these are only authoritative on the leader.
	o.npc, o.npf = 0, 0
	// Reset flow control accounting.
	o.pbytes, o.fcsz, o.fcid = 0, 0, _EMPTY_
	// ok if they are nil, we protect inside unsubscribe()
	o.unsubscribe(o.ackSubOld)
	o.unsubscribe(o.ackSub)
	o.unsubscribe(o.reqSub)
	o.unsubscribe(o.resetSub)
	o.unsubscribe(o.fcSubOld)
	o.unsubscribe(o.fcSub)
	o.ackSubOld, o.ackSub, o.reqSub, o.resetSub, o.fcSubOld, o.fcSub = nil, nil, nil, nil, nil, nil
	if o.infoSub != nil {
		o.srv.sysUnsubscribe(o.infoSub)
		o.infoSub = nil
	}
	// Reset waiting if we are in pull mode.
	if o.isPullMode() {
		o.waiting = newWaitQueue(o.cfg.MaxWaiting)
		o.nextMsgReqs.drain()
	} else if o.srv.gateway.enabled {
		stopAndClearTimer(&o.gwdtmr)
	}
	o.unassignPinId()

	// Make sure to drain queued up acks.
	o.ackMsgs.drain()
	// Reset amount of acks that need to be processed.
	atomic.StoreInt64(&o.awl, 0)
	// Also remove any pending replies since we should not be the one to respond at this point.
	o.replies = nil

	// Set pch to nil, we will recreate the loopAndForwardProposals for the new term.
	if pch := o.pch; pch != nil {
		o.pch = nil
		select {
		case pch <- struct{}{}:
		default:
		}
	}
	// Clear proposals, none are valid anymore.
	o.phead, o.ptail = nil, nil
	o.mu.Unlock()

	// If we are here we have a change in leader status.
	if isLeader {
		if closed || mset == nil {
			return nil
		}

		if needsSelect {
			o.mu.Lock()
			if err := o.selectStartingSeqNo(); err != nil {
				o.srv.Errorf("JetStream consumer '%s > %s > %s' select starting seq failed: %v",
					o.acc.Name, o.stream, o.name, err)
				o.leader.Store(false)
				node := o.node
				o.mu.Unlock()
				if node != nil {
					_ = node.StepDown()
				}
				return err
			}
			o.mu.Unlock()
		}

		mset.mu.RLock()
		s, jsa, stream := mset.srv, mset.jsa, mset.getCfgName()
		mset.mu.RUnlock()

		o.mu.Lock()
		o.rdq = nil
		o.rdqi.Empty()

		// Restore our saved state.
		// During non-leader status we just update our underlying store when not clustered.
		// If clustered we need to propose our initial (possibly skipped ahead) o.sseq to the group.
		if o.node == nil || o.dseq > 1 || (o.store != nil && o.store.HasState()) {
			o.readStoredState()
		} else if o.node != nil && o.sseq >= 1 {
			o.updateSkipped(o.sseq)
		}

		// Setup initial num pending.
		o.streamNumPending()

		// Cleanup lss when we take over in clustered mode.
		if o.hasSkipListPending() && o.sseq >= o.lss.resume {
			o.lss = nil
		}

		// Do info sub.
		if o.infoSub == nil && jsa != nil {
			isubj := fmt.Sprintf(clusterConsumerInfoT, jsa.acc(), stream, o.name)
			// Note below the way we subscribe here is so that we can send requests to ourselves.
			o.infoSub, _ = s.systemSubscribe(isubj, _EMPTY_, false, o.sysc, o.handleClusterConsumerInfoRequest)
		}

		var err error
		if o.cfg.AckPolicy != AckNone && o.cfg.AckPolicy != AckFlowControl {
			if o.ackSubOld, err = o.subscribeInternal(o.ackSubjOld, o.pushAck); err != nil {
				o.mu.Unlock()
				return nil
			}
			if o.ackSub, err = o.subscribeInternal(o.ackSubj, o.pushAck); err != nil {
				o.mu.Unlock()
				return nil
			}
		}

		// Setup the internal sub for next message requests regardless.
		// Will error if wrong mode to provide feedback to users.
		if o.reqSub, err = o.subscribeInternal(o.nextMsgSubj, o.processNextMsgReq); err != nil {
			o.mu.Unlock()
			return nil
		}
		if o.resetSub, err = o.subscribeInternal(o.resetSubj, o.processResetReq); err != nil {
			o.mu.Unlock()
			return nil
		}

		// Check on flow control settings.
		if o.cfg.FlowControl {
			o.setMaxPendingBytes(JsFlowControlMaxPending)
			if o.fcSubOld, err = o.subscribeInternal(o.fcSubjOld, o.processFlowControl); err != nil {
				o.mu.Unlock()
				return nil
			}
			if o.fcSub, err = o.subscribeInternal(o.fcSubj, o.processFlowControl); err != nil {
				o.mu.Unlock()
				return nil
			}
		}

		// If push mode, register for notifications on interest.
		if o.isPushMode() {
			o.inch = make(chan bool, 8)
			o.acc.sl.registerNotification(o.cfg.DeliverSubject, o.cfg.DeliverGroup, o.inch)
			if o.active = <-o.inch; o.active {
				o.checkQueueInterest()
			}

			// Check gateways in case they are enabled.
			if s.gateway.enabled {
				if !o.active {
					o.active = s.hasGatewayInterest(o.acc.Name, o.cfg.DeliverSubject)
				}
				stopAndClearTimer(&o.gwdtmr)
				o.gwdtmr = time.AfterFunc(time.Second, func() { o.watchGWinterest() })
			}
		}

		if o.dthresh > 0 && (o.isPullMode() || !o.active) {
			// Pull consumer. We run the dtmr all the time for this one.
			stopAndClearTimer(&o.dtmr)
			o.dtmr = time.AfterFunc(o.dthresh, o.deleteNotActive)
		}

		// Update the consumer pause tracking.
		o.updatePauseState(&o.cfg)

		// If we are not in ReplayInstant mode mark us as in replay state until resolved.
		if o.cfg.ReplayPolicy != ReplayInstant {
			o.replay = true
		}

		// Recreate quit channel.
		o.qch = make(chan struct{})
		qch := o.qch
		node := o.node
		var pch chan struct{}
		if node != nil && o.pch == nil {
			o.pch = make(chan struct{}, 1)
			pch = o.pch
		}
		pullMode := o.isPullMode()
		o.mu.Unlock()

		// Check if there are any pending we might need to clean up etc.
		o.checkPending()

		// Snapshot initial info.
		o.infoWithSnap(true)

		// These are the labels we will use to annotate our goroutines.
		labels := pprofLabels{
			"type":     "consumer",
			"account":  mset.accName(),
			"stream":   mset.name(),
			"consumer": o.name,
		}

		// Now start up Go routine to deliver msgs.
		go func() {
			setGoRoutineLabels(labels)
			o.loopAndGatherMsgs(qch)
		}()

		// Now start up Go routine to process acks.
		go func() {
			setGoRoutineLabels(labels)
			o.processInboundAcks(qch)
		}()

		if pullMode {
			// Now start up Go routine to process inbound next message requests.
			go func() {
				setGoRoutineLabels(labels)
				o.processInboundNextMsgReqs(qch)
			}()
		}

		// If we are R>1 spin up our proposal loop.
		if node != nil && pch != nil {
			// Determine if we can send pending requests info to the group.
			// They must be on server versions >= 2.7.1
			o.checkAndSetPendingRequestsOk()
			o.checkPendingRequests()
			go func() {
				setGoRoutineLabels(labels)
				o.loopAndForwardProposals(node, qch, pch, term)
			}()
		}
	}
	return nil
}

func (o *consumer) completeRestore() error {
	o.mu.Lock()
	if !o.restoring {
		o.mu.Unlock()
		return nil
	}
	o.restoring = false
	isLeader, term := o.restoreLeader, o.restoreTerm
	o.restoreLeader, o.restoreTerm = false, 0
	o.mu.Unlock()
	if isLeader {
		return o.setLeader(true, term)
	}
	return nil
}

// This is coming on the wire so do not block here.
func (o *consumer) handleClusterConsumerInfoRequest(sub *subscription, c *client, _ *Account, subject, reply string, msg []byte) {
	go o.infoWithSnapAndReply(false, reply)
}

// Lock should be held.
func (o *consumer) subscribeInternal(subject string, cb msgHandler) (*subscription, error) {
	c := o.client
	if c == nil {
		return nil, fmt.Errorf("invalid consumer")
	}
	if !c.srv.EventsEnabled() {
		return nil, ErrNoSysAccount
	}
	if cb == nil {
		return nil, fmt.Errorf("undefined message handler")
	}

	o.sid++

	// Now create the subscription
	return c.processSub([]byte(subject), nil, []byte(strconv.Itoa(o.sid)), cb, false)
}

// Unsubscribe from our subscription.
// Lock should be held.
func (o *consumer) unsubscribe(sub *subscription) {
	if sub == nil || o.client == nil {
		return
	}
	o.client.processUnsub(sub.sid)
}

// We need to make sure we protect access to the outq.
// Do all advisory sends here.
func (o *consumer) sendAdvisory(subject string, e any) {
	if o.acc == nil {
		return
	}

	// If there is no one listening for this advisory then save ourselves the effort
	// and don't bother encoding the JSON or sending it.
	if sl := o.acc.sl; (sl != nil && !sl.HasInterest(subject)) && !o.srv.hasGatewayInterest(o.acc.Name, subject) {
		return
	}

	j, err := json.Marshal(e)
	if err != nil {
		return
	}

	o.outq.sendMsg(subject, j)
}

func (o *consumer) sendDeleteAdvisoryLocked() {
	e := JSConsumerActionAdvisory{
		TypedEvent: TypedEvent{
			Type: JSConsumerActionAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   o.stream,
		Consumer: o.name,
		Action:   DeleteEvent,
		Domain:   o.srv.getOpts().JetStreamDomain,
	}

	subj := JSAdvisoryConsumerDeletedPre + "." + o.stream + "." + o.name
	o.sendAdvisory(subj, e)
}

func (o *consumer) sendPinnedAdvisoryLocked(group string) {
	e := JSConsumerGroupPinnedAdvisory{
		TypedEvent: TypedEvent{
			Type: JSConsumerGroupPinnedAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Account:        o.acc.Name,
		Stream:         o.stream,
		Consumer:       o.name,
		Domain:         o.srv.getOpts().JetStreamDomain,
		PinnedClientId: o.currentPinId,
		Group:          group,
	}

	subj := JSAdvisoryConsumerPinnedPre + "." + o.stream + "." + o.name
	o.sendAdvisory(subj, e)

}
func (o *consumer) sendUnpinnedAdvisoryLocked(group string, reason string) {
	e := JSConsumerGroupUnpinnedAdvisory{
		TypedEvent: TypedEvent{
			Type: JSConsumerGroupUnpinnedAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Account:  o.acc.Name,
		Stream:   o.stream,
		Consumer: o.name,
		Domain:   o.srv.getOpts().JetStreamDomain,
		Group:    group,
		Reason:   reason,
	}

	subj := JSAdvisoryConsumerUnpinnedPre + "." + o.stream + "." + o.name
	o.sendAdvisory(subj, e)

}

func (o *consumer) sendCreateAdvisory() {
	o.mu.Lock()
	defer o.mu.Unlock()

	e := JSConsumerActionAdvisory{
		TypedEvent: TypedEvent{
			Type: JSConsumerActionAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   o.stream,
		Consumer: o.name,
		Action:   CreateEvent,
		Domain:   o.srv.getOpts().JetStreamDomain,
	}

	subj := JSAdvisoryConsumerCreatedPre + "." + o.stream + "." + o.name
	o.sendAdvisory(subj, e)
}

func (o *consumer) sendPauseAdvisoryLocked(cfg *ConsumerConfig) {
	e := JSConsumerPauseAdvisory{
		TypedEvent: TypedEvent{
			Type: JSConsumerPauseAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   o.stream,
		Consumer: o.name,
		Domain:   o.srv.getOpts().JetStreamDomain,
	}

	if cfg.PauseUntil != nil {
		e.PauseUntil = *cfg.PauseUntil
		e.Paused = time.Now().Before(e.PauseUntil)
	}

	subj := JSAdvisoryConsumerPausePre + "." + o.stream + "." + o.name
	o.sendAdvisory(subj, e)
}

// Created returns created time.
func (o *consumer) createdTime() time.Time {
	o.mu.Lock()
	created := o.created
	o.mu.Unlock()
	return created
}

// Internal to allow creation time to be restored.
func (o *consumer) setCreatedTime(created time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.created = created
	if fs, ok := o.store.(*consumerFileStore); ok {
		fs.setCreatedTime(created)
	}
}

// This will check for extended interest in a subject. If we have local interest we just return
// that, but in the absence of local interest and presence of gateways or service imports we need
// to check those as well.
func (o *consumer) hasDeliveryInterest(localInterest bool) bool {
	o.mu.RLock()
	mset := o.mset
	if mset == nil {
		o.mu.RUnlock()
		return false
	}
	acc := o.acc
	deliver := o.cfg.DeliverSubject
	o.mu.RUnlock()

	if localInterest {
		return true
	}

	// If we are here check gateways.
	if s := acc.srv; s != nil && s.hasGatewayInterest(acc.Name, deliver) {
		return true
	}
	return false
}

func (s *Server) hasGatewayInterest(account, subject string) bool {
	gw := s.gateway
	if !gw.enabled {
		return false
	}
	gw.RLock()
	defer gw.RUnlock()
	for _, gwc := range gw.outo {
		psi, qr := gwc.gatewayInterest(account, stringToBytes(subject))
		if psi || qr != nil {
			return true
		}
	}
	return false
}

// This processes an update to the local interest for a deliver subject.
func (o *consumer) updateDeliveryInterest(localInterest bool) bool {
	interest := o.hasDeliveryInterest(localInterest)

	o.mu.Lock()
	defer o.mu.Unlock()

	mset := o.mset
	if mset == nil || o.isPullMode() {
		return false
	}

	if interest && !o.active {
		o.signalNewMessages()
	}
	// Update active status, if not active clear any queue group we captured.
	if o.active = interest; !o.active {
		o.qgroup = _EMPTY_
	} else {
		o.checkQueueInterest()
	}

	// If the delete timer has already been set do not clear here and return.
	// Note that durable can now have an inactive threshold, so don't check
	// for durable status, instead check for dthresh > 0.
	if o.dtmr != nil && o.dthresh > 0 && !interest {
		return true
	}

	// Stop and clear the delete timer always.
	stopAndClearTimer(&o.dtmr)

	// If we do not have interest anymore and have a delete threshold set, then set
	// a timer to delete us. We wait for a bit in case of server reconnect.
	if !interest && o.dthresh > 0 {
		o.dtmr = time.AfterFunc(o.dthresh, o.deleteNotActive)
		return true
	}
	return false
}

const (
	defaultConsumerNotActiveStartInterval = 30 * time.Second
	defaultConsumerNotActiveMaxInterval   = 5 * time.Minute
)

var (
	consumerNotActiveStartInterval = defaultConsumerNotActiveStartInterval
	consumerNotActiveMaxInterval   = defaultConsumerNotActiveMaxInterval
)

// deleteNotActive must only be called from time.AfterFunc or in its own
// goroutine, as it can block on clean-up.
func (o *consumer) deleteNotActive() {
	// Take a copy of these when the goroutine starts, mostly it avoids a
	// race condition with tests that modify these consts, such as
	// TestJetStreamClusterGhostEphemeralsAfterRestart.
	cnaMax := consumerNotActiveMaxInterval
	cnaStart := consumerNotActiveStartInterval

	o.mu.Lock()
	if o.mset == nil || !o.isLeader() {
		o.mu.Unlock()
		return
	}
	// Push mode just look at active.
	if o.isPushMode() {
		// If we are active simply return.
		if o.active {
			o.mu.Unlock()
			return
		}
	} else {
		// Pull mode.
		// Check if we still have valid requests waiting. This also expires
		// requests, which updates the last activity, so must be checked first.
		if o.checkWaitingForInterest() {
			if o.dtmr != nil {
				o.dtmr.Reset(o.dthresh)
			} else {
				o.dtmr = time.AfterFunc(o.dthresh, o.deleteNotActive)
			}
			o.mu.Unlock()
			return
		}
		elapsed := time.Since(o.waiting.last)
		if elapsed < o.dthresh {
			// These need to keep firing so reset but use delta.
			if o.dtmr != nil {
				o.dtmr.Reset(o.dthresh - elapsed)
			} else {
				o.dtmr = time.AfterFunc(o.dthresh-elapsed, o.deleteNotActive)
			}
			o.mu.Unlock()
			return
		}

		// We now know we have no waiting requests, and our last request was long ago.
		// However, based on AckWait the consumer could still be actively processing,
		// even if we haven't been informed if there were no acks in the meantime.
		// We must wait for the message that expires last and start counting down the
		// inactive threshold from there.
		now := time.Now().UnixNano()
		l := len(o.cfg.BackOff)
		var delay time.Duration
		var ackWait time.Duration
		for seq, p := range o.pending {
			if l == 0 {
				ackWait = o.ackWait(0)
			} else {
				bi := int(o.rdc[seq])
				if bi < 0 {
					bi = 0
				} else if bi >= l {
					bi = l - 1
				}
				ackWait = o.ackWait(o.cfg.BackOff[bi])
			}
			if ts := p.Timestamp + ackWait.Nanoseconds() + o.dthresh.Nanoseconds(); ts > now {
				delay = max(delay, time.Duration(ts-now))
			}
		}
		// We'll wait for the latest time we expect an ack, plus the inactive threshold.
		// Acknowledging a message will reset this back down to just the inactive threshold.
		if delay > 0 {
			if o.dtmr != nil {
				o.dtmr.Reset(delay)
			} else {
				o.dtmr = time.AfterFunc(delay, o.deleteNotActive)
			}
			o.mu.Unlock()
			return
		}
	}

	s, js := o.mset.srv, o.srv.js.Load()
	acc, stream, name, isDirect := o.acc.Name, o.stream, o.name, o.direct
	// Capture our own view of the assignment while we still hold the lock.
	ca := o.ca
	var qch, cqch chan struct{}
	if o.srv != nil {
		qch = o.srv.quitCh
	}
	oqch := o.qch
	o.mu.Unlock()
	if js != nil {
		cqch = js.clusterQuitC()
	}

	// Useful for pprof.
	setGoRoutineLabels(pprofLabels{
		"account":  acc,
		"stream":   stream,
		"consumer": name,
	})

	// If we are clustered, check if we still have this consumer assigned.
	// If we do, ask the meta leader to delete us.
	if !isDirect && s.JetStreamIsClustered() {
		js.mu.RLock()
		nca := js.consumerAssignment(acc, stream, name)
		// Only request the delete if the meta-layer assignment still refers to
		// the consumer we captured, otherwise we'd be racing a recreated
		// consumer with the same name.
		requested := js.cluster != nil && ca != nil && ca.sameIdentity(nca)
		js.mu.RUnlock()

		if requested {
			// The creation time is part of the consumer's identity, so the meta leader
			// can equally reject the request if the consumer got recreated meanwhile.
			s.sendConsumerAssignmentDelete(acc, stream, name, ca.Created)

			// Check to make sure we went away.
			// Don't think this needs to be a monitored go routine.
			jitter := time.Duration(rand.Int64N(int64(cnaStart)))
			interval := cnaStart + jitter
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
				case <-qch:
					return
				case <-cqch:
					return
				case <-oqch:
					// The consumer has stopped already, likely by an earlier delete proposal being applied.
					return
				}
				js.mu.RLock()
				if js.shuttingDown {
					js.mu.RUnlock()
					return
				}
				nca = js.consumerAssignment(acc, stream, name)
				// Make sure this is the same consumer assignment, and not a new consumer with the same name.
				match := ca.sameIdentity(nca)
				js.mu.RUnlock()
				if match {
					s.Warnf("Consumer assignment for '%s > %s > %s' not cleaned up, retrying", acc, stream, name)
					s.sendConsumerAssignmentDelete(acc, stream, name, ca.Created)
					if interval < cnaMax {
						interval *= 2
						ticker.Reset(interval)
					}
					continue
				}
				// We saw that consumer has been removed, all done.
				return
			}
		}
	} else {
		// Otherwise, we can delete locally. Either a consumer that's not tracked
		// by the meta layer (direct), or a standalone non-clustered server.
		o.delete()
	}
}

func (o *consumer) watchGWinterest() {
	pa := o.isActive()
	// If there is no local interest...
	if o.hasNoLocalInterest() {
		o.updateDeliveryInterest(false)
		if !pa && o.isActive() {
			o.signalNewMessages()
		}
	}

	// We want this to always be running so we can also pick up on interest returning.
	o.mu.Lock()
	if o.gwdtmr != nil {
		o.gwdtmr.Reset(time.Second)
	} else {
		stopAndClearTimer(&o.gwdtmr)
		o.gwdtmr = time.AfterFunc(time.Second, func() { o.watchGWinterest() })
	}
	o.mu.Unlock()
}

// Config returns the consumer's configuration.
func (o *consumer) config() ConsumerConfig {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.cfg
}

// Check if we have hit max deliveries. If so do notification and cleanup.
// Return whether or not the max was hit.
// Lock should be held.
func (o *consumer) hasMaxDeliveries(seq uint64) bool {
	if o.maxdc == 0 {
		return false
	}
	if dc := o.deliveryCount(seq); dc >= o.maxdc {
		// We have hit our max deliveries for this sequence.
		// Only send the advisory once.
		o.notifyDeliveryExceeded(seq, o.rdc[seq]+1)
		// Determine if we signal to start flow of messages again.
		if o.maxp > 0 && len(o.pending) >= o.maxp {
			o.signalNewMessages()
		}
		// Make sure to remove from pending.
		if p, ok := o.pending[seq]; ok && p != nil {
			delete(o.pending, seq)
			// Increment by one, since the delivery count hasn't been increased above.
			o.updateDelivered(p.Sequence, seq, dc+1, p.Timestamp)
			o.moveAckFloor(p.Sequence, seq)
		}
		// Ensure redelivered state is set, if not already.
		if o.rdc == nil {
			o.rdc = make(map[uint64]uint64)
		}
		o.rdc[seq] = dc
		return true
	}
	return false
}

// Force expiration of all pending.
// Lock should be held.
func (o *consumer) forceExpirePending() {
	var expired []uint64
	for seq := range o.pending {
		if !o.onRedeliverQueue(seq) && !o.hasMaxDeliveries(seq) {
			expired = append(expired, seq)
		}
	}
	if len(expired) > 0 {
		slices.Sort(expired)
		o.addToRedeliverQueue(expired...)
		// Now we should update the timestamp here since we are redelivering.
		// We will use an incrementing time to preserve order for any other redelivery.
		off := time.Now().UnixNano() - o.pending[expired[0]].Timestamp
		for _, seq := range expired {
			if p, ok := o.pending[seq]; ok && p != nil {
				p.Timestamp += off
			}
		}
		o.resetPtmr(o.ackWait(0))
	}
	o.signalNewMessages()
}

// Acquire proper locks and update rate limit.
// Will use what is in config.
func (o *consumer) setRateLimitNeedsLocks() {
	o.mu.RLock()
	mset := o.mset
	o.mu.RUnlock()

	if mset == nil {
		return
	}

	mset.mu.RLock()
	o.mu.Lock()
	o.setRateLimit(o.cfg.RateLimit)
	o.mu.Unlock()
	mset.mu.RUnlock()
}

// Set the rate limiter
// Both mset and consumer lock should be held.
func (o *consumer) setRateLimit(bps uint64) {
	if bps == 0 {
		o.rlimit = nil
		return
	}

	// TODO(dlc) - Make sane values or error if not sane?
	// We are configured in bits per sec so adjust to bytes.
	// Float division, otherwise below 8 bps the limit is 0 and delivery stalls forever.
	rl := rate.Limit(float64(bps) / 8)
	mset := o.mset
	// The consumer is stopping.
	if mset == nil {
		return
	}

	// Burst should be set to maximum msg size for this account, etc.
	var burst int
	// We don't need to get cfgMu's rlock here since this function
	// is already invoked under mset.mu.RLock(), which superseeds cfgMu.
	if mset.cfg.MaxMsgSize > 0 {
		burst = int(mset.cfg.MaxMsgSize)
	} else {
		acc := mset.jsa.account
		acc.mu.RLock()
		mpay := acc.mpay
		acc.mu.RUnlock()
		if mpay > 0 {
			burst = int(mpay)
		} else {
			burst = int(acc.srv.getOpts().MaxPayload)
		}
	}
	// A max_payload of -1 means no limit, but the burst must be positive.
	if burst <= 0 {
		burst = MAX_PAYLOAD_SIZE
	}

	// Update in place to keep the current tokens, so the bucket is not refilled.
	if o.rlimit != nil {
		now := time.Now()
		o.rlimit.SetLimitAt(now, rl)
		o.rlimit.SetBurstAt(now, burst)
		return
	}
	o.rlimit = rate.NewLimiter(rl, burst)
}

// Check if new consumer config allowed vs old.
func (acc *Account) checkNewConsumerConfig(cfg, ncfg *ConsumerConfig) error {
	if reflect.DeepEqual(cfg, ncfg) {
		return nil
	}
	// Something different, so check since we only allow certain things to be updated.
	if cfg.DeliverPolicy != ncfg.DeliverPolicy {
		return errors.New("deliver policy can not be updated")
	}
	if cfg.MemoryStorage != ncfg.MemoryStorage {
		return errors.New("storage type can not be updated")
	}
	// Direct and Sourcing classify the consumer for its whole lifetime, which the
	// stream relies on when walking its consumer list, so they can not change.
	if cfg.Direct != ncfg.Direct {
		return errors.New("direct can not be updated")
	}
	if cfg.Sourcing != ncfg.Sourcing {
		return errors.New("sourcing can not be updated")
	}
	if cfg.OptStartSeq != ncfg.OptStartSeq {
		return errors.New("start sequence can not be updated")
	}
	if cfg.OptStartTime != nil && ncfg.OptStartTime != nil {
		// Both have start times set, compare them directly:
		if !cfg.OptStartTime.Equal(*ncfg.OptStartTime) {
			return errors.New("start time can not be updated")
		}
	} else if cfg.OptStartTime != nil || ncfg.OptStartTime != nil {
		// At least one start time is set and the other is not
		return errors.New("start time can not be updated")
	}
	if cfg.AckPolicy != ncfg.AckPolicy {
		return errors.New("ack policy can not be updated")
	}
	if cfg.ReplayPolicy != ncfg.ReplayPolicy {
		return errors.New("replay policy can not be updated")
	}
	if cfg.Heartbeat != ncfg.Heartbeat {
		return errors.New("heart beats can not be updated")
	}
	if cfg.FlowControl != ncfg.FlowControl {
		return errors.New("flow control can not be updated")
	}

	// Deliver Subject is conditional on if its bound.
	if cfg.DeliverSubject != ncfg.DeliverSubject {
		if cfg.DeliverSubject == _EMPTY_ {
			return errors.New("can not update pull consumer to push based")
		}
		if ncfg.DeliverSubject == _EMPTY_ {
			return errors.New("can not update push consumer to pull based")
		}
		if acc.sl.HasInterest(cfg.DeliverSubject) {
			return NewJSConsumerNameExistError()
		}
	}

	if cfg.MaxWaiting != ncfg.MaxWaiting {
		return errors.New("max waiting can not be updated")
	}

	// Check if BackOff is defined, MaxDeliver is within range.
	if lbo := len(ncfg.BackOff); lbo > 0 && ncfg.MaxDeliver != -1 && lbo > ncfg.MaxDeliver {
		return NewJSConsumerMaxDeliverBackoffError()
	}

	return nil
}

// Update the config based on the new config, or error if update not allowed.
func (o *consumer) updateConfig(cfg *ConsumerConfig) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed || o.mset == nil {
		return NewJSConsumerDoesNotExistError()
	}

	if err := o.acc.checkNewConsumerConfig(&o.cfg, cfg); err != nil {
		return err
	}

	// Make sure we always store PauseUntil in UTC.
	if cfg.PauseUntil != nil {
		utc := (*cfg.PauseUntil).UTC()
		cfg.PauseUntil = &utc
	}

	if o.store != nil {
		// Update local state always.
		if err := o.store.UpdateConfig(cfg); err != nil {
			return err
		}
	}

	// DeliverSubject
	if cfg.DeliverSubject != o.cfg.DeliverSubject {
		o.updateDeliverSubjectLocked(cfg.DeliverSubject)
	}

	// MaxAckPending
	if cfg.MaxAckPending != o.cfg.MaxAckPending {
		o.maxp = cfg.MaxAckPending
		o.signalNewMessages()
		// If MaxAckPending is lowered, we could have allocated a pending deliveries map of larger size.
		// Reset it here, so we can shrink the map.
		if cfg.MaxAckPending < o.cfg.MaxAckPending {
			o.resetPendingDeliveries()
		}
	}
	// AckWait
	if cfg.AckWait != o.cfg.AckWait {
		if o.ptmr != nil {
			o.resetPtmr(100 * time.Millisecond)
		}
	}
	// Rate Limit
	if cfg.RateLimit != o.cfg.RateLimit {
		// We need both locks here so do in Go routine.
		go o.setRateLimitNeedsLocks()
	}
	if cfg.SampleFrequency != o.cfg.SampleFrequency {
		s := strings.TrimSuffix(cfg.SampleFrequency, "%")
		if sampleFreq, err := strconv.ParseInt(s, 10, 32); err == nil {
			o.sfreq = int32(sampleFreq)
		}
	}
	// Set MaxDeliver if changed
	if cfg.MaxDeliver != o.cfg.MaxDeliver {
		// MaxDeliver is negative (-1) when infinite.
		o.maxdc = uint64(max(cfg.MaxDeliver, 0))
	}
	// Set InactiveThreshold if changed.
	if val := cfg.InactiveThreshold; val != o.cfg.InactiveThreshold {
		o.updateInactiveThreshold(cfg)
		stopAndClearTimer(&o.dtmr)
		// Restart timer only if we are the leader.
		if o.isLeader() && o.dthresh > 0 {
			o.dtmr = time.AfterFunc(o.dthresh, o.deleteNotActive)
		}
	}
	// Check whether the pause has changed
	{
		var old, new time.Time
		if o.cfg.PauseUntil != nil {
			old = *o.cfg.PauseUntil
		}
		if cfg.PauseUntil != nil {
			new = *cfg.PauseUntil
		}
		if !old.Equal(new) {
			o.updatePauseState(cfg)
			if o.isLeader() {
				o.sendPauseAdvisoryLocked(cfg)
			}
		}
	}

	// Check for Subject Filters update.
	newSubjects := gatherSubjectFilters(cfg.FilterSubject, cfg.FilterSubjects)
	updatedFilters := !subjectSliceEqual(newSubjects, o.subjf.subjects())
	if updatedFilters {
		newSubjf := make(subjectFilters, 0, len(newSubjects))
		for _, newFilter := range newSubjects {
			fs := &subjectFilter{
				subject:          newFilter,
				hasWildcard:      subjectHasWildcard(newFilter),
				tokenizedSubject: tokenizeSubjectIntoSlice(nil, newFilter),
			}
			newSubjf = append(newSubjf, fs)
		}
		// Make sure we have correct signaling setup.
		// Consumer lock can not be held.
		mset := o.mset
		o.mu.Unlock()
		mset.swapSigSubs(o, newSubjf.subjects())
		o.mu.Lock()

		// When we're done with signaling, we can replace the subjects.
		// If filters were removed, set `o.subjf` to nil.
		if len(newSubjf) == 0 {
			o.subjf = nil
			o.filters = nil
		} else {
			o.subjf = newSubjf
			if len(o.subjf) == 1 {
				o.filters = nil
			} else {
				o.filters = gsl.NewSublist[struct{}]()
				for _, filter := range o.subjf {
					o.filters.Insert(filter.subject, struct{}{})
				}
			}
		}
	}

	// Record new config for others that do not need special handling.
	// Allowed but considered no-op, [Description, SampleFrequency, MaxWaiting, HeadersOnly]
	o.cfg = *cfg

	if cfg.Sourcing && (!o.srv.JetStreamIsClustered() && o.srv.standAloneMode()) {
		o.resetStartingSeqLocked(0, _EMPTY_, false, false)
	}
	if updatedFilters {
		// Cleanup messages that lost interest.
		if o.retention == InterestPolicy {
			// Capture mset under the lock.
			mset := o.mset
			o.mu.Unlock()
			if mset != nil {
				o.cleanupNoInterestMessages(mset, false)
			}
			o.mu.Lock()
		}

		// Re-calculate num pending on update.
		o.streamNumPending()
	}

	return nil
}

// This is a config change for the delivery subject for a
// push based consumer.
func (o *consumer) updateDeliverSubject(newDeliver string) {
	// Update the config and the dsubj
	o.mu.Lock()
	defer o.mu.Unlock()
	o.updateDeliverSubjectLocked(newDeliver)
}

// This is a config change for the delivery subject for a
// push based consumer.
func (o *consumer) updateDeliverSubjectLocked(newDeliver string) {
	if o.closed || o.isPullMode() || o.cfg.DeliverSubject == newDeliver {
		return
	}

	// Force redeliver of all pending on change of delivery subject.
	if len(o.pending) > 0 {
		o.forceExpirePending()
	}

	o.acc.sl.clearNotification(o.dsubj, o.cfg.DeliverGroup, o.inch)
	o.dsubj, o.cfg.DeliverSubject = newDeliver, newDeliver
	// When we register new one it will deliver to update state loop.
	o.acc.sl.registerNotification(newDeliver, o.cfg.DeliverGroup, o.inch)
}

// Check that configs are equal but allow delivery subjects to be different.
func configsEqualSansDelivery(a, b ConsumerConfig) bool {
	// These were copied in so can set Delivery here.
	a.DeliverSubject, b.DeliverSubject = _EMPTY_, _EMPTY_
	return reflect.DeepEqual(a, b)
}

// Helper to send a reply to an ack.
func (o *consumer) sendAckReply(subj string) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	o.outq.sendMsg(subj, nil)
}

type jsAckMsg struct {
	subject string
	reply   string
	hdr     int
	msg     []byte
}

var jsAckMsgPool sync.Pool

func newJSAckMsg(subj, reply string, hdr int, msg []byte) *jsAckMsg {
	var m *jsAckMsg
	am := jsAckMsgPool.Get()
	if am != nil {
		m = am.(*jsAckMsg)
	} else {
		m = &jsAckMsg{}
	}
	// When getting something from a pool it is critical that all fields are
	// initialized. Doing this way guarantees that if someone adds a field to
	// the structure, the compiler will fail the build if this line is not updated.
	(*m) = jsAckMsg{subj, reply, hdr, msg}
	return m
}

func (am *jsAckMsg) returnToPool() {
	if am == nil {
		return
	}
	am.subject, am.reply, am.hdr, am.msg = _EMPTY_, _EMPTY_, -1, nil
	jsAckMsgPool.Put(am)
}

// Push the ack message to the consumer's ackMsgs queue
func (o *consumer) pushAck(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	atomic.AddInt64(&o.awl, 1)
	o.ackMsgs.push(newJSAckMsg(subject, reply, c.pa.hdr, copyBytes(rmsg)))
}

// Processes a message for the ack reply subject delivered with a message.
func (o *consumer) processAck(subject, reply string, hdr int, rmsg []byte) {
	defer atomic.AddInt64(&o.awl, -1)

	var msg []byte
	if hdr > 0 {
		msg = rmsg[hdr:]
	} else {
		msg = rmsg
	}

	sseq, dseq, dc, _, _ := ackReplyInfo(subject)

	skipAckReply := sseq == 0

	switch {
	case len(msg) == 0, bytes.Equal(msg, AckAck), bytes.Equal(msg, AckOK):
		if !o.processAckMsg(sseq, dseq, dc, reply, true) {
			// We handle replies for acks in updateAcks
			skipAckReply = true
		}
	case bytes.HasPrefix(msg, AckNext):
		o.processAckMsg(sseq, dseq, dc, _EMPTY_, true)
		o.processNextMsgRequest(reply, msg[len(AckNext):])
		skipAckReply = true
	case bytes.HasPrefix(msg, AckNak):
		o.processNak(sseq, dseq, dc, msg)
	case bytes.Equal(msg, AckProgress):
		o.progressUpdate(sseq)
	case bytes.HasPrefix(msg, AckTerm):
		var reason string
		if buf := msg[len(AckTerm):]; len(buf) > 0 {
			reason = string(bytes.TrimSpace(buf))
		}
		if !o.processTerm(sseq, dseq, dc, reason, reply) {
			// We handle replies for acks in updateAcks
			skipAckReply = true
		}
	}

	// Ack the ack if requested.
	if len(reply) > 0 && !skipAckReply {
		o.sendAckReply(reply)
	}
}

// Used to process a working update to delay redelivery.
func (o *consumer) progressUpdate(seq uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if p, ok := o.pending[seq]; ok {
		p.Timestamp = time.Now().UnixNano()
		// Update store system.
		o.updateDelivered(p.Sequence, seq, 1, p.Timestamp)
	}
}

// Lock should be held.
func (o *consumer) updateSkipped(seq uint64) {
	// Clustered mode and R>1 only.
	if o.node == nil || !o.isLeader() {
		return
	}
	var b [1 + 8]byte
	b[0] = byte(updateSkipOp)
	var le = binary.LittleEndian
	le.PutUint64(b[1:], seq)
	o.propose(b[:])
}

// resetRequest tracks a reset request that still needs to be responded to
// once the reset has been applied.
type resetRequest struct {
	internal bool // Respond on the internal sys account.
	identity bool // Include the stream identity header in the response.
}

func (o *consumer) resetStartingSeq(seq uint64, reply string, internal, identity bool) (uint64, bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.resetStartingSeqLocked(seq, reply, internal, identity)
}

// Lock should be held.
func (o *consumer) resetStartingSeqLocked(seq uint64, reply string, internal, identity bool) (uint64, bool, error) {
	// Reset to a specific sequence, or back to the ack floor.
	if seq == 0 {
		seq = o.asflr + 1
	} else if o.cfg.DeliverPolicy == DeliverAll {
		// Always allowed.
		goto VALID
	} else if o.cfg.DeliverPolicy == DeliverByStartSequence {
		// Only allowed if not going below what's configured.
		if seq < o.cfg.OptStartSeq {
			return 0, false, errors.New("below start seq")
		}
		goto VALID
	} else if o.cfg.DeliverPolicy == DeliverByStartTime && o.mset != nil {
		// Only allowed if not going below what's configured.
		nseq := o.mset.store.GetSeqFromTime(*o.cfg.OptStartTime)
		if seq < nseq {
			return 0, false, errors.New("below start time")
		}
		goto VALID
	} else {
		return 0, false, errors.New("not allowed")
	}

VALID:
	// Must be a minimum of 1.
	if seq <= 0 {
		seq = 1
	}
	// The replicated path requires quorum first before the reset actually takes effect.
	if o.node != nil {
		if !o.isLeader() {
			return 0, false, nil
		}
		b := make([]byte, 1+8+len(reply))
		b[0] = byte(resetSeqOp)
		var le = binary.LittleEndian
		le.PutUint64(b[1:], seq)
		copy(b[1+8:], reply)
		o.propose(b[:])
		if reply != _EMPTY_ {
			if o.rsm == nil {
				o.rsm = make(map[string]resetRequest, 1)
			}
			o.rsm[reply] = resetRequest{internal: internal, identity: identity}
		}
		return seq, false, nil
	}
	recalcPending := o.resetLocalStartingSeq(seq)
	if o.store != nil {
		o.store.Reset(seq - 1)
		// Cleanup messages that lost interest.
		if o.retention == InterestPolicy {
			if mset := o.mset; mset != nil {
				o.mu.Unlock()
				ss := mset.state()
				o.checkStateForInterestStream(&ss)
				o.mu.Lock()
			}
		}
	}
	// Recalculate pending, and re-trigger message delivery.
	if recalcPending {
		o.streamNumPending()
	}
	o.signalNewMessages()
	return seq, true, nil
}

// Lock should be held.
func (o *consumer) resetLocalStartingSeq(seq uint64) bool {
	recalcPending := o.sseq != seq || len(o.pending) > 0
	// A reset back to the ack floor can be optimized since we know how many were pending.
	// But only for AckAll/AckFlowControl, since those guarantee no out-of-order acks.
	if recalcPending && seq == o.asflr+1 && o.isLeader() &&
		(o.cfg.AckPolicy == AckAll || o.cfg.AckPolicy == AckFlowControl) {
		o.npc += int64(len(o.pending))
		recalcPending = false
	}
	o.pending, o.rdc = nil, nil
	o.rdq = nil
	o.rdqi.Empty()
	o.sseq, o.dseq = seq, 1
	o.adflr, o.asflr = o.dseq-1, o.sseq-1
	o.ldt, o.lat = time.Time{}, time.Time{}
	return recalcPending
}

func (o *consumer) loopAndForwardProposals(node RaftNode, qch, pch chan struct{}, term uint64) {
	if node == nil || qch == nil || pch == nil {
		return
	}

	forwardProposals := func() error {
		o.mu.Lock()
		if node == nil || !node.Leader() || o.term != term {
			o.mu.Unlock()
			return errors.New("no longer leader")
		}
		proposal := o.phead
		o.phead, o.ptail = nil, nil
		o.mu.Unlock()
		// 256k max for now per batch.
		const maxBatch = 256 * 1024
		var entries []*Entry
		for sz := 0; proposal != nil; proposal = proposal.next {
			entries = append(entries, newEntry(EntryNormal, proposal.data))
			sz += len(proposal.data)
			if sz > maxBatch {
				node.ProposeMulti(term, entries)
				// We need to re-create `entries` because there is a reference
				// to it in the node's pae map.
				sz, entries = 0, nil
			}
		}
		if len(entries) > 0 {
			node.ProposeMulti(term, entries)
		}
		return nil
	}

	// In case we have anything pending on entry.
	forwardProposals()

	for {
		select {
		case <-qch:
			forwardProposals()
			return
		case <-pch:
			if err := forwardProposals(); err != nil {
				return
			}
		}
	}
}

// Lock should be held.
func (o *consumer) propose(entry []byte) {
	p := &proposal{data: entry}
	if o.phead == nil {
		o.phead = p
	} else {
		o.ptail.next = p
	}
	o.ptail = p

	// Kick our looper routine.
	select {
	case o.pch <- struct{}{}:
	default:
	}
}

// Lock should be held.
func (o *consumer) updateDelivered(dseq, sseq, dc uint64, ts int64) {
	// Clustered mode and R>1.
	if o.node != nil {
		// Inline for now, use variable compression.
		var b [4*binary.MaxVarintLen64 + 1]byte
		b[0] = byte(updateDeliveredOp)
		n := 1
		n += binary.PutUvarint(b[n:], dseq)
		n += binary.PutUvarint(b[n:], sseq)
		n += binary.PutUvarint(b[n:], dc)
		n += binary.PutVarint(b[n:], ts)
		o.propose(b[:n])
	} else if o.store != nil {
		o.store.UpdateDelivered(dseq, sseq, dc, ts)
	}
	// Update activity.
	o.ldt = time.Now()
}

// Used to remember a pending ack reply in a replicated consumer.
// Lock should be held.
func (o *consumer) addAckReply(sseq uint64, reply string) {
	if o.replies == nil {
		o.replies = make(map[uint64]string)
	}
	o.replies[sseq] = reply
}

// Used to remember messages that need to be sent for a replicated consumer, after delivered quorum.
// Lock should be held.
func (o *consumer) addReplicatedQueuedMsg(pmsg *jsPubMsg) {
	// Is not explicitly limited in size, but will at most hold maximum ack pending.
	if o.pendingDeliveries == nil {
		o.pendingDeliveries = make(map[uint64]*jsPubMsg)
	}
	o.pendingDeliveries[pmsg.seq] = pmsg

	// Is not explicitly limited in size, but will at most hold maximum waiting requests.
	if o.waitingDeliveries == nil {
		o.waitingDeliveries = make(map[string]*waitingDelivery)
	}
	if wd, ok := o.waitingDeliveries[pmsg.dsubj]; ok {
		wd.seq = pmsg.seq
	} else {
		wd := wdPool.Get().(*waitingDelivery)
		wd.seq = pmsg.seq
		o.waitingDeliveries[pmsg.dsubj] = wd
	}
}

// Lock should be held.
func (o *consumer) updateAcks(dseq, sseq uint64, reply string) {
	if o.node != nil {
		// Inline for now, use variable compression.
		var b [2*binary.MaxVarintLen64 + 1]byte
		b[0] = byte(updateAcksOp)
		n := 1
		n += binary.PutUvarint(b[n:], dseq)
		n += binary.PutUvarint(b[n:], sseq)
		o.propose(b[:n])
		if reply != _EMPTY_ {
			o.addAckReply(sseq, reply)
		}
	} else if o.store != nil {
		o.store.UpdateAcks(dseq, sseq)
		if reply != _EMPTY_ {
			// Already locked so send direct.
			o.outq.sendMsg(reply, nil)
		}
	}
	// Update activity.
	o.lat = time.Now()
}

// Communicate to the cluster an addition of a pending request.
// Lock should be held.
func (o *consumer) addClusterPendingRequest(reply string) {
	if o.node == nil || !o.pendingRequestsOk() {
		return
	}
	b := make([]byte, len(reply)+1)
	b[0] = byte(addPendingRequest)
	copy(b[1:], reply)
	o.propose(b)
}

// Communicate to the cluster a removal of a pending request.
// Lock should be held.
func (o *consumer) removeClusterPendingRequest(reply string) {
	if o.node == nil || !o.pendingRequestsOk() {
		return
	}
	b := make([]byte, len(reply)+1)
	b[0] = byte(removePendingRequest)
	copy(b[1:], reply)
	o.propose(b)
}

// Set whether or not we can send pending requests to followers.
func (o *consumer) setPendingRequestsOk(ok bool) {
	o.mu.Lock()
	o.prOk = ok
	o.mu.Unlock()
}

// Lock should be held.
func (o *consumer) pendingRequestsOk() bool {
	return o.prOk
}

// Set whether or not we can send info about pending pull requests to our group.
// Will require all peers have a minimum version.
func (o *consumer) checkAndSetPendingRequestsOk() {
	o.mu.RLock()
	s, isValid := o.srv, o.mset != nil
	o.mu.RUnlock()
	if !isValid {
		return
	}

	if ca := o.consumerAssignment(); ca != nil && len(ca.Group.Peers) > 1 {
		for _, pn := range ca.Group.Peers {
			if si, ok := s.nodeToInfo.Load(pn); ok {
				if !versionAtLeast(si.(nodeInfo).version, 2, 7, 1) {
					// We expect all of our peers to eventually be up to date.
					// So check again in awhile.
					time.AfterFunc(eventsHBInterval, func() { o.checkAndSetPendingRequestsOk() })
					o.setPendingRequestsOk(false)
					return
				}
			}
		}
	}
	o.setPendingRequestsOk(true)
}

// On leadership change make sure we alert the pending requests that they are no longer valid.
func (o *consumer) checkPendingRequests() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.mset == nil || o.outq == nil {
		return
	}
	hdr := []byte("NATS/1.0 409 Leadership Change\r\n\r\n")
	for reply := range o.prm {
		o.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
	}
	o.prm = nil
}

// This will release any pending pull requests if applicable.
// Should be called only by the leader being deleted or stopped.
// Lock should be held.
func (o *consumer) releaseAnyPendingRequests(isAssigned bool) {
	if o.mset == nil || o.outq == nil || o.waiting.len() == 0 {
		return
	}
	wq := o.waiting
	for wr := wq.head; wr != nil; {
		if !isAssigned {
			hdr := []byte("NATS/1.0 409 Consumer Deleted\r\n\r\n")
			o.outq.send(newJSPubMsg(wr.reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		}
		next := wr.next
		wr.recycle()
		wr = next
	}
	// Nil out old queue.
	o.waiting = nil
}

// Process a NAK.
func (o *consumer) processNak(sseq, dseq, dc uint64, nak []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Check for out of range.
	if dseq <= o.adflr || dseq > o.dseq {
		return
	}
	// If we are explicit ack make sure this is still on our pending list.
	if _, ok := o.pending[sseq]; !ok {
		return
	}

	// Deliver an advisory
	e := JSConsumerDeliveryNakAdvisory{
		TypedEvent: TypedEvent{
			Type: JSConsumerDeliveryNakAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:      o.stream,
		Consumer:    o.name,
		ConsumerSeq: dseq,
		StreamSeq:   sseq,
		Deliveries:  dc,
		Domain:      o.srv.getOpts().JetStreamDomain,
	}

	o.sendAdvisory(o.nakEventT, e)

	// Check to see if we have delays attached.
	if len(nak) > len(AckNak) {
		arg := bytes.TrimSpace(nak[len(AckNak):])
		if len(arg) > 0 {
			var d time.Duration
			var err error
			if arg[0] == '{' {
				var nd ConsumerNakOptions
				if err = json.Unmarshal(arg, &nd); err == nil {
					d = nd.Delay
				}
			} else {
				d, err = time.ParseDuration(string(arg))
			}
			if err != nil {
				// Treat this as normal NAK.
				o.srv.Warnf("JetStream consumer '%s > %s > %s' bad NAK delay value: %q", o.acc.Name, o.stream, o.name, arg)
			} else {
				// We have a parsed duration that the user wants us to wait before retrying.
				// Make sure we are not on the rdq.
				o.removeFromRedeliverQueue(sseq)
				if p, ok := o.pending[sseq]; ok {
					// now - ackWait is expired now, so offset from there.
					p.Timestamp = time.Now().Add(-o.cfg.AckWait).Add(d).UnixNano()
					// Update store system which will update followers as well.
					o.updateDelivered(p.Sequence, sseq, dc, p.Timestamp)
					if o.ptmr != nil {
						// Want checkPending to run and figure out the next timer ttl.
						// TODO(dlc) - We could optimize this maybe a bit more and track when we expect the timer to fire.
						o.resetPtmr(10 * time.Millisecond)
					}
				}
				// Nothing else for use to do now so return.
				return
			}
		}
	}

	// If already queued up also ignore.
	if !o.onRedeliverQueue(sseq) {
		o.addToRedeliverQueue(sseq)
	}

	o.signalNewMessages()
}

// Process a TERM
// Returns `true` if the ack was processed in place and the sender can now respond
// to the client, or `false` if there was an error or the ack is replicated (in which
// case the reply will be sent later).
func (o *consumer) processTerm(sseq, dseq, dc uint64, reason, reply string) bool {
	return o.processTermLocked(sseq, dseq, dc, reason, reply, true)
}

func (o *consumer) processTermLocked(sseq, dseq, dc uint64, reason, reply string, needLock bool) bool {
	// Treat like an ack to suppress redelivery.
	ackedInPlace := o.processAckMsgLocked(sseq, dseq, dc, reply, false, needLock)

	if needLock {
		o.mu.Lock()
		defer o.mu.Unlock()
	}

	// Deliver an advisory
	e := JSConsumerDeliveryTerminatedAdvisory{
		TypedEvent: TypedEvent{
			Type: JSConsumerDeliveryTerminatedAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:      o.stream,
		Consumer:    o.name,
		ConsumerSeq: dseq,
		StreamSeq:   sseq,
		Deliveries:  dc,
		Reason:      reason,
		Domain:      o.srv.getOpts().JetStreamDomain,
	}

	subj := JSAdvisoryConsumerMsgTerminatedPre + "." + o.stream + "." + o.name
	o.sendAdvisory(subj, e)
	return ackedInPlace
}

// Introduce a small delay in when timer fires to check pending.
// Allows bursts to be treated in same time frame.
const ackWaitDelay = time.Millisecond

// ackWait returns how long to wait to fire the pending timer.
func (o *consumer) ackWait(next time.Duration) time.Duration {
	if next > 0 {
		return next + ackWaitDelay
	}
	return o.cfg.AckWait + ackWaitDelay
}

func (o *consumer) removeRedeliveredBelow(seq uint64) {
	if seq == 0 {
		return
	}
	o.mu.Lock()
	for sseq := range o.rdc {
		if sseq < seq {
			delete(o.rdc, sseq)
			o.removeFromRedeliverQueue(sseq)
		}
	}
	o.mu.Unlock()

	if o.store != nil {
		o.store.RemoveRedeliveredBelow(seq)
	}
}

// checkRedelivered drops rdq entries at/below asflr or below stream's first sequence.
// But rdc is kept until the message leaves the stream: needAck relies on rdc to mark
// messages past MaxDeliver.
// Lock should be held.
func (o *consumer) checkRedelivered() {
	if o.mset == nil {
		return
	}
	var ss StreamState
	o.mset.store.FastState(&ss)

	var shouldUpdateState bool
	for sseq := range o.rdc {
		if sseq <= o.asflr || sseq < ss.FirstSeq {
			o.removeFromRedeliverQueue(sseq)
		}
		if sseq < ss.FirstSeq {
			delete(o.rdc, sseq)
			shouldUpdateState = true
		}
	}
	if shouldUpdateState {
		if err := o.writeStoreStateUnlocked(); err != nil && o.srv != nil && o.mset != nil && !o.closed {
			s, acc, mset, name := o.srv, o.acc, o.mset, o.name
			s.Warnf("Consumer '%s > %s > %s' error on write store state from check redelivered: %v", acc, mset.getCfgName(), name, err)
		}
	}
}

// This will restore the state from disk.
// Lock should be held.
func (o *consumer) readStoredState() error {
	if o.store == nil {
		return nil
	}
	state, err := o.store.State()
	if err == nil {
		o.applyState(state)
		if len(o.rdc) > 0 {
			o.checkRedelivered()
		}
	}
	return err
}

// Apply the consumer stored state.
// Lock should be held.
func (o *consumer) applyState(state *ConsumerState) {
	if state == nil {
		return
	}

	o.sseq = state.Delivered.Stream + 1
	o.dseq = state.Delivered.Consumer + 1
	o.adflr = state.AckFloor.Consumer
	o.asflr = state.AckFloor.Stream
	o.pending = state.Pending
	o.rdc = state.Redelivered

	// Setup tracking timer if we have restored pending.
	if o.isLeader() && len(o.pending) > 0 {
		// This is on startup or leader change. We want to check pending
		// sooner in case there are inconsistencies etc. Pick between 500ms - 1.5s
		delay := 500*time.Millisecond + time.Duration(rand.Int64N(1000))*time.Millisecond

		// If normal is lower than this just use that.
		if o.cfg.AckWait < delay {
			delay = o.ackWait(0)
		}
		o.resetPtmr(delay)
	}
}

// Sets our store state from another source. Used in clustered mode on snapshot restore.
// Lock should be held.
func (o *consumer) setStoreState(state *ConsumerState) error {
	if state == nil || o.store == nil {
		return nil
	}
	err := o.store.Update(state)
	if err == nil {
		o.applyState(state)
	} else if err == ErrStoreOldUpdate {
		// Our store already has a newer state, which is normal during recovery
		// when the consumer was loaded from disk before the meta snapshot state
		// was applied.
		return nil
	}
	return err
}

// Update our state to the store.
func (o *consumer) writeStoreState() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.writeStoreStateUnlocked()
}

// Update our state to the store.
// Lock should be held.
func (o *consumer) writeStoreStateUnlocked() error {
	if o.store == nil {
		return nil
	}
	state := ConsumerState{
		Delivered: SequencePair{
			Consumer: o.dseq - 1,
			Stream:   o.sseq - 1,
		},
		AckFloor: SequencePair{
			Consumer: o.adflr,
			Stream:   o.asflr,
		},
		Pending:     o.pending,
		Redelivered: o.rdc,
	}
	return o.store.Update(&state)
}

// Returns an initial info. Only applicable for non-clustered consumers.
// We will clear after we return it, so one shot.
func (o *consumer) initialInfo() *ConsumerInfo {
	o.mu.Lock()
	ici := o.ici
	o.ici = nil // gc friendly
	o.mu.Unlock()
	if ici == nil {
		ici = o.info()
	}
	return ici
}

// Clears our initial info.
// Used when we have a leader change in cluster mode but do not send a response.
func (o *consumer) clearInitialInfo() {
	o.mu.Lock()
	o.ici = nil // gc friendly
	o.mu.Unlock()
}

// Info returns our current consumer state.
func (o *consumer) info() *ConsumerInfo {
	return o.infoWithSnap(false)
}

func (o *consumer) infoWithSnap(snap bool) *ConsumerInfo {
	return o.infoWithSnapAndReply(snap, _EMPTY_)
}

func (o *consumer) infoWithSnapAndReply(snap bool, reply string) *ConsumerInfo {
	o.mu.Lock()
	mset := o.mset
	if o.closed || mset == nil || mset.srv == nil {
		o.mu.Unlock()
		return nil
	}
	js := o.js
	if js == nil {
		o.mu.Unlock()
		return nil
	}

	// Capture raftGroup.
	var rg *raftGroup
	if o.ca != nil {
		rg = o.ca.Group
	}

	priorityGroups := []PriorityGroupState{}
	// TODO(jrm): when we introduce supporting many priority groups, we need to update assigning `o.currentNuid` for each group.
	if len(o.cfg.PriorityGroups) > 0 {
		priorityGroups = append(priorityGroups, PriorityGroupState{
			Group:          o.cfg.PriorityGroups[0],
			PinnedClientID: o.currentPinId,
			PinnedTS:       o.pinnedTS,
		})
	}

	np, err := o.checkNumPending()
	if err != nil {
		o.mu.Unlock()
		return nil
	}

	dseq, sseq := o.dseq, o.sseq
	if dseq <= 0 {
		dseq = 1
	}
	if sseq <= 0 {
		sseq = 1
	}

	cfg := o.cfg
	info := &ConsumerInfo{
		Stream:  o.stream,
		Name:    o.name,
		Created: o.created,
		Config:  &cfg,
		Delivered: SequenceInfo{
			Consumer: dseq - 1,
			Stream:   sseq - 1,
		},
		AckFloor: SequenceInfo{
			Consumer: o.adflr,
			Stream:   o.asflr,
		},
		NumAckPending:  len(o.pending),
		NumRedelivered: len(o.rdc),
		NumPending:     np,
		PushBound:      o.isPushMode() && o.active,
		TimeStamp:      time.Now().UTC(),
		PriorityGroups: priorityGroups,
	}
	// Reset redelivered for MaxDeliver 1. Redeliveries are disabled so must not report it (is confusing otherwise).
	// The state does still keep track of these messages.
	if o.cfg.MaxDeliver == 1 {
		info.NumRedelivered = 0
	}
	if o.cfg.PauseUntil != nil {
		p := *o.cfg.PauseUntil
		if info.Paused = time.Now().Before(p); info.Paused {
			info.PauseRemaining = time.Until(p)
		}
	}

	// We always need to pull certain data from our store.
	if o.store != nil {
		state, err := o.store.BorrowState()
		if err != nil {
			o.mu.Unlock()
			return nil
		}

		// If we are the leader we could have o.sseq that is skipped ahead.
		// To maintain consistency in reporting (e.g. jsz) we always take the state for our delivered/ackfloor stream sequence.
		// Only use skipped ahead o.sseq if we're a new consumer and have not yet replicated this state yet.
		leader := o.isLeader()
		if !leader || o.store.HasState() {
			info.Delivered.Consumer, info.Delivered.Stream = state.Delivered.Consumer, state.Delivered.Stream
		}
		info.AckFloor.Consumer, info.AckFloor.Stream = state.AckFloor.Consumer, state.AckFloor.Stream
		if !leader {
			info.NumAckPending = len(state.Pending)
			info.NumRedelivered = len(state.Redelivered)
		}
	}

	// Adjust active based on non-zero etc. Also make UTC here.
	if !o.ldt.IsZero() {
		ldt := o.ldt.UTC() // This copies as well.
		info.Delivered.Last = &ldt
	}
	if !o.lat.IsZero() {
		lat := o.lat.UTC() // This copies as well.
		info.AckFloor.Last = &lat
	}

	// If we are a pull mode consumer, report on number of waiting requests.
	if o.isPullMode() {
		o.processWaiting(false)
		info.NumWaiting = o.waiting.len()
	}
	// If we were asked to snapshot do so here.
	if snap {
		o.ici = info
	}
	sysc := o.sysc
	o.mu.Unlock()

	// Do cluster.
	if rg != nil {
		info.Cluster = js.clusterInfo(rg)
	}

	// If we have a reply subject send the response here.
	if reply != _EMPTY_ && sysc != nil {
		sysc.sendInternalMsg(reply, _EMPTY_, nil, info)
	}

	return info
}

// Will signal us that new messages are available. Will break out of waiting.
func (o *consumer) signalNewMessages() {
	// Kick our new message channel
	select {
	case o.mch <- struct{}{}:
	default:
	}
}

// shouldSample lets us know if we are sampling metrics on acks.
func (o *consumer) shouldSample() bool {
	switch {
	case o.sfreq <= 0:
		return false
	case o.sfreq >= 100:
		return true
	}

	// TODO(ripienaar) this is a tad slow so we need to rethink here, however this will only
	// hit for those with sampling enabled and its not the default
	return rand.Int32N(100) <= o.sfreq
}

func (o *consumer) sampleAck(sseq, dseq, dc uint64) {
	if !o.shouldSample() {
		return
	}

	now := time.Now().UTC()
	unow := now.UnixNano()

	e := JSConsumerAckMetric{
		TypedEvent: TypedEvent{
			Type: JSConsumerAckMetricType,
			ID:   nuid.Next(),
			Time: now,
		},
		Stream:      o.stream,
		Consumer:    o.name,
		ConsumerSeq: dseq,
		StreamSeq:   sseq,
		Delay:       unow - o.pending[sseq].Timestamp,
		Deliveries:  dc,
		Domain:      o.srv.getOpts().JetStreamDomain,
	}

	o.sendAdvisory(o.ackEventT, e)
}

// Process an ACK.
// Returns `true` if the ack was processed in place and the sender can now respond
// to the client, or `false` if there was an error or the ack is replicated (in which
// case the reply will be sent later).
func (o *consumer) processAckMsg(sseq, dseq, dc uint64, reply string, doSample bool) bool {
	return o.processAckMsgLocked(sseq, dseq, dc, reply, doSample, true)
}

func (o *consumer) processAckMsgLocked(sseq, dseq, dc uint64, reply string, doSample bool, needLock bool) bool {
	lock := func() {
		if needLock {
			o.mu.Lock()
		}
	}
	unlock := func() {
		if needLock {
			o.mu.Unlock()
		}
	}
	lock()
	if o.closed {
		unlock()
		return false
	}

	mset := o.mset
	if mset == nil || mset.closed.Load() {
		unlock()
		return false
	}

	// Check if this ack is above the current pointer to our next to deliver.
	// Ignore if it's a flow-controlled consumer, its state could end up further ahead
	// since its state is not replicated before delivery.
	if sseq >= o.sseq && !o.cfg.FlowControl {
		// Let's make sure this is valid.
		// This is only received on the consumer leader, so should never be higher
		// than the last stream sequence. But could happen if we've just become
		// consumer leader, and we are not up-to-date on the stream yet.
		var ss StreamState
		mset.store.FastState(&ss)
		if sseq > ss.LastSeq {
			o.srv.Warnf("JetStream consumer '%s > %s > %s' ACK sequence %d past last stream sequence of %d",
				o.acc.Name, o.stream, o.name, sseq, ss.LastSeq)
			// FIXME(dlc) - For 2.11 onwards should we return an error here to the caller?
		}
		// Even though another leader must have delivered a message with this sequence, we must not adjust
		// the current pointer. This could otherwise result in a stuck consumer, where messages below this
		// sequence can't be redelivered, and we'll have incorrect pending state and ack floors.
		unlock()
		return false
	}

	// Let the owning stream know if we are interest or workqueue retention based.
	// If this consumer is clustered (o.node != nil) this will be handled by
	// processReplicatedAck after the ack has propagated.
	// If we're already holding the lock we can't ack in place, since that will
	// violate lock ordering with respect to the stream.
	ackInPlace := o.node == nil && o.retention != LimitsPolicy && needLock

	var ackAllSeqs []uint64
	var needSignal bool

	switch o.cfg.AckPolicy {
	case AckExplicit:
		if p, ok := o.pending[sseq]; ok {
			if doSample {
				o.sampleAck(sseq, dseq, dc)
			}
			if o.maxp > 0 && len(o.pending) >= o.maxp {
				needSignal = true
			}
			delete(o.pending, sseq)
			// Use the original deliver sequence from our pending record.
			dseq = p.Sequence
			o.moveAckFloor(dseq, sseq)
		}
		delete(o.rdc, sseq)
		o.removeFromRedeliverQueue(sseq)
	case AckAll, AckFlowControl:
		// no-op
		if dseq <= o.adflr || sseq <= o.asflr {
			unlock()
			// Return true to let caller respond back to the client.
			return true
		}
		if o.maxp > 0 && len(o.pending) >= o.maxp {
			needSignal = true
		}
		sgap := sseq - o.asflr
		o.adflr, o.asflr = dseq, sseq

		// Only needed if we ack in place, otherwise we'd never collect anything.
		// At most the pending entries below the ack, don't over-allocate.
		if ackInPlace {
			ackAllSeqs = make([]uint64, 0, min(uint64(len(o.pending)), sgap-1))
		}

		remove := func(seq uint64) {
			// Only collect what was actually delivered to us.
			if ackInPlace && seq != sseq {
				if _, ok := o.pending[seq]; ok {
					ackAllSeqs = append(ackAllSeqs, seq)
				}
			}
			delete(o.pending, seq)
			delete(o.rdc, seq)
			o.removeFromRedeliverQueue(seq)
		}
		// Determine if smarter to walk all of pending vs the sequence range.
		if sgap > uint64(len(o.pending)) {
			for seq := range o.pending {
				if seq <= sseq {
					remove(seq)
				}
			}
		} else {
			for seq := sseq; seq > sseq-sgap && len(o.pending) > 0; seq-- {
				remove(seq)
			}
		}
	case AckNone:
		// FIXME(dlc) - This is error but do we care?
		unlock()
		return ackInPlace
	}

	// No ack replication, so we set reply to "" so that updateAcks does not
	// send the reply. The caller will.
	if ackInPlace {
		reply = _EMPTY_
	}
	// Update underlying store.
	o.updateAcks(dseq, sseq, reply)
	unlock()

	if ackInPlace {
		mset.ackMsg(o, sseq)
		for _, seq := range ackAllSeqs {
			mset.ackMsg(o, seq)
		}
	}

	// If we had max ack pending set and were at limit we need to unblock ourselves.
	if needSignal {
		o.signalNewMessages()
	}
	return ackInPlace
}

// Lock should be held.
func (o *consumer) moveAckFloor(dseq, sseq uint64) {
	// Only move floors if we matched an existing pending.
	if len(o.pending) == 0 {
		o.adflr = o.dseq - 1
		o.asflr = o.sseq - 1
	} else if dseq == o.adflr+1 {
		o.adflr, o.asflr = dseq, sseq
		for ss := sseq + 1; ss < o.sseq; ss++ {
			if p, ok := o.pending[ss]; ok {
				if p.Sequence > 0 {
					o.adflr, o.asflr = p.Sequence-1, ss-1
				}
				break
			}
		}
	}
}

// Determine if this is a truly filtered consumer. Modern clients will place filtered subjects
// even if the stream only has a single non-wildcard subject designation.
// Read lock should be held.
func (o *consumer) isFiltered() bool {
	if o.subjf == nil {
		return false
	}
	// If we are here we want to check if the filtered subject is
	// a direct match for our only listed subject.
	mset := o.mset
	if mset == nil {
		return true
	}

	// Protect access to mset.cfg with the cfgMu mutex.
	mset.cfgMu.RLock()
	msetSubjects := mset.cfg.Subjects
	mset.cfgMu.RUnlock()

	// `isFiltered` need to be performant, so we do
	// as any checks as possible to avoid unnecessary work.
	// Here we avoid iteration over slices if there is only one subject in stream
	// and one filter for the consumer.
	if len(msetSubjects) == 1 && len(o.subjf) == 1 {
		return msetSubjects[0] != o.subjf[0].subject
	}

	// if the list is not equal length, we can return early, as this is filtered.
	if len(msetSubjects) != len(o.subjf) {
		return true
	}

	// if in rare case scenario that user passed all stream subjects as consumer filters,
	// we need to do a more expensive operation.
	// reflect.DeepEqual would return false if the filters are the same, but in different order
	// so it can't be used here.
	cfilters := make(map[string]struct{}, len(o.subjf))
	for _, val := range o.subjf {
		cfilters[val.subject] = struct{}{}
	}
	for _, val := range msetSubjects {
		if _, ok := cfilters[val]; !ok {
			return true
		}
	}
	return false
}

// Check if we need an ack for this store seq.
// This is called for interest based retention streams to remove messages.
func (o *consumer) needAck(sseq uint64, subj string) bool {
	var needAck bool
	var asflr, osseq uint64
	var pending map[uint64]*Pending
	var rdc map[uint64]uint64

	o.mu.RLock()
	defer o.mu.RUnlock()

	isFiltered := o.isFiltered()
	if isFiltered && o.mset == nil {
		return false
	}

	// Check if we are filtered, and if so check if this is even applicable to us.
	if isFiltered {
		if subj == _EMPTY_ {
			var err error
			if subj, err = o.mset.store.SubjectForSeq(sseq); err != nil {
				return false
			}
		}
		if !o.isFilteredMatch(subj) {
			return false
		}
	}
	if o.isLeader() {
		asflr, osseq = o.asflr, o.sseq
		pending, rdc = o.pending, o.rdc
	} else {
		if o.store == nil {
			return false
		}
		state, err := o.store.BorrowState()
		if err != nil || state == nil {
			// Fall back to what we track internally for now.
			return sseq > o.asflr && !o.isFiltered()
		}
		// If loading state as here, the osseq is +1.
		asflr, osseq, pending, rdc = state.AckFloor.Stream, state.Delivered.Stream+1, state.Pending, state.Redelivered
	}

	switch o.cfg.AckPolicy {
	case AckNone, AckAll, AckFlowControl:
		needAck = sseq > asflr
	case AckExplicit:
		if sseq > asflr {
			if sseq >= osseq {
				needAck = true
			} else {
				_, needAck = pending[sseq]
			}
		}
	}

	// Finally check if redelivery of this message is tracked.
	// If the message is not pending, it should be preserved if it reached max delivery.
	if !needAck {
		_, needAck = rdc[sseq]
	}

	return needAck
}

type PriorityGroup struct {
	Group         string `json:"group,omitempty"`
	MinPending    int64  `json:"min_pending,omitempty"`
	MinAckPending int64  `json:"min_ack_pending,omitempty"`
	Id            string `json:"id,omitempty"`
	Priority      int    `json:"priority,omitempty"`
}

// Used in nextReqFromMsg, since the json.Unmarshal causes the request
// struct to escape to the heap always. This should reduce GC pressure.
var jsGetNextPool = sync.Pool{
	New: func() any {
		return &JSApiConsumerGetNextRequest{}
	},
}

// Helper for the next message requests.
func nextReqFromMsg(msg []byte) (time.Time, int, int, bool, time.Duration, time.Time, *PriorityGroup, error) {
	req := bytes.TrimSpace(msg)

	switch {
	case len(req) == 0:
		return time.Time{}, 1, 0, false, 0, time.Time{}, nil, nil

	case req[0] == '{':
		cr := jsGetNextPool.Get().(*JSApiConsumerGetNextRequest)
		defer func() {
			*cr = JSApiConsumerGetNextRequest{}
			jsGetNextPool.Put(cr)
		}()
		if err := json.Unmarshal(req, &cr); err != nil {
			return time.Time{}, -1, 0, false, 0, time.Time{}, nil, err
		}
		var hbt time.Time
		if cr.Heartbeat > 0 {
			if cr.Heartbeat*2 > cr.Expires {
				return time.Time{}, 1, 0, false, 0, time.Time{}, nil, errors.New("heartbeat value too large")
			}
			hbt = time.Now().Add(cr.Heartbeat)
		}
		priorityGroup := cr.PriorityGroup
		if cr.Expires == time.Duration(0) {
			return time.Time{}, cr.Batch, cr.MaxBytes, cr.NoWait, cr.Heartbeat, hbt, &priorityGroup, nil
		}
		return time.Now().Add(cr.Expires), cr.Batch, cr.MaxBytes, cr.NoWait, cr.Heartbeat, hbt, &priorityGroup, nil
	default:
		if n, err := strconv.Atoi(string(req)); err == nil {
			return time.Time{}, n, 0, false, 0, time.Time{}, nil, nil
		}
	}

	return time.Time{}, 1, 0, false, 0, time.Time{}, nil, nil
}

// Represents a request that is on the internal waiting queue
type waitingRequest struct {
	next          *waitingRequest
	acc           *Account
	interest      string
	reply         string
	n             int // For batching
	d             int // num delivered
	b             int // For max bytes tracking
	expires       time.Time
	received      time.Time
	hb            time.Duration
	hbt           time.Time
	noWait        bool
	priorityGroup *PriorityGroup
}

// sync.Pool for waiting requests.
var wrPool = sync.Pool{
	New: func() any {
		return new(waitingRequest)
	},
}

// Recycle this request. This request can not be accessed after this call.
func (wr *waitingRequest) recycleIfDone() bool {
	if wr != nil && wr.n <= 0 {
		wr.recycle()
		return true
	}
	return false
}

// Force a recycle.
func (wr *waitingRequest) recycle() {
	if wr != nil {
		wr.next, wr.acc, wr.interest, wr.reply = nil, nil, _EMPTY_, _EMPTY_
		wrPool.Put(wr)
	}
}

// Represents an (optional) request timeout that's sent after waiting for replicated deliveries.
type waitingDelivery struct {
	seq uint64
	pn  int // Pending messages.
	pb  int // Pending bytes.
}

// sync.Pool for waiting deliveries.
var wdPool = sync.Pool{
	New: func() any {
		return new(waitingDelivery)
	},
}

// Force a recycle.
func (wd *waitingDelivery) recycle() {
	if wd != nil {
		wd.seq, wd.pn, wd.pb = 0, 0, 0
		wdPool.Put(wd)
	}
}

// waiting queue for requests that are waiting for new messages to arrive.
type waitQueue struct {
	n, max int
	last   time.Time
	head   *waitingRequest
	tail   *waitingRequest
}

// Create a new ring buffer with at most max items.
func newWaitQueue(max int) *waitQueue {
	return &waitQueue{max: max}
}

var (
	errWaitQueueFull = errors.New("wait queue is full")
	errWaitQueueNil  = errors.New("wait queue is nil")
)

// insertSorted inserts wr at the correct position based on priority
func (wq *waitQueue) insertSorted(wr *waitingRequest) {

	// Handle empty queue
	if wq.head == nil {
		wq.head = wr
		wq.tail = wr
		wr.next = nil
		return
	}
	insertAtPosition(wr, wq)
}

func (wq *waitQueue) addPrioritized(wr *waitingRequest) error {
	if wq == nil {
		return errWaitQueueNil
	}
	if wq.isFull() {
		return errWaitQueueFull
	}

	wq.insertSorted(wr)
	wq.n++
	wq.last = wr.received
	return nil
}

// Adds in a new request.
func (wq *waitQueue) add(wr *waitingRequest) error {
	if wq == nil {
		return errWaitQueueNil
	}
	if wq.isFull() {
		return errWaitQueueFull
	}
	if wq.head == nil {
		wq.head = wr
	} else {
		wq.tail.next = wr
	}
	// Always set tail.
	wq.tail = wr
	// Make sure nil
	wr.next = nil

	// Track last active via when we receive a request.
	wq.last = wr.received
	wq.n++
	return nil
}

func (wq *waitQueue) isFull() bool {
	if wq == nil {
		return false
	}
	return wq.n == wq.max
}

func (wq *waitQueue) isEmpty() bool {
	if wq == nil {
		return true
	}
	return wq.n == 0
}

func (wq *waitQueue) len() int {
	if wq == nil {
		return 0
	}
	return wq.n
}

// Peek will return the next request waiting or nil if empty.
func (wq *waitQueue) peek() *waitingRequest {
	if wq == nil {
		return nil
	}
	return wq.head
}

func (wq *waitQueue) cycle() {
	wr := wq.peek()
	if wr != nil {
		// Always remove current now on a pop, and move to end if still valid.
		// If we were the only one don't need to remove since this can be a no-op.
		wq.removeCurrent()
		wq.add(wr)
	}
}

func (wq *waitQueue) popOrPopAndRequeue(priority PriorityPolicy) *waitingRequest {
	if wq == nil || wq.head == nil {
		return nil
	}

	if priority == PriorityPrioritized {
		return wq.popAndRequeue()
	}
	return wq.pop()
}

// pop will return the next request and move the read cursor.
// This will now place a request that still has pending items at the ends of the list.
func (wq *waitQueue) pop() *waitingRequest {
	wr := wq.peek()
	if wr != nil {
		wr.d++
		wr.n--
		// Always remove current now on a pop, and move to end if still valid.
		// If we were the only one don't need to remove since this can be a no-op.
		if wr.n > 0 && wq.n > 1 {
			wq.removeCurrent()
			wq.add(wr)
		} else if wr.n <= 0 {
			wq.removeCurrent()
		}
	}
	return wr
}

// popAndRequeue pops the head element and requeues it at the end of its priority group.
// This maintains FIFO order within the same priority level.
func (wq *waitQueue) popAndRequeue() *waitingRequest {
	if wq == nil || wq.head == nil {
		return nil
	}

	// Save the head
	wr := wq.head

	if wr == nil {
		return wr
	}

	wr.d++
	wr.n--

	if wr.n > 0 && wq.n > 1 {
		if wr.next == nil {
			return wr
		}
		wq.head = wq.head.next
		wr.next = nil

		insertAtPosition(wr, wq)

	} else if wr.n <= 0 {
		wq.removeCurrent()
		return wr

	}

	return wr
}

func insertAtPosition(wr *waitingRequest, wq *waitQueue) {
	priority := math.MaxInt32
	if wr.priorityGroup != nil {
		priority = wr.priorityGroup.Priority
	}

	var prev *waitingRequest
	current := wq.head
	for current != nil {
		currentPriority := math.MaxInt32
		if current.priorityGroup != nil {
			currentPriority = current.priorityGroup.Priority
		}
		if currentPriority > priority {
			break
		}
		prev = current
		current = current.next
	}

	if prev == nil {
		// All remaining elements have higher priority
		wr.next = wq.head
		wq.head = wr
	} else {
		wr.next = prev.next
		prev.next = wr
		if wr.next == nil {
			wq.tail = wr
		}
	}
}

// Removes the current read pointer (head FIFO) entry.
func (wq *waitQueue) removeCurrent() {
	wq.remove(nil, wq.head)
}

// Remove the wr element from the wait queue.
func (wq *waitQueue) remove(pre, wr *waitingRequest) {
	if wr == nil {
		return
	}
	if pre != nil {
		pre.next = wr.next
	} else if wr == wq.head {
		// We are removing head here.
		wq.head = wr.next
	}
	// Check if wr was our tail.
	if wr == wq.tail {
		// Check if we need to assign to pre.
		if wr.next == nil {
			wq.tail = pre
		} else {
			wq.tail = wr.next
		}
	}
	wq.n--
}

// Return the map of pending requests keyed by the reply subject.
// No-op if push consumer or invalid etc.
func (o *consumer) pendingRequests() map[string]*waitingRequest {
	if o.waiting == nil {
		return nil
	}
	wq, m := o.waiting, make(map[string]*waitingRequest)
	for wr := wq.head; wr != nil; wr = wr.next {
		m[wr.reply] = wr
	}

	return m
}

func (o *consumer) setPinnedTimer(priorityGroup string) {
	if o.pinnedTtl != nil {
		o.pinnedTtl.Reset(o.cfg.PinnedTTL)
	} else {
		o.pinnedTtl = time.AfterFunc(o.cfg.PinnedTTL, func() {
			o.mu.Lock()
			// Skip if already unset.
			if o.currentPinId == _EMPTY_ {
				o.mu.Unlock()
				return
			}
			o.unassignPinId()
			o.sendUnpinnedAdvisoryLocked(priorityGroup, "timeout")
			o.mu.Unlock()
			o.signalNewMessages()
		})
	}
}

// Lock should be held.
func (o *consumer) assignNewPinId(wr *waitingRequest) {
	if wr.priorityGroup == nil || wr.priorityGroup.Group == _EMPTY_ {
		return
	}
	o.currentPinId = nuid.Next()
	o.pinnedTS = time.Now().UTC()
	wr.priorityGroup.Id = o.currentPinId
	o.setPinnedTimer(wr.priorityGroup.Group)
	o.sendPinnedAdvisoryLocked(wr.priorityGroup.Group)
}

// Lock should be held.
func (o *consumer) unassignPinId() {
	o.currentPinId = _EMPTY_
	o.pinnedTS = time.Time{}
	if o.pinnedTtl != nil {
		o.pinnedTtl.Stop()
		o.pinnedTtl = nil
	}
}

// Return next waiting request. This will check for expirations but not noWait or interest.
// That will be handled by processWaiting.
// Lock should be held.
func (o *consumer) nextWaiting(sz int) *waitingRequest {
	if o.waiting == nil || o.waiting.isEmpty() {
		return nil
	}

	// Check if server needs to assign a new pin id.
	needNewPin := o.currentPinId == _EMPTY_ && o.cfg.PriorityPolicy == PriorityPinnedClient

	numCycled := 0
	for wr := o.waiting.peek(); !o.waiting.isEmpty(); wr = o.waiting.peek() {
		if wr == nil {
			break
		}

		if wr.expires.IsZero() || time.Now().Before(wr.expires) {
			// Track whether this iteration just claimed a new pin for this request.
			assignedPin := false
			if needNewPin {
				if wr.priorityGroup == nil || wr.priorityGroup.Id == _EMPTY_ {
					o.assignNewPinId(wr)
					assignedPin = true
				} else {
					// There is pin id set, but not a matching one. Send a notification to the client and remove the request.
					// Probably this is the old pin id.
					hdr := fmt.Appendf(nil, "NATS/1.0 423 Nats-Wrong-Pin-Id\r\n%s: %d\r\n%s: %d\r\n\r\n", JSPullRequestPendingMsgs, wr.n, JSPullRequestPendingBytes, wr.b)
					o.outq.send(newJSPubMsg(wr.reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
					o.waiting.removeCurrent()
					if o.node != nil {
						o.removeClusterPendingRequest(wr.reply)
					}
					wr.recycle()
					continue
				}
			} else if o.currentPinId != _EMPTY_ {
				// Check if we have a match on the currentNuid
				if wr.priorityGroup != nil && wr.priorityGroup.Id == o.currentPinId {
					// If we have a match, we do nothing here and will deliver the message later down the code path.
				} else if wr.priorityGroup == nil || wr.priorityGroup.Id == _EMPTY_ {
					o.waiting.cycle()
					numCycled++
					if numCycled >= o.waiting.len() {
						return nil
					}
					continue
				} else {
					// There is pin id set, but not a matching one. Send a notification to the client and remove the request.
					hdr := fmt.Appendf(nil, "NATS/1.0 423 Nats-Wrong-Pin-Id\r\n%s: %d\r\n%s: %d\r\n\r\n", JSPullRequestPendingMsgs, wr.n, JSPullRequestPendingBytes, wr.b)
					o.outq.send(newJSPubMsg(wr.reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
					o.waiting.removeCurrent()
					if o.node != nil {
						o.removeClusterPendingRequest(wr.reply)
					}
					wr.recycle()
					continue
				}
			}

			if o.cfg.PriorityPolicy == PriorityOverflow {
				if wr.priorityGroup != nil &&
					// If both limits are zero we don't cycle and the request will be fulfilled.
					(wr.priorityGroup.MinPending > 0 || wr.priorityGroup.MinAckPending > 0) &&
					// We need to check o.npc+1, because before calling nextWaiting, we do o.npc--
					// If one OR the other limit is exceeded, we want to fulfill the request.
					// This is an inverted check. For clarity, we check the positive condition and negate.
					!((wr.priorityGroup.MinPending > 0 && wr.priorityGroup.MinPending <= o.npc+1) ||
						(wr.priorityGroup.MinAckPending > 0 && wr.priorityGroup.MinAckPending <= int64(len(o.pending)))) {
					o.waiting.cycle()
					numCycled++
					// We're done cycling through the requests.
					if numCycled >= o.waiting.len() {
						return nil
					}
					continue
				}
			}
			// Check if we have max bytes set.
			if wr.b > 0 {
				if sz <= wr.b {
					wr.b -= sz
					// If we are right now at zero, set batch to 1 to deliver this one but stop after.
					if wr.b == 0 {
						wr.n = 1
					}
				} else {
					// Since we can't send that message to the requestor, we need to
					// notify that we are closing the request.
					const maxBytesT = "NATS/1.0 409 Message Size Exceeds MaxBytes\r\n%s: %d\r\n%s: %d\r\n\r\n"
					hdr := fmt.Appendf(nil, maxBytesT, JSPullRequestPendingMsgs, wr.n, JSPullRequestPendingBytes, wr.b)
					o.outq.send(newJSPubMsg(wr.reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
					// If we just claimed the pin for this request, release it.
					if assignedPin {
						o.unassignPinId()
					}
					// Remove the current one, no longer valid due to max bytes limit.
					o.waiting.removeCurrent()
					if o.node != nil {
						o.removeClusterPendingRequest(wr.reply)
					}
					wr.recycle()
					continue
				}
			}

			if wr.acc.sl.HasInterest(wr.interest) {
				return o.waiting.popOrPopAndRequeue(o.cfg.PriorityPolicy)
			} else if time.Since(wr.received) < defaultGatewayRecentSubExpiration && (o.srv.leafNodeEnabled || o.srv.gateway.enabled) {
				return o.waiting.popOrPopAndRequeue(o.cfg.PriorityPolicy)
			} else if o.srv.gateway.enabled && o.srv.hasGatewayInterest(wr.acc.Name, wr.interest) {
				return o.waiting.popOrPopAndRequeue(o.cfg.PriorityPolicy)
			}
		} else {
			// We do check for expiration in `processWaiting`, but it is possible to hit the expiry here, and not there.
			rdWait := o.replicateDeliveries()
			if rdWait {
				// Check if we need to send the timeout after pending replicated deliveries, or can do so immediately.
				if wd, ok := o.waitingDeliveries[wr.reply]; !ok {
					rdWait = false
				} else {
					wd.pn, wd.pb = wr.n, wr.b
				}
			}
			if !rdWait {
				hdr := fmt.Appendf(nil, "NATS/1.0 408 Request Timeout\r\n%s: %d\r\n%s: %d\r\n\r\n", JSPullRequestPendingMsgs, wr.n, JSPullRequestPendingBytes, wr.b)
				o.outq.send(newJSPubMsg(wr.reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
			}
			// Expiring a request counts as activity for the inactive threshold.
			if wr.expires.After(o.waiting.last) {
				o.waiting.last = wr.expires
			}
			o.waiting.removeCurrent()
			if o.node != nil {
				o.removeClusterPendingRequest(wr.reply)
			}
			wr.recycle()
			continue

		}
		if wr.interest != wr.reply {
			const intExpT = "NATS/1.0 408 Interest Expired\r\n%s: %d\r\n%s: %d\r\n\r\n"
			hdr := fmt.Appendf(nil, intExpT, JSPullRequestPendingMsgs, wr.n, JSPullRequestPendingBytes, wr.b)
			o.outq.send(newJSPubMsg(wr.reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		}
		// Remove the current one, no longer valid.
		o.waiting.removeCurrent()
		if o.node != nil {
			o.removeClusterPendingRequest(wr.reply)
		}
		wr.recycle()
	}

	return nil
}

// Next message request.
type nextMsgReq struct {
	reply string
	msg   []byte
}

var nextMsgReqPool sync.Pool

func newNextMsgReq(reply string, msg []byte) *nextMsgReq {
	var nmr *nextMsgReq
	m := nextMsgReqPool.Get()
	if m != nil {
		nmr = m.(*nextMsgReq)
	} else {
		nmr = &nextMsgReq{}
	}
	// When getting something from a pool it is critical that all fields are
	// initialized. Doing this way guarantees that if someone adds a field to
	// the structure, the compiler will fail the build if this line is not updated.
	(*nmr) = nextMsgReq{reply, msg}
	return nmr
}

func (nmr *nextMsgReq) returnToPool() {
	if nmr == nil {
		return
	}
	nmr.reply, nmr.msg = _EMPTY_, nil
	nextMsgReqPool.Put(nmr)
}

// processNextMsgReq will process a request for the next message available. A nil message payload means deliver
// a single message. If the payload is a formal request or a number parseable with Atoi(), then we will send a
// batch of messages without requiring another request to this endpoint, or an ACK.
func (o *consumer) processNextMsgReq(_ *subscription, c *client, _ *Account, _, reply string, rmsg []byte) {
	if reply == _EMPTY_ {
		return
	}

	// Short circuit error here.
	if o.nextMsgReqs == nil {
		hdr := []byte("NATS/1.0 409 Consumer is push based\r\n\r\n")
		o.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}

	hdr, msg := c.msgParts(rmsg)
	if errorOnRequiredApiLevel(hdr) {
		hdr = []byte("NATS/1.0 412 Required Api Level\r\n\r\n")
		o.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}
	o.nextMsgReqs.push(newNextMsgReq(reply, copyBytes(msg)))
}

// processResetReq will reset a consumer to a new starting sequence.
func (o *consumer) processResetReq(_ *subscription, c *client, a *Account, _, reply string, rmsg []byte) {
	if reply == _EMPTY_ {
		return
	}

	s := o.srv
	var resp = JSApiConsumerResetResponse{ApiResponse: ApiResponse{Type: JSApiConsumerResetResponseType}}

	hdr, msg := c.msgParts(rmsg)
	if errorOnRequiredApiLevel(hdr) {
		resp.Error = NewJSRequiredApiLevelError()
		s.sendInternalAccountMsg(a, reply, s.jsonResponse(&resp))
		return
	}

	// If the user provided an expected stream identity.
	// We don't reject the request, we only populate the identity in the response.
	var streamIdentity string
	if sliceHeader(JSStreamIdentity, hdr) != nil {
		streamIdentity = o.streamIdentity()
	}

	// An empty message resets back to the ack floor, otherwise a custom sequence can be used.
	var req JSApiConsumerResetRequest
	if len(msg) > 0 {
		if err := json.Unmarshal(msg, &req); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
			s.sendInternalAccountMsg(a, reply, s.jsonResponse(&resp))
			return
		}
	}
	resetSeq, canRespond, err := o.resetStartingSeq(req.Seq, reply, false, streamIdentity != _EMPTY_)
	if err != nil {
		resp.Error = NewJSConsumerInvalidResetError(err)
		s.sendInternalAccountMsg(a, reply, s.jsonResponse(&resp))
	} else if canRespond {
		if resp.ConsumerInfo = setDynamicConsumerInfoMetadata(o.info()); resp.ConsumerInfo == nil {
			// The consumer was closed before we could respond.
			resp.Error = NewJSConsumerInvalidResetError(errConsumerClosed)
			s.sendInternalAccountMsg(a, reply, s.jsonResponse(&resp))
			return
		}
		resp.ResetSeq = resetSeq
		if streamIdentity != _EMPTY_ {
			rhdr := genHeader(nil, JSStreamIdentity, streamIdentity)
			s.sendInternalAccountMsgWithReply(a, reply, _EMPTY_, rhdr, s.jsonResponse(&resp), false)
		} else {
			s.sendInternalAccountMsg(a, reply, s.jsonResponse(&resp))
		}
	}
}

func (o *consumer) processNextMsgRequest(reply string, msg []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()

	mset := o.mset
	if mset == nil {
		return
	}

	sendErr := func(status int, description string) {
		hdr := fmt.Appendf(nil, "NATS/1.0 %d %s\r\n\r\n", status, description)
		o.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
	}

	if o.isPushMode() || o.waiting == nil {
		sendErr(409, "Consumer is push based")
		return
	}

	// Check payload here to see if they sent in batch size or a formal request.
	expires, batchSize, maxBytes, noWait, hb, hbt, priorityGroup, err := nextReqFromMsg(msg)
	if err != nil {
		sendErr(400, fmt.Sprintf("Bad Request - %v", err))
		return
	}

	// Check for request limits
	if o.cfg.MaxRequestBatch > 0 && batchSize > o.cfg.MaxRequestBatch {
		sendErr(409, fmt.Sprintf("Exceeded MaxRequestBatch of %d", o.cfg.MaxRequestBatch))
		return
	}

	if !expires.IsZero() && o.cfg.MaxRequestExpires > 0 && expires.After(time.Now().Add(o.cfg.MaxRequestExpires)) {
		sendErr(409, fmt.Sprintf("Exceeded MaxRequestExpires of %v", o.cfg.MaxRequestExpires))
		return
	}

	if maxBytes > 0 && o.cfg.MaxRequestMaxBytes > 0 && maxBytes > o.cfg.MaxRequestMaxBytes {
		sendErr(409, fmt.Sprintf("Exceeded MaxRequestMaxBytes of %v", o.cfg.MaxRequestMaxBytes))
		return
	}

	if priorityGroup != nil {
		if (priorityGroup.MinPending != 0 || priorityGroup.MinAckPending != 0) && o.cfg.PriorityPolicy != PriorityOverflow {
			sendErr(400, "Bad Request - Not a Overflow Priority consumer")
			return
		}
		if priorityGroup.Id != _EMPTY_ && o.cfg.PriorityPolicy != PriorityPinnedClient {
			sendErr(400, "Bad Request - Not a Pinned Client Priority consumer")
			return
		}
		if priorityGroup.Priority < 0 || priorityGroup.Priority > 9 {
			sendErr(400, "Bad Request - Priority must be between 0 and 9")
			return
		}
	}

	if priorityGroup != nil && o.cfg.PriorityPolicy != PriorityNone {
		if priorityGroup.Group == _EMPTY_ {
			sendErr(400, "Bad Request - Priority Group missing")
			return
		}
		if !slices.Contains(o.cfg.PriorityGroups, priorityGroup.Group) {
			sendErr(400, "Bad Request - Invalid Priority Group")
			return
		}

		if o.currentPinId != _EMPTY_ {
			if priorityGroup.Id == o.currentPinId {
				o.setPinnedTimer(priorityGroup.Group)
			} else if priorityGroup.Id != _EMPTY_ {
				sendErr(423, "Nats-Pin-Id mismatch")
				return
			}
		}
	}

	// If we have the max number of requests already pending try to expire.
	if o.waiting.isFull() {
		// Try to expire some of the requests.
		// We do not want to push too hard here so at maximum process once per sec.
		if time.Since(o.lwqic) > time.Second {
			o.processWaiting(false)
		}
	}

	// If the request is for noWait and we have pending requests already, check if we have room.
	if noWait {
		msgsPending := o.numPending() + uint64(len(o.rdq))
		// If no pending at all, decide what to do with request.
		// If no expires was set then fail.
		if msgsPending == 0 && expires.IsZero() {
			o.waiting.last = time.Now()
			sendErr(404, "No Messages")
			return
		}
		if msgsPending > 0 {
			_, _, batchPending, _ := o.processWaiting(false)
			if msgsPending < uint64(batchPending) {
				o.waiting.last = time.Now()
				sendErr(408, "Requests Pending")
				return
			}
		}
		// If we are here this should be considered a one-shot situation.
		// We will wait for expires but will return as soon as we have any messages.
	}

	// If we receive this request though an account export, we need to track that interest subject and account.
	acc, interest := trackDownAccountAndInterest(o.acc, reply)

	// Create a waiting request.
	wr := wrPool.Get().(*waitingRequest)
	wr.acc, wr.interest, wr.reply, wr.n, wr.d, wr.noWait, wr.expires, wr.hb, wr.hbt, wr.priorityGroup = acc, interest, reply, batchSize, 0, noWait, expires, hb, hbt, priorityGroup
	wr.b = maxBytes
	wr.received = time.Now()

	if o.cfg.PriorityPolicy == PriorityPrioritized {
		if err := o.waiting.addPrioritized(wr); err != nil {
			if hb == 0 {
				sendErr(409, "Exceeded MaxWaiting")
			}
			wr.recycle()
			return
		}
	} else {
		if err := o.waiting.add(wr); err != nil {
			// If the client has a heartbeat interval set, don't bother responding with a 409,
			// otherwise we can end up in a hot loop with the client re-requesting instead of
			// waiting for the missing heartbeats instead and retrying.
			if hb == 0 {
				sendErr(409, "Exceeded MaxWaiting")
			}
			wr.recycle()
			return
		}
	}
	o.signalNewMessages()
	// If we are clustered update our followers about this request.
	if o.node != nil {
		o.addClusterPendingRequest(wr.reply)
	}
}

func trackDownAccountAndInterest(acc *Account, interest string) (*Account, string) {
	for strings.HasPrefix(interest, replyPrefix) {
		oa := acc
		oa.mu.RLock()
		if oa.exports.responses == nil {
			oa.mu.RUnlock()
			break
		}
		si := oa.exports.responses[interest]
		if si == nil {
			oa.mu.RUnlock()
			break
		}
		acc, interest = si.acc, si.to
		oa.mu.RUnlock()
	}
	return acc, interest
}

// Return current delivery count for a given sequence.
func (o *consumer) deliveryCount(seq uint64) uint64 {
	if o.rdc == nil {
		return 1
	}
	if dc := o.rdc[seq]; dc >= 1 {
		return dc
	}
	return 1
}

// Increase the delivery count for this message.
// ONLY used on redelivery semantics.
// Lock should be held.
func (o *consumer) incDeliveryCount(sseq uint64) uint64 {
	if o.rdc == nil {
		o.rdc = make(map[uint64]uint64)
	}
	o.rdc[sseq] += 1
	return o.rdc[sseq] + 1
}

// Used if we have to adjust on failed delivery or bad lookups.
// Those failed attempts should not increase deliver count.
// Lock should be held.
func (o *consumer) decDeliveryCount(sseq uint64) {
	if o.rdc == nil {
		return
	}
	dc, ok := o.rdc[sseq]
	if !ok {
		return
	}
	// Mirror incDeliveryCount, which goes from "no entry" to 1 on the first
	// redelivery. Going back to the initial delivery therefore has to remove
	// the entry rather than leave a zero behind, since a number of call sites
	// (needAck, decStreamPending) test for key presence rather than value.
	if dc > 1 {
		o.rdc[sseq] = dc - 1
	} else {
		delete(o.rdc, sseq)
	}
}

// send a delivery exceeded advisory.
func (o *consumer) notifyDeliveryExceeded(sseq, dc uint64) {
	e := JSConsumerDeliveryExceededAdvisory{
		TypedEvent: TypedEvent{
			Type: JSConsumerDeliveryExceededAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:     o.stream,
		Consumer:   o.name,
		StreamSeq:  sseq,
		Deliveries: dc,
		Domain:     o.srv.getOpts().JetStreamDomain,
	}

	o.sendAdvisory(o.deliveryExcEventT, e)
}

// Check if the candidate subject matches a filter if its present.
// Lock should be held.
func (o *consumer) isFilteredMatch(subj string) bool {
	// No filter is automatic match.
	if o.subjf == nil {
		return true
	}
	for _, filter := range o.subjf {
		if !filter.hasWildcard && subj == filter.subject {
			return true
		}
	}
	// It's quicker to first check for non-wildcard filters, then
	// iterate again to check for subset match.
	tsa := [32]string{}
	tts := tokenizeSubjectIntoSlice(tsa[:0], subj)
	for _, filter := range o.subjf {
		if isSubsetMatchTokenized(tts, filter.tokenizedSubject) {
			return true
		}
	}
	return false
}

// Check if the candidate filter subject is equal to or a subset match
// of one of the filter subjects.
// Lock should be held.
func (o *consumer) isEqualOrSubsetMatch(subj string) bool {
	for _, filter := range o.subjf {
		if !filter.hasWildcard && subj == filter.subject {
			return true
		}
	}
	tsa := [32]string{}
	tts := tokenizeSubjectIntoSlice(tsa[:0], subj)
	for _, filter := range o.subjf {
		if isSubsetMatchTokenized(filter.tokenizedSubject, tts) {
			return true
		}
	}
	return false
}

// Check if all consumer filter subjects are subsets of the candidate subject.
// Lock should be held.
func (o *consumer) isFilterSubsetOf(subj string) bool {
	if len(o.subjf) == 0 {
		return false
	}
	tsa := [32]string{}
	tts := tokenizeSubjectIntoSlice(tsa[:0], subj)
	for _, filter := range o.subjf {
		if !isSubsetMatchTokenized(filter.tokenizedSubject, tts) {
			return false
		}
	}
	return true
}

var (
	errMaxAckPending = errors.New("max ack pending reached")
	errBadConsumer   = errors.New("consumer not valid")
	errNoInterest    = errors.New("consumer requires interest for delivery subject when ephemeral")
)

// Get next available message from underlying store.
// Is partition aware and redeliver aware.
// Lock should be held.
func (o *consumer) getNextMsg() (*jsPubMsg, uint64, error) {
	if o.mset == nil || o.mset.store == nil {
		return nil, 0, errBadConsumer
	}
	// Process redelivered messages before looking at possibly "skip list" (deliver last per subject)
	if o.hasRedeliveries() {
		var seq, dc uint64
		for seq = o.getNextToRedeliver(); seq > 0; seq = o.getNextToRedeliver() {
			dc = o.incDeliveryCount(seq)
			if o.maxdc > 0 && dc > o.maxdc {
				// Only send the advisory once.
				o.notifyDeliveryExceeded(seq, dc-1)
				// Make sure to remove from pending.
				if p, ok := o.pending[seq]; ok && p != nil {
					delete(o.pending, seq)
					// The delivery count has already been incremented once.
					o.updateDelivered(p.Sequence, seq, dc, p.Timestamp)
					o.moveAckFloor(p.Sequence, seq)
				}
				continue
			}
			pmsg := getJSPubMsgFromPool()
			sm, err := o.mset.store.LoadMsg(seq, &pmsg.StoreMsg)
			if sm == nil || err != nil {
				pmsg.returnToPool()
				pmsg = nil
				// Adjust back deliver count.
				o.decDeliveryCount(seq)
			}
			// Message was scheduled for redelivery but was removed in the meantime.
			if err == ErrStoreMsgNotFound || err == errDeletedMsg {
				// This is a race condition where the message is still in o.pending and
				// scheduled for redelivery, but it has been removed from the stream.
				// o.processTerm is called in a goroutine so would normally run. However,
				// if we get here this likely didn't fire, or we are replicated and changed leaders.
				// That will correct the pending state and delivery/ack floors, so just skip here.
				pmsg.returnToPool()
				pmsg = nil
				if p, ok := o.pending[seq]; ok {
					o.processTermLocked(seq, p.Sequence, dc-1, ackTermUnackedLimitsReason, _EMPTY_, false)
				}
				continue
			}
			return pmsg, dc, err
		}
	}

	// Check if we have max pending.
	if o.maxp > 0 && len(o.pending) >= o.maxp {
		// maxp only set when ack policy != AckNone and user set MaxAckPending
		// Stall if we have hit max pending.
		return nil, 0, errMaxAckPending
	}

	if o.hasSkipListPending() {
		seq := o.lss.seqs[0]
		if len(o.lss.seqs) == 1 {
			o.sseq = o.lss.resume
			o.lss = nil
			o.updateSkipped(o.sseq)
		} else {
			o.lss.seqs = o.lss.seqs[1:]
			o.sseq = seq
		}
		pmsg := getJSPubMsgFromPool()
		sm, err := o.mset.store.LoadMsg(seq, &pmsg.StoreMsg)
		if sm == nil || err != nil {
			pmsg.returnToPool()
			pmsg = nil
		}
		o.sseq++
		return pmsg, 1, err
	}

	var sseq uint64
	var err error
	var sm *StoreMsg
	var pmsg = getJSPubMsgFromPool()

	// Grab next message applicable to us.
	filters, subjf, fseq := o.filters, o.subjf, o.sseq
	// Check if we are multi-filtered or not.
	if filters != nil {
		sm, sseq, err = o.mset.store.LoadNextMsgMulti(filters, fseq, &pmsg.StoreMsg)
	} else if len(subjf) > 0 { // Means single filtered subject since o.filters means > 1.
		filter, wc := subjf[0].subject, subjf[0].hasWildcard
		sm, sseq, err = o.mset.store.LoadNextMsg(filter, wc, fseq, &pmsg.StoreMsg)
	} else {
		// No filter here.
		sm, sseq, err = o.mset.store.LoadNextMsg(_EMPTY_, false, fseq, &pmsg.StoreMsg)
	}
	if sm == nil {
		pmsg.returnToPool()
		pmsg = nil
	}
	// Check if we should move our o.sseq.
	if sseq >= o.sseq {
		// If we are moving step by step then sseq == o.sseq.
		// If we have jumped we should update skipped for other replicas.
		if sseq != o.sseq && err == ErrStoreEOF {
			o.updateSkipped(sseq + 1)
		}
		o.sseq = sseq + 1
	}
	return pmsg, 1, err
}

// Will check for expiration and lack of interest on waiting requests.
// Will also do any heartbeats and return the next expiration or HB interval.
func (o *consumer) processWaiting(eos bool) (int, int, int, time.Time) {
	var fexp time.Time
	if o.srv == nil || o.waiting.isEmpty() {
		return 0, 0, 0, fexp
	}
	// Mark our last check time.
	o.lwqic = time.Now()

	var expired, brp int
	s, now := o.srv, time.Now()

	wq := o.waiting
	remove := func(pre, wr *waitingRequest) *waitingRequest {
		expired++
		if o.node != nil {
			o.removeClusterPendingRequest(wr.reply)
		}
		next := wr.next
		wq.remove(pre, wr)
		wr.recycle()
		return next
	}

	var pre *waitingRequest
	for wr := wq.head; wr != nil; {
		// Check expiration.
		expires := !wr.expires.IsZero() && now.After(wr.expires)
		if (eos && wr.noWait) || expires {
			rdWait := o.replicateDeliveries()
			if rdWait {
				// Check if we need to send the timeout after pending replicated deliveries, or can do so immediately.
				if wd, ok := o.waitingDeliveries[wr.reply]; !ok {
					rdWait = false
				} else {
					wd.pn, wd.pb = wr.n, wr.b
				}
				// If we still need to wait for replicated deliveries, remove from waiting list.
				if rdWait {
					wr = remove(pre, wr)
					continue
				}
			}
			// Normally it's a timeout.
			if expires {
				hdr := fmt.Appendf(nil, "NATS/1.0 408 Request Timeout\r\n%s: %d\r\n%s: %d\r\n\r\n", JSPullRequestPendingMsgs, wr.n, JSPullRequestPendingBytes, wr.b)
				o.outq.send(newJSPubMsg(wr.reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
				if wr.expires.After(wq.last) {
					wq.last = wr.expires
				}
				wr = remove(pre, wr)
				continue
			} else if wr.expires.IsZero() || wr.d > 0 {
				// But if we're NoWait without expiry, we've reached the end of the stream, and we've not delivered any messages.
				// Return no messages instead, which is the same as if we'd rejected the pull request initially.
				hdr := fmt.Appendf(nil, "NATS/1.0 404 No Messages\r\n\r\n")
				o.outq.send(newJSPubMsg(wr.reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
				if now.After(wq.last) {
					wq.last = now
				}
				wr = remove(pre, wr)
				continue
			}
		}
		// Now check interest.
		interest := wr.acc.sl.HasInterest(wr.interest)
		if !interest && (s.leafNodeEnabled || s.gateway.enabled) {
			// If we are here check on gateways and leaf nodes (as they can mask gateways on the other end).
			// If we have interest or the request is too young break and do not expire.
			if time.Since(wr.received) < defaultGatewayRecentSubExpiration {
				interest = true
			} else if s.gateway.enabled && s.hasGatewayInterest(wr.acc.Name, wr.interest) {
				interest = true
			}
		}
		// Check if we have interest.
		if !interest {
			// No more interest here so go ahead and remove this one from our list.
			wr = remove(pre, wr)
			continue
		}

		// If interest, update batch pending requests counter and update fexp timer.
		brp += wr.n
		if !wr.hbt.IsZero() {
			if now.After(wr.hbt) {
				// Fire off a heartbeat here.
				o.sendIdleHeartbeat(wr.reply)
				// Update next HB.
				wr.hbt = now.Add(wr.hb)
			}
			if fexp.IsZero() || wr.hbt.Before(fexp) {
				fexp = wr.hbt
			}
		}
		if !wr.expires.IsZero() && (fexp.IsZero() || wr.expires.Before(fexp)) {
			fexp = wr.expires
		}
		// Update pre and wr here.
		pre = wr
		wr = wr.next
	}

	return expired, wq.len(), brp, fexp
}

// Will check to make sure those waiting still have registered interest.
func (o *consumer) checkWaitingForInterest() bool {
	o.processWaiting(true)
	return o.waiting.len() > 0
}

// Lock should be held.
func (o *consumer) hbTimer() (time.Duration, *time.Timer) {
	if o.cfg.Heartbeat == 0 {
		return 0, nil
	}
	return o.cfg.Heartbeat, time.NewTimer(o.cfg.Heartbeat)
}

// Check here for conditions when our ack floor may have drifted below the streams first sequence.
// In general this is accounted for in normal operations, but if the consumer misses the signal from
// the stream it will not clear the message and move the ack state.
// Should only be called from consumer leader.
func (o *consumer) checkAckFloor() {
	o.mu.RLock()
	mset, closed, asflr, numPending := o.mset, o.closed, o.asflr, len(o.pending)
	o.mu.RUnlock()

	if asflr == 0 || closed || mset == nil {
		return
	}

	var ss StreamState
	mset.store.FastState(&ss)

	// If our floor is equal or greater that is normal and nothing for us to do.
	if ss.FirstSeq == 0 || asflr >= ss.FirstSeq-1 {
		return
	}

	// Check which linear space is less to walk.
	if ss.FirstSeq-asflr-1 < uint64(numPending) {
		// Process all messages that no longer exist.
		for seq := asflr + 1; seq < ss.FirstSeq; seq++ {
			// Check if this message was pending.
			o.mu.RLock()
			p, isPending := o.pending[seq]
			rdc := o.deliveryCount(seq)
			o.mu.RUnlock()
			// If it was pending for us, get rid of it.
			if isPending {
				o.processTerm(seq, p.Sequence, rdc, ackTermLimitsReason, _EMPTY_)
			}
		}
	} else if numPending > 0 {
		// here it is shorter to walk pending.
		// toTerm is seq, dseq, rcd for each entry.
		toTerm := make([]uint64, 0, numPending*3)
		o.mu.RLock()
		for seq, p := range o.pending {
			if seq < ss.FirstSeq {
				var dseq uint64 = 1
				if p != nil {
					dseq = p.Sequence
				}
				rdc := o.deliveryCount(seq)
				toTerm = append(toTerm, seq, dseq, rdc)
			}
		}
		o.mu.RUnlock()

		for i := 0; i < len(toTerm); i += 3 {
			seq, dseq, rdc := toTerm[i], toTerm[i+1], toTerm[i+2]
			o.processTerm(seq, dseq, rdc, ackTermLimitsReason, _EMPTY_)
		}
	}

	// Do one final check here.
	o.mu.Lock()
	defer o.mu.Unlock()

	// If we are closed do not change anything and simply return.
	if o.closed {
		return
	}

	// If we are here, and this should be rare, we still are off with our ack floor.
	// We will make sure we are not doing un-necessary work here if only off by a bit
	// since this could be normal for a high activity wq or stream.
	// We will set it explicitly to 1 behind our current lowest in pending, or if
	// pending is empty, to our current delivered -1.
	const minOffThreshold = 50
	if ss.FirstSeq >= minOffThreshold && o.asflr < ss.FirstSeq-minOffThreshold {
		var psseq, pdseq uint64
		for seq, p := range o.pending {
			if psseq == 0 || seq < psseq {
				psseq, pdseq = seq, p.Sequence
			}
		}
		// If we still have none, set to current delivered -1.
		if psseq == 0 {
			psseq, pdseq = o.sseq-1, o.dseq-1
			// If still not adjusted.
			if psseq < ss.FirstSeq-1 {
				psseq = ss.FirstSeq - 1
			}
		} else {
			// Since this was set via the pending, we should not include
			// it directly but set floors to -1.
			psseq, pdseq = psseq-1, pdseq-1
		}
		o.asflr, o.adflr = psseq, pdseq
	}
}

func (o *consumer) processInboundAcks(qch chan struct{}) {
	// Grab the server lock to watch for server quit.
	o.mu.RLock()
	s, mset := o.srv, o.mset
	hasInactiveThresh := o.cfg.InactiveThreshold > 0

	o.mu.RUnlock()

	if s == nil || mset == nil {
		return
	}

	// We will check this on entry and periodically.
	o.checkAckFloor()

	// How often we will check for ack floor drift.
	// Spread these out for large numbers on a server restart.
	delta := time.Duration(rand.Int64N(int64(time.Minute)))
	ticker := time.NewTicker(time.Minute + delta)
	defer ticker.Stop()

	for {
		select {
		case <-o.ackMsgs.ch:
			// If we have an inactiveThreshold set, mark our activity.
			// Do this before processing acks, otherwise we might race if there are no pending messages
			// anymore and the inactivity threshold kicks in before we're able to mark activity.
			if hasInactiveThresh {
				o.suppressDeletion()
			}

			acks := o.ackMsgs.pop()
			for _, ack := range acks {
				o.processAck(ack.subject, ack.reply, ack.hdr, ack.msg)
				ack.returnToPool()
			}
			o.ackMsgs.recycle(&acks)
		case <-ticker.C:
			o.checkAckFloor()
		case <-qch:
			return
		case <-s.quitCh:
			return
		}
	}
}

// Process inbound next message requests.
func (o *consumer) processInboundNextMsgReqs(qch chan struct{}) {
	// Grab the server lock to watch for server quit.
	o.mu.RLock()
	s := o.srv
	o.mu.RUnlock()

	for {
		select {
		case <-o.nextMsgReqs.ch:
			reqs := o.nextMsgReqs.pop()
			for _, req := range reqs {
				o.processNextMsgRequest(req.reply, req.msg)
				req.returnToPool()
			}
			o.nextMsgReqs.recycle(&reqs)
		case <-qch:
			return
		case <-s.quitCh:
			return
		}
	}
}

// Suppress auto cleanup on ack activity of any kind.
func (o *consumer) suppressDeletion() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return
	}

	if o.isPushMode() && o.dtmr != nil {
		// if dtmr is not nil we have started the countdown, simply reset to threshold.
		o.dtmr.Reset(o.dthresh)
	} else if o.isPullMode() && o.waiting != nil {
		// Pull mode always has timer running, update last on waiting queue.
		o.waiting.last = time.Now()
		if o.dtmr != nil {
			o.dtmr.Reset(o.dthresh)
		}
	}
}

// loopAndGatherMsgs waits for messages for the consumer. qch is the quit channel,
// upch is the unpause channel which fires when the PauseUntil deadline is reached.
func (o *consumer) loopAndGatherMsgs(qch chan struct{}) {
	// On startup check to see if we are in a reply situation where replay policy is not instant.
	var (
		lts  int64 // last time stamp seen, used for replay.
		lseq uint64
	)

	o.mu.RLock()
	mset := o.mset
	getLSeq := o.replay
	o.mu.RUnlock()
	// consumer is closed when mset is set to nil.
	if mset == nil {
		return
	}
	if getLSeq {
		lseq = mset.state().LastSeq
	}

	o.mu.Lock()
	s := o.srv
	// need to check again if consumer is closed
	if o.mset == nil {
		o.mu.Unlock()
		return
	}
	// For idle heartbeat support.
	var hbc <-chan time.Time
	hbd, hb := o.hbTimer()
	if hb != nil {
		hbc = hb.C
	}
	// Interest changes.
	inch := o.inch
	o.mu.Unlock()

	// Grab the stream's retention policy and name
	mset.cfgMu.RLock()
	stream, rp := mset.cfg.Name, mset.cfg.Retention
	mset.cfgMu.RUnlock()

	var err error

	// Deliver all the msgs we have now, once done or on a condition, we wait for new ones.
	for {
		var (
			pmsg     *jsPubMsg
			dc       uint64
			dsubj    string
			ackReply string
			delay    time.Duration
			sz       int
			wrn, wrb int
		)

		o.mu.Lock()

		// consumer is closed when mset is set to nil.
		if o.closed || o.mset == nil {
			o.mu.Unlock()
			return
		}

		// Clear last error.
		err = nil

		// If the consumer is paused then stop sending.
		if o.cfg.PauseUntil != nil && !o.cfg.PauseUntil.IsZero() && time.Now().Before(*o.cfg.PauseUntil) {
			// If the consumer is paused and we haven't reached the deadline yet then
			// go back to waiting.
			goto waitForMsgs
		}

		// If we are in push mode and not active or under flowcontrol let's stop sending.
		if o.isPushMode() {
			if !o.active || (o.maxpb > 0 && o.pbytes > o.maxpb) {
				goto waitForMsgs
			}
		} else if o.waiting.isEmpty() {
			// If we are in pull mode and no one is waiting already break and wait.
			goto waitForMsgs
		}

		// Grab our next msg.
		pmsg, dc, err = o.getNextMsg()

		// We can release the lock now under getNextMsg so need to check this condition again here.
		if o.closed || o.mset == nil {
			o.mu.Unlock()
			return
		}

		// On error either wait or return.
		if err != nil || pmsg == nil {
			// On EOF we can optionally fast sync num pending state.
			if err == ErrStoreEOF {
				o.checkNumPendingOnEOF()
			}
			if err == ErrStoreMsgNotFound || err == errDeletedMsg || err == ErrStoreEOF || err == errMaxAckPending {
				goto waitForMsgs
			} else {
				if pmsg != nil {
					s.Errorf("Received an error looking up message with sequence %d for consumer '%s > %s > %s': %v",
						pmsg.seq, o.mset.acc, stream, o.cfg.Name, err)
				} else {
					s.Errorf("Received an error looking up message for consumer '%s > %s > %s': %v",
						o.mset.acc, stream, o.cfg.Name, err)
				}
				goto waitForMsgs
			}
		}

		// Update our cached num pending here first.
		if dc == 1 {
			o.npc--
		}
		// Pre-calculate ackReply
		ackReply = o.ackReply(pmsg.seq, o.dseq, dc, pmsg.ts, o.numPending())

		// If headers only do not send msg payload.
		// Add in msg size itself as header.
		if o.cfg.HeadersOnly {
			convertToHeadersOnly(pmsg)
		}
		// Calculate payload size. This can be calculated on client side.
		// We do not include transport subject here since not generally known on client.
		sz = len(pmsg.subj) + len(ackReply) + len(pmsg.hdr) + len(pmsg.msg)

		if o.isPushMode() {
			dsubj = o.dsubj
		} else if wr := o.nextWaiting(sz); wr != nil {
			wrn, wrb = wr.n, wr.b
			dsubj = wr.reply
			if o.cfg.PriorityPolicy == PriorityPinnedClient {
				pmsg.hdr = genHeader(pmsg.hdr, JSPullRequestNatsPinId, o.currentPinId)
				pmsg.buf = append(pmsg.hdr, pmsg.msg...)
				sz = len(pmsg.subj) + len(ackReply) + len(pmsg.hdr) + len(pmsg.msg)
			}
			if done := wr.recycleIfDone(); done && o.node != nil {
				o.removeClusterPendingRequest(dsubj)
			} else if !done && wr.hb > 0 {
				wr.hbt = time.Now().Add(wr.hb)
			}
		} else {
			// We will redo this one as long as this is not a redelivery.
			// Need to also test that this is not going backwards since if
			// we fail to deliver we can end up here from rdq but we do not
			// want to decrement o.sseq if that is the case.
			if dc == 1 && pmsg.seq == o.sseq-1 {
				o.sseq--
				o.npc++
			} else if !o.onRedeliverQueue(pmsg.seq) {
				// We are not on the rdq so decrement the delivery count
				// and add it back.
				o.decDeliveryCount(pmsg.seq)
				o.addToRedeliverQueue(pmsg.seq)
			}
			pmsg.returnToPool()
			pmsg = nil
			goto waitForMsgs
		}

		// If we are in a replay scenario and have not caught up check if we need to delay here.
		if o.replay && lts > 0 {
			if delay = time.Duration(pmsg.ts - lts); delay > time.Millisecond {
				o.mu.Unlock()
				select {
				case <-qch:
					pmsg.returnToPool()
					pmsg = nil
					return
				case <-time.After(delay):
				}
				o.mu.Lock()
			}
		}

		// Track this regardless.
		lts = pmsg.ts

		// If we have a rate limit set make sure we check that here.
		if o.rlimit != nil {
			now := time.Now()
			var r *rate.Reservation
			if burst := o.rlimit.Burst(); sz > burst {
				// A reservation larger than burst is never allowed, so raise it just for this one.
				o.rlimit.SetBurstAt(now, sz)
				r = o.rlimit.ReserveN(now, sz)
				// The debt carries over after restoring.
				o.rlimit.SetBurstAt(now, burst)
			} else {
				r = o.rlimit.ReserveN(now, sz)
			}
			if delay := r.DelayFrom(now); delay > 0 {
				o.mu.Unlock()
				select {
				case <-qch:
					pmsg.returnToPool()
					pmsg = nil
					return
				case <-time.After(delay):
				}
				o.mu.Lock()
			}
		}

		// Do actual delivery.
		o.deliverMsg(dsubj, ackReply, pmsg, dc, rp)

		// If given request fulfilled batch size, but there are still pending bytes, send information about it.
		if wrn <= 0 && wrb > 0 {
			msg := fmt.Appendf(nil, JsPullRequestRemainingBytesT, JSPullRequestPendingMsgs, wrn, JSPullRequestPendingBytes, wrb)
			o.outq.send(newJSPubMsg(dsubj, _EMPTY_, _EMPTY_, msg, nil, nil, 0))
		}
		// Reset our idle heartbeat timer if set.
		if hb != nil {
			hb.Reset(hbd)
		}

		o.mu.Unlock()
		continue

	waitForMsgs:
		// If we were in a replay state check to see if we are caught up. If so clear.
		if o.replay && o.sseq > lseq {
			o.replay = false
		}

		// Make sure to process any expired requests that are pending.
		var wrExp <-chan time.Time
		if o.isPullMode() {
			// Dont expire oneshots if we are here because of max ack pending limit.
			_, _, _, fexp := o.processWaiting(err != errMaxAckPending)
			if !fexp.IsZero() {
				expires := time.Until(fexp)
				if expires <= 0 {
					expires = time.Millisecond
				}
				wrExp = time.NewTimer(expires).C
			}
		}

		// We will wait here for new messages to arrive.
		mch, odsubj := o.mch, o.cfg.DeliverSubject
		o.mu.Unlock()

		select {
		case <-mch:
			// Messages are waiting.
		case interest := <-inch:
			// inch can be nil on pull-based, but then this will
			// just block and not fire.
			o.updateDeliveryInterest(interest)
		case <-qch:
			return
		case <-wrExp:
			o.mu.Lock()
			o.processWaiting(true)
			o.mu.Unlock()
		case <-hbc:
			if o.isActive() {
				o.mu.Lock()
				o.sendIdleHeartbeat(odsubj)
				// Send flow control on EOS if it's used for acknowledgements.
				if o.cfg.AckPolicy == AckFlowControl && len(o.pending) > 0 && o.fcid == _EMPTY_ {
					o.sendFlowControl()
				}
				o.mu.Unlock()
			}
			// Reset our idle heartbeat timer.
			hb.Reset(hbd)
		}
	}
}

// Lock should be held.
func (o *consumer) sendIdleHeartbeat(subj string) {
	const t = "NATS/1.0 100 Idle Heartbeat\r\n%s: %d\r\n%s: %d\r\n\r\n"
	sseq, dseq := o.sseq-1, o.dseq-1
	hdr := fmt.Appendf(nil, t, JSLastConsumerSeq, dseq, JSLastStreamSeq, sseq)
	if fcp := o.fcid; fcp != _EMPTY_ {
		// Add in that we are stalled on flow control here.
		addOn := fmt.Appendf(nil, "%s: %s\r\n\r\n", JSConsumerStalled, fcp)
		hdr = append(hdr[:len(hdr)-LEN_CR_LF], []byte(addOn)...)
	}
	o.outq.send(newJSPubMsg(subj, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
}

func (o *consumer) ackReply(sseq, dseq, dc uint64, ts int64, pending uint64) string {
	if o.useV2Ack {
		return fmt.Sprintf(o.ackReplyT, dc, sseq, dseq, ts, pending)
	}
	return fmt.Sprintf(o.ackReplyOldT, dc, sseq, dseq, ts, pending)
}

// Used mostly for testing. Sets max pending bytes for flow control setups.
func (o *consumer) setMaxPendingBytes(limit int) {
	o.pblimit = limit
	o.maxpb = limit / 16
	if o.maxpb == 0 {
		o.maxpb = 1
	}
}

// Does some sanity checks to see if we should re-calculate.
// Since there is a race when decrementing when there is contention at the beginning of the stream.
// The race is a getNextMsg skips a deleted msg, and then the decStreamPending call fires.
// This does some quick sanity checks to see if we should re-calculate num pending.
// Lock should be held.
func (o *consumer) checkNumPending() (uint64, error) {
	if o.mset != nil && o.mset.store != nil {
		var state StreamState
		o.mset.store.FastState(&state)
		npc := o.numPending()
		// Make sure we can't report more messages than there are.
		// TODO(nat): It's not great that this means consumer info has side effects,
		// since we can't know whether anyone will call it or not. The previous num
		// pending calculation that this replaces had the same problem though.
		if o.sseq > state.LastSeq {
			o.npc = 0
		} else if npc > 0 {
			o.npc = int64(min(npc, state.Msgs, state.LastSeq-o.sseq+1))
		}
	}
	return o.numPending(), nil
}

// Lock should be held.
func (o *consumer) numPending() uint64 {
	if o.npc < 0 {
		return 0
	}
	return uint64(o.npc)
}

// This will do a quick sanity check on num pending when we encounter
// and EOF in the loop and gather.
// Lock should be held.
func (o *consumer) checkNumPendingOnEOF() {
	if o.mset == nil {
		return
	}
	var state StreamState
	o.mset.store.FastState(&state)
	if o.sseq > state.LastSeq && o.npc != 0 {
		// We know here we can reset our running state for num pending.
		o.npc, o.npf = 0, state.LastSeq
	}
}

// Call into streamNumPending after acquiring the consumer lock.
func (o *consumer) streamNumPendingLocked() (uint64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.streamNumPending()
}

// Will force a set from the stream store of num pending on the consumer leader.
// Depends on delivery policy, for last per subject we calculate differently.
// Lock should be held.
func (o *consumer) streamNumPending() (uint64, error) {
	if o.mset == nil || o.mset.store == nil || !o.isLeader() {
		o.npc, o.npf = 0, 0
		return 0, nil
	}
	npc, npf, err := o.calculateNumPending()
	if err != nil {
		return 0, err
	}
	o.npc, o.npf = int64(npc), npf
	return o.numPending(), nil
}

// Will calculate num pending but only requires a read lock.
// Depends on delivery policy, for last per subject we calculate differently.
// At least RLock should be held.
func (o *consumer) calculateNumPending() (npc, npf uint64, err error) {
	if o.mset == nil || o.mset.store == nil {
		return 0, 0, nil
	}

	isLastPerSubject := o.cfg.DeliverPolicy == DeliverLastPerSubject
	filters, subjf := o.filters, o.subjf

	if filters != nil {
		return o.mset.store.NumPendingMulti(o.sseq, filters, isLastPerSubject)
	} else if len(subjf) > 0 {
		filter := subjf[0].subject
		return o.mset.store.NumPending(o.sseq, filter, isLastPerSubject)
	}
	return o.mset.store.NumPending(o.sseq, _EMPTY_, isLastPerSubject)
}

func convertToHeadersOnly(pmsg *jsPubMsg) {
	// If headers only do not send msg payload.
	// Add in msg size itself as header.
	hdr, msg := pmsg.hdr, pmsg.msg
	var bb bytes.Buffer
	if len(hdr) == 0 {
		bb.WriteString(hdrLine)
	} else {
		bb.Write(hdr)
		bb.Truncate(len(hdr) - LEN_CR_LF)
	}
	bb.WriteString(JSMsgSize)
	bb.WriteString(": ")
	bb.WriteString(strconv.FormatInt(int64(len(msg)), 10))
	bb.WriteString(CR_LF)
	bb.WriteString(CR_LF)
	// Replace underlying buf which we can use directly when we send.
	// TODO(dlc) - Probably just use directly when forming bytes.Buffer?
	pmsg.buf = pmsg.buf[:0]
	pmsg.buf = append(pmsg.buf, bb.Bytes()...)
	// Replace with new header.
	pmsg.hdr = pmsg.buf
	// Cancel msg payload
	pmsg.msg = nil
}

// Deliver a msg to the consumer.
// Lock should be held and o.mset validated to be non-nil.
func (o *consumer) deliverMsg(dsubj, ackReply string, pmsg *jsPubMsg, dc uint64, rp RetentionPolicy) {
	if o.mset == nil {
		pmsg.returnToPool()
		return
	}

	dseq := o.dseq
	o.dseq++

	pmsg.dsubj, pmsg.reply, pmsg.o = dsubj, ackReply, o
	psz := pmsg.size()

	if o.maxpb > 0 {
		o.pbytes += psz
	}

	mset := o.mset
	ap := o.cfg.AckPolicy

	// Cant touch pmsg after this sending so capture what we need.
	seq, ts := pmsg.seq, pmsg.ts

	// Update delivered first.
	o.updateDelivered(dseq, seq, dc, ts)

	if ap == AckNone {
		o.adflr = dseq
		o.asflr = seq
	} else {
		o.trackPending(seq, dseq)
	}

	// Send message.
	if o.replicateDeliveries() {
		o.addReplicatedQueuedMsg(pmsg)
	} else {
		o.outq.send(pmsg)
	}

	// Flow control.
	if o.maxpb > 0 && o.needFlowControl(psz) {
		o.sendFlowControl()
	}

	// If pull mode and we have inactivity threshold, signaled by dthresh, update last activity.
	if o.isPullMode() && o.dthresh > 0 {
		o.waiting.last = time.Now()
	}

	// If we are ack none and mset is interest only we should make sure stream removes interest.
	if ap == AckNone && rp != LimitsPolicy {
		if mset != nil && mset.ackq != nil && (o.node == nil || o.direct) {
			mset.ackq.push(seq)
		} else {
			o.updateAcks(dseq, seq, _EMPTY_)
		}
	}
}

// replicateDeliveries returns whether deliveries should be replicated before sending them.
// If we're replicated we MUST only send the message AFTER we've got quorum for updating
// delivered state. Otherwise, we could be in an invalid state after a leader change.
// We can send immediately if not replicated, not using acks, or using flow control (incompatible).
// Lock should be held.
func (o *consumer) replicateDeliveries() bool {
	return o.node != nil && o.cfg.AckPolicy != AckNone && !o.cfg.FlowControl
}

func (o *consumer) needFlowControl(sz int) bool {
	if o.maxpb == 0 {
		return false
	}
	// Decide whether to send a flow control message which we will need the user to respond.
	// We send when we are over 50% of our current window limit.
	if o.fcid == _EMPTY_ && o.pbytes > o.maxpb/2 {
		return true
	}
	// Or, when acking based on flow control, we need to send it if we've hit the max pending limit earlier.
	if o.fcid == _EMPTY_ && o.cfg.AckPolicy == AckFlowControl && o.maxp > 0 && len(o.pending) >= o.maxp {
		return true
	}
	// If we have an existing outstanding FC, check to see if we need to expand the o.fcsz
	if o.fcid != _EMPTY_ && (o.pbytes-o.fcsz) >= o.maxpb {
		o.fcsz += sz
	}
	return false
}

func (o *consumer) processFlowControl(_ *subscription, c *client, _ *Account, subj, _ string, rmsg []byte) {
	o.mu.Lock()

	// Ignore if not the latest we have sent out.
	if subj != o.fcid {
		o.mu.Unlock()
		return
	}

	// For slow starts and ramping up.
	if o.maxpb < o.pblimit {
		o.maxpb *= 2
		if o.maxpb > o.pblimit {
			o.maxpb = o.pblimit
		}
	}

	// Update accounting.
	o.pbytes -= o.fcsz
	if o.pbytes < 0 {
		o.pbytes = 0
	}
	o.fcid, o.fcsz = _EMPTY_, 0

	o.signalNewMessages()
	ackFlowControl := o.cfg.AckPolicy == AckFlowControl
	o.mu.Unlock()

	if !ackFlowControl {
		return
	}
	hdr, _ := c.msgParts(rmsg)
	if len(hdr) > 0 {
		ldseq := parseInt64(sliceHeader(JSLastConsumerSeq, hdr))
		lsseq := parseInt64(sliceHeader(JSLastStreamSeq, hdr))
		if lsseq > 0 {
			// Delivered sequence is allowed to be zero as a response
			// to flow control without any deliveries.
			if ldseq <= 0 {
				ldseq = 0
			}
			o.processAckMsg(uint64(lsseq), uint64(ldseq), 1, _EMPTY_, false)
		}
	}
}

// Lock should be held.
func (o *consumer) fcReply() string {
	var sb strings.Builder
	if o.useV2Ack {
		sb.WriteString(o.fcPre)
	} else {
		sb.WriteString(o.fcPreOld)
	}
	sb.WriteString(o.stream)
	sb.WriteByte(btsep)
	sb.WriteString(o.name)
	sb.WriteByte(btsep)
	var b [4]byte
	rn := rand.Int64()
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	sb.Write(b[:])
	return sb.String()
}

// sendFlowControl will send a flow control packet to the consumer.
// Lock should be held.
func (o *consumer) sendFlowControl() {
	if !o.isPushMode() {
		return
	}
	subj, rply := o.cfg.DeliverSubject, o.fcReply()
	o.fcsz, o.fcid = o.pbytes, rply
	hdr := []byte("NATS/1.0 100 FlowControl Request\r\n\r\n")
	o.outq.send(newJSPubMsg(subj, _EMPTY_, rply, hdr, nil, nil, 0))
}

// Tracks our outstanding pending acks. Only applicable to AckExplicit mode.
// Lock should be held.
func (o *consumer) trackPending(sseq, dseq uint64) {
	if o.pending == nil {
		o.pending = make(map[uint64]*Pending)
	}

	now := time.Now()
	if p, ok := o.pending[sseq]; ok {
		// Update timestamp but keep original consumer delivery sequence.
		// So do not update p.Sequence.
		p.Timestamp = now.UnixNano()
	} else {
		o.pending[sseq] = &Pending{dseq, now.UnixNano()}
	}

	// We could have a backoff that set a timer higher than what we need for this message.
	// In that case, reset to lowest backoff required for a message redelivery.
	minDelay := o.ackWait(0)
	if l := len(o.cfg.BackOff); l > 0 {
		bi := int(o.rdc[sseq])
		if bi < 0 {
			bi = 0
		} else if bi >= l {
			bi = l - 1
		}
		minDelay = o.ackWait(o.cfg.BackOff[bi])
	}
	minDeadline := now.Add(minDelay)
	if o.ptmr == nil || o.ptmrEnd.After(minDeadline) {
		o.resetPtmr(minDelay)
	}
}

// Credit back a failed delivery.
// lock should be held.
func (o *consumer) creditWaitingRequest(reply string) {
	wq := o.waiting
	for wr := wq.head; wr != nil; wr = wr.next {
		if wr.reply == reply {
			wr.n++
			wr.d--
			return
		}
	}
}

// didNotDeliver is called when a delivery for a consumer message failed.
// Depending on our state, we will process the failure.
func (o *consumer) didNotDeliver(seq uint64, subj string) {
	o.mu.Lock()
	mset := o.mset
	if mset == nil {
		o.mu.Unlock()
		return
	}
	// Adjust back deliver count.
	o.decDeliveryCount(seq)

	var checkDeliveryInterest bool
	if o.isPushMode() {
		o.active = false
		checkDeliveryInterest = true
	} else if o.pending != nil {
		// Good chance we did not deliver because no interest so force a check.
		o.processWaiting(false)
		// If it is still there credit it back.
		o.creditWaitingRequest(subj)
		// pull mode and we have pending.
		if _, ok := o.pending[seq]; ok {
			// We found this messsage on pending, we need
			// to queue it up for immediate redelivery since
			// we know it was not delivered
			if !o.onRedeliverQueue(seq) {
				o.addToRedeliverQueue(seq)
				if !o.waiting.isEmpty() {
					o.signalNewMessages()
				}
			}
		}
	}
	o.mu.Unlock()

	if checkDeliveryInterest {
		localInterest := !o.hasNoLocalInterest()
		o.updateDeliveryInterest(localInterest)
	}
}

// Lock should be held.
func (o *consumer) addToRedeliverQueue(seqs ...uint64) {
	o.rdq = append(o.rdq, seqs...)
	for _, seq := range seqs {
		o.rdqi.Insert(seq)
	}
}

// Lock should be held.
func (o *consumer) hasRedeliveries() bool {
	return len(o.rdq) > 0
}

func (o *consumer) getNextToRedeliver() uint64 {
	if len(o.rdq) == 0 {
		return 0
	}
	seq := o.rdq[0]
	if len(o.rdq) == 1 {
		o.rdq = nil
		o.rdqi.Empty()
	} else {
		o.rdq = append(o.rdq[:0], o.rdq[1:]...)
		o.rdqi.Delete(seq)
	}
	return seq
}

// This checks if we already have this sequence queued for redelivery.
// FIXME(dlc) - This is O(n) but should be fast with small redeliver size.
// Lock should be held.
func (o *consumer) onRedeliverQueue(seq uint64) bool {
	return o.rdqi.Exists(seq)
}

// Remove a sequence from the redelivery queue.
// Lock should be held.
func (o *consumer) removeFromRedeliverQueue(seq uint64) bool {
	if !o.onRedeliverQueue(seq) {
		return false
	}
	for i, rseq := range o.rdq {
		if rseq == seq {
			if len(o.rdq) == 1 {
				o.rdq = nil
				o.rdqi.Empty()
			} else {
				o.rdq = append(o.rdq[:i], o.rdq[i+1:]...)
				o.rdqi.Delete(seq)
			}
			return true
		}
	}
	return false
}

// Checks the pending messages.
func (o *consumer) checkPending() {
	o.mu.Lock()
	defer o.mu.Unlock()

	mset := o.mset
	ttl := int64(o.cfg.AckWait)
	// On stop, mset and timer will be nil.
	if o.closed || mset == nil || o.ptmr == nil || ttl == 0 {
		o.stopAndClearPtmr()
		return
	}

	var shouldUpdateState bool
	var state StreamState
	mset.store.FastState(&state)
	fseq := state.FirstSeq

	now := time.Now().UnixNano()
	next := int64(o.ackWait(0))
	// However, if there is backoff, initializes with the largest backoff.
	// It will be adjusted as needed.
	if l := len(o.cfg.BackOff); l > 0 {
		next = int64(o.cfg.BackOff[l-1])
	}

	// Since we can update timestamps, we have to review all pending.
	// We will now bail if we see an ack pending inbound to us via o.awl.
	var expired []uint64
	check := len(o.pending) > 1024
	for seq, p := range o.pending {
		if check && atomic.LoadInt64(&o.awl) > 0 {
			o.resetPtmr(100 * time.Millisecond)
			return
		}
		// Check if these are no longer valid.
		if seq < fseq || seq <= o.asflr {
			delete(o.pending, seq)
			delete(o.rdc, seq)
			o.removeFromRedeliverQueue(seq)
			shouldUpdateState = true
			// Check if we need to move ack floors.
			if seq > o.asflr {
				o.asflr = seq
			}
			if p.Sequence > o.adflr {
				o.adflr = p.Sequence
			}
			continue
		}
		elapsed, deadline := now-p.Timestamp, ttl
		if len(o.cfg.BackOff) > 0 {
			// This is ok even if o.rdc is nil, we would get dc == 0, which is what we want.
			dc := int(o.rdc[seq])
			if dc < 0 {
				// Prevent consumer backoff from going backwards.
				dc = 0
			}
			// This will be the index for the next backoff, will set to last element if needed.
			nbi := dc + 1
			if dc+1 >= len(o.cfg.BackOff) {
				dc = len(o.cfg.BackOff) - 1
				nbi = dc
			}
			deadline = int64(o.cfg.BackOff[dc])
			// Set `next` to the next backoff (if smaller than current `next` value).
			if nextBackoff := int64(o.cfg.BackOff[nbi]); nextBackoff < next {
				next = nextBackoff
			}
		}
		if elapsed >= deadline {
			// We will check if we have hit our max deliveries. Previously we would do this on getNextMsg() which
			// worked well for push consumers, but with pull based consumers would require a new pull request to be
			// present to process and redelivered could be reported incorrectly.
			if !o.onRedeliverQueue(seq) && !o.hasMaxDeliveries(seq) {
				expired = append(expired, seq)
			}
		} else if deadline-elapsed < next {
			// Update when we should fire next.
			next = deadline - elapsed
		}
	}

	if len(expired) > 0 {
		// We need to sort.
		slices.Sort(expired)
		o.addToRedeliverQueue(expired...)
		// Now we should update the timestamp here since we are redelivering.
		// We will use an incrementing time to preserve order for any other redelivery.
		off := now - o.pending[expired[0]].Timestamp
		for _, seq := range expired {
			if p, ok := o.pending[seq]; ok {
				p.Timestamp += off
			}
		}
		o.signalNewMessages()
	}

	if len(o.pending) > 0 {
		o.resetPtmr(time.Duration(next))
	} else {
		// Make sure to stop timer and clear out any re delivery queues
		o.stopAndClearPtmr()
		o.rdq = nil
		o.rdqi.Empty()
		o.pending = nil
		// Mimic behavior in processAckMsg when pending is empty.
		o.adflr, o.asflr = o.dseq-1, o.sseq-1
	}

	// Update our state if needed.
	if shouldUpdateState {
		if err := o.writeStoreStateUnlocked(); err != nil && o.srv != nil && o.mset != nil && !o.closed {
			s, acc, mset, name := o.srv, o.acc, o.mset, o.name
			s.Warnf("Consumer '%s > %s > %s' error on write store state from check pending: %v", acc, mset.getCfgName(), name, err)
		}
	}
}

// SeqFromReply will extract a sequence number from a reply subject.
func (o *consumer) seqFromReply(reply string) uint64 {
	_, dseq, _, _, _ := ackReplyInfo(reply)
	return dseq
}

// StreamSeqFromReply will extract the stream sequence from the reply subject.
func (o *consumer) streamSeqFromReply(reply string) uint64 {
	sseq, _, _, _, _ := ackReplyInfo(reply)
	return sseq
}

// Quick parser for positive numbers in ack reply encoding.
func parseAckReplyNum(d string) (n int64) {
	if len(d) == 0 {
		return -1
	}
	for _, dec := range d {
		if dec < asciiZero || dec > asciiNine {
			return -1
		}
		n = n*10 + (int64(dec) - asciiZero)
	}
	return n
}

const (
	expectedNumReplyTokensV1 = 9
	expectedNumReplyTokensV2 = 11
)

// Grab encoded information in the reply subject for a delivered message.
func ackReplyInfo(subject string) (sseq, dseq, dc uint64, ts int64, pending uint64) {
	tsa := [expectedNumReplyTokensV2]string{}
	start, tokens := 0, tsa[:0]
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	tokens = append(tokens, subject[start:])
	if (len(tokens) != expectedNumReplyTokensV1 && len(tokens) < expectedNumReplyTokensV2) || tokens[0] != "$JS" || tokens[1] != "ACK" {
		return 0, 0, 0, 0, 0
	}
	offset := 2
	if len(tokens) >= expectedNumReplyTokensV2 {
		offset = 4
	}
	// TODO(dlc) - Should we error if we do not match consumer name?
	// stream is tokens[offset], consumer is offset+1.
	dc = uint64(parseAckReplyNum(tokens[offset+2]))
	sseq, dseq = uint64(parseAckReplyNum(tokens[offset+3])), uint64(parseAckReplyNum(tokens[offset+4]))
	ts = parseAckReplyNum(tokens[offset+5])
	pending = uint64(parseAckReplyNum(tokens[offset+6]))

	return sseq, dseq, dc, ts, pending
}

// NextSeq returns the next delivered sequence number for this consumer.
func (o *consumer) nextSeq() uint64 {
	o.mu.RLock()
	dseq := o.dseq
	o.mu.RUnlock()
	return dseq
}

// Used to hold skip list when deliver policy is last per subject.
type lastSeqSkipList struct {
	resume uint64
	seqs   []uint64
}

// Let's us know we have a skip list, which is for deliver last per subject and we are just starting.
// Lock should be held.
func (o *consumer) hasSkipListPending() bool {
	return o.lss != nil && len(o.lss.seqs) > 0
}

// reconcileStateWithStream reconciles consumer state when the stream has reverted
// due to data loss (e.g., VM crash). This handles the case where consumer state
// is ahead of the stream's last sequence.
// Lock should be held.
func (o *consumer) reconcileStateWithStream(streamLastSeq uint64) {
	// If an ack floor is higher than stream last sequence,
	// reset back down but keep the highest known sequences.
	if o.asflr > streamLastSeq {
		o.asflr = streamLastSeq
		// Delivery floor is one below the delivered sequence,
		// but if it is zero somehow, ensure we don't underflow.
		o.adflr = o.dseq
		if o.adflr > 0 {
			o.adflr--
		}
		o.pending = nil
		o.rdc = nil
	}

	// Remove pending entries that are beyond the stream's last sequence
	if len(o.pending) > 0 {
		for seq := range o.pending {
			if seq > streamLastSeq {
				delete(o.pending, seq)
			}
		}
	}

	// Remove redelivered entries that are beyond the stream's last sequence
	if len(o.rdc) > 0 {
		for seq := range o.rdc {
			if seq > streamLastSeq {
				delete(o.rdc, seq)
			}
		}
	}

	// Update starting sequence and delivery sequence based on pending state
	if len(o.pending) == 0 {
		o.sseq = o.asflr + 1
		o.dseq = o.adflr + 1
	} else {
		// Find highest stream sequence in pending
		var maxStreamSeq uint64
		var maxConsumerSeq uint64

		for streamSeq, p := range o.pending {
			if streamSeq > maxStreamSeq {
				maxStreamSeq = streamSeq
			}
			if p.Sequence > maxConsumerSeq {
				maxConsumerSeq = p.Sequence
			}
		}

		// Set next sequences based on highest pending
		o.sseq = maxStreamSeq + 1
		o.dseq = maxConsumerSeq + 1
	}
}

// Will select the starting sequence.
func (o *consumer) selectStartingSeqNo() error {
	if o.mset == nil || o.mset.store == nil {
		o.sseq = 1
	} else {
		var state StreamState
		o.mset.store.FastState(&state)
		if o.cfg.OptStartSeq == 0 {
			if o.cfg.DeliverPolicy == DeliverAll {
				o.sseq = state.FirstSeq
			} else if o.cfg.DeliverPolicy == DeliverLast {
				if o.subjf == nil {
					o.sseq = state.LastSeq
				} else {
					// If we are partitioned here this will be properly set when we become leader.
					for _, filter := range o.subjf {
						ss, err := o.mset.store.FilteredState(1, filter.subject)
						if err != nil {
							return err
						}
						if ss.Last > o.sseq {
							o.sseq = ss.Last
						}
					}
				}
			} else if o.cfg.DeliverPolicy == DeliverLastPerSubject {
				// If our parent stream is set to max msgs per subject of 1 this is just
				// a normal consumer at this point. We can avoid any heavy lifting.
				o.mset.cfgMu.RLock()
				mmp := o.mset.cfg.MaxMsgsPer
				o.mset.cfgMu.RUnlock()
				if mmp == 1 {
					o.sseq = state.FirstSeq
				} else {
					filters := make([]string, 0, len(o.subjf))
					if o.subjf == nil {
						filters = append(filters, o.cfg.FilterSubject)
					} else {
						for _, filter := range o.subjf {
							filters = append(filters, filter.subject)
						}
					}

					lss := &lastSeqSkipList{resume: state.LastSeq}
					lss.seqs, _ = o.mset.store.MultiLastSeqs(filters, 0, 0)

					if len(lss.seqs) == 0 {
						o.sseq = state.LastSeq
					} else {
						o.sseq = lss.seqs[0]
					}
					// Assign skip list.
					o.lss = lss
				}
			} else if o.cfg.OptStartTime != nil {
				// If we are here we are time based.
				// TODO(dlc) - Once clustered can't rely on this.
				o.sseq = o.mset.store.GetSeqFromTime(*o.cfg.OptStartTime)
				// Here we want to see if we are filtered, and if so possibly close the gap
				// to the nearest first given our starting sequence from time. This is so we do
				// not force the system to do a linear walk between o.sseq and the real first.
				if len(o.subjf) > 0 {
					nseq := state.LastSeq
					for _, filter := range o.subjf {
						// Use first sequence since this is more optimized atm.
						ss, err := o.mset.store.FilteredState(state.FirstSeq, filter.subject)
						if err != nil {
							return err
						}
						if ss.First >= o.sseq && ss.First < nseq {
							nseq = ss.First
						}
					}
					// Skip ahead if possible.
					if nseq > o.sseq && nseq < state.LastSeq {
						o.sseq = nseq
					}
				}
			} else {
				// DeliverNew
				o.sseq = state.LastSeq + 1
			}
		} else {
			o.sseq = o.cfg.OptStartSeq
		}

		if state.FirstSeq == 0 && o.cfg.OptStartSeq == 0 {
			// If the stream is empty, deliver only new.
			o.sseq = 1
		} else if o.sseq > state.LastSeq && o.cfg.OptStartSeq == 0 {
			// If selected sequence is in the future, clamp back down.
			o.sseq = state.LastSeq + 1
		} else if o.sseq < state.FirstSeq {
			// If the first sequence is further ahead than the starting sequence,
			// there are no messages there anymore, so move the sequence up.
			o.sseq = state.FirstSeq
		}
	}

	// Always set delivery sequence to 1.
	o.dseq = 1
	// Set ack delivery floor to delivery-1
	o.adflr = o.dseq - 1
	// Set ack store floor to store-1
	o.asflr = o.sseq - 1
	// Set our starting sequence state.
	// But only if we're not clustered, if clustered we propose upon becoming leader.
	o.mset.cfgMu.RLock()
	isR1 := o.cfg.replicas(&o.mset.cfg) == 1
	o.mset.cfgMu.RUnlock()
	if o.store != nil && o.sseq > 0 && isR1 {
		if err := o.store.SetStarting(o.sseq - 1); err != nil {
			return err
		}
	}
	return nil
}

// Test whether a config represents a durable subscriber.
func isDurableConsumer(config *ConsumerConfig) bool {
	return config != nil && config.Durable != _EMPTY_
}

func (o *consumer) isDurable() bool {
	return o.cfg.Durable != _EMPTY_
}

// Are we in push mode, delivery subject, etc.
func (o *consumer) isPushMode() bool {
	return o.cfg.DeliverSubject != _EMPTY_
}

func (o *consumer) isPullMode() bool {
	return o.cfg.DeliverSubject == _EMPTY_
}

// Name returns the name of this consumer.
func (o *consumer) String() string {
	o.mu.RLock()
	n := o.name
	o.mu.RUnlock()
	return n
}

func createConsumerName() string {
	return getHash(nuid.Next())
}

// Lock should be held.
func (mset *stream) createStableConsumerHash() string {
	id := fmt.Sprintf("%s %s", mset.cfg.Name, mset.acc.Name)
	if domain := mset.srv.getOpts().JetStreamDomain; domain != _EMPTY_ {
		id = fmt.Sprintf("%s %s", id, domain)
	}
	return getHash(id)
}

// Lock should be held.
func (mset *stream) createSourcingConsumerHash(ssi *StreamSource, sources []*StreamSource) string {
	id := mset.createStableConsumerHash()

	// If the stream sources contain the same stream at least twice, we use a more strict hash of
	// an ID that also contains filter subjects etc. If the stream name is only used once, we can
	// support the stable identifier above.
	var once bool
	for _, src := range sources {
		if src.Name == ssi.Name {
			if once {
				if ssi.iname == _EMPTY_ {
					ssi.setIndexName()
				}
				// Append identifying information of the filter subjects, etc. to make it unique
				id = fmt.Sprintf("%s %s", id, ssi.iname)
				break
			} else {
				once = true
			}
		}
	}

	return getHash(id)
}

// deleteConsumer will delete the consumer from this stream.
func (mset *stream) deleteConsumer(o *consumer) error {
	return o.delete()
}

func (o *consumer) getStream() *stream {
	o.mu.RLock()
	mset := o.mset
	o.mu.RUnlock()
	return mset
}

func (o *consumer) streamName() string {
	o.mu.RLock()
	mset := o.mset
	o.mu.RUnlock()
	if mset != nil {
		return mset.name()
	}
	return _EMPTY_
}

// streamIdentity returns the identity of the consumer's stream, which changes
// if the stream is recreated. Returns empty if the stream is not available.
func (o *consumer) streamIdentity() string {
	o.mu.RLock()
	mset := o.mset
	o.mu.RUnlock()
	if mset != nil {
		return mset.identity()
	}
	return _EMPTY_
}

// Active indicates if this consumer is still active.
func (o *consumer) isActive() bool {
	o.mu.RLock()
	active := o.active && o.mset != nil
	o.mu.RUnlock()
	return active
}

// hasNoLocalInterest return true if we have no local interest.
func (o *consumer) hasNoLocalInterest() bool {
	o.mu.RLock()
	interest := o.acc.sl.HasInterest(o.cfg.DeliverSubject)
	o.mu.RUnlock()
	return !interest
}

// This is when the underlying stream has been purged.
// sseq is the new first seq for the stream after purge.
// Consumer lock should NOT be held but the parent stream
// lock MUST be held.
func (o *consumer) purge(sseq uint64, slseq uint64, isWider bool) {
	// Do not update our state unless we know we are the leader.
	if !o.isLeader() {
		return
	}
	// Signals all have been purged for this consumer.
	if sseq == 0 && !isWider {
		sseq = slseq + 1
	}

	var store StreamStore
	if isWider {
		o.mu.RLock()
		if o.mset != nil {
			store = o.mset.store
		}
		o.mu.RUnlock()
	}

	o.mu.Lock()
	// Do not go backwards
	if o.sseq < sseq {
		o.sseq = sseq
	}

	if o.asflr < sseq {
		o.asflr = sseq - 1
		// We need to remove those no longer relevant from pending.
		for seq, p := range o.pending {
			if seq <= o.asflr {
				if p.Sequence > o.adflr {
					o.adflr = p.Sequence
					if o.adflr > o.dseq {
						o.dseq = o.adflr
					}
				}
				delete(o.pending, seq)
				delete(o.rdc, seq)
				o.updateAcks(p.Sequence, seq, _EMPTY_)
				// rdq handled below.
			} else if isWider && store != nil {
				// Our filtered subject, which could be all, is wider than the underlying purge.
				// We need to check if the pending items left are still valid.
				var smv StoreMsg
				if _, err := store.LoadMsg(seq, &smv); err == errDeletedMsg || err == ErrStoreMsgNotFound {
					if p.Sequence > o.adflr {
						o.adflr = p.Sequence
						if o.adflr > o.dseq {
							o.dseq = o.adflr
						}
					}
					delete(o.pending, seq)
					delete(o.rdc, seq)
					o.updateAcks(p.Sequence, seq, _EMPTY_)
				}
			}
		}
	}

	// This means we can reset everything at this point.
	if len(o.pending) == 0 {
		o.pending, o.rdc = nil, nil
		o.adflr, o.asflr = o.dseq-1, o.sseq-1
	}

	// We need to remove all those being queued for redelivery under o.rdq
	if len(o.rdq) > 0 {
		rdq := o.rdq
		o.rdq = nil
		o.rdqi.Empty()
		for _, sseq := range rdq {
			if sseq >= o.sseq {
				o.addToRedeliverQueue(sseq)
			}
		}
	}
	// Grab some info in case of error below.
	s, acc, mset, name := o.srv, o.acc, o.mset, o.name
	o.mu.Unlock()

	if err := o.writeStoreState(); err != nil && s != nil && mset != nil {
		s.Warnf("Consumer '%s > %s > %s' error on write store state from purge: %v", acc, mset.nameLocked(false), name, err)
	}
}

func stopAndClearTimer(tp **time.Timer) {
	if *tp == nil {
		return
	}
	// Will get drained in normal course, do not try to
	// drain here.
	(*tp).Stop()
	*tp = nil
}

// Stop will shutdown  the consumer for the associated stream.
func (o *consumer) stop() error {
	return o.stopWithFlags(false, false, true, false)
}

func (o *consumer) deleteWithoutAdvisory() error {
	return o.stopWithFlags(true, false, true, false)
}

// Delete will delete the consumer for the associated stream and send advisories.
func (o *consumer) delete() error {
	return o.stopWithFlags(true, false, true, true)
}

// To test for closed state.
func (o *consumer) isClosed() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.closed
}

func (o *consumer) stopWithFlags(dflag, sdflag, doSignal, advisory bool) error {
	// If dflag is true determine if we are still assigned.
	var isAssigned bool
	if dflag {
		o.mu.RLock()
		acc, stream, consumer := o.acc, o.stream, o.name
		isClustered := o.js != nil && o.js.isClustered()
		o.mu.RUnlock()
		if isClustered {
			// Grab jsa to check assignment.
			var jsa *jsAccount
			if acc != nil {
				// Need lock here to avoid data race.
				acc.mu.RLock()
				jsa = acc.js
				acc.mu.RUnlock()
			}
			if jsa != nil {
				isAssigned = jsa.consumerAssigned(stream, consumer)
			}
		}
	}

	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil
	}
	o.closed = true

	// Signal to the monitor loop.
	// Can't use only qch here, since that's used when stepping down as a leader.
	if o.mqch != nil {
		close(o.mqch)
		o.mqch = nil
	}

	// Check if we are the leader and are being deleted (as a node).
	if dflag && o.isLeader() {
		// If we are clustered and node leader (probable from above), stepdown.
		if node := o.node; node != nil {
			node.StepDown()
		}

		// dflag does not necessarily mean that the consumer is being deleted,
		// just that the consumer node is being removed from this peer, so we
		// send delete advisories only if we are no longer assigned at the meta layer,
		// or we are not clustered.
		if !isAssigned && advisory {
			o.sendDeleteAdvisoryLocked()
		}
		if o.isPullMode() {
			// Release any pending.
			o.releaseAnyPendingRequests(isAssigned)
		}
	}

	if o.qch != nil {
		close(o.qch)
		o.qch = nil
	}

	a := o.acc
	store := o.store
	mset := o.mset
	o.mset = nil
	o.active = false
	o.unsubscribe(o.ackSubOld)
	o.unsubscribe(o.ackSub)
	o.unsubscribe(o.reqSub)
	o.unsubscribe(o.resetSub)
	o.unsubscribe(o.fcSubOld)
	o.unsubscribe(o.fcSub)
	o.ackSubOld = nil
	o.ackSub = nil
	o.reqSub = nil
	o.resetSub = nil
	o.fcSubOld = nil
	o.fcSub = nil
	if o.infoSub != nil {
		o.srv.sysUnsubscribe(o.infoSub)
		o.infoSub = nil
	}
	c := o.client
	o.client = nil
	sysc := o.sysc
	o.sysc = nil
	o.stopAndClearPtmr()
	stopAndClearTimer(&o.dtmr)
	stopAndClearTimer(&o.gwdtmr)
	delivery := o.cfg.DeliverSubject
	o.waiting = nil
	// Break us out of the readLoop.
	if doSignal {
		o.signalNewMessages()
	}
	n := o.node
	qgroup := o.cfg.DeliverGroup
	o.ackMsgs.unregister()
	if o.nextMsgReqs != nil {
		o.nextMsgReqs.unregister()
	}

	// For cleaning up the node assignment.
	var ca *consumerAssignment
	if dflag {
		ca = o.ca
	}
	js := o.js
	o.mu.Unlock()

	if c != nil {
		c.closeConnection(ClientClosed)
	}
	if sysc != nil {
		sysc.closeConnection(ClientClosed)
	}

	if delivery != _EMPTY_ {
		a.sl.clearNotification(delivery, qgroup, o.inch)
	}

	var rp RetentionPolicy
	if mset != nil {
		mset.mu.Lock()
		mset.removeConsumer(o)
		// No need for cfgMu's lock since mset.mu.Lock superseeds it.
		rp = mset.cfg.Retention
		mset.mu.Unlock()
	}

	// Cleanup messages that lost interest.
	if dflag && rp == InterestPolicy {
		o.cleanupNoInterestMessages(mset, true)
	}

	// Cluster cleanup.
	if n != nil {
		if dflag {
			n.Delete()
		} else {
			n.Stop()
		}
	}

	if ca != nil {
		js.mu.Lock()
		if ca.Group != nil {
			ca.Group.node = nil
		}
		js.mu.Unlock()
	}

	// Clean up our store.
	var err error
	if store != nil {
		if dflag {
			if sdflag {
				err = store.StreamDelete()
			} else {
				err = store.Delete()
			}
		} else {
			err = store.Stop()
		}
	} else if dflag {
		// If there's no store (for example, when it's offline), manually delete the directories.
		o.mu.RLock()
		stream, consumer := o.stream, o.name
		o.mu.RUnlock()
		accDir := filepath.Join(js.config.StoreDir, a.GetName())
		consumersDir := filepath.Join(accDir, streamsDir, stream, consumerDir)
		os.RemoveAll(filepath.Join(consumersDir, consumer))
	}

	return err
}

// We need to optionally remove all messages since we are interest based retention.
// We will do this consistently on all replicas. Note that if in clustered mode the non-leader
// consumers will need to restore state first.
// ignoreInterest marks whether the consumer should be ignored when determining interest.
// No lock held on entry.
func (o *consumer) cleanupNoInterestMessages(mset *stream, ignoreInterest bool) {
	o.mu.Lock()
	if !o.isLeader() {
		o.readStoredState()
	}
	start := o.asflr
	o.mu.Unlock()

	// Make sure we start at worst with first sequence in the stream.
	state := mset.state()
	if start < state.FirstSeq {
		start = state.FirstSeq
	}
	stop := state.LastSeq

	// Consumer's interests are ignored by default. If we should not ignore interest, unset.
	co := o
	if !ignoreInterest {
		co = nil
	}

	var rmseqs []uint64
	mset.mu.RLock()

	// If over this amount of messages to check, optimistically call to checkInterestState().
	// It will not always do the right thing in removing messages that lost interest, but ensures
	// we don't degrade performance by doing a linear scan through the whole stream.
	// Messages might need to expire based on limits to be cleaned up.
	// TODO(dlc) - Better way?
	const bailThresh = 100_000

	// Check if we would be spending too much time here and defer to separate go routine.
	if len(mset.consumers) == 0 {
		mset.mu.RUnlock()
		mset.mu.Lock()
		defer mset.mu.Unlock()
		mset.store.Purge()
		var state StreamState
		mset.store.FastState(&state)
		mset.lseq = state.LastSeq
		// Also make sure we clear any pending acks.
		mset.clearAllPreAcksBelowFloor(state.FirstSeq)
		return
	} else if stop-start > bailThresh {
		mset.mu.RUnlock()
		go mset.checkInterestState()
		return
	}

	mset.mu.RUnlock()
	mset.mu.Lock()
	for seq := start; seq <= stop; seq++ {
		if mset.noInterest(seq, co) {
			rmseqs = append(rmseqs, seq)
		}
	}
	mset.mu.Unlock()

	// These can be removed.
	for _, seq := range rmseqs {
		mset.store.RemoveMsg(seq)
	}
}

// Check that we do not form a cycle by delivering to a delivery subject
// that is part of the interest group.
func deliveryFormsCycle(cfg *StreamConfig, deliverySubject string) bool {
	for _, subject := range cfg.Subjects {
		if subjectIsSubsetMatch(deliverySubject, subject) {
			return true
		}
	}
	return false
}

// switchToEphemeral is called on startup when recovering ephemerals.
func (o *consumer) switchToEphemeral() {
	o.mu.Lock()
	o.cfg.Durable = _EMPTY_
	store := o.store
	interest := o.acc.sl.HasInterest(o.cfg.DeliverSubject)
	// Setup dthresh.
	o.updateInactiveThreshold(&o.cfg)
	o.updatePauseState(&o.cfg)
	// Pull consumers always run the delete timer, push consumers are
	// handled by updateDeliveryInterest below.
	if o.isLeader() && o.dthresh > 0 && o.isPullMode() {
		stopAndClearTimer(&o.dtmr)
		o.dtmr = time.AfterFunc(o.dthresh, o.deleteNotActive)
	}
	cfg := o.cfg
	o.mu.Unlock()

	// Update interest
	o.updateDeliveryInterest(interest)
	// Write out new config
	switch store := store.(type) {
	case *consumerFileStore:
		store.updateConfig(cfg)
	case *consumerMemStore:
		store.UpdateConfig(&cfg)
	}
}

// RequestNextMsgSubject returns the subject to request the next message when in pull or worker mode.
// Returns empty otherwise.
func (o *consumer) requestNextMsgSubject() string {
	return o.nextMsgSubj
}

func (o *consumer) decStreamPending(sseq uint64, subj string) {
	o.mu.Lock()

	// Update our cached num pending only if we think deliverMsg has not done so.
	if sseq >= o.sseq && o.isFilteredMatch(subj) {
		o.npc--
	}

	// Check if this message was pending.
	p, wasPending := o.pending[sseq]
	var rdc uint64
	if wasPending {
		rdc = o.deliveryCount(sseq)
	} else if _, ok := o.rdc[sseq]; ok && o.isLeader() {
		delete(o.rdc, sseq)
		// Pass 0 as the delivered sequence to only remove the redelivered state.
		o.updateAcks(0, sseq, _EMPTY_)
	}

	o.mu.Unlock()

	// If it was pending process it like an ack.
	if wasPending {
		// We could have the lock for the stream so do this in a go routine.
		// TODO(dlc) - We should do this with ipq vs naked go routines.
		go o.processTerm(sseq, p.Sequence, rdc, ackTermUnackedLimitsReason, _EMPTY_)
	}
}

func (o *consumer) account() *Account {
	o.mu.RLock()
	a := o.acc
	o.mu.RUnlock()
	return a
}

// Creates a sublist for consumer.
// All subjects share the same callback.
func (o *consumer) signalSubs() []string {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.sigSubs != nil {
		return o.sigSubs
	}

	if len(o.subjf) == 0 {
		subs := []string{fwcs}
		o.sigSubs = subs
		return subs
	}

	subs := make([]string, 0, len(o.subjf))
	for _, filter := range o.subjf {
		subs = append(subs, filter.subject)
	}
	o.sigSubs = subs
	return subs
}

// This is what will be called when our parent stream wants to kick us regarding a new message.
// We know that this subject matches us by how the parent handles registering us with the signaling sublist,
// but we must check if we are leader.
// We do need the sequence of the message however and we use the msg as the encoded seq.
func (o *consumer) processStreamSignal(seq uint64) {
	// We can get called here now when not leader, so bail fast
	// and without acquiring any locks.
	if !o.leader.Load() {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.mset == nil {
		return
	}
	if seq > o.npf {
		o.npc++
	}
	if seq < o.sseq {
		return
	}
	if o.isPushMode() && o.active || o.isPullMode() && !o.waiting.isEmpty() {
		o.signalNewMessages()
	}
}

// Used to compare if two multiple filtered subject lists are equal.
func subjectSliceEqual(slice1 []string, slice2 []string) bool {
	if len(slice1) != len(slice2) {
		return false
	}
	set2 := make(map[string]struct{}, len(slice2))
	for _, val := range slice2 {
		set2[val] = struct{}{}
	}
	for _, val := range slice1 {
		if _, ok := set2[val]; !ok {
			return false
		}
	}
	return true
}

// Utility for simpler if conditions in Consumer config checks.
// In future iteration, we can immediately create `o.subjf` and
// use it to validate things.
func gatherSubjectFilters(filter string, filters []string) []string {
	if filter != _EMPTY_ {
		filters = append(filters, filter)
	}
	// list of filters should never contain non-empty filter.
	return filters
}

// shouldStartMonitor will return true if we should start a monitor
// goroutine or will return false if one is already running.
func (o *consumer) shouldStartMonitor() bool {
	// monitorMu is held across the monitorWg.Add below so that it cannot race
	// a concurrent monitorWg.Wait in stopMonitoring. It is taken before o.mu to
	// keep a consistent lock ordering.
	o.monitorMu.Lock()
	defer o.monitorMu.Unlock()

	o.mu.Lock()
	if o.inMonitor {
		o.mu.Unlock()
		return false
	}
	o.inMonitor = true
	o.mu.Unlock()

	o.monitorWg.Add(1)
	return true
}

// Clear the monitor running state. The monitor goroutine should
// call this in a defer to clean up on exit.
func (o *consumer) clearMonitorRunning() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.inMonitor {
		o.monitorWg.Done()
		o.inMonitor = false
	}
}

// stopMonitoring signals any running monitor goroutine to quit and waits for
// it to fully exit.
func (o *consumer) stopMonitoring() {
	// monitorMu is held across both the quit signal and the wait so that a
	// concurrent shouldStartMonitor cannot slip a new monitor generation in
	// between.
	o.monitorMu.Lock()
	defer o.monitorMu.Unlock()
	o.signalMonitorQuit()
	o.monitorWg.Wait()
}

// Test whether we are in the monitor routine.
func (o *consumer) isMonitorRunning() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.inMonitor
}

// If we detect that our ackfloor is higher than the stream's last sequence, return this error.
var errAckFloorHigherThanLastSeq = errors.New("consumer ack floor is higher than streams last sequence")
var errAckFloorInvalid = errors.New("consumer ack floor is invalid")

// If we are a consumer of an interest or workqueue policy stream, process that state and make sure consistent.
func (o *consumer) checkStateForInterestStream(ss *StreamState) error {
	o.mu.RLock()
	// See if we need to process this update if our parent stream is not a limits policy stream.
	mset := o.mset
	shouldProcessState := mset != nil && o.retention != LimitsPolicy
	if o.closed || !shouldProcessState || o.store == nil || ss == nil {
		o.mu.RUnlock()
		return nil
	}
	store := mset.store
	state, err := o.store.State()

	filters, subjf, filter := o.filters, o.subjf, _EMPTY_
	var wc bool
	if filters == nil && subjf != nil {
		filter, wc = subjf[0].subject, subjf[0].hasWildcard
	}
	chkfloor := o.chkflr
	o.mu.RUnlock()

	if err != nil {
		return err
	}

	asflr := state.AckFloor.Stream
	// Protect ourselves against rolling backwards.
	if asflr&(1<<63) != 0 {
		return errAckFloorInvalid
	}
	dflr := asflr
	if len(state.Pending) > 0 && state.Delivered.Stream > dflr {
		dflr = state.Delivered.Stream
	}

	// Check if the underlying stream's last sequence is less than our floor.
	// This can happen if the stream has been reset and has not caught up yet.
	if asflr > ss.LastSeq {
		return errAckFloorHigherThanLastSeq
	}

	var smv StoreMsg
	var seq, nseq uint64
	// Start at first stream seq or a previous check floor, whichever is higher.
	// Note this will really help for interest retention, with WQ the loadNextMsg
	// gets us a long way already since it will skip deleted msgs not for our filter.
	fseq := ss.FirstSeq
	if chkfloor > fseq {
		fseq = chkfloor
	}

	var retryAsflr uint64
	for seq = fseq; dflr > 0 && seq <= dflr; seq++ {
		if filters != nil {
			_, nseq, err = store.LoadNextMsgMulti(filters, seq, &smv)
		} else {
			_, nseq, err = store.LoadNextMsg(filter, wc, seq, &smv)
		}
		// if we advanced sequence update our seq. This can be on no error and EOF.
		if nseq > seq {
			seq = nseq
		}
		if err == nil {
			// Only ack though if no error and seq <= ack floor.
			if seq <= asflr {
				didRemove := mset.ackMsg(o, seq)
				// Removing the message could fail. For example if clustered since we need to propose it.
				// Overwrite retry floor (only the first time) to allow us to check next time if the removal was successful.
				if didRemove && retryAsflr == 0 {
					retryAsflr = seq
				}
			} else if seq <= dflr {
				// Store the first entry above our ack floor, so we don't need to look it up again on retryAsflr=0.
				if retryAsflr == 0 {
					retryAsflr = seq
				}
				// If we have pending, we will need to walk through to delivered in case we missed any of those acks as well.
				if _, ok := state.Pending[seq]; !ok {
					// The filters are already taken into account,
					mset.ackMsg(o, seq)
				}
			}
		} else if err == ErrStoreEOF {
			break
		}
	}
	// If retry floor was not overwritten, set to ack floor+1, we don't need to account for any retries below it.
	// However, our ack floor may be lower than the next message we can receive, so we correct it upward if needed.
	if retryAsflr == 0 {
		if filters != nil {
			_, nseq, err = store.LoadNextMsgMulti(filters, asflr+1, &smv)
		} else {
			_, nseq, err = store.LoadNextMsg(filter, wc, asflr+1, &smv)
		}
		if err == nil {
			retryAsflr = max(asflr+1, nseq)
		} else if err == ErrStoreEOF {
			retryAsflr = ss.LastSeq + 1
		}
	}

	o.mu.Lock()
	// Update our check floor.
	// Check floor must never be greater than ack floor+1, otherwise subsequent calls to this function would skip work.
	if retryAsflr > o.chkflr {
		o.chkflr = retryAsflr
	}
	o.mu.Unlock()
	return nil
}

func (o *consumer) resetPtmr(delay time.Duration) {
	// A delay of zero means it should be stopped.
	if delay == 0 {
		o.stopAndClearPtmr()
		return
	}

	if o.ptmr == nil {
		o.ptmr = time.AfterFunc(delay, o.checkPending)
	} else {
		o.ptmr.Reset(delay)
	}
	o.ptmrEnd = time.Now().Add(delay)
}

func (o *consumer) stopAndClearPtmr() {
	// If the end time is unset, short-circuit since the timer will already be stopped.
	if o.ptmrEnd.IsZero() {
		return
	}
	stopAndClearTimer(&o.ptmr)
	o.ptmrEnd = time.Time{}
}

func (o *consumer) resetPendingDeliveries() {
	for _, pmsg := range o.pendingDeliveries {
		pmsg.returnToPool()
	}
	o.pendingDeliveries = nil
	for _, wd := range o.waitingDeliveries {
		wd.recycle()
	}
	o.waitingDeliveries = nil
}
