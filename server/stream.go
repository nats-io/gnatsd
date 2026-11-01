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
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nats-server/v2/server/gsl"
	"github.com/nats-io/nuid"
)

// StreamConfigRequest is used to create or update a stream.
type StreamConfigRequest struct {
	StreamConfig
	// This is not part of the StreamConfig, because its scoped to request,
	// and not to the stream itself.
	Pedantic bool `json:"pedantic,omitempty"`
}

// StreamConfig will determine the name, subjects and retention policy
// for a given stream. If subjects is empty the name will be used.
type StreamConfig struct {
	Name         string           `json:"name"`
	Description  string           `json:"description,omitempty"`
	Subjects     []string         `json:"subjects,omitempty"`
	Retention    RetentionPolicy  `json:"retention"`
	MaxConsumers int              `json:"max_consumers"`
	MaxMsgs      int64            `json:"max_msgs"`
	MaxBytes     int64            `json:"max_bytes"`
	MaxAge       time.Duration    `json:"max_age"`
	MaxMsgsPer   int64            `json:"max_msgs_per_subject"`
	MaxMsgSize   int32            `json:"max_msg_size,omitempty"`
	Discard      DiscardPolicy    `json:"discard"`
	Storage      StorageType      `json:"storage"`
	Replicas     int              `json:"num_replicas"`
	NoAck        bool             `json:"no_ack,omitempty"`
	Duplicates   time.Duration    `json:"duplicate_window,omitempty"`
	Placement    *Placement       `json:"placement,omitempty"`
	Mirror       *StreamSource    `json:"mirror,omitempty"`
	Sources      []*StreamSource  `json:"sources,omitempty"`
	Compression  StoreCompression `json:"compression"`
	FirstSeq     uint64           `json:"first_seq,omitempty"`

	// Allow applying a subject transform to incoming messages before doing anything else
	SubjectTransform *SubjectTransformConfig `json:"subject_transform,omitempty"`

	// Allow republish of the message after being sequenced and stored.
	RePublish *RePublish `json:"republish,omitempty"`

	// Allow higher performance, direct access to get individual messages. E.g. KeyValue
	AllowDirect bool `json:"allow_direct"`
	// Allow higher performance and unified direct access for mirrors as well.
	MirrorDirect bool `json:"mirror_direct"`

	// Allow KV like semantics to also discard new on a per subject basis
	DiscardNewPer bool `json:"discard_new_per_subject,omitempty"`

	// Optional qualifiers. These can not be modified after set to true.

	// Sealed will seal a stream so no messages can get out or in.
	Sealed bool `json:"sealed"`
	// DenyDelete will restrict the ability to delete messages.
	DenyDelete bool `json:"deny_delete"`
	// DenyPurge will restrict the ability to purge messages.
	DenyPurge bool `json:"deny_purge"`
	// AllowRollup allows messages to be placed into the system and purge
	// all older messages using a special msg header.
	AllowRollup bool `json:"allow_rollup_hdrs"`

	// The following defaults will apply to consumers when created against
	// this stream, unless overridden manually.
	// TODO(nat): Can/should we name these better?
	ConsumerLimits StreamConsumerLimits `json:"consumer_limits"`

	// AllowMsgTTL allows header initiated per-message TTLs. If disabled,
	// then the `NATS-TTL` header will be ignored.
	AllowMsgTTL bool `json:"allow_msg_ttl"`

	// SubjectDeleteMarkerTTL sets the TTL of delete marker messages left behind by
	// subject delete markers.
	SubjectDeleteMarkerTTL time.Duration `json:"subject_delete_marker_ttl,omitempty"`

	// AllowMsgCounter allows a stream to use (only) counter CRDTs.
	AllowMsgCounter bool `json:"allow_msg_counter,omitempty"`

	// AllowAtomicPublish allows atomic batch publishing into the stream.
	AllowAtomicPublish bool `json:"allow_atomic,omitempty"`

	// AllowMsgSchedules allows the scheduling of messages.
	AllowMsgSchedules bool `json:"allow_msg_schedules,omitempty"`

	// PersistMode allows to opt-in to different persistence mode settings.
	PersistMode PersistModeType `json:"persist_mode,omitempty"`

	// AllowBatchPublish allows fast batch publishing into the stream.
	AllowBatchPublish bool `json:"allow_batched,omitempty"`

	// Metadata is additional metadata for the Stream.
	Metadata map[string]string `json:"metadata,omitempty"`
}

// clone performs a deep copy of the StreamConfig struct, returning a new clone with
// all values copied.
func (cfg *StreamConfig) clone() *StreamConfig {
	clone := *cfg
	if cfg.Placement != nil {
		placement := *cfg.Placement
		clone.Placement = &placement
	}
	if cfg.Mirror != nil {
		mirror := *cfg.Mirror
		clone.Mirror = &mirror
	}
	if len(cfg.Sources) > 0 {
		clone.Sources = make([]*StreamSource, len(cfg.Sources))
		for i, cfgSource := range cfg.Sources {
			source := *cfgSource
			clone.Sources[i] = &source
		}
	}
	if cfg.SubjectTransform != nil {
		transform := *cfg.SubjectTransform
		clone.SubjectTransform = &transform
	}
	if cfg.RePublish != nil {
		rePublish := *cfg.RePublish
		clone.RePublish = &rePublish
	}
	if cfg.Metadata != nil {
		clone.Metadata = make(map[string]string, len(cfg.Metadata))
		for k, v := range cfg.Metadata {
			clone.Metadata[k] = v
		}
	}
	return &clone
}

type StreamConsumerLimits struct {
	InactiveThreshold time.Duration `json:"inactive_threshold,omitempty"`
	MaxAckPending     int           `json:"max_ack_pending,omitempty"`
}

// SubjectTransformConfig is for applying a subject transform (to matching messages) before doing anything else when a new message is received
type SubjectTransformConfig struct {
	Source      string `json:"src"`
	Destination string `json:"dest"`
}

// RePublish is for republishing messages once committed to a stream.
type RePublish struct {
	Source      string `json:"src,omitempty"`
	Destination string `json:"dest"`
	HeadersOnly bool   `json:"headers_only,omitempty"`
}

// PersistModeType determines what persistence mode the stream uses.
type PersistModeType int

const (
	// DefaultPersistMode specifies the default persist mode. Writes to the stream will immediately be flushed.
	// The publish acknowledgement will be sent after the persisting completes.
	DefaultPersistMode = PersistModeType(iota)
	// AsyncPersistMode specifies writes to the stream will be flushed asynchronously.
	// The publish acknowledgement may be sent before the persisting completes.
	// This means writes could be lost if they weren't flushed prior to a hard kill of the server.
	AsyncPersistMode
)

const (
	defaultPersistModeJSONString = `"default"`
	asyncPersistModeJSONString   = `"async"`
)

var (
	defaultPersistModeJSONBytes = []byte(defaultPersistModeJSONString)
	asyncPersistModeJSONBytes   = []byte(asyncPersistModeJSONString)
)

func (wc PersistModeType) String() string {
	switch wc {
	case DefaultPersistMode:
		return "Default"
	case AsyncPersistMode:
		return "Async"
	default:
		return "Unknown Persist Mode Type"
	}
}

func (wc PersistModeType) MarshalJSON() ([]byte, error) {
	switch wc {
	case DefaultPersistMode:
		return defaultPersistModeJSONBytes, nil
	case AsyncPersistMode:
		return asyncPersistModeJSONBytes, nil
	default:
		return nil, fmt.Errorf("can not marshal %v", wc)
	}
}

func (wc *PersistModeType) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case defaultPersistModeJSONString, `""`:
		*wc = DefaultPersistMode
	case asyncPersistModeJSONString:
		*wc = AsyncPersistMode
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}
	return nil
}

// JSPubAckResponse is a formal response to a publish operation.
type JSPubAckResponse struct {
	Error *ApiError `json:"error,omitempty"`
	*PubAck
}

// ToError checks if the response has a error and if it does converts it to an error
// avoiding the pitfalls described by https://yourbasic.org/golang/gotcha-why-nil-error-not-equal-nil/
func (r *JSPubAckResponse) ToError() error {
	if r.Error == nil {
		return nil
	}
	return r.Error
}

// PubAck is the detail you get back from a publish to a stream that was successful.
// e.g. +OK {"stream": "Orders", "seq": 22}
type PubAck struct {
	Stream    string `json:"stream"`
	Sequence  uint64 `json:"seq"`
	Domain    string `json:"domain,omitempty"`
	Duplicate bool   `json:"duplicate,omitempty"`
	Value     string `json:"val,omitempty"`
	BatchId   string `json:"batch,omitempty"`
	BatchSize uint64 `json:"count,omitempty"`
}

// CounterValue is the body of a message when used as a counter.
// e.g. {"val":"123"}
type CounterValue struct {
	Value string `json:"val"`
}

// CounterSources is the body of the Nats-Counter-Sources header.
// e.g. {"stream":{"subject":"123"}}
type CounterSources map[string]map[string]string

// BatchFlowAck is used for flow control when fast batch publishing into a stream.
// This message is vital to handling acknowledgements and flow control.
// These may technically be lost without the client receiving it. The client can retrieve
// these by using the "ping" operation if it's expecting acks but not receiving any.
type BatchFlowAck struct {
	// Type: "ack"
	Type string `json:"type"`
	// Sequence is the sequence of the message that triggered the ack.
	// If "gap: fail" this means the messages up to and including Sequence were persisted.
	// If "gap: ok" this means _some_ of the messages up to and including Sequence were persisted.
	// But there could have been gaps.
	Sequence uint64 `json:"seq"`
	// Messages indicates acknowledgements will be sent every N messages.
	Messages uint16 `json:"msgs"`
}

func (ack BatchFlowAck) MarshalJSON() ([]byte, error) {
	type Alias BatchFlowAck
	a := Alias(ack)
	a.Type = "ack"
	return json.Marshal(a)
}

// BatchFlowGap is used for reporting gaps when fast batch publishing into a stream.
// A forward gap means messages were lost, a backward gap means a message was duplicated or reordered.
// This message is purely informational and could technically be lost without the client receiving it.
type BatchFlowGap struct {
	// Type: "gap"
	Type string `json:"type"`
	// ExpectedLastSequence is the sequence expected to be received next.
	// If CurrentSequence is higher, messages starting from ExpectedLastSequence up to (but not including) CurrentSequence were lost.
	// If CurrentSequence is lower or equal, it's a backward gap and the batch is ended, the PubAck confirms what was persisted.
	ExpectedLastSequence uint64 `json:"last_seq"`
	// CurrentSequence is the sequence of the message that just came in and detected the gap.
	CurrentSequence uint64 `json:"seq"`
}

func (gap BatchFlowGap) MarshalJSON() ([]byte, error) {
	type Alias BatchFlowGap
	a := Alias(gap)
	a.Type = "gap"
	return json.Marshal(a)
}

// BatchFlowErr is used for reporting errors when fast batch publishing into a stream.
// This message is purely informational and could technically be lost without the client receiving it.
type BatchFlowErr struct {
	// Type: "err"
	Type string `json:"type"`
	// Sequence is the sequence of the message that triggered the error.
	// There are no (relative) guarantees whatsoever about whether the messages up to this sequence were persisted.
	// Such guarantees require the use of "gap: fail" and listening for BatchFlowAck and PubAck.
	Sequence uint64 `json:"seq"`
	// Error is used to return the error for the Sequence.
	Error *ApiError `json:"error"`
}

func (err BatchFlowErr) MarshalJSON() ([]byte, error) {
	type Alias BatchFlowErr
	a := Alias(err)
	a.Type = "err"
	return json.Marshal(a)
}

// StreamInfo shows config and current state for this stream.
type StreamInfo struct {
	Config     StreamConfig        `json:"config"`
	Created    time.Time           `json:"created"`
	State      StreamState         `json:"state"`
	Domain     string              `json:"domain,omitempty"`
	Cluster    *ClusterInfo        `json:"cluster,omitempty"`
	Mirror     *StreamSourceInfo   `json:"mirror,omitempty"`
	Sources    []*StreamSourceInfo `json:"sources,omitempty"`
	Alternates []StreamAlternate   `json:"alternates,omitempty"`
	// TimeStamp indicates when the info was gathered
	TimeStamp time.Time `json:"ts"`
}

// streamInfoClusterResponse is a response used in a cluster to communicate the stream info
// back to the meta leader as part of a stream list request.
type streamInfoClusterResponse struct {
	StreamInfo
	OfflineReason string `json:"offline_reason,omitempty"` // Reporting when a stream is offline.
}

type StreamAlternate struct {
	Name    string `json:"name"`
	Domain  string `json:"domain,omitempty"`
	Cluster string `json:"cluster"`
}

// ClusterInfo shows information about the underlying set of servers
// that make up the stream or consumer.
type ClusterInfo struct {
	Name        string              `json:"name,omitempty"`
	RaftGroup   string              `json:"raft_group,omitempty"`
	Leader      string              `json:"leader,omitempty"`
	LeaderSince *time.Time          `json:"leader_since,omitempty"`
	SystemAcc   bool                `json:"system_account,omitempty"`
	TrafficAcc  string              `json:"traffic_account,omitempty"`
	Replicas    []*PeerInfo         `json:"replicas,omitempty"`
	Desired     *DesiredClusterInfo `json:"desired,omitempty"`
}

// DesiredClusterInfo shows information of the desired set of servers
// that should make up the stream or consumer.
type DesiredClusterInfo struct {
	Created  time.Time                 `json:"created"`
	Name     string                    `json:"name,omitempty"`
	Replicas []*DesiredPeerInfo        `json:"replicas,omitempty"`
	Origin   *DesiredClusterInfoOrigin `json:"origin,omitempty"`
	Status   *DesiredClusterInfoStatus `json:"status,omitempty"`
}

type DesiredClusterInfoOrigin struct {
	// Original replicas before it was updated.
	Replicas int `json:"replicas"`
	// Original placement before it was updated.
	Placement *Placement `json:"placement,omitempty"`
	// Original retention before it was updated.
	Retention *RetentionPolicy `json:"retention,omitempty"`
}

// DesiredPeerInfo is a minimal version of PeerInfo that shows information about the desired peer set.
type DesiredPeerInfo struct {
	Name    string `json:"name"`              // Name is the unique name for the peer
	Offline bool   `json:"offline,omitempty"` // Offline indicates if it has not been seen recently
	Peer    string `json:"peer"`              // Peer is the unique ID for the peer
}

// MigrationStatusType classifies a migration status by what has to change for the
// migration to advance, so it can be matched on without parsing the status line.
type MigrationStatusType string

const (
	MigrationStatusMeta        MigrationStatusType = "meta"        // The meta leader must record or advance desired state.
	MigrationStatusMembership  MigrationStatusType = "membership"  // A proposed membership change must commit.
	MigrationStatusSnapshot    MigrationStatusType = "snapshot"    // A snapshot must be installed.
	MigrationStatusCatchup     MigrationStatusType = "catchup"     // Peers must become store-current.
	MigrationStatusQuorum      MigrationStatusType = "quorum"      // More peers must come online before we can act without losing quorum.
	MigrationStatusBlocked     MigrationStatusType = "blocked"     // Another asset must move first, i.e. the stream/consumer ordering constraint.
	MigrationStatusUnavailable MigrationStatusType = "unavailable" // Nothing to do here, we're shutting down, or the assignment is gone.
)

type DesiredClusterInfoStatus struct {
	// Description is a short status line describing what the group leader is currently
	// doing to move this group toward its desired state, or what it's waiting on.
	Description string `json:"description"`
	// Type classifies Description by what has to change for the migration to
	// advance, so it can be matched on without parsing the status line.
	Type MigrationStatusType `json:"type"`
	// Err is the underlying failure behind this status, if it had one. Only set
	// for faults that persist across cycles, never for races that resolve themselves.
	Err string `json:"err,omitempty"`
}

// mstat builds a migration status of the given type. The line is a short, stable
// phrase; keep it matchable by leaving anything unbounded to Err.
func mstat(t MigrationStatusType, format string, args ...any) *DesiredClusterInfoStatus {
	if len(args) == 0 {
		return &DesiredClusterInfoStatus{Type: t, Description: format}
	}
	return &DesiredClusterInfoStatus{Type: t, Description: fmt.Sprintf(format, args...)}
}

// withErr attaches the underlying failure, unless it's one that resolves itself on
// the next cycle. Those would flap in and out of the status for no good reason.
func (status *DesiredClusterInfoStatus) withErr(err error) *DesiredClusterInfoStatus {
	if err == nil ||
		errors.Is(err, ErrStoreClosed) ||
		errors.Is(err, errNotLeader) ||
		errors.Is(err, errNodeClosed) ||
		errors.Is(err, errMembershipChange) ||
		errors.Is(err, errNoSnapAvailable) {
		return status
	}
	status.Err = err.Error()
	return status
}

// PeerInfo shows information about all the peers in the cluster that
// are supporting the stream or consumer.
type PeerInfo struct {
	Name    string        `json:"name"`              // Name is the unique name for the peer
	Current bool          `json:"current"`           // Current indicates if it was seen recently and fully caught up
	Offline bool          `json:"offline,omitempty"` // Offline indicates if it has not been seen recently
	Active  time.Duration `json:"active"`            // Active is the nanoseconds since this peer was last seen
	Lag     uint64        `json:"lag,omitempty"`     // Lag is how many operations behind it is
	Peer    string        `json:"peer"`              // Peer is the unique ID for the peer
	Pending bool          `json:"pending,omitempty"` // Pending indicates the peer is part of the assignment, but is not a peer of the Raft group yet or is being removed.
	// For migrations.
	cluster string
}

// StreamSourceInfo shows information about an upstream stream source.
type StreamSourceInfo struct {
	Name              string                   `json:"name"`
	External          *ExternalStream          `json:"external,omitempty"`
	Lag               uint64                   `json:"lag"`
	Seq               uint64                   `json:"seq,omitempty"`
	Active            time.Duration            `json:"active"`
	Error             *ApiError                `json:"error,omitempty"`
	FilterSubject     string                   `json:"filter_subject,omitempty"`
	SubjectTransforms []SubjectTransformConfig `json:"subject_transforms,omitempty"`
}

// StreamSource dictates how streams can source from other streams.
type StreamSource struct {
	Name              string                   `json:"name"`
	OptStartSeq       uint64                   `json:"opt_start_seq,omitempty"`
	OptStartTime      *time.Time               `json:"opt_start_time,omitempty"`
	FilterSubject     string                   `json:"filter_subject,omitempty"`
	SubjectTransforms []SubjectTransformConfig `json:"subject_transforms,omitempty"`
	External          *ExternalStream          `json:"external,omitempty"`
	Consumer          *StreamConsumerSource    `json:"consumer,omitempty"`

	// Internal
	iname string // For indexing when stream names are the same for multiple sources.
}

// StreamConsumerSource dictates a durable consumer with a specific name is used for sourcing.
type StreamConsumerSource struct {
	Name           string `json:"name,omitempty"`
	DeliverSubject string `json:"deliver_subject,omitempty"`
}

// ExternalStream allows you to qualify access to a stream source in another account or domain.
type ExternalStream struct {
	ApiPrefix     string `json:"api"`
	DeliverPrefix string `json:"deliver"`
}

// Will return the domain for this external stream.
func (ext *ExternalStream) Domain() string {
	if ext == nil || ext.ApiPrefix == _EMPTY_ {
		return _EMPTY_
	}
	return tokenAt(ext.ApiPrefix, 2)
}

// For managing stream ingest.
const (
	streamDefaultMaxQueueMsgs  = 100_000
	streamDefaultMaxQueueBytes = 128 * 1024 * 1024
)

// For managing stream batches.
const (
	streamDefaultMaxBatchTimeout = 10 * time.Second
	// Atomic batches.
	streamDefaultMaxAtomicBatchInflightPerStream = 50
	streamDefaultMaxAtomicBatchInflightTotal     = 1000
	streamDefaultMaxAtomicBatchSize              = 1000
	// Fast batches.
	streamDefaultMaxFastBatchInflightPerStream = 1000
	streamDefaultMaxFastBatchInflightTotal     = 50_000
)

var (
	streamMaxBatchTimeout = streamDefaultMaxBatchTimeout
	// Atomic batches.
	streamMaxAtomicBatchInflightPerStream = streamDefaultMaxAtomicBatchInflightPerStream
	streamMaxAtomicBatchInflightTotal     = streamDefaultMaxAtomicBatchInflightTotal
	streamMaxAtomicBatchSize              = streamDefaultMaxAtomicBatchSize
	// Fast batches.
	streamMaxFastBatchInflightPerStream = streamDefaultMaxFastBatchInflightPerStream
	streamMaxFastBatchInflightTotal     = streamDefaultMaxFastBatchInflightTotal
)

// Stream is a jetstream stream of messages. When we receive a message internally destined
// for a Stream we will direct link from the client to this structure.
type stream struct {
	mu     sync.RWMutex // Read/write lock for the stream.
	js     *jetStream   // The internal *jetStream for the account.
	jsa    *jsAccount   // The JetStream account-level information.
	acc    *Account     // The account this stream is defined in.
	srv    *Server      // The server we are running in.
	client *client      // The internal JetStream client.
	sysc   *client      // The internal JetStream system client.

	// Isolates direct get and msg get reads from writes (ingest, atomic batch
	// commits, purges and deletes), so those reads never observe a partially
	// applied atomic batch or rollup. Other read paths (consumer delivery,
	// mirrors/sources, mset.mu-based info) are sequentially consistent and may
	// observe a prefix of an inflight batch.
	isolateMu sync.RWMutex

	// The current last subscription ID for the subscriptions through `client`.
	// Those subscriptions are for the subjects filters being listened to and captured by the stream.
	sid atomic.Uint64

	// Whether the stream layer has completed leader setup via setLeader. Unlike isLeader(),
	// which reads the raft node's current state, this tracks our own processed leadership.
	leader atomic.Bool

	pubAck    []byte                  // The template (prefix) to generate the pubAck responses for this stream quickly.
	outq      *jsOutQ                 // Queue of *jsPubMsg for sending messages.
	msgs      *ipQueue[*inMsg]        // Intra-process queue for the ingress of messages.
	gets      *ipQueue[*directGetReq] // Intra-process queue for the direct get requests.
	store     StreamStore             // The storage for this stream.
	ackq      *ipQueue[uint64]        // Intra-process queue for acks.
	lseq      uint64                  // The sequence number of the last message stored in the stream.
	term      uint64                  // Raft term, used to determine if we are still the leader for the current term (if applicable, 0 otherwise).
	lmsgId    string                  // The de-duplication message ID of the last message stored in the stream.
	consumers map[string]*consumer    // The consumers for this stream.
	cfg       StreamConfig            // The stream's config.
	cfgMu     sync.RWMutex            // Config mutex used to solve some races with consumer code
	created   time.Time               // Time the stream was created.
	stype     StorageType             // The storage type.
	tier      string                  // The tier is the number of replicas for the stream (e.g. "R1" or "R3").
	ddMu      sync.Mutex              // Lock for dedupe state.
	ddwin     time.Duration           // The dedupe window, mirrors cfg.Duplicates.
	ddmap     map[string]*ddentry     // The dedupe map.
	ddarr     []*ddentry              // The dedupe array.
	ddindex   int                     // The dedupe index.
	ddtmr     *time.Timer             // The dedupe timer.
	qch       chan struct{}           // The quit channel.
	mqch      chan struct{}           // The monitor's quit channel.
	active    bool                    // Indicates that there are active internal subscriptions (for the subject filters)
	// and/or mirror/sources consumers are scheduled to be established or already started.
	closed        atomic.Bool // Set to true when stop() is called on the stream.
	cisrun        atomic.Bool // Indicates one checkInterestState is already running.
	restoring     bool
	restoreLeader bool
	restoreTerm   uint64

	// Mirror
	mirror              *sourceInfo
	mirrorConsumerSetup *time.Timer

	// Sources
	sources              map[string]*sourceInfo
	sourceSetupSchedules map[string]*time.Timer
	sourcesConsumerSetup *time.Timer
	smsgs                *ipQueue[*inMsg] // Intra-process queue for all incoming sourced messages.

	// Indicates we have direct/sourcing consumers.
	sourcingConsumers int

	// For input subject transform.
	itr *subjectTransform

	// For republishing.
	tr *subjectTransform

	// For processing consumers without main stream lock.
	clsMu sync.RWMutex
	cList []*consumer                    // Consumer list.
	sch   chan struct{}                  // Channel to signal consumers.
	sigq  *ipQueue[*cMsg]                // Intra-process queue for the messages to signal to the consumers.
	csl   *gsl.GenericSublist[*consumer] // Consumer subscription list.

	// Leader will store seq/msgTrace in clustering mode. Used in applyStreamEntries
	// to know if trace event should be sent after processing.
	mt map[uint64]*msgTrace

	// For non limits policy streams when they process an ack before the actual msg.
	// Can happen in stretch clusters, multi-cloud, or during catchup for a restarted server.
	preAcks map[uint64]map[*consumer]struct{}

	// TODO(dlc) - Hide everything below behind two pointers.
	// Clustered mode.
	sa        *streamAssignment       // What the meta controller uses to assign streams to peers.
	node      RaftNode                // Our RAFT node for the stream's group.
	catchup   atomic.Bool             // Used to signal we are in catchup mode.
	catchups  map[string]*catchupPeer // Peers being caught up out of band, see catchupPeer.
	syncSub   *subscription           // Internal subscription for sync messages (on "$JSC.SYNC").
	infoSub   *subscription           // Internal subscription for stream info requests.
	clMu      sync.Mutex              // The mutex for clseq and clfs.
	clseq     uint64                  // The current last seq being proposed to the NRG layer.
	clfs      uint64                  // The count (offset) of the number of failed NRG sequences used to compute clseq.
	lqsent    time.Time               // The time at which the last lost quorum advisory was sent. Used to rate limit.
	uch       chan struct{}           // The channel to signal updates to the monitor routine.
	inMonitor bool                    // True if the monitor routine has been started.
	werr      error                   // If a write error was encountered, and if so what error.

	inflight                    map[string]*inflightSubjectRunningTotal // Inflight message sizes per subject.
	inflightTransform           map[uint64]string                       // Inflight message's optional transformed subject.
	clusteredCounterTotal       map[string]*msgCounterRunningTotal      // Inflight counter totals.
	expectedPerSubjectSequence  map[uint64]string                       // Inflight 'expected per subject' subjects per clseq.
	expectedPerSubjectInProcess map[string]struct{}                     // Current 'expected per subject' subjects in process.

	// Direct get subscription.
	directLeaderSub *subscription
	directSub       *subscription
	lastBySub       *subscription
	mirrorDirectSub *subscription // Mirrors only.
	mirrorLastBySub *subscription // Mirrors only.

	monitorWg sync.WaitGroup // Wait group for the monitor routine.
	monitorMu sync.Mutex     // Serializes monitorWg's Add against Wait to prevent a WaitGroup reuse panic.

	// If standalone/single-server, the offline reason needs to be stored directly in the stream.
	// Otherwise, if clustered it will be part of the stream assignment.
	offlineReason string

	batches *batching // Inflight batches prior to committing them.
}

// inflightSubjectRunningTotal stores a running total of inflight messages for a specific subject.
type inflightSubjectRunningTotal struct {
	bytes    uint64 // Running total of inflight bytes for inflight messages.
	ops      uint64 // Inflight operations, i.e. inflight messages for this subject. If this reaches zero, we can remove the running total.
	schedule bool   // Marks whether the last message is a schedule.
}

// msgCounterRunningTotal stores a running total and a number of inflight
// but not yet applied clustered proposals/operations for this counter.
type msgCounterRunningTotal struct {
	total   *big.Int       // Running total.
	sources CounterSources // Last seen counter sources.
	ops     uint64         // Inflight operations. If this reaches zero, we can remove the running total.
}

// StreamSourceState is the tracked state for a single stream source, as
// persisted by the store so it survives a restart or a leader change.
type StreamSourceState struct {
	// Seq is the last stream message sequence number seen from the source.
	Seq uint64
	// Ident is the identity of the source stream, used to detect that it was
	// recreated. Empty if the source did not report one, i.e. it is an older
	// server or a pre-2.10 message header.
	Ident string
}

type sourceInfo struct {
	name  string        // The name of the stream being sourced.
	iname string        // The unique index name of this particular source.
	cname string        // The name of the current consumer for this source.
	ident string        // The identity of this source, a recreated stream results in a different ident.
	sub   *subscription // The subscription to the consumer.

	msgs  *ipQueue[*inMsg]    // Intra-process queue for incoming messages.
	sseq  uint64              // Last stream message sequence number seen from the source.
	dseq  uint64              // Last delivery (i.e. consumer's) sequence number.
	cseq  uint64              // Stream message sequence number of the last delivery counted towards dseq.
	lag   uint64              // 0 or number of messages pending (as last reported by the consumer) - 1.
	err   *ApiError           // The API error that caused the last consumer setup to fail.
	fails int                 // The number of times trying to setup the consumer failed.
	fcid  string              // The last flow control reply subject the consumer reported being stalled on.
	fcsc  int                 // Consecutive heartbeats reporting the same stalled flow control reply subject.
	last  atomic.Int64        // Time the consumer was created or of last message it received.
	lreq  time.Time           // The last time setupMirrorConsumer/setupSourceConsumer was called.
	qch   chan struct{}       // Quit channel.
	sip   bool                // Setup in progress.
	rc    bool                // Recreate based on a stream identity mismatch.
	wg    sync.WaitGroup      // WaitGroup for the consumer's go routine.
	sf    string              // The subject filter.
	sfs   []string            // The subject filters.
	trs   []*subjectTransform // The subject transforms.
}

// For mirrors and direct get
const (
	dgetGroup          = sysGroup
	dgetCaughtUpThresh = 10
)

// Headers for published messages.
const (
	JSMsgId                   = "Nats-Msg-Id"
	JSExpectedStream          = "Nats-Expected-Stream"
	JSExpectedLastSeq         = "Nats-Expected-Last-Sequence"
	JSExpectedLastSubjSeq     = "Nats-Expected-Last-Subject-Sequence"
	JSExpectedLastSubjSeqSubj = "Nats-Expected-Last-Subject-Sequence-Subject"
	JSExpectedLastMsgId       = "Nats-Expected-Last-Msg-Id"
	JSStreamSource            = "Nats-Stream-Source"
	JSLastConsumerSeq         = "Nats-Last-Consumer"
	JSLastStreamSeq           = "Nats-Last-Stream"
	JSConsumerStalled         = "Nats-Consumer-Stalled"
	JSMsgRollup               = "Nats-Rollup"
	JSMsgSize                 = "Nats-Msg-Size"
	JSResponseType            = "Nats-Response-Type"
	JSMessageTTL              = "Nats-TTL"
	JSMarkerReason            = "Nats-Marker-Reason"
	JSMessageIncr             = "Nats-Incr"
	JSMessageCounterSources   = "Nats-Counter-Sources"
	JSBatchId                 = "Nats-Batch-Id"
	JSBatchSeq                = "Nats-Batch-Sequence"
	JSBatchCommit             = "Nats-Batch-Commit"
	JSSchedulePattern         = "Nats-Schedule"
	JSScheduleTimeZone        = "Nats-Schedule-Time-Zone"
	JSScheduleTTL             = "Nats-Schedule-TTL"
	JSScheduleRollup          = "Nats-Schedule-Rollup"
	JSScheduleTarget          = "Nats-Schedule-Target"
	JSScheduleSource          = "Nats-Schedule-Source"
)

// Headers for published KV messages.
var (
	KVOperation           = "KV-Operation"
	KVOperationValuePurge = []byte("PURGE")
)

// Headers for scheduled messages.
const (
	JSScheduler         = "Nats-Scheduler"
	JSScheduleNext      = "Nats-Schedule-Next"
	JSScheduleNextPurge = "purge" // If it's a non-repeating/delayed message, the schedule is purged.
)

// Headers for republished messages and direct get responses.
const (
	JSStream       = "Nats-Stream"
	JSSequence     = "Nats-Sequence"
	JSTimeStamp    = "Nats-Time-Stamp"
	JSSubject      = "Nats-Subject"
	JSLastSequence = "Nats-Last-Sequence" // The sequence number of the precedent message either republished or in a batch of responses. 0 in the first message.
	JSNumPending   = "Nats-Num-Pending"   // Number of messages pending in the multi/batched get response
	JSUpToSequence = "Nats-UpTo-Sequence"
)

// Rollups, can be subject only or all messages.
const (
	JSMsgRollupSubject = "sub"
	JSMsgRollupAll     = "all"
)

// Applied limits in the Nats-Applied-Limit header.
const (
	JSMarkerReasonMaxAge = "MaxAge"
	JSMarkerReasonPurge  = "Purge"
	JSMarkerReasonRemove = "Remove"
)

const (
	jsCreateResponse = "create"
)

// Dedupe entry
type ddentry struct {
	id  string // The unique message ID provided by the client.
	seq uint64 // The sequence number of the message.
	ts  int64  // The timestamp of the message.
}

// Replicas Range
const StreamMaxReplicas = 5

// AddStream adds a stream for the given account.
func (a *Account) addStream(config *StreamConfig) (*stream, error) {
	return a.addStreamWithAssignment(config, nil, nil, false, false)
}

func (a *Account) addStreamForRestore(config *StreamConfig) (*stream, error) {
	return a.addStreamWithAssignmentAndMode(config, nil, nil, false, false, true)
}

// recoverStream recovers a stream from disk for the given account.
func (a *Account) recoverStream(config *StreamConfig) (*stream, error) {
	return a.addStreamWithAssignment(config, nil, nil, false, true)
}

// AddStreamWithStore adds a stream for the given account with custome store config options.
func (a *Account) addStreamWithStore(config *StreamConfig, fsConfig *FileStoreConfig) (*stream, error) {
	return a.addStreamWithAssignment(config, fsConfig, nil, false, false)
}

func (a *Account) addStreamPedantic(config *StreamConfig, pedantic bool) (*stream, error) {
	return a.addStreamWithAssignment(config, nil, nil, pedantic, false)
}

func (a *Account) addStreamWithAssignment(config *StreamConfig, fsConfig *FileStoreConfig, sa *streamAssignment, pedantic, recovering bool) (*stream, error) {
	return a.addStreamWithAssignmentAndMode(config, fsConfig, sa, pedantic, recovering, false)
}

func (a *Account) addStreamWithAssignmentAndMode(config *StreamConfig, fsConfig *FileStoreConfig, sa *streamAssignment, pedantic, recovering, restoring bool) (*stream, error) {
	s, jsa, err := a.checkForJetStream()
	if err != nil {
		return nil, err
	}

	// If we do not have the stream currently assigned to us in cluster mode we will proceed but warn.
	// This can happen on startup with restored state where on meta replay we still do not have
	// the assignment. Running in single server mode this always returns true.
	if !jsa.streamAssigned(config.Name) {
		s.Debugf("Stream '%s > %s' does not seem to be assigned to this server", a.Name, config.Name)
	}

	// Sensible defaults.
	ccfg, apiErr := s.checkStreamCfg(config, a, pedantic)
	if apiErr != nil {
		return nil, apiErr
	}
	cfg := &ccfg

	singleServerMode := !s.JetStreamIsClustered() && s.standAloneMode()
	if singleServerMode && cfg.Replicas > 1 {
		return nil, ApiErrors[JSStreamReplicasNotSupportedErr]
	}

	// Make sure we are ok when these are done in parallel.
	// We used to call Add(1) in the "else" clause of the "if loaded"
	// statement. This caused a data race because it was possible
	// that one go routine stores (with count==0) and another routine
	// gets "loaded==true" and calls wg.Wait() while the other routine
	// then calls wg.Add(1). It also could mean that two routines execute
	// the rest of the code concurrently.
	swg := &sync.WaitGroup{}
	swg.Add(1)
	v, loaded := jsa.inflight.LoadOrStore(cfg.Name, swg)
	wg := v.(*sync.WaitGroup)
	if loaded {
		wg.Wait()
		// This waitgroup is "thrown away" (since there was an existing one).
		swg.Done()
	} else {
		defer func() {
			jsa.inflight.Delete(cfg.Name)
			wg.Done()
		}()
	}

	// Note that isClustered will be false during recovery, even if we're part of a cluster. It shouldn't be used then.
	js, isClustered := jsa.jetStreamAndClustered()
	jsa.mu.Lock()
	if mset, ok := jsa.streams[cfg.Name]; ok {
		jsa.mu.Unlock()
		// A restore must create its own stream. Another restore or create may
		// have claimed the name since the restore's initial existence check.
		if restoring {
			return nil, NewJSStreamNameExistRestoreFailedError()
		}
		// Check to see if configs are same.
		ocfg := mset.config()

		// set the index name on cfg since it would not contain a value for iname while the return from mset.config() does to ensure the DeepEqual works
		for _, s := range cfg.Sources {
			s.setIndexName()
		}

		// Hold lock, because we'll be reading from and writing to a shared object.
		js.mu.Lock()
		copyStreamMetadata(cfg, &ocfg)
		deepEqual := reflect.DeepEqual(cfg, &ocfg)
		js.mu.Unlock()
		if deepEqual {
			if sa != nil {
				mset.setStreamAssignment(sa)
			}
			return mset, nil
		} else {
			return nil, ApiErrors[JSStreamNameExistErr]
		}
	}
	jsa.usageMu.RLock()
	selected, tier, hasTier := jsa.selectLimits(cfg.Replicas)
	jsa.usageMu.RUnlock()

	if !hasTier {
		jsa.mu.Unlock()
		return nil, NewJSNoLimitsError()
	}

	// Skip if we're recovering.
	if !recovering {
		reserved := int64(0)
		if !isClustered {
			reserved = jsa.tieredReservation(tier, cfg)
		}
		jsa.mu.Unlock()
		js.mu.RLock()
		if isClustered {
			_, reserved = js.tieredStreamAndReservationCount(a.Name, tier, cfg)
		}
		if err := js.checkAllLimits(&selected, tier, cfg, reserved, 0); err != nil {
			js.mu.RUnlock()
			return nil, err
		}
		js.mu.RUnlock()
		jsa.mu.Lock()
	}

	// If mirror, check if the transforms (if any) are valid.
	if cfg.Mirror != nil {
		if len(cfg.Mirror.SubjectTransforms) == 0 {
			if cfg.Mirror.FilterSubject != _EMPTY_ && !IsValidSubject(cfg.Mirror.FilterSubject) {
				jsa.mu.Unlock()
				return nil, fmt.Errorf("subject filter '%s' for the mirror %w", cfg.Mirror.FilterSubject, ErrBadSubject)
			}
		} else {
			for _, st := range cfg.Mirror.SubjectTransforms {
				if st.Source != _EMPTY_ && !IsValidSubject(st.Source) {
					jsa.mu.Unlock()
					return nil, fmt.Errorf("invalid subject transform source '%s' for the mirror: %w", st.Source, ErrBadSubject)
				}
				// check the transform, if any, is valid
				if st.Destination != _EMPTY_ {
					if _, err = NewSubjectTransform(st.Source, st.Destination); err != nil {
						jsa.mu.Unlock()
						return nil, fmt.Errorf("subject transform from '%s' to '%s' for the mirror: %w", st.Source, st.Destination, err)
					}
				}
			}
		}
	}

	// Setup our internal indexed names here for sources and check if the transforms (if any) are valid.
	for _, ssi := range cfg.Sources {
		if len(ssi.SubjectTransforms) == 0 {
			// check the filter, if any, is valid
			if ssi.FilterSubject != _EMPTY_ && !IsValidSubject(ssi.FilterSubject) {
				jsa.mu.Unlock()
				return nil, fmt.Errorf("subject filter '%s' for the source: %w", ssi.FilterSubject, ErrBadSubject)
			}
		} else {
			for _, st := range ssi.SubjectTransforms {
				if st.Source != _EMPTY_ && !IsValidSubject(st.Source) {
					jsa.mu.Unlock()
					return nil, fmt.Errorf("subject filter '%s' for the source: %w", st.Source, ErrBadSubject)
				}
				// check the transform, if any, is valid
				if st.Destination != _EMPTY_ {
					if _, err = NewSubjectTransform(st.Source, st.Destination); err != nil {
						jsa.mu.Unlock()
						return nil, fmt.Errorf("subject transform from '%s' to '%s' for the source: %w", st.Source, st.Destination, err)
					}
				}
			}
		}
	}

	// Check for overlapping subjects with other streams.
	// These are not allowed for now.
	if jsa.subjectsOverlap(cfg.Subjects, nil) {
		jsa.mu.Unlock()
		return nil, NewJSStreamSubjectOverlapError()
	}

	// Setup the internal clients.
	c := s.createInternalJetStreamClient()
	ic := s.createInternalJetStreamClient()

	// Work out the stream ingest limits.
	mlen := s.opts.StreamMaxBufferedMsgs
	msz := uint64(s.opts.StreamMaxBufferedSize)
	if mlen == 0 {
		mlen = streamDefaultMaxQueueMsgs
	}
	if msz == 0 {
		msz = streamDefaultMaxQueueBytes
	}

	qpfx := fmt.Sprintf("[ACC:%s] stream '%s' ", a.Name, config.Name)
	mset := &stream{
		acc:       a,
		jsa:       jsa,
		cfg:       *cfg,
		ddwin:     cfg.Duplicates,
		js:        js,
		srv:       s,
		client:    c,
		sysc:      ic,
		tier:      tier,
		stype:     cfg.Storage,
		consumers: make(map[string]*consumer),
		msgs: newIPQueue[*inMsg](s, qpfx+"messages",
			ipqSizeCalculation(func(msg *inMsg) uint64 {
				return uint64(len(msg.hdr) + len(msg.msg) + len(msg.rply) + len(msg.subj))
			}),
			ipqLimitByLen[*inMsg](mlen),
			ipqLimitBySize[*inMsg](msz),
		),
		gets:      newIPQueue[*directGetReq](s, qpfx+"direct gets"),
		qch:       make(chan struct{}),
		mqch:      make(chan struct{}),
		uch:       make(chan struct{}, 4),
		sch:       make(chan struct{}, 1),
		created:   time.Now().UTC(),
		restoring: restoring,
	}

	// Add created timestamp used for the store, must match that of the stream assignment if it exists.
	if sa != nil {
		// The following assignment does not require mutex
		// protection: sa.Created is immutable.
		mset.created = sa.Created
	}

	// Start our signaling routine to process consumers.
	mset.sigq = newIPQueue[*cMsg](s, qpfx+"obs") // of *cMsg
	go mset.signalConsumersLoop()

	// For no-ack consumers when we are interest retention.
	if cfg.Retention != LimitsPolicy {
		mset.ackq = newIPQueue[uint64](s, qpfx+"acks")
	}

	// Check for input subject transform
	if cfg.SubjectTransform != nil {
		tr, err := NewSubjectTransform(cfg.SubjectTransform.Source, cfg.SubjectTransform.Destination)
		if err != nil {
			jsa.mu.Unlock()
			return nil, fmt.Errorf("stream subject transform from '%s' to '%s': %w", cfg.SubjectTransform.Source, cfg.SubjectTransform.Destination, err)
		}
		mset.itr = tr
	}

	// Check for RePublish.
	if cfg.RePublish != nil {
		tr, err := NewSubjectTransform(cfg.RePublish.Source, cfg.RePublish.Destination)
		if err != nil {
			jsa.mu.Unlock()
			return nil, fmt.Errorf("stream republish transform from '%s' to '%s': %w", cfg.RePublish.Source, cfg.RePublish.Destination, err)
		}
		// Assign our transform for republishing.
		mset.tr = tr
	}
	storeDir := filepath.Join(jsa.storeDir, streamsDir, cfg.Name)
	jsa.mu.Unlock()

	// Bind to the user account.
	c.registerWithAccount(a)
	// Bind to the system account.
	ic.registerWithAccount(s.SystemAccount())

	// Create the appropriate storage
	fsCfg := fsConfig
	if fsCfg == nil {
		fsCfg = &FileStoreConfig{}
		// If we are file based and not explicitly configured
		// we may be able to auto-tune based on max msgs or bytes.
		if cfg.Storage == FileStorage {
			mset.autoTuneFileStorageBlockSize(fsCfg)
		}
	}
	fsCfg.StoreDir = storeDir
	// Grab configured sync interval.
	fsCfg.SyncInterval = s.getOpts().SyncInterval
	fsCfg.SyncAlways = s.getOpts().SyncAlways
	fsCfg.Compression = config.Compression
	// Async flushing is only allowed if the stream has a sync log backing it.
	fsCfg.AsyncFlush = !fsCfg.SyncAlways && config.Replicas > 1

	// Async persist mode opts in to async flushing,
	// sync always would also be disabled if it was configured.
	if config.PersistMode == AsyncPersistMode {
		fsCfg.SyncAlways = false
		fsCfg.AsyncFlush = true
	}

	// If the stream is backed by a Raft log, we can relax
	// SyncAlways so that we flush and sync whenever a
	// stream snapshot is created. We can safely recover
	// from the snapshot and the tail of the log.
	if fsCfg.SyncAlways && config.Replicas > 1 {
		fsCfg.SyncOnFlush = true
	}

	if err := mset.setupStore(fsCfg, recovering || restoring); err != nil {
		mset.stop(true, false)
		return nil, NewJSStreamStoreFailedError(err)
	}

	// Create our pubAck template here. Better than json marshal each time on success.
	if domain := s.getOpts().JetStreamDomain; domain != _EMPTY_ {
		mset.pubAck = fmt.Appendf(nil, "{%q:%q,%q:%q,%q:", "stream", cfg.Name, "domain", domain, "seq")
	} else {
		mset.pubAck = fmt.Appendf(nil, "{%q:%q,%q:", "stream", cfg.Name, "seq")
	}
	end := len(mset.pubAck)
	mset.pubAck = mset.pubAck[:end:end]

	// Set our known last sequence.
	var state StreamState
	mset.store.FastState(&state)

	// Possible race with consumer.setLeader during recovery.
	mset.mu.Lock()
	mset.lseq = state.LastSeq

	// Ensure dedupe state is loaded.
	mset.ddMu.Lock()
	mset.rebuildDedupe()
	mset.ddMu.Unlock()
	mset.mu.Unlock()

	// Set our stream assignment if in clustered mode.
	reserveResources := true
	if sa != nil {
		mset.setStreamAssignment(sa)

		// If the stream is resetting we must not double-account resources, they were already accounted for.
		js.mu.Lock()
		if sa.resetting {
			reserveResources, sa.resetting = false, false
		}
		js.mu.Unlock()
	}

	// Setup our internal send go routine.
	mset.setupSendCapabilities()

	// Reserve resources if MaxBytes present.
	if reserveResources {
		mset.js.reserveStreamResources(&mset.cfg)
	}

	// Call directly to set leader if not in clustered mode.
	// This can be called though before we actually setup clustering, so check both.
	if singleServerMode {
		if err := mset.setLeader(true, 0); err != nil {
			mset.stop(true, false)
			return nil, err
		}
	}

	// This is always true in single server mode.
	if mset.IsLeader() {
		// Send advisory.
		var suppress bool
		if !s.standAloneMode() && sa == nil {
			if cfg.Replicas > 1 {
				suppress = true
			}
		} else if sa != nil {
			suppress = sa.hasResponded()
		}
		if !suppress {
			mset.sendCreateAdvisory()
		}
	}

	// Register with our account last.
	jsa.mu.Lock()
	jsa.streams[cfg.Name] = mset
	jsa.mu.Unlock()
	if recovering && !restoring {
		mset.store.Ready()
	}

	return mset, nil
}

// Composes the index name. Contains the stream name, subject filter, and transform destination
// when the stream is external we will use the api prefix as part of the index name
// (as the same stream name could be used in multiple JS domains)
func (ssi *StreamSource) composeIName() string {
	var iName = ssi.Name

	if ssi.External != nil {
		iName = iName + ":" + getHash(ssi.External.ApiPrefix)
	}
	if ssi.Consumer != nil {
		iName = iName + ":C=" + getHash(ssi.Consumer.Name)
	}

	source := ssi.FilterSubject
	destination := fwcs

	if len(ssi.SubjectTransforms) == 0 {
		// normalize filter and destination in case they are empty
		if source == _EMPTY_ {
			source = fwcs
		}
		if destination == _EMPTY_ {
			destination = fwcs
		}
	} else {
		var sources, destinations []string

		for _, tr := range ssi.SubjectTransforms {
			trsrc, trdest := tr.Source, tr.Destination
			if trsrc == _EMPTY_ {
				trsrc = fwcs
			}
			if trdest == _EMPTY_ {
				trdest = fwcs
			}
			sources = append(sources, trsrc)
			destinations = append(destinations, trdest)
		}
		source = strings.Join(sources, "\f")
		destination = strings.Join(destinations, "\f")
	}

	return strings.Join([]string{iName, source, destination}, " ")
}

// Sets the index name.
func (ssi *StreamSource) setIndexName() {
	ssi.iname = ssi.composeIName()
}

// matchSourceIndexNames sets the index names on cfg's sources only if they are also set on ocfg's sources.
// The index name is not encoded, so this ensures a DeepEqual of both configs can succeed.
func matchSourceIndexNames(cfg *StreamConfig, ocfg *StreamConfig) {
	currentIName := make(map[string]struct{}, len(ocfg.Sources))
	for _, s := range ocfg.Sources {
		currentIName[s.iname] = struct{}{}
	}
	for _, s := range cfg.Sources {
		s.setIndexName()
		if _, ok := currentIName[s.iname]; !ok {
			s.iname = _EMPTY_
		}
	}
}

// Composes the consumer index name. Contains the stream name and consumer name used for durable sourcing (if any).
// When the stream is external we will use the api prefix as part of the index name
// (as the same stream and consumer names could be used in multiple JS domains)
func (ssi *StreamSource) composeCName() string {
	var iName = ssi.Name

	if ssi.External != nil {
		iName = iName + ":" + getHash(ssi.External.ApiPrefix)
	}
	var c string
	if ssi.Consumer != nil {
		c = ssi.Consumer.Name
	}

	return strings.Join([]string{iName, c}, " ")
}

func (mset *stream) streamAssignment() *streamAssignment {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.sa
}

func (mset *stream) setStreamAssignment(sa *streamAssignment) {
	var node RaftNode
	var peers []string

	mset.mu.RLock()
	js := mset.js
	mset.mu.RUnlock()

	if js != nil {
		js.mu.RLock()
		if sa.Group != nil {
			node = sa.Group.node
			peers = copyStrings(sa.Group.Peers)
		}
		js.mu.RUnlock()
	}

	mset.mu.Lock()
	defer mset.mu.Unlock()

	mset.sa = sa
	if sa == nil {
		return
	}

	// Set our node.
	mset.node = node

	// Stop catching up peers if they're no longer part of the group.
	for peer := range mset.catchups {
		if !slices.Contains(peers, peer) {
			mset.clearCatchupPeerLocked(peer)
		}
	}

	// Setup our info sub here as well for all stream members. This is now by design.
	if mset.infoSub == nil {
		isubj := fmt.Sprintf(clusterStreamInfoT, mset.jsa.acc(), mset.cfg.Name)
		// Note below the way we subscribe here is so that we can send requests to ourselves.
		mset.infoSub, _ = mset.srv.systemSubscribe(isubj, _EMPTY_, false, mset.sysc, mset.handleClusterStreamInfoRequest)
	}

	// Trigger update chan.
	mset.signalUpdate()
}

// signalUpdate wakes the monitor routine so it re-evaluates the stream assignment.
func (mset *stream) signalUpdate() {
	select {
	case mset.uch <- struct{}{}:
	default:
	}
}

// identity returns a hash of the stream's creation time, used to detect stream recreation.
func (mset *stream) identity() string {
	return getHash(mset.createdTime().UTC().Format(time.RFC3339Nano))
}

// identity returns a hash of the stream's creation time, used to detect stream recreation.
// JS lock should be held.
func (sa *streamAssignment) identity() string {
	return getHash(sa.Created.UTC().Format(time.RFC3339Nano))
}

// Returned by monitorQuitC once the monitor was signaled to quit, so a monitor
// that only gets there afterwards still exits immediately.
var closedMonitorQuitC = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

func (mset *stream) monitorQuitC() <-chan struct{} {
	if mset == nil {
		return nil
	}
	mset.mu.Lock()
	defer mset.mu.Unlock()
	// Don't recreate, a concurrent stopMonitoring would wait forever for a monitor
	// that can't be signaled anymore. startMonitorWg creates it per generation.
	if mset.mqch == nil {
		return closedMonitorQuitC
	}
	return mset.mqch
}

// signalMonitorQuit signals to exit the monitor loop. If there's no Raft node,
// this will be the only way to stop the monitor goroutine.
func (mset *stream) signalMonitorQuit() {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	if mset.mqch != nil {
		close(mset.mqch)
		mset.mqch = nil
	}
}

func (mset *stream) updateC() <-chan struct{} {
	if mset == nil {
		return nil
	}
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.uch
}

// IsLeader will return if we are the current leader.
func (mset *stream) IsLeader() bool {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.isLeader()
}

// Lock should be held.
func (mset *stream) isLeader() bool {
	if mset.isClustered() {
		return mset.node.Leader()
	}
	return true
}

// isLeaderNodeState should NOT be used normally, use isLeader instead.
// Returns whether the node thinks it is the leader, regardless of whether applies are up-to-date yet
// (unlike isLeader, which requires applies to be caught up).
// May be used to respond to clients after a leader change, when applying entries from a former leader.
// Lock should be held.
func (mset *stream) isLeaderNodeState() bool {
	if mset.isClustered() {
		return mset.node.State() == Leader
	}
	return true
}

// TODO(dlc) - Check to see if we can accept being the leader or we should step down.
func (mset *stream) setLeader(isLeader bool, term uint64) error {
	mset.mu.Lock()
	if mset.restoring {
		mset.restoreLeader, mset.restoreTerm = isLeader, term
		mset.mu.Unlock()
		return nil
	}
	wasLeader := mset.leader.Swap(isLeader)

	// We can skip the teardown if we were leader before and are still the leader now.
	// But only at term 1, since that means scale up from or down to an unreplicated config.
	// R1 assets have no raft node and use the coerced term 1.
	if term < 1 {
		term = 1
	}
	skipTeardown := wasLeader && isLeader && term == 1
	mset.term = term
	if !skipTeardown {
		// cancel timer to create the source consumers if not fired yet
		if mset.sourcesConsumerSetup != nil {
			mset.sourcesConsumerSetup.Stop()
			mset.sourcesConsumerSetup = nil
		} else {
			// Stop any source consumers
			mset.stopSourceConsumers()
		}

		// Stop responding to sync requests.
		mset.stopClusterSubs()
		// Unsubscribe from direct stream.
		mset.unsubscribeToStream(false, false)
		mset.store.ResetState()
	}

	// If we are here we have a change in leader status.
	if isLeader {
		// Make sure we are listening for sync requests.
		// TODO(dlc) - Original design was that all in sync members of the group would do DQ.
		if mset.isClustered() {
			mset.startClusterSubs()
		}

		// Setup subscriptions if we were not already the leader.
		if err := mset.subscribeToStream(); err != nil {
			if mset.isClustered() {
				// Stepdown since we have an error.
				mset.node.StepDown()
			}
			mset.mu.Unlock()
			return err
		}

		// Reset any inflight fast batches. We were likely a follower before and need
		// to send an ack to the publishers so they know we're still there.
		if !skipTeardown && mset.batches != nil {
			mset.batches.mu.Lock()
			for batchId, b := range mset.batches.fast {
				mset.batches.fastBatchReset(mset, batchId, b)
			}
			mset.batches.mu.Unlock()
		}
	}
	mset.mu.Unlock()

	// If we are interest based make sure to check consumers.
	// This is to make sure we process any outstanding acks.
	mset.checkInterestState()

	return nil
}

func (mset *stream) completeRestore() error {
	mset.mu.Lock()
	if !mset.restoring {
		mset.mu.Unlock()
		return nil
	}
	mset.restoring = false
	isLeader, term := mset.restoreLeader, mset.restoreTerm
	mset.restoreLeader, mset.restoreTerm = false, 0
	mset.mu.Unlock()
	if isLeader {
		if err := mset.setLeader(true, term); err != nil {
			return err
		}
	}
	mset.store.Ready()
	return nil
}

// Lock should be held.
func (mset *stream) startClusterSubs() {
	if mset.syncSub == nil {
		mset.syncSub, _ = mset.srv.systemSubscribe(mset.sa.Sync, _EMPTY_, false, mset.sysc, mset.handleClusterSyncRequest)
	}
}

// Lock should be held.
func (mset *stream) stopClusterSubs() {
	if mset.syncSub != nil {
		mset.srv.sysUnsubscribe(mset.syncSub)
		mset.syncSub = nil
	}
}

// account gets the account for this stream.
func (mset *stream) account() *Account {
	return mset.accountLocked(true)
}

func (mset *stream) accountLocked(needLock bool) *Account {
	if needLock {
		mset.mu.RLock()
	}
	jsa := mset.jsa
	if needLock {
		mset.mu.RUnlock()
	}
	if jsa == nil {
		return nil
	}
	return jsa.acc()
}

// Helper to determine the max msg size for this stream if file based.
func (mset *stream) maxMsgSize() uint64 {
	maxMsgSize := mset.cfg.MaxMsgSize
	if maxMsgSize <= 0 {
		// Pull from the account.
		if mset.jsa != nil {
			if acc := mset.jsa.acc(); acc != nil {
				acc.mu.RLock()
				maxMsgSize = acc.mpay
				acc.mu.RUnlock()
			}
		}
		// If all else fails use default.
		if maxMsgSize <= 0 {
			maxMsgSize = MAX_PAYLOAD_SIZE
		}
	}
	// Now determine an estimation for the subjects etc.
	maxSubject := -1
	for _, subj := range mset.cfg.Subjects {
		if subjectIsLiteral(subj) {
			if len(subj) > maxSubject {
				maxSubject = len(subj)
			}
		}
	}
	if maxSubject < 0 {
		const defaultMaxSubject = 256
		maxSubject = defaultMaxSubject
	}
	// filestore will add in estimates for record headers, etc.
	return fileStoreMsgSizeEstimate(maxSubject, int(maxMsgSize))
}

// If we are file based and the file storage config was not explicitly set
// we can autotune block sizes to better match. Our target will be to store 125%
// of the theoretical limit. We will round up to nearest 100 bytes as well.
func (mset *stream) autoTuneFileStorageBlockSize(fsCfg *FileStoreConfig) {
	var totalEstSize uint64

	// MaxBytes will take precedence for now.
	if mset.cfg.MaxBytes > 0 {
		totalEstSize = uint64(mset.cfg.MaxBytes)
	} else if mset.cfg.MaxMsgs > 0 {
		// Determine max message size to estimate.
		totalEstSize = mset.maxMsgSize() * uint64(mset.cfg.MaxMsgs)
	} else if mset.cfg.MaxMsgsPer > 0 {
		fsCfg.BlockSize = uint64(defaultKVBlockSize)
		return
	} else {
		// If nothing set will let underlying filestore determine blkSize.
		return
	}

	blkSize := (totalEstSize / 4) + 1 // (25% overhead)
	// Round up to nearest 100
	if m := blkSize % 100; m != 0 {
		blkSize += 100 - m
	}
	if blkSize <= FileStoreMinBlkSize {
		blkSize = FileStoreMinBlkSize
	} else if blkSize >= FileStoreMaxBlkSize {
		blkSize = FileStoreMaxBlkSize
	} else {
		blkSize = defaultMediumBlockSize
	}
	fsCfg.BlockSize = uint64(blkSize)
}

// rebuildDedupe will rebuild any dedupe structures needed after recovery of a stream.
// Will be called lazily to avoid penalizing startup times.
// TODO(dlc) - Might be good to know if this should be checked at all for streams with no
// headers and msgId in them. Would need signaling from the storage layer.
// mset.mu and mset.ddMu locks should be held.
func (mset *stream) rebuildDedupe() {
	var smv StoreMsg
	var state StreamState
	mset.store.FastState(&state)

	if state.LastSeq > 0 {
		if sm, err := mset.store.LoadMsg(state.LastSeq, &smv); err == nil {
			mset.lmsgId = getMsgId(sm.hdr)
		}
	}

	if mset.ddwin <= 0 {
		return
	}

	// We have some messages. Lookup starting sequence by duplicate time window.
	sseq := mset.store.GetSeqFromTime(time.Now().Add(-mset.ddwin))
	if sseq == 0 {
		return
	}

	for seq := sseq; seq <= state.LastSeq; seq++ {
		sm, err := mset.store.LoadMsg(seq, &smv)
		if err != nil {
			continue
		}
		var msgId string
		if len(sm.hdr) > 0 {
			if msgId = getMsgId(sm.hdr); msgId != _EMPTY_ {
				mset.storeMsgIdLocked(&ddentry{msgId, sm.seq, sm.ts})
			}
		}
	}
}

// Returns true if the underlying store should use the
// Raft log for replaying the tail of stream during
// recovery.
func (mset *stream) shouldReplayFromWAL() bool {
	if mset == nil || !mset.IsClustered() || mset.store.Type() != FileStorage {
		return false
	}
	fs, ok := mset.store.(*fileStore)
	if !ok {
		return false
	}
	return fs.syncOnFlush.Load()
}

// prepareForWALReplay truncates any stream filestore tail past the last
// sequence of the given snapshot, in preparation of replaying the Raft log.
func (mset *stream) prepareForWALReplay(snap *StreamReplicatedState) error {
	if mset == nil {
		return nil
	}

	mset.mu.Lock()
	defer mset.mu.Unlock()

	var snapSeq, clfs uint64
	if snap != nil {
		snapSeq = snap.LastSeq
		clfs = snap.Failed
	} else if mset.cfg.FirstSeq > 0 {
		snapSeq = mset.cfg.FirstSeq - 1
	}

	var state StreamState
	mset.store.FastState(&state)
	if state.LastSeq > snapSeq {
		mset.srv.Debugf("Truncate to snapshot sequence %d", snapSeq)
		if err := mset.store.Truncate(snapSeq); err != nil {
			return err
		}
		mset.store.FastState(&state)
	}

	mset.lseq = state.LastSeq
	mset.setCLFS(clfs)

	mset.ddMu.Lock()
	if mset.ddtmr != nil {
		mset.ddtmr.Stop()
		mset.ddtmr = nil
	}
	mset.ddmap = nil
	mset.ddarr = nil
	mset.ddindex = 0
	mset.lmsgId = _EMPTY_
	mset.rebuildDedupe()
	mset.ddMu.Unlock()
	return nil
}

// Lock should be held.
func (mset *stream) lastSeqAndCLFS() (uint64, uint64) {
	return mset.lseq, mset.getCLFS()
}

func (mset *stream) getCLFS() uint64 {
	if mset == nil {
		return 0
	}
	mset.clMu.Lock()
	defer mset.clMu.Unlock()
	return mset.clfs
}

func (mset *stream) setCLFS(clfs uint64) {
	mset.clMu.Lock()
	mset.clfs = clfs
	mset.clMu.Unlock()
}

func (mset *stream) lastSeq() uint64 {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.lseq
}

func (mset *stream) sendCreateAdvisory() {
	mset.mu.RLock()
	name := mset.cfg.Name
	outq := mset.outq
	srv := mset.srv
	mset.mu.RUnlock()

	if outq == nil {
		return
	}

	// finally send an event that this stream was created
	m := JSStreamActionAdvisory{
		TypedEvent: TypedEvent{
			Type: JSStreamActionAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream: name,
		Action: CreateEvent,
		Domain: srv.getOpts().JetStreamDomain,
	}

	j, err := json.Marshal(m)
	if err != nil {
		return
	}

	subj := JSAdvisoryStreamCreatedPre + "." + name
	outq.sendMsg(subj, j)
}

func (mset *stream) sendDeleteAdvisoryLocked() {
	if mset.outq == nil {
		return
	}

	m := JSStreamActionAdvisory{
		TypedEvent: TypedEvent{
			Type: JSStreamActionAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream: mset.cfg.Name,
		Action: DeleteEvent,
		Domain: mset.srv.getOpts().JetStreamDomain,
	}

	j, err := json.Marshal(m)
	if err == nil {
		subj := JSAdvisoryStreamDeletedPre + "." + mset.cfg.Name
		mset.outq.sendMsg(subj, j)
	}
}

func (mset *stream) sendUpdateAdvisoryLocked() {
	if mset.outq == nil {
		return
	}

	m := JSStreamActionAdvisory{
		TypedEvent: TypedEvent{
			Type: JSStreamActionAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream: mset.cfg.Name,
		Action: ModifyEvent,
		Domain: mset.srv.getOpts().JetStreamDomain,
	}

	j, err := json.Marshal(m)
	if err == nil {
		subj := JSAdvisoryStreamUpdatedPre + "." + mset.cfg.Name
		mset.outq.sendMsg(subj, j)
	}
}

func (mset *stream) sendStreamBatchAbandonedAdvisory(batchId string, reason BatchAbandonReason) {
	if mset == nil {
		return
	}
	s := mset.srv
	stream, acc := mset.name(), mset.account()
	subj := JSAdvisoryStreamBatchAbandonedPre + "." + stream
	adv := &JSStreamBatchAbandonedAdvisory{
		TypedEvent: TypedEvent{
			Type: JSStreamBatchAbandonedAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:  stream,
		Domain:  s.getOpts().JetStreamDomain,
		BatchId: batchId,
		Reason:  reason,
	}

	// Send to the user's account if not the system account.
	if acc != s.SystemAccount() {
		s.publishAdvisory(acc, subj, adv)
	}
	// Now do system level one. Place account info in adv, and nil account means system.
	adv.Account = acc.GetName()
	s.publishAdvisory(nil, subj, adv)
}

// Created returns created time.
func (mset *stream) createdTime() time.Time {
	mset.mu.RLock()
	created := mset.created
	mset.mu.RUnlock()
	return created
}

// Internal to allow creation time to be restored.
func (mset *stream) setCreatedTime(created time.Time) {
	mset.mu.Lock()
	mset.created = created
	store := mset.store
	if fs, ok := store.(*fileStore); ok {
		fs.setCreatedTime(created)
	}
	mset.mu.Unlock()
}

// subjectsOverlap to see if these subjects overlap with existing subjects.
// Use only for non-clustered JetStream
// RLock minimum should be held.
func (jsa *jsAccount) subjectsOverlap(subjects []string, self *stream) bool {
	for _, mset := range jsa.streams {
		if self != nil && mset == self {
			continue
		}
		// Read the other stream's subjects under its cfgMu.
		mset.cfgMu.RLock()
		msubjects := mset.cfg.Subjects
		mset.cfgMu.RUnlock()
		for _, subj := range msubjects {
			for _, tsubj := range subjects {
				if SubjectsCollide(tsubj, subj) {
					return true
				}
			}
		}
	}
	return false
}

// StreamDefaultDuplicatesWindow default duplicates window.
const StreamDefaultDuplicatesWindow = 2 * time.Minute

// Do not hold the jetStream lock, it will be read-locked internally.
func (s *Server) checkStreamCfg(config *StreamConfig, acc *Account, pedantic bool) (StreamConfig, *ApiError) {
	if js := s.getJetStream(); js != nil {
		js.mu.RLock()
		defer js.mu.RUnlock()
	}
	return s.checkStreamCfgLocked(config, acc, pedantic)
}

// jetStream lock (read or write) should be held, if JetStream is enabled.
func (s *Server) checkStreamCfgLocked(config *StreamConfig, acc *Account, pedantic bool) (StreamConfig, *ApiError) {
	lim := &s.getOpts().JetStreamLimits

	if config == nil {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration invalid"))
	}
	if !isValidAssetName(config.Name) {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("stream name is required and can not contain '.', '*', '>', '\\', '/'"))
	}
	if len(config.Name) > JSMaxNameLen {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("stream name is too long, maximum allowed is %d", JSMaxNameLen))
	}
	if len(config.Description) > JSMaxDescriptionLen {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("stream description is too long, maximum allowed is %d", JSMaxDescriptionLen))
	}

	var metadataLen int
	for k, v := range config.Metadata {
		metadataLen += len(k) + len(v)
	}
	if metadataLen > JSMaxMetadataLen {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("stream metadata exceeds maximum size of %d bytes", JSMaxMetadataLen))
	}

	cfg := *config

	if _, err := cfg.Retention.MarshalJSON(); err != nil {
		return cfg, NewJSStreamInvalidConfigError(fmt.Errorf("invalid retention"))
	}
	if _, err := cfg.Discard.MarshalJSON(); err != nil {
		return cfg, NewJSStreamInvalidConfigError(fmt.Errorf("invalid discard policy"))
	}
	if _, err := cfg.Compression.MarshalJSON(); err != nil {
		return cfg, NewJSStreamInvalidConfigError(fmt.Errorf("invalid compression"))
	}

	// Make file the default.
	if cfg.Storage == 0 {
		cfg.Storage = FileStorage
	}
	if _, err := cfg.Storage.MarshalJSON(); err != nil {
		return cfg, NewJSStreamInvalidConfigError(fmt.Errorf("invalid storage type"))
	}

	if cfg.Replicas == 0 {
		cfg.Replicas = 1
	}
	if cfg.Replicas > StreamMaxReplicas {
		return cfg, NewJSStreamInvalidConfigError(fmt.Errorf("maximum replicas is %d", StreamMaxReplicas))
	}
	if cfg.Replicas < 0 {
		return cfg, NewJSReplicasCountCannotBeNegativeError()
	}
	if cfg.MaxMsgs == 0 || cfg.MaxMsgs < -1 {
		if pedantic && cfg.MaxMsgs < -1 {
			return StreamConfig{}, NewJSPedanticError(fmt.Errorf("max_msgs must be set to -1"))
		}
		cfg.MaxMsgs = -1
	}
	if cfg.MaxMsgsPer == 0 || cfg.MaxMsgsPer < -1 {
		if pedantic && cfg.MaxMsgsPer < -1 {
			return StreamConfig{}, NewJSPedanticError(fmt.Errorf("max_msgs_per_subject must be set to -1"))
		}
		cfg.MaxMsgsPer = -1
	}
	if cfg.MaxBytes == 0 || cfg.MaxBytes < -1 {
		if pedantic && cfg.MaxBytes < -1 {
			return StreamConfig{}, NewJSPedanticError(fmt.Errorf("max_bytes must be set to -1"))
		}
		cfg.MaxBytes = -1
	}
	if cfg.MaxMsgSize == 0 || cfg.MaxMsgSize < -1 {
		if pedantic && cfg.MaxMsgSize < -1 {
			return StreamConfig{}, NewJSPedanticError(fmt.Errorf("max_msg_size must be set to -1"))
		}
		cfg.MaxMsgSize = -1
	}
	if cfg.MaxConsumers == 0 || cfg.MaxConsumers < -1 {
		if pedantic && cfg.MaxConsumers < -1 {
			return StreamConfig{}, NewJSPedanticError(fmt.Errorf("max_consumers must be set to -1"))
		}
		cfg.MaxConsumers = -1
	}
	if cfg.MaxAge < 0 {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("max age can not be negative"))
	}
	if cfg.MaxAge != 0 && cfg.MaxAge < 100*time.Millisecond {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("max age needs to be >= 100ms"))
	}

	if cfg.Duplicates == 0 && cfg.Mirror == nil && len(cfg.Sources) == 0 {
		maxWindow := StreamDefaultDuplicatesWindow
		if lim.Duplicates > 0 && maxWindow > lim.Duplicates {
			if pedantic {
				return StreamConfig{}, NewJSPedanticError(fmt.Errorf("duplicate window limits are higher than current limits"))
			}
			maxWindow = lim.Duplicates
		}
		if cfg.MaxAge != 0 && cfg.MaxAge < maxWindow {
			if pedantic {
				return StreamConfig{}, NewJSPedanticError(fmt.Errorf("duplicate window cannot be bigger than max age"))
			}
			cfg.Duplicates = cfg.MaxAge
		} else {
			cfg.Duplicates = maxWindow
		}
	}
	if cfg.Duplicates < 0 {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("duplicates window can not be negative"))
	}
	// Check that duplicates is not larger then age if set.
	if cfg.MaxAge != 0 && cfg.Duplicates > cfg.MaxAge {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("duplicates window can not be larger then max age"))
	}
	if lim.Duplicates > 0 && cfg.Duplicates > lim.Duplicates {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("duplicates window can not be larger then server limit of %v",
			lim.Duplicates.String()))
	}
	if cfg.Duplicates > 0 && cfg.Duplicates < 100*time.Millisecond {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("duplicates window needs to be >= 100ms"))
	}

	if cfg.DenyPurge && cfg.AllowRollup {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("roll-ups require the purge permission"))
	}

	// Counter is not compatible with some settings.
	if cfg.AllowMsgCounter {
		if cfg.Discard == DiscardNew {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("counter stream cannot use discard new"))
		}
		if cfg.AllowMsgTTL {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("counter stream cannot use message TTLs"))
		}
		if cfg.AllowMsgSchedules {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("counter stream cannot use message schedules"))
		}
		if cfg.Retention != LimitsPolicy {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("counter stream can only use limits retention"))
		}
	}

	// Check for new discard new per subject, we require the discard policy to also be new.
	if cfg.DiscardNewPer {
		if cfg.Discard != DiscardNew {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("discard new per subject requires discard new policy to be set"))
		}
		if cfg.MaxMsgsPer <= 0 {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("discard new per subject requires max msgs per subject > 0"))
		}
	}

	if cfg.SubjectDeleteMarkerTTL > 0 {
		if cfg.SubjectDeleteMarkerTTL < time.Second {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("subject delete marker TTL must be at least 1 second"))
		}
		if !cfg.AllowMsgTTL {
			if pedantic {
				return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("subject delete marker cannot be set if message TTLs are disabled"))
			}
			cfg.AllowMsgTTL = true
		}
		if !cfg.AllowRollup {
			if pedantic {
				return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("subject delete marker cannot be set if roll-ups are disabled"))
			}
			cfg.AllowRollup, cfg.DenyPurge = true, false
		}
	} else if cfg.SubjectDeleteMarkerTTL < 0 {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("subject delete marker TTL must not be negative"))
	}

	if cfg.AllowMsgSchedules {
		if cfg.Discard == DiscardNew {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("message scheduling cannot use discard new"))
		}
		if !cfg.AllowRollup {
			if pedantic {
				return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("message scheduling cannot be set if roll-ups are disabled"))
			}
			cfg.AllowRollup, cfg.DenyPurge = true, false
		}
	}

	if cfg.PersistMode == AsyncPersistMode {
		if cfg.Storage != FileStorage {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("async persist mode is only supported on file storage"))
		}
		if cfg.Replicas > 1 {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("async persist mode is not supported on replicated streams"))
		}
		if cfg.AllowAtomicPublish {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("async persist mode is not supported with atomic batch publish"))
		}
	}

	getStream := func(streamName string) (bool, StreamConfig) {
		var exists bool
		var cfg StreamConfig
		if s.JetStreamIsClustered() {
			if js, _ := s.getJetStreamCluster(); js != nil {
				if sa := js.streamAssignmentOrInflight(acc.Name, streamName); sa != nil {
					cfg = *sa.Config.clone()
					exists = true
				}
			}
		} else if mset, err := acc.lookupStream(streamName); err == nil {
			cfg = mset.cfg
			exists = true
		}
		return exists, cfg
	}

	hasStream := func(streamName string) (bool, int32, []string) {
		exists, cfg := getStream(streamName)
		return exists, cfg.MaxMsgSize, cfg.Subjects
	}

	var streamSubs []string
	var deliveryPrefixes []string
	var apiPrefixes []string

	// Do some pre-checking for mirror config to avoid cycles in clustered mode.
	if cfg.Mirror != nil {
		if cfg.FirstSeq > 0 {
			return StreamConfig{}, NewJSMirrorWithFirstSeqError()
		}
		if len(cfg.Subjects) > 0 {
			return StreamConfig{}, NewJSMirrorWithSubjectsError()
		}
		if len(cfg.Sources) > 0 {
			return StreamConfig{}, NewJSMirrorWithSourcesError()
		}
		if cfg.AllowMsgCounter {
			return StreamConfig{}, NewJSMirrorWithCountersError()
		}
		if cfg.AllowAtomicPublish {
			return StreamConfig{}, NewJSMirrorWithAtomicPublishError()
		}
		if cfg.AllowBatchPublish {
			return StreamConfig{}, NewJSMirrorWithBatchPublishError()
		}
		if cfg.AllowMsgSchedules {
			return StreamConfig{}, NewJSMirrorWithMsgSchedulesError()
		}
		if c := cfg.Mirror.Consumer; c != nil {
			if !isValidAssetName(c.Name) {
				return StreamConfig{}, NewJSMirrorDurableConsumerCfgInvalidError()
			}
			if !subjectIsLiteral(c.DeliverSubject) || !IsValidSubject(c.DeliverSubject) {
				return StreamConfig{}, NewJSMirrorDurableConsumerCfgInvalidError()
			}
			if cfg.Mirror.OptStartSeq != 0 || cfg.Mirror.OptStartTime != nil {
				return StreamConfig{}, NewJSMirrorDurableConsumerCfgInvalidError()
			}
			if cfg.Mirror.FilterSubject != _EMPTY_ {
				return StreamConfig{}, NewJSMirrorDurableConsumerCfgInvalidError()
			}
		}
		if cfg.Mirror.FilterSubject != _EMPTY_ && len(cfg.Mirror.SubjectTransforms) != 0 {
			return StreamConfig{}, NewJSMirrorMultipleFiltersNotAllowedError()
		}
		if cfg.SubjectDeleteMarkerTTL > 0 {
			// Delete markers cannot be configured on a mirror as it would result in new
			// tombstones which would use up sequence numbers, diverging from the origin
			// stream.
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("subject delete markers forbidden on mirrors"))
		}
		// Check subject filters overlap.
		for outer, tr := range cfg.Mirror.SubjectTransforms {
			if tr.Source != _EMPTY_ && !IsValidSubject(tr.Source) {
				return StreamConfig{}, NewJSMirrorInvalidSubjectFilterError(fmt.Errorf("%w %s", ErrBadSubject, tr.Source))
			}

			err := ValidateMapping(tr.Source, tr.Destination)
			if err != nil {
				return StreamConfig{}, NewJSMirrorInvalidTransformDestinationError(err)
			}

			for inner, innertr := range cfg.Mirror.SubjectTransforms {
				if inner != outer && SubjectsCollide(tr.Source, innertr.Source) {
					return StreamConfig{}, NewJSMirrorOverlappingSubjectFiltersError()
				}
			}
		}
		// Do not perform checks if External is provided, as it could lead to
		// checking against itself (if sourced stream name is the same on different JetStream)
		if cfg.Mirror.External == nil {
			if !isValidAssetName(cfg.Mirror.Name) {
				return StreamConfig{}, NewJSMirrorInvalidStreamNameError()
			}
			// We do not require other stream to exist anymore, but if we can see it check payloads.
			exists, maxMsgSize, subs := hasStream(cfg.Mirror.Name)
			if len(subs) > 0 {
				streamSubs = append(streamSubs, subs...)
			}
			if exists {
				if cfg.MaxMsgSize > 0 && maxMsgSize > 0 && cfg.MaxMsgSize < maxMsgSize {
					return StreamConfig{}, NewJSMirrorMaxMessageSizeTooBigError()
				}
			}
			// Determine if we are inheriting direct gets.
			if exists, ocfg := getStream(cfg.Mirror.Name); exists {
				if pedantic && cfg.MirrorDirect != ocfg.AllowDirect {
					return StreamConfig{}, NewJSPedanticError(fmt.Errorf("origin stream has direct get set, mirror has it disabled"))
				}
				cfg.MirrorDirect = ocfg.AllowDirect
			} else if js := s.getJetStream(); js != nil && js.isClustered() {
				// Could not find it here. If we are clustered we can look it up.
				if sa := js.streamAssignmentOrInflight(acc.Name, cfg.Mirror.Name); sa != nil {
					if pedantic && cfg.MirrorDirect != sa.Config.AllowDirect {
						return StreamConfig{}, NewJSPedanticError(fmt.Errorf("origin stream has direct get set, mirror has it disabled"))
					}
					cfg.MirrorDirect = sa.Config.AllowDirect
				}
			}
		} else {
			if cfg.Mirror.External.DeliverPrefix != _EMPTY_ {
				deliveryPrefixes = append(deliveryPrefixes, cfg.Mirror.External.DeliverPrefix)
			}

			if cfg.Mirror.External.ApiPrefix != _EMPTY_ {
				apiPrefixes = append(apiPrefixes, cfg.Mirror.External.ApiPrefix)
			}
		}
	}

	if len(cfg.Sources) > 0 {
		if cfg.AllowMsgSchedules {
			return StreamConfig{}, NewJSSourceWithMsgSchedulesError()
		}
	}

	// check sources for duplicates
	var iNames = make(map[string]struct{})
	var cNames = make(map[string]struct{})
	for _, src := range cfg.Sources {
		if src == nil || !isValidAssetName(src.Name) {
			return StreamConfig{}, NewJSSourceInvalidStreamNameError()
		}
		if _, ok := iNames[src.composeIName()]; !ok {
			iNames[src.composeIName()] = struct{}{}
		} else {
			return StreamConfig{}, NewJSSourceDuplicateDetectedError()
		}

		if src.FilterSubject != _EMPTY_ && len(src.SubjectTransforms) != 0 {
			return StreamConfig{}, NewJSSourceMultipleFiltersNotAllowedError()
		}

		for _, tr := range src.SubjectTransforms {
			if tr.Source != _EMPTY_ && !IsValidSubject(tr.Source) {
				return StreamConfig{}, NewJSSourceInvalidSubjectFilterError(fmt.Errorf("%w %s", ErrBadSubject, tr.Source))
			}
			err := ValidateMapping(tr.Source, tr.Destination)
			if err != nil {
				return StreamConfig{}, NewJSSourceInvalidTransformDestinationError(err)
			}
		}

		// Check subject filters overlap.
		for outer, tr := range src.SubjectTransforms {
			for inner, innertr := range src.SubjectTransforms {
				if inner != outer && subjectIsSubsetMatch(tr.Source, innertr.Source) {
					return StreamConfig{}, NewJSSourceOverlappingSubjectFiltersError()
				}
			}
		}

		if c := src.Consumer; c != nil {
			if !isValidAssetName(c.Name) {
				return StreamConfig{}, NewJSSourceDurableConsumerCfgInvalidError()
			}
			if !subjectIsLiteral(c.DeliverSubject) || !IsValidSubject(c.DeliverSubject) {
				return StreamConfig{}, NewJSSourceDurableConsumerCfgInvalidError()
			}
			if src.OptStartSeq != 0 || src.OptStartTime != nil {
				return StreamConfig{}, NewJSSourceDurableConsumerCfgInvalidError()
			}
			if src.FilterSubject != _EMPTY_ {
				return StreamConfig{}, NewJSSourceDurableConsumerCfgInvalidError()
			}
			// Reusing the same consumer for multiple sources of the same stream isn't allowed.
			if _, ok := cNames[src.composeCName()]; !ok {
				cNames[src.composeCName()] = struct{}{}
			} else {
				return StreamConfig{}, NewJSSourceDurableConsumerDuplicateDetectedError()
			}
		}

		// Do not perform checks if External is provided, as it could lead to
		// checking against itself (if sourced stream name is the same on different JetStream)
		if src.External == nil {
			exists, maxMsgSize, subs := hasStream(src.Name)
			if len(subs) > 0 {
				streamSubs = append(streamSubs, subs...)
			}
			if exists {
				if cfg.MaxMsgSize > 0 && maxMsgSize > 0 && cfg.MaxMsgSize < maxMsgSize {
					return StreamConfig{}, NewJSSourceMaxMessageSizeTooBigError()
				}
			}
			continue
		} else {
			if src.External.DeliverPrefix != _EMPTY_ {
				deliveryPrefixes = append(deliveryPrefixes, src.External.DeliverPrefix)
			}
			if src.External.ApiPrefix != _EMPTY_ {
				apiPrefixes = append(apiPrefixes, src.External.ApiPrefix)
			}
		}
	}

	// check prefix overlap with subjects
	for _, pfx := range deliveryPrefixes {
		if !IsValidPublishSubject(pfx) {
			return StreamConfig{}, NewJSStreamInvalidExternalDeliverySubjError(pfx)
		}
		for _, sub := range streamSubs {
			if SubjectsCollide(sub, fmt.Sprintf("%s.%s", pfx, sub)) {
				return StreamConfig{}, NewJSStreamExternalDelPrefixOverlapsError(pfx, sub)
			}
		}
	}
	// check if api prefixes overlap
	for _, apiPfx := range apiPrefixes {
		if !IsValidPublishSubject(apiPfx) {
			return StreamConfig{}, NewJSStreamInvalidConfigError(
				fmt.Errorf("stream external api prefix %q must be a valid subject without wildcards", apiPfx))
		}
		if SubjectsCollide(apiPfx, JSApiPrefix) {
			return StreamConfig{}, NewJSStreamExternalApiOverlapError(apiPfx, JSApiPrefix)
		}
	}

	// cycle check for source cycle
	toVisit := []*StreamConfig{&cfg}
	visited := make(map[string]struct{})
	overlaps := func(subjects []string, filter string) bool {
		if filter == _EMPTY_ {
			return true
		}
		for _, subject := range subjects {
			if SubjectsCollide(subject, filter) {
				return true
			}
		}
		return false
	}

	for len(toVisit) > 0 {
		cfg := toVisit[0]
		toVisit = toVisit[1:]
		visited[cfg.Name] = struct{}{}
		for _, src := range cfg.Sources {
			if src.External != nil {
				continue
			}
			// We can detect a cycle between streams, but let's double check that the
			// subjects actually form a cycle.
			if _, ok := visited[src.Name]; ok {
				if overlaps(cfg.Subjects, src.FilterSubject) {
					return StreamConfig{}, NewJSStreamInvalidConfigError(errors.New("detected cycle"))
				}
			} else if exists, cfg := getStream(src.Name); exists {
				toVisit = append(toVisit, &cfg)
			}
		}
		// Avoid cycles hiding behind mirrors
		if m := cfg.Mirror; m != nil {
			if m.External == nil {
				if _, ok := visited[m.Name]; ok {
					return StreamConfig{}, NewJSStreamInvalidConfigError(errors.New("detected cycle"))
				}
				if exists, cfg := getStream(m.Name); exists {
					toVisit = append(toVisit, &cfg)
				}
			}
		}
	}

	if len(cfg.Subjects) == 0 {
		if cfg.Mirror == nil && len(cfg.Sources) == 0 {
			cfg.Subjects = append(cfg.Subjects, cfg.Name)
		}
	} else {
		if cfg.Mirror != nil {
			return StreamConfig{}, NewJSMirrorWithSubjectsError()
		}

		// Check for literal duplication of subject interest in config
		// and no overlap with any JS or SYS API subject space.
		dset := make(map[string]struct{}, len(cfg.Subjects))
		for i, subj := range cfg.Subjects {
			// Make sure the subject is valid. Check this first.
			if !IsValidSubject(subj) {
				return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("invalid subject"))
			}
			if _, ok := dset[subj]; ok {
				return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("duplicate subjects detected"))
			}
			// Check for trying to capture everything.
			if subj == fwcs {
				if !cfg.NoAck {
					return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("capturing all subjects requires no-ack to be true"))
				}
				// Capturing everything also will require R1.
				if cfg.Replicas != 1 {
					return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("capturing all subjects requires replicas of 1"))
				}
			}
			// Also check to make sure we do not overlap with our $JS API subjects.
			if !cfg.NoAck {
				for _, namespace := range []string{"$JS.>", "$JSC.>", "$NRG.>"} {
					if SubjectsCollide(subj, namespace) {
						// We allow an exception for $JS.EVENT.> since these could have been created in the past.
						if !subjectIsSubsetMatch(subj, "$JS.EVENT.>") {
							return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("subjects that overlap with jetstream api require no-ack to be true"))
						}
					}
				}
				if SubjectsCollide(subj, "$SYS.>") {
					if !subjectIsSubsetMatch(subj, "$SYS.ACCOUNT.>") {
						return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("subjects that overlap with system api require no-ack to be true"))
					}
				}
			}
			// Now check if we have multiple subjects that we do not overlap ourselves
			// which would cause duplicate entries (assuming no MsgID).
			for _, tsubj := range cfg.Subjects[i+1:] {
				if SubjectsCollide(tsubj, subj) {
					return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("subject %q overlaps with %q", subj, tsubj))
				}
			}
			// Mark for duplicate check.
			dset[subj] = struct{}{}
		}
	}

	if len(cfg.Subjects) == 0 && len(cfg.Sources) == 0 && cfg.Mirror == nil {
		return StreamConfig{}, NewJSStreamInvalidConfigError(
			fmt.Errorf("stream needs at least one configured subject or be a source/mirror"))
	}

	// Check for MaxBytes required and it's limit
	if required, limit := acc.maxBytesLimits(&cfg); required && cfg.MaxBytes <= 0 {
		return StreamConfig{}, NewJSStreamMaxBytesRequiredError()
	} else if limit > 0 && cfg.MaxBytes > limit {
		return StreamConfig{}, NewJSStreamMaxStreamBytesExceededError()
	}

	// Check the subject transform if any
	if cfg.SubjectTransform != nil {
		if cfg.SubjectTransform.Source != _EMPTY_ && !IsValidSubject(cfg.SubjectTransform.Source) {
			return StreamConfig{}, NewJSStreamTransformInvalidSourceError(fmt.Errorf("%w %s", ErrBadSubject, cfg.SubjectTransform.Source))
		}

		err := ValidateMapping(cfg.SubjectTransform.Source, cfg.SubjectTransform.Destination)
		if err != nil {
			return StreamConfig{}, NewJSStreamTransformInvalidDestinationError(err)
		}
	}

	// If we have a republish directive check if we can create a transform here.
	if cfg.RePublish != nil {
		// Check to make sure source is a valid subset of the subjects we have.
		// Also make sure it does not form a cycle.
		// Empty same as all.
		if cfg.RePublish.Source == _EMPTY_ {
			if pedantic {
				return StreamConfig{}, NewJSPedanticError(fmt.Errorf("republish source can not be empty"))
			}
			cfg.RePublish.Source = fwcs
		}
		// A RePublish from '>' to '>' could be used, normally this would form a cycle with the stream subjects.
		// But if this aligns to a different subject based on the transform, we allow it still.
		// The RePublish will be implicit based on the transform, but only if the transform's source
		// is the only stream subject.
		if cfg.RePublish.Destination == fwcs && cfg.RePublish.Source == fwcs && cfg.SubjectTransform != nil &&
			len(cfg.Subjects) == 1 && cfg.SubjectTransform.Source == cfg.Subjects[0] {
			if pedantic {
				return StreamConfig{}, NewJSPedanticError(fmt.Errorf("implicit republish based on subject transform"))
			}
			// RePublish all messages with the transformed subject.
			cfg.RePublish.Source, cfg.RePublish.Destination = cfg.SubjectTransform.Destination, cfg.SubjectTransform.Destination
		}
		var formsCycle bool
		for _, subj := range cfg.Subjects {
			if SubjectsCollide(cfg.RePublish.Destination, subj) {
				formsCycle = true
				break
			}
		}
		if formsCycle {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration for republish destination forms a cycle"))
		}
		if _, err := NewSubjectTransform(cfg.RePublish.Source, cfg.RePublish.Destination); err != nil {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration for republish with transform from '%s' to '%s' not valid", cfg.RePublish.Source, cfg.RePublish.Destination))
		}
	}

	// Remove placement if it's an empty object.
	if cfg.Placement != nil && reflect.DeepEqual(cfg.Placement, &Placement{}) {
		cfg.Placement = nil
	}
	// For now don't allow preferred server in placement.
	if cfg.Placement != nil && cfg.Placement.Preferred != _EMPTY_ {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("preferred server not permitted in placement"))
	}

	return cfg, nil
}

// Config returns the stream's configuration.
func (mset *stream) config() StreamConfig {
	mset.cfgMu.RLock()
	defer mset.cfgMu.RUnlock()
	return mset.cfg
}

func (mset *stream) fileStoreConfig() (FileStoreConfig, error) {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	fs, ok := mset.store.(*fileStore)
	if !ok {
		return FileStoreConfig{}, ErrStoreWrongType
	}
	return fs.fileStoreConfig(), nil
}

// Do not hold jsAccount or jetStream lock, the latter will be read-locked internally.
func (jsa *jsAccount) configUpdateCheck(old, new *StreamConfig, s *Server, pedantic bool) (*StreamConfig, error) {
	js, _ := jsa.jetStreamAndClustered()
	js.mu.RLock()
	defer js.mu.RUnlock()
	return jsa.configUpdateCheckLocked(old, new, s, pedantic)
}

// Do not hold jsAccount lock. The jetStream lock (read or write) should be held.
func (jsa *jsAccount) configUpdateCheckLocked(old, new *StreamConfig, s *Server, pedantic bool) (*StreamConfig, error) {
	cfg, apiErr := s.checkStreamCfgLocked(new, jsa.acc(), pedantic)
	if apiErr != nil {
		return nil, apiErr
	}

	// Name must match.
	if cfg.Name != old.Name {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration name must match original"))
	}
	singleServerMode := !s.JetStreamIsClustered() && s.standAloneMode()
	if singleServerMode && cfg.Replicas > 1 {
		return nil, ApiErrors[JSStreamReplicasNotSupportedErr]
	}
	// Can't change storage types.
	if cfg.Storage != old.Storage {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not change storage type"))
	}
	// Can only change retention from limits to interest or back, not to/from work queue for now.
	if cfg.Retention != old.Retention {
		if old.Retention == WorkQueuePolicy || cfg.Retention == WorkQueuePolicy {
			return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not change retention policy to/from workqueue"))
		}
	}
	// Can not change from true to false.
	if !cfg.Sealed && old.Sealed {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not unseal a sealed stream"))
	}
	// Can not change from true to false.
	if !cfg.DenyDelete && old.DenyDelete {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not cancel deny message deletes"))
	}
	// Can not change from true to false.
	if !cfg.DenyPurge && old.DenyPurge {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not cancel deny purge"))
	}
	// Check for mirror changes which are not allowed.
	// We will allow removing the mirror config to "promote" the mirror to a normal stream.
	if cfg.Mirror != nil && !reflect.DeepEqual(cfg.Mirror, old.Mirror) {
		return nil, NewJSStreamMirrorNotUpdatableError()
	}

	// Check on new discard new per subject.
	if cfg.DiscardNewPer {
		if cfg.Discard != DiscardNew {
			return nil, NewJSStreamInvalidConfigError(fmt.Errorf("discard new per subject requires discard new policy to be set"))
		}
		if cfg.MaxMsgsPer <= 0 {
			return nil, NewJSStreamInvalidConfigError(fmt.Errorf("discard new per subject requires max msgs per subject > 0"))
		}
	}

	// Check on the allowed message TTL status.
	if old.AllowMsgTTL && !cfg.AllowMsgTTL {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("message TTL status can not be disabled"))
	}

	// Can't change counter setting.
	if cfg.AllowMsgCounter != old.AllowMsgCounter {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not change message counter setting"))
	}

	// Can't disable message schedules setting.
	if old.AllowMsgSchedules && !cfg.AllowMsgSchedules {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("message schedules can not be disabled"))
	}

	if old.PersistMode != cfg.PersistMode {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not change persist mode"))
	}

	// Do some adjustments for being sealed.
	// Pedantic mode will allow those changes to be made, as they are deterministic and important to get a sealed stream.
	if cfg.Sealed {
		cfg.MaxAge = 0
		cfg.Discard = DiscardNew
		cfg.DenyDelete, cfg.DenyPurge = true, true
		cfg.AllowRollup = false
	}

	// Check limits. We need some extra handling to allow updating MaxBytes.

	// First, let's calculate the difference between the new and old MaxBytes.
	maxBytesDiff := max(cfg.MaxBytes, 0) - max(old.MaxBytes, 0)
	if maxBytesDiff < 0 {
		// If we're updating to a lower MaxBytes (maxBytesDiff is negative),
		// then set to zero so checkBytesLimits doesn't set addBytes to 1.
		maxBytesDiff = 0
	}
	// If maxBytesDiff == 0, then that means MaxBytes didn't change.
	// If maxBytesDiff > 0, then we want to reserve additional bytes.

	// Save the user configured MaxBytes.
	newMaxBytes := cfg.MaxBytes

	// We temporarily set cfg.MaxBytes to maxBytesDiff because checkAllLimits
	// adds cfg.MaxBytes to the current reserved limit and checks if we've gone
	// over. However, we don't want an additional cfg.MaxBytes, we only want to
	// reserve the difference between the new and the old values.
	cfg.MaxBytes = maxBytesDiff

	// Check limits.
	js, isClustered := jsa.jetStreamAndClustered()
	jsa.mu.RLock()
	acc := jsa.account
	jsa.usageMu.RLock()
	selected, tier, hasTier := jsa.selectLimits(cfg.Replicas)
	if !hasTier && old.Replicas != cfg.Replicas {
		selected, tier, hasTier = jsa.selectLimits(old.Replicas)
	}
	jsa.usageMu.RUnlock()
	reserved := int64(0)
	if !isClustered {
		reserved = jsa.tieredReservation(tier, &cfg)
	}
	jsa.mu.RUnlock()
	if !hasTier {
		return nil, NewJSNoLimitsError()
	}
	if isClustered {
		_, reserved = js.tieredStreamAndReservationCount(acc.Name, tier, &cfg)
	}
	// reserved covers only the other streams. checkAllLimits adds this stream's
	// footprint via cfg.MaxBytes, which is currently maxBytesDiff, so it only
	// adds the diff. Add the remaining (newMaxBytes - maxBytesDiff) here so the
	// two together equal this stream's true new footprint, even when Replicas
	// changes on update.
	reserved = addSaturate(reserved, accountReservation(tier, cfg.Replicas, newMaxBytes-maxBytesDiff))
	if err := js.checkAllLimits(&selected, tier, &cfg, reserved, 0); err != nil {
		return nil, err
	}
	// Restore the user configured MaxBytes.
	cfg.MaxBytes = newMaxBytes
	return &cfg, nil
}

// Update will allow certain configuration properties of an existing stream to be updated.
func (mset *stream) update(config *StreamConfig) error {
	return mset.updateWithAdvisory(config, true, false)
}

func (mset *stream) updatePedantic(config *StreamConfig, pedantic bool) error {
	return mset.updateWithAdvisory(config, true, pedantic)
}

// Update will allow certain configuration properties of an existing stream to be updated.
func (mset *stream) updateWithAdvisory(config *StreamConfig, sendAdvisory bool, pedantic bool) error {
	_, jsa, err := mset.acc.checkForJetStream()
	if err != nil {
		return err
	}

	mset.mu.RLock()
	ocfg := mset.cfg
	s := mset.srv
	mset.mu.RUnlock()

	cfg, err := mset.jsa.configUpdateCheck(&ocfg, config, s, pedantic)
	if err != nil {
		return NewJSStreamInvalidConfigError(err, Unless(err))
	}

	// In the event that some of the stream-level limits have changed, yell appropriately
	// if any of the consumers exceed that limit.
	oldInactiveThreshold, newInactiveThreshold := ocfg.ConsumerLimits.InactiveThreshold, cfg.ConsumerLimits.InactiveThreshold
	oldMaxAckPending, newMaxAckPending := ocfg.ConsumerLimits.MaxAckPending, cfg.ConsumerLimits.MaxAckPending
	updateLimits := (newInactiveThreshold > 0 && oldInactiveThreshold != newInactiveThreshold) ||
		(newMaxAckPending > 0 && oldMaxAckPending != newMaxAckPending)

	// Only check if not clustered. The meta leader performs clustered checks.
	if updateLimits && !mset.js.isClustered() {
		var errorConsumers []string
		mset.mu.RLock()
		clist := make([]*consumer, 0, len(mset.consumers))
		for _, c := range mset.consumers {
			clist = append(clist, c)
		}
		mset.mu.RUnlock()
		for _, c := range clist {
			c.mu.RLock()
			name, ccfg := c.name, c.cfg
			if (newInactiveThreshold > 0 && ccfg.InactiveThreshold > newInactiveThreshold) ||
				(newMaxAckPending > 0 && ccfg.MaxAckPending > newMaxAckPending) {
				errorConsumers = append(errorConsumers, name)
			}
			c.mu.RUnlock()
		}
		if len(errorConsumers) > 0 {
			// TODO(nat): Return a parsable error so that we can surface something
			// sensible through the JS API.
			return fmt.Errorf("change to limits violates consumers: %s", strings.Join(errorConsumers, ", "))
		}
	}

	jsa.mu.RLock()
	if jsa.subjectsOverlap(cfg.Subjects, mset) {
		jsa.mu.RUnlock()
		return NewJSStreamSubjectOverlapError()
	}
	jsa.mu.RUnlock()

	mset.mu.Lock()

	var needsStartingSeqNum map[string]struct{}
	if mset.active {
		// Check for mirror promotion.
		if ocfg.Mirror != nil && cfg.Mirror == nil {
			// Only try deleting the sourcing consumer if one wasn't provided to us.
			if ocfg.Mirror.Consumer == nil {
				mset.tryDeleteMirrorConsumer(ocfg.Mirror)
			}
			mset.cancelMirrorConsumer()
			mset.mirror = nil
		}

		// Now check for subject interest differences.
		current := make(map[string]struct{}, len(ocfg.Subjects))
		for _, s := range ocfg.Subjects {
			current[s] = struct{}{}
		}

		// Now walk new subjects. All of these need to be added, but we will check
		// the originals first, since if it is in there we can skip, already added.
		for _, s := range cfg.Subjects {
			if _, ok := current[s]; !ok {
				if _, err := mset.subscribeInternal(s, mset.processInboundJetStreamMsg); err != nil {
					mset.mu.Unlock()
					return err
				}
			}
			delete(current, s)
		}
		// What is left in current needs to be deleted.
		for s := range current {
			if err := mset.unsubscribeInternal(s); err != nil {
				mset.mu.Unlock()
				return err
			}
		}

		// Check for the Duplicates
		mset.ddMu.Lock()
		if cfg.Duplicates != ocfg.Duplicates && mset.ddtmr != nil {
			// Let it fire right away, it will adjust properly on purge.
			mset.ddtmr.Reset(time.Microsecond)
		}
		mset.ddMu.Unlock()

		// Check for Sources.
		if len(cfg.Sources) > 0 || len(ocfg.Sources) > 0 {
			currentIName := make(map[string]struct{})
			currentConsumers := make(map[string]*StreamSource)

			getSourcingConsumerIName := func(ssi *StreamSource, sources []*StreamSource) string {
				var iName = ssi.Name
				if ssi.External != nil {
					iName = iName + ":" + getHash(ssi.External.ApiPrefix)
				}
				return fmt.Sprintf("%s %s", iName, mset.createSourcingConsumerHash(ssi, sources))
			}
			for _, s := range ocfg.Sources {
				currentIName[s.iname] = struct{}{}
				// Only track the sourcing consumer for deletion if one wasn't provided to us.
				if s.Consumer == nil {
					currentConsumers[getSourcingConsumerIName(s, ocfg.Sources)] = s
				}
			}
			for _, s := range cfg.Sources {
				s.setIndexName()
				if _, ok := currentIName[s.iname]; !ok {
					// new source
					if mset.sources == nil {
						mset.sources = make(map[string]*sourceInfo)
					}
					mset.cfgMu.Lock()
					mset.cfg.Sources = append(mset.cfg.Sources, s)
					mset.cfgMu.Unlock()

					var si *sourceInfo

					if len(s.SubjectTransforms) == 0 {
						si = &sourceInfo{name: s.Name, iname: s.iname, sf: s.FilterSubject}
					} else {
						si = &sourceInfo{name: s.Name, iname: s.iname}
						si.trs = make([]*subjectTransform, len(s.SubjectTransforms))
						si.sfs = make([]string, len(s.SubjectTransforms))
						for i := range s.SubjectTransforms {
							// err can be ignored as already validated in config check
							si.sfs[i] = s.SubjectTransforms[i].Source
							var err error
							si.trs[i], err = NewSubjectTransform(s.SubjectTransforms[i].Source, s.SubjectTransforms[i].Destination)
							if err != nil {
								mset.mu.Unlock()
								return fmt.Errorf("unable to get subject transform for source: %v", err)
							}
						}
					}

					mset.sources[s.iname] = si
					if needsStartingSeqNum == nil {
						needsStartingSeqNum = make(map[string]struct{})
					}
					needsStartingSeqNum[s.iname] = struct{}{}
				} else {
					// source already exists
					delete(currentIName, s.iname)
				}

				// Remove the source if it still exists, but only if not using a pre-existing consumer.
				if s.Consumer == nil {
					delete(currentConsumers, getSourcingConsumerIName(s, cfg.Sources))
				}
			}
			// Delete source consumers if any aren't used anymore.
			for _, s := range currentConsumers {
				id := mset.createSourcingConsumerHash(s, ocfg.Sources)
				mset.tryDeleteSourceConsumer(id, s)
			}
			// What is left in currentIName needs to be deleted.
			for iName := range currentIName {
				mset.cancelSourceConsumer(iName)
				delete(mset.sources, iName)
			}
		}
	}

	// Check for a change in allow direct/mirror status.
	// These will run on all members, so just update as appropriate here.
	// We do make sure we are caught up under monitorStream() during initial startup.
	if cfg.AllowDirect != ocfg.AllowDirect {
		if cfg.AllowDirect {
			mset.subscribeToDirect()
		} else {
			mset.unsubscribeToDirect()
		}
	}
	if cfg.MirrorDirect != ocfg.MirrorDirect {
		if cfg.MirrorDirect {
			mset.subscribeToMirrorDirect()
		} else {
			mset.unsubscribeToMirrorDirect()
		}
	}

	// Check for changes to RePublish.
	if cfg.RePublish != nil {
		// Empty same as all.
		if cfg.RePublish.Source == _EMPTY_ {
			cfg.RePublish.Source = fwcs
		}
		if cfg.RePublish.Destination == _EMPTY_ {
			cfg.RePublish.Destination = fwcs
		}
		tr, err := NewSubjectTransform(cfg.RePublish.Source, cfg.RePublish.Destination)
		if err != nil {
			mset.mu.Unlock()
			return fmt.Errorf("stream configuration for republish from '%s' to '%s': %w", cfg.RePublish.Source, cfg.RePublish.Destination, err)
		}
		// Assign our transform for republishing.
		mset.tr = tr
	} else {
		mset.tr = nil
	}

	// Check for changes to subject transform
	if ocfg.SubjectTransform == nil && cfg.SubjectTransform != nil {
		tr, err := NewSubjectTransform(cfg.SubjectTransform.Source, cfg.SubjectTransform.Destination)
		if err != nil {
			mset.mu.Unlock()
			return fmt.Errorf("stream configuration for subject transform from '%s' to '%s': %w", cfg.SubjectTransform.Source, cfg.SubjectTransform.Destination, err)
		}
		mset.itr = tr
	} else if ocfg.SubjectTransform != nil && cfg.SubjectTransform != nil &&
		(ocfg.SubjectTransform.Source != cfg.SubjectTransform.Source || ocfg.SubjectTransform.Destination != cfg.SubjectTransform.Destination) {
		tr, err := NewSubjectTransform(cfg.SubjectTransform.Source, cfg.SubjectTransform.Destination)
		if err != nil {
			mset.mu.Unlock()
			return fmt.Errorf("stream configuration for subject transform from '%s' to '%s': %w", cfg.SubjectTransform.Source, cfg.SubjectTransform.Destination, err)
		}
		mset.itr = tr
	} else if ocfg.SubjectTransform != nil && cfg.SubjectTransform == nil {
		mset.itr = nil
	}

	js := mset.js

	if targetTier := tierName(cfg.Replicas); mset.tier != targetTier {
		// In cases such as R1->R3, only one update is needed
		jsa.usageMu.RLock()
		_, ok := jsa.limits[targetTier]
		jsa.usageMu.RUnlock()
		if ok {
			// error never set
			_, reported, _ := mset.store.Utilization()
			jsa.updateUsage(mset.tier, mset.stype, -int64(reported))
			jsa.updateUsage(targetTier, mset.stype, int64(reported))
			mset.tier = targetTier
		}
		// else in case the new tier does not exist (say on move), keep the old tier around
		// a subsequent update to an existing tier will then move from existing past tier to existing new tier
	}

	// If atomic publish is disabled, delete any in-progress batches.
	if !cfg.AllowAtomicPublish {
		mset.deleteAtomicBatches(false)
	}
	// If fast batch publish is disabled, delete any in-progress batches.
	if !cfg.AllowBatchPublish {
		mset.deleteFastBatches()
	}
	if !cfg.AllowAtomicPublish && !cfg.AllowBatchPublish {
		mset.batches = nil
	}

	// Now update config and store's version of our config.
	// Although we are under the stream write lock, we will also assign the new
	// configuration under mset.cfgMu lock. This is so that in places where
	// mset.mu cannot be acquired (like many cases in consumer.go where code
	// is under the consumer's lock), and the stream's configuration needs to
	// be inspected, one can use mset.cfgMu's read lock to do that safely.
	mset.cfgMu.Lock()
	mset.cfg = *cfg
	mset.cfgMu.Unlock()

	// All replicas track msg ids, so update the dedupe window everywhere.
	mset.ddMu.Lock()
	mset.ddwin = cfg.Duplicates
	mset.ddMu.Unlock()

	// Rate limited consumers use the max msg size as their burst.
	if cfg.MaxMsgSize != ocfg.MaxMsgSize {
		for _, o := range mset.consumers {
			o.mu.Lock()
			o.setRateLimit(o.cfg.RateLimit)
			o.mu.Unlock()
		}
	}

	// If we're changing retention, whip through and update the consumer retention.
	if ocfg.Retention != cfg.Retention {
		toUpdate := make([]*consumer, 0, len(mset.consumers))
		for _, c := range mset.consumers {
			toUpdate = append(toUpdate, c)
		}
		var ss StreamState
		mset.store.FastState(&ss)
		// Switching to Interest without any consumers drops all messages.
		if cfg.Retention == InterestPolicy && len(toUpdate) == 0 && ss.Msgs > 0 {
			mset.store.Compact(ss.LastSeq + 1)
		}
		mset.mu.Unlock()
		for _, c := range toUpdate {
			c.mu.Lock()
			c.retention = cfg.Retention
			c.mu.Unlock()
			if c.retention == InterestPolicy {
				// If we're switching to interest, force a check of the
				// interest of existing stream messages.
				c.checkStateForInterestStream(&ss)
			}
		}
		mset.mu.Lock()
	}

	// If we are the leader never suppress update advisory, simply send.
	if mset.isLeader() && sendAdvisory {
		mset.sendUpdateAdvisoryLocked()
	}
	active := mset.active
	mset.mu.Unlock()

	if js != nil {
		maxBytesDiff := max(cfg.MaxBytes, 0) - max(ocfg.MaxBytes, 0)
		if maxBytesDiff > 0 {
			// Reserve the difference
			js.reserveStreamResources(&StreamConfig{
				MaxBytes: maxBytesDiff,
				Storage:  cfg.Storage,
			})
		} else if maxBytesDiff < 0 {
			// Release the difference
			js.releaseStreamResources(&StreamConfig{
				MaxBytes: -maxBytesDiff,
				Storage:  ocfg.Storage,
			})
		}
	}

	if err = mset.store.UpdateConfig(cfg); err != nil {
		return err
	}

	// Newly added sources can now setup their source consumers, since the store scanned
	// if the source was pre-existing as part of the config update above.
	if active && len(needsStartingSeqNum) > 0 {
		mset.mu.Lock()
		if mset.active {
			mset.setStartingSequenceForSources(needsStartingSeqNum)
			for iName := range needsStartingSeqNum {
				// A concurrent update could have removed or replaced the source
				// in the meantime, only set up the ones still present.
				if si, ok := mset.sources[iName]; ok {
					mset.setupSourceConsumer(iName, si.sseq+1, time.Time{})
				}
			}
		}
		mset.mu.Unlock()
	}

	return nil
}

// tryDeleteMirrorConsumer is a best-effort single try to delete a consumer used for stream mirroring.
// Lock should be held.
func (mset *stream) tryDeleteMirrorConsumer(mirror *StreamSource) {
	id := mset.createStableConsumerHash()
	consumerName := fmt.Sprintf("JS_MIRROR_%s", id)
	log := mset.mirror != nil && mset.mirror.cname == consumerName
	mset.tryDeleteSourcingConsumer("mirror", mirror, consumerName, log)
}

// tryDeleteSourceConsumer is a best-effort single try to delete a consumer used for stream sourcing.
// Lock should be held.
func (mset *stream) tryDeleteSourceConsumer(id string, source *StreamSource) {
	consumerName := fmt.Sprintf("JS_SRC_%s", id)
	si := mset.sources[source.iname]
	log := si != nil && si.cname == consumerName
	mset.tryDeleteSourcingConsumer("source", source, consumerName, log)
}

// tryDeleteSourcingConsumer is a best-effort single try to delete a sourcing consumer.
// Lock should be held.
func (mset *stream) tryDeleteSourcingConsumer(kind string, source *StreamSource, consumerName string, log bool) {
	acc := mset.acc
	accName, streamName, sourceName := acc.Name, mset.cfg.Name, source.Name
	subject := fmt.Sprintf(JSApiConsumerDeleteT, sourceName, consumerName)
	if source.External != nil {
		subject = strings.Replace(subject, JSApiPrefix, source.External.ApiPrefix, 1)
		subject = strings.ReplaceAll(subject, "..", ".")
	}
	s := mset.srv
	go func() {
		warn := func(err error) {
			if log {
				s.Warnf("Cleanup of %s consumer '%s > %s' failed for stream '%s > %s': %v", kind, sourceName, consumerName, accName, streamName, err)
			}
		}

		respCh := make(chan *JSApiConsumerDeleteResponse, 1)
		reply := s.infoReplySubject(source.External.Domain(), acc.Name, sourceName, consumerName)
		cdSub, err := acc.subscribeInternal(reply, func(sub *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
			_, msg := c.msgParts(rmsg)

			var cdr JSApiConsumerDeleteResponse
			if err := json.Unmarshal(msg, &cdr); err != nil {
				warn(err)
				return
			}
			select {
			case respCh <- &cdr:
			default:
			}
		})
		if err != nil {
			warn(err)
			return
		}
		defer acc.unsubscribeInternal(cdSub)

		// Send the delete request.
		err = s.sendInternalAccountMsgWithReply(acc, subject, reply, nil, nil, false)
		if err != nil {
			warn(err)
			return
		}
		select {
		case cdr := <-respCh:
			if cdr.Error != nil {
				warn(cdr.Error)
			}
		case <-time.After(sourceHealthCheckInterval):
			warn(errors.New("timed out"))
		}
	}()
}

// Small helper to return the Name field from mset.cfg, protected by
// the mset.cfgMu mutex. This is simply because we have several places
// in consumer.go where we need it.
func (mset *stream) getCfgName() string {
	mset.cfgMu.RLock()
	defer mset.cfgMu.RUnlock()
	return mset.cfg.Name
}

// Purge will remove all messages from the stream and underlying store based on the request.
func (mset *stream) purge(preq *JSApiStreamPurgeRequest) (purged uint64, err error) {
	// Purges mutate message state, so hold the isolation lock like any other
	// write so direct get requests don't observe a partially applied purge.
	mset.isolateMu.Lock()
	defer mset.isolateMu.Unlock()
	return mset.purgeLocked(preq, true)
}

func (mset *stream) purgeLocked(preq *JSApiStreamPurgeRequest, needLock bool) (purged uint64, err error) {
	if needLock {
		mset.mu.Lock()
		defer mset.mu.Unlock()
	}
	if mset.closed.Load() {
		return 0, errStreamClosed
	}
	if mset.cfg.Sealed {
		return 0, errors.New("sealed stream")
	}
	store, mlseq := mset.store, mset.lseq

	if preq != nil {
		purged, err = mset.store.PurgeEx(preq.Subject, preq.Sequence, preq.Keep)
	} else {
		purged, err = mset.store.Purge()
	}
	if err != nil {
		return purged, err
	}

	// Grab our stream state.
	var state StreamState
	store.FastState(&state)
	fseq, lseq := state.FirstSeq, state.LastSeq

	// Check if our last has moved past what our original last sequence was, if so reset.
	if lseq > mlseq {
		mset.lseq = lseq
	}

	// Clear any pending acks below first seq.
	mset.clearAllPreAcksBelowFloor(fseq)

	// Purge consumers.
	// Check for filtered purge.
	if preq != nil && preq.Subject != _EMPTY_ {
		ss, err := store.FilteredState(fseq, preq.Subject)
		if err != nil {
			return purged, err
		}
		fseq = ss.First
	}

	// Take a copy of cList to avoid o.purge() potentially taking the stream lock and
	// violating the lock ordering.
	mset.clsMu.RLock()
	cList := slices.Clone(mset.cList)
	mset.clsMu.RUnlock()
	for _, o := range cList {
		start := fseq
		o.mu.RLock()
		// we update consumer sequences if:
		// no subject was specified, we can purge all consumers sequences
		doPurge := preq == nil ||
			preq.Subject == _EMPTY_ ||
			// consumer filter subject is equal to purged subject
			// or consumer filter subject is subset of purged subject,
			// but not the other way around.
			o.isEqualOrSubsetMatch(preq.Subject)
		// Check if a consumer has a wider subject space than what we purged
		var isWider bool
		if !doPurge && preq != nil && o.isFilteredMatch(preq.Subject) {
			doPurge, isWider = true, true
			start = state.FirstSeq
		} else if doPurge && preq != nil && preq.Subject != _EMPTY_ && !o.isFilterSubsetOf(preq.Subject) {
			isWider = true
			start = state.FirstSeq
		}
		o.mu.RUnlock()
		if doPurge {
			o.purge(start, lseq, isWider)
		}
	}

	return purged, nil
}

// RemoveMsg will remove a message from a stream.
// FIXME(dlc) - Should pick one and be consistent.
func (mset *stream) removeMsg(seq uint64) (bool, error) {
	return mset.deleteMsg(seq)
}

// DeleteMsg will remove a message from a stream.
func (mset *stream) deleteMsg(seq uint64) (bool, error) {
	if mset.closed.Load() {
		return false, errStreamClosed
	}
	// Deletes mutate message state, so hold the isolation lock like any other write.
	mset.isolateMu.Lock()
	defer mset.isolateMu.Unlock()
	removed, err := mset.store.RemoveMsg(seq)
	if err != nil {
		return removed, err
	}
	mset.mu.Lock()
	mset.clearAllPreAcks(seq)
	mset.mu.Unlock()
	return removed, err
}

// EraseMsg will securely remove a message and rewrite the data with random data.
func (mset *stream) eraseMsg(seq uint64) (bool, error) {
	if mset.closed.Load() {
		return false, errStreamClosed
	}
	// Deletes mutate message state, so hold the isolation lock like any other write.
	mset.isolateMu.Lock()
	defer mset.isolateMu.Unlock()
	removed, err := mset.store.EraseMsg(seq)
	if err != nil {
		return removed, err
	}
	mset.mu.Lock()
	mset.clearAllPreAcks(seq)
	mset.mu.Unlock()
	return removed, err
}

// Are we a mirror?
func (mset *stream) isMirror() bool {
	mset.cfgMu.RLock()
	defer mset.cfgMu.RUnlock()
	return mset.cfg.Mirror != nil
}

func (mset *stream) sourcesInfo() (sis []*StreamSourceInfo) {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	// Use the persisted source state for the sequences, si.sseq can be ahead of what's stored.
	var sourcesState map[string]StreamSourceState
	if mset.store != nil {
		sourcesState = mset.store.SourcesState()
	}
	sis = make([]*StreamSourceInfo, 0, len(mset.sources))
	for _, si := range mset.sources {
		ssi := mset.sourceInfo(si)
		if ssi == nil {
			continue
		}
		// Only report the sequence if the persisted state belongs to the same identity (if known).
		if sss, ok := sourcesState[si.iname]; ok && !si.rc && (sss.Ident == _EMPTY_ || si.ident == sss.Ident) {
			ssi.Seq = sss.Seq
		}
		sis = append(sis, ssi)
	}
	return sis
}

// Lock should be held
func (mset *stream) sourceInfo(si *sourceInfo) *StreamSourceInfo {
	if si == nil {
		return nil
	}

	var ssi = StreamSourceInfo{Name: si.name, Lag: si.lag, Error: si.err, FilterSubject: si.sf}

	trConfigs := make([]SubjectTransformConfig, len(si.sfs))
	for i := range si.sfs {
		var destination string
		if si.trs[i] != nil {
			destination = si.trs[i].dest
		}
		trConfigs[i] = SubjectTransformConfig{si.sfs[i], destination}
	}

	ssi.SubjectTransforms = trConfigs

	// If we have not heard from the source, set Active to -1.
	if last := si.last.Load(); last == 0 {
		ssi.Active = -1
	} else {
		ssi.Active = time.Since(time.Unix(0, last))
	}

	var ext *ExternalStream
	if mset.cfg.Mirror != nil {
		ext = mset.cfg.Mirror.External
	} else if ss := mset.streamSource(si.iname); ss != nil && ss.External != nil {
		ext = ss.External
	}
	if ext != nil {
		ssi.External = &ExternalStream{
			ApiPrefix:     ext.ApiPrefix,
			DeliverPrefix: ext.DeliverPrefix,
		}
	}
	return &ssi
}

// Return our source info for our mirror.
func (mset *stream) mirrorInfo() *StreamSourceInfo {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.sourceInfo(mset.mirror)
}

// retryDisconnectedSyncConsumers() will check if we have any disconnected
// sync consumers for either mirror or a source and will reset and retry to connect.
func (mset *stream) retryDisconnectedSyncConsumers() {
	mset.mu.Lock()
	defer mset.mu.Unlock()

	// Only applicable if we are the stream leader.
	if !mset.isLeader() {
		return
	}

	// Not client.isClosed(): internal account clients have nil nc, which would make isClosed always true here.
	clientClosed := func(c *client) bool {
		return c != nil && (c.flags.isSet(closeConnection) || c.flags.isSet(connMarkedClosed))
	}
	// Stale sources need to be reset: if not seen past the health check interval, it's stale.
	stale := func(si *sourceInfo) bool {
		return time.Since(time.Unix(0, si.last.Load())) > sourceHealthCheckInterval
	}
	shouldRetry := func(si *sourceInfo) bool {
		if si != nil && !si.sip && (si.sub == nil || clientClosed(si.sub.client) || stale(si)) {
			// Skip if a recreate is already scheduled and we can't cancel it.
			if t, ok := mset.sourceSetupSchedules[si.iname]; ok {
				if !t.Stop() {
					return false
				}
				delete(mset.sourceSetupSchedules, si.iname)
			}
			si.fails = 0
			mset.cancelSourceInfo(si)
			return true
		}
		return false
	}

	// Check mirrors first.
	if si := mset.mirror; si != nil {
		if shouldRetry(si) {
			mset.scheduleSetupMirrorConsumerRetry()
		}
	} else {
		for _, si := range mset.sources {
			if shouldRetry(si) {
				mset.setupSourceConsumer(si.iname, si.sseq+1, time.Time{})
			}
		}
	}
}

const (
	// Our consumer HB interval.
	sourceHealthHB = 1 * time.Second
	// How often we check and our stalled interval.
	sourceHealthCheckInterval = 10 * time.Second
)

// Will run as a Go routine to process mirror consumer messages.
func (mset *stream) processMirrorMsgs(mirror *sourceInfo, ready *sync.WaitGroup) {
	s := mset.srv
	defer func() {
		mirror.wg.Done()
		s.grWG.Done()
	}()

	// Grab stream quit channel.
	mset.mu.Lock()
	msgs, qch, siqch := mirror.msgs, mset.qch, mirror.qch
	// If the mirror was already canceled before we got here, exit early.
	if siqch == nil {
		mset.mu.Unlock()
		ready.Done()
		return
	}
	// Set the last seen as now so that we don't fail at the first check.
	mirror.last.Store(time.Now().UnixNano())
	mset.mu.Unlock()

	// Signal the caller that we have captured the above fields.
	ready.Done()

	// Make sure we have valid ipq for msgs.
	if msgs == nil {
		mset.mu.Lock()
		mset.cancelMirrorConsumer()
		mset.mu.Unlock()
		return
	}

	t := time.NewTicker(sourceHealthCheckInterval)
	defer t.Stop()

	for {
		select {
		case <-s.quitCh:
			return
		case <-qch:
			return
		case <-siqch:
			return
		case <-msgs.ch:
			ims := msgs.pop()
			for _, im := range ims {
				if !mset.processInboundMirrorMsg(im) {
					break
				}
				im.returnToPool()
			}
			msgs.recycle(&ims)
		case <-t.C:
			mset.mu.RLock()
			var stalled bool
			if mset.mirror != nil {
				stalled = time.Since(time.Unix(0, mset.mirror.last.Load())) > sourceHealthCheckInterval
			}
			isLeader := mset.isLeader()
			mset.mu.RUnlock()
			// No longer leader.
			if !isLeader {
				mset.mu.Lock()
				mset.cancelMirrorConsumer()
				mset.mu.Unlock()
				return
			}
			// We are stalled.
			if stalled {
				mset.retryMirrorConsumer()
			}
		}
	}
}

// Checks that the message is from our current direct consumer. We can not depend on sub comparison
// since cross account imports break.
func (si *sourceInfo) isCurrentSub(cname string) bool {
	return si.cname != _EMPTY_ && si.cname == cname
}

// processInboundMirrorMsg handles processing messages bound for a stream.
func (mset *stream) processInboundMirrorMsg(m *inMsg) bool {
	mset.mu.Lock()
	if mset.mirror == nil {
		mset.mu.Unlock()
		return false
	}
	if !mset.isLeader() {
		mset.cancelMirrorConsumer()
		mset.mu.Unlock()
		return false
	}

	isControl := m.isControlMsg()
	cname := consumerFromAckReply(m.rply)

	// Ignore from old subscriptions.
	// The reason we can not just compare subs is that on cross account imports they will not match.
	if !mset.mirror.isCurrentSub(cname) && !isControl {
		mset.mu.Unlock()
		return false
	}

	// Check for heartbeats and flow control messages.
	if isControl {
		var needsRetry bool
		// Flow controls have reply subjects.
		if m.rply != _EMPTY_ {
			mset.handleFlowControl(m, mset.mirror.dseq, mset.mirror.sseq)
		} else {
			// For idle heartbeats make sure we did not miss anything and check if we are considered stalled.
			if ldseq := parseInt64(sliceHeader(JSLastConsumerSeq, m.hdr)); ldseq > 0 && uint64(ldseq) != mset.mirror.dseq {
				needsRetry = true
			} else if fcReply := sliceHeader(JSConsumerStalled, m.hdr); len(fcReply) > 0 {
				// Other side thinks we are stalled, so send flow control reply.
				mset.processStalledFlowControl(mset.mirror, string(fcReply))
			} else {
				mset.clearStalledFlowControl(mset.mirror)
			}
		}
		mset.mu.Unlock()
		if needsRetry {
			mset.retryMirrorConsumer()
		}
		return !needsRetry
	}

	sseq, dseq, dc, ts, pending := ackReplyInfo(m.rply)

	if dc > 1 {
		mset.mu.Unlock()
		return false
	}

	// Mirror info tracking.
	olag, osseq, odseq, ocseq := mset.mirror.lag, mset.mirror.sseq, mset.mirror.dseq, mset.mirror.cseq
	if sseq <= mset.mirror.sseq {
		// Ignore older messages.
		// If the deliver sequence matches, we only update delivered accounting.
		// A stale delivery from before a consumer reset could match too, but it won't be above the last counted.
		if dseq == mset.mirror.dseq+1 && sseq > mset.mirror.cseq {
			mset.mirror.dseq++
			mset.mirror.cseq = sseq
		}
		mset.mu.Unlock()
		return true
	} else if sseq == mset.mirror.sseq+1 {
		mset.mirror.dseq = dseq
		mset.mirror.sseq++
		mset.mirror.cseq = sseq
	} else if mset.mirror.cname == _EMPTY_ {
		mset.mirror.cname = cname
		mset.mirror.dseq, mset.mirror.sseq, mset.mirror.cseq = dseq, sseq, sseq
	} else {
		// If the deliver sequence matches then the upstream stream has expired or deleted messages.
		if dseq == mset.mirror.dseq+1 {
			if err := mset.skipMsgs(mset.mirror.sseq+1, sseq-1); err != nil {
				mset.mirror.sseq = osseq
				mset.mirror.dseq = odseq
				mset.mirror.cseq = ocseq
				mset.mu.Unlock()
				mset.retryMirrorConsumer()
				return false
			}
			mset.mirror.dseq++
			mset.mirror.sseq = sseq
			mset.mirror.cseq = sseq
		} else {
			mset.mu.Unlock()
			mset.retryMirrorConsumer()
			return false
		}
	}

	if pending == 0 {
		mset.mirror.lag = 0
	} else {
		mset.mirror.lag = pending - 1
	}
	// Receiving messages means we are not stalled on flow control.
	mset.clearStalledFlowControl(mset.mirror)

	// Check if we allow mirror direct here. If so check they we have mostly caught up.
	// The reason we do not require 0 is if the source is active we may always be slightly behind.
	if mset.cfg.MirrorDirect && mset.mirrorDirectSub == nil && pending < dgetCaughtUpThresh {
		if err := mset.subscribeToMirrorDirect(); err != nil {
			// Disable since we had problems above.
			mset.cfgMu.Lock()
			mset.cfg.MirrorDirect = false
			mset.cfgMu.Unlock()
		}
	}

	// Do the subject transform if there's one
	if len(mset.mirror.trs) > 0 {
		for _, tr := range mset.mirror.trs {
			if tr == nil {
				continue
			} else {
				tsubj, err := tr.Match(m.subj)
				if err == nil {
					m.subj = tsubj
					break
				}
			}
		}
	}

	s, js, stype := mset.srv, mset.js, mset.cfg.Storage
	isClustered, node, term := mset.isClustered(), mset.node, mset.term
	mset.mu.Unlock()

	var err error
	if isClustered {
		if stype == FileStorage && isFileStoreMsgTooLarge(fileStoreMsgSize(m.subj, m.hdr, m.msg)) {
			err = ErrMsgTooLarge
		} else if js.limitsExceeded(stype) {
			s.resourcesExceededError(stype)
			err = ApiErrors[JSInsufficientResourcesErr]
		} else {
			err = node.Propose(term, encodeStreamMsg(m.subj, _EMPTY_, m.hdr, m.msg, sseq-1, ts, true))
		}
	} else {
		err = mset.processJetStreamMsg(m.subj, _EMPTY_, m.hdr, m.msg, sseq-1, ts, nil, true, true)
	}
	if err != nil {
		if strings.Contains(err.Error(), "no space left") {
			s.Errorf("JetStream out of space, will be DISABLED")
			s.ShutdownJetStream()
			return false
		}
		if err != errLastSeqMismatch {
			mset.mu.RLock()
			accName, sname := mset.acc.Name, mset.cfg.Name
			mset.mu.RUnlock()
			s.RateLimitWarnf("Error processing inbound mirror message for '%s' > '%s': %v",
				accName, sname, err)
		} else {
			// We may have missed messages, restart.
			lseq := mset.lastSeq()
			mset.mu.Lock()
			if mset.mirror != nil {
				mset.mirror.lag = olag
				mset.mirror.dseq = odseq
				mset.mirror.sseq = osseq
				mset.mirror.cseq = ocseq
				if sseq <= lseq {
					mset.mirror.sseq = lseq
				}
			}
			mset.mu.Unlock()
			mset.retryMirrorConsumer()
		}
	}
	return err == nil
}

func (mset *stream) setMirrorErr(err *ApiError) {
	mset.mu.Lock()
	if mset.mirror != nil {
		mset.mirror.err = err
	}
	mset.mu.Unlock()
}

// Cancels a mirror consumer.
//
// Lock held on entry
func (mset *stream) cancelMirrorConsumer() {
	if mset.mirror == nil {
		return
	}
	mset.cancelSourceInfo(mset.mirror)
}

// Similar to setupMirrorConsumer except that it will print a debug statement
// indicating that there is a retry.
//
// Lock is acquired in this function
func (mset *stream) retryMirrorConsumer() error {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	mset.srv.Debugf("Retrying mirror consumer for '%s > %s'", mset.acc.Name, mset.cfg.Name)
	mset.cancelMirrorConsumer()
	return mset.setupMirrorConsumer()
}

// Lock should be held.
func (mset *stream) skipMsgs(start, end uint64) error {
	// If we are not clustered we can short circuit now with store.SkipMsgs
	if !mset.isClustered() {
		if err := mset.store.SkipMsgs(start, end-start+1); err != nil {
			return err
		}
		mset.lseq = end
		return nil
	}
	// Send a single deleteRangeOp for the full range.
	return mset.node.Propose(mset.term, encodeDeleteRange(&DeleteRange{First: start, Num: end - start + 1}))
}

const (
	// Base retry backoff duration.
	retryBackOff = 5 * time.Second
	// Maximum amount we will wait.
	retryMaximum = 2 * time.Minute
)

// Calculate our backoff based on number of failures.
func calculateRetryBackoff(fails int) time.Duration {
	backoff := time.Duration(retryBackOff) * time.Duration(fails*2)
	if backoff > retryMaximum {
		backoff = retryMaximum
	}
	return backoff
}

// This will schedule a call to setupMirrorConsumer, taking into account the last
// time it was retried and determine the soonest setupMirrorConsumer can be called
// without tripping the sourceConsumerRetryThreshold. We will also take into account
// number of failures and will back off our retries.
// The mset.mirror pointer has been verified to be not nil by the caller.
//
// Lock held on entry
func (mset *stream) scheduleSetupMirrorConsumerRetry() {
	// We are trying to figure out how soon we can retry. setupMirrorConsumer will reject
	// a retry if last was done less than "sourceConsumerRetryThreshold" ago.
	next := sourceConsumerRetryThreshold - time.Since(mset.mirror.lreq)
	if next < 0 {
		// It means that we have passed the threshold and so we are ready to go.
		next = 0
	}
	// Take into account failures here.
	next += calculateRetryBackoff(mset.mirror.fails)

	// Add some jitter.
	next += time.Duration(rand.IntN(int(100*time.Millisecond))) + 100*time.Millisecond

	stopAndClearTimer(&mset.mirrorConsumerSetup)
	mset.mirrorConsumerSetup = time.AfterFunc(next, func() {
		mset.mu.Lock()
		mset.setupMirrorConsumer()
		mset.mu.Unlock()
	})
}

// How long we wait for a response from a consumer create request for a source or mirror.
var srcConsumerWaitTime = 30 * time.Second

// How long we wait for a response from a consumer reset request for a durable source or
// mirror. Shorter than a create, since the consumer is only repositioned, not stood up.
var srcDurableConsumerWaitTime = 5 * time.Second

// Setup our mirror consumer.
// Lock should be held.
func (mset *stream) setupMirrorConsumer() error {
	if mset.closed.Load() {
		return errStreamClosed
	}
	if mset.outq == nil {
		return errors.New("outq required")
	}
	// We use to prevent update of a mirror configuration in cluster
	// mode but not in standalone. This is now fixed. However, without
	// rejecting the update, it could be that if the source stream was
	// removed and then later the mirrored stream config changed to
	// remove mirror configuration, this function would panic when
	// accessing mset.cfg.Mirror fields. Adding this protection in case
	// we allow in the future the mirror config to be changed (removed).
	if mset.cfg.Mirror == nil {
		return errors.New("invalid mirror configuration")
	}

	// If this is the first time
	if mset.mirror == nil {
		mset.mirror = &sourceInfo{name: mset.cfg.Mirror.Name}
	} else {
		mset.cancelSourceInfo(mset.mirror)
		mset.mirror.sseq = mset.lseq
	}

	// If we are no longer the leader stop trying.
	if !mset.isLeader() {
		return nil
	}

	mirror := mset.mirror

	// We want to throttle here in terms of how fast we request new consumers,
	// or if the previous is still in progress.
	if last := time.Since(mirror.lreq); last < sourceConsumerRetryThreshold || mirror.sip {
		mset.scheduleSetupMirrorConsumerRetry()
		return nil
	}
	mirror.lreq = time.Now()

	// Determine subjects etc.
	var deliverSubject string
	var durableDeliverSubject string
	ext := mset.cfg.Mirror.External
	if mset.cfg.Mirror.Consumer != nil {
		durableDeliverSubject = mset.cfg.Mirror.Consumer.DeliverSubject
		mirror.cname = mset.cfg.Mirror.Consumer.Name
	} else if ext != nil && ext.DeliverPrefix != _EMPTY_ {
		deliverSubject = strings.ReplaceAll(ext.DeliverPrefix+syncSubject(".M"), "..", ".")
	} else {
		deliverSubject = syncSubject("$JS.M")
	}

	// Now send off request to create/update our consumer. This will be all API based even in single server mode.
	// We calculate durable names apriori so we do not need to save them off.

	var state StreamState
	mset.store.FastState(&state)

	id := mset.createStableConsumerHash()
	metadata := map[string]string{}
	metadata["_nats.mirror.stream"] = mset.cfg.Name
	metadata["_nats.mirror.acc"] = mset.acc.Name
	if domain := mset.srv.getOpts().JetStreamDomain; domain != _EMPTY_ {
		metadata["_nats.mirror.domain"] = domain
	}

	req := &CreateConsumerRequest{
		Stream: mset.cfg.Mirror.Name,
		Config: ConsumerConfig{
			Name:              fmt.Sprintf("JS_MIRROR_%s", id),
			DeliverSubject:    deliverSubject,
			DeliverPolicy:     DeliverByStartSequence,
			OptStartSeq:       state.LastSeq + 1,
			AckPolicy:         AckNone,
			AckWait:           22 * time.Hour,
			MaxDeliver:        1,
			Heartbeat:         sourceHealthHB,
			FlowControl:       true,
			Direct:            true,
			Sourcing:          true,
			InactiveThreshold: sourceHealthCheckInterval,
			Metadata:          metadata,
		},
	}

	// Only use start optionals on first time.
	if state.Msgs == 0 && state.FirstSeq == 0 {
		req.Config.OptStartSeq = 0
		if mset.cfg.Mirror.OptStartSeq > 0 {
			req.Config.OptStartSeq = mset.cfg.Mirror.OptStartSeq
		} else if mset.cfg.Mirror.OptStartTime != nil {
			req.Config.OptStartTime = mset.cfg.Mirror.OptStartTime
			req.Config.DeliverPolicy = DeliverByStartTime
		}
	}
	if req.Config.OptStartSeq == 0 && req.Config.OptStartTime == nil {
		// If starting out and lastSeq is 0.
		req.Config.DeliverPolicy = DeliverAll
	}

	// Filters
	if mset.cfg.Mirror.FilterSubject != _EMPTY_ {
		req.Config.FilterSubject = mset.cfg.Mirror.FilterSubject
		mirror.sf = mset.cfg.Mirror.FilterSubject
	}

	if lst := len(mset.cfg.Mirror.SubjectTransforms); lst > 0 {
		sfs := make([]string, lst)
		trs := make([]*subjectTransform, lst)

		for i, tr := range mset.cfg.Mirror.SubjectTransforms {
			// will not fail as already checked before that the transform will work
			subjectTransform, err := NewSubjectTransform(tr.Source, tr.Destination)
			if err != nil {
				mset.srv.Errorf("Unable to get transform for mirror consumer: %v", err)
			}
			sfs[i] = tr.Source
			trs[i] = subjectTransform
		}
		mirror.sfs = sfs
		mirror.trs = trs
		// If there was no explicit FilterSubject defined and we have a single
		// subject transform, use Config.FilterSubject instead of FilterSubjects
		// so that we can use the extended consumer create API down below.
		if req.Config.FilterSubject == _EMPTY_ && len(sfs) == 1 {
			req.Config.FilterSubject = sfs[0]
		} else {
			req.Config.FilterSubjects = sfs
		}
	}

	newReplySubscription := func() (string, chan *JSApiConsumerCreateResponse, *subscription, error) {
		respCh := make(chan *JSApiConsumerCreateResponse, 1)
		consumer := req.Config.Name
		if durableDeliverSubject != _EMPTY_ {
			consumer = mirror.cname
		}
		reply := mset.srv.infoReplySubject(ext.Domain(), mset.acc.Name, mset.cfg.Mirror.Name, consumer)
		crSub, err := mset.subscribeInternal(reply, func(sub *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
			_, msg := c.msgParts(rmsg)

			var ccr JSApiConsumerCreateResponse
			if err := json.Unmarshal(msg, &ccr); err != nil {
				c.Warnf("JetStream bad mirror consumer create response: %q", msg)
				mset.setMirrorErr(ApiErrors[JSInvalidJSONErr])
				return
			}
			select {
			case respCh <- &ccr:
			default:
			}
		})
		return reply, respCh, crSub, err
	}
	reply, respCh, crSub, err := newReplySubscription()
	if err != nil {
		mirror.err = NewJSMirrorConsumerSetupFailedError(err, Unless(err))
		mset.scheduleSetupMirrorConsumerRetry()
		return nil
	}

	generateSubject := func() (subject string) {
		if durableDeliverSubject != _EMPTY_ {
			// If we're using a pre-existing consumer, we'll send a consumer reset request instead.
			subject = fmt.Sprintf(JSApiConsumerResetT, mset.cfg.Mirror.Name, mirror.cname)
		} else if req.Config.FilterSubject != _EMPTY_ {
			subject = fmt.Sprintf(JSApiConsumerCreateExT, mset.cfg.Mirror.Name, req.Config.Name, req.Config.FilterSubject)
		} else {
			subject = fmt.Sprintf(JSApiConsumerCreateT, mset.cfg.Mirror.Name)
		}
		if ext != nil {
			subject = strings.Replace(subject, JSApiPrefix, ext.ApiPrefix, 1)
			subject = strings.ReplaceAll(subject, "..", ".")
		}
		return subject
	}
	subject := generateSubject()

	// Reset
	mirror.msgs = nil
	mirror.err = nil
	mirror.sip = true

	if durableDeliverSubject != _EMPTY_ {
		// Send the consumer reset request
		mset.outq.send(newJSPubMsg(subject, _EMPTY_, reply, nil, nil, nil, 0))
	} else {
		// Marshal now that we are done with `req`.
		b, _ := json.Marshal(req)

		// Send the consumer create request
		// Confirm the server supports API level 4, which contains durable sourcing, AckFlowControl, and consumer reset.
		hdr := genHeader(nil, JSRequiredApiLevel, "4")
		mset.outq.send(newJSPubMsg(subject, _EMPTY_, reply, hdr, b, nil, 0))
	}

	go func() {
		// A durable consumer only needs to be reset, so don't wait as long as we would
		// for a consumer that still has to be created from scratch.
		timeout := srcConsumerWaitTime
		if durableDeliverSubject != _EMPTY_ {
			timeout = srcDurableConsumerWaitTime
		}

		var retry bool
		var timedOut bool
		defer func() {
			mset.mu.Lock()
			// Check that this is still valid and if so, clear the "setup in progress" flag.
			if mset.mirror != nil {
				mset.mirror.sip = false
				// If we need to retry, schedule now
				// If sub is not nil means we re-established somewhere else so do not re-attempt here.
				if retry && mset.mirror.sub == nil {
					mset.mirror.fails++
					// Cancel here since we can not do anything with this consumer at this point.
					mset.cancelSourceInfo(mset.mirror)
					mset.scheduleSetupMirrorConsumerRetry()
					if timedOut && durableDeliverSubject != _EMPTY_ {
						mset.startProbeSubscription(mset.mirror, durableDeliverSubject, func() {
							// Skip if one is already running.
							if t := mset.mirrorConsumerSetup; t != nil && !t.Stop() {
								return
							}
							mset.mirrorConsumerSetup = nil
							mset.scheduleSetupMirrorConsumerRetry()
						})
					}
				} else {
					// Clear on success.
					mset.mirror.fails = 0
				}
			}
			mset.mu.Unlock()
		}()

		// Wait for previous processMirrorMsgs go routine to be completely done.
		// If none is running, this will not block.
		mset.mu.Lock()
		if mset.mirror == nil {
			// Mirror config has been removed.
			mset.unsubscribe(crSub)
			mset.mu.Unlock()
			return
		}
		wg := &mset.mirror.wg
		mset.mu.Unlock()
		wg.Wait()

	SELECT:
		select {
		case ccr := <-respCh:
			mset.mu.Lock()
			// Mirror config has been removed.
			if mset.mirror == nil {
				mset.unsubscribe(crSub)
				mset.mu.Unlock()
				return
			}
			ready := sync.WaitGroup{}
			mirror := mset.mirror
			mirror.err = nil

			if ccr.Error != nil || ccr.ConsumerInfo == nil || ccr.ConsumerInfo.Config == nil {
				// If the responding server doesn't support sourcing consumers, retry without it.
				if req.Config.Sourcing && ccr.Error != nil &&
					(ccr.Error.ErrCode == uint16(JSRequiredApiLevelErr) || ccr.Error.ErrCode == uint16(JSInvalidJSONErr)) {
					// Unset for retry.
					req.Config.Sourcing = false
					// Specify a unique consumer name, as the other end will not know to do this.
					req.Config.Name = fmt.Sprintf("JS_MIRROR_%s_%s", id, createConsumerName())
					b, _ := json.Marshal(req)
					// Regenerate subject since the previous name could've been included in it.
					subject = generateSubject()
					// Recreate the reply subscription so we don't get stale responses from other servers.
					mset.unsubscribe(crSub)
					if reply, respCh, crSub, err = newReplySubscription(); err != nil {
						mirror.err = NewJSMirrorConsumerSetupFailedError(err, Unless(err))
						retry = true
						mset.mu.Unlock()
						return
					}
					mset.outq.send(newJSPubMsg(subject, _EMPTY_, reply, nil, b, nil, 0))
					mset.mu.Unlock()
					goto SELECT
				}
				mset.unsubscribe(crSub)
				cerr := ccr.Error
				if cerr == nil {
					cerr = NewJSMirrorConsumerSetupFailedError(errors.New("invalid consumer create response"))
				}
				mset.srv.Warnf("JetStream error response for create mirror consumer: %+v", cerr)
				mirror.err = cerr
				// Let's retry as soon as possible, but we are gated by sourceConsumerRetryThreshold
				retry = true
				mset.mu.Unlock()
				return
			}

			// If using durable sourcing, we need the consumer to use acks based on flow control.
			if durableDeliverSubject != _EMPTY_ && ccr.ConsumerInfo.Config.AckPolicy != AckFlowControl {
				mset.unsubscribe(crSub)
				mirror.err = NewJSMirrorConsumerRequiresAckFCError()
				retry = true
				mset.mu.Unlock()
				return
			}

			// We can now unsubscribe.
			mset.unsubscribe(crSub)

			// Setup actual subscription to process messages from our source.
			qname := fmt.Sprintf("[ACC:%s] stream mirror '%s' of '%s' msgs", mset.acc.Name, mset.cfg.Name, mset.cfg.Mirror.Name)
			// Create a new queue each time
			mirror.msgs = newIPQueue[*inMsg](mset.srv, qname)
			msgs := mirror.msgs
			if durableDeliverSubject != _EMPTY_ {
				deliverSubject = durableDeliverSubject
			} else {
				deliverSubject = ccr.ConsumerInfo.Config.DeliverSubject
			}
			sub, err := mset.subscribeInternal(deliverSubject, func(sub *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
				hdr, msg := c.msgParts(copyBytes(rmsg)) // Need to copy.
				if len(hdr) > 0 {
					// Remove any Nats-Expected- headers as we don't want to validate them.
					hdr = removeHeaderIfPrefixPresent(hdr, "Nats-Expected-")
					// Remove any Nats-Batch- headers, batching is not supported when mirroring.
					hdr = removeHeaderIfPrefixPresent(hdr, "Nats-Batch-")
				}
				mset.queueInbound(msgs, subject, reply, hdr, msg, nil, nil)
				mirror.last.Store(time.Now().UnixNano())
			})
			if err != nil {
				mirror.err = NewJSMirrorConsumerSetupFailedError(err, Unless(err))
				retry = true
				mset.mu.Unlock()
				return
			}
			// Save our sub.
			mirror.sub = sub

			// Check if we need to skip messages.
			// Re-capture state since the previous may be stale.
			state = StreamState{}
			mset.store.FastState(&state)
			if state.LastSeq < ccr.ConsumerInfo.Delivered.Stream {
				// Local helper: abort consumer setup, leaving the mirror in its
				// pre-setup state so the retry path can re-create it cleanly.
				failSetup := func(setupErr error) {
					mset.cancelSourceInfo(mirror)
					mirror.err = NewJSMirrorConsumerSetupFailedError(setupErr, Unless(setupErr))
					retry = true
					mset.mu.Unlock()
				}
				// Check to see if delivered is past our last and we have no msgs. This will help the
				// case when mirroring a stream that has a very high starting sequence number.
				if state.Msgs == 0 && ccr.ConsumerInfo.Delivered.Stream > state.LastSeq {
					if _, err := mset.store.PurgeEx(_EMPTY_, ccr.ConsumerInfo.Delivered.Stream+1, 0); err != nil {
						failSetup(err)
						return
					}
					mset.lseq = ccr.ConsumerInfo.Delivered.Stream
				} else if err := mset.skipMsgs(state.LastSeq+1, ccr.ConsumerInfo.Delivered.Stream); err != nil {
					failSetup(err)
					return
				}
			}

			// Capture consumer name.
			mirror.cname = ccr.ConsumerInfo.Name
			mirror.dseq, mirror.cseq = 0, ccr.ConsumerInfo.Delivered.Stream
			mirror.fcid, mirror.fcsc = _EMPTY_, 0
			mirror.sseq = max(ccr.ConsumerInfo.Delivered.Stream, state.LastSeq)
			mirror.qch = make(chan struct{})
			mirror.wg.Add(1)
			ready.Add(1)
			if !mset.srv.startGoRoutine(
				func() { mset.processMirrorMsgs(mirror, &ready) },
				pprofLabels{
					"type":     "mirror",
					"account":  mset.acc.Name,
					"stream":   mset.cfg.Name,
					"consumer": mirror.cname,
				},
			) {
				mirror.wg.Done()
				ready.Done()
			}
			mset.mu.Unlock()
			ready.Wait()
		case <-time.After(timeout):
			mset.unsubscribe(crSub)
			// We already waited long enough, let's retry now.
			retry, timedOut = true, true
		}
	}()

	return nil
}

func (mset *stream) streamSource(iname string) *StreamSource {
	for _, ssi := range mset.cfg.Sources {
		if ssi.iname == iname {
			return ssi
		}
	}
	return nil
}

// Lock should be held.
func (mset *stream) retrySourceConsumerAtSeq(iName string, seq uint64) {
	s := mset.srv

	s.Debugf("Retrying source consumer for '%s > %s'", mset.acc.Name, mset.cfg.Name)

	// setupSourceConsumer will check that the source is still configured.
	mset.setupSourceConsumer(iName, seq, time.Time{})
}

// Lock should be held.
func (mset *stream) cancelSourceConsumer(iname string) {
	if si := mset.sources[iname]; si != nil {
		mset.cancelSourceInfo(si)
		si.sseq, si.dseq = 0, 0
	}
}

// The `si` has been verified to be not nil. The sourceInfo's sub will
// be unsubscribed and set to nil (if not already done) and the
// cname will be reset. The message processing's go routine quit channel
// will be closed if still opened.
//
// Lock should be held
func (mset *stream) cancelSourceInfo(si *sourceInfo) {
	if si.sub != nil {
		mset.unsubscribe(si.sub)
		si.sub = nil
	}
	mset.removeInternalConsumer(si)
	if si.qch != nil {
		close(si.qch)
		si.qch = nil
	}
	if si.msgs != nil {
		si.msgs.drain()
		si.msgs.unregister()
	}
	// If we have a schedule setup go ahead and delete that.
	if t := mset.sourceSetupSchedules[si.iname]; t != nil {
		t.Stop()
		delete(mset.sourceSetupSchedules, si.iname)
	}
}

// startProbeSubscription watches a durable consumer's deliver subject after a request for
// it timed out, and stays up for as long as we are backing off. When we receive a heartbeat
// or message, we can short-circuit on retrying the reset request.
// Lock should be held.
func (mset *stream) startProbeSubscription(si *sourceInfo, deliverSubject string, retry func()) {
	if si == nil || si.sub != nil || deliverSubject == _EMPTY_ {
		return
	}
	sub, err := mset.subscribeInternal(deliverSubject, func(_ *subscription, _ *client, _ *Account, _, _ string, _ []byte) {
		mset.mu.Lock()
		defer mset.mu.Unlock()
		// Only short-circuit a backoff we are still in. si.fails is back to zero if we
		// already did so, and si.sip means a request is in flight that tears us down anyway.
		if si.sub == nil || si.sip || si.fails == 0 {
			return
		}
		// si.fails only drives the retry backoff, which does not apply to a consumer we
		// can see is alive, so clear it and ask again now.
		si.fails = 0
		retry()
	})
	if err != nil {
		return
	}
	si.sub = sub
}

const sourceConsumerRetryThreshold = 2 * time.Second

// This is the main function to call when needing to setup a new consumer for the source.
// It actually only does the scheduling of the execution of trySetupSourceConsumer in order to implement retry backoff
// and throttle the number of requests.
// Lock should be held.
func (mset *stream) setupSourceConsumer(iname string, seq uint64, startTime time.Time) {
	if mset.sourceSetupSchedules == nil {
		mset.sourceSetupSchedules = map[string]*time.Timer{}
	}

	if _, ok := mset.sourceSetupSchedules[iname]; ok {
		// If there is already a timer scheduled, we don't need to do anything.
		return
	}

	si := mset.sources[iname]
	if si == nil || si.sip { // if sourceInfo was removed or setup is in progress, nothing to do
		return
	}

	// First calculate the delay until the next time we can
	var scheduleDelay time.Duration

	if !si.lreq.IsZero() { // it's not the very first time we are called, compute the delay
		// We want to throttle here in terms of how fast we request new consumers
		if sinceLast := time.Since(si.lreq); sinceLast < sourceConsumerRetryThreshold {
			scheduleDelay = sourceConsumerRetryThreshold - sinceLast
		}
		// Is it a retry? If so, add a backoff
		if si.fails > 0 {
			scheduleDelay += calculateRetryBackoff(si.fails)
		}
	}

	// Always add some jitter
	scheduleDelay += time.Duration(rand.IntN(int(100*time.Millisecond))) + 100*time.Millisecond

	// Schedule the call to trySetupSourceConsumer
	mset.sourceSetupSchedules[iname] = time.AfterFunc(scheduleDelay, func() {
		mset.mu.Lock()
		defer mset.mu.Unlock()

		delete(mset.sourceSetupSchedules, iname)
		mset.trySetupSourceConsumer(iname, seq, startTime)
	})
}

// This is where we will actually try to create a new consumer for the source
// Lock should be held.
func (mset *stream) trySetupSourceConsumer(iname string, seq uint64, startTime time.Time) {
	// Ignore if closed or not leader.
	if mset.closed.Load() || !mset.isLeader() {
		return
	}

	si := mset.sources[iname]
	if si == nil {
		return
	}

	// Cancel previous instance if applicable
	mset.cancelSourceInfo(si)

	ssi := mset.streamSource(iname)
	if ssi == nil {
		return
	}

	si.lreq = time.Now()

	// Determine subjects etc.
	var deliverSubject string
	var durableDeliverSubject string
	ext := ssi.External
	if ssi.Consumer != nil {
		durableDeliverSubject = ssi.Consumer.DeliverSubject
		si.cname = ssi.Consumer.Name
	} else if ext != nil && ext.DeliverPrefix != _EMPTY_ {
		deliverSubject = strings.ReplaceAll(ext.DeliverPrefix+syncSubject(".S"), "..", ".")
	} else {
		deliverSubject = syncSubject("$JS.S")
	}

	id := mset.createSourcingConsumerHash(ssi, mset.cfg.Sources)
	metadata := map[string]string{}
	metadata["_nats.src.stream"] = mset.cfg.Name
	metadata["_nats.src.acc"] = mset.acc.Name
	if domain := mset.srv.getOpts().JetStreamDomain; domain != _EMPTY_ {
		metadata["_nats.src.domain"] = domain
	}

	req := &CreateConsumerRequest{
		Stream: si.name,
		Config: ConsumerConfig{
			Name:              fmt.Sprintf("JS_SRC_%s", id),
			DeliverSubject:    deliverSubject,
			AckPolicy:         AckNone,
			AckWait:           22 * time.Hour,
			MaxDeliver:        1,
			Heartbeat:         sourceHealthHB,
			FlowControl:       true,
			Direct:            true,
			Sourcing:          true,
			InactiveThreshold: sourceHealthCheckInterval,
			Metadata:          metadata,
		},
	}

	// If starting, check any configs.
	if !startTime.IsZero() && seq > 1 {
		req.Config.OptStartTime = &startTime
		req.Config.DeliverPolicy = DeliverByStartTime
	} else if seq <= 1 {
		if ssi.OptStartSeq > 0 {
			req.Config.OptStartSeq = ssi.OptStartSeq
			req.Config.DeliverPolicy = DeliverByStartSequence
		} else {
			// We have not recovered state so check that configured time is less that our first seq time.
			var state StreamState
			mset.store.FastState(&state)
			if ssi.OptStartTime != nil {
				if !state.LastTime.IsZero() && ssi.OptStartTime.Before(state.LastTime) {
					req.Config.OptStartTime = &state.LastTime
				} else {
					req.Config.OptStartTime = ssi.OptStartTime
				}
				req.Config.DeliverPolicy = DeliverByStartTime
			} else if state.FirstSeq > 1 && !state.LastTime.IsZero() && !si.rc {
				req.Config.OptStartTime = &state.LastTime
				req.Config.DeliverPolicy = DeliverByStartTime
			}
		}

	} else {
		req.Config.OptStartSeq = seq
		req.Config.DeliverPolicy = DeliverByStartSequence
	}
	// Filters
	if ssi.FilterSubject != _EMPTY_ {
		req.Config.FilterSubject = ssi.FilterSubject
	}

	var filterSubjects []string
	for _, tr := range ssi.SubjectTransforms {
		filterSubjects = append(filterSubjects, tr.Source)
	}
	req.Config.FilterSubjects = filterSubjects

	type consumerCreateResp struct {
		ccr   *JSApiConsumerCreateResponse
		ident string
	}
	newReplySubscription := func() (string, chan *consumerCreateResp, *subscription, error) {
		respCh := make(chan *consumerCreateResp, 1)
		consumer := req.Config.Name
		if durableDeliverSubject != _EMPTY_ {
			consumer = si.cname
		}
		reply := mset.srv.infoReplySubject(ext.Domain(), mset.acc.Name, si.name, consumer)
		crSub, err := mset.subscribeInternal(reply, func(sub *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
			hdr, msg := c.msgParts(rmsg)
			var ccr JSApiConsumerCreateResponse
			if err := json.Unmarshal(msg, &ccr); err != nil {
				c.Warnf("JetStream bad source consumer create response: %q", msg)
				return
			}
			var ident string
			if si := sliceHeader(JSStreamIdentity, hdr); si != nil {
				ident = string(si)
			}
			select {
			case respCh <- &consumerCreateResp{ccr: &ccr, ident: ident}:
			default:
			}
		})
		return reply, respCh, crSub, err
	}
	reply, respCh, crSub, err := newReplySubscription()
	if err != nil {
		si.err = NewJSSourceConsumerSetupFailedError(err, Unless(err))
		mset.setupSourceConsumer(iname, seq, startTime)
		return
	}

	generateSubject := func() (subject string) {
		if durableDeliverSubject != _EMPTY_ {
			// If we're using a pre-existing consumer, we'll send a consumer reset request instead.
			subject = fmt.Sprintf(JSApiConsumerResetT, si.name, si.cname)
		} else if req.Config.FilterSubject != _EMPTY_ {
			subject = fmt.Sprintf(JSApiConsumerCreateExT, si.name, req.Config.Name, req.Config.FilterSubject)
		} else if len(req.Config.FilterSubjects) == 1 {
			// It is necessary to switch to using FilterSubject here as the extended consumer
			// create API checks for it, so as to not accidentally allow multiple filtered subjects.
			req.Config.FilterSubject = req.Config.FilterSubjects[0]
			req.Config.FilterSubjects = nil
			subject = fmt.Sprintf(JSApiConsumerCreateExT, si.name, req.Config.Name, req.Config.FilterSubject)
		} else {
			subject = fmt.Sprintf(JSApiConsumerCreateT, si.name)
		}
		if ext != nil {
			subject = strings.Replace(subject, JSApiPrefix, ext.ApiPrefix, 1)
			subject = strings.ReplaceAll(subject, "..", ".")
		}
		return subject
	}
	subject := generateSubject()

	// Reset
	si.msgs = nil
	si.err = nil
	si.sip = true

	// Three API levels can be tried:
	// - API level 5: stream recreation detection.
	// - API level 4: durable sourcing, AckFlowControl, and consumer reset.
	// - not specified: used as fallback.
	apiLevel := 5
	hdr := genHeader(nil, JSRequiredApiLevel, strconv.Itoa(apiLevel))
	hdr = genHeader(hdr, JSStreamIdentity, si.ident)
	if durableDeliverSubject != _EMPTY_ {
		// Send the consumer reset request
		mset.outq.send(newJSPubMsg(subject, _EMPTY_, reply, hdr, nil, nil, 0))
	} else {
		// Marshal request.
		b, _ := json.Marshal(req)

		// Send the consumer create request
		mset.outq.send(newJSPubMsg(subject, _EMPTY_, reply, hdr, b, nil, 0))
	}

	go func() {
		// A durable consumer only needs to be reset, so don't wait as long as we would
		// for a consumer that still has to be created from scratch.
		timeout := srcConsumerWaitTime
		if durableDeliverSubject != _EMPTY_ {
			timeout = srcDurableConsumerWaitTime
		}

		var retry bool
		var recreate bool
		var timedOut bool
		defer func() {
			mset.mu.Lock()
			// Check that this is still valid and if so, clear the "setup in progress" flag.
			if si := mset.sources[iname]; si != nil {
				si.sip = false
				// If we need to retry, schedule now
				// If sub is not nil means we re-established somewhere else so do not re-attempt here.
				if retry && si.sub == nil {
					if !recreate {
						si.fails++
					}
					// Cancel here since we can not do anything with this consumer at this point.
					mset.cancelSourceInfo(si)
					mset.setupSourceConsumer(iname, seq, startTime)
					if timedOut && durableDeliverSubject != _EMPTY_ {
						mset.startProbeSubscription(si, durableDeliverSubject, func() {
							// Drop the scheduled retry, we are going again right now.
							if t, ok := mset.sourceSetupSchedules[iname]; ok {
								// Skip if one is already running.
								if !t.Stop() {
									return
								}
								delete(mset.sourceSetupSchedules, iname)
							}
							mset.setupSourceConsumer(iname, seq, startTime)
						})
					}
				} else {
					// Clear on success.
					si.fails = 0
				}
			}
			mset.mu.Unlock()
		}()

	SELECT:
		select {
		case resp := <-respCh:
			ccr, streamIdentity := resp.ccr, resp.ident
			mset.mu.Lock()
			// Check that it has not been removed or canceled (si.sub would be nil)
			if si := mset.sources[iname]; si == nil {
				mset.unsubscribe(crSub)
			} else {
				si.err = nil

				if ccr.Error != nil || ccr.ConsumerInfo == nil || ccr.ConsumerInfo.Config == nil {
					// Retry after unsetting the expected stream's identity.
					if ccr.Error != nil && ccr.Error.ErrCode == uint16(JSConsumerStreamIdentityMismatchF) {
						mset.unsubscribe(crSub)
						seq, si.sseq, si.dseq = 0, 0, 0
						si.ident, si.rc = _EMPTY_, true
						retry, recreate = true, true
						mset.mu.Unlock()
						return
					}

					// If the responding server doesn't support certain request settings, retry a 'downgraded' request.
					if req.Config.Sourcing && ccr.Error != nil &&
						(ccr.Error.ErrCode == uint16(JSRequiredApiLevelErr) || ccr.Error.ErrCode == uint16(JSInvalidJSONErr)) {
						if apiLevel > 4 {
							apiLevel--
							// Recreate the reply subscription so we don't get stale responses from other servers.
							mset.unsubscribe(crSub)
							if reply, respCh, crSub, err = newReplySubscription(); err != nil {
								si.err = NewJSSourceConsumerSetupFailedError(err, Unless(err))
								retry = true
								mset.mu.Unlock()
								return
							}
							// Retry without stream identity.
							hdr := genHeader(nil, JSRequiredApiLevel, strconv.Itoa(apiLevel))
							var b []byte
							if durableDeliverSubject == _EMPTY_ {
								b, _ = json.Marshal(req)
							}
							mset.outq.send(newJSPubMsg(subject, _EMPTY_, reply, hdr, b, nil, 0))
							mset.mu.Unlock()
							goto SELECT
						}

						// If the responding server doesn't support sourcing consumers, retry without it.
						// Unset for retry.
						req.Config.Sourcing = false
						// Specify a unique consumer name, as the other end will not know to do this.
						req.Config.Name = fmt.Sprintf("JS_SRC_%s_%s", id, createConsumerName())
						b, _ := json.Marshal(req)
						// Regenerate subject since the previous name could've been included in it.
						subject = generateSubject()
						// Recreate the reply subscription so we don't get stale responses from other servers.
						mset.unsubscribe(crSub)
						if reply, respCh, crSub, err = newReplySubscription(); err != nil {
							si.err = NewJSSourceConsumerSetupFailedError(err, Unless(err))
							retry = true
							mset.mu.Unlock()
							return
						}
						mset.outq.send(newJSPubMsg(subject, _EMPTY_, reply, nil, b, nil, 0))
						mset.mu.Unlock()
						goto SELECT
					}
					mset.unsubscribe(crSub)
					// Note: this warning can happen a few times when starting up the server when sourcing streams are
					// defined, this is normal as the streams are re-created in no particular order and it is possible
					// that a stream sourcing another could come up before all of its sources have been recreated.
					cerr := ccr.Error
					if cerr == nil {
						cerr = NewJSSourceConsumerSetupFailedError(errors.New("invalid consumer create response"))
					}
					mset.srv.Warnf("JetStream error response for stream %s create source consumer %s: %+v", mset.cfg.Name, si.name, cerr)
					si.err = cerr
					// Let's retry as soon as possible, but we are gated by sourceConsumerRetryThreshold
					retry = true
					mset.mu.Unlock()
					return
				}

				// If using durable sourcing, we need the consumer to use acks based on flow control.
				if durableDeliverSubject != _EMPTY_ && ccr.ConsumerInfo.Config.AckPolicy != AckFlowControl {
					mset.unsubscribe(crSub)
					si.err = NewJSSourceConsumerRequiresAckFCError()
					retry = true
					mset.mu.Unlock()
					return
				}

				// We can now unsubscribe.
				mset.unsubscribe(crSub)

				// Check if our shared msg queue and go routine is running or not.
				if mset.smsgs == nil {
					qname := fmt.Sprintf("[ACC:%s] stream sources '%s' msgs", mset.acc.Name, mset.cfg.Name)
					mset.smsgs = newIPQueue[*inMsg](mset.srv, qname)
					mset.srv.startGoRoutine(func() { mset.processAllSourceMsgs() },
						pprofLabels{
							"type":    "source",
							"account": mset.acc.Name,
							"stream":  mset.cfg.Name,
						},
					)
				}

				// Capture identity to detect stream recreation.
				recreated := si.rc || (streamIdentity != _EMPTY_ && si.ident != _EMPTY_ && si.ident != streamIdentity)
				if recreated {
					mset.srv.Warnf("JetStream source stream %q for stream '%s > %s' was recreated, resetting source state",
						si.name, mset.acc.Name, mset.cfg.Name)
					si.sseq = 0
				}
				si.ident, si.rc = streamIdentity, false

				// Setup actual subscription to process messages from our source.
				if si.sseq < ccr.ConsumerInfo.Delivered.Stream {
					si.sseq = ccr.ConsumerInfo.Delivered.Stream
				}
				// Capture consumer name.
				si.cname = ccr.ConsumerInfo.Name

				// Do not set si.sseq to seq here. si.sseq will be set in processInboundSourceMsg
				si.dseq, si.cseq = 0, ccr.ConsumerInfo.Delivered.Stream
				si.fcid, si.fcsc = _EMPTY_, 0
				si.qch = make(chan struct{})
				// Set the last seen as now so that we don't fail at the first check.
				si.last.Store(time.Now().UnixNano())

				msgs := mset.smsgs
				if durableDeliverSubject != _EMPTY_ {
					deliverSubject = durableDeliverSubject
				} else {
					deliverSubject = ccr.ConsumerInfo.Config.DeliverSubject
				}
				sub, err := mset.subscribeInternal(deliverSubject, func(sub *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
					hdr, msg := c.msgParts(copyBytes(rmsg)) // Need to copy.
					mset.queueInbound(msgs, subject, reply, hdr, msg, si, nil)
					si.last.Store(time.Now().UnixNano())
				})
				if err != nil {
					si.err = NewJSSourceConsumerSetupFailedError(err, Unless(err))
					retry = true
					mset.mu.Unlock()
					return
				}
				// Save our sub.
				si.sub = sub
			}
			mset.mu.Unlock()
		case <-time.After(timeout):
			mset.unsubscribe(crSub)
			// We already waited long enough, let's retry now.
			retry, timedOut = true, true
		}
	}()
}

// This will process all inbound source msgs.
// We mux them into one go routine to avoid lock contention and high cpu and thread thrashing.
// TODO(dlc) make this more then one and pin sources to one of a group.
func (mset *stream) processAllSourceMsgs() {
	s := mset.srv
	defer s.grWG.Done()

	mset.mu.RLock()
	msgs, qch := mset.smsgs, mset.qch
	mset.mu.RUnlock()

	t := time.NewTicker(sourceHealthCheckInterval)
	defer t.Stop()

	// When we detect we are no longer leader, we will cleanup.
	// Should always return right after this is called.
	cleanUp := func() {
		mset.mu.Lock()
		defer mset.mu.Unlock()
		for _, si := range mset.sources {
			mset.cancelSourceConsumer(si.iname)
		}
		mset.smsgs.drain()
		mset.smsgs.unregister()
		mset.smsgs = nil
	}

	for {
		select {
		case <-s.quitCh:
			return
		case <-qch:
			return
		case <-msgs.ch:
			ims := msgs.pop()
			for _, im := range ims {
				if !mset.processInboundSourceMsg(im.si, im) {
					// If we are no longer leader bail.
					if !mset.IsLeader() {
						msgs.recycle(&ims)
						cleanUp()
						return
					}
					// The batch holds messages from all sources, so only skip this one.
				}
				im.returnToPool()
			}
			msgs.recycle(&ims)
		case <-t.C:
			// If we are no longer leader bail.
			if !mset.IsLeader() {
				cleanUp()
				return
			}

			// Check health of all sources.
			var stalled []*sourceInfo
			mset.mu.RLock()
			for _, si := range mset.sources {
				if time.Since(time.Unix(0, si.last.Load())) > sourceHealthCheckInterval {
					stalled = append(stalled, si)
				}
			}
			numSources := len(mset.sources)
			mset.mu.RUnlock()

			// This can happen on an update when no longer have sources.
			if numSources == 0 {
				cleanUp()
				return
			}

			// We do not want to block here so do in separate Go routine.
			if len(stalled) > 0 {
				go func() {
					mset.mu.Lock()
					defer mset.mu.Unlock()
					for _, si := range stalled {
						mset.setupSourceConsumer(si.iname, si.sseq+1, time.Time{})
					}
				}()
			}
		}
	}
}

// isControlMsg determines if this is a control message.
func (m *inMsg) isControlMsg() bool {
	return len(m.msg) == 0 && len(m.hdr) > 0 && bytes.HasPrefix(m.hdr, []byte("NATS/1.0 100 "))
}

// Sends a reply to a flow control request.
// Lock should be held.
func (mset *stream) sendFlowControlReply(reply string, hdr []byte) {
	if mset.isLeader() && mset.outq != nil {
		dseq := parseInt64(sliceHeader(JSLastConsumerSeq, hdr))
		sseq := parseInt64(sliceHeader(JSLastStreamSeq, hdr))

		// If we're responding to flow control without being delivered messages (for example after a restart),
		// we'll only have the stream sequence.
		if sseq > 0 {
			if dseq < 0 {
				dseq = 0
			}
			const t = "NATS/1.0\r\n%s: %d\r\n%s: %d\r\n\r\n"
			hdr = fmt.Appendf(nil, t, JSLastConsumerSeq, dseq, JSLastStreamSeq, sseq)
			mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		} else {
			mset.outq.sendMsg(reply, nil)
		}
	}
}

// handleFlowControl will properly handle flow control messages for both R==1 and R>1.
// Lock should be held.
func (mset *stream) handleFlowControl(m *inMsg, dseq, sseq uint64) {
	// If we are clustered we will send the flow control message through the replication stack.
	if mset.isClustered() {
		// Append the current delivery and stream sequences, to be sent after replication.
		m.hdr = genHeader(m.hdr, JSLastConsumerSeq, strconv.FormatUint(dseq, 10))
		m.hdr = genHeader(m.hdr, JSLastStreamSeq, strconv.FormatUint(sseq, 10))
		mset.node.Propose(mset.term, encodeStreamMsg(_EMPTY_, m.rply, m.hdr, nil, 0, 0, false))
	} else {
		const t = "NATS/1.0\r\n%s: %d\r\n%s: %d\r\n\r\n"
		hdr := fmt.Appendf(nil, t, JSLastConsumerSeq, dseq, JSLastStreamSeq, sseq)
		mset.outq.send(newJSPubMsg(m.rply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
	}
}

// Number of consecutive heartbeats reporting the same stalled flow control reply
// before we consider our replies to not be reaching the consumer.
const sourceFCStalledThreshold = 3

// processStalledFlowControl sends the flow control reply the consumer reports being stalled on.
// If it keeps reporting the same one, our replies are not reaching it, so report an error.
// Lock should be held.
func (mset *stream) processStalledFlowControl(si *sourceInfo, fcReply string) {
	if fcReply != si.fcid {
		si.fcid, si.fcsc = fcReply, 0
	}
	si.fcsc++
	// Warn exactly once per stall.
	if si.fcsc == sourceFCStalledThreshold {
		si.err = NewJSSourceConsumerFlowControlStalledError()
		kind := "source"
		if si == mset.mirror {
			kind = "mirror"
		}
		mset.srv.Warnf("JetStream stream '%s > %s' %s '%s' is stalled on flow control, replies are not reaching the consumer",
			mset.acc.Name, mset.cfg.Name, kind, si.name)
	}
	mset.outq.sendMsg(fcReply, nil)
}

// clearStalledFlowControl resets stalled flow control tracking once the consumer is no longer stalled.
// Lock should be held.
func (mset *stream) clearStalledFlowControl(si *sourceInfo) {
	if si.fcid == _EMPTY_ {
		return
	}
	si.fcid, si.fcsc = _EMPTY_, 0
	if si.err != nil && si.err.ErrCode == uint16(JSSourceConsumerFlowControlStalledErr) {
		si.err = nil
	}
}

// processInboundSourceMsg handles processing other stream messages bound for this stream.
func (mset *stream) processInboundSourceMsg(si *sourceInfo, m *inMsg) bool {
	mset.mu.Lock()
	// If we are no longer the leader cancel this subscriber.
	if !mset.isLeader() {
		mset.cancelSourceConsumer(si.iname)
		mset.mu.Unlock()
		return false
	}

	isControl := m.isControlMsg()
	cname := consumerFromAckReply(m.rply)

	// Ignore from old subscriptions.
	if !si.isCurrentSub(cname) && !isControl {
		mset.mu.Unlock()
		return false
	}

	// Check for heartbeats and flow control messages.
	if isControl {
		var needsRetry bool
		// Flow controls have reply subjects.
		if m.rply != _EMPTY_ {
			mset.handleFlowControl(m, si.dseq, si.sseq)
		} else {
			// For idle heartbeats make sure we did not miss anything.
			if ldseq := parseInt64(sliceHeader(JSLastConsumerSeq, m.hdr)); ldseq > 0 && uint64(ldseq) != si.dseq {
				needsRetry = true
				mset.retrySourceConsumerAtSeq(si.iname, si.sseq+1)
			} else if fcReply := sliceHeader(JSConsumerStalled, m.hdr); len(fcReply) > 0 {
				// Other side thinks we are stalled, so send flow control reply.
				mset.processStalledFlowControl(si, string(fcReply))
			} else {
				mset.clearStalledFlowControl(si)
			}
		}
		mset.mu.Unlock()
		return !needsRetry
	}

	sseq, dseq, dc, _, pending := ackReplyInfo(m.rply)

	if dc > 1 {
		mset.mu.Unlock()
		return false
	}

	// Tracking is done here.
	osseq, odseq, ocseq := si.sseq, si.dseq, si.cseq
	if sseq <= si.sseq {
		// Ignore older messages.
		// If the deliver sequence matches, we only update delivered accounting.
		// A stale delivery from before a consumer reset could match too, but it won't be above the last counted.
		if dseq == si.dseq+1 && sseq > si.cseq {
			si.dseq++
			si.cseq = sseq
		}
		mset.mu.Unlock()
		return true
	} else if dseq == si.dseq+1 {
		si.dseq++
		si.sseq, si.cseq = sseq, sseq
	} else if dseq > si.dseq && si.cname == _EMPTY_ {
		si.cname = cname
		si.dseq, si.sseq, si.cseq = dseq, sseq, sseq
	} else {
		// Either we missed messages, or this newer message was delivered before one we already counted.
		mset.retrySourceConsumerAtSeq(si.iname, si.sseq+1)
		mset.mu.Unlock()
		return false
	}

	if pending == 0 {
		si.lag = 0
	} else {
		si.lag = pending - 1
	}
	// Receiving messages means we are not stalled on flow control.
	mset.clearStalledFlowControl(si)
	isClustered, ident := mset.isClustered(), si.ident
	mset.mu.Unlock()

	hdr, msg := m.hdr, m.msg

	// If we are daisy chained here make sure to remove the original one.
	if len(hdr) > 0 {
		hdr = removeHeaderIfPresent(hdr, JSStreamSource)
		// Remove any Nats-Expected- headers as we don't want to validate them.
		hdr = removeHeaderIfPrefixPresent(hdr, "Nats-Expected-")
		// Remove any Nats-Batch- headers, batching is not supported when sourcing.
		hdr = removeHeaderIfPrefixPresent(hdr, "Nats-Batch-")
	}
	// Hold onto the origin reply which has all the metadata.
	hdr = genHeader(hdr, JSStreamSource, si.genSourceHeader(m.subj, m.rply, ident))

	// Do the subject transform for the source if there's one
	if len(si.trs) > 0 {
		for _, tr := range si.trs {
			if tr == nil {
				continue
			} else {
				tsubj, err := tr.Match(m.subj)
				if err == nil {
					m.subj = tsubj
					break
				}
			}
		}
	}

	var err error
	// If we are clustered we need to propose this message to the underlying raft group.
	if isClustered {
		err = mset.processClusteredInboundMsg(m.subj, _EMPTY_, hdr, msg, nil, true)
	} else {
		err = mset.processJetStreamMsg(m.subj, _EMPTY_, hdr, msg, 0, 0, nil, true, true)
	}
	if err != nil {
		s := mset.srv
		if strings.Contains(err.Error(), "no space left") {
			s.Errorf("JetStream out of space, will be DISABLED")
			s.ShutdownJetStream()
		} else {
			mset.mu.RLock()
			accName, sname, iName := mset.acc.Name, mset.cfg.Name, si.iname
			mset.mu.RUnlock()
			// Can happen temporarily all the time during normal operations when the sourcing stream is discard new
			// (example use case is for sourcing into a work queue)
			// TODO - Maybe improve sourcing to WQ with limit and new to use flow control rather than re-creating the consumer.
			discardNew := errors.Is(err, ErrMaxMsgs) || errors.Is(err, ErrMaxBytes) || errors.Is(err, ErrMaxMsgsPerSubject)

			// Log some warning for errors.
			if !discardNew && !errors.Is(err, errLastSeqMismatch) && !errors.Is(err, errMsgIdDuplicate) {
				s.RateLimitWarnf("Error processing inbound source %q for '%s' > '%s': %v",
					iName, accName, sname, err)
			}

			// Duplicates can be skipped, continue to the next message.
			if errors.Is(err, errMsgIdDuplicate) {
				return true
			}

			// Do not need to do a full retry that includes finding the last sequence in the stream
			// for that source. Just re-create starting with the seq we couldn't store instead.
			// Especially if we're replicated, we could have inflight proposals that will not be in our store yet.
			mset.mu.Lock()
			si.dseq = odseq
			si.sseq = osseq
			si.cseq = ocseq
			mset.retrySourceConsumerAtSeq(iName, sseq)
			mset.mu.Unlock()
		}
		return false
	}

	return true
}

// Generate a new (2.10) style source header (stream name, sequence number, source filter, source destination transform).
// The source's ident must be passed in, as it can be mutated under the stream lock, which is not held here.
// Format: <stream> <seq> <filter> <transform> <orig> [<ident>]
func (si *sourceInfo) genSourceHeader(orig, reply, ident string) string {
	var b strings.Builder
	iNameParts := strings.Split(si.iname, " ")

	b.WriteString(iNameParts[0])
	b.WriteByte(' ')
	// Grab sequence as text here from reply subject.
	var tsa [expectedNumReplyTokensV2]string
	start, tokens := 0, tsa[:0]
	for i := 0; i < len(reply); i++ {
		if reply[i] == btsep {
			tokens, start = append(tokens, reply[start:i]), i+1
		}
	}
	tokens = append(tokens, reply[start:])
	seq := "1" // Default
	if tokens[0] == "$JS" && tokens[1] == "ACK" {
		if len(tokens) == expectedNumReplyTokensV1 {
			seq = tokens[5]
		} else if len(tokens) >= expectedNumReplyTokensV2 {
			seq = tokens[7]
		}
	}
	b.WriteString(seq)

	b.WriteByte(' ')
	b.WriteString(iNameParts[1])
	b.WriteByte(' ')
	b.WriteString(iNameParts[2])
	b.WriteByte(' ')
	b.WriteString(orig)
	if ident != _EMPTY_ {
		b.WriteByte(' ')
		b.WriteString(ident)
	}
	return b.String()
}

// Original version of header that stored ack reply direct.
func streamAndSeqFromAckReply(reply string) (string, string, uint64) {
	tsa := [expectedNumReplyTokensV2]string{}
	start, tokens := 0, tsa[:0]
	for i := 0; i < len(reply); i++ {
		if reply[i] == btsep {
			tokens, start = append(tokens, reply[start:i]), i+1
		}
	}
	tokens = append(tokens, reply[start:])
	if (len(tokens) != expectedNumReplyTokensV1 && len(tokens) < expectedNumReplyTokensV2) || tokens[0] != "$JS" || tokens[1] != "ACK" {
		return _EMPTY_, _EMPTY_, 0
	}
	offset := 2
	if len(tokens) >= expectedNumReplyTokensV2 {
		offset = 4
	}
	return tokens[offset], _EMPTY_, uint64(parseAckReplyNum(tokens[offset+3]))
}

func consumerFromAckReply(reply string) string {
	tsa := [expectedNumReplyTokensV2]string{}
	start, tokens := 0, tsa[:0]
	for i := 0; i < len(reply); i++ {
		if reply[i] == btsep {
			tokens, start = append(tokens, reply[start:i]), i+1
		}
	}
	tokens = append(tokens, reply[start:])
	if (len(tokens) != expectedNumReplyTokensV1 && len(tokens) < expectedNumReplyTokensV2) || tokens[0] != "$JS" || tokens[1] != "ACK" {
		return _EMPTY_
	}
	offset := 3
	if len(tokens) >= expectedNumReplyTokensV2 {
		offset = 5
	}
	return tokens[offset]
}

// Extract the stream name, the source index name and the message sequence number from the source header.
// Uses the filter and transform arguments to provide backwards compatibility
// Format: <stream> <seq> <filter> <transform> <orig> [<ident>]
func streamAndSeq(shdr string) (string, string, uint64, string) {
	if strings.HasPrefix(shdr, jsAckPre) {
		streamName, iname, seq := streamAndSeqFromAckReply(shdr)
		return streamName, iname, seq, _EMPTY_
	}
	// New version which is stream index name <SPC> sequence
	fields := strings.Split(shdr, " ")
	nFields := len(fields)

	if nFields != 2 && nFields <= 3 {
		return _EMPTY_, _EMPTY_, 0, _EMPTY_
	}

	streamName := fields[0]
	if nFields >= 4 {
		iname := strings.Join([]string{fields[0], fields[2], fields[3]}, " ")
		seq := uint64(parseAckReplyNum(fields[1]))
		var identity string
		if nFields >= 6 {
			// Copy, since it's stored on the sourceInfo.
			identity = copyString(fields[5])
		}
		return streamName, iname, seq, identity
	} else {
		seq := uint64(parseAckReplyNum(fields[1]))
		return streamName, _EMPTY_, seq, _EMPTY_
	}

}

// Lock should be held.
func (mset *stream) setStartingSequenceForSources(iNames map[string]struct{}) {
	// The store contains an index of the source state, use it here without doing lookups ourselves.
	sourcesState := mset.store.SourcesState()
	for iName := range iNames {
		if si, ok := mset.sources[iName]; ok {
			if sss, ok := sourcesState[iName]; ok {
				si.sseq = sss.Seq
				si.dseq = 0
				si.ident, si.rc = sss.Ident, false
			} else {
				// If it doesn't exist, only reset the delivery sequence.
				si.dseq = 0
			}
		}
	}
}

// Resets the SourceInfo for all the sources, seeding each with the
// persisted starting sequence and identity.
// lock should be held.
func (mset *stream) resetSourceInfo() {
	// Reset if needed.
	mset.stopSourceConsumers()
	mset.sources = make(map[string]*sourceInfo)

	// The store contains an index of the source state, use it here without doing lookups ourselves.
	// Seed the source infos with it right away, the actual consumer setup can be delayed and until
	// then we'd otherwise have no idea how far we've sourced already.
	var sourcesState map[string]StreamSourceState
	if mset.store != nil {
		sourcesState = mset.store.SourcesState()
	}

	for _, ssi := range mset.cfg.Sources {
		if ssi.iname == _EMPTY_ {
			ssi.setIndexName()
		}

		var si *sourceInfo

		if len(ssi.SubjectTransforms) == 0 {
			si = &sourceInfo{name: ssi.Name, iname: ssi.iname, sf: ssi.FilterSubject}
		} else {
			sfs := make([]string, len(ssi.SubjectTransforms))
			trs := make([]*subjectTransform, len(ssi.SubjectTransforms))
			for i, str := range ssi.SubjectTransforms {
				tr, err := NewSubjectTransform(str.Source, str.Destination)
				if err != nil {
					mset.srv.Errorf("Unable to get subject transform for source: %v", err)
				}
				sfs[i] = str.Source
				trs[i] = tr
			}
			si = &sourceInfo{name: ssi.Name, iname: ssi.iname, sfs: sfs, trs: trs}
		}
		if sss, ok := sourcesState[ssi.iname]; ok {
			si.sseq, si.ident = sss.Seq, sss.Ident
		}
		mset.sources[ssi.iname] = si
	}
}

// Setup our source consumers.
// Lock should be held.
func (mset *stream) setupSourceConsumers() error {
	if mset.outq == nil {
		return errors.New("outq required")
	}
	// Reset if needed.
	for _, si := range mset.sources {
		if si.sub != nil {
			mset.cancelSourceConsumer(si.iname)
		}
	}

	// If we are no longer the leader, give up
	if !mset.isLeader() {
		return nil
	}

	mset.resetSourceInfo()

	// Setup our consumers at the proper starting position.
	for _, ssi := range mset.cfg.Sources {
		if si := mset.sources[ssi.iname]; si != nil {
			mset.setupSourceConsumer(ssi.iname, si.sseq+1, time.Time{})
		}
	}

	return nil
}

// Will create internal subscriptions for the stream.
// Lock should be held.
func (mset *stream) subscribeToStream() error {
	if mset.active {
		return nil
	}
	for _, subject := range mset.cfg.Subjects {
		if _, err := mset.subscribeInternal(subject, mset.processInboundJetStreamMsg); err != nil {
			return err
		}
	}
	// Check if we need to setup mirroring.
	if mset.cfg.Mirror != nil {
		// setup the initial mirror sourceInfo
		mset.mirror = &sourceInfo{name: mset.cfg.Mirror.Name}
		sfs := make([]string, len(mset.cfg.Mirror.SubjectTransforms))
		trs := make([]*subjectTransform, len(mset.cfg.Mirror.SubjectTransforms))

		for i, tr := range mset.cfg.Mirror.SubjectTransforms {
			// will not fail as already checked before that the transform will work
			subjectTransform, err := NewSubjectTransform(tr.Source, tr.Destination)
			if err != nil {
				mset.srv.Errorf("Unable to get transform for mirror consumer: %v", err)
			}

			sfs[i] = tr.Source
			trs[i] = subjectTransform
		}
		mset.mirror.sfs = sfs
		mset.mirror.trs = trs
		// delay the actual mirror consumer creation for after a delay
		mset.scheduleSetupMirrorConsumerRetry()
	} else if len(mset.cfg.Sources) > 0 && mset.sourcesConsumerSetup == nil {
		// Setup the initial source infos for the sources
		mset.resetSourceInfo()
		// Delay the actual source consumer(s) creation(s) for after a delay if a replicated stream.
		// If it's an R1, this is done at startup and we will do inline.
		if mset.cfg.Replicas == 1 {
			mset.setupSourceConsumers()
		} else {
			mset.sourcesConsumerSetup = time.AfterFunc(time.Duration(rand.IntN(int(500*time.Millisecond)))+100*time.Millisecond, func() {
				mset.mu.Lock()
				mset.setupSourceConsumers()
				mset.mu.Unlock()
			})
		}
	}
	// Check for direct get access.
	// We spin up followers for clustered streams in monitorStream().
	if mset.cfg.AllowDirect {
		if err := mset.subscribeToDirect(); err != nil {
			return err
		}
	}

	mset.active = true
	return nil
}

// Lock should be held.
func (mset *stream) subscribeToDirect() error {
	// We will make this listen on a queue group by default, which can allow mirrors to participate on opt-in basis.
	if mset.directSub == nil {
		dsubj := fmt.Sprintf(JSDirectMsgGetT, mset.cfg.Name)
		if sub, err := mset.queueSubscribeInternal(dsubj, dgetGroup, mset.processDirectGetRequest); err == nil {
			mset.directSub = sub
		} else {
			return err
		}
	}
	// Now the one that will have subject appended past stream name.
	if mset.lastBySub == nil {
		dsubj := fmt.Sprintf(JSDirectGetLastBySubjectT, mset.cfg.Name, fwcs)
		// We will make this listen on a queue group by default, which can allow mirrors to participate on opt-in basis.
		if sub, err := mset.queueSubscribeInternal(dsubj, dgetGroup, mset.processDirectGetLastBySubjectRequest); err == nil {
			mset.lastBySub = sub
		} else {
			return err
		}
	}

	return nil
}

// Lock should be held.
func (mset *stream) unsubscribeToDirect() {
	if mset.directSub != nil {
		mset.unsubscribe(mset.directSub)
		mset.directSub = nil
	}
	if mset.lastBySub != nil {
		mset.unsubscribe(mset.lastBySub)
		mset.lastBySub = nil
	}
}

// Lock should be held.
func (mset *stream) subscribeToMirrorDirect() error {
	if mset.cfg.Mirror == nil {
		return nil
	}
	mirrorName := mset.cfg.Mirror.Name

	// We will make this listen on a queue group by default, which can allow mirrors to participate on opt-in basis.
	if mset.mirrorDirectSub == nil {
		dsubj := fmt.Sprintf(JSDirectMsgGetT, mirrorName)
		// We will make this listen on a queue group by default, which can allow mirrors to participate on opt-in basis.
		if sub, err := mset.queueSubscribeInternal(dsubj, dgetGroup, mset.processDirectGetRequest); err == nil {
			mset.mirrorDirectSub = sub
		} else {
			return err
		}
	}
	// Now the one that will have subject appended past stream name.
	if mset.mirrorLastBySub == nil {
		dsubj := fmt.Sprintf(JSDirectGetLastBySubjectT, mirrorName, fwcs)
		// We will make this listen on a queue group by default, which can allow mirrors to participate on opt-in basis.
		if sub, err := mset.queueSubscribeInternal(dsubj, dgetGroup, mset.processDirectGetLastBySubjectRequest); err == nil {
			mset.mirrorLastBySub = sub
		} else {
			return err
		}
	}

	return nil
}

// Lock should be held.
func (mset *stream) unsubscribeToMirrorDirect() {
	if mset.mirrorDirectSub != nil {
		mset.unsubscribe(mset.mirrorDirectSub)
		mset.mirrorDirectSub = nil
	}
	if mset.mirrorLastBySub != nil {
		mset.unsubscribe(mset.mirrorLastBySub)
		mset.mirrorLastBySub = nil
	}
}

// Stop our source consumers.
// Lock should be held.
func (mset *stream) stopSourceConsumers() {
	for _, si := range mset.sources {
		mset.cancelSourceInfo(si)
	}
}

// Lock should be held.
func (mset *stream) removeInternalConsumer(si *sourceInfo) {
	if si == nil || si.cname == _EMPTY_ {
		return
	}
	si.cname = _EMPTY_
}

// Will unsubscribe from the stream.
// Lock should be held.
func (mset *stream) unsubscribeToStream(stopping, shuttingDown bool) error {
	for _, subject := range mset.cfg.Subjects {
		mset.unsubscribeInternal(subject)
	}
	if mset.mirror != nil {
		mset.cancelSourceInfo(mset.mirror)
		mset.mirror = nil
	}

	if len(mset.sources) > 0 {
		mset.stopSourceConsumers()
		mset.sources = nil
	}
	// Clear batching state.
	mset.deleteAtomicBatches(shuttingDown)
	if stopping || shuttingDown {
		mset.deleteFastBatches()
	}
	if mset.batches != nil {
		mset.batches.mu.Lock()
		reset := len(mset.batches.atomic) == 0 && len(mset.batches.fast) == 0
		mset.batches.mu.Unlock()
		if reset {
			mset.batches = nil
		}
	}

	if stopping {
		// In case we had a direct get subscriptions.
		mset.unsubscribeToDirect()
		mset.unsubscribeToMirrorDirect()
	}

	if mset.directLeaderSub == nil {
		// Always unsubscribe the leader sub.
		mset.unsubscribe(mset.directLeaderSub)
		mset.directLeaderSub = nil
	}

	mset.active = false
	return nil
}

// Lock should be held.
func (mset *stream) deleteAtomicBatches(shuttingDown bool) {
	if mset.batches != nil {
		mset.batches.mu.Lock()
		for batchId, b := range mset.batches.atomic {
			// If shutting down, do fixup during startup. In-memory batches don't require manual cleanup.
			if shuttingDown {
				b.stopLocked()
			} else {
				b.cleanupLocked(batchId, mset.batches)
			}
		}
		mset.batches.atomic = nil
		mset.batches.mu.Unlock()
	}
}

// Lock should be held.
func (mset *stream) deleteFastBatches() {
	if mset.batches != nil {
		mset.batches.mu.Lock()
		for batchId, b := range mset.batches.fast {
			b.cleanupLocked(batchId, mset.batches)
		}
		mset.batches.fast = nil
		mset.batches.mu.Unlock()
	}
}

// Lock does NOT need to be held, we set the client on setup and never change it at this point.
func (mset *stream) subscribeInternal(subject string, cb msgHandler) (*subscription, error) {
	if mset.closed.Load() {
		return nil, errStreamClosed
	}
	if cb == nil {
		return nil, errInvalidMsgHandler
	}
	c := mset.client
	sid := int(mset.sid.Add(1))
	// Now create the subscription
	return c.processSub([]byte(subject), nil, []byte(strconv.Itoa(sid)), cb, false)
}

// Lock does NOT need to be held, we set the client on setup and never change it at this point.
func (mset *stream) queueSubscribeInternal(subject, group string, cb msgHandler) (*subscription, error) {
	if mset.closed.Load() {
		return nil, errStreamClosed
	}
	if cb == nil {
		return nil, errInvalidMsgHandler
	}
	c := mset.client
	sid := int(mset.sid.Add(1))
	// Now create the subscription
	return c.processSub([]byte(subject), []byte(group), []byte(strconv.Itoa(sid)), cb, false)
}

// This will unsubscribe us from the exact subject given.
// We do not currently track the subs so do not have the sid.
// This should be called only on an update.
// Lock does NOT need to be held, we set the client on setup and never change it at this point.
func (mset *stream) unsubscribeInternal(subject string) error {
	if mset.closed.Load() {
		return errStreamClosed
	}
	c := mset.client
	var sid []byte
	c.mu.Lock()
	for _, sub := range c.subs {
		if subject == string(sub.subject) {
			sid = sub.sid
			break
		}
	}
	c.mu.Unlock()

	if sid != nil {
		return c.processUnsub(sid)
	}
	return nil
}

func (mset *stream) unsubscribe(sub *subscription) {
	if sub == nil || mset.closed.Load() {
		return
	}
	mset.client.processUnsub(sub.sid)
}

func (mset *stream) setupStore(fsCfg *FileStoreConfig, recovering bool) error {
	mset.mu.Lock()
	switch mset.cfg.Storage {
	case MemoryStorage:
		ms, err := newMemStoreWithMode(&mset.cfg, recovering)
		if err != nil {
			mset.mu.Unlock()
			return err
		}
		mset.store = ms
	case FileStorage:
		s := mset.srv
		prf := s.jsKeyGen(s.getOpts().JetStreamKey, mset.acc.Name)
		if prf != nil {
			// We are encrypted here, fill in correct cipher selection.
			fsCfg.Cipher = s.getOpts().JetStreamCipher
		}
		oldprf := s.jsKeyGen(s.getOpts().JetStreamOldKey, mset.acc.Name)
		cfg := *fsCfg
		cfg.srv = s
		fs, err := newFileStoreWithCreatedAndMode(cfg, mset.cfg, mset.created, prf, oldprf, recovering)
		if err != nil {
			mset.mu.Unlock()
			return err
		}
		mset.store = fs
	}
	// This will fire the callback but we do not require the lock since md will be 0 here.
	mset.store.RegisterStorageUpdates(mset.storeUpdates)
	mset.store.RegisterStorageRemoveMsg(func(seq uint64) {
		if mset.IsClustered() {
			mset.mu.RLock()
			if mset.isLeader() {
				md := streamMsgDelete{Seq: seq, NoErase: true, Stream: mset.cfg.Name}
				mset.node.Propose(mset.term, encodeMsgDelete(&md))
			}
			mset.mu.RUnlock()
		} else {
			mset.removeMsg(seq)
		}
	})
	mset.store.RegisterProcessJetStreamMsg(func(im *inMsg) {
		if mset.IsClustered() {
			if mset.IsLeader() {
				mset.processClusteredInboundMsg(im.subj, im.rply, im.hdr, im.msg, im.mt, true)
			}
		} else {
			mset.processJetStreamMsg(im.subj, im.rply, im.hdr, im.msg, 0, 0, im.mt, true, true)
		}
	})
	mset.mu.Unlock()

	return nil
}

// Called for any updates to the underlying stream. We pass through the bytes to the
// jetstream account. We do local processing for stream pending for consumers, but only
// for removals.
// Lock should not be held.
func (mset *stream) storeUpdates(md, bd int64, seq uint64, subj string) {
	// If we have a single negative update then we will process our consumers for stream pending.
	// Purge and Store handled separately inside individual calls.
	if md == -1 && seq > 0 && subj != _EMPTY_ {
		// We use our consumer list mutex here instead of the main stream lock since it may be held already.
		mset.clsMu.RLock()
		if mset.csl != nil {
			mset.csl.Match(subj, func(o *consumer) {
				o.decStreamPending(seq, subj)
			})
		} else {
			for _, o := range mset.cList {
				o.decStreamPending(seq, subj)
			}
		}
		mset.clsMu.RUnlock()
	} else if md < 0 {
		// Batch decrements we need to force consumers to re-calculate num pending.
		var ss StreamState
		mset.store.FastState(&ss)
		mset.clsMu.RLock()
		for _, o := range mset.cList {
			o.streamNumPendingLocked()
			o.removeRedeliveredBelow(ss.FirstSeq)
		}
		mset.clsMu.RUnlock()
	}

	if mset.jsa != nil {
		mset.jsa.updateUsage(mset.tier, mset.stype, bd)
	}
}

// NumMsgIds returns the number of message ids being tracked for duplicate suppression.
func (mset *stream) numMsgIds() int {
	mset.ddMu.Lock()
	defer mset.ddMu.Unlock()
	return len(mset.ddmap)
}

// checkMsgId will process and check for duplicates.
// mset.ddMu lock should be held.
func (mset *stream) checkMsgId(id string) *ddentry {
	if id == _EMPTY_ || len(mset.ddmap) == 0 {
		return nil
	}
	return mset.ddmap[id]
}

// Will purge the entries that are past the window.
// Should be called from a timer.
func (mset *stream) purgeMsgIds() {
	now := time.Now().UnixNano()
	mset.ddMu.Lock()
	defer mset.ddMu.Unlock()
	tmrNext := mset.ddwin
	window := int64(tmrNext)

	for i, dde := range mset.ddarr[mset.ddindex:] {
		if now-dde.ts >= window {
			delete(mset.ddmap, dde.id)
		} else {
			mset.ddindex += i
			// Check if we should garbage collect here if we are 1/3 total size.
			if cap(mset.ddarr) > 3*(len(mset.ddarr)-mset.ddindex) {
				mset.ddarr = append([]*ddentry(nil), mset.ddarr[mset.ddindex:]...)
				mset.ddindex = 0
			}
			tmrNext = time.Duration(window - (now - dde.ts))
			break
		}
	}
	if len(mset.ddmap) > 0 {
		// Make sure to not fire too quick
		const minFire = 50 * time.Millisecond
		if tmrNext < minFire {
			tmrNext = minFire
		}
		if mset.ddtmr != nil {
			mset.ddtmr.Reset(tmrNext)
		} else {
			mset.ddtmr = time.AfterFunc(tmrNext, mset.purgeMsgIds)
		}
	} else {
		if mset.ddtmr != nil {
			mset.ddtmr.Stop()
			mset.ddtmr = nil
		}
		mset.ddmap = nil
		mset.ddarr = nil
		mset.ddindex = 0
	}
}

// storeMsgId will store the message id for duplicate detection.
func (mset *stream) storeMsgId(dde *ddentry) {
	mset.ddMu.Lock()
	defer mset.ddMu.Unlock()
	mset.storeMsgIdLocked(dde)
}

// storeMsgIdLocked will store the message id for duplicate detection.
// mset.ddMu lock should be held.
func (mset *stream) storeMsgIdLocked(dde *ddentry) {
	// Zero means disabled.
	if mset.ddwin <= 0 {
		return
	}
	if mset.ddmap == nil {
		mset.ddmap = make(map[string]*ddentry)
	}
	mset.ddmap[dde.id] = dde
	mset.ddarr = append(mset.ddarr, dde)
	if mset.ddtmr == nil {
		mset.ddtmr = time.AfterFunc(mset.ddwin, mset.purgeMsgIds)
	}
}

// Fast lookup of msgId.
func getMsgId(hdr []byte) string {
	return string(getHeader(JSMsgId, hdr))
}

// Fast lookup of expected last msgId.
func getExpectedLastMsgId(hdr []byte) string {
	return string(getHeader(JSExpectedLastMsgId, hdr))
}

// Fast lookup of expected stream.
func getExpectedStream(hdr []byte) string {
	return string(getHeader(JSExpectedStream, hdr))
}

// Fast lookup of expected last sequence.
func getExpectedLastSeq(hdr []byte) (uint64, bool) {
	bseq := sliceHeader(JSExpectedLastSeq, hdr)
	if len(bseq) == 0 {
		return 0, false
	}
	return uint64(parseInt64(bseq)), true
}

// Fast lookup of rollups.
func getRollup(hdr []byte) string {
	r := getHeader(JSMsgRollup, hdr)
	if len(r) == 0 {
		return _EMPTY_
	}
	return strings.ToLower(string(r))
}

// Fast lookup of expected stream sequence per subject.
func getExpectedLastSeqPerSubject(hdr []byte) (uint64, bool) {
	bseq := sliceHeader(JSExpectedLastSubjSeq, hdr)
	if len(bseq) == 0 {
		return 0, false
	}
	return uint64(parseInt64(bseq)), true
}

// Fast lookup of expected subject for the expected stream sequence per subject.
func getExpectedLastSeqPerSubjectForSubject(hdr []byte) string {
	return bytesToString(sliceHeader(JSExpectedLastSubjSeqSubj, hdr))
}

// Fast lookup of the message TTL from headers:
// - Positive return value: duration in seconds.
// - Zero return value: no TTL or parse error.
// - Negative return value: never expires.
func getMessageTTL(hdr []byte) (int64, error) {
	ttl := getHeader(JSMessageTTL, hdr)
	if len(ttl) == 0 {
		return 0, nil
	}
	return parseMessageTTL(bytesToString(ttl))
}

// - Positive return value: duration in seconds.
// - Zero return value: no TTL or parse error.
// - Negative return value: never expires.
func parseMessageTTL(ttl string) (int64, error) {
	if strings.ToLower(ttl) == "never" {
		return -1, nil
	}
	dur, err := time.ParseDuration(ttl)
	if err == nil {
		if dur < time.Second {
			return 0, NewJSMessageTTLInvalidError()
		}
		return int64(dur.Seconds()), nil
	}
	t := parseInt64(stringToBytes(ttl))
	if t < 0 {
		// This probably means a parse failure, hence why
		// we have a special case "never" for returning -1.
		// Otherwise we can't know if it's a genuine TTL
		// that says never expire or if it's a parse error.
		return 0, NewJSMessageTTLInvalidError()
	}
	return t, nil
}

// Fast lookup of the message Incr from headers.
// Return includes the value or nil, and success.
func getMessageIncr(hdr []byte) (*big.Int, bool) {
	incr := sliceHeader(JSMessageIncr, hdr)
	if len(incr) == 0 {
		return nil, true
	}
	var v big.Int
	return v.SetString(bytesToString(incr), 10)
}

// Fast lookup of message schedule.
func getMessageSchedule(hdr []byte) (time.Time, *ApiError) {
	if len(hdr) == 0 {
		return time.Time{}, nil
	}
	return nextMessageSchedule(hdr, time.Now().UTC().UnixNano())
}

// Fast lookup and calculation of next message schedule.
func nextMessageSchedule(hdr []byte, ts int64) (time.Time, *ApiError) {
	if len(hdr) == 0 {
		return time.Time{}, nil
	}
	loc, apiErr := loadMessageScheduleLocation(hdr)
	if apiErr != nil {
		return time.Time{}, apiErr
	}
	val := bytesToString(sliceHeader(JSSchedulePattern, hdr))
	schedule, _, ok := parseMsgSchedule(val, loc, ts)
	if !ok {
		return time.Time{}, NewJSMessageSchedulesPatternInvalidError()
	}
	return schedule, nil
}

// loadMessageScheduleLocation returns the *time.Location for the schedule's
// time zone header. Returns nil loc when the header is absent. A header that
// is present but empty or names an unknown zone yields a TimeZoneInvalid error.
func loadMessageScheduleLocation(hdr []byte) (*time.Location, *ApiError) {
	tz := sliceHeader(JSScheduleTimeZone, hdr)
	if tz == nil {
		return nil, nil
	}
	if len(tz) == 0 {
		return nil, NewJSMessageSchedulesTimeZoneInvalidError()
	}
	loc, err := time.LoadLocation(bytesToString(tz))
	if err != nil {
		return nil, NewJSMessageSchedulesTimeZoneInvalidError()
	}
	return loc, nil
}

// Fast lookup of the message schedule TTL from headers.
// The TTL is confirmed to be valid, but the raw TTL string is returned.
func getMessageScheduleTTL(hdr []byte) (string, bool) {
	ttl := getHeader(JSScheduleTTL, hdr)
	if len(ttl) == 0 {
		return _EMPTY_, true
	}
	if _, err := parseMessageTTL(bytesToString(ttl)); err != nil {
		return _EMPTY_, false
	}
	return string(ttl), true
}

// Fast lookup of the message schedule rollup from headers.
func getMessageScheduleRollup(hdr []byte) string {
	return string(sliceHeader(JSScheduleRollup, hdr))
}

// Fast lookup of message schedule target.
func getMessageScheduleTarget(hdr []byte) string {
	if len(hdr) == 0 {
		return _EMPTY_
	}
	return string(getHeader(JSScheduleTarget, hdr))
}

// Fast lookup of message schedule source.
func getMessageScheduleSource(hdr []byte) string {
	if len(hdr) == 0 {
		return _EMPTY_
	}
	return string(getHeader(JSScheduleSource, hdr))
}

// Fast lookup of message scheduler.
func getMessageScheduler(hdr []byte) string {
	if len(hdr) == 0 {
		return _EMPTY_
	}
	return string(getHeader(JSScheduler, hdr))
}

// Fast lookup of batch ID.
func getBatchId(hdr []byte) string {
	if len(hdr) == 0 {
		return _EMPTY_
	}
	if atomicBatchId := sliceHeader(JSBatchId, hdr); atomicBatchId != nil {
		return string(atomicBatchId)
	}
	return _EMPTY_
}

type FastBatch struct {
	id        string
	seq       uint64
	flow      uint16
	ping      bool
	gapOk     bool
	commit    bool
	commitEob bool
}

const (
	FastBatchSuffix  = ".$FI"
	FastBatchGapFail = "fail"
	FastBatchGapOk   = "ok"
)

const (
	FastBatchOpStart = iota
	FastBatchOpAppend
	FastBatchOpCommit
	FastBatchOpCommitEob
	FastBatchOpPing
)

var fastBatchPool sync.Pool

func getFastBatchFromPool() *FastBatch {
	idx := fastBatchPool.Get()
	if idx != nil {
		return idx.(*FastBatch)
	}
	return new(FastBatch)
}

func (b *FastBatch) returnToPool() {
	if b == nil {
		return
	}
	// Nil out all values.
	*b = FastBatch{}
	fastBatchPool.Put(b)
}

// getFastBatch gets fast batch info from the reply subject in the form:
// <prefix>.<uuid>.<initial flow>.<gap mode>.<batch seq>.<operation>.$FI
func getFastBatch(reply string, hdr []byte) (*FastBatch, bool) {
	lreply := len(reply)
	if lreply <= 4 || reply[lreply-4:] != FastBatchSuffix {
		if !isServiceReply(stringToBytes(reply)) {
			return nil, false
		}
		// If account imports/exports are used, the reply might be internal.
		// Check the client header for the original reply subject.
		ci := sliceHeader(ClientInfoHdr, hdr)
		if ci == nil {
			return nil, false
		}
		var cis ClientInfo
		if err := json.Unmarshal(ci, &cis); err != nil || cis.Reply == _EMPTY_ {
			return nil, false
		}
		reply = cis.Reply
		lreply = len(reply)
		if lreply <= 4 || reply[lreply-4:] != FastBatchSuffix {
			return nil, false
		}
	}

	n := lreply - 4 // Move to just before the dot
	o := strings.LastIndexByte(reply[:n], '.')
	if o == -1 {
		return nil, true
	}
	// Batch operation.
	ops := reply[o+1 : n]
	op := parseInt64(stringToBytes(ops))
	if op < FastBatchOpStart || op > FastBatchOpPing {
		return nil, true
	}

	b := getFastBatchFromPool()
	b.ping = op == FastBatchOpPing
	b.commitEob = op == FastBatchOpCommitEob
	b.commit = b.commitEob || op == FastBatchOpCommit
	p := o

	// Batch seq.
	if o = strings.LastIndexByte(reply[:o], '.'); o == -1 {
		return nil, true
	}
	seq, ok := parseUint64(stringToBytes(reply[o+1 : p]))
	// Reject math.MaxUint64 to prevent b.lseq overflowing on the next b.lseq++.
	if !ok || seq == 0 || seq == math.MaxUint64 {
		return nil, true
	}
	b.seq = seq
	p = o
	if b.seq == 1 && b.commitEob {
		return nil, true
	}
	if op == FastBatchOpStart && b.seq != 1 {
		return nil, true
	} else if op == FastBatchOpAppend && b.seq <= 1 {
		return nil, true
	}

	// Gap mode.
	if o = strings.LastIndexByte(reply[:o], '.'); o == -1 {
		return nil, true
	}
	gapMode := reply[o+1 : p]
	if gapMode != FastBatchGapFail && gapMode != FastBatchGapOk {
		return nil, true // Not recognized.
	}
	b.gapOk = gapMode == FastBatchGapOk
	p = o

	// Ack flow.
	if o = strings.LastIndexByte(reply[:o], '.'); o == -1 {
		return nil, true
	}
	a := parseInt64(stringToBytes(reply[o+1 : p]))
	if a <= 0 {
		a = 10
	} else if a > math.MaxUint16 {
		a = math.MaxUint16
	}
	b.flow = uint16(a)
	p = o

	// Batch id.
	if o = strings.LastIndexByte(reply[:o], '.'); o == -1 {
		return nil, true
	}
	b.id = reply[o+1 : p]
	return b, false
}

// Fast lookup of batch sequence.
func getBatchSequence(hdr []byte) (uint64, bool) {
	bseq := sliceHeader(JSBatchSeq, hdr)
	if len(bseq) == 0 {
		return 0, false
	}
	return parseUint64(bseq)
}

// Signal if we are clustered. Will acquire rlock.
func (mset *stream) IsClustered() bool {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.isClustered()
}

// Lock should be held.
func (mset *stream) isClustered() bool {
	return mset.node != nil
}

// Used if we have to queue things internally to avoid the route/gw path.
type inMsg struct {
	subj string
	rply string
	hdr  []byte
	msg  []byte
	si   *sourceInfo
	mt   *msgTrace
	seq  uint64 // seq that can be optionally used for sorting out-of-band.
}

var inMsgPool = sync.Pool{
	New: func() any {
		return &inMsg{}
	},
}

func (im *inMsg) returnToPool() {
	im.subj, im.rply, im.hdr, im.msg, im.si, im.mt = _EMPTY_, _EMPTY_, nil, nil, nil, nil
	inMsgPool.Put(im)
}

func (mset *stream) queueInbound(ib *ipQueue[*inMsg], subj, rply string, hdr, msg []byte, si *sourceInfo, mt *msgTrace) {
	im := inMsgPool.Get().(*inMsg)
	im.subj, im.rply, im.hdr, im.msg, im.si, im.mt = subj, rply, hdr, msg, si, mt
	if _, err := ib.push(im); err != nil {
		im.returnToPool()
		streamName := mset.getCfgName()
		mset.srv.RateLimitWarnf("Dropping messages due to excessive stream ingest rate on '%s' > '%s': %s", mset.acc.Name, streamName, err)
		if rply != _EMPTY_ {
			hdr := []byte("NATS/1.0 429 Too Many Requests\r\n\r\n")
			b, _ := json.Marshal(&JSPubAckResponse{PubAck: &PubAck{Stream: streamName}, Error: NewJSStreamTooManyRequestsError()})
			mset.outq.send(newJSPubMsg(rply, _EMPTY_, _EMPTY_, hdr, b, nil, 0))
		}
	}
}

var dgPool = sync.Pool{
	New: func() any {
		return &directGetReq{}
	},
}

// For when we need to not inline the request.
type directGetReq struct {
	// Copy of this is correct for this.
	req   JSApiMsgGetRequest
	reply string
}

// processDirectGetRequest handles direct get request for stream messages.
func (mset *stream) processDirectGetRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	if len(reply) == 0 {
		return
	}
	hdr, msg := c.msgParts(rmsg)
	if errorOnRequiredApiLevel(hdr) {
		hdr := []byte("NATS/1.0 412 Required Api Level\r\n\r\n")
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}
	if len(msg) == 0 {
		hdr := []byte("NATS/1.0 408 Empty Request\r\n\r\n")
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}
	var req JSApiMsgGetRequest
	err := json.Unmarshal(msg, &req)
	if err != nil {
		hdr := []byte("NATS/1.0 408 Malformed Request\r\n\r\n")
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}
	// Check if nothing set.
	if req.Seq == 0 && req.LastFor == _EMPTY_ && req.NextFor == _EMPTY_ && len(req.MultiLastFor) == 0 && req.StartTime == nil {
		hdr := []byte("NATS/1.0 408 Empty Request\r\n\r\n")
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}
	// Check we don't have conflicting options set.
	// We do not allow batch mode for lastFor requests.
	if (req.Seq > 0 && req.LastFor != _EMPTY_) ||
		(req.Seq > 0 && req.StartTime != nil) ||
		(req.StartTime != nil && req.LastFor != _EMPTY_) ||
		(req.LastFor != _EMPTY_ && req.NextFor != _EMPTY_) ||
		(req.LastFor != _EMPTY_ && req.Batch > 0) ||
		(req.LastFor != _EMPTY_ && len(req.MultiLastFor) > 0) ||
		(req.NextFor != _EMPTY_ && len(req.MultiLastFor) > 0) ||
		(req.UpToSeq > 0 && req.UpToTime != nil) {
		hdr := []byte("NATS/1.0 408 Bad Request\r\n\r\n")
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}

	inlineOk := c.kind != ROUTER && c.kind != GATEWAY && c.kind != LEAF
	if !inlineOk {
		dg := dgPool.Get().(*directGetReq)
		dg.req, dg.reply = req, reply
		mset.gets.push(dg)
	} else {
		mset.getDirectRequest(&req, reply)
	}
}

// This is for direct get by last subject which is part of the subject itself.
func (mset *stream) processDirectGetLastBySubjectRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	if len(reply) == 0 {
		return
	}
	hdr, msg := c.msgParts(rmsg)
	if errorOnRequiredApiLevel(hdr) {
		hdr := []byte("NATS/1.0 412 Required Api Level\r\n\r\n")
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}

	var req JSApiMsgGetRequest
	if len(msg) > 0 {
		err := json.Unmarshal(msg, &req)
		if err != nil {
			hdr := []byte("NATS/1.0 408 Malformed Request\r\n\r\n")
			mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
			return
		}
	}

	// Extract the key.
	var key string
	for i, n := 0, 0; i < len(subject); i++ {
		if subject[i] == btsep {
			if n == 4 {
				if start := i + 1; start < len(subject) {
					key = subject[i+1:]
				}
				break
			}
			n++
		}
	}
	if len(key) == 0 {
		hdr := []byte("NATS/1.0 408 Bad Request\r\n\r\n")
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}

	// Error if request fields are provided that don't make sense for this endpoint.
	switch {
	case req.LastFor != _EMPTY_, req.NextFor != _EMPTY_,
		len(req.MultiLastFor) > 0, req.Seq != 0, req.Batch != 0,
		req.StartTime != nil, req.UpToSeq != 0, req.UpToTime != nil:
		hdr := []byte("NATS/1.0 408 Bad Request\r\n\r\n")
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	default:
		req.LastFor = key
	}

	inlineOk := c.kind != ROUTER && c.kind != GATEWAY && c.kind != LEAF
	if !inlineOk {
		dg := dgPool.Get().(*directGetReq)
		dg.req, dg.reply = req, reply
		mset.gets.push(dg)
	} else {
		mset.getDirectRequest(&req, reply)
	}
}

// For direct get batch and multi requests.
const (
	dg   = "NATS/1.0\r\nNats-Stream: %s\r\nNats-Subject: %s\r\nNats-Sequence: %d\r\nNats-Time-Stamp: %s\r\n\r\n"
	dgb  = "NATS/1.0\r\nNats-Stream: %s\r\nNats-Subject: %s\r\nNats-Sequence: %d\r\nNats-Time-Stamp: %s\r\nNats-Num-Pending: %d\r\nNats-Last-Sequence: %d\r\n\r\n"
	eob  = "NATS/1.0 204 EOB\r\nNats-Num-Pending: %d\r\nNats-Last-Sequence: %d\r\n\r\n"
	eobm = "NATS/1.0 204 EOB\r\nNats-Num-Pending: %d\r\nNats-Last-Sequence: %d\r\nNats-UpTo-Sequence: %d\r\n\r\n"
)

// Handle a multi request.
func (mset *stream) getDirectMulti(req *JSApiMsgGetRequest, reply string) {
	// TODO(dlc) - Make configurable?
	const maxAllowedResponses = 1024

	store, s := mset.store, mset.srv
	name := mset.getCfgName()

	// Ensure this read request is isolated and doesn't interleave with writes.
	mset.isolateMu.RLock()
	defer mset.isolateMu.RUnlock()

	// Grab MaxBytes
	mb := req.MaxBytes
	if mb == 0 && s != nil {
		// Fill in with the server's MaxPending.
		mb = int(s.opts.MaxPending)
	}

	upToSeq := req.UpToSeq
	// If we have UpToTime set get the proper sequence.
	if req.UpToTime != nil {
		upToSeq = store.GetSeqFromTime((*req.UpToTime).UTC())
		var state StreamState
		store.FastState(&state)
		// Avoid selecting a first sequence that will take us to before the stream first
		// sequence, otherwise we can return messages after the supplied UpToTime.
		if upToSeq <= state.FirstSeq {
			hdr := []byte("NATS/1.0 404 No Results\r\n\r\n")
			mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
			return
		}
		// We need to back off one since this is used to determine start sequence normally,
		// whereas here we want it to be the ceiling.
		upToSeq--
	}
	// If not set, set to the last sequence and remember that for EOB.
	if upToSeq == 0 {
		var state StreamState
		store.FastState(&state)
		upToSeq = state.LastSeq
	}

	var lseq uint64
	var sentBytes, sent int
	total, np, err := store.MultiLastMsgs(req.MultiLastFor, req.Seq, upToSeq, maxAllowedResponses, func(sm *StoreMsg, np uint64) bool {
		hdr := sm.hdr
		ts := time.Unix(0, sm.ts).UTC()

		if len(hdr) == 0 {
			hdr = fmt.Appendf(nil, dgb, name, sm.subj, sm.seq, ts.Format(time.RFC3339Nano), np, lseq)
		} else {
			hdr = copyBytes(hdr)
			hdr = genHeader(hdr, JSStream, name)
			hdr = genHeader(hdr, JSSubject, sm.subj)
			hdr = genHeader(hdr, JSSequence, strconv.FormatUint(sm.seq, 10))
			hdr = genHeader(hdr, JSTimeStamp, ts.Format(time.RFC3339Nano))
			hdr = genHeader(hdr, JSNumPending, strconv.FormatUint(np, 10))
			hdr = genHeader(hdr, JSLastSequence, strconv.FormatUint(lseq, 10))
		}
		// Track our lseq
		lseq = sm.seq
		// Send out our message.
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, sm.msg, nil, 0))
		// Check if we have exceeded max bytes.
		sentBytes += len(sm.subj) + len(sm.hdr) + len(sm.msg)
		if sentBytes >= mb {
			return false
		}
		sent++
		if req.Batch > 0 && sent >= req.Batch {
			return false
		}
		return true
	})
	if err != nil {
		var hdr []byte
		switch {
		case err == ErrTooManyResults:
			hdr = []byte("NATS/1.0 413 Too Many Results\r\n\r\n")
		case total > 0:
			// A resolved message failed to load. Errors are folded into a 404
			// regardless of cause, so internal errors don't leak to clients.
			hdr = []byte("NATS/1.0 404 Message Not Found\r\n\r\n")
		default:
			hdr = []byte(fmt.Sprintf("NATS/1.0 500 %v\r\n\r\n", err))
		}
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}
	if total == 0 {
		hdr := []byte("NATS/1.0 404 No Results\r\n\r\n")
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}

	// Send out EOB
	hdr := fmt.Appendf(nil, eobm, np, lseq, upToSeq)
	mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
}

// Do actual work on a direct msg request.
// This could be called in a Go routine if we are inline for a non-client connection.
func (mset *stream) getDirectRequest(req *JSApiMsgGetRequest, reply string) {
	// Handle multi in separate function.
	if len(req.MultiLastFor) > 0 {
		mset.getDirectMulti(req, reply)
		return
	}

	store, s := mset.store, mset.srv
	name := mset.getCfgName()

	// Ensure this read request is isolated and doesn't interleave with writes.
	mset.isolateMu.RLock()
	defer mset.isolateMu.RUnlock()

	var seq uint64
	// Lookup start seq if AsOfTime is set.
	if req.StartTime != nil {
		seq = store.GetSeqFromTime(*req.StartTime)
	} else {
		seq = req.Seq
	}

	wc := subjectHasWildcard(req.NextFor)
	// For tracking num pending if we are batch.
	var np, lseq, validThrough uint64
	var isBatchRequest bool
	batch := req.Batch
	if batch == 0 {
		batch = 1
	} else {
		// This is a batch request, capture initial numPending.
		isBatchRequest = true
		var err error
		if np, validThrough, err = store.NumPending(seq, req.NextFor, false); err != nil {
			return
		}
	}

	// Grab MaxBytes
	mb := req.MaxBytes
	if mb == 0 && s != nil {
		// Fill in with the server's MaxPending.
		mb = int(s.opts.MaxPending)
	}
	// Track what we have sent.
	var sentBytes int

	// Loop over batch, which defaults to 1.
	for i := 0; i < batch; i++ {
		var (
			svp StoreMsg
			sm  *StoreMsg
			err error
		)
		if seq > 0 && req.NextFor == _EMPTY_ {
			// Only do direct lookup for a non batch.
			if i == 0 && !isBatchRequest {
				sm, err = store.LoadMsg(seq, &svp)
			} else {
				// We want to use load next with fwcs to step over deleted msgs.
				sm, seq, err = store.LoadNextMsg(fwcs, true, seq, &svp)
			}
			// Bump for next loop if applicable.
			seq++
		} else if req.NextFor != _EMPTY_ {
			sm, seq, err = store.LoadNextMsg(req.NextFor, wc, seq, &svp)
			seq++
		} else {
			// Batch is not applicable here, this is checked before we get here.
			sm, err = store.LoadLastMsg(req.LastFor, &svp)
		}
		if err != nil {
			// For batches, if we stop early we want to do EOB logic below.
			if batch > 1 && i > 0 {
				break
			}
			hdr := []byte("NATS/1.0 404 Message Not Found\r\n\r\n")
			mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
			return
		}

		ts := time.Unix(0, sm.ts).UTC()
		var hdr []byte
		if !req.NoHeaders {
			hdr = sm.hdr
			if isBatchRequest {
				// Decrement num pending. This is an optimization, and we do not continue to look it up for these operations.
				if np > 0 {
					np--
				}
				if len(hdr) == 0 {
					hdr = fmt.Appendf(nil, dgb, name, sm.subj, sm.seq, ts.Format(time.RFC3339Nano), np, lseq)
				} else {
					hdr = copyBytes(hdr)
					hdr = genHeader(hdr, JSStream, name)
					hdr = genHeader(hdr, JSSubject, sm.subj)
					hdr = genHeader(hdr, JSSequence, strconv.FormatUint(sm.seq, 10))
					hdr = genHeader(hdr, JSTimeStamp, ts.Format(time.RFC3339Nano))
					hdr = genHeader(hdr, JSNumPending, strconv.FormatUint(np, 10))
					hdr = genHeader(hdr, JSLastSequence, strconv.FormatUint(lseq, 10))
				}
			} else {
				if len(hdr) == 0 {
					hdr = fmt.Appendf(nil, dg, name, sm.subj, sm.seq, ts.Format(time.RFC3339Nano))
				} else {
					hdr = copyBytes(hdr)
					hdr = genHeader(hdr, JSStream, name)
					hdr = genHeader(hdr, JSSubject, sm.subj)
					hdr = genHeader(hdr, JSSequence, strconv.FormatUint(sm.seq, 10))
					hdr = genHeader(hdr, JSTimeStamp, ts.Format(time.RFC3339Nano))
				}
			}
		}
		// Track our lseq
		lseq = sm.seq
		// Send out our message.
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, sm.msg, nil, 0))
		// Check if we have exceeded max bytes.
		sentBytes += len(sm.subj) + len(sm.hdr) + len(sm.msg)
		if sentBytes >= mb {
			break
		}
	}

	// If batch was requested send EOB.
	if isBatchRequest {
		// Update if the stream's last sequence has moved past our validThrough.
		var state StreamState
		store.FastState(&state)
		if state.LastSeq > validThrough {
			var err error
			if np, _, err = store.NumPending(seq, req.NextFor, false); err != nil {
				return
			}
		}
		hdr := fmt.Appendf(nil, eob, np, lseq)
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
	}
}

// processInboundJetStreamMsg handles processing messages bound for a stream.
func (mset *stream) processInboundJetStreamMsg(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	hdr, msg := c.msgParts(copyBytes(rmsg)) // Need to copy.
	hdr = removeHeaderStatusIfPresent(hdr)
	if mt, traceOnly := c.isMsgTraceEnabled(); mt != nil {
		// If message is delivered, we need to disable the message trace headers
		// to prevent a trace event to be generated when a stored message
		// is delivered to a consumer and routed.
		if !traceOnly {
			hdr = setHeader(MsgTraceDest, MsgTraceDestDisabled, hdr)
		}
		// This will add the jetstream event while in the client read loop.
		// Since the event will be updated in a different go routine, the
		// tracing object will have a separate reference to the JS trace
		// object.
		mt.addJetStreamEvent(mset.name())
	}
	mset.queueInbound(mset.msgs, subject, reply, hdr, msg, nil, c.pa.trace)
}

var (
	errLastSeqMismatch   = errors.New("last sequence mismatch")
	errMsgIdDuplicate    = errors.New("msgid is duplicate")
	errStreamClosed      = errors.New("stream closed")
	errInvalidMsgHandler = errors.New("undefined message handler")
	errStreamMismatch    = errors.New("expected stream does not match")
	errMsgTTLDisabled    = errors.New("message TTL disabled")
)

// processJetStreamMsg is where we try to actually process the stream msg.
// needIsolation should be false only if the caller already holds isolateMu
// across a whole atomic batch; mset.mu must NOT be held by the caller.
func (mset *stream) processJetStreamMsg(subject, reply string, hdr, msg []byte, lseq uint64, ts int64, mt *msgTrace, sourced bool, needIsolation bool) error {
	return mset.processJetStreamMsgWithBatch(subject, reply, hdr, msg, lseq, ts, mt, sourced, needIsolation, nil, false)
}

func (mset *stream) processJetStreamMsgWithBatch(subject, reply string, hdr, msg []byte, lseq uint64, ts int64, mt *msgTrace, sourced bool, needIsolation bool, fastBatch *FastBatch, deferResponse bool) (retErr error) {
	if mt != nil && !deferResponse {
		// Only the leader/standalone will have mt!=nil. On exit, send the
		// message trace event.
		defer func() {
			mt.sendEventFromJetStream(retErr)
		}()
	}

	if mset == nil || mset.closed.Load() {
		return errStreamClosed
	}

	// Hold lock while storing the message, and potentially purging as part of a rollup.
	// If we're writing an atomic batch of multiple messages, the lock is already held.
	if needIsolation {
		mset.isolateMu.Lock()
		defer mset.isolateMu.Unlock()
	}
	// Hold the stream lock for the lifetime of processing a single message.
	mset.mu.Lock()
	defer mset.mu.Unlock()

	s, store := mset.srv, mset.store

	traceOnly := mt.traceOnly()
	bumpCLFS := func() {
		// Do not bump if tracing and not doing message delivery.
		if traceOnly {
			return
		}
		mset.clMu.Lock()
		mset.clfs++
		mset.clMu.Unlock()
	}

	// Apply the input subject transform if any
	if mset.itr != nil {
		ts, err := mset.itr.Match(subject)
		if err == nil {
			// no filtering: if the subject doesn't map the source of the transform, don't change it
			subject = ts
		}
	}

	var accName string
	if mset.acc != nil {
		accName = mset.acc.Name
	}

	js, jsa, doAck := mset.js, mset.jsa, !mset.cfg.NoAck
	name, stype := mset.cfg.Name, mset.cfg.Storage
	maxMsgSize := int(mset.cfg.MaxMsgSize)
	numConsumers := len(mset.consumers)
	interestRetention := mset.cfg.Retention == InterestPolicy
	allowMsgCounter, allowMsgSchedules := mset.cfg.AllowMsgCounter, mset.cfg.AllowMsgSchedules
	allowRollupPurge := mset.cfg.AllowRollup && !mset.cfg.DenyPurge
	// Snapshot if we are the leader and if we can respond.
	isLeader, isSealed := mset.isLeaderNodeState(), mset.cfg.Sealed
	isClustered, isMirror := mset.isClustered(), mset.cfg.Mirror != nil
	canConsistencyCheck := !isClustered || traceOnly
	canRespond := doAck && len(reply) > 0 && isLeader
	outq := mset.outq

	var resp = &JSPubAckResponse{}

	if canConsistencyCheck && stype == FileStorage && isFileStoreMsgTooLarge(fileStoreMsgSize(subject, hdr, msg)) {
		if canRespond {
			resp.PubAck = &PubAck{Stream: name}
			resp.Error = NewJSStreamStoreFailedError(ErrMsgTooLarge)
			response, _ := json.Marshal(resp)
			outq.sendMsg(reply, response)
		}
		return ErrMsgTooLarge
	}

	var (
		batchId  string
		batchSeq uint64
	)
	// Populate batch details.
	if fastBatch != nil {
		// For R1 we can reuse without regenerating.
		batchId, batchSeq = fastBatch.id, fastBatch.seq
		// Disable consistency checking if this was already done
		// earlier as part of the batch consistency check.
		canConsistencyCheck = traceOnly
	} else if fastBatch, _ = getFastBatch(reply, hdr); fastBatch != nil {
		defer fastBatch.returnToPool()
		batchId, batchSeq = fastBatch.id, fastBatch.seq
		// Disable consistency checking if this was already done
		// earlier as part of the batch consistency check.
		canConsistencyCheck = traceOnly
	}
	if len(hdr) > 0 && batchId == _EMPTY_ {
		if batchId = getBatchId(hdr); batchId != _EMPTY_ {
			batchSeq, _ = getBatchSequence(hdr)
			// Disable consistency checking if this was already done
			// earlier as part of the batch consistency check.
			canConsistencyCheck = traceOnly
		}
	}

	// Bail here if sealed.
	if canConsistencyCheck && isSealed {
		if canRespond && outq != nil {
			resp.PubAck = &PubAck{Stream: name}
			resp.Error = ApiErrors[JSStreamSealedErr]
			b, _ := json.Marshal(resp)
			outq.sendMsg(reply, b)
		}
		return ApiErrors[JSStreamSealedErr]
	}

	var buf [256]byte
	pubAck := append(buf[:0], mset.pubAck...)

	// If this is a non-clustered msg and we are not considered active, meaning no active subscription, do not process.
	if lseq == 0 && ts == 0 && !mset.active {
		return nil
	}

	// For clustering the lower layers will pass our expected lseq. If it is present check for that here.
	var clfs uint64
	if lseq > 0 {
		clfs = mset.getCLFS()
		if lseq != (mset.lseq + clfs) {
			isMisMatch := true
			// We may be able to recover here if we have no state whatsoever, or we are a mirror.
			// See if we have to adjust our starting sequence.
			if mset.lseq == 0 || isMirror {
				var state StreamState
				mset.store.FastState(&state)
				if state.FirstSeq == 0 {
					if _, err := mset.store.Compact(lseq + 1); err != nil {
						return err
					}
					mset.lseq = lseq
					isMisMatch = false
				}
			}
			// Really is a mismatch.
			if isMisMatch {
				if canRespond && outq != nil {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = ApiErrors[JSStreamSequenceNotMatchErr]
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return errLastSeqMismatch
			}
		}
	}

	// If we have received this message across an account we may have request information attached.
	// For now remove. TODO(dlc) - Should this be opt-in or opt-out?
	if len(hdr) > 0 {
		hdr = removeHeaderIfPresent(hdr, ClientInfoHdr)
	}

	// Process additional msg headers if still present.
	var msgId string
	var incr *big.Int
	var rollupSub, rollupAll bool

	// Check to see if we are over the max msg size.
	// Subtract to prevent against overflows.
	if canConsistencyCheck && maxMsgSize >= 0 && (len(hdr) > maxMsgSize || len(msg) > maxMsgSize-len(hdr)) {
		if canRespond {
			resp.PubAck = &PubAck{Stream: name}
			resp.Error = NewJSStreamMessageExceedsMaximumError()
			b, _ := json.Marshal(resp)
			outq.sendMsg(reply, b)
		}
		return ErrMaxPayload
	}

	if canConsistencyCheck && len(hdr) > math.MaxUint16 {
		if canRespond {
			resp.PubAck = &PubAck{Stream: name}
			resp.Error = NewJSStreamHeaderExceedsMaximumError()
			b, _ := json.Marshal(resp)
			outq.sendMsg(reply, b)
		}
		return ErrMaxPayload
	}

	if len(hdr) > 0 {
		// Certain checks have already been performed if in clustered mode, so only check if not.
		// Note, for cluster mode but with message tracing (without message delivery), we need
		// to do this check here since it was not done in processClusteredInboundMsg().
		if canConsistencyCheck {
			// Counter increments.
			// Only supported on counter streams, and payload must be empty (if not coming from a source).
			var ok bool
			if incr, ok = getMessageIncr(hdr); !ok {
				apiErr := NewJSMessageIncrInvalidError()
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = apiErr
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return apiErr
			} else if incr != nil && !sourced {
				// Only do checks if the message isn't sourced. Otherwise, we need to store verbatim.
				if !allowMsgCounter {
					apiErr := NewJSMessageIncrDisabledError()
					if canRespond {
						resp.PubAck = &PubAck{Stream: name}
						resp.Error = apiErr
						b, _ := json.Marshal(resp)
						outq.sendMsg(reply, b)
					}
					return apiErr
				} else if len(msg) > 0 {
					apiErr := NewJSMessageIncrPayloadError()
					if canRespond {
						resp.PubAck = &PubAck{Stream: name}
						resp.Error = apiErr
						b, _ := json.Marshal(resp)
						outq.sendMsg(reply, b)
					}
					return apiErr
				} else {
					// Check for incompatible headers.
					var doErr bool
					if getRollup(hdr) != _EMPTY_ ||
						getExpectedStream(hdr) != _EMPTY_ ||
						getExpectedLastMsgId(hdr) != _EMPTY_ ||
						getExpectedLastSeqPerSubjectForSubject(hdr) != _EMPTY_ {
						doErr = true
					} else if _, ok := getExpectedLastSeq(hdr); ok {
						doErr = true
					} else if _, ok := getExpectedLastSeqPerSubject(hdr); ok {
						doErr = true
					}

					if doErr {
						apiErr := NewJSMessageIncrInvalidError()
						if canRespond {
							resp.PubAck = &PubAck{Stream: name}
							resp.Error = apiErr
							b, _ := json.Marshal(resp)
							outq.sendMsg(reply, b)
						}
						return apiErr
					}
				}
			}

			// Expected stream.
			if sname := getExpectedStream(hdr); sname != _EMPTY_ && sname != name {
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = NewJSStreamNotMatchError()
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return errStreamMismatch
			}

			// TTL'd messages are rejected entirely if TTLs are not enabled on the stream.
			// Shouldn't happen in clustered mode since we should have already caught this
			// in processClusteredInboundMsg, but needed here for non-clustered etc.
			if ttl, _ := getMessageTTL(hdr); !sourced && ttl != 0 && !mset.cfg.AllowMsgTTL {
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = NewJSMessageTTLDisabledError()
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return errMsgTTLDisabled
			}

			// Expected last sequence per subject.
			if seq, exists := getExpectedLastSeqPerSubject(hdr); exists {
				// Allow override of the subject used for the check.
				seqSubj := subject
				if optSubj := getExpectedLastSeqPerSubjectForSubject(hdr); optSubj != _EMPTY_ {
					seqSubj = optSubj
				}

				fseq, err := store.LoadLastSeq(seqSubj)
				if err == ErrStoreMsgNotFound && seq == 0 {
					fseq, err = 0, nil
				}
				if err != nil || fseq != seq {
					if canRespond {
						resp.PubAck = &PubAck{Stream: name}
						resp.Error = NewJSStreamWrongLastSequenceError(fseq)
						b, _ := json.Marshal(resp)
						outq.sendMsg(reply, b)
					}
					return fmt.Errorf("last sequence by subject mismatch: %d vs %d", seq, fseq)
				}
			} else if getExpectedLastSeqPerSubjectForSubject(hdr) != _EMPTY_ {
				apiErr := NewJSStreamExpectedLastSeqPerSubjectInvalidError()
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = apiErr
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return apiErr
			}

			// Expected last sequence.
			if seq, exists := getExpectedLastSeq(hdr); exists && seq != mset.lseq {
				mlseq := mset.lseq
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = NewJSStreamWrongLastSequenceError(mlseq)
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return fmt.Errorf("last sequence mismatch: %d vs %d", seq, mlseq)
			}

			// Message scheduling.
			if sourced {
				// noop, sourced messages were already validated by the origin stream.
			} else if schedule, apiErr := getMessageSchedule(hdr); apiErr != nil {
				if !allowMsgSchedules {
					apiErr = NewJSMessageSchedulesDisabledError()
				}
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = apiErr
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return apiErr
			} else if !schedule.IsZero() {
				if !allowMsgSchedules {
					apiErr := NewJSMessageSchedulesDisabledError()
					if canRespond {
						resp.PubAck = &PubAck{Stream: name}
						resp.Error = apiErr
						b, _ := json.Marshal(resp)
						outq.sendMsg(reply, b)
					}
					return apiErr
				} else if scheduleTtl, ok := getMessageScheduleTTL(hdr); !ok {
					apiErr := NewJSMessageSchedulesTTLInvalidError()
					if canRespond {
						resp.PubAck = &PubAck{Stream: name}
						resp.Error = apiErr
						b, _ := json.Marshal(resp)
						outq.sendMsg(reply, b)
					}
					return apiErr
				} else if scheduleRollup := getMessageScheduleRollup(hdr); scheduleRollup != _EMPTY_ && scheduleRollup != JSMsgRollupSubject {
					apiErr := NewJSMessageSchedulesRollupInvalidError()
					if canRespond {
						resp.PubAck = &PubAck{Stream: name}
						resp.Error = apiErr
						b, _ := json.Marshal(resp)
						outq.sendMsg(reply, b)
					}
					return apiErr
				} else if scheduleTtl != _EMPTY_ && !mset.cfg.AllowMsgTTL {
					if canRespond {
						resp.PubAck = &PubAck{Stream: name}
						resp.Error = NewJSMessageTTLDisabledError()
						b, _ := json.Marshal(resp)
						outq.sendMsg(reply, b)
					}
					return errMsgTTLDisabled
				} else if scheduleTarget := getMessageScheduleTarget(hdr); scheduleTarget == _EMPTY_ ||
					!IsValidPublishSubject(scheduleTarget) || scheduleTarget == subject {
					apiErr := NewJSMessageSchedulesTargetInvalidError()
					if canRespond {
						resp.PubAck = &PubAck{Stream: name}
						resp.Error = apiErr
						b, _ := json.Marshal(resp)
						outq.sendMsg(reply, b)
					}
					return apiErr
				} else if scheduleSource := getMessageScheduleSource(hdr); scheduleSource != _EMPTY_ &&
					(scheduleSource == scheduleTarget || scheduleSource == subject || !IsValidPublishSubject(scheduleSource)) {
					apiErr := NewJSMessageSchedulesSourceInvalidError()
					if canRespond {
						resp.PubAck = &PubAck{Stream: name}
						resp.Error = apiErr
						b, _ := json.Marshal(resp)
						outq.sendMsg(reply, b)
					}
					return apiErr
				} else {
					match := slices.ContainsFunc(mset.cfg.Subjects, func(subj string) bool {
						return SubjectsCollide(subj, scheduleTarget)
					})
					if !match {
						apiErr := NewJSMessageSchedulesTargetInvalidError()
						if canRespond {
							resp.PubAck = &PubAck{Stream: name}
							resp.Error = apiErr
							b, _ := json.Marshal(resp)
							outq.sendMsg(reply, b)
						}
						return apiErr
					}
					if scheduleSource != _EMPTY_ {
						match = slices.ContainsFunc(mset.cfg.Subjects, func(subj string) bool {
							return SubjectsCollide(subj, scheduleSource)
						})
						if !match {
							apiErr := NewJSMessageSchedulesSourceInvalidError()
							if canRespond {
								resp.PubAck = &PubAck{Stream: name}
								resp.Error = apiErr
								b, _ := json.Marshal(resp)
								outq.sendMsg(reply, b)
							}
							return apiErr
						}
					}

					// Add a rollup sub header if it doesn't already exist.
					// Otherwise, it must exist already as a rollup on the subject.
					if rollup := getRollup(hdr); rollup == _EMPTY_ {
						hdr = genHeader(hdr, JSMsgRollup, JSMsgRollupSubject)
					} else if rollup != JSMsgRollupSubject {
						apiErr := NewJSMessageSchedulesRollupInvalidError()
						if canRespond {
							resp.PubAck = &PubAck{Stream: name}
							resp.Error = apiErr
							b, _ := json.Marshal(resp)
							outq.sendMsg(reply, b)
						}
						return apiErr
					}
				}
			}
			if scheduleNext := sliceHeader(JSScheduleNext, hdr); len(scheduleNext) > 0 && !sourced {
				// Clients may only use Nats-Schedule-Next to purge a schedule.
				if bytesToString(scheduleNext) != JSScheduleNextPurge {
					apiErr := NewJSMessageSchedulesSchedulerInvalidError()
					if canRespond {
						resp.PubAck = &PubAck{Stream: name}
						resp.Error = apiErr
						b, _ := json.Marshal(resp)
						outq.sendMsg(reply, b)
					}
					return apiErr
				}
				// Nats-Scheduler must accompany the purge and:
				// - it must NOT be empty.
				// - it must NOT match the publish subject.
				if scheduler := sliceHeader(JSScheduler, hdr); len(scheduler) == 0 ||
					bytesToString(scheduler) == subject || !IsValidPublishSubject(bytesToString(scheduler)) {
					apiErr := NewJSMessageSchedulesSchedulerInvalidError()
					if canRespond {
						resp.PubAck = &PubAck{Stream: name}
						resp.Error = apiErr
						b, _ := json.Marshal(resp)
						outq.sendMsg(reply, b)
					}
					return apiErr
				} else if !allowMsgSchedules {
					apiErr := NewJSMessageSchedulesDisabledError()
					if canRespond {
						resp.PubAck = &PubAck{Stream: name}
						resp.Error = apiErr
						b, _ := json.Marshal(resp)
						outq.sendMsg(reply, b)
					}
					return apiErr
				} else {
					// Check that the to-be-purged subject is a schedule message.
					// We still allow this message through if there exists no message for this subject,
					// to remain backward-compatible. An "expected at sequence" check can still be
					// performed to make this stricter.
					var smv StoreMsg
					sm, _ := store.LoadLastMsg(bytesToString(scheduler), &smv)
					if sm != nil && len(sliceHeader(JSSchedulePattern, sm.hdr)) == 0 {
						apiErr := NewJSMessageSchedulesSchedulerInvalidError()
						if canRespond {
							resp.PubAck = &PubAck{Stream: name}
							resp.Error = apiErr
							b, _ := json.Marshal(resp)
							outq.sendMsg(reply, b)
						}
						return apiErr
					}
				}
			} else if !sourced && len(sliceHeader(JSScheduler, hdr)) > 0 {
				// Clients may only use Nats-Scheduler alongside Nats-Schedule-Next.
				apiErr := NewJSMessageSchedulesSchedulerInvalidError()
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = apiErr
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return apiErr
			}

			// Non-sourced messages aren't allowed to have the stream source header.
			if !sourced && len(sliceHeader(JSStreamSource, hdr)) > 0 {
				apiErr := NewJSMessageSourceHdrNotAllowedError()
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = apiErr
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return apiErr
			}
		}

		// Dedupe detection. This is done at the cluster level for dedupe detection above the
		// lower layers. But we still need to pull out the msgId.
		if msgId = getMsgId(hdr); msgId != _EMPTY_ {
			// Do real check only if not clustered or traceOnly flag is set.
			// If we're mirroring we can't deduplicate on our own.
			if canConsistencyCheck && !isMirror {
				var seq uint64
				mset.ddMu.Lock()
				dde := mset.checkMsgId(msgId)
				if dde != nil {
					seq = dde.seq
				}
				mset.ddMu.Unlock()
				if seq > 0 {
					if canRespond {
						response := append(pubAck, strconv.FormatUint(seq, 10)...)
						response = append(response, ",\"duplicate\": true}"...)
						outq.sendMsg(reply, response)
					}
					return errMsgIdDuplicate
				}
			}
		}

		// Expected last msgId.
		if lmsgId := getExpectedLastMsgId(hdr); lmsgId != _EMPTY_ {
			if lmsgId != mset.lmsgId {
				last := mset.lmsgId
				bumpCLFS()
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = NewJSStreamWrongLastMsgIDError(last)
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return fmt.Errorf("last msgid mismatch: %q vs %q", lmsgId, last)
			}
		}
		// Check for any rollups.
		if rollup := getRollup(hdr); rollup != _EMPTY_ {
			if canConsistencyCheck && !allowRollupPurge && !sourced {
				err := errors.New("rollup not permitted")
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = NewJSStreamRollupFailedError(err)
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return err
			}
			switch rollup {
			case JSMsgRollupSubject:
				rollupSub = true
			case JSMsgRollupAll:
				rollupAll = true
			default:
				if canConsistencyCheck {
					err := fmt.Errorf("rollup value invalid: %q", rollup)
					if canRespond {
						resp.PubAck = &PubAck{Stream: name}
						resp.Error = NewJSStreamRollupFailedError(err)
						b, _ := json.Marshal(resp)
						outq.sendMsg(reply, b)
					}
					return err
				}
			}
		}
	}

	if canConsistencyCheck && incr == nil && allowMsgCounter {
		apiErr := NewJSMessageIncrMissingError()
		if canRespond {
			resp.PubAck = &PubAck{Stream: name}
			resp.Error = apiErr
			b, _ := json.Marshal(resp)
			outq.sendMsg(reply, b)
		}
		return apiErr
	}

	// Response Ack.
	var (
		response []byte
		seq      uint64
		err      error
	)

	// Apply increment for counter.
	// But only if it's allowed for this stream. This can happen when we store verbatim for a sourced stream.
	if canConsistencyCheck && incr != nil && allowMsgCounter {
		var initial big.Int
		var sources CounterSources
		var smv StoreMsg
		sm, err := store.LoadLastMsg(subject, &smv)
		if err == nil && sm != nil {
			var val CounterValue
			// Return an error if the counter is broken somehow.
			if json.Unmarshal(sm.msg, &val) != nil {
				apiErr := NewJSMessageCounterBrokenError()
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = apiErr
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return apiErr
			}
			if ncs := sliceHeader(JSMessageCounterSources, sm.hdr); len(ncs) > 0 {
				if err := json.Unmarshal(ncs, &sources); err != nil {
					apiErr := NewJSMessageCounterBrokenError()
					if canRespond {
						resp.PubAck = &PubAck{Stream: name}
						resp.Error = apiErr
						b, _ := json.Marshal(resp)
						outq.sendMsg(reply, b)
					}
					return apiErr
				}
			}
			initial.SetString(val.Value, 10)
		}
		srchdr := sliceHeader(JSStreamSource, hdr)
		if len(srchdr) > 0 {
			// This is a sourced message, so we can't apply Nats-Incr but
			// instead should just update the source count header.
			fields := strings.Split(string(srchdr), " ")
			origStream := fields[0]
			origSubj := subject
			if len(fields) >= 5 {
				origSubj = fields[4]
			}
			var val CounterValue
			if json.Unmarshal(msg, &val) != nil {
				apiErr := NewJSMessageCounterBrokenError()
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = apiErr
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return apiErr
			}
			var sourced big.Int
			sourced.SetString(val.Value, 10)
			if sources == nil {
				sources = map[string]map[string]string{}
			}
			if sources[origStream] == nil {
				sources[origStream] = map[string]string{}
			}
			prevVal := sources[origStream][origSubj]
			sources[origStream][origSubj] = sourced.String()
			// We will also replace the Nats-Incr header with the diff
			// between our last value from this source and this one, so
			// that the arithmetic is always correct.
			var previous big.Int
			previous.SetString(prevVal, 10)
			incr.Sub(&sourced, &previous)
			hdr = setHeader(JSMessageIncr, incr.String(), hdr)
		}
		// Now make the change.
		initial.Add(&initial, incr)
		// Generate the new payload.
		var _msg [128]byte
		msg = fmt.Appendf(_msg[:0], "{%q:%q}", "val", initial.String())
		// Write the updated source count headers.
		if len(sources) > 0 {
			nhdr, err := json.Marshal(sources)
			if err != nil {
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = NewJSMessageCounterBrokenError()
					response, _ = json.Marshal(resp)
					outq.sendMsg(reply, response)
				}
				return err
			}
			hdr = setHeader(JSMessageCounterSources, string(nhdr), hdr)
		}

		// Check to see if we are over the max msg size.
		// Subtract to prevent against overflows.
		maxPayload := int64(mset.srv.getOpts().MaxPayload)
		hdrLen, msgLen := int64(len(hdr)), int64(len(msg))
		if hdrLen > maxPayload || msgLen > maxPayload-hdrLen {
			if canRespond {
				resp.PubAck = &PubAck{Stream: name}
				resp.Error = NewJSStreamMessageExceedsMaximumError()
				response, _ = json.Marshal(resp)
				outq.sendMsg(reply, response)
			}
			return ErrMaxPayload
		}
	}

	// Header processing above may have changed the message, such as when a
	// counter increment generates its value payload. Check the resulting size
	// against the stream limit as well as checking the inbound size early.
	// Subtract to prevent against overflows.
	if canConsistencyCheck && maxMsgSize >= 0 && (len(hdr) > maxMsgSize || len(msg) > maxMsgSize-len(hdr)) {
		if canRespond {
			resp.PubAck = &PubAck{Stream: name}
			resp.Error = NewJSStreamMessageExceedsMaximumError()
			response, _ = json.Marshal(resp)
			outq.sendMsg(reply, response)
		}
		return ErrMaxPayload
	}

	if canConsistencyCheck && len(hdr) > math.MaxUint16 {
		if canRespond {
			resp.PubAck = &PubAck{Stream: name}
			resp.Error = NewJSStreamHeaderExceedsMaximumError()
			response, _ = json.Marshal(resp)
			outq.sendMsg(reply, response)
		}
		return ErrMaxPayload
	}

	// Check to see if we have exceeded our limits.
	// Don't error and log/stepdown if we're tracing when clustered.
	if !isClustered && js.limitsExceeded(stype) {
		s.resourcesExceededError(stype)
		if canRespond {
			resp.PubAck = &PubAck{Stream: name}
			resp.Error = NewJSInsufficientResourcesError()
			response, _ = json.Marshal(resp)
			outq.sendMsg(reply, response)
		}
		// Stepdown regardless.
		if node := mset.node; node != nil {
			node.StepDown()
		}
		return NewJSInsufficientResourcesError()
	}

	var noInterest bool

	// If we are interest based retention and have no consumers then we can skip.
	if interestRetention {
		mset.clsMu.RLock()
		noInterest = numConsumers == 0 || mset.csl == nil || !mset.csl.HasInterest(subject)
		mset.clsMu.RUnlock()
	}

	// Grab timestamp if not already set.
	if ts == 0 && lseq > 0 {
		ts = time.Now().UnixNano()
	}

	mt.updateJetStreamEvent(subject, noInterest)
	if traceOnly {
		return nil
	}

	// Skip msg here.
	if noInterest {
		mset.lseq, _ = store.SkipMsgNoInterest(0)
		mset.lmsgId = msgId
		// If we have a msgId make sure to save.
		if msgId != _EMPTY_ {
			mset.storeMsgId(&ddentry{msgId, mset.lseq, ts})
		}
		if allowRollupPurge {
			if err = mset.processJetStreamMsgWithRollup(subject, rollupSub, rollupAll, hdr, 0); err != nil {
				return err
			}
		}
		// If using fast batch publish, we occasionally send flow control messages.
		// And, we need to ensure a PubAck is sent if the commit happens through EOB.
		if fastBatch != nil {
			if mset.batches == nil {
				mset.batches = &batching{}
			}
			mset.batches.mu.Lock()
			// Check full leader state so we only send the client an update once we're caught up.
			commit := mset.batches.fastBatchRegisterSequences(mset, reply, mset.lseq, mset.isLeader(), fastBatch)
			mset.batches.mu.Unlock()
			if !commit {
				reply = _EMPTY_
				canRespond = false
			}
		}
		if canRespond {
			response = append(pubAck, strconv.FormatUint(mset.lseq, 10)...)
			if batchId != _EMPTY_ {
				response = append(response, fmt.Sprintf(",\"batch\":%q,\"count\":%d}", batchId, batchSeq)...)
			} else {
				response = append(response, '}')
			}
			outq.sendMsg(reply, response)
		}
		return nil
	}

	// Republish state if needed.
	var tsubj string
	var tlseq uint64
	var thdrsOnly bool
	if mset.tr != nil {
		tsubj, _ = mset.tr.Match(subject)
		if tsubj != _EMPTY_ && !IsValidPublishSubject(tsubj) {
			s.RateLimitWarnf("Stream '%s > %s' suppressing republish with invalid subject %q", accName, name, tsubj)
			tsubj = _EMPTY_ // ... stops the republish.
		}
		if mset.cfg.RePublish != nil {
			thdrsOnly = mset.cfg.RePublish.HeadersOnly
		}
	}
	republish := tsubj != _EMPTY_ && isLeader

	// If we are republishing grab last sequence for this exact subject. Aids in gap detection for lightweight clients.
	if republish {
		tlseq, _ = store.LoadLastSeq(subject)
	}

	// If clustered this was already checked and we do not want to check here and possibly introduce skew.
	// Don't error and log if we're tracing when clustered.
	if !isClustered {
		if exceeded, err := jsa.wouldExceedLimits(stype, mset.tier, mset.cfg.Replicas, subject, hdr, msg); exceeded {
			if err == nil {
				err = NewJSAccountResourcesExceededError()
			}
			s.RateLimitWarnf("JetStream %s resource limits exceeded for account: %q", strings.ToLower(stype.String()), accName)
			if canRespond {
				resp.PubAck = &PubAck{Stream: name}
				resp.Error = err
				response, _ = json.Marshal(resp)
				outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, nil, response, nil, 0))
			}
			return err
		}
	}

	// Find the message TTL if any.
	ttl, err := getMessageTTL(hdr)
	if err != nil {
		if canRespond {
			resp.PubAck = &PubAck{Stream: name}
			resp.Error = NewJSMessageTTLInvalidError()
			response, _ = json.Marshal(resp)
			outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, nil, response, nil, 0))
		}
		return err
	}

	// If subject delete markers are used, ensure message TTL is that at minimum.
	// Otherwise, subject delete markers could be missed if one already exists for this subject.
	// MaxMsgsPer=1 is an exception, because we'll only ever have one message.
	if ttl > 0 && mset.cfg.SubjectDeleteMarkerTTL > 0 && mset.cfg.MaxMsgsPer != 1 {
		if minTtl := int64(mset.cfg.SubjectDeleteMarkerTTL.Seconds()); ttl < minTtl {
			ttl = minTtl
			hdr = removeHeaderIfPresent(hdr, JSMessageTTL)
			hdr = genHeader(hdr, JSMessageTTL, strconv.FormatInt(ttl, 10))
		}
	}

	// Store actual msg.
	if lseq == 0 && ts == 0 {
		seq, ts, err = store.StoreMsg(subject, hdr, msg, ttl)
	} else {
		// Make sure to take into account any message assignments that we had to skip (clfs).
		seq = lseq + 1 - clfs
		err = store.StoreRawMsg(subject, hdr, msg, seq, ts, ttl, canConsistencyCheck)
	}

	if err != nil {
		if isPermissionError(err) {
			// messages in block cache could be lost in the worst case.
			// In the clustered mode it is very highly unlikely as a result of replication.
			go mset.srv.ShutdownJetStream()
			mset.srv.Warnf("Filesystem permission denied while writing msg, disabling JetStream: %v", err)
			return err
		}

		switch err {
		case ErrMaxMsgs, ErrMaxBytes, ErrMaxMsgsPerSubject, ErrMsgTooLarge:
			s.RateLimitDebugf("JetStream failed to store a msg on stream '%s > %s': %v", accName, name, err)
		case ErrStoreClosed:
		default:
			// We don't want to respond back to the user, and definitely not up CLFS either.
			// This was likely an IO issue, so only log and return the error. This will stop
			// the stream if it was replicated.
			s.RateLimitErrorf("JetStream failed to store a msg on stream '%s > %s': %v", accName, name, err)
			mset.setWriteErrLocked(err)
			return err
		}

		// If we did not succeed increment clfs in case we are clustered.
		bumpCLFS()
		if canRespond {
			resp.PubAck = &PubAck{Stream: name}
			resp.Error = NewJSStreamStoreFailedError(err, Unless(err))
			response, _ = json.Marshal(resp)
			outq.sendMsg(reply, response)
		}
		return err
	}

	// Check for preAcks and the need to clear it.
	if mset.hasAllPreAcks(seq, subject) {
		mset.clearAllPreAcks(seq)
		// If we're clustered and the stream leader, we can now propose deleting this message.
		// We still store it below, so we remain properly synchronized with our followers.
		// If this proposal fails, we retry out-of-band.
		if isClustered && isLeader {
			md := streamMsgDelete{Seq: seq, NoErase: true, Stream: mset.cfg.Name}
			_ = mset.node.Propose(mset.term, encodeMsgDelete(&md))
		}
	}

	// If here we succeeded in storing the message.
	mset.lmsgId = msgId
	mset.lseq = seq

	// If we have a msgId make sure to save.
	// This will replace our estimate from the cluster layer if we are clustered.
	if msgId != _EMPTY_ {
		mset.ddMu.Lock()
		if isClustered && isLeader && mset.ddmap != nil {
			if dde := mset.ddmap[msgId]; dde != nil {
				dde.seq, dde.ts = seq, ts
			} else {
				mset.storeMsgIdLocked(&ddentry{msgId, seq, ts})
			}
		} else {
			// R1 or not leader..
			mset.storeMsgIdLocked(&ddentry{msgId, seq, ts})
		}
		mset.ddMu.Unlock()
	}

	// No errors, this is the normal path.
	if allowRollupPurge {
		if err = mset.processJetStreamMsgWithRollup(subject, rollupSub, rollupAll, hdr, 1); err != nil {
			return err
		}
	}

	// Check for republish.
	if republish {
		const ht = "NATS/1.0\r\nNats-Stream: %s\r\nNats-Subject: %s\r\nNats-Sequence: %d\r\nNats-Time-Stamp: %s\r\nNats-Last-Sequence: %d\r\n\r\n"
		const htho = "NATS/1.0\r\nNats-Stream: %s\r\nNats-Subject: %s\r\nNats-Sequence: %d\r\nNats-Time-Stamp: %s\r\nNats-Last-Sequence: %d\r\nNats-Msg-Size: %d\r\n\r\n"
		// When adding to existing headers, will use the fmt.Append version so this skips the headers from above.
		const hoff = 10

		tsStr := time.Unix(0, ts).UTC().Format(time.RFC3339Nano)
		var rpMsg []byte
		if len(hdr) == 0 {
			if !thdrsOnly {
				hdr = fmt.Appendf(nil, ht, name, subject, seq, tsStr, tlseq)
				rpMsg = copyBytes(msg)
			} else {
				hdr = fmt.Appendf(nil, htho, name, subject, seq, tsStr, tlseq, len(msg))
			}
		} else {
			// use hdr[:end:end] to make sure as we add we copy the original hdr.
			end := len(hdr) - LEN_CR_LF
			if !thdrsOnly {
				hdr = fmt.Appendf(hdr[:end:end], ht[hoff:], name, subject, seq, tsStr, tlseq)
				rpMsg = copyBytes(msg)
			} else {
				hdr = fmt.Appendf(hdr[:end:end], htho[hoff:], name, subject, seq, tsStr, tlseq, len(msg))
			}
		}
		outq.send(newJSPubMsg(tsubj, _EMPTY_, _EMPTY_, hdr, rpMsg, nil, seq))
	}

	// If using fast batch publish, we occasionally send flow control messages.
	// And, we need to ensure a PubAck is sent if the commit happens through EOB.
	if fastBatch != nil {
		if mset.batches == nil {
			mset.batches = &batching{}
		}
		mset.batches.mu.Lock()
		// Check full leader state so we only send the client an update once we're caught up.
		commit := mset.batches.fastBatchRegisterSequences(mset, reply, mset.lseq, mset.isLeader(), fastBatch)
		mset.batches.mu.Unlock()
		if !commit {
			reply = _EMPTY_
			canRespond = false
		}
	}

	// Send response here.
	if canRespond {
		response = append(pubAck, strconv.FormatUint(seq, 10)...)
		if batchId != _EMPTY_ {
			response = append(response, fmt.Sprintf(",\"batch\":%q,\"count\":%d}", batchId, batchSeq)...)
		} else if allowMsgCounter {
			var counter CounterValue
			json.Unmarshal(msg, &counter)
			response = append(response, fmt.Sprintf(",\"val\":%q}", counter.Value)...)
		} else {
			response = append(response, '}')
		}
		outq.sendMsg(reply, response)
	}

	// Signal consumers for new messages.
	if numConsumers > 0 {
		mset.sigq.push(newCMsg(subject, seq))
		select {
		case mset.sch <- struct{}{}:
		default:
		}
	}

	return nil
}

// Lock should be held.
func (mset *stream) processJetStreamMsgWithRollup(subject string, rollupSub, rollupAll bool, hdr []byte, keep uint64) error {
	if rollupSub {
		if _, err := mset.purgeLocked(&JSApiStreamPurgeRequest{Subject: subject, Keep: keep}, false); err != nil {
			return err
		}
	} else if rollupAll {
		if _, err := mset.purgeLocked(&JSApiStreamPurgeRequest{Keep: keep}, false); err != nil {
			return err
		}
	}
	if scheduleNext := sliceHeader(JSScheduleNext, hdr); len(scheduleNext) > 0 && bytesToString(scheduleNext) == JSScheduleNextPurge {
		// Purge the message schedule.
		scheduler := getMessageScheduler(hdr)
		if scheduler != _EMPTY_ {
			if _, err := mset.purgeLocked(&JSApiStreamPurgeRequest{Subject: scheduler}, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// processJetStreamAtomicBatchMsg processes a JetStream message that's part of an atomic batch publish.
// Handles constraints around the batch, storing messages, doing consistency checks, and performing the commit.
func (mset *stream) processJetStreamAtomicBatchMsg(batchId, subject, reply string, hdr, msg []byte, mt *msgTrace) (retErr error) {
	mset.mu.RLock()
	canRespond := !mset.cfg.NoAck && len(reply) > 0
	name, stype := mset.cfg.Name, mset.cfg.Storage
	discard, discardNewPer, maxMsgs, maxMsgsPer, maxBytes := mset.cfg.Discard, mset.cfg.DiscardNewPer, mset.cfg.MaxMsgs, mset.cfg.MaxMsgsPer, mset.cfg.MaxBytes
	s, js, jsa, r, tierName, outq, node := mset.srv, mset.js, mset.jsa, mset.cfg.Replicas, mset.tier, mset.outq, mset.node
	maxMsgSize, lseq := int(mset.cfg.MaxMsgSize), mset.lseq
	isLeader, isClustered, isSealed, allowRollup, denyPurge, allowTTL, allowMsgCounter, allowMsgSchedules, allowAtomicPublish := mset.isLeader(), mset.isClustered(), mset.cfg.Sealed, mset.cfg.AllowRollup, mset.cfg.DenyPurge, mset.cfg.AllowMsgTTL, mset.cfg.AllowMsgCounter, mset.cfg.AllowMsgSchedules, mset.cfg.AllowAtomicPublish
	mset.mu.RUnlock()

	// Errors before message processing are reported here. A coalesced R1
	// durability boundary also defers the successful trace until its final
	// sync; other successful traces are sent from processJetStreamMsg.
	deferResponse := false
	if mt != nil {
		defer func() {
			if retErr != nil || deferResponse {
				mt.sendEventFromJetStream(retErr)
			}
		}()
	}

	// Check that we are the leader. This can be false if we have scaled up from an R1 that had inbound queued messages.
	if !isLeader {
		return NewJSClusterNotLeaderError()
	}

	respondError := func(apiErr *ApiError) error {
		if canRespond {
			buf, _ := json.Marshal(&JSPubAckResponse{PubAck: &PubAck{Stream: name}, Error: apiErr})
			outq.sendMsg(reply, buf)
		}
		return apiErr
	}

	// Bail here if sealed.
	if isSealed {
		return respondError(NewJSStreamSealedError())
	}

	// Check here pre-emptively if we have exceeded this server limits.
	if js.limitsExceeded(stype) {
		s.resourcesExceededError(stype)
		// Stepdown regardless.
		if node := mset.raftNode(); node != nil {
			node.StepDown()
		}
		return respondError(NewJSInsufficientResourcesError())
	}

	// Check here pre-emptively if we have exceeded our account limits.
	if exceeded, err := jsa.wouldExceedLimits(stype, tierName, r, subject, hdr, msg); exceeded {
		if err == nil {
			err = NewJSAccountResourcesExceededError()
		}
		s.RateLimitWarnf("JetStream account limits exceeded for '%s': %s", jsa.acc().GetName(), err.Error())
		return respondError(err)
	}

	// Check msgSize if we have a limit set there. Again this works if it goes through but better to be pre-emptive.
	// Subtract to prevent against overflows.
	if maxMsgSize >= 0 && (len(hdr) > maxMsgSize || len(msg) > maxMsgSize-len(hdr)) {
		err := fmt.Errorf("JetStream message size exceeds limits for '%s > %s'", jsa.acc().Name, mset.cfg.Name)
		s.RateLimitWarnf("%s", err.Error())
		_ = respondError(NewJSStreamMessageExceedsMaximumError())
		return err
	}

	if !allowAtomicPublish {
		return respondError(NewJSAtomicPublishDisabledError())
	}

	// Batch ID is too long.
	if len(batchId) > 64 {
		return respondError(NewJSAtomicPublishInvalidBatchIDError())
	}

	batchSeq, exists := getBatchSequence(hdr)
	if !exists {
		return respondError(NewJSAtomicPublishMissingSeqError())
	}

	jsa.mu.RLock()
	storeDir := jsa.storeDir
	jsa.mu.RUnlock()

	// Parse the batch commit header.
	var commit, commitEob, commitInvalid bool
	if c := sliceHeader(JSBatchCommit, hdr); c != nil {
		commitEob = bytes.Equal(c, []byte("eob"))
		commitInvalid = !commitEob && !bytes.Equal(c, []byte("1"))
		commit = true
	}

	// If not clustered, the commit message runs the consistency checks and
	// commits straight to the store below, so hold the isolation lock across
	// that whole section.
	if !isClustered && commit {
		mset.isolateMu.Lock()
		defer mset.isolateMu.Unlock()
	}
	mset.mu.Lock()
	if mset.batches == nil {
		mset.batches = &batching{}
	}
	batches := mset.batches
	// Acquire the batches lock.
	// Can't release the stream lock now, we need to keep holding it while we hold the batches lock.
	// Re-acquiring the stream lock with the batches lock already held would be a lock inversion.
	batches.mu.Lock()

	respondIncompleteBatch := func() error {
		return respondError(NewJSAtomicPublishIncompleteBatchError())
	}

	// Get batch.
	b, ok := batches.atomic[batchId]
	if !ok {
		if batchSeq != 1 {
			batches.mu.Unlock()
			mset.mu.Unlock()
			maxBatchSize := streamMaxAtomicBatchSize
			opts := s.getOpts()
			if opts.JetStreamLimits.MaxBatchSize > 0 {
				maxBatchSize = opts.JetStreamLimits.MaxBatchSize
			}
			size := batchSeq
			// Don't count the "End Of Batch" marker toward the batch size.
			if commitEob {
				size--
			}
			if size > uint64(maxBatchSize) {
				err := NewJSAtomicPublishTooLargeBatchError(maxBatchSize)
				return respondError(err)
			}
			return respondIncompleteBatch()
		}

		// Limits.
		maxInflightPerStream := streamMaxAtomicBatchInflightPerStream
		maxInflightTotal := streamMaxAtomicBatchInflightTotal
		opts := s.getOpts()
		if opts.JetStreamLimits.MaxBatchInflightPerStream > 0 {
			maxInflightPerStream = opts.JetStreamLimits.MaxBatchInflightPerStream
		}
		if opts.JetStreamLimits.MaxBatchInflightTotal > 0 {
			maxInflightTotal = opts.JetStreamLimits.MaxBatchInflightTotal
		}

		// Confirm we can facilitate an additional batch.
		if len(batches.atomic)+1 > maxInflightPerStream {
			batches.mu.Unlock()
			mset.mu.Unlock()
			return respondError(NewJSAtomicPublishTooManyInflightError())
		}

		// Confirm we'll not exceed the server limit.
		if globalInflightAtomicBatches.Add(1) > int64(maxInflightTotal) {
			globalInflightAtomicBatches.Add(-1)
			batches.mu.Unlock()
			mset.mu.Unlock()
			return respondError(NewJSAtomicPublishTooManyInflightError())
		}

		var err error
		b, err = batches.newAtomicBatch(mset, batchId, r, stype, storeDir, name)
		if err != nil {
			globalInflightAtomicBatches.Add(-1)
			batches.mu.Unlock()
			mset.mu.Unlock()
			return respondIncompleteBatch()
		}
		if batches.atomic == nil {
			batches.atomic = make(map[string]*atomicBatch, 1)
		}
		batches.atomic[batchId] = b
	}

	// Reject the batch if the commit is not recognized.
	if commitInvalid {
		b.cleanupLocked(batchId, batches)
		batches.mu.Unlock()
		mset.mu.Unlock()
		err := NewJSAtomicPublishInvalidBatchCommitError()
		return respondError(err)
	}

	// The required API level can have the batch be rejected. But the header is always removed.
	if len(sliceHeader(JSRequiredApiLevel, hdr)) != 0 {
		if errorOnRequiredApiLevel(hdr) {
			b.cleanupLocked(batchId, batches)
			batches.mu.Unlock()
			mset.mu.Unlock()
			mset.sendStreamBatchAbandonedAdvisory(batchId, BatchRequirementsNotMet)
			err := NewJSRequiredApiLevelError()
			return respondError(err)
		}
		hdr = removeHeaderIfPresent(hdr, JSRequiredApiLevel)
	}

	// If cleanup has already happened, we can't continue.
	cleanup := !b.resetCleanupTimer(mset)

	// Detect gaps.
	b.lseq++
	if b.lseq != batchSeq || cleanup || (batchSeq == 1 && commitEob) {
		b.cleanupLocked(batchId, batches)
		batches.mu.Unlock()
		mset.mu.Unlock()
		mset.sendStreamBatchAbandonedAdvisory(batchId, BatchIncomplete)
		return respondIncompleteBatch()
	}

	// Confirm the batch doesn't exceed the allowed size.
	maxSize := streamMaxAtomicBatchSize
	if maxBatchSize := s.getOpts().JetStreamLimits.MaxBatchSize; maxBatchSize > 0 {
		maxSize = maxBatchSize
	}
	size := batchSeq
	// Don't count the "End Of Batch" marker toward the batch size.
	if commitEob {
		size--
	}
	if size > uint64(maxSize) {
		b.cleanupLocked(batchId, batches)
		batches.mu.Unlock()
		mset.mu.Unlock()
		mset.sendStreamBatchAbandonedAdvisory(batchId, BatchLarge)
		err := NewJSAtomicPublishTooLargeBatchError(maxSize)
		return respondError(err)
	}

	// Persist, but optimize if we're committing because we already know last.
	// If the underlying store is file-based we need to persist everything to survive hard kills, because we're R1.
	if !commit || b.store.Type() == FileStorage {
		seq, _, err := b.store.StoreMsg(subject, hdr, msg, 0)
		if err != nil || seq != batchSeq {
			b.cleanupLocked(batchId, batches)
			batches.mu.Unlock()
			mset.mu.Unlock()
			mset.sendStreamBatchAbandonedAdvisory(batchId, BatchIncomplete)
			return respondIncompleteBatch()
		}
	}
	if !commit {
		batches.mu.Unlock()
		mset.mu.Unlock()
		// Send empty ack to let them know we've persisted the data prior to commit.
		if canRespond {
			outq.sendMsg(reply, nil)
		}
		return nil
	}

	// Ensure the batch is prepared for the commit and will not be cleaned up while committing.
	if abandonReason := b.readyForCommit(); abandonReason != nil {
		// Don't do cleanup, this is already done.
		batches.mu.Unlock()
		mset.mu.Unlock()
		mset.sendStreamBatchAbandonedAdvisory(batchId, *abandonReason)
		return respondIncompleteBatch()
	}

	// Proceed with proposing the batch.
	ts := time.Now().UnixNano()

	// Need to hold this lock even if we're not clustered, because we'll
	// be accessing state that requires this lock (even if it's empty).
	mset.clMu.Lock()

	// If not clustered, set up the last sequence.
	if !isClustered {
		mset.clseq, mset.clfs = mset.lseq, 0
	}

	// We only use mset.clseq for clustering and in case we run ahead of actual commits.
	// Check if we need to set initial value here
	if isClustered && (mset.clseq == 0 || mset.clseq < lseq+mset.clfs) {
		lseq = recalculateClusteredSeq(mset, false)
	}

	oclseq := mset.clseq
	rollback := func() {
		if isClustered {
			// Only need to move the clustered sequence back if the batch fails to commit.
			// Other changes were staged but not applied, so this is the only thing we need to do.
			mset.clseq = oclseq
		}
		mset.clMu.Unlock()
	}

	errorOnUnsupported := func(header string) *ApiError {
		apiErr := NewJSAtomicPublishUnsupportedHeaderBatchError(header)
		rollback()
		b.cleanupLocked(batchId, batches)
		batches.mu.Unlock()
		mset.mu.Unlock()
		_ = respondError(apiErr)
		return apiErr
	}

	type checkedMsg struct {
		subj string
		hdr  []byte
		msg  []byte
	}
	var (
		entries []*Entry
		checked []checkedMsg
		bsubj   string
		bhdr    []byte
		bmsg    []byte
		apiErr  *ApiError
		err     error
		smv     StoreMsg
		sm      *StoreMsg
		sz      int
	)

	// If the commit ends with an "End Of Batch" message, we don't store this.
	if commitEob {
		batchSeq--
	}

	diff := &batchStagedDiff{}
	for seq := uint64(1); seq <= batchSeq; seq++ {
		if seq == batchSeq && !commitEob && b.store.Type() != FileStorage {
			bsubj, bhdr, bmsg = subject, hdr, msg
		} else if sm, err = b.store.LoadMsg(seq, &smv); sm != nil && err == nil {
			bsubj, bhdr, bmsg = sm.subj, sm.hdr, sm.msg
		} else {
			rollback()
			b.cleanupLocked(batchId, batches)
			batches.mu.Unlock()
			mset.mu.Unlock()
			return respondIncompleteBatch()
		}

		// Apply the input subject transform if any
		csubj := bsubj
		if mset.itr != nil {
			ts, err := mset.itr.Match(csubj)
			if err == nil {
				// no filtering: if the subject doesn't map the source of the transform, don't change it
				csubj = ts
			}
		}

		// Reject unsupported headers.
		if getExpectedLastMsgId(bhdr) != _EMPTY_ {
			return errorOnUnsupported(JSExpectedLastMsgId)
		}

		if bhdr, bmsg, _, apiErr, err = checkMsgHeadersPreClusteredProposal(diff, mset, csubj, bsubj, bhdr, bmsg, false, name, jsa, allowRollup, denyPurge, allowTTL, allowMsgCounter, allowMsgSchedules, discard, discardNewPer, maxMsgSize, maxMsgs, maxMsgsPer, maxBytes); err != nil {
			rollback()
			b.cleanupLocked(batchId, batches)
			batches.mu.Unlock()
			mset.mu.Unlock()
			_ = respondError(apiErr)
			return err
		}

		if isClustered {
			var _reply string
			isCommit := seq == batchSeq
			if isCommit {
				_reply = reply
				// If committed by EOB, the last message must get the normal commit header.
				if commitEob {
					bhdr = genHeader(bhdr, JSBatchCommit, "1")
				}
			}
			esm := encodeStreamMsgAllowCompressAndBatch(bsubj, _reply, bhdr, bmsg, mset.clseq, ts, false, batchId, seq, isCommit)
			entries = append(entries, newEntry(EntryNormal, esm))
			sz += len(esm)
		} else {
			// Preserve the (possibly rewritten) headers and message, for example for counters and
			// scheduled messages, so the commit below stores the transformed message.
			// Need to copy, the staged message buffer is reused on every load.
			checked = append(checked, checkedMsg{bsubj, copyBytes(bhdr), copyBytes(bmsg)})
		}
		mset.clseq++
	}

	// Commit batch.
	if !isClustered {
		// Reset, we only used this to do the batching checks.
		mset.clseq, mset.clfs = 0, 0
		mset.clMu.Unlock()

		// Ensure the whole batch is fully isolated, and reads can only happen
		// after the full batch is committed. isolateMu stays held until return
		// via the defer above, but release mset.mu and batches.mu.
		mset.mu.Unlock()
		batches.mu.Unlock()

		fs, hasFileStore := mset.store.(*fileStore)
		coalesceSync := false
		if hasFileStore {
			fs.mu.Lock()
			coalesceSync = fs.syncAlways.Load()
			if coalesceSync {
				fs.updateDurabilitySettingsLocked(true)
			}
			fs.mu.Unlock()
		}
		deferResponse = coalesceSync
		durabilityReset := !coalesceSync
		defer func() {
			if !durabilityReset {
				fs.mu.Lock()
				fs.resetDurabilitySettingsLocked()
				fs.mu.Unlock()
			}
		}()

		for seq := uint64(1); seq <= batchSeq; seq++ {
			// Use the checked (and possibly rewritten) message from above, not the raw staged
			// message, so transformations like counter increments and scheduled message
			// rollups are persisted just like in the clustered proposal path.
			cm := checked[seq-1]
			bsubj, bhdr, bmsg = cm.subj, cm.hdr, cm.msg
			var _reply string
			if seq == batchSeq {
				if !coalesceSync {
					_reply = reply
				}
				// If committed by EOB, the last message must get the normal commit header.
				if commitEob {
					bhdr = genHeader(bhdr, JSBatchCommit, "1")
				}
			}
			// If errored, assume all subsequent calls will fail too (for example, store is closed).
			// Don't clean up the batch so that a restart can try to recover it.
			if err = mset.processJetStreamMsgWithBatch(bsubj, _reply, bhdr, bmsg, 0, 0, mt, false, false, nil, deferResponse); err != nil {
				return err
			}
		}
		if coalesceSync {
			if err = fs.FlushAllPending(); err != nil {
				mset.setWriteErr(err)
				return err
			}
			fs.mu.Lock()
			fs.resetDurabilitySettingsLocked()
			fs.mu.Unlock()
			durabilityReset = true
			mset.mu.RLock()
			lseq, pubAck, outq := mset.lseq, mset.pubAck, mset.outq
			mset.mu.RUnlock()
			if canRespond && !mt.traceOnly() {
				var buf [256]byte
				response := append(buf[:0], pubAck...)
				response = append(response, strconv.FormatUint(lseq, 10)...)
				response = append(response, fmt.Sprintf(",\"batch\":%q,\"count\":%d}", batchId, batchSeq)...)
				outq.sendMsg(reply, response)
			}
		}
		// Re-acquire for the cleanup below. If a concurrent staging error
		// already cleaned the batch up, cleanupLocked is a no-op.
		batches.mu.Lock()
	} else {
		term := mset.term
		mset.mu.Unlock()
		// Do a single multi proposal. This ensures we get to push all entries to the proposal queue in-order
		// and not interleaved with other proposals.
		if err = node.ProposeMulti(term, entries); err == nil {
			diff.commit(mset)
			mset.trackReplicationTraffic(node, sz, r)

			// Check to see if we are being overrun.
			// TODO(dlc) - Make this a limit where we drop messages to protect ourselves, but allow to be configured.
			if mset.clseq-(lseq+mset.clfs) > streamLagWarnThreshold {
				lerr := fmt.Errorf("JetStream stream '%s > %s' has high message lag", jsa.acc().Name, name)
				s.RateLimitWarnf("%s", lerr.Error())
			}
		} else {
			mset.clseq = oclseq
		}
		mset.clMu.Unlock()
	}
	b.cleanupLocked(batchId, batches)
	batches.mu.Unlock()
	return err
}

// processJetStreamFastBatchMsg processes a JetStream message that's part of an atomic batch publish.
// Handles constraints around the batch, storing messages, doing consistency checks, and performing the commit.
func (mset *stream) processJetStreamFastBatchMsg(batch *FastBatch, subject, reply string, hdr, msg []byte, mt *msgTrace) (retErr error) {
	mset.mu.RLock()
	canRespond := !mset.cfg.NoAck && len(reply) > 0
	name, stype := mset.cfg.Name, mset.cfg.Storage
	discard, discardNewPer, maxMsgs, maxMsgsPer, maxBytes := mset.cfg.Discard, mset.cfg.DiscardNewPer, mset.cfg.MaxMsgs, mset.cfg.MaxMsgsPer, mset.cfg.MaxBytes
	s, js, jsa, st, r, tierName, outq, node, term := mset.srv, mset.js, mset.jsa, mset.cfg.Storage, mset.cfg.Replicas, mset.tier, mset.outq, mset.node, mset.term
	maxMsgSize, lseq := int(mset.cfg.MaxMsgSize), mset.lseq
	isLeader, isClustered, isSealed, allowRollup, denyPurge, allowTTL, allowMsgCounter, allowMsgSchedules, allowBatchPublish := mset.isLeader(), mset.isClustered(), mset.cfg.Sealed, mset.cfg.AllowRollup, mset.cfg.DenyPurge, mset.cfg.AllowMsgTTL, mset.cfg.AllowMsgCounter, mset.cfg.AllowMsgSchedules, mset.cfg.AllowBatchPublish

	// Apply the input subject transform if any
	csubject := subject
	if mset.itr != nil {
		ts, err := mset.itr.Match(csubject)
		if err == nil {
			// no filtering: if the subject doesn't map the source of the transform, don't change it
			csubject = ts
		}
	}
	mset.mu.RUnlock()

	// If message tracing (with message delivery), we will need to send the
	// event on exit in case there was an error (if message was not proposed).
	// Otherwise, the event will be sent from processJetStreamMsg when
	// invoked by the leader (from applyStreamEntries).
	if mt != nil {
		defer func() {
			if retErr != nil {
				mt.sendEventFromJetStream(retErr)
			}
		}()
	}

	// Check that we are the leader. This can be false if we have scaled up from an R1 that had inbound queued messages.
	if !isLeader {
		return NewJSClusterNotLeaderError()
	}

	respondError := func(apiErr *ApiError) error {
		if canRespond {
			buf, _ := json.Marshal(&JSPubAckResponse{PubAck: &PubAck{Stream: name}, Error: apiErr})
			outq.sendMsg(reply, buf)
		}
		return apiErr
	}

	// Bail here if sealed.
	if isSealed {
		return respondError(NewJSStreamSealedError())
	}

	// Check here pre-emptively if we have exceeded this server limits.
	if js.limitsExceeded(stype) {
		s.resourcesExceededError(stype)
		// Stepdown regardless.
		if node := mset.raftNode(); node != nil {
			node.StepDown()
		}
		return respondError(NewJSInsufficientResourcesError())
	}

	// Check here pre-emptively if we have exceeded our account limits.
	if exceeded, err := jsa.wouldExceedLimits(st, tierName, r, csubject, hdr, msg); exceeded {
		if err == nil {
			err = NewJSAccountResourcesExceededError()
		}
		s.RateLimitWarnf("JetStream account limits exceeded for '%s': %s", jsa.acc().GetName(), err.Error())
		return respondError(err)
	}

	// Check msgSize if we have a limit set there. Again this works if it goes through but better to be pre-emptive.
	// Subtract to prevent against overflows.
	if maxMsgSize >= 0 && (len(hdr) > maxMsgSize || len(msg) > maxMsgSize-len(hdr)) {
		err := fmt.Errorf("JetStream message size exceeds limits for '%s > %s'", jsa.acc().Name, mset.cfg.Name)
		s.RateLimitWarnf("%s", err.Error())
		_ = respondError(NewJSStreamMessageExceedsMaximumError())
		return err
	}

	if !allowBatchPublish {
		return respondError(NewJSBatchPublishDisabledError())
	}

	if batch == nil {
		return respondError(NewJSBatchPublishInvalidPatternError())
	}

	// Batch ID is too long.
	if len(batch.id) > 64 {
		return respondError(NewJSBatchPublishInvalidBatchIDError())
	}

	mset.mu.Lock()
	if mset.batches == nil {
		mset.batches = &batching{}
	}
	batches := mset.batches
	// Acquire the batches lock.
	// Can't release the stream lock now, we need to keep holding it while we hold the batches lock.
	// Re-acquiring the stream lock with the batches lock already held would be a lock inversion.
	batches.mu.Lock()

	// Get batch.
	b, ok := batches.fast[batch.id]
	if !ok {
		// A new batch can only be started at sequence 1 and not by a ping.
		if batch.seq != 1 || batch.ping {
			batches.mu.Unlock()
			mset.mu.Unlock()
			return respondError(NewJSBatchPublishUnknownBatchIDError())
		}

		// Limits.
		maxInflightPerStream := streamMaxFastBatchInflightPerStream
		maxInflightTotal := streamMaxFastBatchInflightTotal
		opts := s.getOpts()
		if opts.JetStreamLimits.MaxBatchInflightPerStream > 0 {
			maxInflightPerStream = opts.JetStreamLimits.MaxBatchInflightPerStream
		}
		if opts.JetStreamLimits.MaxBatchInflightTotal > 0 {
			maxInflightTotal = opts.JetStreamLimits.MaxBatchInflightTotal
		}

		// Confirm we can facilitate an additional batch.
		if len(batches.fast)+1 > maxInflightPerStream {
			batches.mu.Unlock()
			mset.mu.Unlock()
			return respondError(NewJSBatchPublishTooManyInflightError())
		}

		// Confirm we'll not exceed the server limit.
		if globalInflightFastBatches.Add(1) > int64(maxInflightTotal) {
			globalInflightFastBatches.Add(-1)
			batches.mu.Unlock()
			mset.mu.Unlock()
			return respondError(NewJSBatchPublishTooManyInflightError())
		}

		// We'll need a copy as we'll use it as a key and later for cleanup.
		batchId := copyString(batch.id)
		b = batches.newFastBatch(mset, batchId, batch.gapOk, batch.flow)
	}

	// The required API level can have the batch be rejected. But the header is always removed.
	if len(sliceHeader(JSRequiredApiLevel, hdr)) != 0 {
		if errorOnRequiredApiLevel(hdr) {
			b.cleanupLocked(batch.id, batches)
			batches.mu.Unlock()
			mset.mu.Unlock()
			return respondError(NewJSRequiredApiLevelError())
		}
		hdr = removeHeaderIfPresent(hdr, JSRequiredApiLevel)
	}

	// Fast publishing resets the cleanup timer.
	// If cleanup has already happened, we can't continue.
	cleanup := !b.resetCleanupTimer(mset)

	// A ping operation confirms we've received a minimum amount of data and resends ack messages.
	if batch.ping {
		sendFlowControl := true
		// Detect a gap or if the batch was cleaned up in the meantime.
		if batch.seq > b.lseq || cleanup {
			// If a gap is detected, we always report about it.
			buf, _ := BatchFlowGap{ExpectedLastSequence: b.lseq + 1, CurrentSequence: batch.seq + 1}.MarshalJSON()
			outq.sendMsg(reply, buf)
			// If the gap is okay, we can continue without rejecting.
			if b.gapOk && !cleanup {
				b.lseq = batch.seq
				if b.pending == 0 {
					b.pseq = b.lseq
				}
				sendFlowControl = !b.checkFlowControl(mset, reply, batches)
			} else if cleanup = batches.fastBatchCommit(b, batch.id, mset, reply); cleanup {
				b.cleanupLocked(batch.id, batches)
				sendFlowControl = false
			}
		}
		if sendFlowControl {
			b.sendFlowControl(b.fseq, mset, reply)
		}
		batches.mu.Unlock()
		mset.mu.Unlock()
		return nil
	}

	// If the batch is committing, due to an error, we can't add more messages.
	// We simply skip, since the client will be waiting for the PubAck.
	if b.commit {
		// MUST NOT clean up, that will happen when the commit completes.
		batches.mu.Unlock()
		mset.mu.Unlock()
		return nil
	}

	// Detect gaps.
	b.lseq++
	if b.lseq != batch.seq || cleanup {
		// If a forward or backward gap is detected, we always report about it.
		if batch.seq != b.lseq {
			buf, _ := BatchFlowGap{ExpectedLastSequence: b.lseq, CurrentSequence: batch.seq}.MarshalJSON()
			outq.sendMsg(reply, buf)
		}
		// If the forward gap is okay, we can continue without rejecting.
		if b.gapOk && !cleanup && batch.seq > b.lseq {
			b.lseq = batch.seq
		} else {
			// We've reached either a backward gap, or were cleaned up already, or it's gap-fail mode.
			// Revert, since we incremented for the gap check.
			b.lseq--
			if cleanup = batches.fastBatchCommit(b, batch.id, mset, reply); cleanup {
				b.cleanupLocked(batch.id, batches)
			}
			batches.mu.Unlock()
			mset.mu.Unlock()
			return nil
		}
	}

	if batch.commit {
		if batch.commitEob {
			// Revert, since we incremented for the gap check.
			b.lseq--
			// If there is none pending, correct the persisted sequence as we need to commit below.
			if b.pending == 0 {
				b.pseq = b.lseq
			}
		}
		// We'll try to immediately send a PubAck if we can.
		// Only possible if EOB is used and the last message was already persisted
		// Otherwise, this sets up the commit for the last message we're about to propose.
		cleanup = batches.fastBatchCommit(b, batch.id, mset, reply)
		if batch.commitEob {
			if cleanup {
				b.cleanupLocked(batch.id, batches)
			}
			batches.mu.Unlock()
			mset.mu.Unlock()
			return nil
		}
	}

	// The first message in the batch responds with the settings used for flow control.
	// If committing immediately, we only send the PubAck.
	if batch.seq == 1 && canRespond && !batch.commit {
		buf, _ := BatchFlowAck{Sequence: 0, Messages: b.ackMessages}.MarshalJSON()
		outq.sendMsg(reply, buf)
	}

	// Proceed with proposing this message.

	// We only use mset.clseq for clustering and in case we run ahead of actual commits.
	// Check if we need to set initial value here
	mset.clMu.Lock()
	if mset.clseq == 0 || mset.clseq < lseq+mset.clfs {
		lseq = recalculateClusteredSeq(mset, false)
	}
	// We can now unlock, since we've potentially recalculated the clustered seq above.
	mset.mu.Unlock()

	var (
		dseq   uint64
		apiErr *ApiError
		err    error
	)
	diff := &batchStagedDiff{}
	if hdr, msg, dseq, apiErr, err = checkMsgHeadersPreClusteredProposal(diff, mset, csubject, subject, hdr, msg, false, name, jsa, allowRollup, denyPurge, allowTTL, allowMsgCounter, allowMsgSchedules, discard, discardNewPer, maxMsgSize, maxMsgs, maxMsgsPer, maxBytes); err != nil {
		mset.clMu.Unlock()

		// If the message is a duplicate, and we have no pending messages, we should check if we need to
		// send the flow control message here.
		if err == errMsgIdDuplicate {
			if b.pending == 0 {
				b.pseq = batch.seq
				b.checkFlowControl(mset, reply, batches)
			}
			if !batch.commit {
				// Otherwise, just skip.
				batches.mu.Unlock()
				return err
			}
		}

		// If a batch immediately errors, we send the same response as we would a normal publish.
		if !batch.gapOk && b.lseq == 1 {
			var response []byte
			if err == errMsgIdDuplicate && dseq > 0 {
				var buf [256]byte
				response = append(buf[:0], mset.pubAck...)
				response = append(response, strconv.FormatUint(dseq, 10)...)
				response = append(response, fmt.Sprintf(",\"duplicate\": true,\"batch\":%q,\"count\":%d}", batch.id, batch.seq)...)
			} else {
				response, _ = json.Marshal(&JSPubAckResponse{PubAck: &PubAck{Stream: name}, Error: apiErr})
			}
			b.cleanupLocked(batch.id, batches)
			batches.mu.Unlock()
			outq.sendMsg(reply, response)
			return err
		}

		// We always return the error to the client, unless it's a duplicate.
		if err != errMsgIdDuplicate {
			buf, _ := BatchFlowErr{Sequence: batch.seq, Error: apiErr}.MarshalJSON()
			outq.sendMsg(reply, buf)
		}

		// If gaps are okay, we just allow them to continue.
		// Unless this is the commit message, in which case we must still complete it below.
		if batch.gapOk && !batch.commit {
			batches.mu.Unlock()
			return err
		}

		// Revert the last sequence, we might be able to immediately return the PubAck as part of the commit.
		// Otherwise, the batch is cleaned up automatically later.
		if err != errMsgIdDuplicate {
			b.lseq--
			// If there is none pending, correct the persisted sequence as we need to commit below.
			if b.pending == 0 {
				b.pseq = b.lseq
			}
		}
		if cleanup = batches.fastBatchCommit(b, batch.id, mset, reply); cleanup {
			b.cleanupLocked(batch.id, batches)
		}
		batches.mu.Unlock()
		return err
	}
	b.pending++
	batches.mu.Unlock()
	if !isClustered {
		mset.clMu.Unlock()
		return mset.processJetStreamMsgWithBatch(subject, reply, hdr, msg, 0, 0, mt, false, true, batch, false)
	}
	err = commitSingleMsg(diff, mset, subject, reply, hdr, msg, name, jsa, mt, node, term, r, lseq)
	mset.clMu.Unlock()
	return err
}

// Used to signal inbound message to registered consumers.
type cMsg struct {
	seq  uint64
	subj string
}

// Pool to recycle consumer bound msgs.
var cMsgPool sync.Pool

// Used to queue up consumer bound msgs for signaling.
func newCMsg(subj string, seq uint64) *cMsg {
	var m *cMsg
	cm := cMsgPool.Get()
	if cm != nil {
		m = cm.(*cMsg)
	} else {
		m = new(cMsg)
	}
	m.subj, m.seq = subj, seq

	return m
}

func (m *cMsg) returnToPool() {
	if m == nil {
		return
	}
	m.subj, m.seq = _EMPTY_, 0
	cMsgPool.Put(m)
}

// Go routine to signal consumers.
// Offloaded from stream msg processing.
func (mset *stream) signalConsumersLoop() {
	mset.mu.RLock()
	s, qch, sch, msgs := mset.srv, mset.qch, mset.sch, mset.sigq
	mset.mu.RUnlock()

	for {
		select {
		case <-s.quitCh:
			return
		case <-qch:
			return
		case <-sch:
			cms := msgs.pop()
			for _, m := range cms {
				seq, subj := m.seq, m.subj
				m.returnToPool()
				// Signal all appropriate consumers.
				mset.signalConsumers(subj, seq)
			}
			msgs.recycle(&cms)
		}
	}
}

// This will update and signal all consumers that match.
func (mset *stream) signalConsumers(subj string, seq uint64) {
	mset.clsMu.RLock()
	defer mset.clsMu.RUnlock()
	csl := mset.csl
	if csl == nil {
		return
	}
	csl.Match(subj, func(o *consumer) {
		o.processStreamSignal(seq)
	})
}

// Internal message for use by jetstream subsystem.
type jsPubMsg struct {
	dsubj string // Subject to send to, e.g. _INBOX.xxx
	reply string
	StoreMsg
	o *consumer
}

var jsPubMsgPool = sync.Pool{
	New: func() any {
		return &jsPubMsg{}
	},
}

func newJSPubMsg(dsubj, subj, reply string, hdr, msg []byte, o *consumer, seq uint64) *jsPubMsg {
	m := getJSPubMsgFromPool()
	if m.buf == nil {
		m.buf = make([]byte, 0, len(hdr)+len(msg))
	}
	buf := append(m.buf[:0], hdr...)
	buf = append(buf, msg...)
	hdr = buf[:len(hdr):len(hdr)]
	msg = buf[len(hdr):]
	// When getting something from a pool it is critical that all fields are
	// initialized. Doing this way guarantees that if someone adds a field to
	// the structure, the compiler will fail the build if this line is not updated.
	(*m) = jsPubMsg{dsubj, reply, StoreMsg{subj, hdr, msg, buf, seq, 0}, o}
	return m
}

// Gets a jsPubMsg from the pool.
func getJSPubMsgFromPool() *jsPubMsg {
	return jsPubMsgPool.Get().(*jsPubMsg)
}

func (pm *jsPubMsg) returnToPool() {
	if pm == nil {
		return
	}
	pm.subj, pm.dsubj, pm.reply, pm.hdr, pm.msg, pm.o = _EMPTY_, _EMPTY_, _EMPTY_, nil, nil, nil
	if len(pm.buf) > 0 {
		pm.buf = pm.buf[:0]
	}
	jsPubMsgPool.Put(pm)
}

func (pm *jsPubMsg) size() int {
	if pm == nil {
		return 0
	}
	return len(pm.dsubj) + len(pm.reply) + len(pm.hdr) + len(pm.msg)
}

// Queue of *jsPubMsg for sending internal system messages.
type jsOutQ struct {
	*ipQueue[*jsPubMsg]
}

func (q *jsOutQ) sendMsg(subj string, msg []byte) {
	if q != nil {
		q.send(newJSPubMsg(subj, _EMPTY_, _EMPTY_, nil, msg, nil, 0))
	}
}

func (q *jsOutQ) send(msg *jsPubMsg) {
	if q == nil || msg == nil {
		return
	}
	q.push(msg)
}

func (q *jsOutQ) unregister() {
	if q == nil {
		return
	}
	q.ipQueue.unregister()
}

// StoredMsg is for raw access to messages in a stream.
type StoredMsg struct {
	Subject  string    `json:"subject"`
	Sequence uint64    `json:"seq"`
	Header   []byte    `json:"hdrs,omitempty"`
	Data     []byte    `json:"data,omitempty"`
	Time     time.Time `json:"time"`
}

// This is similar to system semantics but did not want to overload the single system sendq,
// or require system account when doing simple setup with jetstream.
func (mset *stream) setupSendCapabilities() {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	if mset.outq != nil {
		return
	}
	qname := fmt.Sprintf("[ACC:%s] stream '%s' sendQ", mset.acc.Name, mset.cfg.Name)
	mset.outq = &jsOutQ{newIPQueue[*jsPubMsg](mset.srv, qname)}
	go mset.internalLoop()
}

// Returns the associated account name.
func (mset *stream) accName() string {
	if mset == nil {
		return _EMPTY_
	}
	mset.mu.RLock()
	acc := mset.acc
	mset.mu.RUnlock()
	return acc.Name
}

// Name returns the stream name.
func (mset *stream) name() string {
	return mset.nameLocked(true)
}

func (mset *stream) nameLocked(needLock bool) string {
	if mset == nil {
		return _EMPTY_
	}
	if needLock {
		mset.mu.RLock()
		defer mset.mu.RUnlock()
	}
	return mset.cfg.Name
}

func (mset *stream) internalLoop() {
	mset.mu.RLock()
	setGoRoutineLabels(pprofLabels{
		"account": mset.acc.Name,
		"stream":  mset.cfg.Name,
	})
	s := mset.srv
	c := s.createInternalJetStreamClient()
	c.registerWithAccount(mset.acc)
	defer c.closeConnection(ClientClosed)
	outq, qch, msgs, gets := mset.outq, mset.qch, mset.msgs, mset.gets

	// For the ack msgs queue for interest retention.
	var (
		amch chan struct{}
		ackq *ipQueue[uint64]
	)
	if mset.ackq != nil {
		ackq, amch = mset.ackq, mset.ackq.ch
	}
	mset.mu.RUnlock()

	// Raw scratch buffer.
	// This should be rarely used now so can be smaller.
	var _r [1024]byte

	// To optimize for not converting a string to a []byte slice.
	var (
		subj  [256]byte
		dsubj [256]byte
		rply  [256]byte
		szb   [10]byte
		hdb   [10]byte
	)

	for {
		select {
		case <-outq.ch:
			pms := outq.pop()
			for _, pm := range pms {
				c.pa.subject = append(dsubj[:0], pm.dsubj...)
				c.pa.deliver = append(subj[:0], pm.subj...)
				c.pa.size = len(pm.msg) + len(pm.hdr)
				c.pa.szb = append(szb[:0], strconv.Itoa(c.pa.size)...)
				if len(pm.reply) > 0 {
					c.pa.reply = append(rply[:0], pm.reply...)
				} else {
					c.pa.reply = nil
				}

				// If we have an underlying buf that is the wire contents for hdr + msg, else construct on the fly.
				var msg []byte
				if len(pm.buf) > 0 {
					msg = pm.buf
				} else {
					if len(pm.hdr) > 0 {
						msg = pm.hdr
						if len(pm.msg) > 0 {
							msg = _r[:0]
							msg = append(msg, pm.hdr...)
							msg = append(msg, pm.msg...)
						}
					} else if len(pm.msg) > 0 {
						// We own this now from a low level buffer perspective so can use directly here.
						msg = pm.msg
					}
				}

				if len(pm.hdr) > 0 {
					c.pa.hdr = len(pm.hdr)
					c.pa.hdb = []byte(strconv.Itoa(c.pa.hdr))
					c.pa.hdb = append(hdb[:0], strconv.Itoa(c.pa.hdr)...)
				} else {
					c.pa.hdr = -1
					c.pa.hdb = nil
				}

				msg = append(msg, _CRLF_...)

				didDeliver, _ := c.processInboundClientMsg(msg)
				c.pa.szb, c.pa.subject, c.pa.deliver = nil, nil, nil

				// Check to see if this is a delivery for a consumer and
				// we failed to deliver the message. If so alert the consumer.
				if pm.o != nil && pm.seq > 0 && !didDeliver {
					pm.o.didNotDeliver(pm.seq, pm.dsubj)
				}
				pm.returnToPool()
			}
			// TODO: Move in the for-loop?
			c.flushClients(0)
			outq.recycle(&pms)
		case <-msgs.ch:
			// This can possibly change now so needs to be checked here.
			isClustered := mset.IsClustered()
			ims := msgs.pop()
			for _, im := range ims {
				// If we are clustered we need to propose this message to the underlying raft group.
				if batch, err := getFastBatch(im.rply, im.hdr); batch != nil || err {
					mset.processJetStreamFastBatchMsg(batch, im.subj, im.rply, im.hdr, im.msg, im.mt)
					batch.returnToPool()
				} else if batchId := getBatchId(im.hdr); batchId != _EMPTY_ {
					mset.processJetStreamAtomicBatchMsg(batchId, im.subj, im.rply, im.hdr, im.msg, im.mt)
				} else if isClustered {
					mset.processClusteredInboundMsg(im.subj, im.rply, im.hdr, im.msg, im.mt, false)
				} else {
					mset.processJetStreamMsg(im.subj, im.rply, im.hdr, im.msg, 0, 0, im.mt, false, true)
				}
				im.returnToPool()
			}
			msgs.recycle(&ims)
		case <-gets.ch:
			dgs := gets.pop()
			for _, dg := range dgs {
				mset.getDirectRequest(&dg.req, dg.reply)
				dgPool.Put(dg)
			}
			gets.recycle(&dgs)

		case <-amch:
			seqs := ackq.pop()
			for _, seq := range seqs {
				mset.ackMsg(nil, seq)
			}
			ackq.recycle(&seqs)
		case <-qch:
			return
		case <-s.quitCh:
			return
		}
	}
}

// Used to break consumers out of their monitorConsumer go routines.
func (mset *stream) resetAndWaitOnConsumers() {
	mset.mu.RLock()
	consumers := make([]*consumer, 0, len(mset.consumers))
	for _, o := range mset.consumers {
		consumers = append(consumers, o)
	}
	mset.mu.RUnlock()

	for _, o := range consumers {
		if node := o.raftNode(); node != nil {
			node.StepDown()
			node.Stop()
		}
		o.stopMonitoring()
	}
}

// Internal function to delete a stream.
func (mset *stream) delete() error {
	if mset == nil {
		return nil
	}
	return mset.stop(true, true)
}

// Internal function to stop or delete the stream.
func (mset *stream) stop(deleteFlag, advisory bool) error {
	mset.mu.RLock()
	js, jsa, name, offlineReason := mset.js, mset.jsa, mset.cfg.Name, mset.offlineReason
	mset.mu.RUnlock()

	if jsa == nil {
		return NewJSNotEnabledForAccountError()
	}

	isShuttingDown := js.isShuttingDown()

	// Remove from our account map first.
	jsa.mu.Lock()
	// Preserve in the account if it's marked offline, to have it remain queryable.
	if deleteFlag || offlineReason == _EMPTY_ {
		delete(jsa.streams, name)
	}
	accName := jsa.account.Name
	jsa.mu.Unlock()

	// Kick monitor and collect consumers first.
	mset.mu.Lock()

	// Mark closed.
	mset.closed.Store(true)

	// Both flags set mean a delete where we are the stream leader.
	// Try to clean up any consumers used for sourcing (if one wasn't provided to us).
	if deleteFlag && advisory {
		if mset.cfg.Mirror != nil && mset.cfg.Mirror.Consumer == nil {
			mset.tryDeleteMirrorConsumer(mset.cfg.Mirror)
		}
		for _, s := range mset.cfg.Sources {
			if s.Consumer == nil {
				id := mset.createSourcingConsumerHash(s, mset.cfg.Sources)
				mset.tryDeleteSourceConsumer(id, s)
			}
		}
	}

	// Signal to the monitor loop.
	// Can't use qch here.
	if mset.mqch != nil {
		close(mset.mqch)
		mset.mqch = nil
	}

	// Stop responding to sync requests.
	mset.stopClusterSubs()
	// Unsubscribe from direct stream.
	mset.unsubscribeToStream(true, isShuttingDown)

	// Our info sub if we spun it up.
	if mset.infoSub != nil {
		mset.srv.sysUnsubscribe(mset.infoSub)
		mset.infoSub = nil
	}

	// Clean up consumers.
	var obs []*consumer
	for _, o := range mset.consumers {
		obs = append(obs, o)
	}
	// Preserve the consumers if it's marked offline, to have them remain queryable.
	if deleteFlag || offlineReason == _EMPTY_ {
		mset.clsMu.Lock()
		mset.consumers, mset.cList, mset.csl = nil, nil, nil
		mset.clsMu.Unlock()
	}

	// Check if we are a mirror.
	if mset.mirror != nil && mset.mirror.sub != nil {
		mset.unsubscribe(mset.mirror.sub)
		mset.mirror.sub = nil
		mset.removeInternalConsumer(mset.mirror)
	}
	// Now check for sources.
	if len(mset.sources) > 0 {
		for _, si := range mset.sources {
			mset.cancelSourceConsumer(si.iname)
		}
	}
	mset.mu.Unlock()

	// If we're shutting down, wait for the monitor routine, which attempts to snapshot.
	if isShuttingDown && !deleteFlag {
		mset.stopMonitoring()
	}

	for _, o := range obs {
		if !o.isClosed() {
			// Third flag says do not broadcast a signal.
			// TODO(dlc) - If we have an err here we don't want to stop
			// but should we log?
			o.stopWithFlags(deleteFlag, deleteFlag, false, advisory)
			if !isShuttingDown {
				o.stopMonitoring()
			}
		}
	}

	mset.mu.Lock()
	// Send stream delete advisory after the consumers.
	if deleteFlag && advisory {
		mset.sendDeleteAdvisoryLocked()
	}

	// Quit channel, do this after sending the delete advisory
	if mset.qch != nil {
		close(mset.qch)
		mset.qch = nil
	}

	// Cluster cleanup
	var sa *streamAssignment
	if n := mset.node; n != nil {
		if deleteFlag {
			n.Delete()
			sa = mset.sa
		} else if !isShuttingDown {
			// Stop Raft, unless JetStream is already shutting down, in which case they'll be stopped separately.
			n.Stop()
		}
	}

	// Cleanup duplicate timer if running.
	mset.ddMu.Lock()
	if mset.ddtmr != nil {
		mset.ddtmr.Stop()
		mset.ddtmr = nil
		mset.ddmap = nil
		mset.ddarr = nil
		mset.ddindex = 0
	}
	mset.ddMu.Unlock()

	sysc := mset.sysc
	mset.sysc = nil

	if deleteFlag {
		// Unregistering ipQueues do not prevent them from push/pop
		// just will remove them from the central monitoring map
		mset.msgs.unregister()
		mset.ackq.unregister()
		mset.outq.unregister()
		mset.sigq.unregister()
		mset.smsgs.unregister()
	}

	// Snapshot store.
	store := mset.store
	c := mset.client

	// Clustered cleanup.
	mset.mu.Unlock()

	// Check if the stream assignment has the group node specified.
	// We need this cleared for if the stream gets reassigned here.
	if sa != nil {
		js.mu.Lock()
		if sa.Group != nil {
			sa.Group.node = nil
		}
		js.mu.Unlock()
	}

	if c != nil {
		c.closeConnection(ClientClosed)
	}

	if sysc != nil {
		sysc.closeConnection(ClientClosed)
	}

	if deleteFlag {
		// cleanup directories after the stream
		if store != nil {
			// Ignore errors.
			store.Delete(false)
		} else {
			accDir := filepath.Join(js.config.StoreDir, accName)
			streamDir := filepath.Join(accDir, streamsDir)
			os.RemoveAll(filepath.Join(streamDir, name))
		}
		// Release any resources.
		js.releaseStreamResources(&mset.cfg)
	} else if store != nil {
		// Ignore errors.
		store.Stop()
	}

	return nil
}

func (mset *stream) getMsg(seq uint64) (*StoredMsg, error) {
	var smv StoreMsg
	sm, err := mset.store.LoadMsg(seq, &smv)
	if err != nil {
		return nil, err
	}
	// This only used in tests directly so no need to pool etc.
	return &StoredMsg{
		Subject:  sm.subj,
		Sequence: sm.seq,
		Header:   sm.hdr,
		Data:     sm.msg,
		Time:     time.Unix(0, sm.ts).UTC(),
	}, nil
}

// getConsumers will return a copy of all the current consumers for this stream.
func (mset *stream) getConsumers() []*consumer {
	mset.clsMu.RLock()
	defer mset.clsMu.RUnlock()
	return append([]*consumer(nil), mset.cList...)
}

// numLimitableConsumers returns the number of consumers that are not direct/sourcing consumers.
// Used to limit the number of consumers for MaxConsumers limits or WQ exclusivity.
// Lock should be held for this one.
func (mset *stream) numLimitableConsumers() int {
	return len(mset.consumers) - mset.sourcingConsumers
}

// This returns all consumers that are not DIRECT.
func (mset *stream) getPublicConsumers() []*consumer {
	mset.clsMu.RLock()
	defer mset.clsMu.RUnlock()

	var obs []*consumer
	for _, o := range mset.cList {
		if !o.direct {
			obs = append(obs, o)
		}
	}
	return obs
}

// This returns all consumers that are DIRECT.
func (mset *stream) getDirectConsumers() []*consumer {
	mset.clsMu.RLock()
	defer mset.clsMu.RUnlock()

	var obs []*consumer
	for _, o := range mset.cList {
		if o.direct {
			obs = append(obs, o)
		}
	}
	return obs
}

// 2 minutes plus up to 30s jitter.
const (
	defaultCheckInterestStateT = 2 * time.Minute
	defaultCheckInterestStateJ = 30
)

var (
	checkInterestStateT = defaultCheckInterestStateT // Interval
	checkInterestStateJ = defaultCheckInterestStateJ // Jitter (secs)
)

// Will check for interest retention and make sure messages
// that have been acked are processed and removed.
// This will check the ack floors of all consumers, and adjust our first sequence accordingly.
func (mset *stream) checkInterestState() {
	if mset == nil || !mset.isInterestRetention() {
		// If we are limits based nothing to do.
		return
	}

	// Ensure only one of these runs at the same time.
	if !mset.cisrun.CompareAndSwap(false, true) {
		return
	}
	defer mset.cisrun.Store(false)

	var ss StreamState
	mset.store.FastState(&ss)

	asflr := uint64(math.MaxUint64)
	for _, o := range mset.getConsumers() {
		o.checkStateForInterestStream(&ss)
		o.mu.RLock()
		chkflr := o.chkflr
		o.mu.RUnlock()
		asflr = min(asflr, chkflr)
	}

	mset.cfgMu.RLock()
	rp := mset.cfg.Retention
	mset.cfgMu.RUnlock()
	// Remove as many messages from the "head" of the stream if there's no interest anymore.
	// Only compact up to the current stream state, consumers may be ahead while replaying.
	// A check floor of 0 means a consumer has not completed a check yet, and Compact(0) purges.
	if rp == InterestPolicy && asflr != math.MaxUint64 && asflr > 0 {
		mset.store.Compact(min(asflr, ss.LastSeq+1))
	}
}

func (mset *stream) isInterestRetention() bool {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.cfg.Retention != LimitsPolicy
}

// NumConsumers reports on number of active consumers for this stream.
func (mset *stream) numConsumers() int {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return len(mset.consumers)
}

// Lock should be held.
func (mset *stream) setConsumer(o *consumer) {
	if _, ok := mset.consumers[o.name]; ok {
		return
	}
	mset.consumers[o.name] = o
	if o.direct || o.sourcing {
		mset.sourcingConsumers++
	}
	// Now update consumers list as well
	mset.clsMu.Lock()
	mset.cList = append(mset.cList, o)
	if mset.csl == nil {
		mset.csl = gsl.NewSublist[*consumer]()
	}
	for _, sub := range o.signalSubs() {
		mset.csl.Insert(sub, o)
	}
	mset.clsMu.Unlock()
}

// Lock should be held.
func (mset *stream) removeConsumer(o *consumer) {
	if c, ok := mset.consumers[o.name]; !ok || c != o {
		return
	}
	delete(mset.consumers, o.name)

	if (o.direct || o.sourcing) && mset.sourcingConsumers > 0 {
		mset.sourcingConsumers--
	}

	// Now update consumers list as well
	mset.clsMu.Lock()
	for i, ol := range mset.cList {
		if ol == o {
			mset.cList = append(mset.cList[:i], mset.cList[i+1:]...)
			break
		}
	}
	// Always remove from the leader sublist.
	if mset.csl != nil {
		for _, sub := range o.signalSubs() {
			mset.csl.Remove(sub, o)
		}
	}
	mset.clsMu.Unlock()
}

// swapSigSubs will update signal Subs for a new subject filter.
// consumer lock should not be held.
func (mset *stream) swapSigSubs(o *consumer, newFilters []string) {
	mset.clsMu.Lock()
	o.mu.Lock()

	if o.closed || o.mset == nil {
		o.mu.Unlock()
		mset.clsMu.Unlock()
		return
	}

	if o.sigSubs != nil {
		if mset.csl != nil {
			for _, sub := range o.sigSubs {
				mset.csl.Remove(sub, o)
			}
		}
		o.sigSubs = nil
	}

	if mset.csl == nil {
		mset.csl = gsl.NewSublist[*consumer]()
	}
	// If no filters are present, add fwcs to sublist for that consumer.
	if newFilters == nil {
		mset.csl.Insert(fwcs, o)
		o.sigSubs = append(o.sigSubs, fwcs)
	} else {
		// If there are filters, add their subjects to sublist.
		for _, filter := range newFilters {
			mset.csl.Insert(filter, o)
			o.sigSubs = append(o.sigSubs, filter)
		}
	}
	o.mu.Unlock()
	mset.clsMu.Unlock()
}

// lookupConsumer will retrieve a consumer by name.
func (mset *stream) lookupConsumer(name string) *consumer {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.consumers[name]
}

func (mset *stream) numDirectConsumers() (num int) {
	mset.clsMu.RLock()
	defer mset.clsMu.RUnlock()

	// Consumers that are direct are not recorded at the store level.
	for _, o := range mset.cList {
		if o.direct {
			num++
		}
	}
	return num
}

// State will return the current state for this stream.
func (mset *stream) state() StreamState {
	return mset.stateWithDetail(false)
}

func (mset *stream) stateWithDetail(details bool) StreamState {
	// mset.store does not change once set, so ok to reference here directly.
	// We do this elsewhere as well.
	store := mset.store
	if store == nil {
		return StreamState{}
	}

	// Currently rely on store for details.
	if details {
		return store.State()
	}
	// Here we do the fast version.
	var state StreamState
	store.FastState(&state)
	return state
}

func (mset *stream) Store() StreamStore {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.store
}

// Determines if the new proposed partition is unique amongst all public consumers.
// Lock should be held.
func (mset *stream) partitionUnique(name string, partitions []string) bool {
	for _, partition := range partitions {
		for n, o := range mset.consumers {
			// Skip the consumer being checked.
			if n == name {
				continue
			}
			// Ignore direct/sourcing consumers.
			if o.direct || o.sourcing {
				continue
			}
			o.mu.RLock()
			if o.subjf == nil {
				o.mu.RUnlock()
				return false
			}
			for _, filter := range o.subjf {
				if SubjectsCollide(partition, filter.subject) {
					o.mu.RUnlock()
					return false
				}
			}
			o.mu.RUnlock()
		}
	}
	return true
}

// Lock should be held.
func (mset *stream) potentialFilteredConsumers() bool {
	numSubjects := len(mset.cfg.Subjects)
	if len(mset.consumers) == 0 || numSubjects == 0 {
		return false
	}
	if numSubjects > 1 || subjectHasWildcard(mset.cfg.Subjects[0]) {
		return true
	}
	return false
}

// Check if there is no interest in this sequence number across our consumers.
// The consumer passed is optional if we are processing the ack for that consumer.
// Write lock should be held.
func (mset *stream) noInterest(seq uint64, obs *consumer) bool {
	return !mset.checkForInterest(seq, obs)
}

// Check if there is no interest in this sequence number and subject across our consumers.
// The consumer passed is optional if we are processing the ack for that consumer.
// Write lock should be held.
func (mset *stream) noInterestWithSubject(seq uint64, subj string, obs *consumer) bool {
	return !mset.checkForInterestWithSubject(seq, subj, obs)
}

// Write lock should be held here for the stream to avoid race conditions on state.
func (mset *stream) checkForInterest(seq uint64, obs *consumer) bool {
	var subj string
	if mset.potentialFilteredConsumers() {
		pmsg := getJSPubMsgFromPool()
		defer pmsg.returnToPool()
		sm, err := mset.store.LoadMsg(seq, &pmsg.StoreMsg)
		if err != nil {
			if err == ErrStoreEOF {
				// Register this as a preAck.
				mset.registerPreAck(obs, seq)
				return true
			}
			mset.clearAllPreAcks(seq)
			return false
		}
		subj = sm.subj
	}
	return mset.checkForInterestWithSubject(seq, subj, obs)
}

// Checks for interest given a sequence and subject.
func (mset *stream) checkForInterestWithSubject(seq uint64, subj string, obs *consumer) bool {
	for _, o := range mset.consumers {
		// If this is us or we have a registered preAck for this consumer continue inspecting.
		if o == obs || mset.hasPreAck(o, seq) {
			continue
		}
		// Check if we need an ack.
		if o.needAck(seq, subj) {
			return true
		}
	}
	mset.clearAllPreAcks(seq)
	return false
}

// Check if we have a pre-registered ack for this sequence.
// Write lock should be held.
func (mset *stream) hasPreAck(o *consumer, seq uint64) bool {
	if o == nil || len(mset.preAcks) == 0 {
		return false
	}
	consumers := mset.preAcks[seq]
	if len(consumers) == 0 {
		return false
	}
	_, found := consumers[o]
	return found
}

// Check if we have all consumers pre-acked for this sequence and subject.
// Write lock should be held.
func (mset *stream) hasAllPreAcks(seq uint64, subj string) bool {
	if len(mset.preAcks) == 0 || len(mset.preAcks[seq]) == 0 {
		return false
	}
	// Since these can be filtered and mutually exclusive,
	// if we have some preAcks we need to check all interest here.
	return mset.noInterestWithSubject(seq, subj, nil)
}

// Check if we have all consumers pre-acked.
// Write lock should be held.
func (mset *stream) clearAllPreAcks(seq uint64) {
	delete(mset.preAcks, seq)
}

// Clear all preAcks below floor.
// Write lock should be held.
func (mset *stream) clearAllPreAcksBelowFloor(floor uint64) {
	for seq := range mset.preAcks {
		if seq < floor {
			delete(mset.preAcks, seq)
		}
	}
}

// Clear all preAcks in [first, last]. Iterates the preAcks map, not the
// range, so callers can pass very wide ranges cheaply.
// Write lock should be held.
func (mset *stream) clearAllPreAcksInRange(first, last uint64) {
	for seq := range mset.preAcks {
		if seq >= first && seq <= last {
			delete(mset.preAcks, seq)
		}
	}
}

// This will register an ack for a consumer if it arrives before the actual message.
func (mset *stream) registerPreAckLock(o *consumer, seq uint64) {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	mset.registerPreAck(o, seq)
}

// This will register an ack for a consumer if it arrives before
// the actual message.
// Write lock should be held.
func (mset *stream) registerPreAck(o *consumer, seq uint64) {
	if o == nil {
		return
	}
	if mset.preAcks == nil {
		mset.preAcks = make(map[uint64]map[*consumer]struct{})
	}
	if mset.preAcks[seq] == nil {
		mset.preAcks[seq] = make(map[*consumer]struct{})
	}
	mset.preAcks[seq][o] = struct{}{}
}

// This will clear an ack for a consumer.
// Write lock should be held.
func (mset *stream) clearPreAck(o *consumer, seq uint64) {
	if o == nil || len(mset.preAcks) == 0 {
		return
	}
	if consumers := mset.preAcks[seq]; len(consumers) > 0 {
		delete(consumers, o)
		if len(consumers) == 0 {
			delete(mset.preAcks, seq)
		}
	}
}

// ackMsg is called into from a consumer when we have a WorkQueue or Interest Retention Policy.
// Returns whether the message at seq was removed as a result of the ACK.
// (Or should be removed in the case of clustered streams, since it requires a message delete proposal)
func (mset *stream) ackMsg(o *consumer, seq uint64) bool {
	if seq == 0 {
		return false
	}

	// Don't make this RLock(). We need to have only 1 running at a time to gauge interest across all consumers.
	mset.mu.Lock()
	if mset.closed.Load() || mset.cfg.Retention == LimitsPolicy {
		mset.mu.Unlock()
		return false
	}

	store := mset.store
	var state StreamState
	store.FastState(&state)

	// If this has arrived before we have processed the message itself.
	if seq > state.LastSeq {
		mset.registerPreAck(o, seq)
		mset.mu.Unlock()
		// We have not removed the message, but should still signal so we could retry later
		// since we potentially need to remove it then.
		return true
	}

	// Always clear pre-ack if here.
	mset.clearPreAck(o, seq)

	// Make sure this sequence is not below our first sequence.
	if seq < state.FirstSeq {
		mset.mu.Unlock()
		return false
	}

	// If there's no interest left on this message for all consumers, we can remove it.
	shouldRemove := mset.noInterest(seq, nil)

	// If nothing else to do.
	if !shouldRemove {
		mset.mu.Unlock()
		return false
	}

	if !mset.isClustered() {
		mset.mu.Unlock()
		// If we are here we should attempt to remove.
		if _, err := store.RemoveMsg(seq); err == ErrStoreEOF {
			// This should not happen, but being pedantic.
			mset.registerPreAckLock(o, seq)
		}
		return true
	}

	// Only propose message deletion to the stream if we're the leader, otherwise followers would also propose.
	// We must be the stream leader, since we are the only one that can guarantee message ordering and ack handling.
	// Either we've stored the message, and we know for sure all consumers have acked the message.
	// Or, we've not stored the message yet (rare), and all consumers have registered as pre-acks,
	// then we do the message delete proposal after we've stored the message instead.
	// Except for a Direct AckNone consumer, as that has a nil consumer here, we still forward the delete proposal.
	if o != nil && !mset.isLeader() {
		// Currently, interest-based streams can race on "no interest" because consumer creates/updates go over
		// the meta layer and published messages go over the stream layer. Some servers could then either store
		// or not store some initial set of messages that gained new interest. To get the stream back in sync,
		// we allow moving the first sequence up.
		// TODO(mvv): later on only the stream leader should determine "no interest"
		interestRaiseFirst := mset.cfg.Retention == InterestPolicy && seq == state.FirstSeq
		mset.mu.Unlock()
		if interestRaiseFirst {
			if _, err := store.RemoveMsg(seq); err == ErrStoreEOF {
				// This should not happen, but being pedantic.
				mset.registerPreAckLock(o, seq)
			}
		}
		// Must still mark as removal if follower. If we become leader later, we must be able to retry the proposal.
		return true
	}

	md := streamMsgDelete{Seq: seq, NoErase: true, Stream: mset.cfg.Name}
	// Directly proposes if stream leader, otherwise forwards it.
	mset.node.ForwardProposal(encodeMsgDelete(&md))
	mset.mu.Unlock()
	return true
}

// Snapshot creates a snapshot for the stream and possibly consumers.
func (mset *stream) snapshot(deadline time.Duration, checkMsgs, includeConsumers bool) (*SnapshotResult, error) {
	if mset.closed.Load() {
		return nil, errStreamClosed
	}
	store := mset.store
	// V2 snapshots intentionally do not run the v1 checkMsgs pre-scan. Unlike
	// v1's raw file copy, v2 reads each message through LoadNextMsg, which uses
	// cacheLookup/msgFromBufEx and verifies the per-record checksum when a
	// freshly loaded cache entry is first read. Corrupt records therefore make
	// LoadNextMsg fail and abort the snapshot instead of silently entering the
	// backup.
	return mset.js.CreateStreamSnapshotV2(store, deadline, includeConsumers, mset.streamAssignment())
}

const snapsDir = "__snapshots__"

// RestoreStream will restore a stream from a snapshot.
func (a *Account) RestoreStream(ncfg *StreamConfig, r io.Reader) (*stream, error) {
	if ncfg == nil {
		return nil, errors.New("nil config on stream restore")
	}

	s, jsa, err := a.checkForJetStream()
	if err != nil {
		return nil, err
	}
	js := jsa.js
	if js == nil {
		return nil, NewJSNotEnabledForAccountError()
	}

	cfg, apiErr := s.checkStreamCfg(ncfg, a, false)
	if apiErr != nil {
		return nil, apiErr
	}

	sd := filepath.Join(jsa.storeDir, snapsDir)
	if _, err := os.Stat(sd); os.IsNotExist(err) {
		if err := os.MkdirAll(sd, defaultDirPerms); err != nil {
			return nil, fmt.Errorf("could not create snapshots directory - %v", err)
		}
	}
	sdir, err := os.MkdirTemp(sd, "snap-")
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(sdir); os.IsNotExist(err) {
		if err := os.MkdirAll(sdir, defaultDirPerms); err != nil {
			return nil, fmt.Errorf("could not create snapshots directory - %v", err)
		}
	}
	defer os.RemoveAll(sdir)

	logAndReturnError := func() error {
		a.mu.RLock()
		err := fmt.Errorf("unexpected content (account=%s)", a.Name)
		if a.srv != nil {
			a.srv.Errorf("Stream restore failed due to %v", err)
		}
		a.mu.RUnlock()
		return err
	}
	sdirCheck := filepath.Clean(sdir) + string(os.PathSeparator)

	_, isClustered := jsa.jetStreamAndClustered()
	jsa.usageMu.RLock()
	selected, tier, hasTier := jsa.selectLimits(cfg.Replicas)
	jsa.usageMu.RUnlock()
	reserved := int64(0)
	if hasTier {
		if isClustered {
			js.mu.RLock()
			_, reserved = js.tieredStreamAndReservationCount(a.Name, tier, &cfg)
			js.mu.RUnlock()
		} else {
			reserved = jsa.tieredReservation(tier, &cfg)
		}
	}

	var bc int64
	tr := tar.NewReader(s2.NewReader(r))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break // End of snapshot
		}
		if err != nil {
			return nil, err
		}
		if hdr.Typeflag != tar.TypeReg {
			return nil, logAndReturnError()
		}
		bc += hdr.Size
		js.mu.RLock()
		err = js.checkAllLimits(&selected, tier, &cfg, reserved, bc)
		js.mu.RUnlock()
		if err != nil {
			return nil, err
		}
		fpath := filepath.Join(sdir, filepath.Clean(hdr.Name))
		if !strings.HasPrefix(fpath, sdirCheck) {
			return nil, logAndReturnError()
		}
		os.MkdirAll(filepath.Dir(fpath), defaultDirPerms)
		fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		_, err = io.Copy(fd, tr)
		fd.Close()
		if err != nil {
			return nil, err
		}
	}

	// Check metadata.
	// The cfg passed in will be the new identity for the stream.
	var fcfg FileStreamInfo
	b, err := os.ReadFile(filepath.Join(sdir, JetStreamMetaFile))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &fcfg); err != nil {
		return nil, err
	}

	// Check to make sure names match.
	if fcfg.Name != cfg.Name {
		return nil, errors.New("stream names do not match")
	}

	// See if this stream already exists.
	if _, err := a.lookupStream(cfg.Name); err == nil {
		return nil, NewJSStreamNameExistRestoreFailedError()
	}
	// Move into the correct place here.
	ndir := filepath.Join(jsa.storeDir, streamsDir, cfg.Name)
	// Remove old one if for some reason it is still here.
	if _, err := os.Stat(ndir); err == nil {
		os.RemoveAll(ndir)
	}
	// Make sure our destination streams directory exists.
	if err := os.MkdirAll(filepath.Join(jsa.storeDir, streamsDir), defaultDirPerms); err != nil {
		return nil, err
	}
	// Move into new location.
	if err := os.Rename(sdir, ndir); err != nil {
		return nil, err
	}

	mset, err := a.addStream(&cfg)
	if err != nil {
		// Make sure to clean up after ourselves here.
		os.RemoveAll(ndir)
		return nil, err
	}
	if !fcfg.Created.IsZero() {
		mset.setCreatedTime(fcfg.Created)
	}

	// Make sure we do an update if the configs have changed.
	if !reflect.DeepEqual(fcfg.StreamConfig, cfg) {
		if err := mset.update(&cfg); err != nil {
			return nil, err
		}
	}

	// Now do consumers.
	odir := filepath.Join(ndir, consumerDir)
	ofis, _ := os.ReadDir(odir)
	for _, ofi := range ofis {
		metafile := filepath.Join(odir, ofi.Name(), JetStreamMetaFile)
		metasum := filepath.Join(odir, ofi.Name(), JetStreamMetaFileSum)
		if _, err := os.Stat(metafile); os.IsNotExist(err) {
			mset.stop(true, false)
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
		buf, err := os.ReadFile(metafile)
		if err != nil {
			mset.stop(true, false)
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
		if _, err := os.Stat(metasum); os.IsNotExist(err) {
			mset.stop(true, false)
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
		var cfg FileConsumerInfo
		if err := json.Unmarshal(buf, &cfg); err != nil {
			mset.stop(true, false)
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
		isEphemeral := !isDurableConsumer(&cfg.ConsumerConfig)
		if isEphemeral {
			// This is an ephermal consumer and this could fail on restart until
			// the consumer can reconnect. We will create it as a durable and switch it.
			cfg.ConsumerConfig.Durable = ofi.Name()
		}
		obs, err := mset.addConsumer(&cfg.ConsumerConfig)
		if err != nil {
			mset.stop(true, false)
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
		if isEphemeral {
			obs.switchToEphemeral()
		}
		if !cfg.Created.IsZero() {
			obs.setCreatedTime(cfg.Created)
		}
		obs.mu.Lock()
		err = obs.readStoredState()
		obs.mu.Unlock()
		if err != nil {
			mset.stop(true, false)
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
	}
	return mset, nil
}

// This is to check for dangling messages on interest retention streams. Only called on account enable.
// Issue https://github.com/nats-io/nats-server/issues/3612
func (mset *stream) checkForOrphanMsgs() {
	mset.mu.RLock()
	consumers := make([]*consumer, 0, len(mset.consumers))
	for _, o := range mset.consumers {
		consumers = append(consumers, o)
	}
	accName, stream := mset.acc.Name, mset.cfg.Name

	var ss StreamState
	mset.store.FastState(&ss)
	mset.mu.RUnlock()

	for _, o := range consumers {
		if err := o.checkStateForInterestStream(&ss); err == errAckFloorHigherThanLastSeq {
			o.mu.RLock()
			s, consumer := o.srv, o.name
			state, _ := o.store.State()
			asflr := state.AckFloor.Stream
			o.mu.RUnlock()
			// Warn about stream state vs our ack floor.
			s.RateLimitWarnf("Detected consumer '%s > %s > %s' ack floor %d is ahead of stream's last sequence %d",
				accName, stream, consumer, asflr, ss.LastSeq)
		}
	}
}

// Check on startup to make sure that consumers replication matches us.
// Interest retention requires replication matches.
func (mset *stream) checkConsumerReplication() {
	mset.mu.RLock()
	defer mset.mu.RUnlock()

	if mset.cfg.Retention != InterestPolicy {
		return
	}

	s, acc := mset.srv, mset.acc
	for _, o := range mset.consumers {
		o.mu.RLock()
		// Consumer replicas 0 can be a legit config for the replicas and we will inherit from the stream
		// when this is the case.
		if mset.cfg.Replicas != o.cfg.Replicas && o.cfg.Replicas != 0 {
			s.Errorf("consumer '%s > %s > %s' MUST match replication (%d vs %d) of stream with interest policy",
				acc, mset.cfg.Name, o.cfg.Name, mset.cfg.Replicas, o.cfg.Replicas)
		}
		o.mu.RUnlock()
	}
}

// startMonitorWg registers a pending monitor goroutine on monitorWg. It is
// held under monitorMu so that the monitorWg.Add can never race a concurrent
// monitorWg.Wait in stopMonitoring. The corresponding monitorWg.Done is done by
// the monitor goroutine directly and must not be wrapped with monitorMu.
func (mset *stream) startMonitorWg() {
	mset.monitorMu.Lock()
	// Quit channel for this new monitor generation. Not if the stream is (being)
	// stopped, stop won't signal again so the monitor must exit right away instead.
	mset.mu.Lock()
	if mset.mqch == nil && !mset.closed.Load() {
		mset.mqch = make(chan struct{})
	}
	mset.mu.Unlock()
	mset.monitorWg.Add(1)
	mset.monitorMu.Unlock()
}

// stopMonitoring signals any running monitor goroutine to quit and waits for
// it to fully exit.
func (mset *stream) stopMonitoring() {
	// monitorMu is held across both the quit signal and the wait so that a
	// concurrent startMonitorWg cannot slip a new monitor generation in
	// between.
	mset.monitorMu.Lock()
	defer mset.monitorMu.Unlock()
	mset.signalMonitorQuit()
	mset.monitorWg.Wait()
}

// Will check if we are running in the monitor already and if not set the appropriate flag.
func (mset *stream) checkInMonitor() bool {
	mset.mu.Lock()
	defer mset.mu.Unlock()

	if mset.inMonitor {
		return true
	}
	mset.inMonitor = true
	return false
}

// Clear us being in the monitor routine.
func (mset *stream) clearMonitorRunning() {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	mset.inMonitor = false
}

// Check if our monitor is running.
func (mset *stream) isMonitorRunning() bool {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.inMonitor
}

// setWriteErr stores the write error in the stream.
func (mset *stream) setWriteErr(err error) {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	mset.setWriteErrLocked(err)
}

func (mset *stream) setWriteErrLocked(err error) {
	if mset.werr != nil {
		return
	}
	// Ignore non-write errors.
	if err == ErrStoreClosed {
		return
	}
	mset.srv.Errorf("JetStream stream '%s > %s' critical write error: %v", mset.acc.Name, mset.cfg.Name, err)
	mset.werr = err
	assert.Unreachable("Stream encountered write error", map[string]any{
		"account": mset.acc.Name,
		"stream":  mset.cfg.Name,
		"err":     err,
	})

	// If stream is replicated, put it in observer mode to make sure another server can pick it up.
	if node := mset.node; node != nil {
		node.StepDown()
		node.SetObserver(true)
	}
}

// getWriteErr returns the write error stored in the stream (if any).
func (mset *stream) getWriteErr() error {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.werr
}

// Adjust accounting for sent messages as part of replication.
func (mset *stream) trackReplicationTraffic(node RaftNode, sz int, r int) {
	// If we are using the system account for NRG, add in the extra sent msgs and bytes to our account
	// so that the end user / account owner has visibility.
	if node.IsSystemAccount() && mset.acc != nil && r > 1 {
		outMsgs := int64(r - 1)
		outBytes := int64(sz * (r - 1))

		mset.acc.stats.Lock()
		mset.acc.stats.outMsgs += outMsgs
		mset.acc.stats.outBytes += outBytes
		mset.acc.stats.rt.outMsgs += outMsgs
		mset.acc.stats.rt.outBytes += outBytes
		mset.acc.stats.Unlock()
	}
}
